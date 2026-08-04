from core.models.db.manufacturer import Manufacturer
from core.models.field_types import MfgETLDType, OntologyVersionIDType
from core.services.manufacturer_service import find_manufacturer_by_etld1
from core.services.ttl_generator_service import generate_triples_for_single_mfg
from core.utils.graph_db_client import send_update_query_to_db
from data_etl_app.services.knowledge.ontology_service import OntologyService
from data_etl_app.services.manufacturer_user_form_service import (
    get_manufacturer_user_form_by_mfg_etld1,
)


def _get_consistent_ontology_version_id(
    manufacturer: Manufacturer,
) -> OntologyVersionIDType:
    # certificates/industries/process_caps/material_caps must all trace back to the same ontology snapshot
    versions_by_field = {
        "certificates": manufacturer.certificates,
        "industries": manufacturer.industries,
        "process_caps": manufacturer.process_caps,
        "material_caps": manufacturer.material_caps,
    }
    found_versions = {
        field: results.stats.ontology_version_id
        for field, results in versions_by_field.items()
        if results is not None
    }
    distinct_versions = set(found_versions.values())
    if not distinct_versions:
        raise ValueError(
            f"Cannot determine ontology version for {manufacturer.etld1}: no concept extraction stats found."
        )
    if len(distinct_versions) > 1:
        raise ValueError(
            f"Ontology version mismatch for {manufacturer.etld1} across concept fields: {found_versions}"
        )
    return distinct_versions.pop()


async def replace_manufacturer_in_graph(mfg_etdl1: MfgETLDType) -> None:
    mfg_user_form = await get_manufacturer_user_form_by_mfg_etld1(mfg_etdl1)
    if not mfg_user_form:
        raise ValueError(
            f"Cannot replace manufacturer, ManufacturerUserForm not found for ETLD1: {mfg_etdl1}"
        )

    manufacturer = await find_manufacturer_by_etld1(mfg_etdl1)
    if not manufacturer:
        raise ValueError(
            f"Cannot determine ontology version, Manufacturer not found for ETLD1: {mfg_etdl1}"
        )
    ontology_version_id = _get_consistent_ontology_version_id(manufacturer)
    ont_inst = await OntologyService.create_for_version(ontology_version_id)
    ttl_data = generate_triples_for_single_mfg(ont_inst, mfg_user_form, False)
    mfg_uri_prefix = f"http://asu.edu/semantics/SUDOKN/{mfg_etdl1}"

    query = f"""
    DELETE {{
        ?s ?p ?o .
    }}
    WHERE {{
        ?s ?p ?o .
        FILTER(isIRI(?s) && STRSTARTS(STR(?s), "{mfg_uri_prefix}"))
    }};
    
    INSERT DATA {{
        {ttl_data}
    }}
    """
    await send_update_query_to_db(query, debug=False)
