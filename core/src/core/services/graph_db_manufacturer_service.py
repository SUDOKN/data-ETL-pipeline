from core.models.field_types import MfgETLDType
from core.services.ttl_generator_service import generate_triples_for_single_mfg
from core.utils.graph_db_client import send_update_query_to_db
from data_etl_app.services.knowledge.ontology_service import OntologyService
from data_etl_app.services.manufacturer_user_form_service import (
    get_manufacturer_user_form_by_mfg_etld1,
)


async def replace_manufacturer_in_graph(mfg_etdl1: MfgETLDType) -> None:
    mfg_user_form = await get_manufacturer_user_form_by_mfg_etld1(mfg_etdl1)
    if not mfg_user_form:
        raise ValueError(
            f"Cannot replace manufacturer, ManufacturerUserForm not found for ETLD1: {mfg_etdl1}"
        )
    ont_inst = await OntologyService.get_instance()
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
