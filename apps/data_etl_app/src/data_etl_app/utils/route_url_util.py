from pure_utils.env_util import require_env

from core.models.types_and_enums import ConceptTypeEnum

ONTOLOGY_REFRESH_URL = "/ontology/refresh"


def get_full_hosted_url() -> str:
    port = require_env("PORT")
    if not port.isdigit():
        raise ValueError("PORT must be an integer.")
    return f"{require_env('PROTOCOL')}://{require_env('HOSTED_AT')}:{int(port)}"


def get_ontology_concept_tree_route(concept_type: ConceptTypeEnum) -> str:
    """
    Get the ontology concept tree route for a specific concept type.
    """
    return f"/ontology/{concept_type.value}/tree"


def get_full_ontology_concept_tree_url(concept_type: ConceptTypeEnum) -> str:
    """
    Get the ontology concept tree URL for a specific concept type.
    """
    return f"{get_full_hosted_url()}{get_ontology_concept_tree_route(concept_type)}"


def get_ontology_concept_flat_route(concept_type: ConceptTypeEnum) -> str:
    """
    Get the ontology concept flat route for a specific concept type.
    """
    return f"/ontology/{concept_type.value}/flat"


def get_full_ontology_concept_flat_url(concept_type: ConceptTypeEnum) -> str:
    """
    Get the ontology concept flat URL for a specific concept type.
    """
    return f"{get_full_hosted_url()}{get_ontology_concept_flat_route(concept_type)}"
