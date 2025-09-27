import os

from data_etl_app.models.types_and_enums import ConceptTypeEnum

PROTOCOL = os.getenv("PROTOCOL")
HOSTED_AT = os.getenv("HOSTED_AT")
PORT = os.getenv("PORT")

if not PROTOCOL:
    raise ValueError("PROTOCOL must be set in the environment variables.")
if not HOSTED_AT:
    raise ValueError("HOSTED_AT must be set in the environment variables.")
if not PORT:
    raise ValueError("PORT must be set in the environment variables.")

try:
    PORT = int(PORT)
except ValueError:
    raise ValueError("PORT must be an integer.")

FULL_HOSTED_URL = f"{PROTOCOL}://{HOSTED_AT}:{PORT}"

ONTOLOGY_REFRESH_URL = "/ontology/refresh"


def get_ontology_concept_tree_route(concept_type: ConceptTypeEnum) -> str:
    """
    Get the ontology concept tree route for a specific concept type.
    """
    return f"/ontology/{concept_type.value}/tree"


def get_full_ontology_concept_tree_url(concept_type: ConceptTypeEnum) -> str:
    """
    Get the ontology concept tree URL for a specific concept type.
    """
    return f"{FULL_HOSTED_URL}{get_ontology_concept_tree_route(concept_type)}"


def get_ontology_concept_flat_route(concept_type: ConceptTypeEnum) -> str:
    """
    Get the ontology concept flat route for a specific concept type.
    """
    return f"/ontology/{concept_type.value}/flat"


def get_full_ontology_concept_flat_url(concept_type: ConceptTypeEnum) -> str:
    """
    Get the ontology concept flat URL for a specific concept type.
    """
    return f"{FULL_HOSTED_URL}{get_ontology_concept_flat_route(concept_type)}"
