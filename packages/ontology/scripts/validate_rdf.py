import os
import sys
from urllib.parse import urlparse
from rdflib import RDFS, Graph, RDF, OWL
from rdflib.term import URIRef

from packages.core.src.core.dependencies.load_core_env import load_core_env
from apps.data_etl_app.src.data_etl_app.dependencies.load_scraper_env import (
    load_scraper_env,
)
from apps.data_etl_app.src.data_etl_app.dependencies.load_open_ai_app_env import (
    load_open_ai_app_env,
)
from apps.data_etl_app.src.data_etl_app.dependencies.load_data_etl_env import (
    load_data_etl_env,
)

# Load environment variables
load_core_env()
load_scraper_env()
load_data_etl_env()
load_open_ai_app_env()

print("Python executable:", sys.executable)
print("Python version:", sys.version)
print("Current working directory:", os.getcwd())
print("\n\n")

from apps.data_etl_app.src.data_etl_app.utils.ttl_generator_util import uri_strip
from packages.core.src.core.utils.rdf_to_graph_util import (
    build_concept_tree,
    get_alt_labels,
    get_label,
)
from packages.core.src.core.models.ontology import Ontology

# === Step 2: Define helpers ===


def is_valid_uri(uri: str) -> bool:
    """Check if a URI is valid and not empty."""
    if not uri or not isinstance(uri, str):
        print(f"Invalid URI (empty or not a string): {uri}")
        return False

    # stripped_uri = uri_strip(uri)
    # if not stripped_uri == uri:
    #     print(
    #         f"Invalid URI (contains unsafe characters), original: {uri}, stripped: {stripped_uri}"
    #     )
    #     return False

    try:
        parsed = urlparse(uri)
        # Must have scheme and netloc for a valid URI, and scheme must be http or https
        return bool(
            parsed.scheme
            and parsed.netloc
            and parsed.scheme.lower() in ("http", "https")
        )
    except Exception:
        print(f"Invalid URI (parsing error): {uri}")
        return False


def is_owl_class(graph: Graph, uri_str: str) -> bool:
    """Check if a URI represents an owl:Class (excluding OWL meta-vocabulary)"""
    uri_ref = URIRef(uri_str)

    # Check if it's declared as owl:Class
    if not (uri_ref, RDF.type, OWL.Class) in graph:
        return False

    # Exclude OWL meta-vocabulary classes
    if uri_str.startswith("http://www.w3.org/2002/07/owl#"):
        return False

    return True


def run_tests(graph: Graph):
    all_subjects = set()
    for subject, _, _ in graph:
        if isinstance(subject, URIRef):
            all_subjects.add(str(subject))

    print(f"Total unique subjects found: {len(all_subjects)}")
    for uri_str in all_subjects:
        if not is_valid_uri(uri_str):
            raise ValueError(f"Invalid URI found: {uri_str}")

        if is_owl_class(graph, uri_str) and uri_str.startswith(
            "http://asu.edu/semantics/SUDOKN/"
        ):
            build_concept_tree(graph, URIRef(uri_str))  # Test building children


def unit_tests(graph: Graph):
    uri = "http://asu.edu/semantics/SUDOKN/SmallDisadvantagedBusiness"
    print(get_label(graph, uri))
    print(get_alt_labels(graph, uri))
    nodes = build_concept_tree(graph, URIRef(uri))
    print(nodes)
    # for subclass, _, _ in graph.triples((None, RDFS.subClassOf, URIRef(uri))):
    #     print(f"subclass: {subclass}")


async def main():
    try:
        from apps.data_etl_app.src.data_etl_app.dependencies.aws_clients import (
            cleanup_data_etl_aws_clients,
            initialize_data_etl_aws_clients,
        )

        # Initialize AWS clients
        await initialize_data_etl_aws_clients()

        ontology_file_path = "../SUDOKN1.1/SUDOKN1_1.rdf"

        print(f"Ontology file path: {ontology_file_path}")
        with open(ontology_file_path, "r", encoding="utf-8") as file:
            ontology_data = file.read()

        ontology = Ontology(rdf=ontology_data, s3_version_id="local-test-version-id")

        # unit_tests(ontology.graph)
        run_tests(ontology.graph)
    finally:
        # Cleanup AWS clients
        await cleanup_data_etl_aws_clients()


if __name__ == "__main__":
    import asyncio

    try:
        asyncio.run(main())
    except Exception as e:
        print(f"Error during validation: {e}")
        sys.exit(1)
    print("RDF validation completed successfully.")
    sys.exit(0)
