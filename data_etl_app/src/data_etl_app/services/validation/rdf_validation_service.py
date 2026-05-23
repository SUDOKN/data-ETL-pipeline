from __future__ import annotations

from urllib.parse import urlparse

from rdflib import Graph, OWL, RDF
from rdflib.term import URIRef

from data_etl_app.utils.rdf_to_graph_util import build_concept_tree, get_graph

SUDOKN_BASE_URI = "http://asu.edu/semantics/SUDOKN/"


def is_valid_uri(uri: str) -> bool:
    if not uri or not isinstance(uri, str):
        return False

    try:
        parsed = urlparse(uri)
        return bool(
            parsed.scheme
            and parsed.netloc
            and parsed.scheme.lower() in ("http", "https")
        )
    except Exception:
        return False


def is_owl_class(graph: Graph, uri_str: str) -> bool:
    uri_ref = URIRef(uri_str)

    if not (uri_ref, RDF.type, OWL.Class) in graph:
        return False

    return not uri_str.startswith("http://www.w3.org/2002/07/owl#")


def validate_rdf_graph(graph: Graph) -> dict:
    all_subjects = sorted(
        {str(subject) for subject in graph.subjects() if isinstance(subject, URIRef)}
    )

    issues: list[dict[str, str]] = []
    validated_concept_roots: list[str] = []

    for uri_str in all_subjects:
        if not is_valid_uri(uri_str):
            issues.append(
                {
                    "type": "invalid_uri",
                    "uri": uri_str,
                    "message": f"Invalid URI found: {uri_str}",
                }
            )
            continue

        if is_owl_class(graph, uri_str) and uri_str.startswith(SUDOKN_BASE_URI):
            try:
                build_concept_tree(graph, URIRef(uri_str), set())
                validated_concept_roots.append(uri_str)
            except Exception as exc:
                issues.append(
                    {
                        "type": "concept_tree_validation_error",
                        "uri": uri_str,
                        "message": str(exc),
                    }
                )

    return {
        "valid": len(issues) == 0,
        "total_unique_subjects": len(all_subjects),
        "validated_concept_roots": validated_concept_roots,
        "issues": issues,
    }


def validate_rdf_content(rdf_text: str) -> dict:
    graph = get_graph(rdf_text)
    return validate_rdf_graph(graph)