import rdflib
from rdflib.term import URIRef
from rdflib.namespace import RDFS, SKOS
from typing import Callable, List

from models.skos_concept import ConceptNode, Concept


def get_graph(rdf_raw: str) -> rdflib.Graph:
    rdf_cleaned = rdf_raw.replace('xml:lang="asu.edu"', 'xml:lang="en"')
    graph = rdflib.Graph()
    graph.parse(data=rdf_cleaned, format="application/rdf+xml")
    return graph


def get_label(graph: rdflib.Graph, uri: str) -> str:
    """Get the rdfs:label or raise ValueError."""
    label = graph.value(subject=URIRef(uri), predicate=RDFS.label)
    if label:
        return str(label)

    raise ValueError(f"Label not found for URI: {uri}")


def get_alt_labels(graph: rdflib.Graph, uri: str) -> list[str]:
    """Get all skos:altLabel values."""
    return [
        str(label) for _, _, label in graph.triples((URIRef(uri), SKOS.altLabel, None))
    ]


def build_children(graph: rdflib.Graph, parent_uri: URIRef) -> List[ConceptNode]:
    """Recursively find subclasses and build children structure."""
    children: List[ConceptNode] = []
    for subclass, _, _ in graph.triples((None, RDFS.subClassOf, parent_uri)):
        if not isinstance(subclass, URIRef):
            continue
        child: ConceptNode = {
            "name": get_label(graph, str(subclass)),
            "altLabels": get_alt_labels(graph, str(subclass)),
            "children": build_children(graph, subclass),
            "ancestors": None,
            "antiLabels": None,
        }
        children.append(child)
    return children


def insert_ancestors(
    node: ConceptNode,
    ancestors_so_far: list[str],
):
    """
    Insert ancestors into the node
    """
    # print(f'ancestors_so_far: {ancestors_so_far}')
    node["ancestors"] = ancestors_so_far.copy()
    for child in node["children"]:
        insert_ancestors(child, ancestors_so_far + [node["name"]])
    # print(f'node: {node}')
    return node


def tree_list_to_flat(tree_knowns: list[ConceptNode]) -> list[Concept]:
    """
    Convert a tree list of knowns to a flat list of knowns.
    CAUTION: removes children from each node
    """
    flat_knowns: list[Concept] = []
    for known in tree_knowns:
        flat_known: Concept = Concept(
            known["name"],
            known["altLabels"],
            known.get("ancestors", None),
            known.get("antiLabels", None),
        )

        flat_knowns.append(flat_known)
        if known.get("children") and known["children"]:
            flat_knowns.extend(tree_list_to_flat(known["children"]))

    return flat_knowns


def transform_node(node: ConceptNode, fn: Callable[[ConceptNode], ConceptNode]):
    fn(node)
    if node.get("children"):
        for child in node["children"]:
            transform_node(child, fn)


def insert_dummy_antiLabels(node: ConceptNode):
    if not node.get("antiLabels"):
        node["antiLabels"] = []
    return node
