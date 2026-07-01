from collections import deque
import logging
import rdflib
from rdflib.term import URIRef
from rdflib.namespace import RDFS, SKOS
from typing import Callable, List, Optional

from data_etl_app.models.skos_concept import ConceptNode, Concept

logger = logging.getLogger(__name__)


def get_graph(rdf_raw: str) -> rdflib.Graph:
    rdf_cleaned = rdf_raw.replace('xml:lang="asu.edu"', 'xml:lang="en"')
    graph = rdflib.Graph()
    graph.parse(data=rdf_cleaned, format="application/rdf+xml")
    # graph.parse(data=rdf_cleaned, format="turtle")
    return graph


def get_label(graph: rdflib.Graph, uri: str) -> str:
    """Get the rdfs:label or raise ValueError."""
    labels = list(graph.objects(subject=URIRef(uri), predicate=RDFS.label))

    if len(labels) == 0:
        raise ValueError(f"Label not found for URI: {uri}")
    elif len(labels) > 1:
        raise ValueError(
            f"Multiple labels found for URI: {uri}. Found {len(labels)} labels: {[str(label) for label in labels]}"
        )

    return str(labels[0])


def get_alt_labels(graph: rdflib.Graph, uri: str) -> list[str]:
    """Get all skos:altLabel values."""
    return [
        str(label) for _, _, label in graph.triples((URIRef(uri), SKOS.altLabel, None))
    ]


def get_definition(graph: rdflib.Graph, uri: str) -> str:
    """Get the skos:definition value or raise ValueError if not present."""
    definitions = list(graph.objects(subject=URIRef(uri), predicate=SKOS.definition))
    if not definitions:
        raise ValueError(f"Definition not found for URI: {uri}")
    return str(definitions[0])


def build_concept_tree(
    graph: rdflib.Graph, parent_uri: URIRef, labels_seen: set[str]
) -> ConceptNode:
    """Recursively find subclasses and build children structure."""
    label = get_label(graph, str(parent_uri))
    # print(f"Processing label: {label} for URI: {parent_uri}")
    if label in labels_seen:
        raise ValueError(f"Duplicate label '{label}' for URI: {parent_uri}.")
    labels_seen.add(label)

    alt_labels = get_alt_labels(graph, str(parent_uri))
    for alt in alt_labels:
        if alt in labels_seen:
            raise ValueError(f"Duplicate altLabel '{alt}' for URI: {parent_uri}.")
        labels_seen.add(alt)

    definition = get_definition(graph, str(parent_uri))

    children: List[ConceptNode] = []
    for subclass, _, _ in graph.triples((None, RDFS.subClassOf, parent_uri)):
        if not isinstance(subclass, URIRef):
            raise ValueError("Expected subclass to be a URIRef")

        child: ConceptNode = build_concept_tree(graph, subclass, labels_seen)
        children.append(child)

    return {
        "name": label,
        "uri": parent_uri,
        "altLabels": alt_labels,
        "definition": definition,
        "children": children,
        "childrenCount": len(children),
    }


def tree_list_to_flat_helper(
    node: ConceptNode,
    ancestors_so_far: list[str],
) -> list[Concept]:
    """
    Insert ancestors into the node using queue-based expansion
    """
    result = []
    # Queue stores tuples of (node, ancestors)
    queue = deque([(node, ancestors_so_far)])

    while queue:
        current_node, current_ancestors = queue.popleft()
        logger.debug(f"ancestors_so_far: {current_ancestors}")

        # Add current node to result
        result.append(
            Concept(
                name=current_node["name"],
                uri=current_node["uri"],
                altLabels=current_node["altLabels"],
                ancestors=current_ancestors.copy(),
                definition=current_node["definition"],
            )
        )

        # Add children to queue with updated ancestors
        for child in current_node["children"]:
            queue.append((child, current_ancestors + [current_node["name"]]))

        logger.debug(f"node: {current_node}")

    return result


def tree_list_to_flat(tree_knowns: list[ConceptNode]) -> set[Concept]:
    """
    Returns a flat list of Concepts from a list of ConceptNodes.
    Basically, children are replaced with ancestors.
    """
    flat_knowns: list[Concept] = []
    for known in tree_knowns:
        flat_knowns.extend(tree_list_to_flat_helper(known, []))

    return set(flat_knowns)


def find_concept_node_by_name(
    nodes: List[ConceptNode], target_name: str
) -> Optional[ConceptNode]:
    """Recursively search a concept tree for a node matching target_name.

    Matches case-insensitively against the node's name or any of its altLabels.
    """
    normalized = target_name.strip().lower()
    for node in nodes:
        labels = [node["name"], *node.get("altLabels", [])]
        if any(label.lower() == normalized for label in labels):
            return node
        found = find_concept_node_by_name(node["children"], target_name)
        if found is not None:
            return found
    return None


def prune_tree_to_depth(
    nodes: List[ConceptNode], depth: Optional[int]
) -> List[ConceptNode]:
    """Return a copy of the concept tree truncated to `depth` levels.

    The given `nodes` are treated as level 1. ``depth=1`` keeps only those nodes
    with their children removed, ``depth=2`` keeps them plus their immediate
    children, and so on. ``depth=None`` returns the full tree unchanged.

    Each node's ``childrenCount`` is preserved so callers can still tell how many
    immediate children a truncated node has, even when its ``children`` list is
    empty due to the depth cutoff.
    """
    if depth is None:
        return nodes
    if depth < 1:
        return []
    pruned: List[ConceptNode] = []
    for node in nodes:
        children = prune_tree_to_depth(node["children"], depth - 1) if depth > 1 else []
        pruned.append(
            {
                "name": node["name"],
                "uri": node["uri"],
                "altLabels": node["altLabels"],
                "definition": node["definition"],
                "children": children,
                "childrenCount": node["childrenCount"],
            }
        )
    return pruned


def transform_node(node: ConceptNode, fn: Callable[[ConceptNode], ConceptNode]):
    fn(node)
    if node.get("children"):
        for child in node["children"]:
            transform_node(child, fn)
