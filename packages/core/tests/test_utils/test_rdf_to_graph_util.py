from rdflib.term import URIRef

from packages.knowledge.src.knowledge.utils.rdf_to_graph_util import (
    build_concept_tree,
    get_graph,
    tree_list_to_flat,
)


def test_build_concept_tree_allows_imported_root_without_definition():
    rdf_text = """<?xml version='1.0' encoding='utf-8'?>
<rdf:RDF
    xmlns:rdf='http://www.w3.org/1999/02/22-rdf-syntax-ns#'
    xmlns:rdfs='http://www.w3.org/2000/01/rdf-schema#'
    xmlns:owl='http://www.w3.org/2002/07/owl#'
    xmlns:skos='http://www.w3.org/2004/02/skos/core#'>
  <owl:Class rdf:about='http://asu.edu/semantics/SUDOKN/Industry'>
    <rdfs:label>Industry</rdfs:label>
  </owl:Class>
  <owl:Class rdf:about='http://asu.edu/semantics/SUDOKN/AerospaceIndustry'>
    <rdfs:subClassOf rdf:resource='http://asu.edu/semantics/SUDOKN/Industry' />
    <rdfs:label>Aerospace</rdfs:label>
    <skos:definition>A defined industry concept.</skos:definition>
  </owl:Class>
</rdf:RDF>
"""

    graph = get_graph(rdf_text)
    tree = build_concept_tree(
        graph,
        URIRef("http://asu.edu/semantics/SUDOKN/Industry"),
    )

    assert tree["name"] == "Industry"
    assert [child["name"] for child in tree["children"]] == ["Aerospace"]

    flattened_concepts = tree_list_to_flat(tree["children"])
    assert {concept.name for concept in flattened_concepts} == {"Aerospace"}
