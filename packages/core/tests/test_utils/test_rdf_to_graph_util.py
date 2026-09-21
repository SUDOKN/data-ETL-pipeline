from rdflib.term import URIRef

from core.utils.rdf_to_graph_util import (
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


def _concept(name, level, ancestors=(), alt=(), definition="", children=()):
    from core.models.skos_concept import Concept

    return Concept(
        name=name,
        uri=f"urn:{name}",
        level=level,
        altLabels=list(alt),
        children=list(children),
        ancestors=list(ancestors),
        definition=definition,
    )


def _small_vocabulary():
    return {
        _concept("Coating", 1, alt=(), definition="Covering a substrate with a layer.", children=("Painting",)),
        _concept("Painting", 2, ancestors=("Coating",), alt=("Paint Application",), definition="Applying paint to a surface.", children=("Wet Painting",)),
        _concept("Wet Painting", 3, ancestors=("Coating", "Painting"), definition="Liquid paint applied and cured."),
        _concept("Blank", 1, definition=""),
    }


def test_the_plain_outline_is_unchanged_by_the_dash_line_option():
    """Default rendering: aliases inline as "(also: …)", no definitions — the
    shape every published grounding prompt describes."""
    from core.utils.rdf_to_graph_util import render_concept_outline

    assert render_concept_outline(_small_vocabulary()).splitlines() == [
        "Blank",
        "Coating",
        "  Painting (also: Paint Application)",
        "    Wet Painting",
    ]


def test_the_dash_line_outline_keeps_the_label_line_bare():
    """Step 2 (V2, user decision 2026-09-19): the label line carries the label
    and nothing else; the line beneath it, at the same indentation, begins with
    the dash marker and carries the other names then the full definition; a
    concept with neither gets no dash line."""
    from core.utils.rdf_to_graph_util import DASH_LINE_MARKER, render_concept_outline

    lines = render_concept_outline(_small_vocabulary(), with_definitions=True).splitlines()
    assert lines == [
        "Blank",
        "Coating",
        f"{DASH_LINE_MARKER}Covering a substrate with a layer.",
        "  Painting",
        f"  {DASH_LINE_MARKER}also known as: Paint Application. Applying paint to a surface.",
        "    Wet Painting",
        f"    {DASH_LINE_MARKER}Liquid paint applied and cured.",
    ]
    label_lines = [l for l in lines if not l.lstrip().startswith(DASH_LINE_MARKER)]
    assert all("(" not in l for l in label_lines)
