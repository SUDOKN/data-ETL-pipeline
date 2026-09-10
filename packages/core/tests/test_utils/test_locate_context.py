"""The fold's code-derived mention location (aggregation_fold docstring, D;
2026-09-05): the nearest Markdown heading or pipe-table header row STRICTLY
ABOVE a snippet's first line, within the snippet's page block.

Replaces the synthesis model's per-snippet context quotes (2026-09-03 to
2026-09-05), which nothing downstream read and whose enumeration invited a
repetition loop — one 65-snippet record under one heading drew 4,850 copies of
that heading and a truncated answer (run 20260904T184906).
"""

from core.utils.aggregation_fold import WindowInput, fold_document

SEP = "#" * 50


def _locations(text: str, forms: list[str]) -> dict[str, str | None]:
    """snippet -> location, over one window."""
    result = fold_document([WindowInput(text, forms)])
    return {m.snippet: m.location for b in result.bundles for m in b.mentions}


def test_the_nearest_heading_above_wins_and_blank_lines_do_not_matter():
    text = (
        f"{SEP}\nhttps://acme.example/\n\n"
        "# Acme Machine\n\n## Materials\n\n\nWe stock Aluminum.\n"
        "## Services\nWe machine Brass.\n"
    )
    locs = _locations(text, ["Aluminum", "Brass"])
    assert locs["We stock Aluminum."] == "## Materials"
    assert locs["We machine Brass."] == "## Services"


def test_a_table_row_locates_to_its_header_row_not_the_row_above():
    text = (
        f"{SEP}\nhttps://acme.example/parts\n\n"
        "## Replacement Parts\n\n"
        "| Part | Model |\n|---|---|\n| Bearing A | 204B |\n| Bearing B | 206L |\n"
    )
    locs = _locations(text, ["Bearing A", "Bearing B"])
    assert locs["| Bearing A | 204B |"] == "| Part | Model |"
    assert locs["| Bearing B | 206L |"] == "| Part | Model |"  # not "| Bearing A | 204B |"


def test_a_snippet_that_is_itself_a_heading_or_header_row_takes_the_line_above_it():
    """Strictly preceding: a heading is located by the heading over IT."""
    text = (
        f"{SEP}\nhttps://acme.example/\n\n"
        "# Acme Machine\n\n## Aluminum Casting\nWe cast it.\n"
        "| Alloy | Temper |\n|---|---|\n| A356 | T6 |\n"
    )
    locs = _locations(text, ["Aluminum Casting", "Alloy"])
    assert locs["## Aluminum Casting"] == "# Acme Machine"
    assert locs["| Alloy | Temper |"] == "## Aluminum Casting"


def test_a_page_boundary_stops_the_search_and_a_headingless_page_gets_none():
    text = (
        f"{SEP}\nhttps://acme.example/materials\n\n"
        "## Materials\nWe stock Aluminum.\n"
        f"{SEP}\nhttps://acme.example/about\n\n"
        "Family owned. We machine Brass.\n"
    )
    locs = _locations(text, ["Aluminum", "Brass"])
    assert locs["We stock Aluminum."] == "## Materials"
    assert locs["We machine Brass."] is None  # the previous page's heading never leaks


def test_the_page_separator_and_decoration_lines_are_not_headings():
    text = (
        f"{SEP}\nhttps://acme.example/\n\n"
        "-----\nWe stock Aluminum.\n"  # a legacy divider, then the snippet, no heading above
    )
    assert _locations(text, ["Aluminum"])["We stock Aluminum."] is None


def test_a_table_data_row_above_a_prose_snippet_is_passed_over():
    """Only a HEADER row (one over a |---| separator) can be a location; a
    data row is skipped, so the prose below a table takes the heading."""
    text = (
        f"{SEP}\nhttps://acme.example/\n\n"
        "## Parts\n| Part | Model |\n|---|---|\n| Bearing | 204B |\n\n"
        "All parts ship with Aluminum housings.\n"
    )
    assert _locations(text, ["Aluminum"])["All parts ship with Aluminum housings."] == "## Parts"


def test_an_html_island_table_falls_through_to_the_nearest_heading():
    text = (
        f"{SEP}\nhttps://acme.example/\n\n"
        "## Specifications\n<table>\n<tr><th>Alloy</th></tr>\n<tr><td>Aluminum 6061</td></tr>\n</table>\n"
    )
    assert _locations(text, ["Aluminum 6061"])["<tr><td>Aluminum 6061</td></tr>"] == "## Specifications"


def test_a_recurring_snippet_is_located_at_each_occurrence():
    """What the per-distinct-snippet quote could not do (14 of 15 recurring
    snippets in run 20260905T014738 sat under different headings)."""
    text = (
        f"{SEP}\nhttps://acme.example/\n\n"
        "## Milling\nCNC machining on request.\n"
        "## Turning\nCNC machining on request.\n"
    )
    result = fold_document([WindowInput(text, ["CNC machining"])])
    mentions = sorted((m.start, m.location) for b in result.bundles for m in b.mentions)
    assert [loc for _, loc in mentions] == ["## Milling", "## Turning"]
