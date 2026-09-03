"""Tests for the ``markdown_v1`` rendering contract (``html_to_markdown``).

Each test pins one rule from the module docstring's contract. The rendering is
frozen: a deliberate rule change bumps FORMAT_MARKDOWN to markdown_v2 and
updates the pinned expectations here — it never silently changes output shape
(dedup line-matching, phrase identity and eval baselines all key on the bytes).
"""

import re

import pytest

from scraper.utils.html_to_markdown import (
    FORMAT_LEGACY_TEXT,
    FORMAT_MARKDOWN,
    html_to_markdown,
)


class TestFormatConstants:
    def test_version_strings(self):
        assert FORMAT_MARKDOWN == "markdown_v3"
        assert FORMAT_LEGACY_TEXT == "text_v1"


class TestHeadingPropagation:
    """THE V3 RULE: bare-label runs inherit their nearest heading (measured
    2026-09-02: 2/335 -> 332/335 model-code recall on the dealer-catalog
    window with unchanged prompts; list items deliberately untouched)."""

    @staticmethod
    def _labels(n, prefix="M-"):
        return "".join(f"<div>{prefix}{i} (1)</div>" for i in range(n))

    def test_run_of_five_labels_inherits_heading(self):
        md = html_to_markdown(f"<h1>WELLSAW</h1>{self._labels(5)}")
        for i in range(5):
            assert f"WELLSAW: M-{i} (1)" in md

    def test_run_of_four_is_left_alone(self):
        md = html_to_markdown(f"<h1>WELLSAW</h1>{self._labels(4)}")
        assert "WELLSAW: M-0" not in md
        assert "M-0 (1)" in md

    def test_blank_lines_inside_run_do_not_break_it(self):
        body = "<div>A1x</div><p></p><div>B2x</div><div>C3x</div><div>D4x</div><div>E5x</div>"
        md = html_to_markdown(f"<h2>JET</h2>{body}")
        assert "JET: A1x" in md and "JET: E5x" in md

    def test_nearest_heading_wins_and_any_level_counts(self):
        md = html_to_markdown(
            f"<h1>Equipment</h1><h3>WELLSAW</h3>{self._labels(5)}"
        )
        assert "WELLSAW: M-0 (1)" in md
        assert "Equipment: M-0" not in md

    def test_long_heading_propagates_nothing(self):
        heading = "Our full range of metalworking machinery and accessories catalog"
        md = html_to_markdown(f"<h1>{heading}</h1>{self._labels(5)}")
        assert ": M-0 (1)" not in md

    def test_label_already_starting_with_heading_is_untouched(self):
        md = html_to_markdown(
            "<h1>JET</h1><div>JET 100</div><div>JET 200</div><div>JET 300</div>"
            "<div>JET 400</div><div>JET 500</div>"
        )
        assert "JET: JET 100" not in md
        assert "JET 100" in md

    def test_list_items_are_not_prefixed(self):
        items = "".join(f"<li>Part {i}x</li>" for i in range(6))
        md = html_to_markdown(f"<h1>JET</h1><ul>{items}</ul>")
        assert "JET: Part" not in md
        assert "- Part 0x" in md

    def test_sentence_lines_break_and_escape_the_run(self):
        md = html_to_markdown(
            f"<h1>JET</h1>{self._labels(3)}"
            "<p>These are the finest machines we carry today.</p>"
            f"{self._labels(3, prefix='N-')}"
        )
        # two runs of 3, split by prose: neither reaches the 5-label minimum
        assert "JET: M-0" not in md and "JET: N-0" not in md

    def test_headingless_page_propagates_nothing(self):
        md = html_to_markdown(self._labels(5))
        assert ": M-0 (1)" not in md
        assert "M-0 (1)" in md


class TestNavigationCompaction:
    """V2: nav compacts to one labeled line — user decision 2026-08-29
    (kept over dropping, for tail recall + Location self-labeling)."""

    def test_nav_becomes_single_labeled_line(self):
        md = html_to_markdown(
            "<nav><ul><li><a href='/'>Home</a></li><li><a href='/cap'>Capabilities</a></li>"
            "<li><a href='/contact'>Contact</a></li></ul></nav><p>Real content.</p>"
        )
        assert "Navigation: Home · Capabilities · Contact" in md
        assert "- Home" not in md
        assert "Real content." in md

    def test_role_navigation_div_compacts_too(self):
        md = html_to_markdown(
            "<div role='navigation'><a href='/a'>About</a> <a href='/b'>News</a></div>"
        )
        assert md == "Navigation: About · News"

    def test_duplicate_mobile_menu_emits_once(self):
        nav = "<nav><a href='/'>Home</a><a href='/x'>About</a></nav>"
        md = html_to_markdown(f"{nav}<p>Body.</p>{nav}")
        assert md.count("Navigation: Home · About") == 1

    def test_two_different_navs_both_kept(self):
        md = html_to_markdown(
            "<nav><a href='/'>Home</a></nav>"
            "<nav aria-label='breadcrumb'><a href='/'>Home</a><a href='/w'>Welding</a></nav>"
        )
        assert "Navigation: Home\n" in md + "\n"
        assert "Navigation: Home · Welding" in md

    def test_linkless_nav_falls_back_to_text_and_empty_nav_vanishes(self):
        assert html_to_markdown("<nav><span>Menu</span></nav>") == "Navigation: Menu"
        assert html_to_markdown("<nav><a href='/'><img src='x.png'></a></nav>") == ""

    def test_mega_menu_flattens_into_the_one_line(self):
        md = html_to_markdown(
            "<nav><h3>Products</h3><ul><li><a href='/1'>Doors</a></li>"
            "<li><a href='/2'>Frames</a></li></ul></nav>"
        )
        assert md.count("\n") == 0
        assert "Doors · Frames" in md


class TestHiddenAndModalDrops:
    def test_hidden_attribute_subtree_dropped(self):
        md = html_to_markdown("<div hidden><p>Consent text</p></div><p>Kept</p>")
        assert md == "Kept"

    def test_aria_hidden_true_dropped(self):
        md = html_to_markdown("<div aria-hidden='true'>Skip to content</div><p>Kept</p>")
        assert md == "Kept"

    def test_role_dialog_and_aria_modal_dropped(self):
        md = html_to_markdown(
            "<div role='dialog'><p>Subscribe to our newsletter!</p></div>"
            "<div aria-modal='true'><p>We value your privacy</p></div><p>Kept</p>"
        )
        assert md == "Kept"

    def test_closed_details_still_included(self):
        """Regression guard: the accordion win survives the hidden-narrowing."""
        md = html_to_markdown(
            "<details><summary>Certs</summary><p>ITAR registered</p></details>"
        )
        assert "ITAR registered" in md


class TestConsentVendorDrops:
    def test_complianz_container_dropped(self):
        md = html_to_markdown(
            "<div id='cmplz-cookiebanner-container'><p>We use cookies to improve.</p></div>"
            "<p>Kept</p>"
        )
        assert md == "Kept"

    def test_onetrust_and_cookiebot_dropped_by_class(self):
        md = html_to_markdown(
            "<div class='onetrust-pc-dark-filter'>x</div>"
            "<div id='CybotCookiebotDialog'>y</div><p>Kept</p>"
        )
        assert md == "Kept"

    def test_cookie_depositor_product_survives(self):
        """The food-machinery guard: bare 'cookie' in ids/classes/content is
        NOT a drop signal — cookie depositors are real products."""
        md = html_to_markdown(
            "<div class='cookie-depositor-section'><h2>Cookie Depositor Model CD-2</h2>"
            "<p>Deposits 200 cookies per minute.</p></div>"
        )
        assert "Cookie Depositor Model CD-2" in md
        assert "200 cookies per minute" in md


class TestAltCollapse:
    def test_consecutive_duplicate_logo_alts_collapse(self):
        md = html_to_markdown(
            "<a><img src='1.png' alt='Alec Model'></a>"
            "<a><img src='2.png' alt='Alec Model'></a>"
            "<div><img src='3.png' alt='Alec Model'></div><p>Body</p>"
        )
        assert md.count("Alec Model") == 1

    def test_real_text_between_identical_alts_resets(self):
        md = html_to_markdown(
            "<p><img alt='ISO 9001'> audited annually <img alt='ISO 9001'></p>"
        )
        assert md.count("ISO 9001") == 2

    def test_different_alts_all_kept(self):
        md = html_to_markdown("<img alt='ISO 9001'><img alt='AS9100'>")
        assert "ISO 9001" in md and "AS9100" in md


class TestHeadings:
    def test_levels_map_to_hash_runs(self):
        md = html_to_markdown("<h1>Alpha</h1><h3>Beta</h3><h6>Gamma</h6>")
        assert "# Alpha" in md
        assert "### Beta" in md
        assert "###### Gamma" in md

    def test_heading_never_reaches_separator_length(self):
        """floor_scan's page separator is 10+ hashes alone on a line; headings
        max out at 6 hashes plus a space plus text."""
        md = html_to_markdown("<h6>Deep</h6>")
        assert not re.search(r"^#{10,}\s*$", md, re.M)

    def test_inner_markup_flattens_into_heading_line(self):
        md = html_to_markdown("<h2><span>Our</span> <b>Capabilities</b></h2>")
        assert "## Our Capabilities" in md


class TestLists:
    def test_unordered_items_get_dashes(self):
        md = html_to_markdown("<ul><li>Milling</li><li>Turning</li></ul>")
        assert "- Milling" in md
        assert "- Turning" in md

    def test_nested_list_indents_two_spaces_per_level(self):
        md = html_to_markdown(
            "<ul><li>CNC Machining<ul><li>3-axis</li><li>5-axis</li></ul></li></ul>"
        )
        assert "- CNC Machining" in md
        assert "\n  - 3-axis" in md
        assert "\n  - 5-axis" in md

    def test_ordered_list_gets_real_numbers(self):
        md = html_to_markdown("<ol><li>First</li><li>Second</li></ol>")
        assert "1. First" in md
        assert "2. Second" in md

    def test_empty_items_emit_nothing(self):
        """Carousel-indicator dots (user-spotted 2026-08-28): empty <li> must
        not become bare markers — innerText never showed them either."""
        md = html_to_markdown('<ol class="carousel-indicators"><li></li><li></li><li></li></ol>')
        assert md == ""

    def test_numbering_stays_gapless_around_empty_items(self):
        md = html_to_markdown("<ol><li>Real</li><li></li><li>Also real</li></ol>")
        assert "1. Real" in md
        assert "2. Also real" in md
        assert "3." not in md

    def test_definition_list_pairs_term_and_definition(self):
        md = html_to_markdown("<dl><dt>Lead time</dt><dd>Two weeks</dd></dl>")
        assert "**Lead time:** Two weeks" in md


class TestSimpleTables:
    def test_pipe_table_with_header_rule(self):
        md = html_to_markdown(
            "<table><tr><th>Process</th><th>Tolerance</th></tr>"
            "<tr><td>Milling</td><td>0.005</td></tr></table>"
        )
        assert "| Process | Tolerance |" in md
        assert "|---|---|" in md
        assert "| Milling | 0.005 |" in md

    def test_pipes_in_cell_text_are_escaped(self):
        md = html_to_markdown("<table><tr><td>A|B</td></tr></table>")
        assert "A\\|B" in md

    def test_caption_precedes_the_rows(self):
        md = html_to_markdown(
            "<table><caption>Machines</caption><tr><td>Haas</td></tr></table>"
        )
        assert md.index("Machines") < md.index("| Haas |")


class TestIslandTables:
    def test_rowspan_triggers_island_and_survives(self):
        md = html_to_markdown(
            '<table><tr><th rowspan="2">Machine</th><th>X</th></tr>'
            "<tr><th>Y</th></tr><tr><td>Haas</td><td>30</td></tr></table>"
        )
        assert '<th rowspan="2">Machine</th>' in md
        assert "|---|" not in md

    def test_colspan_triggers_island(self):
        md = html_to_markdown(
            '<table><tr><td colspan="3">Envelope</td></tr></table>'
        )
        assert '<td colspan="3">Envelope</td>' in md

    def test_island_strips_every_other_attribute(self):
        md = html_to_markdown(
            '<table class="fancy" style="x"><tr><td class="c" rowspan="2" data-x="1">A</td></tr>'
            "<tr><td>B</td></tr></table>"
        )
        assert "class" not in md
        assert "style" not in md
        assert "data-x" not in md
        assert 'rowspan="2"' in md

    def test_list_inside_cell_triggers_island(self):
        md = html_to_markdown(
            "<table><tr><td><ul><li>a</li><li>b</li></ul></td></tr></table>"
        )
        assert md.startswith("<table>")

    def test_island_is_one_row_per_line(self):
        md = html_to_markdown(
            '<table><tr><td rowspan="2">A</td><td>B</td></tr><tr><td>C</td></tr></table>'
        )
        lines = md.split("\n")
        assert sum(1 for ln in lines if ln.startswith("<tr>") and ln.endswith("</tr>")) == 2

    def test_nested_table_rows_not_promoted_to_outer_rows(self):
        md = html_to_markdown(
            "<table><tr><td><table><tr><td>inner</td></tr></table></td></tr></table>"
        )
        # inner content flattens into the outer cell; exactly one <tr> line
        assert md.count("<tr>") == 1
        assert "inner" in md

    def test_unparseable_span_counts_as_one(self):
        md = html_to_markdown('<table><tr><td rowspan="garbage">A</td></tr></table>')
        assert "| A |" in md  # stays on the pipe path


class TestLinksAndImages:
    def test_link_text_kept_href_dropped(self):
        md = html_to_markdown('<p><a href="https://example.com/x">Contact us</a></p>')
        assert "Contact us" in md
        assert "example.com" not in md

    def test_no_bare_url_lines_minted(self):
        """A bare URL alone on a line is a floor_scan page BARRIER; bodies must
        not gain new ones through link rendering."""
        md = html_to_markdown(
            '<div><a href="https://example.com/a">Read more</a></div>'
            '<div><a href="https://example.com/b">Details</a></div>'
        )
        assert not re.search(r"^[ \t]*https?://\S+[ \t]*$", md, re.M)

    def test_img_alt_becomes_text(self):
        md = html_to_markdown('<p>Certified <img src="iso.png" alt="ISO 9001:2015"> since 2010</p>')
        assert "ISO 9001:2015" in md

    def test_img_without_alt_vanishes(self):
        assert html_to_markdown('<p>Before <img src="deco.png"> after</p>') == "Before after"


class TestDroppedAndHidden:
    def test_scripts_styles_svg_dropped(self):
        md = html_to_markdown(
            "<script>var x=1;</script><style>.a{}</style><svg><path d='M0 0'/></svg><p>Kept</p>"
        )
        assert md == "Kept"

    def test_form_controls_dropped_but_labels_kept(self):
        md = html_to_markdown(
            "<form><label>First Name</label><input value='x'>"
            "<select><option>USA</option></select><button>Submit</button></form>"
        )
        assert "First Name" in md
        assert "USA" not in md
        assert "Submit" not in md

    def test_closed_details_content_included(self):
        """The 53 measured accordions: in the DOM means in the text, rendered
        or not (innerText silently lost these)."""
        md = html_to_markdown(
            "<details><summary>Certifications</summary><p>ITAR registered</p></details>"
        )
        assert "Certifications" in md
        assert "ITAR registered" in md


class TestTextPlane:
    def test_entities_decode_to_visible_text(self):
        md = html_to_markdown("<p>Smith &amp; Sons &lt;precision&gt;</p>")
        assert "Smith & Sons" in md
        assert "&amp;" not in md

    def test_inline_siblings_keep_their_space(self):
        md = html_to_markdown("<p><b>Swiss</b> <i>machining</i></p>")
        assert "Swiss machining" in md

    def test_whitespace_runs_collapse(self):
        md = html_to_markdown("<p>Too   many\n\t spaces</p>")
        assert md == "Too many spaces"

    def test_pre_keeps_line_structure(self):
        # interior space runs collapse globally (see the pre rule's comment);
        # the line structure is what pre preserves
        md = html_to_markdown("<pre>line one\n  line two</pre>")
        assert "line one\n  line two" in md

    def test_at_most_one_blank_line(self):
        md = html_to_markdown("<p>A</p><div></div><div></div><div></div><p>B</p>")
        assert "\n\n\n" not in md


class TestContract:
    def test_deterministic_byte_identical(self):
        html = (
            "<h1>FZE</h1><ul><li>Machining<ul><li>Swiss</li></ul></li></ul>"
            '<table><tr><th rowspan="2">M</th><th>X</th></tr><tr><th>Y</th></tr></table>'
        )
        assert html_to_markdown(html) == html_to_markdown(html)

    def test_empty_and_textless_input(self):
        assert html_to_markdown("") == ""
        assert html_to_markdown("<div><script>x</script></div>") == ""

    def test_plain_text_passes_through(self):
        assert html_to_markdown("Company information") == "Company information"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-q"]))

    def test_ordered_list_items_are_not_prefixed(self):
        items = "".join(f"<li>Brochure {i}x</li>" for i in range(6))
        md = html_to_markdown(f"<h2>The Latest Brochures</h2><ol>{items}</ol>")
        assert "The Latest Brochures: " not in md
        assert "1. Brochure 0x" in md

    def test_colon_ended_heading_never_doubles_the_colon(self):
        md = html_to_markdown(
            "<h2>Our Brochures:</h2>" + "".join(f"<div>Item {i}x</div>" for i in range(5))
        )
        assert "Our Brochures: Item 0x" in md
        assert "Brochures:: " not in md
