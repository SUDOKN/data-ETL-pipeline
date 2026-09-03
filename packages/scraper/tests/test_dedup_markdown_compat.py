"""Dedup on markdown_v1-shaped blocks (2026-08-28 cutover guard).

Measured on all 20 golden-corpus manufacturers before the cutover
(pipeline_v3_evidence/2026-08-28_scraper_output_format_sample/): the dedup
pass strips repeated Markdown navigation exactly as it strips legacy flat
text — 26.9% (text) vs 29.3% (markdown) of corpus tokens removed. These
fixtures freeze that compatibility: identical Markdown nav/footer lines
across pages majority-vote out, structured page bodies survive, and the
50-hash page separator can never collide with a Markdown heading (max 6
hashes plus text).
"""

from scraper.utils.dedup_util import deduplicate_scraped_content

SEP = "#" * 50

NAV = "- Home\n- Capabilities\n- Industries\n- Contact\n"
FOOTER = "**FZE Manufacturing:** 123 Shop Rd\nISO 9001 certified\n(c) 2026\n"


def _block(url: str, body: str) -> str:
    return f"{SEP}\n{url}\n\n{NAV}\n{body}\n\n{FOOTER}"


def _combined(*bodies: str) -> str:
    return "".join(
        _block(f"https://fze.example/page{i}", body) for i, body in enumerate(bodies)
    )


BODY_A = "# Hydraulic Machining\n\n- Swiss turning\n- 5-axis milling\n\nSince 1974."
BODY_B = "## Industries\n\n1. Marine\n2. Aerospace\n\n| Alloy | Temper |\n|---|---|\n| 6061 | T6 |"
BODY_C = "### Contact\n\nCall us. Email us."


def test_repeated_markdown_nav_and_footer_stripped_bodies_kept():
    deduped = deduplicate_scraped_content(_combined(BODY_A, BODY_B, BODY_C))
    # boilerplate gone from every block
    assert deduped.count("- Home") == 0
    assert deduped.count("**FZE Manufacturing:** 123 Shop Rd") == 0
    # structured bodies fully intact — headings, bullets, numbering, the table
    for fragment in ("# Hydraulic Machining", "- 5-axis milling", "1. Marine",
                     "|---|", "| 6061 | T6 |", "### Contact"):
        assert fragment in deduped


def test_markdown_headings_never_split_blocks():
    """Block iteration keys on the 50-hash separator; a Markdown heading is at
    most 6 hashes plus text and must not create a phantom block boundary."""
    deduped = deduplicate_scraped_content(_combined(BODY_A, BODY_B, BODY_C))
    assert deduped.count(SEP) == 3


def test_duplicate_markdown_page_stubbed():
    deduped = deduplicate_scraped_content(_combined(BODY_A, BODY_B, BODY_A))
    assert deduped.count("# Hydraulic Machining") == 1
    assert "[duplicate — content identical to a previously scraped page]" in deduped
