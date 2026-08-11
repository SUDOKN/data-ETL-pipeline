"""What the four summary-carrying stages put in front of the model.

This was a round-trip suite until 2026-08-11: a matching reader pulled the map back
out of the request context at parse time, so that quoted evidence could be checked
against the summary byte-for-byte. With the evidence field gone the reader went with
it, and what is left to pin is the render side — the block is now purely an input
payload, and these tests say it arrives as well-formed, unmangled JSON.
"""

import json

from core.services.phrase_summaries_block import render_phrase_summaries_block

SUMMARIES = {
    "Class A appearance trim": "Anchor stamps Class A appearance trim for OEMs.",
    "welded assemblies": "Anchor produces complex welded assemblies.",
}

_OPEN = "<<<PHRASE_SUMMARIES"
_CLOSE = "PHRASE_SUMMARIES>>>"


def _payload_of(block: str) -> str:
    assert block.startswith(_OPEN)
    assert block.endswith(_CLOSE)
    return block[len(_OPEN) : -len(_CLOSE)].strip()


def test_the_block_is_fenced_and_holds_the_map_as_json():
    assert json.loads(_payload_of(render_phrase_summaries_block(SUMMARIES))) == SUMMARIES


def test_an_empty_map_still_renders_a_block():
    """What the dummy requests carry. Every stage renders the block unconditionally,
    so its presence never has to be interpreted."""
    assert json.loads(_payload_of(render_phrase_summaries_block({}))) == {}


def test_one_phrase_per_line():
    """The separator is chosen so a long map stays readable to the model rather than
    collapsing onto a single line."""
    payload = _payload_of(render_phrase_summaries_block(SUMMARIES))

    assert len(payload.splitlines()) == len(SUMMARIES)


def test_summaries_containing_the_markers_stay_inside_the_json_string():
    """A marker in the scraped copy must not look like the end of the block."""
    summaries = {
        "trim": f"The page literally says {_OPEN} and {_CLOSE}."
    }

    assert json.loads(_payload_of(render_phrase_summaries_block(summaries))) == summaries


def test_json_punctuation_in_a_summary_is_escaped():
    summaries = {'quotes " and {braces}': 'he said "we stamp {parts}", verbatim'}

    assert json.loads(_payload_of(render_phrase_summaries_block(summaries))) == summaries


def test_newlines_and_unicode_survive_verbatim():
    """The model is asked to quote this text back in its explanations, so a
    normalising render would put words in front of it that the source never had."""
    summaries = {"N.V.H. parts": "line one\nline two — “curly quotes”, café"}

    assert json.loads(_payload_of(render_phrase_summaries_block(summaries))) == summaries
