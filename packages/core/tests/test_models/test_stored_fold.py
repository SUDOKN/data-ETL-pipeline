"""The persisted aggregation fold (2026-08-27): document-absolute offsets.

The stored shape keeps positions, not passages, so its whole correctness claim
is that a stored offset resolves to the same text the run folded. These tests
prove that claim end to end rather than by inspection:

* the offsets index the DOCUMENT, not the window, which is the one thing that
  cannot be eyeballed — a base-offset bug only shows in windows after the first
  and only for mentions whose local offset happens to differ from the absolute
  one;
* the resolved bundles are EQUAL to the folded ones, ids and hashes included,
  which is what makes dropping ``form``/``snippet``/``record_id``/``mention_id``
  a derivation rather than a loss;
* the full chain works from the ORIGINAL text: replay the stored page
  exclusion, then slice — no rule versions consulted anywhere;
* every way of getting it wrong fails loudly (bad window bounds, wrong text).
"""

from datetime import datetime, timezone

import pytest

from core.models.extraction_schemas.run_provenance import (
    RunProvenance,
    StoredDroppedPage,
    StoredPageExclusion,
)
from core.models.extraction_schemas.stored_fold import (
    OffsetsOutOfRange,
    StoredFold,
    StoredMention,
    WindowBoundsMismatch,
    build_stored_fold,
    window_base_offset,
    window_texts_of,
)
from core.utils.aggregation_fold import DEFAULT_LOCATION, WindowInput, fold_document
from core.utils.record_id_util import mention_id_for_snippet, record_id_for_phrase

SEP = "#" * 50
_WHEN = datetime(2026, 8, 27, tzinfo=timezone.utc)

# Three windows, so the base offset is non-zero for two of them and a mention's
# local offset differs from its absolute one everywhere but window 0.
DOCUMENT = (
    f"{SEP}\nhttps://acme.example/materials\n\n"
    "We stock Aluminum and Brass. Aluminum ships daily.\n"
    f"{SEP}\nhttps://acme.example/process\n\n"
    "CNC machining of Aluminum housings. Brass fittings on request.\n"
    f"{SEP}\nhttps://acme.example/about\n\n"
    "Family owned. We machine Brass and Aluminum every day.\n"
)

FORMS = ["Aluminum", "Brass", "CNC machining", "titanium"]


def _sub_bounds(text: str, count: int) -> list[str]:
    """*count* contiguous windows covering *text*, split on page barriers so a
    window never starts mid-line."""
    barriers = [i for i in range(len(text)) if text.startswith(SEP, i)]
    cuts = [0] + barriers[1:count] + [len(text)]
    return [f"{cuts[i]}:{cuts[i + 1]}" for i in range(len(cuts) - 1)]


def _windows(text: str, bounds: list[str]):
    return [
        WindowInput(
            text=text[int(b.split(":")[0]) : int(b.split(":")[1])],
            sent_forms=FORMS,
            window_id=b,
        )
        for b in bounds
    ]


def _fold(text: str = DOCUMENT, *, count: int = 3):
    bounds = _sub_bounds(text, count)
    return fold_document(_windows(text, bounds)), bounds


# ---------------------------------------------------------------------------
# the offsets
# ---------------------------------------------------------------------------


def test_stored_offsets_are_document_absolute_not_window_local():
    result, bounds = _fold()
    stored = build_stored_fold(result, text_version_id="v1")

    # The premise of the whole test file: more than one window, and at least
    # one of them starting somewhere other than 0.
    assert len(stored.windows) == 3
    assert [w.base_offset for w in stored.windows] == [
        int(b.split(":")[0]) for b in bounds
    ]
    assert stored.windows[1].base_offset > 0

    for bundle in stored.bundles:
        for mention in bundle.mentions:
            base = stored.base_offset_of(mention.window_index)
            # absolute, and demonstrably not the window-local value whenever
            # the window does not start at 0
            assert mention.start >= base
            assert mention.end <= base + stored.windows[mention.window_index].text_length


def test_every_stored_span_slices_back_to_the_text_it_was_written_from():
    result, _ = _fold()
    stored = build_stored_fold(result, text_version_id="v1")

    original = {
        (m.window_index, m.start, m.end): m
        for bundle in result.bundles
        for m in bundle.mentions
    }
    assert original, "the fixture must produce mentions or this proves nothing"

    seen = 0
    for bundle in stored.bundles:
        for mention in bundle.mentions:
            base = stored.base_offset_of(mention.window_index)
            folded = original[
                (mention.window_index, mention.start - base, mention.end - base)
            ]
            assert mention.form_in(DOCUMENT) == folded.form
            assert mention.snippet_in(DOCUMENT) == folded.snippet
            assert mention.record_id_in(DOCUMENT) == folded.record_id
            assert mention.mention_id_in(DOCUMENT) == folded.mention_id
            assert mention.is_discovered_casing_in(DOCUMENT) == folded.is_discovered_casing
            seen += 1
    assert seen == len(original)


def result_mentions(result):
    return [m for b in result.bundles for m in b.mentions]


def test_resolve_rebuilds_the_folded_bundles_exactly():
    """The round trip that licenses storing offsets instead of passages."""
    result, _ = _fold()
    stored = build_stored_fold(result, text_version_id="v1")
    assert stored.resolve(DOCUMENT) == result.bundles


HEADED = (
    f"{SEP}\nhttps://acme.example/materials\n\n"
    "## Materials\n\nWe stock Aluminum and Brass.\n\n"
    "| Alloy | Form |\n|---|---|\n| Aluminum | sheet |\n| Brass | rod |\n"
    f"{SEP}\nhttps://acme.example/about\n\n"
    "Family owned. We machine Aluminum every day.\n"
)


def test_the_folds_own_location_rides_the_stored_mention_and_resolves_back():
    """2026-09-05: ``location`` is the fold's own — the heading or table header
    row above the snippet, derived in code (aggregation_fold docstring D) —
    stored under source "code"; a mention with none stores the default under
    source "none". Resolve is a round trip either way, location included."""
    result, _ = _fold(HEADED, count=2)
    stored = build_stored_fold(result, text_version_id="v1")
    assert stored.resolve(HEADED) == result.bundles

    by_snippet = {
        m.snippet_in(HEADED): (m.location, m.location_source)
        for b in stored.bundles
        for m in b.mentions
    }
    assert by_snippet["We stock Aluminum and Brass."] == ("## Materials", "code")
    assert by_snippet["| Aluminum | sheet |"] == ("| Alloy | Form |", "code")
    assert by_snippet["| Brass | rod |"] == ("| Alloy | Form |", "code")
    # the about page has no heading: the default, and the fold's None survives resolve
    assert by_snippet["We machine Aluminum every day."] == (DEFAULT_LOCATION, "none")
    assert all(
        m.location is None
        for b in stored.resolve(HEADED)
        for m in b.mentions
        if m.snippet == "We machine Aluminum every day."
    )


def test_a_single_window_fold_still_stores_absolute_offsets():
    """The degenerate case that hides base bugs: one window, base 0."""
    result, _ = _fold(count=1)
    stored = build_stored_fold(result, text_version_id="v1")
    assert [w.base_offset for w in stored.windows] == [0]
    assert stored.resolve(DOCUMENT) == result.bundles


def test_window_texts_of_slices_each_window_back():
    result, bounds = _fold()
    stored = build_stored_fold(result, text_version_id="v1")
    expected = [DOCUMENT[int(b.split(":")[0]) : int(b.split(":")[1])] for b in bounds]
    assert list(window_texts_of(stored, DOCUMENT)) == expected


def test_stored_fold_survives_a_json_round_trip():
    result, _ = _fold()
    stored = build_stored_fold(result, text_version_id="v1")
    reloaded = StoredFold.model_validate_json(stored.model_dump_json())
    assert reloaded == stored
    assert reloaded.resolve(DOCUMENT) == result.bundles


# ---------------------------------------------------------------------------
# the ways it can be wrong, each loud
# ---------------------------------------------------------------------------


def test_a_window_id_that_is_not_bounds_refuses_to_produce_a_base():
    result, _ = _fold()
    result.windows[1].window_id = "the second one"
    with pytest.raises(WindowBoundsMismatch, match="not 'start:end' bounds"):
        build_stored_fold(result, text_version_id="v1")


def test_a_window_id_naming_the_wrong_span_refuses_to_produce_a_base():
    """The check that makes reading the base out of a 'free label' safe."""
    result, bounds = _fold()
    start, end = bounds[1].split(":")
    result.windows[1].window_id = f"{start}:{int(end) + 5}"
    with pytest.raises(WindowBoundsMismatch, match="names a span of"):
        build_stored_fold(result, text_version_id="v1")


def test_a_window_without_an_id_refuses_to_produce_a_base():
    result, _ = _fold()
    result.windows[0].window_id = None
    with pytest.raises(WindowBoundsMismatch, match="no window_id"):
        build_stored_fold(result, text_version_id="v1")


def test_window_base_offset_reads_the_bounds_when_they_match():
    result, bounds = _fold()
    for window, bound in zip(result.windows, bounds):
        assert window_base_offset(window) == int(bound.split(":")[0])


def test_resolving_against_a_shorter_text_raises_rather_than_truncating():
    """Python slicing past the end returns a short string silently; the point
    of the explicit range check is that a wrong text version cannot come back
    as a plausible-looking snippet."""
    result, _ = _fold()
    stored = build_stored_fold(result, text_version_id="v1")
    with pytest.raises(OffsetsOutOfRange):
        stored.resolve(DOCUMENT[:40])


def test_a_snippet_that_does_not_contain_its_occurrence_is_rejected():
    with pytest.raises(ValueError, match="does not contain the occurrence"):
        StoredMention(
            window_index=0,
            start=10,
            end=20,
            snippet_start=12,
            snippet_end=18,
            page=None,
            sent_form="x",
            location="l",
            location_source="none",
        )


def test_an_empty_or_inverted_occurrence_is_rejected():
    with pytest.raises(ValueError, match="empty or inverted"):
        StoredMention(
            window_index=0,
            start=20,
            end=20,
            snippet_start=0,
            snippet_end=40,
            page=None,
            sent_form="x",
            location="l",
            location_source="none",
        )


# ---------------------------------------------------------------------------
# the full chain: original S3 text -> replay exclusion -> slice
# ---------------------------------------------------------------------------


EXCLUDED = (
    f"{SEP}\nhttps://acme.example/privacy\n\n"
    "We use cookies. This policy explains Brass tacks of data handling.\n"
)
ORIGINAL = DOCUMENT[: len(DOCUMENT) // 2] + EXCLUDED + DOCUMENT[len(DOCUMENT) // 2 :]


def _exclusion_of(original: str, excluded: str) -> StoredPageExclusion:
    start = original.index(excluded)
    return StoredPageExclusion(
        version="1",
        chars_before=len(original),
        chars_removed=len(excluded),
        chars_after=len(original) - len(excluded),
        pages=[
            StoredDroppedPage(
                url="https://acme.example/privacy",
                start=start,
                end=start + len(excluded),
            )
        ],
    )


def test_replaying_the_stored_exclusion_reconstructs_what_the_run_chunked():
    exclusion = _exclusion_of(ORIGINAL, EXCLUDED)
    assert exclusion.apply_to(ORIGINAL) == DOCUMENT


def test_offsets_resolve_from_the_original_text_through_the_stored_exclusion():
    """The chain a reader actually walks, with no rule version consulted: the
    S3 object, the stored spans, then a slice."""
    result, _ = _fold()
    stored = build_stored_fold(result, text_version_id="v1")
    provenance = RunProvenance(
        run_timestamp=_WHEN,
        scraped_text_version_id="v1",
        scraped_text_num_tokens=1,
        scraped_text_last_modified_on=_WHEN,
        page_exclusion=_exclusion_of(ORIGINAL, EXCLUDED),
    )
    assert stored.resolve(provenance.post_exclusion_text(ORIGINAL)) == result.bundles


def test_replaying_an_exclusion_over_the_wrong_text_raises():
    exclusion = _exclusion_of(ORIGINAL, EXCLUDED)
    with pytest.raises(ValueError, match="was recorded over"):
        exclusion.apply_to(ORIGINAL + "extra")


def test_multiple_dropped_pages_are_removed_back_to_front():
    """Deleting front-first would shift every later span; the ordering is the
    whole implementation, so it gets its own case."""
    first, second = "<<AAAA>>", "<<BBBB>>"
    original = f"one{first}two{second}three"
    exclusion = StoredPageExclusion(
        version="1",
        chars_before=len(original),
        chars_removed=len(first) + len(second),
        chars_after=len(original) - len(first) - len(second),
        pages=[
            StoredDroppedPage(
                url="a",
                start=original.index(first),
                end=original.index(first) + len(first),
            ),
            StoredDroppedPage(
                url="b",
                start=original.index(second),
                end=original.index(second) + len(second),
            ),
        ],
    )
    assert exclusion.apply_to(original) == "onetwothree"


def test_no_exclusion_leaves_the_text_alone():
    provenance = RunProvenance(
        run_timestamp=_WHEN,
        scraped_text_version_id="v1",
        scraped_text_num_tokens=1,
        scraped_text_last_modified_on=_WHEN,
    )
    assert provenance.post_exclusion_text(ORIGINAL) == ORIGINAL


# ---------------------------------------------------------------------------
# what the stored shape keeps and drops
# ---------------------------------------------------------------------------


def test_the_window_report_survives_but_the_window_text_does_not():
    result, _ = _fold()
    stored = build_stored_fold(result, text_version_id="v1")
    dumped = stored.model_dump_json()

    window = stored.windows[0]
    assert window.sent_forms == FORMS
    assert "titanium" in window.zero_hit_forms  # sent, never found
    assert window.text_length == len(result.windows[0].collection.text)
    # The passages are the bulk this shape exists to not store.
    assert "We stock Aluminum" not in dumped


def test_the_grouping_decision_is_stored_verbatim():
    """Keys, member forms and collapses are the part no recomputation under
    changed rules can be trusted to reproduce."""
    result, _ = _fold()
    stored = build_stored_fold(result, text_version_id="v1")
    by_id = {b.group_id: b for b in stored.bundles}
    for bundle in result.bundles:
        assert by_id[bundle.group_id].key == bundle.key
        assert by_id[bundle.group_id].forms == list(bundle.forms)
        assert by_id[bundle.group_id].collapsed_into == list(bundle.collapsed_into)


def test_empty_bundles_are_kept():
    result, _ = _fold()
    stored = build_stored_fold(result, text_version_id="v1")
    assert any(b.is_empty for b in stored.bundles), "titanium was sent and never found"
    assert stored.mention_count == len(result_mentions(result))


def test_the_rule_identity_rides_along_so_the_block_reads_alone():
    result, _ = _fold()
    stored = build_stored_fold(result, text_version_id="v1")
    assert stored.normalizer_version == result.normalizer_version
    assert stored.verb_fold == result.verb_fold
    assert stored.snippet_radius == result.snippet_radius
    assert stored.collapse_compounds == result.collapse_compounds
    assert stored.text_version_id == "v1"
