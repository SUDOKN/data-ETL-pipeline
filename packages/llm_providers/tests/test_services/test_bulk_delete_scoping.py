"""Which requests a scoped bulk delete actually selects.

Custom IDs are laid out ``{subject}>{field}>{stage_token}>...``, so a delete can
be narrowed to a field, to a set of stages, or to both — but only if the filter
is built to respect that layout. The trap worth guarding: two stage tokens where
one is a prefix of the other (``llm_phrase_relationship`` and
``llm_phrase_relationship_screening``), which a range or regex that forgets the
trailing ``>`` would happily delete together.

The filter builder is pure (literal field names, no Beanie expression fields),
so every case below is decided here rather than against a live collection.
"""

from __future__ import annotations

import re
from typing import Any, Optional

from llm_providers.services.gpt_batch_request.gpt_batch_request_writes import (
    build_scoped_delete_filter,
)

_SUBJECT = "alecmfg.com"

_SEARCH = f"{_SUBJECT}>material_caps>llm_search>chunk>0:24294>sub>0:8098>gpt-4.1"
_RELATIONSHIP = (
    f"{_SUBJECT}>material_caps>llm_phrase_relationship>group>0>chunk>0:24294>gpt-4.1"
)
_SCREENING = (
    f"{_SUBJECT}>material_caps>llm_phrase_relationship_screening"
    f">group>0>chunk>0:24294>gpt-4.1"
)
_OTHER_FIELD_SEARCH = (
    f"{_SUBJECT}>industries>llm_search>chunk>0:24294>sub>0:8098>gpt-4.1"
)
_OTHER_FIELD_SCREENING = (
    f"{_SUBJECT}>industries>llm_phrase_relationship_screening"
    f">group>0>chunk>0:24294>gpt-4.1"
)


def _selects(query_filter: dict[str, Any], custom_id: str) -> bool:
    """Whether *custom_id* satisfies the custom_id clause of *query_filter*.

    Mirrors the two clause shapes the builder emits (a range, or a rooted
    regex), so a test can ask about a concrete ID rather than about a dict.
    """

    def _matches(clause: dict[str, str]) -> bool:
        if "$regex" in clause:
            return re.search(clause["$regex"], custom_id) is not None
        return clause["$gte"] <= custom_id < clause["$lt"]

    if "$or" in query_filter:
        return any(_matches(branch["request.custom_id"]) for branch in query_filter["$or"])
    clause: Optional[dict[str, str]] = query_filter.get("request.custom_id")
    if clause is None:
        return True  # no custom_id narrowing: the whole subject is in scope
    return _matches(clause)


def test_no_narrowing_takes_the_whole_subject():
    query_filter, index_bounds_the_scan = build_scoped_delete_filter(
        subject_unique_id=_SUBJECT, field_name=None
    )

    assert query_filter == {"subject_unique_id": _SUBJECT}
    # Nothing bounds the scan, so hinting the custom_id index would only force a
    # full index walk over a filter that does not use it.
    assert index_bounds_the_scan is False


def test_field_alone_takes_every_stage_of_that_field():
    query_filter, index_bounds_the_scan = build_scoped_delete_filter(
        subject_unique_id=_SUBJECT, field_name="material_caps"
    )

    assert index_bounds_the_scan is True
    assert _selects(query_filter, _SEARCH)
    assert _selects(query_filter, _RELATIONSHIP)
    assert _selects(query_filter, _SCREENING)
    assert not _selects(query_filter, _OTHER_FIELD_SEARCH)


def test_stage_alone_takes_that_stage_across_fields():
    query_filter, _ = build_scoped_delete_filter(
        subject_unique_id=_SUBJECT,
        field_name=None,
        stage_request_id_tokens=["llm_phrase_relationship_screening"],
    )

    assert _selects(query_filter, _SCREENING)
    assert _selects(query_filter, _OTHER_FIELD_SCREENING)
    assert not _selects(query_filter, _SEARCH)
    assert not _selects(query_filter, _RELATIONSHIP)


def test_field_and_stage_take_only_that_pair():
    query_filter, _ = build_scoped_delete_filter(
        subject_unique_id=_SUBJECT,
        field_name="material_caps",
        stage_request_id_tokens=["llm_search"],
    )

    assert _selects(query_filter, _SEARCH)
    assert not _selects(query_filter, _RELATIONSHIP)
    assert not _selects(query_filter, _OTHER_FIELD_SEARCH)


def test_relationship_does_not_drag_screening_along():
    """The prefix trap, on the range branch."""
    query_filter, _ = build_scoped_delete_filter(
        subject_unique_id=_SUBJECT,
        field_name="material_caps",
        stage_request_id_tokens=["llm_phrase_relationship"],
    )

    assert _selects(query_filter, _RELATIONSHIP)
    assert not _selects(query_filter, _SCREENING)


def test_stage_alone_does_not_drag_screening_along():
    """The same trap on the regex branch, which has no range bound to save it."""
    query_filter, _ = build_scoped_delete_filter(
        subject_unique_id=_SUBJECT,
        field_name=None,
        stage_request_id_tokens=["llm_phrase_relationship"],
    )

    assert _selects(query_filter, _RELATIONSHIP)
    assert not _selects(query_filter, _SCREENING)


def test_several_stages_are_selected_together():
    query_filter, _ = build_scoped_delete_filter(
        subject_unique_id=_SUBJECT,
        field_name="material_caps",
        stage_request_id_tokens=[
            "llm_phrase_relationship",
            "llm_phrase_relationship_screening",
        ],
    )

    assert _selects(query_filter, _RELATIONSHIP)
    assert _selects(query_filter, _SCREENING)
    assert not _selects(query_filter, _SEARCH)


def test_a_subject_whose_name_carries_regex_metacharacters_stays_literal():
    """etld1s are full of dots; an unescaped one would match a sibling subject."""
    query_filter, _ = build_scoped_delete_filter(
        subject_unique_id="alec.mfg.com",
        field_name=None,
        stage_request_id_tokens=["llm_search"],
    )

    assert _selects(
        query_filter,
        "alec.mfg.com>material_caps>llm_search>chunk>0:24294>sub>0:8098>gpt-4.1",
    )
    assert not _selects(
        query_filter,
        "alecXmfg.com>material_caps>llm_search>chunk>0:24294>sub>0:8098>gpt-4.1",
    )


def test_the_regex_is_rooted_at_the_subject():
    """A subject appearing mid-ID must not be enough to select the request."""
    query_filter, _ = build_scoped_delete_filter(
        subject_unique_id=_SUBJECT,
        field_name=None,
        stage_request_id_tokens=["llm_search"],
    )

    assert not _selects(
        query_filter,
        f"other-mfg.com>{_SUBJECT}>material_caps>llm_search>chunk>0:24294>sub>0:8098",
    )
