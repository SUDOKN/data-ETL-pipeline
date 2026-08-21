"""Phase 1.1 of pipeline v2 (PIPELINE_V2_PLAN.md): the content-derived record id
and the masking of relationship records under it."""

import re

import pytest

from core.models.extraction_schemas.relationship import (
    PhraseMention,
    PhraseRelationshipRecord,
)
from core.utils import record_id_util
from core.utils.record_id_util import (
    RecordIdCollisionError,
    assign_record_ids,
    mask_relationship_records,
    phrases_by_record_id,
    record_id_for_phrase,
)

_ID_SHAPE = re.compile(r"^r[0-9a-z]{7}$")


def test_id_is_deterministic_and_well_formed():
    first = record_id_for_phrase("aerospace")
    assert first == record_id_for_phrase("aerospace")
    assert _ID_SHAPE.match(first)


def test_same_phrase_gets_the_same_id_in_any_collection():
    """Cross-chunk identity: derivation reads the phrase alone, so membership of
    the surrounding set can never shift an assignment — the property that keeps
    a stale replay merely stale instead of misattributed."""
    alone = assign_record_ids(["aerospace"])["aerospace"]
    among_others = assign_record_ids(["medical devices", "aerospace", "defense"])[
        "aerospace"
    ]
    assert alone == among_others == record_id_for_phrase("aerospace")


def test_case_variants_are_distinct_phrases():
    assert record_id_for_phrase("Aerospace") != record_id_for_phrase("aerospace")


def test_a_realistic_chunk_of_phrases_yields_unique_ids():
    phrases = [f"phrase number {i} with some wording" for i in range(500)]
    ids = assign_record_ids(phrases)
    assert len(set(ids.values())) == 500
    assert all(_ID_SHAPE.match(record_id) for record_id in ids.values())


def test_duplicate_input_phrases_collapse_rather_than_collide():
    ids = assign_record_ids(["aerospace", "aerospace"])
    assert list(ids) == ["aerospace"]


def test_distinct_phrases_sharing_an_id_raise_loudly(monkeypatch):
    monkeypatch.setattr(record_id_util, "record_id_for_phrase", lambda phrase: "rsameid0")
    with pytest.raises(RecordIdCollisionError) as excinfo:
        assign_record_ids(["aerospace", "medical devices"])
    assert "aerospace" in str(excinfo.value)
    assert "medical devices" in str(excinfo.value)


def _record(form: str) -> PhraseRelationshipRecord:
    return PhraseRelationshipRecord(
        mentions=[PhraseMention(form=form, page="/", account="a")],
        synthesis="s",
    )


def test_masking_keys_by_id_and_persists_the_phrase_join():
    records = {
        "aerospace": _record("Aerospace"),
        "medical devices": _record("medical devices"),
    }
    masked = mask_relationship_records(records)

    assert set(masked) == {
        record_id_for_phrase("aerospace"),
        record_id_for_phrase("medical devices"),
    }
    entry = masked[record_id_for_phrase("aerospace")]
    assert entry.phrase == "aerospace"
    # The record rides through unaltered — masking touches only the key.
    assert entry.record is records["aerospace"]

    assert phrases_by_record_id(masked) == {
        record_id_for_phrase("aerospace"): "aerospace",
        record_id_for_phrase("medical devices"): "medical devices",
    }


def test_masking_an_empty_map_is_empty():
    assert mask_relationship_records({}) == {}
