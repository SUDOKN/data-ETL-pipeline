"""The v2 extraction-dump rows: one per relationship RECORD.

Each row joins every later stage through the record_id, carries the phrase for
the reader, and keeps the distinctions the dump exists for: the empty-mentions
branch is not "grounding declined", a grounding pass that never ran (the OOV
pass is run config) has its key omitted rather than null, and a declination
keeps its explanation.
"""

from requests.structures import CaseInsensitiveDict

from core.models.extraction_schemas.applied_rule import AppliedRule
from core.models.extraction_schemas.grounding import RecordGroundingEntry
from core.models.extraction_schemas.iterative_tagging import (
    IterativelyTaggedPhrase,
    PhraseTrail,
)
from core.models.extraction_schemas.relationship import (
    MaskedPhraseRelationshipRecord,
    PhraseMention,
    PhraseRelationshipRecord,
)
from core.models.extraction_schemas.screening import CandidateScreeningVerdict
from core.models.skos_concept import Concept
from core.utils.extraction_dump_util import (
    build_concept_record_rows,
    build_keyword_record_rows,
)


def _concept(
    name: str,
    level: int,
    children: list[str] | None = None,
    altLabels: list[str] | None = None,
) -> Concept:
    return Concept(
        name=name,
        uri=f"http://asu.edu/semantics/SUDOKN/{name.replace(' ', '')}",
        level=level,
        altLabels=altLabels or [],
        ancestors=[],
        children=children or [],
        definition=f"The {name} concept.",
    )


def _match_label_map(*concepts: Concept) -> CaseInsensitiveDict:
    match_label_to_concept_map: CaseInsensitiveDict = CaseInsensitiveDict()
    for concept in concepts:
        for label in concept.matchLabels:
            match_label_to_concept_map[label] = concept
    return match_label_to_concept_map


def _rules(explanation: str) -> list[AppliedRule]:
    return [
        AppliedRule(rule_id="IGR-E1", outcome="satisfied", explanation=explanation)
    ]


def _verdict(passed: bool) -> CandidateScreeningVerdict:
    return CandidateScreeningVerdict(
        passed=passed, applied_rules=_rules("screening verdict")
    )


def _masked(
    phrase: str, *, mentions: bool = True, account: str = "does this work"
) -> MaskedPhraseRelationshipRecord:
    return MaskedPhraseRelationshipRecord(
        phrase=phrase,
        record=PhraseRelationshipRecord(
            mentions=(
                [PhraseMention(form=phrase, page="/", account=account)]
                if mentions
                else []
            ),
            synthesis=f"synthesis about {phrase}" if mentions else "",
        ),
    )


JOINING = _concept("Joining", level=1, children=["Metal Joining"], altLabels=["Assembly"])
MMAP = _match_label_map(JOINING)

SEARCH_ROUNDS = {
    0: {"Automotive"},
    1: {"complex welded assemblies", "powder coating services"},
}


def _rows(**overrides: object):
    kwargs: dict = dict(
        masked_flat={},
        in_vocab_flat={},
        oov_flat=None,
        screening_flat={},
        phrase_trails=[],
        search_rounds=SEARCH_ROUNDS,
        match_label_to_concept_map=MMAP,
    )
    kwargs.update(overrides)
    return {
        row["record_id"]: row for row in build_concept_record_rows(**kwargs)
    }


def test_no_mentions_record_reads_as_the_honest_not_found_branch():
    """Fork F9: the record skipped grounding AND screening code-side, so its
    stage keys are null — never 'declined'."""
    rows = _rows(
        masked_flat={"raaaaaa1": _masked("ghost phrase", mentions=False)},
    )
    row = rows["raaaaaa1"]
    assert row["status"] == "no_mentions"
    assert row["in_vocab_grounding"] is None
    assert row["screening"] is None
    assert "oov_grounding" not in row  # the pass never ran this run


def test_declined_record_keeps_its_explanation_and_reads_no_candidates():
    rows = _rows(
        masked_flat={"raaaaaa1": _masked("powder coating services")},
        in_vocab_flat={
            "raaaaaa1": RecordGroundingEntry(
                tags={}, explanation="a service, not a material"
            )
        },
    )
    row = rows["raaaaaa1"]
    assert row["status"] == "no_candidates"
    assert row["in_vocab_grounding"] == {"declined": "a service, not a material"}
    assert row["search_round"] == 1
    assert row["provenance"] == "llm_round_1"


def test_screened_out_record_shows_every_failed_verdict():
    rows = _rows(
        masked_flat={"raaaaaa1": _masked("complex welded assemblies")},
        in_vocab_flat={
            "raaaaaa1": RecordGroundingEntry(tags={"Assembly": _rules("assembly")})
        },
        screening_flat={"raaaaaa1": {"Assembly": _verdict(False)}},
    )
    row = rows["raaaaaa1"]
    assert row["status"] == "screened_out"
    assert row["screening"]["Assembly"]["passed"] is False
    assert row["in_vocab_grounding"]["tags"].keys() == {"Assembly"}


def test_grounded_record_carries_its_trail_with_origin_source_and_vocab():
    trail = PhraseTrail(
        phrase="raaaaaa1",
        lvl_by_lvl_itps={
            1: {
                IterativelyTaggedPhrase(
                    parent_group_id=None,
                    group_id="Joining",
                    direct_og_tag_w_rules={"Assembly": _rules("assembly work")},
                    iterative_og_tag_w_rules={},
                )
            }
        },
    )
    rows = _rows(
        masked_flat={"raaaaaa1": _masked("complex welded assemblies")},
        in_vocab_flat={
            "raaaaaa1": RecordGroundingEntry(tags={"Assembly": _rules("assembly")})
        },
        screening_flat={"raaaaaa1": {"Assembly": _verdict(True)}},
        phrase_trails=[trail],
    )
    row = rows["raaaaaa1"]
    assert row["status"] == "grounded"
    assert row["phrase"] == "complex welded assemblies"
    (node,) = row["lvl_by_lvl_itps"][1]
    assert node["group_id"] == "Joining"
    assert node["origin"] == "initial_grounding"
    assert node["source"] == "iterative_tagging"
    assert node["in_vocab"] is True


def test_candidates_without_verdicts_are_marked_dropped():
    """Defensive: the two-axis hold should make this unreachable; until then it
    must not masquerade as screened_out."""
    rows = _rows(
        masked_flat={"raaaaaa1": _masked("complex welded assemblies")},
        in_vocab_flat={
            "raaaaaa1": RecordGroundingEntry(tags={"Assembly": _rules("assembly")})
        },
    )
    assert rows["raaaaaa1"]["status"] == "screening_dropped"


def test_oov_pass_key_is_present_exactly_when_the_pass_ran():
    masked = {"raaaaaa1": _masked("complex welded assemblies")}
    grounded = {
        "raaaaaa1": RecordGroundingEntry(tags={"Assembly": _rules("assembly")})
    }

    without_pass = _rows(masked_flat=masked, in_vocab_flat=grounded)
    assert "oov_grounding" not in without_pass["raaaaaa1"]

    with_pass = _rows(
        masked_flat=masked,
        in_vocab_flat=grounded,
        oov_flat={
            "raaaaaa1": RecordGroundingEntry(
                tags={}, explanation="the vocabulary covers everything here"
            )
        },
    )
    assert with_pass["raaaaaa1"]["oov_grounding"] == {
        "declined": "the vocabulary covers everything here"
    }


def test_oov_candidates_count_toward_status():
    """A record the in-vocab pass declined but the OOV pass minted for is a
    candidate-bearing record, not a no_candidates one."""
    rows = _rows(
        masked_flat={"raaaaaa1": _masked("complex welded assemblies")},
        in_vocab_flat={
            "raaaaaa1": RecordGroundingEntry(tags={}, explanation="nothing in vocab")
        },
        oov_flat={
            "raaaaaa1": RecordGroundingEntry(tags={"Weldments": _rules("minted")})
        },
        screening_flat={"raaaaaa1": {"Weldments": _verdict(True)}},
    )
    assert rows["raaaaaa1"]["status"] == "grounded"


def test_keyword_rows_cover_all_statuses():
    masked = {
        "raaaaaa1": _masked("failed phrase"),
        "raaaaaa2": _masked("grounded phrase"),
        "raaaaaa3": _masked("declined phrase"),
        "raaaaaa4": _masked("ghost phrase", mentions=False),
    }
    freehand = {
        "raaaaaa1": RecordGroundingEntry(tags={"Widgets": _rules("category")}),
        "raaaaaa2": RecordGroundingEntry(tags={"Metal Stampings": _rules("category")}),
        "raaaaaa3": RecordGroundingEntry(tags={}, explanation="nothing fits"),
    }
    screening = {
        "raaaaaa1": {"Widgets": _verdict(False)},
        "raaaaaa2": {"Metal Stampings": _verdict(True)},
    }

    rows = {
        row["record_id"]: row
        for row in build_keyword_record_rows(
            masked_flat=masked,
            freehand_flat=freehand,
            screening_flat=screening,
            search_rounds={1: {entry.phrase for entry in masked.values()}},
        )
    }

    assert rows["raaaaaa1"]["status"] == "screened_out"
    assert rows["raaaaaa2"]["status"] == "grounded"
    assert rows["raaaaaa3"]["status"] == "no_candidates"
    assert rows["raaaaaa4"]["status"] == "no_mentions"
    assert rows["raaaaaa2"]["freehand_grounding"]["tags"].keys() == {
        "Metal Stampings"
    }


def test_a_relationship_repair_is_recorded_and_clean_rows_stay_bare():
    """The record stages hold exactly, so relationship is the one stage left
    where the model can answer under a drifted string; rows the model echoed
    verbatim must not grow a null field to say nothing happened."""
    masked = {
        "raaaaaa1": _masked("Schlage® hardware"),
        "raaaaaa2": _masked("brake components"),
    }
    rows = {
        row["record_id"]: row
        for row in build_keyword_record_rows(
            masked_flat=masked,
            freehand_flat={},
            screening_flat={},
            search_rounds={1: {"Schlage® hardware", "brake components"}},
            relationship_repairs={
                "Schlage® hardware": "Schlageae hardware"
            },
        )
    }

    assert (
        rows["raaaaaa1"]["phrase_as_answered"] == "Schlageae hardware"
    )
    assert "phrase_as_answered" not in rows["raaaaaa2"]


def test_own_name_hits_recorded_only_on_violation():
    """The record-only lint reads the model-authored record texts (accounts and
    synthesis); a clean row, or a builder never given the name, grows no field."""
    rows = _rows(
        masked_flat={
            "raaaaaa1": _masked(
                "complex welded assemblies",
                account="Steelcraft welds assemblies for OEMs",
            ),
            "raaaaaa2": _masked("powder coating services"),
        },
        subject_name="Steelcraft, Inc.",
    )

    assert rows["raaaaaa1"]["record_own_name_hits"] == 1
    assert "record_own_name_hits" not in rows["raaaaaa2"]


def test_own_name_lint_defaults_off_so_existing_callers_are_unchanged():
    rows = _rows(
        masked_flat={
            "raaaaaa1": _masked(
                "complex welded assemblies", account="Steelcraft everywhere"
            )
        },
    )
    assert "record_own_name_hits" not in rows["raaaaaa1"]
