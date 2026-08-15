"""The rewritten phrase-trail dump rows (2026-08-12).

The old dump emitted one row per GROUNDED phrase: screening.passed was a
constant true, out-of-vocab proposals had no row at all, and an unmatched
phrase was filed as search round 0 — the bucket reserved for brute-search
survivors. These tests pin the row-per-screened-phrase shape: statuses,
level-0 placement for positionless tags, the defensive row for an in-vocab
tag the trail failed to carry, and honest provenance.
"""

from requests.structures import CaseInsensitiveDict

from core.models.extraction_schemas.applied_rule import AppliedRule
from core.models.extraction_schemas.grounding import NONE_OF_THE_ABOVE_TAG
from core.models.extraction_schemas.iterative_tagging import (
    IterativelyTaggedPhrase,
    PhraseTrail,
)
from core.models.extraction_schemas.screening import ScreeningVerdict
from core.models.skos_concept import Concept
from core.utils.extraction_dump_util import (
    build_concept_phrase_rows,
    build_keyword_phrase_rows,
    merge_stage_repairs,
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
        AppliedRule(rule_id="IGR-Q1", outcome="satisfied", explanation=explanation)
    ]


def _verdict(passed: bool) -> ScreeningVerdict:
    return ScreeningVerdict(
        passed=passed,
        identified_entity="something" if passed else None,
        applied_rules=_rules("screening verdict"),
    )


JOINING = _concept("Joining", level=1, children=["Metal Joining"], altLabels=["Assembly"])
AUTOMOTIVE = _concept("Automotive", level=1, children=["Passenger Vehicles"])
MMAP = _match_label_map(JOINING, AUTOMOTIVE)

SEARCH_ROUNDS = {
    0: {"Automotive"},
    1: {"complex welded assemblies", "powder coating services"},
}


def _rows(**overrides):
    kwargs = dict(
        screening_flat={},
        relationship_flat={},
        initial_grounding_flat={},
        phrase_trails=[],
        search_rounds=SEARCH_ROUNDS,
        match_label_to_concept_map=MMAP,
    )
    kwargs.update(overrides)
    return {
        row["phrase"]: row for row in build_concept_phrase_rows(**kwargs)
    }


def test_screened_out_phrase_gets_a_row_without_levels():
    """The 57% the old dump hid: screening.passed false is now representable."""
    rows = _rows(screening_flat={"decorative widgets": _verdict(False)})

    row = rows["decorative widgets"]
    assert row["status"] == "screened_out"
    assert row["screening"]["passed"] is False
    assert "lvl_by_lvl_itps" not in row


def test_trail_phrase_is_grounded_with_origin_source_and_vocab():
    trail = PhraseTrail(
        phrase="complex welded assemblies",
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
        screening_flat={"complex welded assemblies": _verdict(True)},
        initial_grounding_flat={
            "complex welded assemblies": {"Assembly": _rules("assembly work")}
        },
        phrase_trails=[trail],
    )

    row = rows["complex welded assemblies"]
    assert row["status"] == "grounded"
    assert row["search_round"] == 1
    assert row["provenance"] == "llm_round_1"
    (node,) = row["lvl_by_lvl_itps"][1]
    assert node["group_id"] == "Joining"
    assert node["origin"] == "initial_grounding"
    assert node["source"] == "iterative_tagging"
    assert node["in_vocab"] is True
    # The initial tag resolves to a concept that HAS a trail node, so no
    # duplicate settled node is synthesized anywhere.
    assert set(row["lvl_by_lvl_itps"].keys()) == {1}


def test_initial_sentinel_lands_at_level_zero_as_no_match():
    """The Iron Phosphate / Powder Coating case: an initial-grounding sentinel
    has no tree position. It used to fan out into every sentinel node at every
    level; now it is one level-0 record and the row reads no_match."""
    rows = _rows(
        screening_flat={"powder coating services": _verdict(True)},
        initial_grounding_flat={
            "powder coating services": {
                NONE_OF_THE_ABOVE_TAG: _rules("a service, not a material")
            }
        },
    )

    row = rows["powder coating services"]
    assert row["status"] == "no_match"
    (node,) = row["lvl_by_lvl_itps"][0]
    assert node["group_id"] == NONE_OF_THE_ABOVE_TAG
    assert node["parent_group_id"] is None
    assert node["source"] == "initial_grounding_sentinel"
    assert node["in_vocab"] is False


def test_in_vocab_tag_without_a_trail_node_still_gets_a_row():
    """Defensive branch: every recognized tag descends now (the exact-label
    settle policy is gone), so an in-vocab tag with no real trail node marks a
    walk gap — but the tag must still reach the dump at its concept's level,
    as a grounded row, rather than vanish."""
    rows = _rows(
        screening_flat={"Automotive": _verdict(True)},
        initial_grounding_flat={"Automotive": {"Automotive": _rules("exact label")}},
    )

    row = rows["Automotive"]
    assert row["status"] == "grounded"
    assert row["search_round"] == 0
    assert row["provenance"] == "brute"
    (node,) = row["lvl_by_lvl_itps"][AUTOMOTIVE.level]
    assert node["group_id"] == "Automotive"
    assert node["source"] == "initial_grounding_unwalked"
    assert node["in_vocab"] is True


def test_oov_proposal_lands_at_level_zero_as_grounded():
    """The Welding case: an out-of-vocab proposal reaches results.out_of_vocab,
    so its phrase attribution belongs in the dump."""
    rows = _rows(
        screening_flat={"complex welded assemblies": _verdict(True)},
        initial_grounding_flat={
            "complex welded assemblies": {
                "Welding": _rules("proposed as an option of its own"),
                "Assembly": _rules("assembly work"),
            }
        },
    )

    row = rows["complex welded assemblies"]
    assert row["status"] == "grounded"
    (oov_node,) = row["lvl_by_lvl_itps"][0]
    assert oov_node["group_id"] == "Welding"
    assert oov_node["source"] == "initial_grounding_oov"
    assert oov_node["in_vocab"] is False
    # The alt-label tag resolves to Joining; with no trail node built in this
    # fixture it lands on the defensive branch at Joining's level.
    (direct_node,) = row["lvl_by_lvl_itps"][JOINING.level]
    assert direct_node["group_id"] == "Joining"
    assert direct_node["source"] == "initial_grounding_unwalked"


def test_drifted_grounding_phrase_is_kept_and_noted():
    """The 'Takeover / Off-Load Tool Experience' case: a grounding response
    introduced a phrase screening never saw. It must stay visible — with a note
    and honest provenance — not read as a brute-search hit."""
    rows = _rows(
        initial_grounding_flat={
            "Takeover / Off-Load Tool Experience": {
                NONE_OF_THE_ABOVE_TAG: _rules("invented by the model")
            }
        },
    )

    row = rows["Takeover / Off-Load Tool Experience"]
    assert row["note"] == "phrase_not_in_screening"
    assert row["screening"] is None
    assert row["search_round"] is None
    assert row["provenance"] == "unmatched"


def test_passed_phrase_missing_from_grounding_is_marked_dropped():
    """The 'tool experience' case: passed screening, never grounded. Defensive
    until response-vs-input validation makes it unreachable."""
    rows = _rows(screening_flat={"tool experience": _verdict(True)})

    row = rows["tool experience"]
    assert row["status"] == "grounding_dropped"
    assert "lvl_by_lvl_itps" not in row


def test_keyword_rows_cover_all_statuses():
    screening = {
        "failed phrase": _verdict(False),
        "grounded phrase": _verdict(True),
        "declined phrase": _verdict(True),
        "dropped phrase": _verdict(True),
    }
    freehand = {
        "grounded phrase": {"Metal Stampings": _rules("a product category")},
        "declined phrase": {"Cannot categorize": _rules("nothing fits")},
    }

    rows = {
        row["phrase"]: row
        for row in build_keyword_phrase_rows(
            screening_flat=screening,
            relationship_flat={},
            freehand_grounding_flat=freehand,
            search_rounds={1: set(screening)},
        )
    }

    assert rows["failed phrase"]["status"] == "screened_out"
    assert rows["grounded phrase"]["status"] == "grounded"
    assert rows["declined phrase"]["status"] == "no_match"
    assert rows["dropped phrase"]["status"] == "grounding_dropped"
    assert rows["grounded phrase"]["freehand_grounding"].keys() == {"Metal Stampings"}


def test_a_repaired_phrase_names_the_stage_and_clean_rows_stay_bare():
    """The trail joins every stage on the phrase string, so a row keyed by the
    sent phrase after the model answered under a different one is joinable but
    not auditable. `phrase_as_answered` carries the substitution, keyed by the
    stage that made it; rows the model echoed verbatim must not grow a null
    field to say nothing happened."""
    screening = {
        "Schlage\u00ae hardware": _verdict(True),
        "brake components": _verdict(True),
    }
    freehand = {
        "Schlage\u00ae hardware": {"Door Hardware": _rules("a product category")},
        "brake components": {"Brakes": _rules("a product category")},
    }

    rows = {
        row["phrase"]: row
        for row in build_keyword_phrase_rows(
            screening_flat=screening,
            relationship_flat={},
            freehand_grounding_flat=freehand,
            search_rounds={1: set(screening)},
            repairs_flat=merge_stage_repairs(
                {
                    "screening": {
                        "Schlage\u00ae hardware": "Schlage\u0000ae hardware"
                    },
                    "freehand_grounding": {},
                }
            ),
        )
    }

    assert rows["Schlage\u00ae hardware"]["phrase_as_answered"] == {
        "screening": "Schlage\u0000ae hardware"
    }
    assert "phrase_as_answered" not in rows["brake components"]


def test_a_phrase_mis_echoed_at_two_stages_keeps_both():
    """The reason the field is keyed by stage. A flat map would keep whichever
    stage was parsed last and drop the other silently — the same class of loss
    the field exists to end."""
    phrase = "Paladin\u2122 PW Series"
    merged = merge_stage_repairs(
        {
            "screening": {phrase: "Paladin\u00122 PW Series"},
            "freehand_grounding": {phrase: "Paladin PW Series"},
        }
    )

    (row,) = build_keyword_phrase_rows(
        screening_flat={phrase: _verdict(True)},
        relationship_flat={},
        freehand_grounding_flat={phrase: {"Doors": _rules("a product category")}},
        search_rounds={1: {phrase}},
        repairs_flat=merged,
    )

    assert row["phrase_as_answered"] == {
        "screening": "Paladin\u00122 PW Series",
        "freehand_grounding": "Paladin PW Series",
    }


def test_repairs_default_to_absent_so_existing_callers_are_unchanged():
    screening = {"brake components": _verdict(True)}
    freehand = {"brake components": {"Brakes": _rules("a product category")}}

    (row,) = build_keyword_phrase_rows(
        screening_flat=screening,
        relationship_flat={},
        freehand_grounding_flat=freehand,
        search_rounds={1: set(screening)},
    )

    assert "phrase_as_answered" not in row


def test_own_name_hits_recorded_only_on_violation():
    """The record-only lint: a leaked subject name is counted on the row, and a
    clean row (or a builder never given the name) grows no field at all."""
    leaked = "complex welded assemblies"
    clean = "powder coating services"
    rows = _rows(
        screening_flat={leaked: _verdict(False), clean: _verdict(False)},
        relationship_flat={
            leaked: 'Mention: "Steelcraft welds assemblies." Relationship: welds them.',
            clean: "Mention: recurring list entry. Relationship: offered service.",
        },
        subject_name="Steelcraft, Inc.",
    )

    assert rows[leaked]["relationship_own_name_hits"] == 1
    assert "relationship_own_name_hits" not in rows[clean]


def test_own_name_lint_defaults_off_so_existing_callers_are_unchanged():
    phrase = "complex welded assemblies"
    rows = _rows(
        screening_flat={phrase: _verdict(False)},
        relationship_flat={phrase: "Steelcraft everywhere, unlinted."},
    )
    assert "relationship_own_name_hits" not in rows[phrase]
