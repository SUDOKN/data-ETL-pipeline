"""Tests for the diff-set pass (export_diffset_packets + merge_diffset).

Matching code is where this corpus's silent wrongness lives — three prior
matcher defects were each found by a test after the metric looked plausible.
These pin the credit resolution the whole difference-set rests on."""

import sys
from pathlib import Path

_CHECKS = Path(__file__).resolve().parents[1] / "checks"
sys.path.insert(0, str(_CHECKS))

import export_diffset_packets as diffset  # type: ignore[import-not-found]  # noqa: E402
import merge_diffset  # type: ignore[import-not-found]  # noqa: E402
from expectations import Expectations  # type: ignore[import-not-found]  # noqa: E402


def _exp(entries):
    return Expectations(
        subject="s.com", field="material_caps", path="x", eval_set_version=1,
        entries=entries,
    )


def _entry(eid, name, status, forms=None, quotes=None):
    return {
        "id": eid, "name": name, "status": status,
        "acceptable_forms": forms or [name],
        "evidence": [{"quote": q} for q in (quotes or [])],
    }


# ------------------------------------------------------------- credit rules

def test_confirmed_credit_beats_candidate():
    exp = _exp([
        _entry("c1", "Weld", "candidate"),
        _entry("e1", "Welding", "confirmed"),
    ])
    accs = diffset._acceptables(exp)
    best = diffset._best_credit(accs, "welding services")
    assert best is not None and best.entry_id == "e1" and best.status == "confirmed"


def test_disputed_credits_only_when_nothing_stronger():
    exp = _exp([_entry("d1", "Design", "disputed")])
    best = diffset._best_credit(diffset._acceptables(exp), "design engineering")
    assert best is not None and best.status == "disputed"


def test_retired_never_credits():
    exp = _exp([_entry("r1", "Lead", "retired")])
    assert diffset._acceptables(exp) == []
    assert diffset._best_credit([], "Lead") is None


def test_short_form_guard_holds_in_credit_resolution():
    exp = _exp([_entry("e1", "TIG", "confirmed")])
    accs = diffset._acceptables(exp)
    assert diffset._best_credit(accs, "tight tolerances") is None  # the trap
    assert diffset._best_credit(accs, "TIG welding") is not None


def test_unmatched_form_gets_no_credit():
    exp = _exp([_entry("e1", "Aluminum", "confirmed")])
    assert diffset._best_credit(diffset._acceptables(exp), "Brass") is None


# ------------------------------------------------------------- part splitting

def test_split_parts_are_self_contained(monkeypatch):
    monkeypatch.setattr(diffset, "MAX_ITEMS_PER_PART", 2)
    unit = diffset.UnitDiffset(subject="s.com", field="material_caps")
    unit.unmatched = {"0:10": ["a", "b"], "10:20": ["c"], "20:30": ["d"]}
    unit.miss_validations = [
        {"entry_id": "m1", "entry_name": "M1", "acceptable_forms": ["M1"],
         "quotes": ["q"], "scope_windows": ["20:30"]},
    ]
    unit.candidate_checks = [
        {"entry_id": "c1", "entry_name": "C1", "forms": ["c"], "windows": ["10:20"],
         "quotes": ["q"], "scope_windows": ["10:20"]},
    ]
    unit.credit_samples = [
        {"form": "a", "window": "0:10", "entry_id": "e1", "entry_name": "E1"},
    ]
    order = ["0:10", "10:20", "20:30"]
    texts = {w: "text " * 5 for w in order}
    parts = diffset._split_parts(unit, order, texts)
    assert len(parts) > 1
    seen_b = seen_c = seen_d = 0
    for part in parts:
        wset = set(part["windows"])
        for c in part["candidate_checks"]:
            seen_b += 1
            assert c["scope_windows"][0] in wset
        for m in part["miss_validations"]:
            seen_c += 1
            assert m["scope_windows"][0] in wset
        for s in part["credit_samples"]:
            seen_d += 1
            assert s["window"] in wset
    assert (seen_b, seen_c, seen_d) == (1, 1, 1)  # nothing dropped, nothing doubled


# ------------------------------------------------------------- gap quotes

def test_derived_gap_quote_is_verbatim_slice():
    text = "We machine parts.  Our shop offers custom anodizing for aluminum housings daily."
    quote = merge_diffset.derive_gap_quote("custom anodizing", text)
    assert quote is not None
    assert quote in text  # verbatim, straight from the window


def test_derived_gap_quote_missing_form_returns_none():
    assert merge_diffset.derive_gap_quote("titanium", "no such material here") is None


def test_derived_gap_quote_respects_cap():
    text = ("x" * 300) + " milling " + ("y" * 300)
    quote = merge_diffset.derive_gap_quote("milling", text)
    assert quote is not None and len(quote) <= merge_diffset.QUOTE_CAP
    assert quote in text and "milling" in quote


# ------------------------------------------------------------- merge math

def test_precision_estimate_arithmetic():
    unit_index = {
        "forms_total": 10,
        "credited": {"confirmed": 5, "candidate": 2, "disputed": 1},
        "unmatched": 2,
        "candidate_occurrences": {"c1": 2},
        "miss_validations": 2,
    }
    rows = {
        "form": [
            {"window": "0:10", "form": "good", "code": "V"},
            {"window": "0:10", "form": "junk", "code": "U"},
        ],
        "candidate_check": [{"entry_id": "c1", "verdict": "confirm"}],
        "miss_validation": [
            {"entry_id": "m1", "verdict": "valid_miss"},
            {"entry_id": "m2", "verdict": "entry_wrong", "suggested_status": "retired"},
        ],
        "credit_check": [
            {"form": "a", "entry_id": "e1", "verdict": "true_credit"},
        ],
    }
    summary = merge_diffset.summarize_unit(unit_index, rows, [])
    # confirmed 5 + candidate-confirmed occurrences 2 + in-field unmatched 1 = 8/10
    assert summary["precision_estimate"] == 0.8
    assert summary["miss_validation"] == {
        "judged": 2, "expected": 2, "valid_miss": 1, "entry_wrong": 1,
    }
    assert summary["credit_sample"]["false_credit_rate"] == 0.0
    assert summary["unmatched_judged"]["coverage"] == 1.0


def test_unconfirmed_candidate_credit_stays_out_of_precision():
    unit_index = {
        "forms_total": 10,
        "credited": {"confirmed": 5, "candidate": 2, "disputed": 1},
        "unmatched": 2,
        "candidate_occurrences": {"c1": 2},
        "miss_validations": 0,
    }
    rows = {
        "form": [], "candidate_check": [{"entry_id": "c1", "verdict": "dispute"}],
        "miss_validation": [], "credit_check": [],
    }
    summary = merge_diffset.summarize_unit(unit_index, rows, [])
    assert summary["precision_estimate"] == 0.5  # only the confirmed credits


def test_evolution_rows_route_verdicts_and_gaps():
    rows = {
        "form": [
            {"window": "0:99", "form": "anodizing", "code": "V", "actor": "own"},
            {"window": "0:99", "form": "ghost", "code": "V"},
            {"window": "0:99", "form": "menu", "code": "U"},
        ],
        "candidate_check": [{"entry_id": "c1", "verdict": "confirm", "note": "seen"}],
        "miss_validation": [
            {"entry_id": "m1", "verdict": "entry_wrong", "suggested_status": "retired",
             "reason": "quote is a nav label"},
            {"entry_id": "m2", "verdict": "entry_wrong"},  # no suggested_status
            {"entry_id": "m3", "verdict": "valid_miss"},
        ],
        "credit_check": [],
    }
    texts = {"0:99": "We offer anodizing and painting."}
    verdicts, gaps, problems = merge_diffset.evolution_rows(
        "s.com", "process_caps", rows, texts, "RUNID"
    )
    assert {(v["id"], v["verdict"]) for v in verdicts} == {("c1", "confirm"), ("m1", "retire")}
    assert len(gaps) == 1 and gaps[0]["name"] == "anodizing"
    assert gaps[0]["evidence"][0]["quote"] in texts["0:99"]
    # the un-locatable V form and the statusless entry_wrong both become problems
    assert len(problems) == 2


# ---------------------------------------------- cross-field collision rollup

def test_collision_rollup_counts_a_form_claimed_twice_only_once():
    """A form both siblings return is ONE collision. Summing the per-pair
    counts would report it twice and overstate the metric the prompt edits are
    meant to move."""
    import scorecard as sc_mod  # type: ignore[import-not-found]

    scorecards = [
        {"subject": "s.com", "field": "products", "metrics": {"cross_field_overlap": {
            "distinct_forms": 10, "shared_with_any": 4,
            "by_field": {
                "process_caps": {"shared_forms": 3, "union_forms": 20},
                "equipments": {"shared_forms": 3, "union_forms": 15},
            }}}},
        {"subject": "s.com", "field": "process_caps", "metrics": {"cross_field_overlap": {
            "distinct_forms": 6, "shared_with_any": 3,
            "by_field": {"products": {"shared_forms": 3, "union_forms": 20}}}}},
    ]
    out = "\n".join(sc_mod._collision_rollup(scorecards))
    # products 4/10 and process_caps 3/6, so ALL is 7/16 = 43.8% — not 6+3+3
    assert "| products | 10 | 4 | 40.0% |" in out
    assert "| process_caps | 6 | 3 | 50.0% |" in out
    assert "**7**" in out and "**43.8%**" in out
    # the pair is reported by both fields but counted once, at its own Jaccard
    assert out.count("process_caps ↔ products") == 1
    assert "| process_caps ↔ products | 3 | 15.0% |" in out


def test_collision_rollup_is_silent_without_the_metric():
    import scorecard as sc_mod  # type: ignore[import-not-found]
    assert sc_mod._collision_rollup([{"subject": "s", "field": "products", "metrics": {}}]) == []
