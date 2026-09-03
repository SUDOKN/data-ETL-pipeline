"""The search-stage eval harness's own tests: pure functions and tmp-dir
plumbing only — no Mongo, no run dumps, no LLM. The evaluation itself is the
RUNBOOK protocol; these tests pin the code it leans on."""

import json
import sys
from pathlib import Path

import yaml

_CHECKS = Path(__file__).resolve().parents[1] / "checks"
sys.path.insert(0, str(_CHECKS))

import masking  # type: ignore[import-not-found]  # noqa: E402
import mechanical  # type: ignore[import-not-found]  # noqa: E402
import paths as eval_paths  # type: ignore[import-not-found]  # noqa: E402
import scorecard as scorecard_mod  # type: ignore[import-not-found]  # noqa: E402
from expectations import Expectations  # type: ignore[import-not-found]  # noqa: E402
from loading import (  # type: ignore[import-not-found]  # noqa: E402
    FieldRun,
    WindowRecord,
    _parse_custom_id,
    is_live,
)

SEG = "gpt-4.1|max_completion_tokens=4000|temperature=0.0|seed=12345|pv=PVID"


def _window(sub="0:100", chunk="0:1000", phrases=None, domain=None, **kw):
    return WindowRecord(
        subject="s.com",
        field="products",
        chunk_bounds=chunk,
        sub_bounds=sub,
        custom_id=f"s.com>products>llm_search>chunk>{chunk}>sub>{sub}>{SEG}",
        phrases=phrases,
        domain=domain,
        wire_text=domain,
        **kw,
    )


# ------------------------------------------------------------------- masking

def test_leading_nonce_is_stripped():
    msg = "request nonce (ignore): abc123\n\nreal text\nmore"
    assert masking.wire_text_from_user_message(msg) == "real text\nmore"


def test_trailing_nonce_vintage_is_stripped():
    msg = "real text\nmore\n\nrequest nonce (ignore): abc123"
    assert masking.wire_text_from_user_message(msg) == "real text\nmore\n"


def test_scan_domain_blanks_non_content_lines_length_preserving():
    wire = "##########\nhttps://x.com/page\nSteel doors here\n[page content omitted]\n"
    domain = masking.scan_domain(wire)
    assert len(domain) == len(wire)
    assert "https" not in domain
    assert "#" not in domain
    assert "omitted" not in domain
    assert "Steel doors here" in domain


# ------------------------------------------------------------------- loading

def test_search_and_recursive_custom_ids_parse():
    parsed = _parse_custom_id(f"a.com>equipments>llm_search>chunk>0:96974>sub>0:23771>{SEG}")
    assert parsed == {
        "subject": "a.com", "field": "equipments", "chunk": "0:96974",
        "sub": "0:23771", "segment": SEG, "round": None,
    }
    parsed = _parse_custom_id(
        f"a.com>products>llm_recursive_search>round>2>chunk>0:96974>sub>0:23771>{SEG}"
    )
    assert parsed is not None and parsed["round"] == 2
    assert _parse_custom_id("a.com>products>llm_phrase_mention_collection>chunk>0:1>x") is None


def test_live_vs_replayed_by_created_at_proximity():
    live = _window(created_at="2026-08-25T19:44:57+00:00")
    replayed = _window(created_at="2026-08-23T04:45:00+00:00")
    assert is_live(live, "20260825T194457") is True
    assert is_live(replayed, "20260825T194457") is False
    assert is_live(_window(), "20260825T194457") is None


# ---------------------------------------------------------------- mechanical

def test_verbatim_tiers_and_short_form_case_rule():
    domain = "We sell Steel Doors and lead times.\nCNC machining too."
    assert mechanical.classify_form("Steel Doors", domain) == "exact"
    assert mechanical.classify_form("steel doors", domain) == "casing_only"
    # word boundary: 'lead' inside 'lead times' IS a word hit; 'ead' is not
    assert mechanical.classify_form("lead", domain) == "exact"
    assert mechanical.classify_form("Lead", domain) != "exact"  # short: case-sensitive only
    assert mechanical.classify_form("nowhere at all", domain) == "not_in_window"


def test_degeneration_flags_the_production_loop_threshold():
    looped = _window(phrases=["x"] * 60 + ["y"] * 5)
    quiet = _window(sub="100:200", phrases=[f"p{i}" for i in range(60)])
    metrics = mechanical.degeneration_metrics([looped, quiet])
    assert len(metrics["repetition_loops"]) == 1
    assert metrics["repetition_loops"][0]["top"] == "x"


def test_finish_reason_length_is_surfaced():
    w = _window(finish_reason="length")
    assert mechanical.degeneration_metrics([w])["finish_reason_length_windows"] == ["0:100"]


def test_duplicates_split_casing_families_and_cross_chunk_twins():
    w1 = _window(chunk="0:1000", phrases=["CNC Machining", "CNC machining", "doors"])
    w2 = _window(chunk="1000:2000", sub="1000:1500", phrases=["doors", "doors"])
    metrics = mechanical.duplicate_metrics([w1, w2])
    assert metrics["casing_split_families"] == 1
    assert metrics["cross_chunk_shared_families"] == 1  # 'doors'
    assert metrics["within_window_exact_duplicates"] == 1


def test_consistency_counts_missed_occurrences_in_other_windows():
    # 'steel doors' is vocabulary (window A listed it); window B's text has it
    # but B did not list it -> one missed occurrence.
    wa = _window(sub="0:100", phrases=["steel doors"], domain="steel doors sold here")
    wb = _window(sub="100:200", phrases=["frames"], domain="more steel doors and frames")
    metrics = mechanical.consistency_metrics([wa, wb])
    assert metrics["missed"] == 1
    assert metrics["consistency"] < 1.0


def test_sweep_floor_counts_uncovered_lexicon_hits():
    w = _window(
        phrases=["stainless steel"],
        domain="stainless steel and titanium parts",
    )
    sweeps = [{"name": "metals", "pattern": r"\b(?:stainless steel|titanium)\b"}]
    metrics = mechanical.sweep_metrics([w], sweeps)
    assert metrics["hits"] == 2
    assert metrics["covered"] == 1
    assert metrics["uncovered"][0]["hit"] == "titanium"


def _exp(entries, false_friends=None):
    return Expectations(
        subject="s.com", field="products", path="x", eval_set_version=3,
        entries=entries, false_friends=false_friends or [],
    )


def test_expectation_recall_scopes_to_searched_windows():
    w = _window(phrases=["valve body"], domain="the valve body page",)
    w.wire_text = "the valve body page"
    entries = [
        {"id": "e1", "name": "valve body", "status": "confirmed",
         "acceptable_forms": ["valve body"],
         "evidence": [{"quote": "valve body"}], "provenance": []},
        {"id": "e2", "name": "unreached", "status": "confirmed",
         "acceptable_forms": ["unreached thing"],
         "evidence": [{"quote": "text not in any window"}], "provenance": []},
        {"id": "e3", "name": "missed", "status": "confirmed",
         "acceptable_forms": ["missing designation"],
         "evidence": [{"quote": "valve body page"}], "provenance": []},
        {"id": "e4", "name": "candidate", "status": "candidate",
         "acceptable_forms": ["valve body"],
         "evidence": [{"quote": "valve body"}], "provenance": []},
    ]
    metrics = mechanical.expectation_metrics([w], _exp(entries))
    assert metrics["confirmed_covered"] == 1
    assert [m["id"] for m in metrics["confirmed_missed"]] == ["e3"]
    assert metrics["confirmed_out_of_coverage"] == 1
    assert metrics["confirmed_recall"] == 0.5
    assert metrics["candidates"] == {"covered": 1, "missed": 0, "out_of_coverage": 0}


def test_false_friend_hits_are_reported_not_gated():
    w = _window(phrases=["Lead"], domain="long lead times")
    w.wire_text = "long lead times"
    metrics = mechanical.expectation_metrics(
        [w], _exp([], [{"form": "Lead", "reason": "lead time"}])
    )
    assert metrics["false_friend_hits"] == [{"form": "Lead", "window": "0:100"}]
    verdict = mechanical.verdict({"expectations": metrics}, {})
    assert verdict["status"] == "OK"  # recall-only gating


def test_low_yield_smoke_alarm_flags_but_never_gates():
    # a fat window answered with 1 phrase fires the flag; a thin window and a
    # productive window do not; and the flag lands in warnings, never in reds.
    fat_quiet = _window(sub="0:100", phrases=["one"], domain="x" * 9_000)
    thin_quiet = _window(sub="100:200", phrases=[], domain="x" * 500)
    productive = _window(sub="200:300", phrases=["a", "b", "c"], domain="x" * 9_000)
    wh = mechanical.window_metrics([fat_quiet, thin_quiet, productive])
    flagged = [d["window"] for d in wh["low_yield_windows"]]
    assert flagged == ["0:100"]
    assert wh["empty_windows"] == ["100:200"]
    v = mechanical.verdict({"window_health": wh}, {})
    assert v["status"] == "OK"
    assert v["warnings"] == ["low_yield_windows:1"]
    clean = mechanical.verdict({"window_health": {"low_yield_windows": []}}, {})
    assert "warnings" not in clean


def test_verdict_reds_on_recall_miss_and_tripwires_only():
    metrics = {
        "expectations": {"confirmed_missed": [{"id": "e3"}]},
        "verbatim": {"not_in_window_rate": 0.5, "forms_judged": 2},
        "degeneration": {"repetition_loops": [{"window": "0:100"}],
                         "unparseable_windows": []},
        "lengths": {"line_break_crossers": 0},
    }
    verdict = mechanical.verdict(metrics, {"not_in_window_rate_max": 0.02})
    assert verdict["status"] == "RED"
    assert any(r.startswith("confirmed_recall_misses") for r in verdict["reds"])
    assert any(r.startswith("not_in_window_rate") for r in verdict["reds"])
    assert any(r.startswith("repetition_loops") for r in verdict["reds"])
    # precision-family numbers alone never gate
    assert mechanical.verdict(
        {"verbatim": {"not_in_window_rate": 0.0, "forms_judged": 10}}, {}
    )["status"] == "OK"


# ---------------------------------------------------------------- scorecard

def test_scorecard_roundtrip_and_fingerprint_comparability(tmp_path, monkeypatch):
    monkeypatch.setattr(eval_paths, "RUNS_DIR", tmp_path / "runs")
    monkeypatch.setattr(scorecard_mod, "METRICS_JSONL", tmp_path / "metrics.jsonl")

    def field_run(run_id):
        fr = FieldRun(run_id=run_id, subject="s.com", field="products")
        fr.metadata = {"llm_phrase_search": {"llm_model": {"name": "gpt-4.1"}}}
        fr.scraped_text = {"s3_version_id": "v1"}
        fr.windows = [_window(phrases=["a"], domain="a")]
        return fr

    metrics = {"verbatim": {"not_in_window_rate": 0.01, "forms_judged": 1},
               "verdict": {"status": "OK", "reds": []}}
    first = scorecard_mod.write_scorecard(field_run("20260825T194457"), metrics, [])
    scorecard_mod.append_history(first)
    assert first["comparable_baseline_run"] is None

    second = scorecard_mod.write_scorecard(
        field_run("20260826T000000"),
        {"verbatim": {"not_in_window_rate": 0.03, "forms_judged": 1},
         "verdict": {"status": "OK", "reds": []}},
        [],
    )
    assert second["comparable_baseline_run"] == "20260825T194457"
    assert second["deltas_vs_baseline"]["verbatim.not_in_window_rate"] == 0.02

    # a bounds change breaks comparability
    changed = field_run("20260827T000000")
    changed.windows = [_window(sub="0:999", phrases=["a"], domain="a")]
    third = scorecard_mod.write_scorecard(changed, metrics, [])
    assert third["comparable_baseline_run"] is None

    on_disk = json.loads(
        (tmp_path / "runs" / "20260825T194457" / "scorecard_s_com_products.json").read_text()
    )
    assert on_disk["metrics"]["verbatim"]["forms_judged"] == 1


# ----------------------------------------------------- expectations integrity

def test_every_seeded_expectations_file_parses_and_respects_the_schema():
    expectations_dir = eval_paths.EXPECTATIONS_DIR
    yaml_files = sorted(expectations_dir.glob("*/*.yaml"))
    if not yaml_files:
        return  # seeding not yet done in this checkout
    problems = []
    for path in yaml_files:
        try:
            payload = yaml.safe_load(path.read_text())
        except yaml.YAMLError as error:
            problems.append(f"{path}: YAML parse error: {error}")
            continue
        if not isinstance(payload, dict):
            problems.append(f"{path}: not a mapping")
            continue
        if path.name == "subject.yaml":
            continue
        seen = set()
        for entry in payload.get("entries") or []:
            missing = {"id", "name", "status", "acceptable_forms", "evidence",
                       "provenance"} - set(entry)
            if missing:
                problems.append(f"{path}:{entry.get('id')}: missing {sorted(missing)}")
            if entry.get("status") not in {"candidate", "confirmed", "disputed", "retired"}:
                problems.append(f"{path}:{entry.get('id')}: bad status")
            if entry.get("id") in seen:
                problems.append(f"{path}: duplicate id {entry.get('id')}")
            seen.add(entry.get("id"))
            for evidence in entry.get("evidence") or []:
                quote = evidence.get("quote", "")
                if not quote or len(quote) > 200:
                    problems.append(f"{path}:{entry.get('id')}: bad quote length")
    assert not problems, "\n".join(problems)


# ------------------------------------------- matching hazards found 2026-08-26

def test_non_breaking_spaces_do_not_read_as_fabrication():
    """steelcraft carries 843 NBSPs MID-PHRASE ("in accordance with\\xa0ASTM
    D4585"). Unnormalized, a form typed with a normal space scores
    not_in_window — a false fabrication alarm — and its confirmed entity reads
    as a recall miss, firing a RED gate on correct output."""
    domain = masking.scan_domain("Condensation testing with ASTM D4585-D4585M-18\n")
    assert mechanical.classify_form("ASTM D4585", domain) == "exact"
    assert mechanical._form_covers("ASTM D4585", "ASTM D4585-D4585M-18")
    # and length is preserved, so offsets stay valid
    raw = "a b c"
    assert len(masking.normalize_spaces(raw)) == len(raw)


def test_short_forms_match_on_word_boundaries_not_substrings():
    """The brute `Lead` lesson applied to the eval's own matcher: plain
    containment credits "tight" for TIG and "absolute" for ABS."""
    assert not mechanical._form_covers("TIG", "tight tolerances")
    assert mechanical._form_covers("TIG", "TIG welding")
    assert not mechanical._form_covers("ABS", "absolute positioning")
    assert mechanical._form_covers("ABS", "ABS plastic")
    # long forms keep containment in the COVERING direction only. The reverse
    # ("stainless steel" returned, "304 stainless steel bar" expected) was
    # credited until 2026-08-28, when 410 blind spot-checks of awarded credits
    # measured 8.0% of them false — trap 5 in _shared/text_matching.
    assert mechanical._form_covers("stainless steel", "304 stainless steel bar")
    assert not mechanical._form_covers("304 stainless steel bar", "stainless steel")


def test_expectation_quote_matching_survives_nbsp_in_the_wire_text():
    w = _window(phrases=["INPACT Door System"], domain="x")
    w.wire_text = "The INPACTTM Door System is available on L Series doors"
    w.domain = masking.scan_domain(w.wire_text)
    entries = [{
        "id": "e1", "name": "INPACT Door System", "status": "confirmed",
        "acceptable_forms": ["INPACT Door System"],
        "evidence": [{"quote": "The INPACTTM Door System is available"}],
        "provenance": [],
    }]
    metrics = mechanical.expectation_metrics([w], _exp(entries))
    assert metrics["confirmed_out_of_coverage"] == 0, "quote must locate its window"
    assert metrics["confirmed_covered"] == 1


def test_whitespace_runs_inside_a_phrase_are_not_fabrication():
    """The scraped text carries double spaces and newlines INSIDE phrases
    ("Full  glass architectural entrance doors"). A model echoing the phrase
    with single spaces is quoting faithfully; scoring it not_in_window would
    report fabrication — the gate-firing tier — for correct output."""
    domain = masking.scan_domain("Typical applications\nFull  glass architectural entrance doors.\n")
    assert mechanical.classify_form("Full glass architectural entrance doors", domain) == "exact"
    # case difference is still reported as a casing difference, not as exact
    assert mechanical.classify_form("full glass architectural entrance doors", domain) == "casing_only"
    # a phrase the model composed from two places is still fabrication
    composed = masking.scan_domain("architectural entrances and exterior entrance applications")
    assert mechanical.classify_form("architectural entrance applications", composed) == "not_in_window"
    # newlines inside the phrase behave the same way
    assert mechanical.classify_form(
        "steel doors and frames", masking.scan_domain("we make steel doors\nand frames")
    ) == "exact"


def test_summary_never_prints_null_recall_as_a_result(tmp_path, monkeypatch):
    """An unverified expectation set yields confirmed_recall=None; the summary
    must say "not gated", never render a bare None that reads as "no misses"."""
    monkeypatch.setattr(eval_paths, "RUNS_DIR", tmp_path / "runs")
    monkeypatch.setattr(scorecard_mod, "METRICS_JSONL", tmp_path / "metrics.jsonl")
    sc = {
        "run_id": "20260825T194457", "subject": "s.com", "field": "products",
        "fingerprint_hash": "abc", "comparable_baseline_run": None,
        "deltas_vs_baseline": None,
        "metrics": {
            "verdict": {"status": "OK", "reds": []},
            "cost": {"live": 0, "replayed": 9},
            "expectations": {"confirmed_recall": None,
                             "candidates": {"covered": 3, "missed": 2, "out_of_coverage": 1}},
        },
    }
    out = scorecard_mod.write_summary("20260825T194457", [sc])
    text = out.read_text()
    assert "not gated (6 candidates)" in text
    assert "| None |" not in text


# ------------------------------------------------------- judged-census merge

def _judgments(tmp_path, name, records):
    path = tmp_path / name
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n")
    return path


def test_judged_summary_rolls_field_codes_into_comparable_buckets(tmp_path):
    import merge_judgments as mj  # type: ignore[import-not-found]

    path = _judgments(tmp_path, "s__products.jsonl", [
        {"window": "0:100", "form": "steel doors", "code": "V", "actor": "own"},
        {"window": "0:100", "form": "hinges", "code": "B", "actor": "supplier"},
        {"window": "0:100", "form": "CNC machining", "code": "P"},
        {"window": "0:100", "form": "parts", "code": "G"},
        {"window": "0:100", "form": "ADD TO CART", "code": "U"},
        {"type": "miss", "window": "0:100", "entity": "T Series doors", "quote": "T Series doors"},
    ])
    forms, misses = mj.read_judgments(path)
    summary = mj.summarize(forms, misses)

    assert summary["forms_judged"] == 5
    assert summary["rollup"] == {"adjacent_field": 1, "generic": 1, "in_field": 2, "junk": 1}
    assert summary["precision_in_field"] == 0.4
    assert summary["junk_rate"] == 0.2
    # a supplier's part is still in_field (recall-first) and shows as wrong-actor
    assert summary["wrong_actor_share"] == 0.2
    assert summary["miss_count"] == 1


def test_agreement_floor_is_computed_only_over_forms_both_judges_coded(tmp_path):
    import merge_judgments as mj  # type: ignore[import-not-found]

    primary, _ = mj.read_judgments(_judgments(tmp_path, "a.jsonl", [
        {"window": "w", "form": "one", "code": "V"},
        {"window": "w", "form": "two", "code": "G"},
        {"window": "w", "form": "three", "code": "V"},
    ]))
    second, _ = mj.read_judgments(_judgments(tmp_path, "b.jsonl", [
        {"window": "w", "form": "one", "code": "B"},   # same rollup (in_field)
        {"window": "w", "form": "two", "code": "U"},   # differs: generic vs junk
    ]))
    result = mj.agreement(primary, second)

    assert result is not None
    assert result["compared"] == 2, "the form only one judge coded is excluded"
    assert result["agreement"] == 0.5
    assert result["disagreements"][0]["form"] == "two"


def test_agreement_is_null_when_nobody_double_judged(tmp_path):
    import merge_judgments as mj  # type: ignore[import-not-found]

    primary, _ = mj.read_judgments(_judgments(tmp_path, "a.jsonl", [
        {"window": "w", "form": "one", "code": "V"},
    ]))
    assert mj.agreement(primary, []) is None


def test_cross_field_overlap_measures_the_field_axis_defect():
    """A field whose vocabulary is mostly another field's is not holding its
    boundary — the measured signature of equipments returning products.
    `share_of_own` normalizes by the field's OWN size, so a small field
    drowning in a big field's forms scores high while the big field does not.
    """
    subject_runs = {
        "equipments": [_window(phrases=["steel doors", "frames", "CNC lathe"])],
        "products": [_window(phrases=["steel doors", "frames", "hinges", "trim", "glass"])],
        "industries": [_window(phrases=["healthcare"])],
    }
    equipment = mechanical.cross_field_overlap(subject_runs, "equipments")
    assert equipment is not None
    assert equipment["distinct_forms"] == 3
    assert equipment["worst_field"] == "products"
    # 2 of its own 3 forms are products' forms
    assert equipment["worst_share"] == round(2 / 3, 4)

    products = mechanical.cross_field_overlap(subject_runs, "products")
    assert products is not None
    # same 2 shared forms, but only 2 of 5 of ITS vocabulary — the asymmetry
    # is the point: the small field is the one being contaminated
    assert products["by_field"]["equipments"]["share_of_own"] == 0.4


def test_cross_field_overlap_is_absent_when_a_field_returned_nothing():
    assert mechanical.cross_field_overlap({"equipments": []}, "equipments") is None


def test_a_mechanical_rerun_never_discards_a_judged_census(tmp_path, monkeypatch):
    """A judged census costs many agent-hours. Re-running the cheap mechanical
    pass must carry it forward — and must MARK it stale if the fingerprint it
    was judged under no longer matches."""
    monkeypatch.setattr(eval_paths, "RUNS_DIR", tmp_path / "runs")
    monkeypatch.setattr(scorecard_mod, "METRICS_JSONL", tmp_path / "metrics.jsonl")

    def field_run(sub_bounds="0:100"):
        fr = FieldRun(run_id="20260825T194457", subject="s.com", field="products")
        fr.metadata = {"llm_phrase_search": {"llm_model": {"name": "gpt-4.1"}}}
        fr.scraped_text = {"s3_version_id": "v1"}
        fr.windows = [_window(sub=sub_bounds, phrases=["a"], domain="a")]
        return fr

    base = {"verdict": {"status": "OK", "reds": []}}
    scorecard_mod.write_scorecard(field_run(), dict(base), [])

    # a judged census lands
    path = tmp_path / "runs" / "20260825T194457" / "scorecard_s_com_products.json"
    payload = json.loads(path.read_text())
    payload["metrics"]["judged"] = {"forms_judged": 42, "precision_in_field": 0.37}
    path.write_text(json.dumps(payload))

    # the mechanical pass runs again on the SAME geometry
    again = scorecard_mod.write_scorecard(field_run(), dict(base), [])
    assert again["metrics"]["judged"]["forms_judged"] == 42
    assert "stale" not in again["metrics"]["judged"]

    # and again after the windows changed — the census is kept but flagged
    moved = scorecard_mod.write_scorecard(field_run(sub_bounds="0:999"), dict(base), [])
    assert moved["metrics"]["judged"]["forms_judged"] == 42
    assert "re-judge" in moved["metrics"]["judged"]["stale"]


def test_recall_leniency_is_deliberate_and_short_forms_stay_strict():
    """Pinned so nobody tightens these into false REDs (see
    EXPECTATIONS_SCHEMA.md "Two scoring properties that are DELIBERATE").
    A broader entity is credited by a narrower returned form; a short form is
    not credited by a longer word that merely contains its letters."""
    assert mechanical._form_covers("Steel", "Stainless Steel")
    assert mechanical._form_covers("shear", "Shearing")
    assert mechanical._form_covers("press brake", "CNC 7 Axis Press Brakes")
    # ...but the brute-`Lead` class stays strict
    assert not mechanical._form_covers("EMS", "quality assurance systems")


def test_an_entity_quoted_in_every_footer_is_not_charged_a_miss_per_window():
    """Entry-level coverage: `Tool & Die` sits in 15 copyright footers, so it
    is in scope for many windows. One window finding it is enough."""
    found = _window(sub="0:100", phrases=["Tool & Die"], domain="x")
    found.wire_text = "Tool & Die capabilities here"
    found.domain = masking.scan_domain(found.wire_text)
    silent = _window(sub="100:200", phrases=["terms of sale"], domain="x")
    silent.wire_text = "legal text ... Tool & Die (footer)"
    silent.domain = masking.scan_domain(silent.wire_text)
    entries = [{
        "id": "e1", "name": "Tool & Die", "status": "confirmed",
        "acceptable_forms": ["Tool & Die"],
        "evidence": [{"quote": "Tool & Die"}], "provenance": [],
    }]
    metrics = mechanical.expectation_metrics([found, silent], _exp(entries))
    assert metrics["confirmed_covered"] == 1
    assert metrics["confirmed_missed"] == []


def test_window_keys_normalize_so_two_judges_can_be_compared(tmp_path):
    """Judges have written the window both as "0:23771" and as the full packet
    heading. Un-normalized, the two share zero keys and the agreement floor
    silently reads as "no data" instead of "format mismatch"."""
    import merge_judgments as mj  # type: ignore[import-not-found]

    assert mj.normalize_window("Window 0:23771 (chunk 0:96974)") == "0:23771"
    assert mj.normalize_window("0:23771") == "0:23771"
    assert mj.normalize_window(None) is None

    primary, _ = mj.read_judgments(_judgments(tmp_path, "a.jsonl", [
        {"window": "Window 0:100 (chunk 0:900)", "form": "x", "code": "V"},
    ]))
    second, _ = mj.read_judgments(_judgments(tmp_path, "b.jsonl", [
        {"window": "0:100", "form": "x", "code": "V"},
    ]))
    result = mj.agreement(primary, second)
    assert result is not None and result["compared"] == 1 and result["agreement"] == 1.0


def test_merged_designations_are_detected_including_exotic_separators():
    """Search should emit ONE designation per form. The judged census found
    two merge shapes: several full designations in one string, and shorthand
    where the body is stated once ("ASTM E1886/E1996"). One real case used
    U+2044 FRACTION SLASH, which a splitter keyed on "/" passes through
    intact — so the metric flags non-ASCII separators explicitly."""
    sweeps = [{"name": "std",
               "pattern": r"\b(?:ASTM|FEMA|UL)[\s-]?(?:[A-Z]{1,2})?-?\d[\dA-Za-z.-]*"}]
    w = _window(phrases=[
        "ASTM E1886/E1996",                      # shorthand merge, ASCII slash
        "FEMA 361\u2044320",                      # shorthand merge, U+2044
        "Compliance with ASTM A666, ASTM E152",  # two full designations
        "ASTM E330",                             # clean single designation
        "steel doors",                           # not a designation at all
    ])
    metrics = mechanical.merged_designation_metrics([w], sweeps)
    assert metrics is not None
    assert metrics["forms_checked"] == 5
    assert metrics["merged_forms"] == 3
    assert metrics["with_non_ascii_separator"] == 1
    # the clean designation and the non-designation are not flagged
    flagged = {e["form"] for e in metrics["examples"]}
    assert "ASTM E330" not in flagged and "steel doors" not in flagged


def test_merged_designation_metric_is_absent_without_a_sweep():
    assert mechanical.merged_designation_metrics([_window(phrases=["x"])], []) is None


def test_the_conformity_sweep_matches_letter_prefixed_designations():
    """Regression: the shipped conformity sweep must match ASTM's E-series and
    ANSI A250.8. An earlier pattern required a DIGIT right after the body, so
    it matched none of them — most of steelcraft's standards — and understated
    the sweep floor with no error to notice."""
    import re

    import yaml

    config = yaml.safe_load(
        (eval_paths.FIELDS_CONFIG_DIR / "conformity_attestations.yaml").read_text()
    )
    pattern = re.compile(
        next(s["pattern"] for s in config["sweeps"] if s["name"] == "standards_ids")
    )
    for designation in ("ASTM E1886", "ASTM E1996", "ASTM E152", "ASTM E330",
                        "ASTM A653", "ANSI A250.8", "ISO 9001", "UL 10C",
                        "NFPA 80", "ICC 500-2020", "TAS 201", "FEMA P-320"):
        assert pattern.search(designation), designation


# --- Seeding pipeline: agent JSONL -> expectation YAML -> verification ---------
# These pin the mechanical half of seeding a new corpus subject, which is the
# half that goes silently wrong: ids, offsets, dedup, status transitions, and
# above all the refusal to let an unverified entry look verified.


def _seed_fixture(tmp_path, monkeypatch, jsonl_lines, text):
    """A throwaway corpus of one subject, wired into both seeding scripts."""
    import apply_verifications as av  # type: ignore[import-not-found]
    import build_expectations as be  # type: ignore[import-not-found]

    # The built YAML records `snapshot.file` as a REPO-RELATIVE path, and
    # apply_verifications resolves it against REPO_ROOT — so the fixture has to
    # mirror that layout, not just park the text anywhere.
    texts = tmp_path / be.SAMPLE_TEXTS_REL
    texts.mkdir(parents=True, exist_ok=True)
    text_file = texts / "widget.co.txt"
    text_file.write_text(text)
    expectations = tmp_path / "expectations"
    expectations.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(be, "EXPECTATIONS_DIR", expectations)
    monkeypatch.setattr(be, "text_path_for", lambda slug: text_file)
    monkeypatch.setattr(av, "EXPECTATIONS_DIR", expectations)
    monkeypatch.setattr(av, "REPO_ROOT", tmp_path)

    jsonl = tmp_path / "seed.jsonl"
    jsonl.write_text("\n".join(json.dumps(row) for row in jsonl_lines))
    return be, av, expectations, jsonl


_SEED_TEXT = (
    "https://widget.co/capabilities\n"
    "We run vacuum brazing in two furnaces.\n"
    "Our partner holds AS9100D for the Houston site.\n"
    "https://widget.co/process\n"
    "Inside our vacuum brazing furnaces the joint is heated slowly.\n"
)


def _entry(**overrides):
    row = {
        "field": "process_caps",
        "name": "vacuum brazing",
        "acceptable_forms": ["vacuum brazing"],
        "evidence": [{"quote": "We run vacuum brazing in two furnaces."}],
        "actor": "own",
    }
    row.update(overrides)
    return row


def test_seeding_assigns_ids_offsets_and_leaves_everything_candidate(tmp_path, monkeypatch):
    be, _, expectations, jsonl = _seed_fixture(
        tmp_path, monkeypatch,
        [{"type": "subject", "role": "attribution_negative"}, _entry()],
        _SEED_TEXT,
    )
    assert be.build("widget_co", jsonl, merge=False) == 0

    payload = yaml.safe_load((expectations / "widget_co" / "process_caps.yaml").read_text())
    entry = payload["entries"][0]
    assert entry["id"] == "widget_co-process_caps-0001"
    # A seed is never self-confirming: promotion is the verification pass's act.
    assert entry["status"] == "candidate"
    assert entry["evidence"][0]["approx_offset"] == _SEED_TEXT.index("We run vacuum")
    assert "actor" not in entry  # `own` is the default and stays implicit
    subject = yaml.safe_load((expectations / "widget_co" / "subject.yaml").read_text())
    assert subject["role"] == "attribution_negative"


def test_seeding_complains_when_a_quote_is_not_in_the_text(tmp_path, monkeypatch):
    """The failure mode this catches is a retyped quote — the commonest defect,
    and one that turns into a false fabrication verdict months later."""
    be, _, _, jsonl = _seed_fixture(
        tmp_path, monkeypatch,
        [_entry(evidence=[{"quote": "We run vacuum brazing in three furnaces."}])],
        _SEED_TEXT,
    )
    assert be.build("widget_co", jsonl, merge=False) == 1


def test_seeding_deduplicates_across_slices_of_one_subject(tmp_path, monkeypatch):
    """agstech is seeded by eight parallel slice agents over one document whose
    boilerplate repeats; the same entity arriving twice must not become two."""
    be, _, expectations, jsonl = _seed_fixture(
        tmp_path, monkeypatch,
        [_entry(), _entry(name="Vacuum  Brazing")],
        _SEED_TEXT,
    )
    be.build("widget_co", jsonl, merge=False)
    payload = yaml.safe_load((expectations / "widget_co" / "process_caps.yaml").read_text())
    assert len(payload["entries"]) == 1


def test_verification_promotes_disputes_retires_and_amends(tmp_path, monkeypatch):
    be, av, expectations, jsonl = _seed_fixture(
        tmp_path, monkeypatch,
        [
            _entry(),
            _entry(name="AS9100D", field="conformity_attestations",
                   acceptable_forms=["AS9100D"], actor="supplier",
                   evidence=[{"quote": "Our partner holds AS9100D for the Houston site."}]),
        ],
        _SEED_TEXT,
    )
    be.build("widget_co", jsonl, merge=False)

    verdicts = tmp_path / "verify.jsonl"
    verdicts.write_text("\n".join(json.dumps(row) for row in [
        {"id": "widget_co-process_caps-0001", "verdict": "amend",
         "corrected_forms": ["vacuum brazing", "vacuum braze"], "reason": "spelling"},
        {"id": "widget_co-conformity_attestations-0001", "verdict": "dispute",
         "reason": "held by the partner, not the subject"},
    ]))
    av.apply("widget_co", verdicts, by="verify-agent-widget", date="2026-08-27")

    process = yaml.safe_load((expectations / "widget_co" / "process_caps.yaml").read_text())
    amended = process["entries"][0]
    assert amended["status"] == "confirmed"
    assert amended["acceptable_forms"] == ["vacuum brazing", "vacuum braze"]
    assert amended["provenance"][-1]["by"] == "verify-agent-widget"
    assert process["eval_set_version"] == 2

    conformity = yaml.safe_load(
        (expectations / "widget_co" / "conformity_attestations.yaml").read_text())
    disputed = conformity["entries"][0]
    assert disputed["status"] == "disputed"
    assert "held by the partner" in disputed["notes"]


def test_an_unjudged_entry_stays_candidate_and_is_reported(tmp_path, monkeypatch, capsys):
    """Silence is not consent. A verification pass that quietly skips entries
    must leave them non-gating AND say which ones, or a thin census reads as a
    complete one."""
    be, av, expectations, jsonl = _seed_fixture(
        tmp_path, monkeypatch, [_entry(), _entry(name="furnaces")], _SEED_TEXT,
    )
    be.build("widget_co", jsonl, merge=False)
    verdicts = tmp_path / "verify.jsonl"
    verdicts.write_text(json.dumps(
        {"id": "widget_co-process_caps-0001", "verdict": "confirm"}))
    av.apply("widget_co", verdicts, by="verify-agent-widget", date="2026-08-27")

    payload = yaml.safe_load((expectations / "widget_co" / "process_caps.yaml").read_text())
    statuses = {e["id"]: e["status"] for e in payload["entries"]}
    assert statuses["widget_co-process_caps-0001"] == "confirmed"
    assert statuses["widget_co-process_caps-0002"] == "candidate"
    assert "widget_co-process_caps-0002" in capsys.readouterr().out


def test_verification_misses_enter_as_candidate_not_confirmed(tmp_path, monkeypatch):
    """Finding an entity is one reading, not two — a miss the verifier adds has
    been seen by exactly one person and must not gate."""
    be, av, expectations, jsonl = _seed_fixture(
        tmp_path, monkeypatch, [_entry()], _SEED_TEXT,
    )
    be.build("widget_co", jsonl, merge=False)
    verdicts = tmp_path / "verify.jsonl"
    verdicts.write_text(json.dumps({
        "type": "miss", "field": "conformity_attestations", "name": "AS9100D",
        "acceptable_forms": ["AS9100D"],
        "evidence": [{"quote": "Our partner holds AS9100D for the Houston site."}],
        "actor": "supplier", "reason": "on the quality page",
    }))
    av.apply("widget_co", verdicts, by="verify-agent-widget", date="2026-08-27")

    payload = yaml.safe_load(
        (expectations / "widget_co" / "conformity_attestations.yaml").read_text())
    added = payload["entries"][0]
    assert added["status"] == "candidate"
    assert added["actor"] == "supplier"
    assert added["provenance"][0]["source"] == "verification-pass-miss"


def test_a_seed_may_dispute_an_entry_but_never_confirm_one(tmp_path, monkeypatch):
    """A seed can argue an entity sits outside its field's clause — that argument
    must survive as `disputed`. What it must NOT be able to do is promote itself
    to `confirmed`, which is the second reader's word alone."""
    be, _, expectations, jsonl = _seed_fixture(
        tmp_path, monkeypatch,
        [
            _entry(name="vacuum brazing", status_hint="disputed"),
            _entry(name="furnaces", notes="DISPUTED — tooling, not a process"),
            _entry(name="two furnaces", status_hint="confirmed"),
        ],
        _SEED_TEXT,
    )
    assert be.build("widget_co", jsonl, merge=False) == 1  # the self-confirm complains

    payload = yaml.safe_load((expectations / "widget_co" / "process_caps.yaml").read_text())
    statuses = {e["name"]: e["status"] for e in payload["entries"]}
    assert statuses["vacuum brazing"] == "disputed"
    assert statuses["furnaces"] == "disputed"      # the notes-prefix convention
    assert statuses["two furnaces"] == "candidate"  # refused, not honoured


def test_a_corrected_quote_is_added_beside_the_old_ones_not_swapped_for_them(tmp_path, monkeypatch):
    """Recall is WINDOW-SCOPED: an entry counts against a run only if one of its
    quotes falls in a window that run searched. So replacing the evidence list
    with a single correction silently deletes window coverage — the entry stops
    being findable in pages it really appears on. Found by the superiortech
    verification pass, 2026-08-27."""
    be, av, expectations, jsonl = _seed_fixture(
        tmp_path, monkeypatch,
        [_entry(evidence=[{"quote": "We run vacuum brazing in two furnaces."}])],
        _SEED_TEXT,
    )
    be.build("widget_co", jsonl, merge=False)
    verdicts = tmp_path / "verify.jsonl"
    verdicts.write_text(json.dumps({
        "id": "widget_co-process_caps-0001", "verdict": "amend",
        "corrected_quote": "our vacuum brazing furnaces",
        "reason": "a second occurrence on another page",
    }))
    av.apply("widget_co", verdicts, by="verify-agent-widget", date="2026-08-27")

    payload = yaml.safe_load((expectations / "widget_co" / "process_caps.yaml").read_text())
    quotes = [e["quote"] for e in payload["entries"][0]["evidence"]]
    assert "We run vacuum brazing in two furnaces." in quotes  # kept
    assert "our vacuum brazing furnaces" in quotes             # added


def test_replace_evidence_drops_the_old_quotes_when_asked_explicitly(tmp_path, monkeypatch):
    """The escape hatch for a quote that is actually WRONG, not merely unhelpful."""
    be, av, expectations, jsonl = _seed_fixture(
        tmp_path, monkeypatch,
        [_entry(evidence=[{"quote": "We run vacuum brazing in two furnaces."}])],
        _SEED_TEXT,
    )
    be.build("widget_co", jsonl, merge=False)
    verdicts = tmp_path / "verify.jsonl"
    verdicts.write_text(json.dumps({
        "id": "widget_co-process_caps-0001", "verdict": "amend",
        "corrected_quote": "our vacuum brazing furnaces",
        "replace_evidence": True, "reason": "the original quote was misattributed",
    }))
    av.apply("widget_co", verdicts, by="verify-agent-widget", date="2026-08-27")

    payload = yaml.safe_load((expectations / "widget_co" / "process_caps.yaml").read_text())
    quotes = [e["quote"] for e in payload["entries"][0]["evidence"]]
    assert quotes == ["our vacuum brazing furnaces"]


def test_an_entry_shadowed_by_its_siblings_is_flagged_not_failed(tmp_path, monkeypatch):
    """`forms_overlap` credits an entry when a RETURNED form contains one of its
    acceptable forms. So an entry whose forms are all substrings of sibling
    entries' forms is credited whenever any sibling is returned — its recall
    number carries no information of its own. That leniency is deliberate and
    must stay (tightening it manufactures false REDs), so this is a WARNING the
    reader can weigh, never an error. Measured corpus-wide at 9.8% on
    2026-08-27."""
    import expectations as expectations_mod  # type: ignore[import-not-found]
    import validate_expectations as ve  # type: ignore[import-not-found]

    be, _, expectations, jsonl = _seed_fixture(
        tmp_path, monkeypatch,
        [
            {"type": "subject", "role": "full_unit"},
            _entry(name="brazing", acceptable_forms=["brazing"]),
            _entry(name="vacuum brazing", acceptable_forms=["vacuum brazing"]),
        ],
        _SEED_TEXT,
    )
    be.build("widget_co", jsonl, merge=False)
    monkeypatch.setattr(ve, "EXPECTATIONS_DIR", expectations)
    monkeypatch.setattr(ve, "REPO_ROOT", tmp_path)
    # the loader resolves its own copy of the constant, not the validator's
    monkeypatch.setattr(expectations_mod, "EXPECTATIONS_DIR", expectations)

    errors, warnings, _ = ve.validate_subject("widget_co")
    assert errors == []  # shadowing is never an error
    shadowed = [w for w in warnings if "never independently miss" in w]
    assert len(shadowed) == 1
    assert "process_caps-0001" in shadowed[0]  # `brazing` ⊂ `vacuum brazing`
