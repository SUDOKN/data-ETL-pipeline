"""Tests for the grounding evaluation instrument's code half (run_eval.py).

The instrument itself is not a pytest suite — it needs a run id and agent
judgment. These tests pin the deterministic checks against a synthetic fixture
dump that exercises every check's firing path, so the instrument's own code is
covered by the normal suite.
"""

import importlib.util
import json
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
INSTRUMENT = HERE.parent
FIXTURE_RUN = HERE / "fixtures" / "fixture_run"

_spec = importlib.util.spec_from_file_location(
    "grounding_run_eval", INSTRUMENT / "checks" / "run_eval.py"
)
assert _spec is not None and _spec.loader is not None
run_eval = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(run_eval)


@pytest.fixture(scope="module")
def material_metrics():
    fd = run_eval.FieldDump(FIXTURE_RUN / "testsubject_com__material_caps.json")
    version, labels = run_eval.load_label_set("material_caps")
    assert labels, "material ontology labels must load"
    metrics, findings = run_eval.check_field_dump(fd, labels, version)
    return metrics, findings


def test_item_enumeration(material_metrics):
    metrics, _ = material_metrics
    assert metrics["items"] == {"tag_instances": 7, "declines": 8, "descent_hops": 1}
    assert metrics["rows"] == 8


def test_failed_rule_gate(material_metrics):
    metrics, findings = material_metrics
    assert metrics["gate_violations"] == {"count": 1, "shipped": 1}
    gate = [f for f in findings if f["check"] == "failed_rule_gate"]
    assert len(gate) == 1 and gate[0]["group_id"] == "ggate001"
    assert "SHIPPED" in gate[0]["detail"]


def test_membership_and_sentinel(material_metrics):
    metrics, findings = material_metrics
    # 4 in-vocab tags + 1 descent tag checked; only the sentinel string violates
    assert metrics["membership"]["checked"] == 5
    assert metrics["membership"]["violations"] == 1
    assert metrics["sentinel_leaks"] == 1
    viol = [f for f in findings if f["check"] == "vocab_membership"]
    assert len(viol) == 1 and viol[0]["group_id"] == "gsent001"


def test_dropped_options(material_metrics):
    metrics, findings = material_metrics
    assert metrics["dropped_options"] == {"total": 2, "false_drops": 1, "oov_recaptured": 1}
    deco = [f for f in findings if f["check"] == "decoration_echo"]
    assert len(deco) == 1 and "Carbon Steel" in deco[0]["detail"]


def test_duplicates_twins_churn(material_metrics):
    metrics, _ = material_metrics
    assert metrics["duplicate_tag_rows"] == 1
    assert metrics["twins"] == {"twin_groups": 1, "divergent": 1, "divergence_rate": 1.0}
    assert metrics["tag_churn"]["clusters"] == 1
    assert metrics["tag_churn"]["examples"] == {"steel": ["Steel", "steel"]}


def test_descent_metrics(material_metrics):
    metrics, _ = material_metrics
    d = metrics["descent"]
    assert d["minted_tags"] == 1 and d["screened"] == 0
    assert d["requests_listed"] == 2 and d["requests_real"] == 1


def test_status_accounting(material_metrics):
    metrics, findings = material_metrics
    assert metrics["status_counts"]["weird_status"] == 1
    status_findings = [f for f in findings if f["check"] == "status_accounting"]
    details = {f["group_id"]: f["detail"] for f in status_findings}
    assert "gbad001" in details and "unknown status" in details["gbad001"]
    assert "gnm001" in details and "grounding tags present" in details["gnm001"]


def test_ops_rollup(material_metrics):
    metrics, _ = material_metrics
    ops = metrics["ops"]
    assert ops["llm_phrase_initial_grounding"]["requests"] == 1
    assert ops["llm_phrase_oov_grounding"]["retry_requests"] == 1
    # the phantom descent request is excluded from the rollup
    assert ops["llm_phrase_recursive_tagging"]["requests"] == 1
    assert ops["llm_phrase_initial_grounding"]["latency_ms_p50"] == 1200


def test_reproducibility_aa():
    dumps = [
        run_eval.FieldDump(FIXTURE_RUN / "testsubject_com__products.json"),
        run_eval.FieldDump(FIXTURE_RUN / "testsubject_com__contract_products.json"),
    ]
    aa = run_eval.reproducibility_aa(dumps)
    v = aa["testsubject.com"]
    assert v["comparable"] is True
    assert v["groups_compared"] == 2
    assert v["identical_tag_sets"] == 1
    assert v["decline_vs_tag_flips"] == 1
    assert v["row_divergence"] == 0.5


def test_noise_band_shrinks_with_n():
    b50 = run_eval.noise_band(50, 0.16)
    b500 = run_eval.noise_band(500, 0.16)
    assert 0 < b500 < b50 < 1
    assert run_eval.noise_band(0, 0.16) == 1.0


def test_merge_judgments_end_to_end(tmp_path, monkeypatch):
    monkeypatch.setattr(run_eval, "RESULTS_DIR", tmp_path / "results")
    monkeypatch.setattr(run_eval, "LEDGER_JSON", tmp_path / "ledger.json")
    monkeypatch.setattr(run_eval, "LEDGER_MD", tmp_path / "LEDGER.md")

    out_dir = tmp_path / "results" / "testrun"
    (out_dir / "judgments").mkdir(parents=True)
    metrics = {
        "run_id": "testrun",
        "taxonomy_version": 1,
        "row_divergence_estimate": 0.16,
        "deterministic": {
            "testsubject_com__material_caps": {
                "items": {"tag_instances": 2, "declines": 1, "descent_hops": 0},
                "gate_violations": {"count": 0, "shipped": 0},
                "membership": {"checked": 2, "violations": 0},
                "sentinel_leaks": 0,
                "dropped_options": {"total": 0, "false_drops": 0, "oov_recaptured": 0},
                "twins": {"twin_groups": 0, "divergent": 0, "divergence_rate": None},
                "tag_churn": {"surviving_tags": 2, "clusters": 0, "examples": {}},
                "descent": {"minted_tags": 0, "screened": 0},
            }
        },
    }
    (out_dir / "metrics.json").write_text(json.dumps(metrics))
    lines = [
        {"item": "tag", "code": "D", "screening": "passed", "row_status": "grounded"},
        {
            "item": "tag",
            "code": "F",
            "tag": "Invented",
            "chunk": "0:100",
            "group_id": "gx",
            "screening": "passed",
            "row_status": "grounded",
            "watch_items": ["GW-MC-1"],
        },
        {"item": "decline", "code": "DS"},
        {"item": "summary", "recall_gaps": []},
    ]
    (out_dir / "judgments" / "testsubject_com__material_caps.jsonl").write_text(
        "\n".join(json.dumps(x) for x in lines)
    )

    run_eval.merge_judgments("testrun")

    merged = json.loads((out_dir / "metrics.json").read_text())
    jv = merged["judged"]["testsubject_com__material_caps"]
    assert jv["clean_share"] == 0.5
    assert jv["fabrication_count"] == 1
    assert jv["shipped_defects"] == 1
    assert jv["decline_sound_share"] == 1.0
    assert jv["completeness"]["tags"] == [2, 2]
    assert jv["watch_item_hits"] == {"GW-MC-1": 1}
    # a watch-item hit on a first-ever row lands as BASELINE/WATCH
    assert merged["tiers"]["testsubject_com__material_caps"] == "BASELINE/WATCH"

    ledger = json.loads((tmp_path / "ledger.json").read_text())
    assert len(ledger) == 1 and ledger[0]["run_id"] == "testrun"
    assert (tmp_path / "LEDGER.md").exists()
