"""Tests for the recoverability readout (checks/recoverability.py) against the
synthetic fixture run, so the survey's code half is covered by the suite."""

import importlib.util
import json
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
INSTRUMENT = HERE.parent
FIXTURE_RUN = HERE / "fixtures" / "fixture_run"

_spec = importlib.util.spec_from_file_location(
    "grounding_recoverability", INSTRUMENT / "checks" / "recoverability.py"
)
assert _spec is not None and _spec.loader is not None
recoverability = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(recoverability)


@pytest.fixture(scope="module")
def items():
    return recoverability.enumerate_items(FIXTURE_RUN)


def test_enumeration_matches_the_harness_item_counts(items):
    material = [it for it in items if it["field"] == "material_caps"]
    kinds = {}
    for it in material:
        kinds[it["kind"]] = kinds.get(it["kind"], 0) + 1
    # The harness's own enumeration of the same fixture: 7 tags, 8 declines, 1 hop.
    assert kinds == {"tag": 7, "decline": 8, "hop": 1}
    assert len({it["item_id"] for it in items}) == len(items), "item ids must be unique"


def test_tag_items_carry_the_mechanical_screening_outcome(items):
    steel = next(
        it for it in items
        if it["kind"] == "tag" and it["group_id"] == "gtwin001" and it["chunk"] == "0:100"
    )
    assert steel["stage"] == "in_vocab" and steel["tag"] == "Steel"
    assert steel["screening"]["verdict"] == "passed"
    assert steel["shipped"] is True
    assert steel["twin"] is True and steel["twin_divergent"] is True
    unscreened = next(it for it in items if it["kind"] == "tag" and it["group_id"] == "gdrop001")
    assert unscreened["screening"]["verdict"] == "absent"
    assert unscreened["shipped"] is False


def test_mechanical_matrix_totals(items):
    matrix = recoverability.mechanical_matrix(items)
    allb = matrix["ALL"]
    assert sum(allb["tags"].values()) == sum(1 for it in items if it["kind"] == "tag")
    assert allb["hops"] == sum(1 for it in items if it["kind"] == "hop")
    material = matrix["testsubject_com__material_caps"]
    assert material["tags"] == {"in_vocab": 4, "oov": 3}
    assert material["twin_divergent_items"] > 0


def test_packets_sample_outlines_and_merge_round_trip(items, tmp_path):
    index = recoverability.write_packets(items, tmp_path, packet_size=5)
    assert sum(p["items"] for p in index) == len(items)
    assert all(p["items"] <= 5 for p in index)
    sample = recoverability.write_omission_sample(FIXTURE_RUN, tmp_path, per_dump=2, seed=1)
    assert all(s["sampled"] <= 2 for s in sample)
    recoverability.render_outlines(tmp_path)
    assert (tmp_path / "outlines" / "material_caps.txt").exists()

    # A judge codes one defective tag as unrecoverable, one decline as a loss.
    tag = next(it for it in items if it["kind"] == "tag" and it["screening"]["verdict"] == "passed")
    decline = next(it for it in items if it["kind"] == "decline")
    verdicts_dir = tmp_path / "verdicts"
    verdicts_dir.mkdir()
    (verdicts_dir / "j.jsonl").write_text(
        json.dumps({"item_id": tag["item_id"], "code": "X", "recoverable_by": "none"}) + "\n"
        + json.dumps({"item_id": decline["item_id"], "code": "DF", "lost_label": "Copper"}) + "\n"
        + "{not json\n"
    )
    matrix = recoverability.recoverability_matrix(items, recoverability.load_verdicts(verdicts_dir))
    assert matrix["coverage"]["judged"] == {"tag": 1, "decline": 1}
    assert matrix["tags"]["ALL"]["structurally_unrecoverable"] == 1
    assert matrix["tags"]["ALL"]["shipped_by_code"] == {"X": 1}
    assert matrix["lost_labels"][0]["lost_label"] == "Copper"
    recoverability.write_recoverability_report(tmp_path, matrix, None)
    assert "structurally unrecoverable" in (tmp_path / "RECOVERABILITY.md").read_text()


def test_verify_packets_and_corrections_overlay(items, tmp_path):
    tag = next(it for it in items if it["kind"] == "tag")
    clean = next(it for it in items if it["kind"] == "tag" and it is not tag)
    verdicts_dir = tmp_path / "verdicts"
    verdicts_dir.mkdir()
    (verdicts_dir / "j.jsonl").write_text(
        json.dumps({"item_id": tag["item_id"], "code": "F", "recoverable_by": "SCR-1"}) + "\n"
        + json.dumps({"item_id": clean["item_id"], "code": "D"}) + "\n"
    )
    index = recoverability.write_verify_packets(
        items, recoverability.load_verdicts(verdicts_dir), tmp_path, random_clean_per_file=1
    )
    rows = [json.loads(l) for l in (tmp_path / "verify" / "packet_1.jsonl").read_text().splitlines()]
    assert {r["verdict"]["code"] for r in rows} == {"F", "D"}
    assert index[0]["rows"] == 2
    (tmp_path / "verify" / "corrections_1.jsonl").write_text(
        json.dumps({"item_id": tag["item_id"], "code": "B", "recoverable_by": "none"}) + "\n"
    )
    merged = recoverability.load_verdicts(verdicts_dir, tmp_path / "verify")
    assert merged[tag["item_id"]]["code"] == "B" and merged[tag["item_id"]]["verified"] is True
    assert merged[clean["item_id"]]["code"] == "D"


def test_packet_coverage_names_the_resume_worklist(items, tmp_path):
    recoverability.write_packets(items, tmp_path, packet_size=1000)
    packet = next((tmp_path / "packets").glob("*.jsonl"))
    sent = [json.loads(l)["item_id"] for l in packet.read_text().splitlines()]
    verdicts_dir = tmp_path / "verdicts"
    verdicts_dir.mkdir()
    (verdicts_dir / packet.name).write_text(
        json.dumps({"item_id": sent[0], "code": "D"}) + "\n"
        + json.dumps({"item_id": sent[0], "code": "D"}) + "\n"
        + "{broken\n"
    )
    report = recoverability.packet_coverage(tmp_path)
    row = next(r for r in report if r["packet"] == packet.name)
    assert row["sent"] == len(sent) and row["judged"] == 1 and row["duplicates"] == 1
    assert row["missing"] == len(sent) - 1 and row["malformed_lines"] == 1 and row["status"] == "partial"
    assert all(r["status"] == "not_started" for r in report if r["packet"] != packet.name)
