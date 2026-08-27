"""Unit half: the harness's own logic, no dumps required (runs in default CI)."""

from __future__ import annotations

from typing import Any

from checks import ledger, lints, loading, mechanical


def _record(**overrides) -> loading.SynthRecord:
    base: dict[str, Any] = dict(
        subject="s_com",
        field="products",
        chunk_bounds="0:100",
        group_id="gaaaaaaa",
        focal_form="L Series Doors",
        synthesis="The manufacturer offers L Series doors.",
    )
    base.update(overrides)
    return loading.SynthRecord(**base)


def test_content_key_changes_with_every_input() -> None:
    base = ledger.content_key("g1", "text", "ev", "pv", "rubric")
    assert base != ledger.content_key("g2", "text", "ev", "pv", "rubric")
    assert base != ledger.content_key("g1", "TEXT", "ev", "pv", "rubric")
    assert base != ledger.content_key("g1", "text", "ev2", "pv", "rubric")
    assert base != ledger.content_key("g1", "text", "ev", "pv2", "rubric")
    assert base != ledger.content_key("g1", "text", "ev", "pv", "rubric2")
    assert base == ledger.content_key("g1", "text", "ev", "pv", "rubric")


def test_identical_clusters_fire_within_one_request_only() -> None:
    rows = [
        ("g1", "same text", "req_a"),
        ("g2", "same text", "req_a"),
        ("g3", "same text", "req_b"),  # other request: no cluster partner
        ("g4", "other text", "req_a"),
        ("g5", None, "req_a"),
    ]
    clusters = lints.identical_synthesis_clusters(rows)
    assert len(clusters) == 1
    assert clusters[0]["group_ids"] == ["g1", "g2"]


def test_contract_byte_copy_detects_divergence() -> None:
    products = [_record()]
    contract_same = [_record(field="contract_products")]
    contract_diff = [
        _record(field="contract_products", synthesis="Different paragraph.")
    ]
    assert (
        mechanical.contract_products_invariant(products, contract_same)["status"]
        == "pass"
    )
    assert (
        mechanical.contract_products_invariant(products, contract_diff)["status"]
        == "FAIL"
    )


def test_empty_group_invariant_flags_synthesized_no_mentions() -> None:
    dump: dict = {"chunks": {}, "run": {}}
    bad = _record(status="no_mentions")
    results = mechanical.run_invariants("s_com", "products", dump, [bad])
    inv4 = next(r for r in results if r["id"] == "INV-4-empty-groups")
    assert inv4["status"] == "FAIL"


def test_enumerators_nominate_without_judging() -> None:
    records = [
        _record(group_id="g1"),
        _record(group_id="g1", chunk_bounds="100:200"),  # twin
        _record(group_id="g2", focal_form="Lead", forms=["Lead"]),
        _record(
            group_id="g3",
            synthesis="The manufacturer does not offer factory paint.",
        ),
    ]
    out = mechanical.enumerate_candidates(records, None, [])
    assert {"0:100:g1", "100:200:g1"} <= set(out["twins"])
    assert any(tag.endswith("g2") for tag in out["polyseme"])
    assert any(tag.endswith("g3") for tag in out["negation"])


def test_pair_key_carries_chunk_bounds() -> None:
    twin_a = _record(chunk_bounds="0:100")
    twin_b = _record(chunk_bounds="100:200")
    assert twin_a.pair_key != twin_b.pair_key


def test_pv_and_ud_parse_from_custom_id() -> None:
    cid = (
        "a.com>products>llm_phrase_synthesis>chunk>0:9>group>0>gpt-4.1"
        "|max_completion_tokens=20000|pv=VERSIONID|gs=50|loc=1|ud=abc123"
    )
    assert loading.pv_of_custom_id(cid) == "VERSIONID"
    assert loading.ud_of_custom_id(cid) == "abc123"


def test_append_verdicts_is_idempotent(tmp_path, monkeypatch) -> None:
    """Re-ingesting the same verdict file must not duplicate ledger rows.

    Regression: after the 2026-08-26 layout change the ledger was re-keyed but
    the source verdict files were not, so a second --finalize appended 546
    stale-key duplicates. They never matched a lookup (results were unaffected)
    but they doubled the ledger.
    """
    monkeypatch.setattr(ledger, "LEDGER_ROOT", tmp_path)
    row = {
        "content_key": "abc123",
        "subject": "s_com",
        "field": "products",
        "group_id": "g1",
        "chunk_bounds": "0:100",
        "taxonomy_version": "tax1",
        "checks": {"J1": {"verdict": "pass"}},
    }
    assert ledger.append_verdicts("s_com", "products", [row]) == 1
    assert ledger.append_verdicts("s_com", "products", [row]) == 0
    assert len(ledger.load_ledger("s_com", "products")) == 1
