"""Unit half: the harness's own logic, no dumps required (runs in default CI)."""

from __future__ import annotations

from typing import Any

from checks import designations, ledger, lints, loading, mechanical, pull


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
        _record(group_id="g4", retried=True),
    ]
    out = mechanical.enumerate_candidates(records, None, [])
    assert {"0:100:g1", "100:200:g1"} <= set(out["twins"])
    assert any(tag.endswith("g2") for tag in out["polyseme"])
    assert any(tag.endswith("g3") for tag in out["negation"])
    assert out["retried"] == ["0:100:g4"]


def test_enumerators_read_snippets_and_code_locations() -> None:
    record = _record(group_id="g1")
    index = {
        "s_com|products|0:100|g1": {
            "snippets": ["L Series doors are listed in the catalog PDF."],
            "request_custom_id": "req",
        }
    }
    locations = {("0:100", "g1"): ["## More from Allegion"]}
    out = mechanical.enumerate_candidates(
        [record], index, ["Allegion"], locations=locations
    )
    assert out["thin_single_snippet"] == ["0:100:g1"]
    assert out["third_party_evidence"] == ["0:100:g1"]  # matched via the location
    assert out["document_listing"] == ["0:100:g1"]


def test_pair_key_carries_chunk_bounds() -> None:
    twin_a = _record(chunk_bounds="0:100")
    twin_b = _record(chunk_bounds="100:200")
    assert twin_a.pair_key != twin_b.pair_key


def test_pv_and_ud_parse_from_custom_id() -> None:
    cid = (
        "a.com>products>llm_phrase_synthesis>chunk>0:9>group>0>gpt-4.1"
        "|max_completion_tokens=10000|pv=VERSIONID|gs=50|ud=abc123"
    )
    assert loading.pv_of_custom_id(cid) == "VERSIONID"
    assert loading.ud_of_custom_id(cid) == "abc123"


def test_iter_records_reads_snippet_counts_and_retry_flag() -> None:
    dump = {
        "chunks": {
            "0:100": {
                "synthesis": {
                    "summary": {},
                    "records": [
                        {
                            "group_id": "g1",
                            "focal_form": "x",
                            "snippets": 3,
                            "retried": True,
                            "status": "synthesized",
                            "synthesis": "text",
                        },
                        {  # a dump from before the wire port
                            "group_id": "g2",
                            "focal_form": "y",
                            "entries": 2,
                            "status": "synthesized",
                            "synthesis": "text",
                        },
                    ],
                }
            }
        }
    }
    records = list(loading.iter_records("s_com", "products", dump))
    assert [(r.group_id, r.snippets, r.retried) for r in records] == [
        ("g1", 3, True),
        ("g2", 2, None),
    ]


def test_address_keeps_the_dumps_subject_spelling() -> None:
    """anchor-mfg.com dumps as anchor-mfg_com; the old parser wrote
    anchor_mfg_com and silently lost every evidence lookup for the subject."""
    cid = (
        "anchor-mfg.com>products>llm_phrase_synthesis>chunk>0:41177>retry>1>group>0>"
        "gpt-4.1|max_completion_tokens=10000|pv=P|gs=50|ud=u"
    )
    assert pull._address_of(cid) == ("anchor-mfg_com", "products", "0:41177")
    assert pull.retry_index(cid) == 1
    assert pull.retry_index(cid.replace(">retry>1", "")) == 0
    assert loading.safe_subject("anchor-mfg.com") == "anchor-mfg_com"


def test_split_user_message_isolates_chunk_text_name_and_records() -> None:
    message = (
        "request nonce (ignore): abc\n\n"
        "text scraped from a manufacturer's website:\n"
        "##################################################\n"
        "https://a.com/\n\n# Heading\n\nBody line.\n\n\n"
        "the name of the manufacturer in question: Acme Co\n\n"
        '<<<RECORD_IDS\n["g1"]\nRECORD_IDS>>>\n\n'
        '<<<RECORDS\n[{"record_id":"g1",\n"focal_form":"widget",\n'
        '"snippets":["Body line."]}]\nRECORDS>>>'
    )
    parts = pull.split_user_message(message)
    assert parts["subject_name"] == "Acme Co"
    assert parts["chunk_text"].startswith("####")
    assert parts["chunk_text"].endswith("Body line.")
    assert parts["tail"].startswith("the name of the manufacturer in question")
    records = pull.parse_wire_records(parts["tail"])
    assert records == [{"record_id": "g1", "focal_form": "widget", "snippets": ["Body line."]}]
    assert pull.evidence_digest(records[0]["snippets"]) == pull.evidence_digest(["Body line."])


def test_fold_locations_and_coverage_read_the_fold_block() -> None:
    dump = {
        "chunks": {
            "0:100": {
                "fold": {
                    "summary": {"mentions": 4, "mentions_located": 3},
                    "groups": [
                        {
                            "group_id": "g1",
                            "mentions": [
                                {"location": "## A"},
                                {"location": "## A"},
                                {"location": None},
                                {"location": "| Model | Size |"},
                            ],
                        }
                    ],
                }
            }
        }
    }
    assert loading.fold_locations(dump) == {("0:100", "g1"): ["## A", "| Model | Size |"]}
    coverage = loading.fold_location_summary(dump)
    assert (coverage["mentions"], coverage["located"], coverage["coverage"]) == (4, 3, 0.75)


def test_dropped_designations_use_cores_conservation_check() -> None:
    """The harness recomputes the pipeline's OWN designation check (core's
    T1/T2 tiers, word-boundary, case-insensitive, elastic pair spacing)."""
    snippets = ["Certified to ISO 9001:2015.", "Runs a Chevalier EM2040L and an FM-3VK."]
    synthesis = "The shop is ISO  9001 certified and runs a Chevalier em2040l, among others."
    detail = designations.dropped_designations(snippets, synthesis)
    assert "FM-3VK" in detail["dropped"]
    assert "EM2040L" not in detail["dropped"]
    assert "ISO 9001" not in detail["dropped"]
    assert detail["collapse_phrases"] == ["among others"]
    # No designations in the evidence: nothing to drop, no collapse nomination.
    assert designations.dropped_designations(["plain prose"], "various models etc.") == {
        "tokens": [],
        "dropped": [],
        "collapse_phrases": [],
    }


def test_designation_versions_are_stamped() -> None:
    assert "designation_tokens" in lints.lint_versions()


def test_designation_report_and_enumerator_agree() -> None:
    record = _record(group_id="g1", synthesis="The manufacturer offers the L100 model.")
    index = {
        "s_com|products|0:100|g1": {
            "snippets": ["Models L100 and L200 are offered."],
            "request_custom_id": "req",
        }
    }
    report = designations.report([record], index, pull.evidence_for)
    assert report["summary"]["tokens"] == 2
    assert report["summary"]["tokens_preserved"] == 1
    assert report["per_record"][record.pair_key]["dropped"] == ["L200"]
    out = mechanical.enumerate_candidates([record], index, [], designation_report=report)
    assert out["designation_dropped"] == ["0:100:g1"]


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
