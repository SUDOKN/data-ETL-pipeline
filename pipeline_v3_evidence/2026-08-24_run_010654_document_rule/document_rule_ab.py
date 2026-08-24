"""Synthesis-stage analysis of run 20260824T010654 (the document-listing rule)
against 20260824T002404 (the same statics without it), same subjects, same fold,
same arm.

Both runs replay the SAME mention state (every request's `ud=` digest is
identical), so every difference below is the prompt and nothing else.

Run from the repo root:
    python3 pipeline_v3_evidence/2026-08-24_run_010654_document_rule/document_rule_ab.py
"""

from __future__ import annotations

import glob
import json
import os
import re
import statistics
import sys
from collections import Counter

sys.path.insert(0, "packages/core/src")
from core.utils.focal_form_lint import focal_form_absent, is_entity_shaped

DUMPS = "packages/logs/extraction_dumps"
OLD, NEW = "20260824T002404", "20260824T010654"
SHARED_DUPLICATE = "contract_products"

# Evidence that the record's only support is a title in a downloads/literature list.
DOCUMENT = re.compile(
    r"downloadable|literature|brochure|data sheet|sell sheet|stock sheet|catalog|downloads", re.I
)
# What the rule ASKED for: the claim scoped to the document, not to the thing it names.
SCOPED = re.compile(
    r"offers? a document|document titled|a document bearing|no dealing with it beyond"
    r"|beyond a document|offering a document|only a document",
    re.I,
)
# What the rule was written to remove: an unscoped dealing with the entity itself.
UNSCOPED = re.compile(
    r"provides? this (resource|data sheet|brochure|portfolio|nomenclature)"
    r"|provides? (a|the) (data sheet|brochure|resource)",
    re.I,
)


def dumps_of(run: str, include_shared_duplicate: bool = False):
    for path in sorted(glob.glob(f"{DUMPS}/{run}/*.json")):
        base = os.path.basename(path)
        subject, field = base.split("__")[0], base.split("__")[1]
        if field == SHARED_DUPLICATE and not include_shared_duplicate:
            continue
        yield subject, field, json.load(open(path))


def synthesis_chunks(doc):
    for bounds, chunk in doc.get("chunks", {}).items():
        block = chunk.get("synthesis")
        if block:
            yield bounds, chunk, block


def rows(run):
    out = {}
    for subject, field, doc in dumps_of(run):
        for _, chunk, block in synthesis_chunks(doc):
            fold = {g["group_id"]: g for g in chunk["fold"]["groups"]}
            for row in block["records"]:
                group = fold.get(row["group_id"])
                evidence = " ".join(
                    (m.get("snippet") or "") + " || " + (m.get("location") or "")
                    for m in (group["mentions"] if group else [])
                )
                out[(subject, field, row["group_id"])] = (row, evidence)
    return out


def section(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


section("1. Same inputs, different prompt")
store = {}
for run in (OLD, NEW):
    ids = {}
    for _, _, doc in dumps_of(run):
        for _, chunk, _ in synthesis_chunks(doc):
            for request in chunk["requests"]["llm_phrase_synthesis"]:
                key = re.sub(r"\|pv=[^|]*", "", request["custom_id"])
                ids[key] = re.search(r"ud=([0-9a-f]+)", request["custom_id"]).group(1)
    store[run] = ids
    versions = sorted(
        {
            doc["run"]["extraction_metadata"]["llm_phrase_synthesis"]["prompt_version_id"][:10]
            for _, _, doc in dumps_of(run)
            if doc["run"]["extraction_metadata"].get("llm_phrase_synthesis")
        }
    )
    print(f"{run}: {len(ids)} synthesis requests | prompt version ids {versions}")
print(f"same request-id set once pv= is stripped: {set(store[OLD]) == set(store[NEW])}")
print(
    "requests whose input digest (ud=) changed: "
    f"{sum(1 for k in store[OLD] if store[OLD][k] != store[NEW].get(k))}"
)


section("2. Delivery and cost")
for run in (OLD, NEW):
    total = Counter()
    for _, _, doc in dumps_of(run):
        for _, _, block in synthesis_chunks(doc):
            for key, value in block["summary"].items():
                total[key] += value if isinstance(value, int) else len(value)
    seen, ins, outs = set(), [], []
    for _, _, doc in dumps_of(run, include_shared_duplicate=True):
        for _, chunk, _ in synthesis_chunks(doc):
            for request in chunk["requests"]["llm_phrase_synthesis"]:
                if request["custom_id"] in seen:
                    continue
                seen.add(request["custom_id"])
                ins.append(request["input_tokens"])
                outs.append(request["output_tokens"])
    print(
        f"{run}: records={total['records']} synthesized={total['synthesized']} "
        f"not_synthesized={total['not_synthesized']} retried={total['retried']} "
        f"unknown={total['unknown_answer_ids']} | in={sum(ins):,} out={sum(outs):,} "
        f"est. ${sum(ins) / 1e6 * 2 + sum(outs) / 1e6 * 8:.2f}"
    )


OLD_ROWS, NEW_ROWS = rows(OLD), rows(NEW)
common = [k for k in NEW_ROWS if k in OLD_ROWS]

section("3. Did the rule land? (records whose evidence is a document listing)")
document_records = [k for k in common if DOCUMENT.search(OLD_ROWS[k][1])]
print(f"records whose evidence is a document listing: {len(document_records)}")
for run, table in ((OLD, OLD_ROWS), (NEW, NEW_ROWS)):
    scoped = sum(1 for k in document_records if SCOPED.search(table[k][0]["synthesis"] or ""))
    unscoped = sum(1 for k in document_records if UNSCOPED.search(table[k][0]["synthesis"] or ""))
    print(
        f"  {run}: scoped to the document {scoped} ({scoped / len(document_records):.0%})"
        f"   unscoped 'provides this resource' {unscoped} ({unscoped / len(document_records):.0%})"
    )

section("4. The overreach check — conformity_attestations")
WITHHELD = re.compile(
    r"no dealing|does not show|does not describe any|beyond a document|offers? a document"
    r"|document titled",
    re.I,
)
for run, table in ((OLD, OLD_ROWS), (NEW, NEW_ROWS)):
    attestations = [k for k in common if k[1] == "conformity_attestations"]
    withheld = [k for k in attestations if WITHHELD.search(table[k][0]["synthesis"] or "")]
    print(f"{run}: {len(attestations)} attestation records, {len(withheld)} withholding a dealing")
newly = [
    k
    for k in common
    if k[1] == "conformity_attestations"
    and WITHHELD.search(NEW_ROWS[k][0]["synthesis"] or "")
    and not WITHHELD.search(OLD_ROWS[k][0]["synthesis"] or "")
]
print(f"newly withholding: {len(newly)}")
for k in newly:
    print(f"\n  {k[0]} focal={NEW_ROWS[k][0]['focal_form']!r}")
    print(f"    evidence: {OLD_ROWS[k][1][:160]}")
    print(f"    OLD: {(OLD_ROWS[k][0]['synthesis'] or '')[:200]}")
    print(f"    NEW: {(NEW_ROWS[k][0]['synthesis'] or '')[:200]}")

section("5. Verbosity")
for run, table in ((OLD, OLD_ROWS), (NEW, NEW_ROWS)):
    lengths = [len(r["synthesis"] or "") for r, _ in table.values()]
    print(
        f"{run}: n={len(lengths)} mean={statistics.mean(lengths):.0f} "
        f"median={statistics.median(lengths):.0f} total={sum(lengths):,} chars"
    )

section("6. Focal-form lint, applied uniformly to both runs")
# The dump's own counter used a different version of the lint in each run, so it
# cannot be compared across the two; recompute both with the shipped code.
for run in (OLD, NEW):
    shaped, flags = 0, []
    for subject, field, doc in dumps_of(run):
        for _, _, block in synthesis_chunks(doc):
            for row in block["records"]:
                if is_entity_shaped(row["focal_form"] or ""):
                    shaped += 1
                if focal_form_absent(
                    row["synthesis"] or "", row["focal_form"] or "", row.get("forms") or []
                ):
                    flags.append((field, row["focal_form"]))
    print(f"{run}: {shaped} entity-shaped records, {len(flags)} flagged {flags}")
