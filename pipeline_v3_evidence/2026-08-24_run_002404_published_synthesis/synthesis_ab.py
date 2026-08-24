"""Synthesis-stage analysis of run 20260824T002404 (the six statics as PUBLISHED —
own-name ban retired, party-preservation rule added) against 20260823T200044
(the statics as they ran before that edit), same subjects, same fold, same arm.

Both runs replay the SAME mention state (every request's `ud=` digest is
identical), so every difference below is the prompt and nothing else.

Metric definitions are lifted verbatim from
pipeline_v3_evidence/2026-08-23_run_200044_synthesis/synthesis_ab.py so the two
write-ups are comparable line for line.

Run from the repo root:  python3 pipeline_v3_evidence/2026-08-24_run_002404_published_synthesis/synthesis_ab.py
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
OLD, NEW = "20260823T200044", "20260824T002404"

# products and contract_products SHARE every phrase stage through synthesis
# (ContractProductSynthesisNode mints the products custom_id), so the two dumps
# hold the same rows. Skipping contract_products counts each request once.
SHARED_DUPLICATE = "contract_products"

OWN_NAME = {"steelcraft_com": "Steelcraft", "alecmfg_com": "Alec"}

ACTIVE_DEALING = re.compile(
    r"\b(the manufacturer|manufacturer's)\b[^.]{0,80}?"
    r"\b(offers?|produces?|provides?|manufactures?|supplies|performs?|uses?|makes?|serves?|carries)\b",
    re.I,
)
# the same clause, but the manufacturer is now named rather than masked, so the
# subject of the sentence is the company name and not the phrase "the manufacturer"
NAMED_DEALING = re.compile(
    r"\b(Steelcraft|Alec Model|the manufacturer|manufacturer's)\b[^.]{0,80}?"
    r"\b(offers?|produces?|provides?|manufactures?|supplies|performs?|uses?|makes?|serves?|carries)\b",
    re.I,
)
FLOOR = re.compile(
    r"do(es)? not (refer|pertain|relate|bear|indicate|show|describe)|unrelated sense"
    r"|metaphoric|figure of speech|no dealing|in an unrelated",
    re.I,
)
OTHER_PARTY = re.compile(
    r"\b(a customer|customer's|client's|the client|a supplier|third[- ]party|another party"
    r"|not the manufacturer)\b",
    re.I,
)
SERIES = re.compile(r"\b([A-Z][A-Za-z0-9&]{0,12}) Series\b")

# the party-preservation rule's two direct test cases (Allegion, not Steelcraft,
# is what the "More from Allegion" block belongs to)
PARTY_TARGETS = {"LEED Credits", "CalGreen Building Standards"}


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


def norm(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (text or "").lower()).strip()


def section(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


# --- 1. the A/B is clean ---------------------------------------------------------------

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
    prompts = sorted(
        meta["prompt_version_id"][:10]
        for meta in {
            doc["run"]["extraction_metadata"]["llm_phrase_synthesis"]["prompt_version_id"]: doc["run"][
                "extraction_metadata"
            ]["llm_phrase_synthesis"]
            for _, _, doc in dumps_of(run)
            if doc["run"]["extraction_metadata"].get("llm_phrase_synthesis")
        }.values()
    )
    print(f"{run}: {len(ids)} synthesis requests | prompt version ids {prompts}")
old_ids, new_ids = store[OLD], store[NEW]
print(f"same request-id set once pv= is stripped: {set(old_ids) == set(new_ids)}")
print(f"requests whose input digest (ud=) changed: {sum(1 for k in old_ids if old_ids[k] != new_ids.get(k))}")


# --- 2. delivery ------------------------------------------------------------------------

section("2. Delivery")
for run in (OLD, NEW):
    total = Counter()
    for _, _, doc in dumps_of(run):
        for _, _, block in synthesis_chunks(doc):
            for key, value in block["summary"].items():
                total[key] += value if isinstance(value, int) else len(value)
    print(
        f"{run}: records={total['records']} synthesized={total['synthesized']} "
        f"not_synthesized={total['not_synthesized']} retried={total['retried']} "
        f"retry_requests={total['retry_requests']} unknown_answer_ids={total['unknown_answer_ids']}"
    )


# --- 3. cost ----------------------------------------------------------------------------

section("3. Cost and headroom (unique requests only)")
for run in (OLD, NEW):
    seen, ins, outs, latency = set(), [], [], []
    for _, _, doc in dumps_of(run, include_shared_duplicate=True):
        for _, chunk, _ in synthesis_chunks(doc):
            for request in chunk["requests"]["llm_phrase_synthesis"]:
                if request["custom_id"] in seen:
                    continue
                seen.add(request["custom_id"])
                ins.append(request["input_tokens"])
                outs.append(request["output_tokens"])
                if request.get("openai_processing_ms"):
                    latency.append(request["openai_processing_ms"] / 1000)
    cost = sum(ins) / 1e6 * 2.0 + sum(outs) / 1e6 * 8.0
    print(
        f"{run}: {len(ins)} requests  in={sum(ins):,}  out={sum(outs):,}  "
        f"max out/req={max(outs)} (cap 20,000)  mean latency={statistics.mean(latency):.1f}s  "
        f"est. gpt-4.1 ${cost:.2f}"
    )


# --- 4. verbosity -----------------------------------------------------------------------

section("4. Verbosity — did output fall once the read-back check was removed?")
lengths_by_run = {}
for run in (OLD, NEW):
    lengths, by_entries = [], {}
    for _, _, doc in dumps_of(run):
        for _, _, block in synthesis_chunks(doc):
            for row in block["records"]:
                lengths.append(len(row["synthesis"] or ""))
                by_entries.setdefault(min(row["entries"], 10), []).append(len(row["synthesis"] or ""))
    lengths_by_run[run] = lengths
    print(
        f"{run}: n={len(lengths)} mean={statistics.mean(lengths):.0f} "
        f"median={statistics.median(lengths):.0f} max={max(lengths)} total={sum(lengths):,} chars"
    )
    print("   mean chars by entry count: " + "  ".join(
        f"{k}{'+' if k == 10 else ''}:{statistics.mean(v):.0f}" for k, v in sorted(by_entries.items())
    ))
old_l, new_l = lengths_by_run[OLD], lengths_by_run[NEW]
print(
    f"   -> mean {(statistics.mean(new_l) - statistics.mean(old_l)) / statistics.mean(old_l):+.1%}, "
    f"total {(sum(new_l) - sum(old_l)) / sum(old_l):+.1%}"
)


# --- 5. the own-name counter now measures identification, not violation -------------------

section("5. Own-name: the counter changed meaning when the ban was retired")
for run in (OLD, NEW):
    for subject in ("alecmfg_com", "steelcraft_com"):
        name = OWN_NAME[subject]
        hits = focal_caused = rows_with = rows_total = 0
        context = Counter()
        for subj, field, doc in dumps_of(run):
            if subj != subject:
                continue
            for _, _, block in synthesis_chunks(doc):
                for row in block["records"]:
                    rows_total += 1
                    text = row["synthesis"] or ""
                    n = len(re.findall(rf"(?<![A-Za-z0-9]){name}(?![A-Za-z0-9])", text, re.I))
                    if not n:
                        continue
                    hits += n
                    rows_with += 1
                    if re.search(name, row["focal_form"] or "", re.I):
                        focal_caused += n
                    for m in re.finditer(
                        rf"(?<![A-Za-z0-9]){name}(?![A-Za-z0-9])(\s+\w+)?", text, re.I
                    ):
                        context[m.group(0)] += 1
        print(
            f"{run} {subject}: hits={hits}  records naming it={rows_with}/{rows_total} "
            f"({rows_with / rows_total:.0%})  of which the focal_form itself carries the name={focal_caused}"
        )
        print("     top contexts: " + ", ".join(f"{v}x {k!r}" for k, v in context.most_common(4)))


# --- 6. the dealing rule ------------------------------------------------------------------

section("6. The dealing rule, on heading-only evidence (1 entry, snippet <= 60 chars)")
for run in (OLD, NEW):
    total = asserts_masked = asserts_named = 0
    for _, _, doc in dumps_of(run):
        for _, chunk, block in synthesis_chunks(doc):
            fold = {g["group_id"]: g for g in chunk["fold"]["groups"]}
            for row in block["records"]:
                if row["entries"] != 1:
                    continue
                group = fold.get(row["group_id"])
                if not group or len(group["mentions"][0].get("snippet") or "") > 60:
                    continue
                total += 1
                asserts_masked += bool(ACTIVE_DEALING.search(row["synthesis"] or ""))
                asserts_named += bool(NAMED_DEALING.search(row["synthesis"] or ""))
    print(
        f"{run}: {total} such records; asserts a dealing "
        f"masked-subject only {asserts_masked} ({asserts_masked / total:.0%}), "
        f"subject named or masked {asserts_named} ({asserts_named / total:.0%})"
    )

section("6b. Attribution and the set-aside floor (all records)")
for run in (OLD, NEW):
    n = party = floor = 0
    for _, _, doc in dumps_of(run):
        for _, _, block in synthesis_chunks(doc):
            for row in block["records"]:
                text = row["synthesis"] or ""
                if not text:
                    continue
                n += 1
                party += bool(OTHER_PARTY.search(text))
                floor += bool(FLOOR.search(text))
    print(
        f"{run}: n={n}  attributes to a non-manufacturer party {party} ({party / n:.1%})  "
        f"set-aside/no-dealing floor {floor} ({floor / n:.1%})"
    )

section("6c. Party preservation — the two Allegion records it was written for")
for run in (OLD, NEW):
    for subj, field, doc in dumps_of(run):
        if field != "conformity_attestations":
            continue
        for _, _, block in synthesis_chunks(doc):
            for row in block["records"]:
                if row["focal_form"] in PARTY_TARGETS:
                    print(f"\n{run} [{subj}] focal={row['focal_form']!r}")
                    print(f"   {row['synthesis']}")
print()
for run in (OLD, NEW):
    named = 0
    for _, _, doc in dumps_of(run):
        for _, _, block in synthesis_chunks(doc):
            for row in block["records"]:
                named += bool(re.search(r"\bAllegion\b", row["synthesis"] or "", re.I))
    print(f"{run}: records naming Allegion (the real owning party) = {named}")


# --- 7. fidelity ---------------------------------------------------------------------------

section("7. Fidelity: did the synthesis describe the entity it was asked about?")
print("naive check (focal form must appear verbatim, punctuation collapsed):")
for run in (OLD, NEW):
    drift = 0
    for _, _, doc in dumps_of(run):
        for _, _, block in synthesis_chunks(doc):
            for row in block["records"]:
                focal = norm(row["focal_form"])
                if focal and focal not in norm(row["synthesis"] or ""):
                    drift += 1
    print(f"  {run}: focal_form absent from its own synthesis: {drift}")

print("\nshipped lint (entity-shaped focal forms only — core.utils.focal_form_lint):")
flagged_rows = {}
for run in (OLD, NEW):
    shaped = 0
    flags = []
    for subj, field, doc in dumps_of(run):
        for _, _, block in synthesis_chunks(doc):
            for row in block["records"]:
                if is_entity_shaped(row["focal_form"] or ""):
                    shaped += 1
                if focal_form_absent(
                    row["synthesis"] or "", row["focal_form"] or "", row.get("forms") or []
                ):
                    flags.append((subj, field, row))
    flagged_rows[run] = flags
    print(f"  {run}: {shaped} entity-shaped records, {len(flags)} flagged")

print("\nevery flagged row in the new run, classified:")
CONNECTOR = re.compile(r"[&,]")
for subj, field, row in flagged_rows[NEW]:
    focal = row["focal_form"] or ""
    # does the form match once '&'/',' are read as the word 'and'?
    relaxed = norm(re.sub(r"\s*&\s*", " and ", focal))
    cause = (
        "connector (& or , written as 'and')"
        if relaxed in norm(row["synthesis"] or "") or CONNECTOR.search(focal)
        else "re-ordered or split by inserted words"
    )
    print(f"  [{subj}/{field}] focal={focal!r} entries={row['entries']} -> {cause}")
    print(f"      {(row['synthesis'] or '')[:200]}")

section("7b. The FE->DE swap — the one real defect the lint was built to catch")
for run in (OLD, NEW):
    for subj, field, doc in dumps_of(run):
        for _, _, block in synthesis_chunks(doc):
            for row in block["records"]:
                if row["focal_form"] == "FE Series Double-Egress Frames":
                    print(f"\n{run} [{subj}/{field}] group={row['group_id']} entries={row['entries']}")
                    print(f"   {(row['synthesis'] or '')[:320]}")

section("7c. Unsupported '<X> Series' names (entity invented or swapped)")
for run in (OLD, NEW):
    found = []
    for subj, field, doc in dumps_of(run):
        if subj != "steelcraft_com":
            continue
        for _, chunk, block in synthesis_chunks(doc):
            fold = {g["group_id"]: g for g in chunk["fold"]["groups"]}
            for row in block["records"]:
                text = row["synthesis"] or ""
                group = fold.get(row["group_id"])
                evidence = " ".join(
                    (m.get("snippet") or "") + " " + (m.get("location") or "")
                    for m in (group["mentions"] if group else [])
                ) + " " + (row["focal_form"] or "")
                supported = {m.group(1).lower() for m in SERIES.finditer(evidence)}
                for m in SERIES.finditer(text):
                    if m.group(1).lower() not in supported:
                        found.append((field, row["focal_form"], m.group(0)))
    real = [f for f in found if not f[2].startswith("Paladin")]
    print(f"  {run}: {len(found)} flagged, {len(real)} after removing the (TM)-stripping false positives")
    for f in real:
        print(f"     {f[0]} | focal={f[1]!r} -> wrote {f[2]!r}")


# --- 8. the set-aside floor on document listings -------------------------------------------
#
# Found by sampling the records that newly assert a dealing: most of the 45% -> 64% shift in
# section 6 is register (an active sentence with a named subject where the old prompt wrote a
# passive one the regex missed), but ONE class really did change verdict — a phrase whose only
# evidence is a document title in a Downloads / Literature list.

section("8. Records whose only evidence is a document title in a downloads/literature list")

# widened past section 6b's FLOOR to catch the two phrasings this class actually uses
DOC_FLOOR = re.compile(
    r"do(es)? not (refer|pertain|relate|bear|indicate|show|describe)|unrelated sense"
    r"|metaphoric|figure of speech|no dealing|in an unrelated|does not show any|no further details",
    re.I,
)
DOCUMENT = re.compile(
    r"downloadable|literature resource|data sheet|brochure|catalog|\.pdf|downloads section", re.I
)


def records_by_key(run):
    out = {}
    for subj, field, doc in dumps_of(run):
        for _, chunk, block in synthesis_chunks(doc):
            fold = {g["group_id"]: g for g in chunk["fold"]["groups"]}
            for row in block["records"]:
                group = fold.get(row["group_id"])
                evidence = " ".join(
                    (m.get("snippet") or "") + " " + (m.get("location") or "")
                    for m in (group["mentions"] if group else [])
                )
                out[(subj, field, row["group_id"])] = (row, evidence)
    return out


old_rows, new_rows = records_by_key(OLD), records_by_key(NEW)
common = [k for k in new_rows if k in old_rows]
lost = [
    k for k in common
    if DOC_FLOOR.search(old_rows[k][0]["synthesis"] or "")
    and not DOC_FLOOR.search(new_rows[k][0]["synthesis"] or "")
]
gained = [
    k for k in common
    if not DOC_FLOOR.search(old_rows[k][0]["synthesis"] or "")
    and DOC_FLOOR.search(new_rows[k][0]["synthesis"] or "")
]
print(f"records compared: {len(common)}")
print(f"floor fired in OLD but not in NEW: {len(lost)}   |   NEW but not OLD: {len(gained)}")

document_records = [k for k in common if DOCUMENT.search(new_rows[k][1])]
old_floor = sum(1 for k in document_records if DOC_FLOOR.search(old_rows[k][0]["synthesis"] or ""))
new_floor = sum(1 for k in document_records if DOC_FLOOR.search(new_rows[k][0]["synthesis"] or ""))
print(
    f"document-listing records: {len(document_records)}  "
    f"floor fired OLD={old_floor}  NEW={new_floor}"
)
print("\nthe ones that changed verdict (evidence is a document listing):")
for k in [k for k in lost if DOCUMENT.search(old_rows[k][1])]:
    print(f"\n  {k[0]}/{k[1]} focal={new_rows[k][0]['focal_form']!r}")
    print(f"    OLD: {(old_rows[k][0]['synthesis'] or '')[:190]}")
    print(f"    NEW: {(new_rows[k][0]['synthesis'] or '')[:190]}")
