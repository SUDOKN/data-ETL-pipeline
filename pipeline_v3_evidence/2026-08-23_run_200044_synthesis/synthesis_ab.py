"""Synthesis-stage analysis of run 20260823T200044 (new statics) against
20260823T195031 (previous statics), same subjects, same fold, same arm.

Both runs replay the SAME mention state (every request's `ud=` digest is
identical), so every difference below is the prompt and nothing else.

Run from the repo root:  python3 pipeline_v3_evidence/2026-08-23_run_200044_synthesis/synthesis_ab.py
"""

from __future__ import annotations

import glob
import json
import os
import re
import statistics
from collections import Counter

DUMPS = "packages/logs/extraction_dumps"
OLD, NEW = "20260823T195031", "20260823T200044"

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
for run in (OLD, NEW):
    ids = {}
    for _, _, doc in dumps_of(run):
        for _, chunk, _ in synthesis_chunks(doc):
            for request in chunk["requests"]["llm_phrase_synthesis"]:
                key = re.sub(r"\|pv=[^|]*", "", request["custom_id"])
                ids[key] = re.search(r"ud=([0-9a-f]+)", request["custom_id"]).group(1)
    globals()[f"_ids_{run}"] = ids
    prompts = {
        meta["prompt_version_id"][:8]
        for _, _, doc in dumps_of(run)
        if (meta := doc["run"]["extraction_metadata"].get("llm_phrase_synthesis"))
    }
    print(f"{run}: {len(ids)} synthesis requests, {len(prompts)} distinct prompt versions")
old_ids, new_ids = globals()[f"_ids_{OLD}"], globals()[f"_ids_{NEW}"]
print(f"same request-id set: {set(old_ids) == set(new_ids)}")
print(f"requests whose input digest (ud=) changed: {sum(1 for k in old_ids if old_ids[k] != new_ids.get(k))}")


# --- 2. delivery ------------------------------------------------------------------------

section("2. Delivery (both runs)")
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

section("2b. Empty groups never reach synthesis")
groups = empty = records = mismatch = 0
for _, _, doc in dumps_of(NEW, include_shared_duplicate=True):
    for _, chunk, block in synthesis_chunks(doc):
        fold = chunk["fold"]["summary"]
        groups += fold["groups"]
        empty += fold["empty_groups"]
        records += block["summary"]["records"]
        mismatch += fold["groups"] - fold["empty_groups"] != block["summary"]["records"]
print(f"groups={groups} empty={empty} -> expected {groups - empty}, actual {records}, mismatches={mismatch}")


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

section("4. Verbosity")
for run in (OLD, NEW):
    lengths, by_entries = [], {}
    for _, _, doc in dumps_of(run):
        for _, _, block in synthesis_chunks(doc):
            for row in block["records"]:
                lengths.append(len(row["synthesis"] or ""))
                by_entries.setdefault(min(row["entries"], 10), []).append(len(row["synthesis"] or ""))
    print(
        f"{run}: n={len(lengths)} mean={statistics.mean(lengths):.0f} "
        f"median={statistics.median(lengths):.0f} max={max(lengths)} total={sum(lengths):,} chars"
    )
    print("   mean chars by entry count: " + "  ".join(
        f"{k}{'+' if k == 10 else ''}:{statistics.mean(v):.0f}" for k, v in sorted(by_entries.items())
    ))

section("4b. How thin the evidence usually is")
entries = Counter()
for _, _, doc in dumps_of(NEW):
    for _, _, block in synthesis_chunks(doc):
        for row in block["records"]:
            entries[row["entries"]] += 1
total = sum(entries.values())
print(f"records={total}; 1 entry={entries[1]} ({entries[1] / total:.0%}); "
      f"<=2 entries={(entries[1] + entries[2]) / total:.0%}; max entries={max(entries)}")


# --- 5. the own-name ban ------------------------------------------------------------------

section("5. Own-name ban")
for run in (OLD, NEW):
    for subject in ("alecmfg_com", "steelcraft_com"):
        name = OWN_NAME[subject]
        hits = focal_caused = 0
        context = Counter()
        for subj, field, doc in dumps_of(run):
            if subj != subject:
                continue
            for _, _, block in synthesis_chunks(doc):
                for row in block["records"]:
                    n = row.get("own_name_hits_in_synthesis", 0)
                    if not n:
                        continue
                    hits += n
                    if re.search(name, row["focal_form"] or "", re.I):
                        focal_caused += n
                    for m in re.finditer(
                        rf"(?<![A-Za-z0-9]){name}(?![A-Za-z0-9])(\s+\w+)?", row["synthesis"], re.I
                    ):
                        context[m.group(0)] += 1
        print(f"{run} {subject}: hits={hits}  of which the focal_form itself carries the name={focal_caused}")
        print("     top contexts: " + ", ".join(f"{v}x {k!r}" for k, v in context.most_common(5)))

section("5b. Where the name enters the stage's own inputs (new run, steelcraft)")
pattern = re.compile(r"(?<![A-Za-z0-9])Steelcraft(?![A-Za-z0-9])", re.I)
mentions = loc_hits = snip_hits = rows = focal_hits = 0
for subj, field, doc in dumps_of(NEW):
    if subj != "steelcraft_com":
        continue
    for _, chunk, block in synthesis_chunks(doc):
        for group in chunk["fold"]["groups"]:
            for mention in group.get("mentions", []):
                mentions += 1
                loc_hits += bool(pattern.search(mention.get("location") or ""))
                snip_hits += bool(pattern.search(mention.get("snippet") or ""))
        for row in block["records"]:
            rows += 1
            focal_hits += bool(pattern.search(row["focal_form"] or ""))
print(f"mentions={mentions}: location carries the name {loc_hits} ({loc_hits / mentions:.1%}), "
      f"snippet {snip_hits} ({snip_hits / mentions:.1%})")
print(f"records={rows}: focal_form carries the name {focal_hits} ({focal_hits / rows:.1%})")


# --- 6. the new "what the manufacturer does" section ---------------------------------------

section("6. The dealing rule, on heading-only evidence (1 entry, snippet <= 60 chars)")
for run in (OLD, NEW):
    total = asserts = floor = 0
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
                asserts += bool(ACTIVE_DEALING.search(row["synthesis"] or ""))
    print(f"{run}: {total} such records; asserts a manufacturer dealing in {asserts} ({asserts / total:.0%})")

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
    print(f"{run}: n={n}  attributes to a non-manufacturer party {party} ({party / n:.1%})  "
          f"set-aside/no-dealing floor {floor} ({floor / n:.1%})")


# --- 7. fidelity ----------------------------------------------------------------------------

section("7. Fidelity checks (new run)")
for run in (OLD, NEW):
    drift = 0
    for _, _, doc in dumps_of(run):
        for _, _, block in synthesis_chunks(doc):
            for row in block["records"]:
                focal = norm(row["focal_form"])
                if focal and focal not in norm(row["synthesis"] or ""):
                    drift += 1
    print(f"{run}: focal_form absent from its own synthesis: {drift}")

print("\nunsupported '<X> Series' names (entity invented or swapped):")
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
    # 'Paladin Series' cases are the (TM) being dropped from "Paladin(TM) Series" — not inventions.
    real = [f for f in found if not f[2].startswith("Paladin")]
    print(f"  {run}: {len(found)} flagged, {len(real)} after removing the (TM)-stripping false positives")
    for f in real:
        print(f"     {f[0]} | focal={f[1]!r} -> wrote {f[2]!r}")

section("8. Suspicious fold merges reaching synthesis (new run)")
def skeleton(text: str) -> str:
    return re.sub(r"[^a-z]", "", text.lower()).rstrip("s")

suspicious = 0
for subj, field, doc in dumps_of(NEW):
    for _, _, block in synthesis_chunks(doc):
        for row in block["records"]:
            if len(row["forms"]) < 2:
                continue
            if len({skeleton(f) for f in row["forms"]}) > 1:
                suspicious += 1
                print(f"  {subj}/{field} key={row['key']!r} forms={row['forms']}")
print(f"  total: {suspicious} (all verb-fold / punctuation merges — no cross-entity collapse)")

section("9. Locations reaching synthesis")
missing = total = 0
for _, _, doc in dumps_of(NEW):
    for _, chunk, _ in synthesis_chunks(doc):
        for group in chunk["fold"]["groups"]:
            for mention in group.get("mentions", []):
                total += 1
                location = mention.get("location") or ""
                missing += (not location) or ("not described" in location)
print(f"mention entries={total}; without a usable location={missing}")
