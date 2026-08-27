"""Analysis of run 20260824T190359 — the first end-to-end v3 run (Phase 3 gate).

Reads the dumps in packages/logs/extraction_dumps/20260824T190359/ and reports:
  1. request freshness per stage (what was live vs replayed from cache)
  2. the group funnel per field (groups -> grounded -> screened -> distinct tags)
  3. freehand-grounding reproducibility (products vs contract_products share a
     byte-identical payload and prompt, so the two calls are a free A/A test)
  4. cross-field group overlap (does a field's search return another field's
     entities?)
  5. SCR-2 evidence quality on the contract screen
  6. evidence thickness and the own-name lint
Run from the repo root: python3 pipeline_v3_evidence/2026-08-24_run_190359_first_end_to_end/analyze_run.py
"""

import collections
import glob
import json
import os
import re
import statistics

RUN = "packages/logs/extraction_dumps/20260824T190359/"
GROUNDING_KEYS = ("freehand_grounding", "in_vocab_grounding", "oov_grounding")


def load_rows(filename):
    """group_id -> row, de-duplicated across chunks (group ids are global)."""
    doc = json.load(open(os.path.join(RUN, filename)))
    rows = {}
    for chunk in doc.get("chunks", {}).values():
        for row in chunk.get("rows", []):
            rows[row["group_id"]] = row
    return doc, rows


def each_request(doc):
    for chunk in doc.get("chunks", {}).values():
        for stage, value in chunk.get("requests", {}).items():
            items = (
                value
                if isinstance(value, list)
                else [x for sub in value.values() for x in (sub if isinstance(sub, list) else [sub])]
            )
            for item in items:
                if isinstance(item, dict) and "custom_id" in item:
                    yield stage, item


def section(title):
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def freshness():
    section("1. REQUEST FRESHNESS — which stages actually ran in this session")
    for path in sorted(glob.glob(RUN + "*.json")):
        doc, _ = load_rows(os.path.basename(path))
        dates = collections.defaultdict(set)
        counts = collections.Counter()
        for stage, item in each_request(doc):
            if "created_at" not in item:
                continue
            dates[stage].add(item["created_at"][:16])
            counts[stage] += 1
        print(f"\n{os.path.basename(path)}")
        for stage in sorted(dates):
            print(f"   {stage:36s} n={counts[stage]:4d}  created={sorted(dates[stage])}")


def funnel():
    section("2. GROUP FUNNEL PER FIELD")
    header = ("field", "groups", "no_mnt", "ground", "scr_out", "tags", "tags/grp")
    print(f"{header[0]:44s}{header[1]:>7s}{header[2]:>7s}{header[3]:>7s}{header[4]:>8s}{header[5]:>7s}{header[6]:>9s}")
    for path in sorted(glob.glob(RUN + "*.json")):
        name = os.path.basename(path)
        _, rows = load_rows(name)
        if not rows:
            continue
        tags = collections.Counter()
        for row in rows.values():
            if row["status"] != "grounded":
                continue
            screening = row.get("screening")
            if screening:
                for tag, verdict in screening.items():
                    if verdict["passed"]:
                        tags[tag] += 1
            else:
                for key in GROUNDING_KEYS:
                    for tag in (row.get(key) or {}).get("tags", {}):
                        tags[tag] += 1
        st = collections.Counter(r["status"] for r in rows.values())
        per = sum(tags.values()) / max(st["grounded"], 1)
        print(
            f"{name.replace('.json',''):44s}{len(rows):7d}{st['no_mentions']:7d}"
            f"{st['grounded']:7d}{st['screened_out']:8d}{len(tags):7d}{per:9.2f}"
        )


def reproducibility():
    section("3. FREEHAND-GROUNDING REPRODUCIBILITY (free A/A test)")
    print("products and contract_products share every prompt version except screening,")
    print("and their freehand-grounding requests carry identical |ud= payload digests.")
    for mfg in ("steelcraft_com", "alecmfg_com"):
        doc_a, a = load_rows(f"{mfg}__products.json")
        doc_b, b = load_rows(f"{mfg}__contract_products.json")

        def digests(doc):
            out = set()
            for stage, item in each_request(doc):
                if stage != "llm_phrase_freehand_grounding":
                    continue
                grp = re.search(r">(group>\d+>chunk>[^>]+)>", item["custom_id"])
                ud = re.search(r"\|ud=([0-9a-f]+)", item["custom_id"])
                if grp and ud:
                    out.add((grp.group(1), ud.group(1)))
            return out

        da, db = digests(doc_a), digests(doc_b)
        n = same = 0
        decline_flips = 0
        examples = []
        for gid in a:
            fa, fb = a[gid].get("freehand_grounding"), b[gid].get("freehand_grounding")
            if not fa or not fb:
                continue
            n += 1
            ta = tuple(sorted(fa["tags"])) if "tags" in fa else ("<declined>",)
            tb = tuple(sorted(fb["tags"])) if "tags" in fb else ("<declined>",)
            if ta == tb:
                same += 1
            else:
                if ("<declined>" in ta) != ("<declined>" in tb):
                    decline_flips += 1
                examples.append((a[gid]["focal_form"], ta, tb))
        print(f"\n{mfg}: {len(da)} requests each, identical (group, digest) pairs = {len(da & db)}")
        print(f"   groups compared            {n}")
        print(f"   identical candidate sets   {same} ({same / n:.1%})")
        print(f"   divergent                  {n - same}  (decline-vs-tag flips: {decline_flips})")
        for form, ta, tb in examples[:5]:
            print(f"     {form!r}\n        run A = {ta}\n        run B = {tb}")


def cross_field_overlap():
    section("4. CROSS-FIELD GROUP OVERLAP (group_id is a hash of the normalized key)")
    files = [
        "steelcraft_com__products.json",
        "steelcraft_com__equipments.json",
        "steelcraft_com__industries.json",
        "steelcraft_com__conformity_attestations.json",
    ]
    loaded = {f: load_rows(f) for f in files}
    for f, (doc, rows) in loaded.items():
        pvs = {
            re.search(r"pv=([A-Za-z0-9_.\-]+)", item["custom_id"]).group(1)
            for stage, item in each_request(doc)
            if stage == "llm_phrase_search"
        }
        print(f"{f:46s} search_pv={sorted(pvs)} groups={len(rows)}")
    import itertools

    for a, b in itertools.combinations(files, 2):
        ia, ib = set(loaded[a][1]), set(loaded[b][1])
        print(
            f"   overlap {a.split('__')[1][:-5]:24s} x {b.split('__')[1][:-5]:24s} = {len(ia & ib)}"
        )


def scr2_quality():
    section("5. SCR-2 EVIDENCE QUALITY ON THE CONTRACT SCREEN")
    print("SCR-2 asks whether the work is customer-directed. Count how often the")
    print("stated evidence is circular ('made by X, therefore customer-directed').")
    circular_marker = "indicating customer-directed work"
    real_customer = re.compile(r"client|customer spec|for a |for its |OEM|private label", re.I)
    weak = re.compile(r"standard|requirement|application|certif|code|rating", re.I)
    for mfg in ("steelcraft_com", "alecmfg_com"):
        _, rows = load_rows(f"{mfg}__contract_products.json")
        n = circular = weakish = 0
        samples = []
        for row in rows.values():
            for tag, verdict in (row.get("screening") or {}).items():
                for rule in verdict["applied_rules"]:
                    if rule["rule_id"] != "SCR-2" or rule["outcome"] != "satisfied":
                        continue
                    n += 1
                    text = rule["explanation"]
                    if circular_marker in text and not real_customer.search(text):
                        circular += 1
                        samples.append(text)
                    elif weak.search(text) and not real_customer.search(text):
                        weakish += 1
        print(f"\n{mfg}: SCR-2 satisfied on {n} tag-verdicts")
        print(f"   circular ('made by X => customer-directed')  {circular:4d} ({circular / n:.0%})")
        print(f"   leans on a standard/rating/application       {weakish:4d} ({weakish / n:.0%})")
        for s in samples[:3]:
            print(f"     - {s[:140]}")


def evidence_and_lints():
    section("6. EVIDENCE THICKNESS AND THE OWN-NAME LINT")
    print(f"{'field':46s}{'groups':>7s}{'own-name>0':>12s}{'mean mc':>9s}{'mc==1':>8s}")
    everything = []
    for path in sorted(glob.glob(RUN + "*.json")):
        name = os.path.basename(path)
        _, rows = load_rows(name)
        if not rows:
            continue
        mc = [r["mention_count"] for r in rows.values() if r["mention_count"]]
        if not mc:
            continue
        own = sum(1 for r in rows.values() if (r.get("record_own_name_hits") or 0) > 0)
        everything += mc
        singles = sum(1 for x in mc if x == 1) / len(mc)
        print(
            f"{name.replace('.json',''):46s}{len(rows):7d}"
            f"{own / len(rows):11.1%} {statistics.mean(mc):9.2f}{singles:8.1%}"
        )
    print(
        f"\nALL groups n={len(everything)} mean mentions={statistics.mean(everything):.2f} "
        f"median={statistics.median(everything)} single-mention={sum(1 for x in everything if x == 1) / len(everything):.1%}"
    )


def cost():
    section("7. TOKENS AND COST BY STAGE (gpt-4.1 list rates: $2/Mtok in, $8/Mtok out)")
    tin, tout = collections.Counter(), collections.Counter()
    for path in sorted(glob.glob(RUN + "*.json")):
        doc = json.load(open(path))
        for stage, usage in doc["run"]["token_usage"]["by_stage"].items():
            if isinstance(usage, dict):
                tin[stage] += usage.get("input_tokens", 0)
                tout[stage] += usage.get("output_tokens", 0)
    print(f"{'stage':38s}{'input':>12s}{'output':>12s}{'cost $':>10s}")
    for stage in sorted(tin, key=lambda s: -(tin[s] + tout[s])):
        c = tin[stage] / 1e6 * 2 + tout[stage] / 1e6 * 8
        print(f"{stage:38s}{tin[stage]:12,d}{tout[stage]:12,d}{c:10.2f}")
    total = sum(tin.values()) / 1e6 * 2 + sum(tout.values()) / 1e6 * 8
    print(f"{'TOTAL':38s}{sum(tin.values()):12,d}{sum(tout.values()):12,d}{total:10.2f}")


def near_duplicates():
    section("8. NEAR-DUPLICATE TAG EXPLOSION (steelcraft products)")
    _, rows = load_rows("steelcraft_com__products.json")
    tags = collections.Counter()
    for row in rows.values():
        if row["status"] == "grounded" and row.get("screening"):
            for tag, verdict in row["screening"].items():
                if verdict["passed"]:
                    tags[tag] += 1
    doors = [t for t in tags if "door" in t.lower()]
    frames = [t for t in tags if "frame" in t.lower()]
    print(f"distinct final product tags = {len(tags)}")
    print(f'   contain "door": {len(doors)}   contain "frame": {len(frames)}   '
          f'= {(len(doors) + len(frames)) / len(tags):.0%} of the output vocabulary')
    for probe in ("flush door", "drywall frame", "hurricane", "tornado"):
        hits = sorted(t for t in tags if probe in t.lower())
        print(f'\n   "{probe}" -> {len(hits)} distinct tags:')
        for h in hits:
            print(f"      {h}")


if __name__ == "__main__":
    freshness()
    funnel()
    reproducibility()
    cross_field_overlap()
    scr2_quality()
    evidence_and_lints()
    cost()
    near_duplicates()
