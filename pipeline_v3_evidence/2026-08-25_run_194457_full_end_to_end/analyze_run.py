"""Analysis of run 20260825T194457 — the first COMPLETED end-to-end v3 run.

Both subjects finished all 7 multi-stage fields. This run is the live test of
the three 2026-08-25 fixes (A1 retry spend, B1 dropped_options, the mentions
prompt fix) and re-measures every finding from run 20260824T190359.

Run from the repo root:
  python3 pipeline_v3_evidence/2026-08-25_run_194457_full_end_to_end/analyze_run.py
"""

import collections
import glob
import itertools
import json
import os
import re
import statistics

RUN = "packages/logs/extraction_dumps/20260825T194457/"
PREV = "packages/logs/extraction_dumps/20260824T190359/"
ONT = "apps/data_etl_app/src/data_etl_app/knowledge/ontology/"
GROUNDING_KEYS = ("freehand_grounding", "in_vocab_grounding", "oov_grounding")

FIELD_ONT = {
    "process_caps": "process_caps.json",
    "material_caps": "material_caps.json",
    "industries": "industries.json",
    "conformity_attestations": "certificates.json",
}


def ontology_labels(fname):
    """(names, altlabels) — concept nodes carry name/altLabels/children."""
    d = json.load(open(ONT + fname))
    names, alts = set(), set()

    def walk(x):
        if isinstance(x, dict):
            if "name" in x and isinstance(x["name"], str):
                names.add(x["name"])
            for a in x.get("altLabels") or []:
                if isinstance(a, str):
                    alts.add(a)
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for i in x:
                walk(i)

    walk(d)
    return names, alts


def load_rows(filename, run=RUN):
    """group_id -> row, de-duplicated across chunks (group ids are global)."""
    doc = json.load(open(os.path.join(run, filename)))
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
    agg = collections.defaultdict(lambda: collections.defaultdict(set))
    counts = collections.Counter()
    for path in sorted(glob.glob(RUN + "*.json")):
        doc, _ = load_rows(os.path.basename(path))
        for stage, item in each_request(doc):
            if "created_at" not in item:
                continue
            agg[stage][item["created_at"][:13]].add(os.path.basename(path))
            counts[stage] += 1
    for stage in sorted(agg):
        dates = sorted(agg[stage])
        print(f"   {stage:42s} n={counts[stage]:5d}  created={dates}")


def funnel():
    section("2. GROUP FUNNEL PER FIELD (vs run 190359 where it completed)")
    header = ("field", "groups", "no_mnt", "ground", "scr_out", "tags", "tags/grp", "prev tags")
    print(f"{header[0]:44s}{header[1]:>7s}{header[2]:>7s}{header[3]:>7s}{header[4]:>8s}{header[5]:>6s}{header[6]:>9s}{header[7]:>10s}")

    def final_tags(rows):
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
        return tags

    for path in sorted(glob.glob(RUN + "*.json")):
        name = os.path.basename(path)
        _, rows = load_rows(name)
        if not rows:
            continue
        tags = final_tags(rows)
        st = collections.Counter(r["status"] for r in rows.values())
        per = sum(tags.values()) / max(st["grounded"], 1)
        prev = ""
        if os.path.exists(os.path.join(PREV, name)):
            _, prows = load_rows(name, PREV)
            if prows:
                prev = str(len(final_tags(prows)))
        print(
            f"{name.replace('.json',''):44s}{len(rows):7d}{st['no_mentions']:7d}"
            f"{st['grounded']:7d}{st['screened_out']:8d}{len(tags):6d}{per:9.2f}{prev:>10s}"
        )


def b1_drops():
    section("3. B1 LIVE — dropped_options inventory, classified against the ontology")
    print("A drop is CORRECT if the option is genuinely not a vocabulary label;")
    print("a drop is FALSE if the label IS vocabulary but was echoed in a drifted")
    print("display form (e.g. 'Machining (also: ...)').")
    also_re = re.compile(r"^(.*?)\s*\(also: .*\)$")
    total = correct = false_drop = 0
    kept_beside = 0
    for path in sorted(glob.glob(RUN + "*.json")):
        name = os.path.basename(path)
        field = name.split("__")[1][:-5]
        names, alts = ontology_labels(FIELD_ONT[field]) if field in FIELD_ONT else (set(), set())
        vocab = names | alts
        _, rows = load_rows(name)
        hits = []
        for gid, row in rows.items():
            for key in GROUNDING_KEYS:
                g = row.get(key)
                if g and g.get("dropped_options"):
                    hits.append((gid, row["focal_form"], key, g["dropped_options"], sorted((g.get("tags") or {}))))
        if not hits:
            continue
        print(f"\n{name}: {len(hits)} row(s) with drops")
        for gid, form, stage, drops, kept in hits:
            for d in drops:
                total += 1
                bare = also_re.match(d)
                core = bare.group(1) if bare else d
                if vocab and core in vocab:
                    false_drop += 1
                    which = "name" if core in names else "altLabel"
                    tag = f"FALSE DROP ({which}, drifted display form)" if bare else f"FALSE DROP (bare {which}!)"
                else:
                    correct += 1
                    tag = "correct (not in vocabulary)"
                print(f"   {gid} {form!r:48.48s} dropped {d!r:44.44s} -> {tag}")
            if kept:
                kept_beside += 1
                print(f"      (kept beside the drop: {kept})")
    print(f"\nTOTAL drops={total}  correct={correct}  FALSE={false_drop}  rows that still kept a tag={kept_beside}")


def oov_capture():
    section("4. DID THE OOV PASS RECORD THE DROPPED CONCEPTS AS REAL GAPS?")
    print("B1's contract: a correct drop is not lost — the same record's OOV pass")
    print("should surface the concept as out-of-vocabulary.")
    for path in sorted(glob.glob(RUN + "*.json")):
        name = os.path.basename(path)
        _, rows = load_rows(name)
        report = []
        for gid, row in rows.items():
            iv = row.get("in_vocab_grounding") or {}
            drops = iv.get("dropped_options")
            if not drops:
                continue
            oov = row.get("oov_grounding") or {}
            oov_tags = sorted((oov.get("tags") or {}))
            report.append((gid, row["focal_form"], drops, oov_tags, bool(oov.get("declined"))))
        if not report:
            continue
        print(f"\n{name}")
        for gid, form, drops, oov_tags, declined in report:
            covered = [d for d in drops if any(d.lower() in t.lower() or t.lower() in d.lower() for t in oov_tags)]
            status = "COVERED" if covered else ("declined" if declined else ("oov=" + ",".join(oov_tags) if oov_tags else "NOTHING"))
            print(f"   {gid} {form!r:44.44s} drops={drops} oov={oov_tags} -> {status}")


def retries():
    section("5. RETRY PORT — first live firings")
    found = 0
    for path in sorted(glob.glob(RUN + "*.json")):
        doc, _ = load_rows(os.path.basename(path))
        for stage, item in each_request(doc):
            if ">retry>" in item["custom_id"]:
                found += 1
                print(f"\n{os.path.basename(path)} stage={stage}")
                print(f"   custom_id={item['custom_id']}")
                print(f"   created={item.get('created_at')} completed={item.get('completed_at')} in/out={item.get('input_tokens')}/{item.get('output_tokens')}")
    print(f"\ntotal retry requests: {found}" if found else "no retry requests in any dump")


def mentions_citation():
    section("6. THE MENTIONS PROMPT FIX — do explanations still cite a 'mentions' field?")
    print("Baseline (run 190359, stale prompts): 30.6% of 5,239 explanations cited")
    print("'mentions'. The fixed prompts say 'focal form and synthesis'.")
    pat = re.compile(r"\bmentions?\b", re.I)
    focal = re.compile(r"focal form", re.I)
    for run, label in ((PREV, "190359 (stale)"), (RUN, "194457 (fixed)")):
        n = cite = focal_cite = 0
        for path in sorted(glob.glob(run + "*.json")):
            _, rows = load_rows(os.path.basename(path), run)
            for row in rows.values():
                for key in GROUNDING_KEYS:
                    g = row.get(key)
                    if not g:
                        continue
                    texts = [g.get("declined") or ""]
                    for tag_val in (g.get("tags") or {}).values():
                        if isinstance(tag_val, dict):
                            texts.append(tag_val.get("explanation") or "")
                        elif isinstance(tag_val, str):
                            texts.append(tag_val)
                    for t in texts:
                        if not t:
                            continue
                        n += 1
                        if pat.search(t):
                            cite += 1
                        if focal.search(t):
                            focal_cite += 1
        print(f"   {label:18s} explanations={n:6d}  say 'mention(s)'={cite:5d} ({cite/max(n,1):.1%})  say 'focal form'={focal_cite:5d} ({focal_cite/max(n,1):.1%})")


def reproducibility():
    section("7. FREEHAND-GROUNDING REPRODUCIBILITY (free A/A test)")
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
            ta = tuple(sorted(fa["tags"])) if fa.get("tags") else ("<declined>",)
            tb = tuple(sorted(fb["tags"])) if fb.get("tags") else ("<declined>",)
            if ta == tb:
                same += 1
            else:
                if ("<declined>" in ta) != ("<declined>" in tb):
                    decline_flips += 1
                examples.append((a[gid]["focal_form"], ta, tb))
        print(f"\n{mfg}: {len(da)} requests each, identical (group, digest) pairs = {len(da & db)}")
        print(f"   groups compared            {n}")
        print(f"   identical candidate sets   {same} ({same / max(n,1):.1%})")
        print(f"   divergent                  {n - same}  (decline-vs-tag flips: {decline_flips})")
        for form, ta, tb in examples[:5]:
            print(f"     {form!r}\n        run A = {ta}\n        run B = {tb}")


def cross_field_overlap():
    section("8. CROSS-FIELD GROUP OVERLAP (equipment-search-returns-products check)")
    for mfg in ("steelcraft_com", "alecmfg_com"):
        files = [f"{mfg}__{f}.json" for f in ("products", "equipments", "industries", "conformity_attestations", "process_caps", "material_caps")]
        loaded = {}
        for f in files:
            if os.path.exists(os.path.join(RUN, f)):
                loaded[f] = load_rows(f)[1]
        print(f"\n{mfg}:")
        for f, rows in loaded.items():
            grounded = sum(1 for r in rows.values() if r["status"] == "grounded")
            print(f"   {f.split('__')[1][:-5]:26s} groups={len(rows):4d} grounded={grounded:4d}")
        for a, b in itertools.combinations(loaded, 2):
            inter = len(set(loaded[a]) & set(loaded[b]))
            if inter:
                print(f"   overlap {a.split('__')[1][:-5]:24s} x {b.split('__')[1][:-5]:24s} = {inter}")


def scr2_quality():
    section("9. SCR-2 EVIDENCE QUALITY ON THE CONTRACT SCREEN")
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
        if n:
            print(f"   circular ('made by X => customer-directed')  {circular:4d} ({circular / n:.0%})")
            print(f"   leans on a standard/rating/application       {weakish:4d} ({weakish / n:.0%})")
        for s in samples[:3]:
            print(f"     - {s[:140]}")


def evidence_and_lints():
    section("10. EVIDENCE THICKNESS AND THE OWN-NAME LINT")
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
    section("11. TOKENS AND COST BY STAGE (gpt-4.1 list rates: $2/Mtok in, $8/Mtok out)")
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
    section("12. NEAR-DUPLICATE TAG EXPLOSION (steelcraft products, vs 206 last run)")
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
    if tags:
        print(f'   contain "door": {len(doors)}   contain "frame": {len(frames)}   '
              f'= {(len(doors) + len(frames)) / len(tags):.0%} of the output vocabulary')
    lower = collections.Counter()
    for t in tags:
        lower[t.lower()] += 1
    case_dupes = [t for t, c in lower.items() if c > 1]
    print(f"   case-only duplicate tag pairs: {len(case_dupes)}")
    for probe in ("flush door", "drywall frame"):
        hits = sorted(t for t in tags if probe in t.lower())
        print(f'\n   "{probe}" -> {len(hits)} distinct tags')


if __name__ == "__main__":
    freshness()
    funnel()
    b1_drops()
    oov_capture()
    retries()
    mentions_citation()
    reproducibility()
    cross_field_overlap()
    scr2_quality()
    evidence_and_lints()
    cost()
    near_duplicates()
