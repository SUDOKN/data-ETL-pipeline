"""Mechanical mention collection vs the LLM collector, on run 20260822T195947.
For every real mention-collection window: Ctrl+F (tier-1 exact, tier-2 casing-expanded) over the window text the model saw,
longest-span containment, clip to line/sentence, dedupe; estimate the Location stage's wire size & tokens; compare to actuals."""
import json, re, sys, bisect, collections, math, statistics, glob, os
sys.path.insert(0, "/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline/packages/core/src")
from core.utils.floor_scan import floor_scan, preceding_page_of, page_at, Occurrence
from core.utils.aggregation_fold import obligations_by_form
import tiktoken
enc = tiktoken.get_encoding("o200k_base")
T = lambda s: len(enc.encode(s))
ROOT = "/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline"
reqs = json.load(open("/private/tmp/claude-501/-Users-amaryadav-Documents-ASU-PhD-SUDOKN-data-ETL-pipeline/e68bb82e-2b6e-425f-9356-56bf89848bcf/scratchpad/all_requests.json"))
full = {s: open(f"{ROOT}/apps/data_etl_app/src/data_etl_app/knowledge/sample_scraped_texts/{s}.txt", encoding="utf-8").read() for s in ("alecmfg.com", "steelcraft.com")}
STATIC_OVERHEAD = 961  # prompt_tokens - user-message tokens on a real request (system prompt + chat framing)
LOC_OUT_PER_ITEM = 40  # id + JSON + a ~106-char location
_SENT = re.compile(r"(?<=[.!?])\s+")

def clip(text, lines, starts, s):
    li = bisect.bisect_right(starts, s) - 1; ls = starts[li]; line = lines[li]
    cuts = [0] + [m.end() for m in _SENT.finditer(line)] + [len(line)]
    rel = s - ls
    sb = max(c for c in cuts if c <= rel); se = min([c for c in cuts if c > rel] or [len(line)])
    return line[sb:se].strip(), li

# --- gather windows -----------------------------------------------------------
win = {}
for r in reqs:
    cid = r["custom_id"] or ""
    if ">llm_phrase_mention_collection>" not in cid or not r["user_message"] or "<<<PHRASES" not in r["user_message"]:
        continue
    subj, field = cid.split(">")[0:2]
    sub = re.search(r">sub>(\d+:\d+)>", cid).group(1)
    um = r["user_message"]
    text = um.split("text scraped from a manufacturer's website:\n", 1)[1].rsplit("<<<PHRASES", 1)[0].rstrip("\n")
    a = um.index("<<<PHRASES") + len("<<<PHRASES"); b = um.index("PHRASES>>>")
    forms = json.loads(um[a:b].strip())
    w = win.setdefault((subj, field, sub), dict(text=text, forms=[], groups=0, pin=0, pout=0, llm_mentions=0))
    w["forms"] += [f for f in forms if f not in w["forms"]]; w["groups"] += 1
    w["pin"] += r["usage"]["prompt_tokens"]; w["pout"] += r["usage"]["completion_tokens"]
    try:
        for f in json.loads(r["content"])["forms"]: w["llm_mentions"] += len(f.get("mentions") or [])
    except Exception: pass
print(f"windows {len(win)}, requests {sum(w['groups'] for w in win.values())}, actual mention-stage tokens in {sum(w['pin'] for w in win.values()):,} / out {sum(w['pout'] for w in win.values()):,}")

# --- today's fold output (what synthesis would see now) from the dumps -----------
fold_snip_chars = fold_loc_chars = fold_mentions = fold_oblig = 0
for fp in glob.glob(f"{ROOT}/packages/logs/extraction_dumps/20260822T195947/*__partial.json"):
    d = json.load(open(fp))
    for ch in d.get("chunks", {}).values():
        f = ch.get("fold")
        if not f: continue
        fold_oblig += f["summary"]["obligations"]
        for g in f["groups"]:
            for m in g["mentions"]:
                fold_mentions += 1; fold_snip_chars += len(m["snippet"]); fold_loc_chars += len(m.get("location") or "")
print(f"dumps: fold mentions {fold_mentions:,} (obligations {fold_oblig:,}); snippet chars {fold_snip_chars:,} ({T('x')*0+fold_snip_chars//4:,}~tok), location chars {fold_loc_chars:,}")

# --- mechanical collection per window --------------------------------------------
tot = collections.Counter(); per_field = collections.defaultdict(collections.Counter); dist_items = []; dist_text_tok = []
snip_lens = []; privacy = collections.Counter(); sample_rows = []
groups_by_K = {K: 0 for K in (30, 50, 100)}; prompt_by_K = {K: 0 for K in (30, 50, 100)}; out_by_K = {K: 0 for K in (30, 50, 100)}
for (subj, field, sub), w in sorted(win.items()):
    text, forms = w["text"], w["forms"]
    start = int(sub.split(":")[0])
    head = text[:120]; off = full[subj].find(head)
    pre = preceding_page_of(full[subj], off if off >= 0 else start)
    scan = floor_scan(text, forms, preceding_page=pre)
    t1 = obligations_by_form(scan); n_t1 = sum(len(v) for v in t1.values())
    # tier-2 union with longest-span containment; one mention per distinct span; forms sharing a span = casing family
    occs = collections.defaultdict(set)
    for f, lst in scan.tier2.items():
        for o in lst: occs[(o.start, o.end)].add(f)
    spans = sorted(occs, key=lambda s: (s[0], -(s[1] - s[0])))
    kept = []
    for s in spans:
        if any(k[0] <= s[0] and s[1] <= k[1] and k != s for k in kept): continue
        kept.append(s)
    # clip + dedupe
    lines = text.split("\n"); starts = [0]
    for ln in lines: starts.append(starts[-1] + len(ln) + 1)
    by_form_snip = collections.Counter(); by_snip = collections.Counter(); total_chars = 0
    for s in kept:
        snip, li = clip(text, lines, starts, s[0])
        fam = tuple(sorted(occs[s])); key = min(occs[s], key=lambda f: (f.lower(), f))  # casing family -> one key
        by_form_snip[(key, snip)] += 1; by_snip[snip] += 1; snip_lens.append(len(snip))
        if subj == "steelcraft.com" and 63831 <= start + s[0] <= 98888: privacy[field] += 1
        if len(sample_rows) < 8 and by_form_snip[(key, snip)] == 1 and len(snip) > 30: sample_rows.append((subj, field, key, snip[:140]))
    n_t2 = len(kept); n_fs = len(by_form_snip); n_s = len(by_snip)
    forms_with_hits = sum(1 for f in forms if scan.tier2.get(f))
    tot.update(forms=len(forms), forms_hit=forms_with_hits, tier1=n_t1, tier2_spans=n_t2, form_snip=n_fs, snip=n_s, llm=w["llm_mentions"], groups_now=w["groups"], pin_now=w["pin"], pout_now=w["pout"])
    per_field[field].update(tier1=n_t1, tier2_spans=n_t2, form_snip=n_fs, snip=n_s, llm=w["llm_mentions"])
    dist_items.append(n_s)
    # Location-stage cost: items = distinct snippets; wire = [{"mention_id":"m1","mention":snip}]
    items = [{"mention_id": f"m{i+1}", "mention": s} for i, s in enumerate(by_snip)]
    items_tok = T(json.dumps(items, ensure_ascii=False)) if items else 0
    text_tok = T(text); dist_text_tok.append(text_tok)
    tot["snip_chars"] += sum(len(s) for s in by_snip); tot["snip_chars_per_form"] += sum(len(s) for _, s in by_form_snip)
    for K in groups_by_K:
        g = max(1, math.ceil(n_s / K)) if n_s else 0
        groups_by_K[K] += g
        prompt_by_K[K] += g * (STATIC_OVERHEAD + text_tok) + items_tok
        out_by_K[K] += n_s * LOC_OUT_PER_ITEM

print(f"\n=== mechanical collection (all {len(win)} windows) ===")
print(f"sent forms {tot['forms']:,} (with any hit {tot['forms_hit']:,}; zero-hit {tot['forms']-tot['forms_hit']:,})")
print(f"LLM collector mentions (raw answers)      {tot['llm']:,}")
print(f"tier-1 exact obligations (what fold holds) {tot['tier1']:,}")
print(f"tier-2 casing-expanded distinct spans      {tot['tier2_spans']:,}   <- every occurrence, after longest-span containment")
print(f"dedupe by (form-family, clipped snippet)   {tot['form_snip']:,}   <- mentions synthesis would get")
print(f"dedupe by clipped snippet alone            {tot['snip']:,}   <- Location-stage wire items")
print(f"clipped snippet chars: median {statistics.median(snip_lens):.0f}, p90 {sorted(snip_lens)[int(len(snip_lens)*.9)]}, max {max(snip_lens)}")
print(f"items per window: median {statistics.median(dist_items):.0f}, p90 {sorted(dist_items)[int(len(dist_items)*.9)]}, max {max(dist_items)}; windows with 0 items: {sum(1 for x in dist_items if x==0)}")
print(f"downstream text: today's fold snippets {fold_snip_chars:,} chars (+ {fold_loc_chars:,} location) | mechanical per-form deduped {tot['snip_chars_per_form']:,} chars | distinct snippets {tot['snip_chars']:,} chars")
print(f"\n=== Location-stage cost estimate vs today's mention stage ===")
print(f"today: {tot['groups_now']} requests, prompt {tot['pin_now']:,}, completion {tot['pout_now']:,}")
for K in groups_by_K:
    print(f"K={K:>3} items/request: {groups_by_K[K]} requests, prompt ≈ {prompt_by_K[K]:,} ({prompt_by_K[K]/tot['pin_now']:.0%} of today), completion ≈ {out_by_K[K]:,} ({out_by_K[K]/tot['pout_now']:.0%} of today)")
print(f"(assumes a static the size of today's mention static ≈{STATIC_OVERHEAD} tok, window text re-sent per request, {LOC_OUT_PER_ITEM} output tok per item; items dedupe by distinct snippet)")
print("\n=== per field ===")
print(f"{'field':24} {'LLM':>6} {'tier1':>6} {'tier2':>6} {'form+snip':>9} {'snip':>6}")
for f, c in sorted(per_field.items()): print(f"{f:24} {c['llm']:>6} {c['tier1']:>6} {c['tier2_spans']:>6} {c['form_snip']:>9} {c['snip']:>6}")
print("\nsteelcraft privacy-page spans (tier-2, by field):", dict(privacy), "total", sum(privacy.values()))
print("\nsample clipped snippets:")
for r in sample_rows: print("  ", r)
