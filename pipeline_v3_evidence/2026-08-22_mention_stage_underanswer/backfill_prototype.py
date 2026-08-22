"""Backfill illustration on the real steelcraft process_caps window 102672:126084 (stored run answers)."""
import json, re, sys, bisect, dataclasses
from types import SimpleNamespace
sys.path.insert(0, "/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline/packages/core/src")
from core.utils.aggregation_fold import fold_window, WindowInput
from core.utils.floor_scan import page_spans, page_at, preceding_page_of
reqs=json.load(open("/private/tmp/claude-501/-Users-amaryadav-Documents-ASU-PhD-SUDOKN-data-ETL-pipeline/b0b8a829-bab9-4333-b43c-60e0b05cf84b/scratchpad/mention_requests.json"))
pref="steelcraft.com>process_caps>llm_phrase_mention_collection>chunk>81331:178148>sub>102672:126084>group>"
grp=[r for r in reqs if r["custom_id"].startswith(pref)]
sent=[]; mbf={}; text=None
for r in sorted(grp,key=lambda r:r["custom_id"]):
    um=r["user_message"]; a=um.index("<<<PHRASES")+len("<<<PHRASES"); b=um.index("PHRASES>>>")
    forms=json.loads(um[a:b].strip()); sent+=forms
    text=um.split("text scraped from a manufacturer's website:\n",1)[1].rsplit("<<<PHRASES",1)[0]
    for f in json.loads(r["content"])["forms"]:
        mbf.setdefault(f["form"],[]).extend(SimpleNamespace(**m) for m in (f.get("mentions") or []))
print("groups:", len(grp), "forms:", len(sent), "forms with >=1 mention:", sum(1 for f in sent if mbf.get(f)))
full=open("/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline/apps/data_etl_app/src/data_etl_app/knowledge/sample_scraped_texts/steelcraft.com.txt",encoding="utf-8").read()
win=WindowInput(text=text, sent_forms=sent, mentions_by_form=mbf, preceding_page=preceding_page_of(full,102672), window_id="102672:126084")
try:
    wf=fold_window(win, window_index=1, verb_fold=True)
except TypeError as e:
    print("sig:", e); import inspect; print(inspect.signature(fold_window)); raise
print("WindowFold fields:", [f.name for f in dataclasses.fields(wf)])
un=wf.unaccounted
print("unaccounted type:", type(un).__name__, "| obligations", getattr(wf,"obligations",None) if not isinstance(getattr(wf,"obligations",None),(list,dict)) else len(wf.obligations))
# normalize to list of (form, start, end)
items=[]
if isinstance(un, dict):
    for f, spans in un.items():
        for s in spans:
            s0,e0 = (s.start,s.end) if hasattr(s,"start") else (s[0],s[1]); items.append((f,s0,e0))
else:
    for x in un:
        items.append((getattr(x,"form",None), getattr(x,"start",None), getattr(x,"end",None)))
print("unaccounted hits:", len(items), "forms:", len({i[0] for i in items}))
# the backfill rule: snippet = the line holding the hit, clipped to the sentence holding it when the line has sentence punctuation
lines=text.split("\n"); starts=[0]
for ln in lines: starts.append(starts[-1]+len(ln)+1)
pages=page_spans(text, preceding_page=win.preceding_page)
_SENT=re.compile(r"(?<=[.!?])\s+")
def backfill(form, s, e):
    li=bisect.bisect_right(starts,s)-1; ls=starts[li]; line=lines[li]
    # sentence clip inside the line
    cuts=[0]+[m.end() for m in _SENT.finditer(line)]+[len(line)]
    rel=s-ls
    sb=max(c for c in cuts if c<=rel); se=min(c for c in cuts if c>rel) if any(c>rel for c in cuts) else len(line)
    snippet=line[sb:se].strip()
    return dict(form=form, start=s, end=e, page=page_at(pages,s), line_no=li, snippet=snippet, location="(not described by the collector; backfilled from the exact-match scan)", source="floor_scan")
shown=0
for f,s,e in items:
    if shown>=6: break
    bf=backfill(f,s,e)
    print(f"\n- form {f!r} @{s}:{e}  page …{(bf['page'] or '')[-55:]}  line {bf['line_no']}\n  snippet: {bf['snippet'][:220]!r}\n  location: {bf['location']}  source={bf['source']}")
    shown+=1
# how long are the backfilled snippets vs the collector's?
import statistics
bl=[len(backfill(f,s,e)["snippet"]) for f,s,e in items]
cl=[len(m.snippet) for ms in mbf.values() for m in ms]
print(f"\nbackfilled snippet chars: median {statistics.median(bl):.0f}, max {max(bl)} | collector's: median {statistics.median(cl):.0f}, max {max(cl)}")
