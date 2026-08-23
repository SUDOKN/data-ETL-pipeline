"""Simulate a PAGE-ALIGNED packer: windows = whole pages greedily up to 5k tok
(a page bigger than 5k is split by the existing line splitter; only its
continuations start mid-page); macro chunks = up to 4 consecutive windows
(<=20k); max_chunks=2. Compare with today's geometry."""
import sys, glob, os, re, statistics
sys.path.insert(0, "packages/core/src"); sys.path.insert(0, "packages/llm_providers/src")
import litellm
from core.models.chunking_strat import wide, merge_trailing_remainder
from llm_providers.utils.chunk_util import get_chunks_respecting_line_boundaries_sync
MODEL="gpt-4.1"
class M: name=MODEL
llm=M()
def tok(s): return litellm.token_counter(model=MODEL, text=s)
SEP = re.compile(r"^[ \t]*#{10,}[ \t]*$", re.M)
SUB = wide.max_tokens_per_chunk // wide.search_divisor; DIV = wide.search_divisor; MAXC = wide.max_chunks

TODAY = {"101machine.com":(1,7075),"ableengineering.com":(7,90425),"alecmfg.com":(6,142826),"anchor-mfg.com":(3,55004),
         "austinelectricservices.com":(6,141406),"steelcraft.com":(8,189216),"taylordunn.com":(8,181851)}

def pages_of(text):
    starts = [m.start() for m in SEP.finditer(text)]
    if not starts or starts[0] != 0: starts = [0] + starts
    return [(a, b) for a, b in zip(starts, starts[1:] + [len(text)])]

for f in sorted(glob.glob("apps/data_etl_app/src/data_etl_app/knowledge/sample_scraped_texts/*.txt")):
    name = os.path.basename(f)[:-4]; text = open(f, encoding="utf-8").read()
    pg = pages_of(text)
    if len(text) > 600_000:
        sizes = [(b-a)/4 for a,b in pg]  # chars/4 ≈ tokens
        print(f"\n## {name}: {len(text):,} chars ≈{len(text)//4:,} tok (chars/4), {len(pg)} pages; page ≈tok median {statistics.median(sizes):.0f} p90 {sorted(sizes)[int(.9*len(sizes))]:.0f} max {max(sizes):.0f}; pages >{SUB}: {sum(1 for s in sizes if s>SUB)} ({100*sum(1 for s in sizes if s>SUB)/len(sizes):.1f}%)")
        continue
    sizes = [tok(text[a:b]) for a,b in pg]
    # greedy page packing
    windows=[]; cur_start=None; cur_tok=0; mid=0
    def close():
        global cur_start, cur_tok
        if cur_start is not None: windows.append((cur_start, cur_end_holder[0], cur_tok)); cur_start=None; cur_tok=0
    cur_end_holder=[0]
    for (a,b),s in zip(pg,sizes):
        if s > SUB:  # oversize page: flush, then split it by lines
            close()
            parts = get_chunks_respecting_line_boundaries_sync(text[a:b], llm, SUB, 0, None)
            pb = merge_trailing_remainder([(a+int(k.split(":")[0]), a+int(k.split(":")[1])) for k in parts])
            for i,(x,y) in enumerate(pb):
                windows.append((x,y,tok(text[x:y])));
                if i>0: mid+=1
            continue
        if cur_start is not None and cur_tok + s > SUB: close()
        if cur_start is None: cur_start=a
        cur_end_holder[0]=b; cur_tok+=s
    close()
    # trailing tiny window merge (P5 analogue) only if it fits
    if len(windows)>1 and windows[-1][2] < 0.2*windows[-2][2] and windows[-2][2]+windows[-1][2] <= SUB*1.1:
        (x1,y1,t1),(x2,y2,t2)=windows[-2],windows[-1]; windows=windows[:-2]+[(x1,y2,t1+t2)]
    chunks=[windows[i:i+DIV] for i in range(0,len(windows),DIV)][:MAXC]
    used=[w for c in chunks for w in c]
    fills=[w[2] for w in used]
    covered = used[-1][1] if used else 0
    tw, tc = TODAY[name]
    print(f"\n## {name}: {len(pg)} pages, page tok median {statistics.median(sizes):.0f} max {max(sizes)}")
    print(f"   page-aligned: {len(used)} windows in {len(chunks)} chunks (today {tw}); fill median {statistics.median(fills):.0f} min {min(fills)} max {max(fills)} tok; mid-page windows {mid}; covered {covered:,}/{len(text):,} chars (today {tc:,})")
    print("   windows: " + ", ".join(f"{a}:{b}({t})" for a,b,t in used))
