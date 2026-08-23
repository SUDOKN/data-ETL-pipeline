"""Measure: page sizes vs the 5k sub-window / 20k chunk, and how many windows
produced by TODAY'S chunker (wide: 20k chunk, divisor 4, overlap 0, max_chunks=2)
start mid-page, on the sample scraped texts."""
import sys, glob, os, statistics
sys.path.insert(0, "packages/core/src"); sys.path.insert(0, "packages/llm_providers/src")
import litellm
from core.utils.floor_scan import page_spans, _URL_LINE_RE
from core.models.chunking_strat import wide, merge_trailing_remainder
from llm_providers.utils.chunk_util import get_chunks_respecting_line_boundaries_sync
from llm_providers.models.llm_model import LLM_Model

MODEL = "gpt-4.1"
class M:  # minimal stand-in with .name, matching what token_counter needs
    name = MODEL
llm = M()

def tok(s): return litellm.token_counter(model=MODEL, text=s)

SUB = wide.max_tokens_per_chunk // wide.search_divisor  # 5000
files = sorted(glob.glob("apps/data_etl_app/src/data_etl_app/knowledge/sample_scraped_texts/*.txt"))
for f in files:
    name = os.path.basename(f).replace(".txt", "")
    text = open(f, encoding="utf-8").read()
    if len(text) > 600_000:
        print(f"\n## {name}: {len(text):,} chars — SKIPPED (too large to tokenize quickly)")
        continue
    spans = page_spans(text)
    sizes = [tok(text[s.start:s.end]) for s in spans]
    total = tok(text)
    print(f"\n## {name}: {len(text):,} chars, {total:,} tok, {len(spans)} pages; "
          f"page tok median {statistics.median(sizes):.0f} max {max(sizes)}; "
          f"pages >{SUB} tok: {sum(1 for s in sizes if s > SUB)}; >{wide.max_tokens_per_chunk}: {sum(1 for s in sizes if s > wide.max_tokens_per_chunk)}; "
          f"starts with URL line: {bool(_URL_LINE_RE.match(text))}")
    big = sorted(((s, sp.url) for s, sp in zip(sizes, spans) if s > SUB), reverse=True)[:5]
    for s, u in big: print(f"    big page {s} tok: {u}")
    # today's geometry: macro chunks then sub-windows
    chunks = get_chunks_respecting_line_boundaries_sync(text, llm, wide.max_tokens_per_chunk, wide.overlap, wide.max_chunks)
    n_win = n_mid = 0
    covered_end = 0
    for cb, ct in chunks.items():
        cs = int(cb.split(":")[0])
        subs = get_chunks_respecting_line_boundaries_sync(ct, llm, SUB, wide.overlap, None)
        bounds = [(cs + int(k.split(":")[0]), cs + int(k.split(":")[1])) for k in subs]
        bounds = merge_trailing_remainder(bounds)
        for (a, b) in bounds:
            n_win += 1
            line_end = text.find("\n", a); line = text[a:line_end if line_end != -1 else len(text)]
            starts_url = bool(_URL_LINE_RE.fullmatch(line))
            if not starts_url: n_mid += 1
            covered_end = max(covered_end, b)
            # distance from window start back to the previous URL line, and forward to the next
            prev_url = max((m.start() for m in _URL_LINE_RE.finditer(text, 0, a)), default=None)
            next_url = next((m.start() for m in _URL_LINE_RE.finditer(text, a)), None)
            back = tok(text[prev_url:a]) if (prev_url is not None and not starts_url) else 0
            fwd = tok(text[a:next_url]) if (next_url is not None and not starts_url) else 0
            print(f"    window {a}:{b} ({tok(text[a:b])} tok) starts_at_url={starts_url}"
                  + ("" if starts_url else f"  [prev URL {back} tok back, next URL {fwd} tok ahead]"))
    print(f"  => {n_win} windows, {n_mid} start mid-page; covered {covered_end:,}/{len(text):,} chars (max_chunks={wide.max_chunks})")
