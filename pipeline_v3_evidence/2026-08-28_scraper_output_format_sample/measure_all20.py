"""Format evaluation across all 20 golden-corpus manufacturers.

For each pinned subject: take up to 8 page URLs from its pinned scraped text,
fetch live, render 4 ways (raw HTML / sanitized HTML / markdown+islands /
innerText-approx text), tokenize, and run the repo's real dedup on the text
and markdown renderings. Reports per-site and aggregate numbers."""
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, "/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline/packages/scraper/src")
sys.path.insert(0, str(Path(__file__).parent))
from scraper.utils.dedup_util import deduplicate_scraped_content  # noqa: E402
from format_tokens import fetch, parse, to_text, to_sanitized, to_markdown, ENC  # noqa: E402

PINNED = Path("/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline/apps/data_etl_app/tests/test_stages/sample_scraped_texts")
SEP = "#" * 50
MAX_PAGES = 8
URL_RE = re.compile(r"^https?://\S+$")


def urls_of(subject_file: Path) -> list[str]:
    seen, out = set(), []
    for line in subject_file.read_text(errors="replace").splitlines():
        line = line.strip()
        if URL_RE.fullmatch(line) and line not in seen:
            seen.add(line)
            out.append(line)
        if len(out) >= MAX_PAGES:
            break
    return out


def tok(s: str) -> int:
    return len(ENC.encode(s, disallowed_special=()))


def eval_site(subject_file: Path):
    site = subject_file.stem
    result = {"site": site, "fetched": 0, "failed": 0, "pages": [],
              "raw": 0, "san": 0, "md": 0, "txt": 0,
              "tables": 0, "tables_span": 0, "details": 0}
    renders = {}

    def one(url):
        try:
            html = fetch(url)
            tree = parse(html)
            stats = {"tables": 0, "tables_span": 0, "nested_lists": 0, "details": 0}
            md = to_markdown(tree, hybrid_tables=True, stats=stats)
            return url, html, to_text(tree), to_sanitized(tree), md, stats, None
        except Exception as e:
            return url, None, None, None, None, None, f"{type(e).__name__}: {e}"

    with ThreadPoolExecutor(max_workers=4) as ex:
        for url, html, text, san, md, stats, err in ex.map(one, urls_of(subject_file)):
            if err:
                result["failed"] += 1
                result["pages"].append({"url": url, "error": err})
                continue
            result["fetched"] += 1
            result["raw"] += tok(html)
            result["san"] += tok(san)
            result["md"] += tok(md)
            result["txt"] += tok(text)
            for k in ("tables", "tables_span", "details"):
                result[k] += stats[k]
            renders[url] = {"text": text, "markdown": md}

    if result["fetched"] >= 2:
        for kind in ("text", "markdown"):
            combined = "".join(f"{SEP}\n{u}\n\n{renders[u][kind]}\n" for u in renders)
            deduped = deduplicate_scraped_content(combined)
            result[f"{kind}_pre"] = tok(combined)
            result[f"{kind}_post"] = tok(deduped)
    return result


def main():
    t0 = time.monotonic()
    results = [eval_site(f) for f in sorted(PINNED.glob("*.txt"))]

    hdr = f"{'site':<28} {'ok':>3} {'fail':>4} {'raw/txt':>8} {'md/txt':>7} {'txt-dedup%':>10} {'md-dedup%':>9} {'post md/txt':>11} {'tbl':>4} {'span':>4} {'det':>4}"
    print(hdr)
    print("-" * len(hdr))
    agg = {"raw": 0, "md": 0, "txt": 0, "tp": 0, "tq": 0, "mp": 0, "mq": 0,
           "tables": 0, "tables_span": 0, "details": 0, "ok": 0, "fail": 0}
    for r in results:
        if r["fetched"] == 0:
            print(f"{r['site']:<28} {0:>3} {r['failed']:>4}  ALL FETCHES FAILED: {r['pages'][0]['error'][:60] if r['pages'] else '?'}")
            agg["fail"] += r["failed"]
            continue
        have_dedup = "text_pre" in r
        td = 100 * (r["text_pre"] - r["text_post"]) / r["text_pre"] if have_dedup else float("nan")
        md_d = 100 * (r["markdown_pre"] - r["markdown_post"]) / r["markdown_pre"] if have_dedup else float("nan")
        post_ratio = r["markdown_post"] / r["text_post"] if have_dedup and r["text_post"] else float("nan")
        print(f"{r['site']:<28} {r['fetched']:>3} {r['failed']:>4} {r['raw'] / max(r['txt'], 1):>7.1f}x {r['md'] / max(r['txt'], 1):>6.2f}x {td:>9.1f}% {md_d:>8.1f}% {post_ratio:>10.3f}x {r['tables']:>4} {r['tables_span']:>4} {r['details']:>4}")
        agg["ok"] += r["fetched"]; agg["fail"] += r["failed"]
        agg["raw"] += r["raw"]; agg["md"] += r["md"]; agg["txt"] += r["txt"]
        for k in ("tables", "tables_span", "details"):
            agg[k] += r[k]
        if have_dedup:
            agg["tp"] += r["text_pre"]; agg["tq"] += r["text_post"]
            agg["mp"] += r["markdown_pre"]; agg["mq"] += r["markdown_post"]
    print("-" * len(hdr))
    print(f"{'TOTAL':<28} {agg['ok']:>3} {agg['fail']:>4} {agg['raw'] / max(agg['txt'], 1):>7.1f}x {agg['md'] / max(agg['txt'], 1):>6.2f}x "
          f"{100 * (agg['tp'] - agg['tq']) / max(agg['tp'], 1):>9.1f}% {100 * (agg['mp'] - agg['mq']) / max(agg['mp'], 1):>8.1f}% "
          f"{agg['mq'] / max(agg['tq'], 1):>10.3f}x {agg['tables']:>4} {agg['tables_span']:>4} {agg['details']:>4}")
    print(f"\nelapsed: {time.monotonic() - t0:.0f}s")
    (Path(__file__).parent / "all20_results.json").write_text(json.dumps(results, indent=1))
    for r in results:
        for p in r["pages"]:
            if "error" in p:
                print(f"  FAIL {r['site']}: {p['url']} -> {p['error'][:90]}")


if __name__ == "__main__":
    main()
