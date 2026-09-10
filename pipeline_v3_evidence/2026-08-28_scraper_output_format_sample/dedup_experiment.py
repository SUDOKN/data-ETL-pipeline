"""Run the repo's real deduplicate_scraped_content on the fetched corpus pages,
in both renderings (innerText-approx plain text vs markdown), and measure what
it strips. Answers: (a) is dedup earning its keep, (b) does it survive markdown."""
import json
import sys
from pathlib import Path

sys.path.insert(0, "/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline/packages/scraper/src")
from scraper.utils.dedup_util import deduplicate_scraped_content  # noqa: E402

import tiktoken  # noqa: E402
ENC = tiktoken.get_encoding("o200k_base")

renders = json.loads((Path(__file__).parent / "fetched_html" / "renders.json").read_text())

SITES = {
    "steelcraft.com": [u for u in renders if "steelcraft" in u],
    "fzemanufacturing.com": [u for u in renders if "fzemanufacturing" in u],
    "lucasmilhaupt.com": [u for u in renders if "lucasmilhaupt" in u],
}

SEP = "#" * 50


def build_combined(urls, kind):
    return "".join(f"{SEP}\n{u}\n\n{renders[u][kind]}\n" for u in urls)


print(f"{'site':<22} {'rendering':<10} {'pages':>5} {'tok before':>10} {'tok after':>10} {'removed':>8} {'%':>6}")
for site, urls in SITES.items():
    for kind in ("text", "markdown"):
        combined = build_combined(urls, kind)
        deduped = deduplicate_scraped_content(combined)
        b = len(ENC.encode(combined, disallowed_special=()))
        a = len(ENC.encode(deduped, disallowed_special=()))
        print(f"{site:<22} {kind:<10} {len(urls):>5} {b:>10} {a:>10} {b - a:>8} {100 * (b - a) / b:>5.1f}%")
