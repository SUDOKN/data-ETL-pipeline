"""Build the end-to-end format sample: real corpus pages -> prototype converter
renderings (already in renders.json) -> the repo's real dedup -> final files.
Also runs a synthetic merged-cell table through the converter to demo the
HTML-island escape hatch (the real sample pages contained zero tables)."""
import json
import sys
from pathlib import Path

sys.path.insert(0, "/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline/packages/scraper/src")
sys.path.insert(0, str(Path(__file__).parent))
from scraper.utils.dedup_util import deduplicate_scraped_content  # noqa: E402
from format_tokens import parse, to_markdown  # noqa: E402

import tiktoken  # noqa: E402
ENC = tiktoken.get_encoding("o200k_base")

HERE = Path(__file__).parent
renders = json.loads((HERE / "fetched_html" / "renders.json").read_text())

SEP = "#" * 50
SITE = "fzemanufacturing"
urls = [u for u in renders if SITE in u]

out = {}
for kind in ("markdown", "text"):
    combined = "".join(f"{SEP}\n{u}\n\n{renders[u][kind]}\n" for u in urls)
    deduped = deduplicate_scraped_content(combined)
    out[kind] = deduped
    print(f"{kind}: {len(urls)} pages, {len(ENC.encode(deduped, disallowed_special=()))} tokens post-dedup")

(HERE / "proposed_scrape_output.txt").write_text(out["markdown"])
(HERE / "current_output_for_comparison.txt").write_text(out["text"])

# --- escape-hatch demo: a merged-cell spec table (synthetic; corpus had none) ---
SYNTH = """
<h2>Machining Capabilities</h2>
<table>
  <tr><th rowspan="2">Machine</th><th colspan="3">Work Envelope (in)</th><th rowspan="2">Qty</th></tr>
  <tr><th>X</th><th>Y</th><th>Z</th></tr>
  <tr><td>Haas VF-2SS</td><td>30</td><td>16</td><td>20</td><td>3</td></tr>
  <tr><td>Mazak QT-250</td><td colspan="3">Ø10 x 21 between centers</td><td>2</td></tr>
</table>
<h2>Tolerances</h2>
<table>
  <tr><th>Process</th><th>Standard</th><th>Precision</th></tr>
  <tr><td>Milling</td><td>+/- 0.005"</td><td>+/- 0.0005"</td></tr>
  <tr><td>Turning</td><td>+/- 0.003"</td><td>+/- 0.0002"</td></tr>
</table>
"""
demo = to_markdown(parse(SYNTH), hybrid_tables=True)
(HERE / "escape_hatch_demo.txt").write_text(demo)
print("--- escape hatch demo ---")
print(demo)
