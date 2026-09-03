# Scraper output format sample — Markdown with HTML-island tables (2026-08-28)

**Status: BUILT 2026-08-28** (same day; decision → measurement → implementation).
The production converter is `packages/scraper/src/scraper/utils/html_to_markdown.py`
(its module docstring is the canonical rendering contract; this folder is the
evidence it cites). Landed with it:

- `ScraperService(output_format="markdown"|"text")` — one flag, two frozen
  renderings, markdown default (`url_scraper_service.py`); the envelope is
  unchanged in both modes.
- The latent fallback bug fixed: JS innerText is now gated on an empty
  primary read instead of unconditionally overwriting it.
- Format provenance: `ScrapingResult.text_format` → S3 `text_format` object
  tag → `ScrapedTextFile.text_format` (None = pre-cutover legacy object).
- Viewport pinned to 1920x1080 (`chrome_driver_manager.py`) — none was set
  before; headless default was 800x600.
- Snippet catcher (`core.utils.aggregation_fold`): no mode flag needed — the
  clip is line-oriented; markers stay in snippets as location signals;
  decoration lines (`|---|` separator rows, `-----` dividers) are not
  sentence units, so a radius-1 clip around a table data row reaches the
  header row.
- Tests: 35 converter contract tests, toggle + fallback-regression tests,
  markdown dedup-compat fixtures, 4 markdown-shape fold tests (244 green).
- `beautifulsoup4>=4.12` added to the scraper package.

This folder holds the worked sample of the rendering plus the measurements
that motivated the format choice.

## The format in one paragraph

The page-block **envelope is unchanged**: 50-hash separator line, URL line,
blank line, body. Only the body's rendering changes. The body is **Markdown**
— headings `#`–`######`, `-`/`1.` lists with indentation for nesting, pipe
tables, `**term:**` definition lists, image alt text inline, link text kept
with URLs dropped. The **one place HTML survives** is a table whose structure
Markdown cannot express (a `rowspan`/`colspan` merged cell, or block content
inside a cell): that table alone is emitted as a sanitized HTML island —
structural tags only, all attributes stripped except `rowspan`/`colspan` —
inside the surrounding Markdown. GitHub-flavored Markdown explicitly permits
embedded HTML blocks, and LLMs parse the mixed input natively. Scripts,
styles, SVG, iframes, and form controls are dropped. Content that is in the
DOM but not rendered (closed accordions, inactive tab panels) IS included —
today's innerText silently omits it.

No new collisions with the pipeline's text contracts: Markdown headings are at
most 6 hashes and always followed by text, so `floor_scan`'s separator regex
(`#{10,}` alone on a line) cannot match them; URLs are dropped from links, so
no new bare-URL lines (page barriers) appear inside bodies.

## Files

- `proposed_scrape_output.txt` — the deliverable sample: 4 real
  fzemanufacturing.com pages, fetched 2026-08-28, converted by the prototype,
  then passed through the repo's real `deduplicate_scraped_content`. This is
  byte-for-byte the shape that would land in S3. 5,170 tokens.
- `current_output_for_comparison.txt` — the same 4 pages rendered
  innerText-style (today's shape), same dedup. 5,102 tokens. Diff the two:
  the Markdown version costs +1.3% tokens.
- `escape_hatch_demo.txt` — a SYNTHETIC merged-cell spec table (the 15 real
  pages contained zero `<table>` elements) run through the converter: the
  rowspan/colspan table stays as an HTML island, the simple table beside it
  becomes a pipe table.
- `format_tokens.py` — the stdlib-only prototype converter + token
  measurement. Caches fetched HTML beside itself; re-runnable.
- `make_sample.py`, `dedup_experiment.py` — sample assembly and the dedup
  necessity/compatibility measurement.

## Measurements (15 pages: steelcraft 8, fzemanufacturing 4, lucasmilhaupt 3)

Token cost per rendering (o200k_base, totals):

| rendering | tokens | vs text |
|---|---|---|
| raw HTML as served | 211,470 | 12.4x (range 6.2–23.1x) |
| sanitized structural HTML | 25,841 | 1.52x |
| **Markdown + HTML-island tables** | **19,319** | **1.14x** |
| plain text (innerText shape) | 17,011 | 1.00x |

Raw HTML is ruled out: at 12.4x, the `wide` strategy's 2×20k-token budget
holds ~3 pages instead of ~35.

Structure found on those pages: **0 tables, 0 merged cells** (modern
manufacturer sites build "tables" from styled divs), 143 nested `<ul>`/`<ol>`
(mostly nav menus, which dedup strips anyway — in-content nesting is rarer),
4 `<details>` accordions (content innerText loses when closed).

Dedup (the repo's real code) on both renderings:

| site | pages | removed (text) | removed (markdown) |
|---|---|---|---|
| steelcraft.com | 8 | 46.2% | 53.5% |
| fzemanufacturing.com | 4 | 21.0% | 23.5% |
| lucasmilhaupt.com | 3 | 42.8% | 48.3% |

Dedup is needed (nav/footer is 21–46% of scraped tokens) and works on
Markdown unchanged. **Post-dedup, Markdown costs only +1–4% over today's
text.** The 93-test dedup suite passes untouched.

## Known prototype cosmetics (a production converter fixes these)

- FIXED 2026-08-28 (user-spotted): empty list items — e.g. a carousel's
  `<ol class="carousel-indicators">` pagination dots — rendered as bare
  `1.`–`5.` markers. Rule added: a list item whose content renders empty
  emits nothing. innerText never showed these (nothing rendered to read),
  so without the rule the conversion was minting new noise.
- Adjacent inline links concatenate without a separator ("Explore Our
  CapabilitiesRequest a Quote" on the fze homepage) — the prototype has no
  CSS awareness; innerText breaks these because the anchors are block-styled.
- The HTML-island serializer is airy (blank lines between cells).
- Form field labels render as list items (they are `<li>` in the source);
  whether to drop `<form>` subtrees is an open tuning call.
- Fetched HTML is the served page, not the Selenium-rendered DOM; rendered
  raw HTML would be larger (strengthens the anti-raw-HTML case), text/Markdown
  similar on these server-rendered sites.

## Addendum 2: the corpus reseed (2026-08-29)

`reseed_markdown_corpus.py` re-scraped all 20 golden-corpus subjects through
the shipped code (`output_format="markdown"`, production settings: 5 browsers,
depth 5) into `apps/data_etl_app/tests/test_stages/sample_scraped_markdowns/`.
**36 minutes** for the whole corpus; the 1,430-page outlier acimachine.com
finished in 14 minutes without hitting the timeout.

**17 subjects usable — 2,819 pages, 3,496,216 tokens**, carrying 75,221
headings, 21,246 bullets, 10,502 pipe-table rows and **30 HTML islands**. The
islands matter: the 144-page pre-cutover sample found ZERO merged-cell tables
and concluded the escape hatch was insurance. Across the full corpus it fires
30 times on 7 subjects (taylordunn 11, pradeepmetals 8, tanfel 8, acimachine /
alecmfg / steelcraft 1 each) — the hatch is load-bearing, not theoretical.

The clearest single win is howcogroup.com's alloy chemistry: 409 pipe-table
rows where the legacy file has none, e.g.

    | | C | Si | Mn | P | S | Cr | Mo | Fe |
    | Min | 0.18 | 0.25 | 0.25 | . | . | 12.50 | . | 0.00 |
    | Max | 0.22 | 1.00 | 1.00 | 0.020 | 0.005 | 14.00 | . | 0.00 |

Per-subject counts are in `reseed_run_record.json` (it also holds every stat
needed to publish without re-crawling).

### 3 subjects are NOT usable — and none of it is the format's doing

Quarantined under `sample_scraped_markdowns/_unusable/` (with a README) and
marked `publish_blocked` in the run record:

- `sterlingmfg.net` — lapsed; now a GoDaddy parking page (92 tokens). Content
  moved to eptam.com long ago (the pinned legacy file is full of eptam URLs).
- `austinelectricservices.com` — dead; a WP Engine "not configured" 404 (121
  tokens). Content moved to austin-companies.com/electrical/. Its HTTPS cert
  is also invalid for its own hostname.
- `superiortech.org` — unreachable (connect timeout on https/http/www, twice).

The first two are the dangerous ones: both clear `ScrapingResult.is_valid()`
(>30 tokens, >80% success, no timeout), so **without the explicit
`publish_blocked` veto they would have been uploaded and had the Mongo pointer
moved onto them**, silently replacing a real subject's text with a
placeholder. Re-pointing those subjects to their new domains is a data
decision (it changes `subject_unique_id`, the S3 key, and every ground-truth
anchor), so it is left for a human.

### Pre-existing gap found on the way

A subject with a broken TLS certificate is unscrapeable even though Chrome is
launched with `--ignore-certificate-errors`: the pre-flight
`get_final_landing_url` uses cert-verifying `requests` and throws before a
browser starts. Unfixed — changing verification in shared code is a security
decision. `--start-url http://...` is the per-run workaround.

### Page-count drift worth knowing before re-seeding evals

Re-crawling found materially different page counts on several subjects
(new vs pinned): taylordunn 415 vs 31, ableengineering 50 vs 27, howcogroup
116 vs 97, blackadvtech 25 vs 87, lucasmilhaupt 42 vs 69 (88% success — its
failures are `Empty content after extraction` on text-free pages). Sites
changed between pinning and now, so any recall delta measured against the old
baselines mixes format change with site drift.

### Publishing is still PENDING

The S3 upload + Mongo pointer step has not run: it was denied by the
permission classifier and is awaiting user approval. Nothing has been written
to S3 or Mongo. When approved:
`python reseed_markdown_corpus.py --upload-from-local` (no re-crawl —
the record holds every needed stat), and `--revert` restores the pointers
from `revert_pointers.json`.

## Decisions (user, 2026-08-28)

1. Format CONFIRMED: Markdown + HTML-island tables.
2. Transition: DIRECT CUTOVER — no dual-emit. The user will revise the eval
   corpora/baselines; prompts are expected to need little change.
3. IN SCOPE for the implementation: the snippet-catcher logic (the code that
   takes a phrase occurrence and clips the surrounding sentence/context for
   snippets) — Markdown changes what a "sentence" looks like around bullets,
   headings, and table rows.
4. The latent fallback bug in `_extract_text_with_fallback` (the "fallback"
   unconditionally overwrites the primary extraction) is fixed as part of
   this change (agreed earlier 2026-08-28).
5. Still open: converter library install (`markdownify` or `beautifulsoup4`;
   neither is in the venv). Also owed: pin an explicit desktop viewport in
   the driver factory (none is set today — headless default is 800x600).

## Addendum 3: markdown_v2 + manifest v2 + the crawl-4 reseed (2026-08-29)

Between Addendum 2 and this one, three things shipped (all user-decided), and
the corpus was re-crawled through them (crawl-4, 34 minutes, 19/20 subjects,
user instruction: "stop the current crawl and restart with the new manifest
and the new code, with include PDFs turned off"):

1. **`markdown_v2` rendering** (`html_to_markdown.py`): nav subtrees collapse
   to a single `Navigation: Home · About · …` line (kept as location signal —
   the user chose this over dropping nav outright), consent/cookie UI dropped
   by attribute + vendor-prefix evidence (never by the word "cookie" — food
   machinery legitimately deposits cookies), `[hidden]`/`aria-hidden` subtrees
   dropped, consecutive-duplicate image alts collapsed (the dedup-vote breaker
   on alecmfg).
2. **Manifest v2** (`scrape_manifest.py`, written beside every scrape): PRE-dedup
   per-page sha256 + chars + depth, the URL sets with order-independent set
   hashes, sitemap lastmod claims, `skipped_by_extension` (SKIP_EXTENSIONS
   links used to vanish without trace), `pdfs`, `include_pdfs`.
3. **PDF pipeline, flagged OFF** (`pdf_to_markdown.py` + `include_pdfs=False`
   on the service): pdfplumber text+pipe-tables, magic-byte check, scanned-PDF
   detection, caps (20MB / 40 pages / 200k chars / 50 PDFs per site).

### Crawl-4 corpus: 18 usable subjects

superiortech.org came back up (34 pages, 9,211 tokens — its outage was
transient, as the quarantine README guessed), so the corpus is now 18 usable
of 20. The two dead domains stay quarantined; **the `--force` veto-erasure
trap fired exactly as documented** (the rerun rebuilt sterlingmfg's entry
with `valid: true` for its 92-token parked page) and the `publish_blocked`
vetoes were re-applied post-run. Every corpus file now has a
`<etld1>.manifest.json` sidecar (all version 2, all `include_pdfs: false`).

### Rendering delta, v1 → v2 (same page counts, crawl-2 vs crawl-4)

**−22.2% tokens** ex-acimachine (948,154 → 737,554). Where it comes from:

| subjects | v2 vs v1 tokens |
|---|---|
| chrome-heavy: alecmfg, ableengineering, taylordunn, steelcraft | **−45% to −54%** |
| moderate: pradeepmetals, fzemanufacturing, tanfel | −10% to −16% |
| static/simple: 101machine, anchor-mfg, mathewsco, med-tek, howcogroup, acimachine, agstech, decimal | −0.6% to 0.0% (five byte-identical) |
| lucasmilhaupt | +14% — site dynamics, not rendering (81% A/A stability) |

The zero-delta rows are the proof the v2 rules only remove chrome: on sites
with no nav-repeat/consent problem, v2 output is byte-identical to v1.
(blackadvtech's +289% is crawl variance — crawl-2 discovered 25 pages, both
later crawls found 57 and agree 100% on their hashes.)

### A/A pre-dedup hash stability (crawl-3 vs crawl-4, hours apart)

The manifest's core design bet — hash PRE-dedup so a page's fingerprint
depends only on its own DOM — is validated: **95.2% of common URLs
byte-identical** (612/643, ex-outliers) vs the 26.6% measured post-dedup
before. 7 of 12 comparable subjects ≥98.8%. The two annotations:

- `mathewsco` (2 vs 25 pages) is a stopped-crawl artifact — crawl-3 was
  killed mid-subject; its 2 finished pages match 100%.
- `acimachine.com` is genuinely unstable: **8.7%** (134/1,532). Diffing an
  unstable page showed why — its equipment-catalog widgets populate
  asynchronously, so the brand/category tree is present or absent depending
  on when the DOM was captured (pages change in BOTH directions,
  1,573→7,860 chars here, 4,185→1,421 there; token total moves only −0.1%
  because the lottery cancels in aggregate). The scraper captures at
  page-load with no wait-for-network-idle — pre-existing behavior, now
  measurable per-page for the first time.

### acimachine is 76% of the corpus

2,531,501 of 3,331,197 usable-corpus tokens are one used-machinery dealer's
1,532-page catalog, and its pages are render-timing-unstable (above). Any
eval that walks the whole corpus pays 2.5M tokens for it. Whether to cap,
sample, or keep it is a corpus-composition decision — flagged, not made.

### PDF inventory, corpus-wide (from `skipped_by_extension`)

**1,130 PDF links on 13 of 18 subjects** — the static-sample estimate (78)
was off by 14x. Skewed: pradeepmetals 526 (an Indian public company's
financial-results filings, not manufacturing data) + agstech 419 (product
brochures) are 84% of it. The tail is the extraction-relevant matter:
alecmfg's IATF 16949 certificate, decimal's AS9100 certificate, equipment
lists on fzemanufacturing and blackadvtech, howcogroup policy documents.
`MAX_PDFS_PER_SITE = 50` would cap both outliers if the flag were flipped —
the cap is load-bearing. (One pradeepmetals link reads `hhttps://…` — a typo
in the site's own href, faithfully recorded; inventory = what the site
declares, not what resolves.) Other skipped extensions: 186 .jpg, 23 .png,
8 .xls, 3 .docx, 3 .pptx.

### Publishing: still PENDING (unchanged)

Nothing written to S3 or Mongo. The local corpus + manifests + run record
hold everything `--upload-from-local` needs when the user gives the word.

## Addendum: full 20-manufacturer evaluation (2026-08-28)

Ran the same evaluation across ALL 20 golden-corpus subjects (page URLs taken
from the pinned scraped texts, up to 8 per site, fetched live): 144 pages
fetched, 12 failed (pages 404 since pinning; sterlingmfg.net's pinned text
redirects to eptam.com URLs). Full table in `all20_report.txt`, raw numbers in
`all20_results.json`, script `measure_all20.py`.

Headline totals:

- raw HTML: **43.2x** plain-text tokens (per-site range 4.0x–178.0x; the
  worst sites carry huge inline JS/CSS payloads). Raw HTML stays ruled out.
- Markdown pre-dedup: 1.07x text (range 1.01–1.21x).
- **Markdown post-dedup: 1.032x** — the structure costs 3.2% tokens
  corpus-wide.
- Dedup removes 26.9% (text) / 29.3% (markdown) of tokens across the corpus;
  it strips slightly MORE from markdown on most sites. Three sites showed 0%
  header/footer dedup (agstech, anchor-mfg, austinelectric) — no common
  header/footer found on the static pages; faithful, not a failure.
- Real `<table>` elements: 12 across 144 pages (howcogroup 6, superiortech 3,
  acimachine/austinelectric/mathewsco 1 each). **Merged-cell tables: 0** —
  the HTML-island escape hatch never fires on this corpus sample; it stays
  (deterministic + cheap) as insurance.
- `<details>` accordions: 53, concentrated on alecmfg (32) and lucasmilhaupt
  (21) — real content today's innerText silently loses on 2 of 20 sites.

Caveat: fetches are the served (static) HTML, not the Selenium-rendered DOM.
For JS-heavy sites the static pages under-represent rendered content — the
two sites with 85–87% dedup (acimachine, blackadvtech) look like JS shells
serving near-identical static pages, so their per-site ratios are least
trustworthy. Rendered raw HTML would be LARGER (anti-raw case strengthens);
text/markdown ratios on server-rendered sites (the majority here) transfer.
