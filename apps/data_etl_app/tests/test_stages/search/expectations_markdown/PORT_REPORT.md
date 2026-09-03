# Port report — the innerText → markdown_v2 cutover, 2026-08-29

What the cutover actually did, per subject, with the numbers it can be checked
against. `README.md` explains WHY it was a port rather than a reseed; this file
is the record of the run.

## The one-line version

**9,124 live entries over 18 surviving subjects became 10,828.** The eval set
came out of a scraper change and a re-crawl LARGER than it went in, despite
three sites being restructured and two dying — because the mechanical port cost
nothing and the agent budget went entirely into the two places the text actually
changed.

The legacy set (`../expectations/`, 9,499 entries over 20 subjects) is untouched
and still selectable with `SEARCH_EVAL_CORPUS=legacy`.

## Per subject

`ported` = carried mechanically. `to repair` = held out for an agent because a
quote no longer matched. `re-anchored`/`retired`/`disputed` = that agent's
verdicts. `final` includes the top-up pass.

| subject | legacy | ported | to repair | re-anchored | retired | disputed | final | delta |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| `101machine_com` | 41 | 41 | 0 | 0 | 0 | 0 | **61** | +20 |
| `ableengineering_com` | 85 | 68 | 17 | 16 | 1 | 0 | **177** | +92 |
| `acimachine_com` | 25 | 25 | 0 | 0 | 0 | 0 | **149** | +124 |
| `agstech_net` | 5184 | 5135 | 3 | 3 | 0 | 0 | **5333** | +149 |
| `alecmfg_com` | 135 | 135 | 0 | 0 | 0 | 0 | **251** | +116 |
| `anchor-mfg_com` | 76 | 21 | 55 | 41 | 14 | 0 | **171** | +95 |
| `blackadvtech_com` | 477 | 186 | 290 | 90 | 200 | 0 | **600** | +123 |
| `decimal_net` | 390 | 356 | 34 | 31 | 0 | 3 | **549** | +159 |
| `fzemanufacturing_com` | 490 | 489 | 0 | 0 | 0 | 0 | **619** | +129 |
| `howcogroup_com` | 356 | 347 | 9 | 7 | 2 | 0 | **481** | +125 |
| `lucasmilhaupt_com` | 429 | 30 | 399 | 43 | 355 | 1 | **448** | +19 |
| `mathewsco_com` | 261 | 261 | 0 | 0 | 0 | 0 | **309** | +48 |
| `med-tekinc_com` | 53 | 53 | 0 | 0 | 0 | 0 | **73** | +20 |
| `pradeepmetals_com` | 319 | 312 | 7 | 5 | 0 | 2 | **436** | +117 |
| `steelcraft_com` | 132 | 120 | 12 | 5 | 7 | 0 | **228** | +96 |
| `superiortech_org` | 309 | 308 | 0 | 0 | 0 | 0 | **385** | +76 |
| `tanfel_com` | 349 | 319 | 30 | 21 | 9 | 0 | **456** | +107 |
| `taylordunn_com` | 13 | 13 | 0 | 0 | 0 | 0 | **102** | +89 |
| **total (18)** | **9124** | **8219** | **856** | **262** | **588** | **6** | **10828** | **+1704** |

austinelectricservices.com and sterlingmfg.net are absent: dead domains, not in
the 2026-08-29 crawl. Their legacy expectations remain under `../expectations/`.

## How to read the two extremes

**lucasmilhaupt** ported 30 of 429 entries and retired 355. That is not a
scraper failure — the site removed its whole `/EN/Industries/*` and
`/EN/Products/*` tree, and of its 42 crawled pages only 8 carry unique body
text (34 are `[duplicate — content identical to a previously scraped page]`).
The repair agent verified each loss individually and rejected several tempting
false matches: `flux coring`'s only hit was *core* inside "At the core are our
Filler Metals", and `NATE` matched only inside "Fortu**nate**ly".

**agstech** ported 5,135 of 5,138 and gained only 55. Its site barely changed
and its inventory was already the corpus's largest and most thoroughly verified,
so there was little left to find. This is the case that most justified porting:
re-seeding it from scratch would have risked 5,135 verified entries to recover
almost nothing.

## Duplicate pages: why page count is a bad proxy for content

The dedup pass replaces a repeated page's body with a marker, so a large page
count can be almost entirely empty. **528 of the corpus's 2,885 pages (18.3%)
are duplicate markers**, and they are concentrated:

| subject | pages | duplicates | real |
|---|--:|--:|--:|
| taylordunn.com | 415 | 374 | 41 |
| lucasmilhaupt.com | 42 | 34 | 8 |
| pradeepmetals.com | 116 | 30 | 86 |
| alecmfg.com | 80 | 23 | 57 |
| superiortech.org | 34 | 15 | 19 |
| acimachine.com | 1532 | 15 | 1517 |

taylordunn's 376 "new" pages are mostly `/store/*` dealer-locator pages carrying
only homepage boilerplate; its real yield came from the `/manuals/` and
fault-code pages, which held a whole tier of legacy model numbers the innerText
crawl never surfaced (13 entries became 102).

## What the top-up pass was actually worth

Every subject gained, including the five with NO new pages at all — decimal
(+159), tanfel (+107), steelcraft (+96), mathewsco (+48) and 101machine (+20)
gained entirely from content markdown newly exposes. Recurring sources:

- **Closed accordions and inactive tab panels.** superiortech's `facilities-list`
  page alone yielded ~80 named machines (HARDINGE, OKUMA, MORI SEIKI, DOOSAN,
  MAZAK, MATSUURA …) from a nested spec list innerText could not reach.
- **Pipe tables and HTML islands.** tanfel's investment-casting alloy tables,
  pradeepmetals' grade catalogue (+73 material_caps), howcogroup's spec tables.
- **Cells the old rendering FUSED.** decimal's legacy innerText ran adjacent
  table cells together with no separator — `HipernikKovar`, `PipePlate`,
  `Leak TestingMagnetic Particle Testing`, `GTAWMicro Spot Welding`. Markdown
  renders these as `**Label:** Value` pairs, so both halves became separately
  visible for the first time.
- **Image alt text.** lucasmilhaupt's `welding` entry re-anchored onto the
  homepage hero's "woman welding" alt text.

## Defects found and fixed during the run

Three in `build_expectations.py`, all latent before this cutover, all now pinned
by `../tests/test_corpus_cutover.py`:

1. **`--merge` erased prior judgment.** A merging JSONL carries only what its
   agent found, so merging silently reset `expected_empty` to false, dropped the
   field notes, and deleted every false friend the subject had. Same hole
   existed for `subject.yaml` (profile, scrape hazards, sampling plan).
2. **Unfindable quotes were written anyway.** The script complained and appended
   the row regardless, putting 24 rows into the set that the validator then
   errored on. It now enforces the validator's own two contracts and leaves the
   entry out with a named complaint.
3. **Ids were assigned by list position, so merges produced DUPLICATES.**
   `apply_repairs` inserts in document order, so an entry holding `-0116` can
   sit at position 200 and the next new entry at position 116 claims `-0116`
   too. This corrupted agstech and blackadvtech for real; both were rebuilt.

## The agent failure mode worth knowing about

Three agents independently lost entries the same way: **windowing a quote to a
fixed width cuts the trailing word.** `flexible_pattern` bounds BOTH edges, so
such a quote never matches even though it is a true substring of the file. It
cost 122 entries on agstech, 27 on acimachine and 16 on ableengineering — all
recovered on a second pass, because the entities were real and only the windows
were broken.

It is the same rule in three guises, and all three are now stated in the briefs
with their measurements:

- a FORM must occur in its quote as a whole word (`Casting` ≠ `Castings`)
- a QUOTE must start on a word boundary (`ated & shown keen interest…`, cut out
  of the middle of *appreciated*)
- a QUOTE must end on one too

## Reproducing this

```bash
.venv/bin/python checks/port_corpus.py --out-dir <worklists>   # pass 1
.venv/bin/python checks/apply_repairs.py --slug <s> \
    --worklist <worklists>/repair_<s>.jsonl --verdicts <agent out>.jsonl
.venv/bin/python checks/build_expectations.py --slug <s> \
    --jsonl <agent out>.jsonl --merge                          # pass 3
.venv/bin/python checks/validate_expectations.py               # 0 errors
```
