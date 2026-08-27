# Search eval — run 20260825T194457 (first evaluation ever run)

Written by the assistant 2026-08-26. SUMMARY.md holds the machine-generated
table and is regenerated on every pass; this file is the analysis and is not.

## Status

Mechanical pass complete for all 12 units. Judged census complete for the 6
alecmfg units (1,048 forms coded one by one); steelcraft's 6 were judged in a
second wave — JUDGED.md is authoritative for what has landed.

**All 8 subjects' expectation sets are verified** (514 confirmed / 8 disputed
/ 0 candidate), so both run subjects now gate recall.

**Search was REPLAYED here** (requests created 2026-08-23T04:45), so these
numbers describe the 08-23 search output every run since has reused. They are
the first numbers ever taken on the CURRENT chunk bounds — every earlier judged
measurement on record used superseded geometry.

## Judged census — ALL 12 units (2,757 forms coded one by one)

| subject | field | judged | precision | junk | wrong-actor | misses |
|---|---|---|---|---|---|---|
| steelcraft | **equipments** | 93 | **0%** | 0% | 0% | 0 |
| alecmfg | **equipments** | 40 | **25%** | 0% | 0% | 0 (was misreported as 18 — see the correction below) |
| alecmfg | products | 442 | **22%** | 4% | 48% | 22 |
| steelcraft | products | 898 | 55% | 2% | 3% | 6 |
| steelcraft | process_caps | 246 | 64% | 2% | 8% | **40** |
| alecmfg | industries | 141 | 72% | 0% | 56% | 8 |
| alecmfg | process_caps | 288 | 74% | 3% | 9% | 27 |
| alecmfg | conformity_attestations | 55 | 78% | 5% | 55% | 2 |
| steelcraft | material_caps | 140 | 78% | 1% | 5% | 11 |
| alecmfg | material_caps | 82 | 79% | 0% | 1% | 6 |
| steelcraft | industries | 98 | 82% | 10% | 74% | 14 |
| steelcraft | conformity_attestations | 234 | 86% | 4% | 7% | 10 |

**Agreement floors (five units double-judged 2026-08-27):** steelcraft
equipments **100% over 93** · alecmfg equipments **100% over 39** · alecmfg
products 94% over 424 · steelcraft process_caps 94% over 231 · steelcraft
products 90% over 821. The two equipments findings — the run's headline — are
therefore confirmed at PERFECT agreement between independent readers: the 0%
and the 25% are not one agent's opinion. The seven remaining units are
single-judged and their precision figures stay provisional.

*Caveat on the table's `misses` column for alecmfg equipments (18):* those
miss records were written before the field's boundary was corrected, and
almost all of them are metrology or tooling — out of scope, not misses. Read
that cell as ~0; the mechanical recall figure (7/7) is the accurate one.

**Agreement floor: 94% over 424 shared forms** (alecmfg products, two
independent judges). A 22% precision against a 6% disagreement rate is a
result, not noise. That same 22% independently reproduces the 2026-08-22 hand
measurement of **10%** for this field on *different bounds by a different
coder* — two instruments, one conclusion.

**Read the table as three bands:**

1. **equipments is broken on both subjects — on PRECISION** (0% and 25%,
   against 64–86% for every concept field). Its recall is fine once the
   field's real boundary is applied. Nothing else looks like this.
2. **products is the volume problem** — alecmfg 22%, steelcraft 55%, and
   together they are 1,340 of the 2,757 judged forms. This is where the
   token cost of over-generation lives.
3. **the concept fields are healthy** — conformity, materials and industries
   run 72–86% in-field on both subjects.

High wrong-actor shares (steelcraft industries 74%, alecmfg industries 56%,
products 48%, conformity 55%) are **not defects**: they are client-, supplier-
and parent-owned entities correctly extracted and correctly flagged, which is
what recall-first asks of this stage. They are a downstream attribution load,
not a search error.

## Findings

- **CORRECTION 2026-08-27 — the recall half of my first equipments finding
  was WRONG, and the eval set was the thing at fault.** I reported alecmfg
  equipments recall at 53.8% with 6 misses. All six — `Zeiss CONTURA G2 CMM`,
  `CMM`, `laser interferometer`, `Renishaw probe`, `vacuum fixture`,
  `bending molds` — are metrology or workholding, and the equipment search
  prompt **explicitly excludes both**: a production machine "acts on the work
  itself, which is what separates it from equipment that only measures,
  inspects, or tests the work ... and from tooling, which is what a machine
  holds, guides, or acts through". Search was obeying its own definition.
  Seeding those entities as must-finds made **correct restraint read as a
  recall failure and fired a false RED**. Corrected: those 6 are now
  `disputed` (kept, not deleted — if the field is ever widened to include
  metrology, re-promote them), and **alecmfg equipments recall is 7/7 =
  100%**. Methodological lesson, now in EXPECTATIONS_SCHEMA.md: an
  expectation set must encode the FIELD'S DEFINED BOUNDARY, not a colloquial
  reading of the field name.

- **THE HEADLINE, restated after that correction: equipments has a PRECISION
  problem, not a recall problem.**
  *steelcraft* names no production machinery at all, so the correct answer for
  every window was the empty list — search returned **93 forms, 92 of them the
  company's own doors and frames: 0% precision, zero machines, zero misses**.
  A door is a finished good; the prompt's own gloss ("convert materials into
  finished goods") puts it plainly outside. This one is unambiguous and needs
  no interpretation.
  *alecmfg* returned 40 forms of which **25 are process names** ("CNC
  machining", "5-axis milling") and only 10 are machines — 25% precision —
  while missing nothing the field actually asks for. One window returned the
  same process under six spellings and no machine at all.
  Mechanically the same story: steelcraft equipments shares **89.9% of its own
  vocabulary with products**, alecmfg 63.6% with products and 48.5% with
  process_caps.
  **The two failure shapes are: the goods the subject makes, and the
  operations it performs — returned in place of the machines.**

- **alecmfg process_caps 92.1%** — misses `tapping`, `rapid prototyping`,
  `mold design`. **alecmfg conformity 92.9%** — miss `WPS/PQR`.
- **Window consistency reproduces the historical Q3 pattern on new bounds**:
  alecmfg industries 0.562 (0.58 on 2026-08-22 bounds), steelcraft equipments
  0.570 (0.53), steelcraft process 0.593 (0.61), alecmfg conformity 0.968
  (0.95). An independently rebuilt instrument landing on the same shape after a
  re-chunk is good evidence the metric is measuring the thing and not the
  geometry.
- **Fabrication is near zero**: max 1.02% (steelcraft industries, 1 composed
  phrase of 98 forms). Every other field 0–0.7%.
- **Every fixed-flaw tripwire holds** across all 96 windows: 0 repetition
  loops, 0 unparseable responses, 0 near-cap outputs, 0 line-break crossers.

## Two findings that CLEAR the search stage

An evaluation that only ever indicts its own stage is not measuring; these are
the cases where search is innocent and the defect belongs downstream.

- **The windstorm family is fully recalled at search.** TAS 201, TAS 202,
  TAS 203, ASTM E1886 and ASTM E1996 are all returned — E1886/E1996 as
  separate single designations from the H Series window, plus a merged
  `ASTM E1886/E1996` form from the windstorm-applications window. The
  2026-08-25 run lost this family in its OUTPUT, and the loss is therefore
  **not a search-recall failure**; it happens downstream (grounding/screening).
  This is exactly the reassignment the stage-eval split exists to make.
- **steelcraft equipments has zero misses** because the site names no
  production equipment at all. Search's recall there is vacuously complete;
  its problem is purely precision (below).

## A search-side output-shape defect worth fixing (new)

**Merged designations arrive as one string**, which is fragile for every
downstream stage that expects one entity per form: `ASTM E1886/E1996` (×2),
`FEMA P-320 & P-361/ICC500-2014 standards`, `neutral pressure testing (ASTM
E152 and UL-10B)` (×3), `Compliance with ASTM A666, ASTM E152, UL-10B, and
USL-10C`, and the nav run-on `Steel Door Institute FEMA UL Intertek Florida
Building Code`. One of them, `FEMA 361⁄320 guidelines`, uses a U+2044 FRACTION
SLASH rather than an ASCII "/", so any downstream splitter keyed on "/" will
miss it silently. 10 of 234 steelcraft conformity forms have this shape.

## Caveats binding every number above

- **Search's own A/A noise floor, measured for the first time this session**
  (`AA_PROBE_NOTE.md`): whole-window set identity **28.4%**, mean per-form
  Jaccard **0.769**; equipments/products least stable (J ≈ 0.68). Per-window
  comparisons below ~25% are unreadable.
- These rows were scored twice — before and after two matcher defects were
  found and fixed (NBSP; whitespace runs + short-form substrings). Only
  post-fix rows are kept in `metrics.jsonl`. The pre-fix pass produced one
  false RED (steelcraft industries) that the fix cleared, which is itself the
  argument for the RUNBOOK's "verify before believing" step.

## Other judged findings worth acting on

- **steelcraft process_caps has the run's worst recall: 40 misses.** Two
  systematic gaps recur across nearly every window — fire-door pressure
  testing/listing ("neutral pressure testing (ASTM E152 and UL-10B)"), missed
  in 6 of 9 windows though it appears on almost every product page, and
  boilerplate operations ("Beveled hinge and lock side edges", "dressed with
  top and bottom caps") returned in some windows and missed in the same
  sentence in others. That is an intra-run consistency failure, not a text
  problem. Its two flood classes are also now counted: **installation work
  15.4%** and **lab/test-house procedures 6.9%**.
- **steelcraft products carries a 62-form document-title flood** (6.9%),
  77% of it in two windows that re-scrape the same Literature page. But a
  blanket rejection of that list would be wrong: two confirmed must-find
  entities ("Integral Kerfed Frame", the FT Series) are recoverable ONLY
  through a document title.
- **Heavy intra-window redundancy**: steelcraft products' 898 forms reduce to
  608 distinct strings, with 77 exact duplicates inside their own window.
- **The `Lead` polyseme did not bite this run** — on steelcraft both returned
  lead forms are the genuine radiation-shielding sense, and the lead-time page
  produced no `lead` form at all.

## Owed next

1. **The equipments prompt** is the one clearly indicated fix, and it is the
   user's call (publishing is theirs). Direction the evidence supports: the
   field must reject goods the subject makes or sells, and must look for
   machines where this corpus actually names them — metrology and workholding
   lines inside case studies, not just "arsenal" sentences.
2. **Double-judge more fields.** Only alecmfg products has an agreement floor
   (94%); every other precision number in the table is single-judged and
   should be read as provisional until it has one.
3. Re-run the A/A probe (`checks/aa_probe.py`) after any search prompt change.
4. Consider the merged-designation split (now tracked as a metric) — it is
   cheap and removes a downstream failure mode.
