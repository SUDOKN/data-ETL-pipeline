# Run summary — 20260903T212112 (search stage, full census)

**This run is the declared BASELINE for search-stage quality on the current
architecture**: general-only prompts, v3 markdown corpus (100k cutoff),
heading propagation, hardened synthesis upstream, `search_union_pass=True`,
divisor 4. All future search A/Bs compare here.

Census depth (user-selected): every returned form judged — 25,440 forms,
108 units, 18 subjects, 260 judge tasks; 36-packet double-judge; personal
slice verification (305 rows, 1 correction). Details in `FINDINGS.md`,
`JUDGED.md`, `CENSUS_REPORT.md`, `CENSUS_NOTES.md`.

**Eval-set evolution applied 2026-09-04 (user votes, all five executed;
record in CENSUS_NOTES "Eval-vote resolutions"):** the absent-must-find /
nav-evidence / CSA-CUS flags all closed with ZERO edits (reserved-by-coverage
or already `out_of_snapshot` — none ever gated); 11 too-narrow
acceptable_forms repaired; the 16 zero-gate units reseeded/verified through
the two-pass protocol (7 agents + assistant second pass). Post-evolution
mechanical state on this same run: **61 OK / 47 RED; recall vs the evolved
set 95.4% overall on 2,960 gated entries** (conformity 98.6 / equipments
93.1 / industries 95.1 / materials 96.4 / process 95.3 / products 95.0).
Numbers in the table BELOW are the census-time values against the
census-time eval versions; the scorecards carry both.

## Floors this run's numbers are read against

- **A/A reproducibility** (fresh pairs, production params, $1.51 sample):
  identical phrase sets **73/100** (old floor 28.4%); per-form Jaccard
  **0.897** (old 0.769); 19/100 windows deterministically empty.
- **Double-judge agreement** (2,909 shared forms): raw code **85.3%**,
  rollup **89.7%**, actor **91.7%**. Axes: B↔V family/specific, G↔O
  industries, G↔S products, N↔U taper facets, client↔own on
  served-industries prose.
- **Mechanical eval**: 62 OK / 46 RED at census time on the recall-only
  gates — the REDs ARE the confirmed-recall misses this census judged.
  (An earlier revision said "RED=0/OK=108"; that conflated the packet
  COVERAGE check — 260/260 packets, 108/108 units, 0 problems — with the
  verdicts. Corrected 2026-09-04.)

## Per-field verdicts (precision on shared rollup · recall vs eval set)

| field | in-field | rollup floor | recall | verdict |
|---|---|---|---|---|
| conformity | 77.6% | 89.0% | 98.5% | Healthy. Junk 4.4% is doc/patent noise; governance-doc footer class bucketed. |
| equipments | 65.5% | 94.6% | 92.6% | Lowest recall, and it is prompt STANCE: declines facet/heading machine lists products captures on identical text (F2). Adjacent 33% = product-name flood (D) on door/vehicle makers. |
| industries | 62.6% | 92.0% | 95.1% | Healthy read: pooled not-a-sector share 33.1% (adjacent O/W + generic G — pooling absorbs the measured O↔G judge axis); 50.8% non-own actor is the field's client-sector nature, flags stable. |
| material_caps | 67.6% | 84.6% | 95.2% | Part-name flood N (ableengineering 74% vs 33% on 09-01) is the top precision item; loosest judge floor (B↔V). |
| process_caps | 67.9% | 90.6% | 95.3% | Healthy; generic 12.8% is capability-prose noise. 1 prompt-echo case census-wide (F6). |
| products | 32.4% | 85.0% | 94.9% | In-field share low BY STRUCTURE: 53.9% adjacent = specs (S), processes (P), client products (C) swept at scale (10,301 forms, 40% of census). Junk only 0.7%. Field-boundary question, not noise. |

## Deltas vs the 09-01 census (only where comparable)

- Intra-run in-field conversion **54.8% → 53.0%**: the union pass did NOT
  move the top defect (probe-level pass1≈pass2; formal per-pass diff owed).
- Junk overall stays low; pipe-table miss effect essentially gone after the
  markdown cutover (base rates <1–8.5%, shares track them).
- Split-window phantom misses now mechanically cancelled (31% of claims —
  1,007/3,280); 09-01-style raw miss counts are not comparable without it.
- material_caps N-share on ableengineering 33% → 74% (corpus v3 heading
  propagation over part lists and/or union pass; per-pass attribution owed).

## The original question (taylordunn "material handling") — answered

Dealer-directory company names ("PAPE MATERIAL HANDLING INC", …) swept into
material_caps by substring; all coded U/junk actor=partner by two
independent judges. Precision fix belongs in the field prompt's judge-by-use
doctrine vs directory listings; scraping is not implicated.

## Skipped / owed (explicit)

- Formal run-wide union-pass unique-yield diff (`>pass>2>` custom_ids).
- `search_divisor` A/B (F4's dense-window ceiling is direct motivation).
- Per-window exact-string dedupe fix (F5) — not yet implemented.
- Full-pipeline run on v3 (this run is search-only) + synthesis eval
  designation-preservation dimension.
- Eval-set evolution votes (blackadvtech/howco absent must-finds, nav-
  evidence seeds, CSA/CUS, 16 zero-gate units) — user-vetoable, not silent.
- Judge2 independence caveats: a few judge2 agents read sibling judge2 files
  to calibrate conventions; two glimpsed judge1/CENSUS_NOTES content. Floors
  are if anything slightly optimistic.
