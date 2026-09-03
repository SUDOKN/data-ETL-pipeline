# Search-stage evaluation — run 20260901T013332

Method: **full census** (user's decision, 2026-08-31): every one of the run's
**21,176 returned forms** was judged per (window, form) against plain meaning
— 102 units, 17 subjects x 6 fields — and every window was swept for entities
the stage should have returned but did not (**1,530 miss records**). Judging
was fanned out to Sonnet subagents in bounded 150-form tasks; every unit
passed a mechanical coverage check (`verify_census_coverage.py`) before the
merge, so no silent truncation survives in these numbers. Judgments live in
`judgments/`, the merged table in `JUDGED.md`, merge-time analytics in
`checks/census_report.py`.

**Comparability: none — confounded by design.** Three search prompts were
edited 2026-08-29, the text corpus moved to markdown_v2, and window bounds
changed with it; no prior run shares a fingerprint (`none comparable` on
every scorecard). The user chose to run anyway and report as confounded.
**This run is the new baseline.**

**Reliability floor: measured 2026-09-02.** Twelve units (2 per field, 10
subjects, 1,402 forms) were independently re-judged cold (`.judge2.jsonl`),
every file verified complete against its packet. **Pooled: 74.1% exact-code,
87.6% same-bucket** — so no bucket-level judged difference under ~12 points is
readable. Excluding the two industries units the floor tightens to **85.4%
exact / 91.9% bucket**, in line with the 2026-08-28 floor (81.0%/88.7%).

The two industries units are not noise, they are unsettled definitions:
acimachine 2.8% exact / 77.3% bucket (primary coded the dealer's catalog
labels `W`, the second judge `O` — same bucket, so precision holds), and
pradeepmetals **50.4% / 58.8%** — the judges genuinely split on whether a
director's former sectors count as industries served (primary leaned
in-field, second judge leaned word-association/generic). **Until the
employment-history eval ruling lands, the industries precision figures for
bio-heavy and catalog subjects are not reliably judgeable and should not be
quoted.** Dominant cross-bucket confusion overall: borderline `B` against
`O`/`W`/`G`/`P`/`S` — the soft edge of every field, as designed. Per-unit
floors are in `JUDGED.md`; the double-judge pass replicated every headline
finding independently (the ESG window skip, the metrology blind spot, the
AKIRA SEIKI sentence, four fresh same-phrase-across-windows instances, the
bio contamination — now with named must-find entries).

**Standing caveats.** acimachine reads **1.3%** of its site text under the
40k-token cap and agstech **14%**; their numbers describe a thin, unrepresentative
slice (user chose report-with-caveat). Recall denominators exclude eval
entries whose evidence sits outside the windows the run actually read
(`confirmed_out_of_coverage`) — the eval set is compared only against text the
extraction saw, per the user's 2026-08-31 instruction.

---

## Numbers

### Recall against the eval set (mechanical, prompt-boundary standard)

| field | scored | covered | recall | out-of-coverage |
|---|---|---|---|---|
| conformity_attestations | 255 | 232 | 91.0% | 132 |
| equipments | 224 | 201 | 89.7% | 216 |
| industries | 356 | 335 | 94.1% | 249 |
| material_caps | 500 | 453 | 90.6% | 523 |
| process_caps | 867 | 798 | 92.0% | 1036 |
| products | 668 | 631 | 94.5% | 2266 |

High and flat — which is exactly why the census mattered: the defects below
are invisible to this table by construction.

Three qualifications on reading "missed" (added 2026-09-02 after the user
challenged the miss lists against the dumps):

1. **Recall is window-scoped**: an entry is charged in the window(s) holding
   its evidence quote; 18 of the 220 misses were returned by a different
   window of the same run (the F1 flicker seen from the recall side).
2. **8 more have their name returned but a narrower compound recorded as the
   only acceptable form** (howcogroup `iron` demands `Iron and Chromium`
   while `Iron` was returned) — proposed eval-set repair, awaiting veto.
3. **The LLM search is not the pipeline's only finder.** The dumps' per-chunk
   `rows` mix `provenance: "llm_round_N"` with `provenance: "brute"` — the
   mechanical ontology-vocabulary scan. 16 of the 202 never-LLM-returned
   entries were caught by brute and DID enter the pipeline (rubber, glass,
   polyethylene, titanium, Chromium...). **186 entries remain missed at
   pipeline level.** Brute sharpens F2 rather than softening it: it can only
   rescue terms already in the vocabulary, so specific designations —
   `Aluminum 356`, `Makino P-300`, the whole F2 class — have no safety net;
   the LLM search is the only finder for exactly the class it drops.

### Census of everything returned (plain-meaning standard)

| field | judged | in-field | adjacent | generic | junk | wrong-actor | misses |
|---|---|---|---|---|---|---|---|
| conformity_attestations | 1,035 | 83.7% | 7.2% | 8.2% | 0.9% | 6.6% | 92 |
| equipments | 1,570 | 62.2% | 37.2% | 0.3% | 0.4% | 36.9% | 925 |
| industries | 2,444 | 65.1% | 26.8% | 7.7% | 0.5% | 50.5% | 86 |
| material_caps | 2,528 | 74.6% | 19.3% | 5.7% | 0.5% | 8.4% | 108 |
| process_caps | 4,791 | 74.9% | 17.8% | 6.4% | 0.9% | 10.6% | 222 |
| products | 8,808 | 42.3% | 45.0% | 12.0% | 0.8% | 28.8% | 97 |
| **ALL** | **21,176** | **59.6%** | **31.2%** | **8.4%** | **0.7%** | **24.3%** | **1,530** |

Junk is near zero everywhere: what the stage returns is real site content.
The loss is (a) field-boundary leakage (products: 45% adjacent) and (b) what
never gets returned at all (the miss column, and F1–F3 below). Wrong-actor is
dominated by the two dealer/rep subjects, where flagging is CORRECT behaviour
under recall-first (acimachine 100% by design); industries' 50.5% is a real
finding (F5). Census and eval-set standards differ deliberately
(EXPECTATIONS_SCHEMA pins recall to the prompt's boundary; the census pins
codes to plain meaning so a wrong prompt boundary stays detectable) — never
quote the two side by side without naming the standard.

Mixed-provenance note: `alecmfg material_caps` (+2) and `alecmfg
process_caps` (+5) carry union-of-two-judges miss records; immaterial at
field level, but exclude them from per-unit miss comparisons.

---

## Findings

### F1 — Intra-run inconsistency is the largest measured recall defect

Mechanical metric, no judges involved: take every form the run itself
returned somewhere, find the windows of the same field run whose text
contains it (word-boundary match, ≥2 windows), and ask how often the stage
returned it where it appears.

| probe set | forms | conversion | returned everywhere it appears |
|---|---|---|---|
| all returned forms | 5,620 | 47.5% | 20.0% |
| census-confirmed in-field only | 3,076 | **54.8%** | **27.1%** |

Read the in-field row: **even for entities the census confirms belong in the
field, the stage returns them in only ~55% of the windows whose text contains
them, and fewer than a third of such entities are returned consistently.**
Some inflation remains from nav-menu strings that recur in every window (a
form returned only where substantive counts as inconsistent here), so treat
54.8% as the metric and the judge-corroborated cases as the floor — eight
units had judges independently flag the same shape: howcogroup process_caps
(heat-treat condition rows, ~half that unit's 23 misses), taylordunn products
(same dealer-directory pattern caught in window 3, missed in 2/4/5), tanfel
process_caps ("Quality Control & Inspection Services" on 7 pages of one
window, returned in every sibling window), decimal material_caps (`carbon
steel`), fze process_caps (`repairs`), howcogroup conformity (a whole ESG
document list skipped in one window, returned in others).

This is self-controlled evidence — same string, same site, same prompt, same
model, same run — so no boundary clarification can fix it, and the three
2026-08-29 prompt edits do not touch it. It is also consistent with the
2026-08-26 A/A probe (77% per-form Jaccard on byte-identical repeats): the
stage's sampling variance, compounded across windows. Remedies live in
dispatch, not wording: e.g. per-window retry-and-union, or accepting the
variance and unioning across overlapping reads.

### F2 — Category returned, specific designations dropped

Where text offers both a generic term and specific designations of the same
thing, the stage returns the generic and drops the specifics — backwards for
every downstream consumer (`Aluminum 356` identifies; `Aluminum` does not).

- acimachine equipments: **634 misses vs 387 returns**; the Jet catalogue
  window returned **0 of ~340 named models** while returning "CNC Lathes".
- blackadvtech (a manufacturer, killing the "dealer page furniture"
  objection): Makino P-300, Trumpf MB 4200, KUKA KR 16 dropped, categories kept.
- mathewsco material_caps: aluminium grades **319, 356, A357** in one
  sentence → only `Aluminum` returned; `CRS`, `HRPO` likewise missed.
- steelcraft products (2026-08-27 run): same direction.

Fields spanned: equipments, material_caps, products. The eval set could not
see this at acimachine because its expectations are sampled — **the census is
the first instrument to see it**, and 61% of all miss records (925/1,530) are
equipments, mostly this class.

### F3 — Enumerated listing blocks yield nothing; spec tables are a real, subject-local variant

Whole list-structured regions produce zero output while prose in the same
window is answered normally: agstech's networking-products page (30 of one
part's 35 misses in a single block), howcogroup's QC-instrument window
(**0 forms returned** from a window holding two named CMMs, a Shadow Graph,
boroscopes), fze's hand-tool and turf-machinery enumerations, anchor-mfg's
automation paragraph (7 misses from one paragraph).

The pipe-table hypothesis was tested twice. Corpus-wide it is refuted
(misses on `|` lines: 0.9% vs 2.9% base rate = 0.32x — acimachine's 394
non-table misses drown the pool). Per subject it is REAL where spec tables
carry values: **alecmfg 18.5% of located misses on pipe lines vs 3.1% base
(6x); mathewsco 18.2% vs 0.5% (36x)**. The markdown cutover exposed these
tables specifically so the stage could read them; on these subjects it
demonstrably under-reads them.

CORRECTION 2026-09-02: `empty_windows` DOES catch an exactly-empty answer
(it listed howcogroup's zero-form window) — what it missed was the near-empty
case (the skipped ESG page returned 1 phrase, so "empty" never fired) and the
fact that nothing surfaced the metric in a verdict. The smoke alarm is now
BUILT (user approval 2026-09-02): `low_yield_windows` in `window_metrics`
flags any responded window with >=8,000 chars of text and <=2 phrases, and
`verdict()` carries it in a non-gating `warnings` channel that run_eval
prints. On this run it fires on 84 windows, including both known bad cases;
legitimately-thin answers (lucasmilhaupt) fire too, which is why it warns and
never gates.

### F4 — products is the leakiest field: 42% in-field, 45% adjacent

The August boundary-leak finding survives the prompt edits. Document titles,
capability nouns and nav labels still flood products. Not gated (recall-only
by design), but it sets the noise floor every downstream stage inherits.

### F5 — industries fails only where the site does not label its sectors

fzemanufacturing is the control: explicit "Industries Served" pages → 489
forms, 90% in-field, **1 miss**. Subjects without labelled sector pages
(alecmfg, acimachine, agstech, ableengineering) fill the field with the
subject's own activity or page furniture instead — acimachine 23% in-field
with machine-category names coded as word-association; ableengineering drew
"industries" from a named employee's CV. The failure is inference under
absence, not extraction: 50.5% of judged industries forms carry a non-own
actor flag. Fix candidates: prompt permission to return nothing when no
sector is stated, or a downstream consumer that trusts only labelled-page
provenance.

### F6 — RESOLVED BY USER RULING 2026-09-02: metrology IS equipment

The census and the eval set disagree about the same entities under two
defensible standards (census: plain meaning; eval set: the prompt's "only
measure, inspect, or test" exclusion). **80 of the 925 equipments miss
records are metrology instruments** (decimal 37 of its 54, superiortech 15,
alecmfg 11, howcogroup 6, fze 3, others 8) — and two units (anchor-mfg,
blackadvtech) were judged under an older brief wording whose judges excluded
metrology, so their miss lists omit named CMMs their reports list. Both
readings, honestly stated:

- **metrology counts** → equipments misses stand at 925 and decimal's
  equipments is a 54-miss failure;
- **metrology excluded** → 845 misses (anchor-mfg/blackadvtech become
  lower bounds), decimal drops to 17.

**Ruling (user, 2026-09-02): "metrology is a form of equipment I want to
have."** Consequences, applied same day:

- The prompt's measure/inspect/test exclusion is now the DEFECT; a prompt
  edit is drafted and awaiting approval. Until it ships, every metrology
  omission is a fair miss.
- The 925 equipments miss count STANDS (metrology included); anchor-mfg and
  blackadvtech miss lists are lower bounds (their judges excluded metrology
  under the old brief wording and their reports name the excluded CMMs).
- 10 disputed eval entries whose dispute cited the scope exclusion were
  promoted back to confirmed with provenance (alecmfg 4, superiortech 5,
  decimal 1); two metrology-named entries stay disputed for unrelated
  evidence-quality reasons. Recomputed on this run's output: alecmfg
  equipments recall 100% -> 63.6%, superiortech 82.1% -> 76.7%, decimal
  93.8% -> 88.2%. Those drops are the ruling making the field honest, and
  they are the recovery target for the prompt edit.

Related refile, applied to the reading (files untouched): 12 of anchor-mfg
process_caps' 17 misses are equipment/instrument nouns (robots, assembly
cells, PLCs, DCS, CMMs, trim press) — as process_caps misses they are
mis-filed; that unit's true process-vocabulary misses number ~5.

### F7 — 12 units say "OK" because there was nothing to gate on

Recall-only gating shows OK for any unit with zero confirmed entries in
coverage. The census looked at those units for the first time: med-tekinc
products **0/39 in-field**, steelcraft equipments **0/78**, taylordunn
equipments **0/49**, med-tekinc conformity 0/4 — all verdict OK. These are
structural blind spots of the recall-first design, not regressions; they are
listed in `census_report.py` section E and belong in any future "expand the
eval set here first" queue.

### F8 — the stage CAN return nothing; its failure mode is substitution

lucasmilhaupt equipments: no equipment on the site, **0 forms returned, 0
misses found** — correct restraint, falsifying August's "never says none".
steelcraft, same input condition (no equipment on site), returned 78 of its
own doors and frames. The stage stays silent when the page is thin and
substitutes the subject's own goods when the page is rich but off-topic — so
"return nothing when nothing qualifies" wording would not help; the defect is
substitution under pressure.

### F9 — instrument defects found and fixed while building the census

1. **Judge agents silently truncate at the 64,000-output-token ceiling** —
   measured 0%/34%/72% of forms skipped by the first three returning agents,
   all self-reporting success; one later agent surfaced the explicit error.
   Fixed by bounding tasks at 150 forms (`split_judge_packets.py`) and
   verifying every unit (`verify_census_coverage.py`). **The 2026-08-27
   census (2,757 forms) was never coverage-checked and its precision figures
   carry unknown truncation risk.**
2. `repetition_loops` reads the parsed phrase list, so all 6 genuine
   degeneration loops this run (e.g. `"products"` x1,900) reported as zero
   loops; `unparseable_windows` caught them instead. Scan raw text when a
   window is unparseable.
3. `merge_judgments.py` would have silently dropped every `__partNN` file
   (field parsed from filename); `assemble_census.py` now reconciles parts
   first.
4. Packet exports switch to double quotes for forms containing apostrophes;
   a single-quote regex dropped exactly 47 forms until fixed. Counts now
   reconcile at 21,176 everywhere.
5. Two pre-ledger double-assignments produced mixed-provenance files (noted
   above); an in-flight ledger now prevents reassignment.

---

## Proposed eval-set changes — AWAITING USER VETO, nothing applied

1. **acimachine conformity `CSA/CUS`** — unreachable: the quote occurs
   nowhere in any window text (judge grepped all 11 windows). Mark disputed.
2. **pradeepmetals employment-history sweep** — `furnaces` and `industrial
   gases` trace to a director's personal bio; August disputed instances, the
   class was never swept. Dispute every entry anchored in the
   directors'/board biography section.
3. **pradeepmetals conformity charter/governance documents** — incorporation
   certificates and internal policies are not third-party conformity
   attestations; review the entries sourced from the Charter Documents and
   Policies pages.
4. ~~Cap future corpus seeding at the production read budget~~ —
   **WITHDRAWN, superseded by user ruling 2026-09-01:** entries beyond a
   run's read coverage are *reserved*, not defective — they sit out of the
   recall denominator (already mechanical: `confirmed_out_of_coverage`,
   2,266 products entries this run) and activate automatically, gating AND
   open to critique, whenever a run's coverage reaches them. Recorded in
   EXPECTATIONS_SCHEMA.md; seeding beyond the current budget stays allowed.

## Recommended order of work

1. ~~User ruling on F6~~ — DONE 2026-09-02: metrology is in-field.
   Applied to the eval set (10 promotions); prompt edit drafted.
2. ~~This run's double-judge pass~~ — DONE 2026-09-02: 74.1% exact /
   87.6% bucket over 1,402 forms (85.4%/91.9% excluding the two contested
   industries units).
3. **F1 dispatch experiment** — retry-and-union on one subject's windows;
   the conversion metric in `census_report.py` is the before/after gauge.
4. **F3 tripwire** — flag 0-form windows above a text-length floor; cheap,
   mechanical, would have caught howcogroup's QC window.
5. Eval-set votes (above), then the F7 list as the seeding queue for thin
   units.
