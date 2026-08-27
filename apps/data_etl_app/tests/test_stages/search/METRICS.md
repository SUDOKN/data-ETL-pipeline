# The search-stage metric battery

Every number this instrument reports, what flaw it tracks, and where its
baseline came from. Mechanical metrics (M) are computed by `checks/`; judged
metrics (J) come from the RUNBOOK's agent census. **Only the recall and
tripwire metrics gate** (user decision 2026-08-26); everything else is trended.

Read every judged number beside its agreement floor, and every per-window
comparison beside the A/A noise floor: **whole-window set identity is 28.4%
and mean per-form Jaccard 0.769** on byte-identical replays, so a per-window
difference under ~25% is not a result.

## Mechanical

| id | metric | what it tracks | baseline / today |
|---|---|---|---|
| M1 | **verbatim tiers** — exact / casing-only / substring / not-in-window | fabrication at search: a form the window text does not contain | 96–97% exact; not-in-window ~1%. **Gates** above 2% |
| M2 | **length profile** — ≥6-word share, max words, line-break crossers | the "claims not designations" regression (D2 Extent) | ≥6-word ~10–19% per field, max 25 words, crossers 0. Crossers **gate** |
| M3 | **degeneration** — repetition loops, near-cap, `finish_reason: length`, unparseable | the 2026-08-16 repetition-loop family (once 2,453 repeats of one phrase) | all 0 across 96 windows. Loops and unparseable **gate** |
| M4 | **window health** — empty windows, forms per window, recursive requests | junk minted on low-yield windows; the degenerate-input probes | median 14 forms/window |
| M5 | **duplicates** — within-window repeats, casing-split families, cross-chunk families | the casing hole and the per-chunk twin seed | casing splits are expected; rescued downstream |
| M6 | **window consistency** (the Q3 instrument) | search listing a family in one window and not in another where it also occurs | 0.74 overall on 2026-08-22 bounds; reproduced 0.56–0.97 per field on current bounds |
| M7 | **cross-field overlap** — share of a field's own vocabulary another field also returned | the field-axis defect (equipments returning products/processes) | steelcraft equipments **89.9%** vs products; alecmfg equipments 63.6% |
| M8 | **sweep floor** — high-precision lexicon hits not covered by any returned form | cheap recall floor without judgment; conformity standards ids are the strongest case | per field, see `config/fields/*.yaml` |
| M9 | **coverage & geometry** — windows, bounds, excluded pages, fingerprint | comparability; the `max_chunks` truncation | fingerprint gates all trending |
| M10 | **brute health** (concept fields) | ontology labels planted by regex that never ground (`Foam`, the `Lead` substring bug) | 8 brute rows last run |
| M11 | **cost & delivery** — tokens, USD, live vs replayed | flood cost; and whether a run's numbers are new or inherited | ~$1.18 for the search stage |

## Judged (the agent census)

| id | metric | what it tracks |
|---|---|---|
| J1 | **precision composition** — every form coded, rolled up to in_field / adjacent_field / generic / junk | over-generation; the 37% products baseline |
| J2 | **judged recall** — in-window entities no form covers | the unrecoverable failure: recall lost at search is lost for good |
| J3 | **wrong-actor share** — forms whose referent is a client, supplier, lab, parent or resale inventory | mis-attribution pressure passed downstream |
| J4 | **junk families** — document titles, file extensions, UI fragments, sentence-shaped forms, undisambiguated polysemes | the named flood classes |
| J5 | **judge agreement** — two independent judges on the same forms | the floor under J1–J4; print it beside every judged number |

## Gating rule

RED = a confirmed-recall miss, fabrication above threshold, or a tripwire
regression (repetition loops, unparseable responses, line-break crossers).
Precision, junk rate, wrong-actor share and consistency are **tracked, never
gated** — the stage is recall-first by design, and a precision gate here would
push the search prompts back toward the blocklists that were deliberately
removed. An unverified expectation set gates nothing: `confirmed_recall: null`
means "not gated", never "no misses".

**How much of a recall figure is load-bearing (measured 2026-08-27).** Coverage
is credited by containment, so an entry whose acceptable forms are all
substrings of SIBLING entries' forms is credited whenever any sibling is
returned and can never independently miss. Corpus-wide that is **1,229 of 9,499
entries — 12.9%** (882 of 8,987 = 9.8% before agstech's verification misses) (`validate_expectations.py` flags them; per-subject table in
`expectations/VERIFICATION_LEDGER.md`, range 0% to 20.1%). The leniency is
deliberate and stays — tightening it manufactures false REDs — but read a
subject's recall knowing roughly a tenth of its denominator is carried by its
siblings, and check that subject's own share before treating a small recall
delta as real.

## Known leniencies in the recall metric (deliberate)

Coverage is entry-level, not per window: an entity is missed only when NO
window found it, so a quote that also appears in page footers does not charge
a miss per silent window. And containment credits a broader entry when a
narrower form is returned (`Steel` by `Stainless Steel`), so a substring-of-
its-siblings entry cannot independently fail. Both keep the gate from firing
falsely; over-generation is the judged census's job, not the gate's. Short
forms (<=3 chars) stay strict. See EXPECTATIONS_SCHEMA.md for the reasoning.
