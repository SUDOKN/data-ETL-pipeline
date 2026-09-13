# Synthesis-stage evaluation

The permanent, evolving evaluation of the pipeline's **synthesis stage** — the
LLM pass that writes one evidence-grounded paragraph per group record (a focal
form plus its verbatim snippets, read against the chunk of site text they came
from), sitting between the aggregation fold and the grounding/screening tail.
Ported 2026-09-05 to the flattened wire (`snippets: [str]`, no per-snippet
location; location is code on the fold — see RUNBOOK §Definitions). Built 2026-08-26 from the defect history of
runs `20260823T195031`…`20260825T194457`. Follows the sibling stage evals'
convention (`search/`, `grounding/`): **RUNBOOK.md** is the protocol,
**TAXONOMY.md** is the judgment contract, `checks/` is the deterministic code,
`expectations/` is the evolving eval set, `history/` is append-only results.

Scope discipline: this evaluation judges the synthesis **only**. Upstream junk
(search precision) and downstream misuse (grounding inference) are *noted*
(`not_a_product` flags, upstream-leak probes) but scored against their own
stages, not against the paragraph.

## Layout

| path | what | committed |
|---|---|---|
| `RUNBOOK.md` | the protocol the assistant follows on "run the synthesis eval" | yes |
| `TAXONOMY.md` | J1–J7 dimensions + per-field extensions; its hash is `taxonomy_version` | yes |
| `JUDGE_PROMPT_TEMPLATE.md` | the judge agent prompt with the accumulated calibration rulings; each run copies it filled to `history/runs/<run_id>/JUDGE_PROMPT.md` | yes |
| `CANDIDATE_DIMENSIONS.md` | found-but-not-yet-promoted defect classes | yes |
| `checks/` | `run_eval.py` CLI, loading, Mongo pull, lints, mechanical checks, ledger, scorecards | yes |
| `config/common.yaml` | thresholds, prices, born-from baselines | yes |
| `expectations/<subject>/` | `subject.yaml` + one file per field — the evolving probe set | yes |
| `history/runs/<run_id>/` | scorecards, `REPORT.md`, `pending/` work orders, `verdicts/` | yes |
| `history/judgments/` | cumulative content-keyed verdict ledger (JSONL per subject×field) | yes |
| `history/metrics_scoreboard.csv` | one row per metric per run — the trend view | yes |
| `evidence_snapshots/` | wire payloads pulled from Mongo (hold site text) | **no** (gitignored) |
| `tests/` | pytest guarding this harness's own code, not the pipeline | yes |

## Quick start

```bash
# from this directory, with the repo .venv
../../../../../../.venv/bin/python checks/run_eval.py --run <run_id> --pull
#   … then judge per RUNBOOK.md …
../../../../../../.venv/bin/python checks/run_eval.py --run <run_id> --finalize
```

`--pull` snapshots the run's synthesis wire evidence from Mongo (needs the
repo `.env` Mongo URI) — **do it promptly, a scoped delete erases it**. Omit
`--run` to use the newest dump directory. The snapshot holds each chunk's
text once (`chunk_text/`), the static once per `pv` (`system_prompts/`), the
per-record snippet index, and the manufacturer name each subject's prompts
carried (`subject_names.json` — the harness's first source for
`subject_name`, ahead of `expectations/` and the business_desc dumps).

Harness self-tests: `pytest apps/data_etl_app/tests/test_stages/synthesis`
(add `-m integration` with `SYNTH_EVAL_RUN=<run_id>` for the invariants over a
real run's dumps).

## What the numbers mean

- **Invariants** (INV-1 delivery, INV-2 pv-witness, INV-3 contract byte-copy,
  INV-4 empty-groups, INV-5 synthesis-block-present) must always pass; a
  failure is a pipeline or instrument break, not a quality trend.
- **Metrics** are tracked, never gated per run. The grounding reproducibility
  noise floor is 13–19% row-level divergence on byte-identical input;
  **synthesis's own A/A floor is unmeasured** — until it is, treat any delta
  below ~15% relative as unreadable and prefer paired (same-`ud=`)
  comparisons. `own_name_record_rate` is an identification counter (saturated
  at ~100%), **never** a defect count.
- **Judged rates** (J1 faithfulness, J2 under-claim, J3 entity identity, J4
  party attribution, J5 claim scoping, J6 field serviceability, J7 agent
  laundering — added 2026-09-10) come from the ledger; the denominator is
  that run's judged coverage, printed beside every rate.
- **Probes** are named regression tests over known cases; failures are
  reported by id.
- **Watch numbers added 2026-09-05** (tracked, never gated): `location_coverage`
  = code-located mentions / all mentions on the fold (the model never sees
  locations, so this measures the corpus + locator, not the synthesis);
  `designation_preservation` = a token-level PROXY for the statics' rule that
  every specific designation the snippets carry must be named in the
  synthesis (code-shaped tokens looked up after a loose normalization; blind
  to bare-number grades, and a token in a set-aside snippet is not a drop) —
  the judged answer is J2's designation clause. Scoreboard rows before
  2026-09-05 carry `single_snippet_share` under its old name
  `single_entry_share` (same measure).
- **Watch numbers added 2026-09-10** (the focal-form paragraph's focus
  numbers, design doc D3/D16; tracked, never gated), with their values
  recomputed on run 20260905T213127 by the current code so the next run reads
  against the same method: `sibling_mention` = share of paragraphs naming a
  sibling record's form that is not a nested variant of its own (72.9%;
  16,154 of 22,162; 359.8 chars naming vs 232.8 not; one word-bounded
  alternation regex per chunk — a nomination-grade count); `retried_records`
  = trigger firings (1,593) and `retry_requests` (450) against
  `first_pass_requests` (1,598); `own_designation_drops` = records whose
  focal form's own tokens are missing from the paragraph (0 of 2,484);
  `designation_preservation` now measures the record's OWN designations
  (99.3% of 5,004 tokens; 21 records still dropping after the retry — the
  old any-token demand read 61.7%); `identical_synthesis_records` (1,327 in
  within-request clusters). The design doc's 71.1% / 1,665 / 1,722 for the
  same quantities came from one-off scripts with slightly different
  denominators; the harness numbers above are the baselines from now on.

## Measurement traps (each one bit a real analysis; do not relearn them)

1. `contract_products` is a byte-copy of `products` through synthesis — never
   sum over dump files (~32% overstatement). The harness skips it everywhere
   and only checks the copy invariant.
2. Pair records by (subject, field, **chunk_bounds**, group_id) — group ids
   are global; 242 appear in both chunks; a bounds-less key once dropped 114
   records and hid all 7 party-name losses.
3. Regex over LLM prose is enumeration, never measurement (three census
   regexes ran 10× under, 69× over, 2× over; a passive→active voice change
   faked a 45→64% jump). Judgments come from reading; the harness's string
   scans only nominate.
4. Dump-stored lint counters are incomparable across lint-code versions; the
   harness recomputes lints and stamps `lint_versions` + `taxonomy_version`
   into every scorecard (`method_stamp` on scoreboard rows).
5. Full-run dumps carry the synthesis block only since the 2026-08-26
   instrument fix (`keyword_reconcile_node.py` / `concept_reconcile_node.py`);
   older runs fall back to group rows, and their evidence exists only in the
   Mongo snapshot.
6. `turnaround_seconds` and run-level `time_span` are meaningless on
   eager/resumed runs; read `client_latency_ms` and per-stage spans.
7. An identical synthesis string within one request is a collapse but usually
   an accurate composite; only the subclass whose own name is absent is a
   defect, and the focal-form lint catches exactly that subclass.
8. A `pv=` change orphans stored rows: cross-run comparisons are clean only
   where `ud=` digests match. `publish` ships every edited static — never
   evaluate a run with two pending prompt edits bundled.
9. Subjects are spelled the way the DUMP spells them (`anchor-mfg_com`, not
   `anchor_mfg_com`): the snapshot's address parser once normalised the hyphen
   away and every evidence lookup for that subject silently returned nothing.
   `loading.safe_subject` mirrors core's rule; expectation folders follow it.
10. A retried record's accepted paragraph came from the RETRY request, whose
   co-pack is the subset that went unanswered — the evidence index points
   `request_custom_id` at the retry and keeps the group request under
   `superseded_request_custom_ids`. Identical-synthesis clusters and J3 use
   the accepted request.
11. contract_products copies products' synthesis state at ITS OWN pass: a
   products record answered by a LATER retry shows as `not_synthesized` on
   the contract copy (run 20260905T213127: decimal ×3, tanfel ×1). INV-3
   reports it; it is a pipeline sequencing fact, not an eval artifact.
   ROOT CAUSE FOUND AND FIXED 2026-09-10: the recursive base's eager loop
   exited as soon as nothing was missing and nothing unanswered — the state
   of a branch whose ids are already answered — so the contract node's
   assessment pass never ran and no retry ids were recorded on its bundle.
   The loop now keeps going while a pass embeds new ids
   (`base_llm_recursive_extraction_node.py`); INV-3 is expected to pass on
   the next run, and a failure there is a regression, not a known fact.

## The corpus

Eight manufacturers in `knowledge/sample_scraped_texts/`; only alecmfg and
steelcraft have synthesis history. Recommended order for adding the rest (new
coverage per dollar, from the 2026-08-26 survey — rationale in each subject's
`expectations/<subject>/subject.yaml` and its field probes): **anchor-mfg →
austinelectricservices → ableengineering → taylordunn → 101machine →
acimachine** (the last needs chunk-budget decisions at ~1M tokens;
taylordunn's scrape is 97.5% cookie boilerplate — a deliberate degeneracy
probe, and a re-scrape would serve OEM coverage better). Runs are launched by
the user from `mfg_extraction_test.ipynb`; the eval evaluates whatever
subjects a run contains.

## STATE (2026-09-13, run `20260913T023316` — the PACKING run: cap 50 → 10, same pins as 191548, mathewsco.com + tanfel.com regenerated, judged on 1,494 records, paired against BOTH gs=50 draws)

The synthesis packing cap (`max_entries_per_request`, custom-id segment `|gs=`) went 50 → 10 as a diagnostic
(design doc §25/§27): every synthesis custom id changed, so the run regenerated synthesis by itself while search
replayed — no stored-request deletion. Mechanical: 712 first-pass requests (94 + 618; 180 at gs=50), 14.76M input +
329k output tokens ≈ $32 (≈ $10 at gs=50), request latency p50 9.0 s / p90 14.1 s / max 41 s (10.5 / 19.3 / 91 at
gs=50 — the wave tail halved), mean paragraph 489 chars (≈417: +17%), 2,761/2,761 synthesized, 2 retried, 0
invariant failures. Judged population = the A/A's 1,494 keys exactly (10 Sonnet jobs, 1,494/1,494, 0 malformed);
2 Opus packets, 179 items, 50 rows changed — the judges' dominant error was scoring the shape-(b) hedge ("available
at Tanfel … whether it manufactures or sources it is not specified") as J2 major + J6 fail: J2 major→minor 25, J6
major→pass 28, J6 minor→pass 14.

**Paired against 191548 (1,492 pairs, first gs=50 draw → the gs=10 draw):**

| readout | gs=50 draw 1 | gs=10 |
|---|---|---|
| records with any fail | 3.2% (47) | 7.8% (116) — 93 newly failing, 24 newly passing |
| records with a major | 2.1% (32) | 1.9% (29) — 19 newly failing, 22 newly passing |
| mathewsco (353): any-fail / major | 22 / 20 | 24 / 16 — flips **28 = 7.9%** any-fail (floor 8.2%), **22 = 6.2%** major (floor 6.2%) |
| tanfel (1,139): any-fail / major | 25 / 12 | 92 / 13 — flips **89 = 7.8%** any-fail (floor 3.0%), 19 = 1.7% major (floor 1.6%) |
| mathewsco/process_caps (85): any-fail / major | 18 / 17 | 10 / 8 |
| tanfel/material_caps (226): any-fail / major | 0 / 0 | 53 / 1 (52 J2 minors: the make-vs-source hedge on grade-table cells) |
| tanfel/conformity (20): any-fail / major | 1 / 0 | 5 / 5 (under-claims on the standards bullet) |
| clusters with a failing member / mixed | 11 / 2 | 37 / 17 |

Against 225723 (1,473 pairs, second gs=50 draw): any-fail 5.8% → 7.8%, majors 3.9% → 1.9%; flips mathewsco 12.2% /
9.6%, tanfel 8.1% / 2.1%, ALL 9.1% / 3.9%.

**Per-request clustering of verified fails** (`checks/request_mode_readout.py`; judged records grouped by
`request_custom_id`, requests with ≥ 2 judged records): failing records that sit in a request where the majority
fails — 22% on 191548, 51% on 225723, **81% on the gs=10 run**; whole-request fails 0 / 1 / 18. Whole 8- and
10-record tanfel material_caps requests came back hedged as a block and one whole 8-record mathewsco process_caps
request laundered as a block, while their neighbours from the same tables and lists passed.

**Reading.** (1) Smaller requests did NOT lower the per-record flip rate: on the representative site the gs=10
draw flips 7.9% any-fail / 6.2% major against the first gs=50 draw, the floor is 8.2% / 6.2%. (2) The mechanism
is unchanged: gpt-4.1 still lands a whole request in one reading, at 4 records per request as at 15 — the coin
is per request whatever its size. (3) Decorrelation works as predicted (design doc §27.4): subject totals stop
swinging as a block — majors 29 against 32 and 58 on the two gs=50 draws, mathewsco/process_caps 8 against 17
and 26. (4) A systematic cost appeared: with few co-packed siblings the model hedges the subject's OWN listings
more — 52 J2 minors on tanfel's grade tables (0 and 6 on the gs=50 draws) and 5 majors on its standards bullet;
the list rule has less list to read in a 4-record request. (5) 3.2× the synthesis tokens, +17% paragraph length.
**Recommendation: revert the cap to 50 — DONE 2026-09-13 on the user's word.** A second gs=10 draw (the gs=10-vs-gs=10 floor) would only confirm the
per-record number and does not change the decision; the per-record lever is the N=3 majority, at the 50 cap.

## STATE (2026-09-12, run `20260912T225723` — the TWO-SUBJECT A/A: same pins as 20260912T191548, mathewsco.com + tanfel.com re-generated, judged on 1,473 records, paired against 191548 on identical evidence = THE VERDICT-LEVEL NOISE FLOOR)

The stored synthesis requests of the two subjects were deleted first (a re-run on unchanged pins otherwise
REPLAYS stored responses — run 20260912T224107 was such a replay, 3,817/3,817 paragraphs identical, and was
discarded). Same evidence, same static (`18d3e093…`), same call shape: **74 of 3,817 paragraphs (1.9%) came back
byte-identical; mean text similarity of the rest 0.45.** Judged by 10 Sonnet jobs, verified by 2 Opus packets
(172 items, 71 rows changed — 37 of them normalising the laundering shape to J1+J4+J6+J7, which is why the
per-DIMENSION counts below are not comparable across runs; the per-RECORD any-fail and major counts are).

**Paired against run 191548 (1,471 pairs on identical evidence, first draw → second draw of the SAME prompt):**

| readout | first draw | second draw |
|---|---|---|
| records with any fail | 3.1% (46) | 5.8% (85) — 51 newly failing, 12 newly passing |
| records with a major | 2.2% (32) | 3.9% (58) |
| mathewsco (353 pairs): any-fail / major | 22 / 20 | 43 / 34 — **29 records (8.2%) flipped their any-fail verdict**, 22 flipped major |
| tanfel (1,118 pairs): any-fail / major | 24 / 12 | 42 / 24 — **34 records (3.0%) flipped**, 18 flipped major |
| clusters with a failing member / mixed | 11 / 2 | 22 / 5 |

**Reading — the floor.** On identical inputs the per-record verdict flips 3% (a maker) to 8% (a
representative), and the NET can swing by two points of majors on a subset dominated by one bimodal
site. Mechanism (design doc §23, `tryout/repeat_mw2.py`): gpt-4.1 at temperature 0 with a fixed seed is
bimodal PER REQUEST on the rep-site class — the same 39-record Mathews request comes back either all
laundered ("the dealing shown is that Mathews & Company offers X") or all hedged ("…; the capacity is
unstated"); five repeats of the published static drew the laundering mode once and a partial once. So:
(1) the C21 "regression" of run 191548 (17 majors) was a draw, not a prompt effect; (2) the C16 pass's
net −46 majors on 10,503 pairs is above this floor in size but its per-subject reading is not — no
per-subject class under ~30 records is readable from one run; (3) every prompt tryout arm is now read as
a MODE FRACTION over ≥5 repeats with the production call shape; (4) any A/B smaller than the flip mass
(~3–8% of records) needs either more repeats or a variance fix at the source (smaller request packing so
a mode flip touches fewer records; or N-sample majority per request). Per-dimension counts: J4 0 → 44
is the verifier's normalisation (the first draw's judges scored the same shape as J1/J7 only).

## STATE (2026-09-12, run `20260912T191548` — the C16 pass: run A + four validated edits, judged on 10,551 records, paired against run A 20260911T223222)

The pass validated on the production model in `docs_local/field_requirements_survey_20260911/tryout/pass2/JUDGE_C16.md`
(design doc §19.7): run A's static + the wire-vocabulary ban (incl. "snippet", "the manufacturer"), the doer
clause, the list rule ("an item in a list takes the dealing its introducing sentence gives it … never the
doer"), and the first-person rule. Six statics byte-identical (md5 `18d3e093…`), published 2026-09-12 by the
user, 18 subjects, stop-after-synthesis, 126/126 dumps, 22,162 records, 0 invariant failures; the
duplicate-id parser tolerance did not need to fire. Mechanical vs run A: mean paragraph 439 → 417 chars,
output tokens −4.5%, retried records 155 → 211, identical-synthesis records 315 → 425, focal-form-absent 0 → 2.

**Judged population:** `judged_population.py --run 20260912T191548 --baseline 20260911T223222
--singletons-from-baseline --singletons 4000` = 7,467 cluster + 117 baseline-retried + 2,967 singletons =
10,551; `judge_jobs.py` → 70 Sonnet jobs (`verdict_coverage.py`: 10,551/10,551, 0 malformed, 0 duplicates);
verification 7 Opus packets, 822 items (6 J3, 170 majors, 24 calibration J2/J6 from the 14 slices whose
judges flagged the list rule, 622 passes), **77 rows changed** (J6 major→pass 40, J6 major→minor 22, J2
major→minor 22, J2 major→pass 9, J1 major→minor/pass 15, J3 major→pass 3, J7 major→pass 5, …). Ingested 10,551.

**Paired result (10,503 pairs on identical evidence, run A → this run):**

| readout | run A | this run |
|---|---|---|
| records with any fail | 3.4% (356) | **2.8% (293)** — 229 newly passing, 166 newly failing |
| records with a major | 1.4% (152) | **1.0% (106)** |
| J2 under-claim / J6 serviceability | 186 / 230 | **114 / 152** |
| J7 agent laundering / J4 party / J1 faithfulness | 56 / 46 / 133 | 51 / 43 / 121 |
| J3 entity swaps | 0 | 2 (alecmfg products: a paragraph about a sibling entity) |
| identical-evidence twins that flip | 5 of 214 | 5 of 214 |
| mixed clusters / containment breaches | 34 / 24 | 34 / **3** |

Per field (any-fail / major): conformity 26/417 = 6.2% / 11; equipments 28/687 = 4.1% / 3; industries 27/988
= 2.7% / 5; material_caps 41/1,241 = 3.3% / 2; process_caps 91/3,033 = 3.0% / 44; products 83/4,185 = 2.0% / 41.

**Stage 2 (answerability, 900 paired, six Opus graders;
`docs_local/field_requirements_survey_20260911/READOUT_PAIRED_20260912T191548.md`):** vs the 003500 baseline,
D capacity 28.1% → 8.8%, G frame 29.2% → 2.2%, C party 13.0% → 2.4%, H 7.7% → 1.4%; vs run A (same 900):
C 6.3% → 2.4%, G 6.4% → 2.2%, D 10.0% → 8.8% (61 fixed / 50 broken), field item 12.8% → 8.6%; fully clean
per field run A → this run: conformity 130 → 134, equipments 114 → 119, industries 121 → 128, material_caps
127 → 124, process_caps 120 → 128, products 103 → 122.

**Reading.** The list rule did what the production-model tryout predicted: the own-listing under-claim class
fell (J2 186 → 114, J6 230 → 152; tanfel process_caps 17 majors → 10 on that chunk, agstech's private-label
catalog 18/18 correct), majors fell to 1.0%, and the frame is now carried on 98% of stage-2 records. The
cost is exactly the class the pass was designed to avoid, on one subject: **the sales representative
(mathewsco process_caps) carries 17 J1+J6+J7 majors** on "Kansas Product Line" entries whose own words say
"This American manufacturer…", "They offer…", "Their equipment list…" — re-narrated as "Mathews & Company
offers X" (run A: 5 fails on that class; the tryout request, a different packing of the same site, showed 0
under this static). Net J7 56 → 51 because other sites improved. Residue classes: bare-noun capability
indexes and "Related Products" cross-links still hedged (tanfel 10 records; alecmfg material_caps 18 minors
under "materials available for the manufacturing processes we offer"); case-study titles read both ways
inside one subject (alecmfg, AL-P8); parent voice untouched (taylordunn TD-P3 9 fails); wire vocabulary
persists on 21 rows incl. two internal record-id leaks ("This snippet repeats the content of record …");
two J3 content swaps. Probes: MW-P1 9/9 pass (its named groups), FZ-P1/P2/P3 pass, AG-P7 20/20 pass (the
document rule on the private-label catalog is fixed), AG-P6 5 fail (disjunction collapsed to one standard),
TD-P3 9 fail / 3 pass, PM-P3 3 fail / 7 pass, TF-P6 1 fail / 3 pass.

## STATE (2026-09-12, run `20260911T223222` — run A, the shared-block statics, judged on 10,503 records, paired against 20260911T003500; COMPLETED with alecmfg's two re-run fields the same day)

The first run on the field-specific redesign's SHARED BLOCK (design doc §17;
requirements doc `docs_local/SYNTHESIS_FIELD_REQUIREMENTS_2026-09-11.md`): the
six statics byte-identical, pins `63ed7e98…`, published 2026-09-11 22:30Z.
Same 18 subjects; search replayed so every record pairs on identical evidence.
**alecmfg process_caps and material_caps were missing on the first pass** (a
synthesis response answered one record id twice with two paragraphs; the
parser raised on a duplicate id; three re-asks repeated it; the orchestrator
dropped the subject at process_caps and material_caps never ran — 124 of 126
dumps). **Fixed and completed 2026-09-12:** `parse_synthesis_response` now
DROPS a repeated id with both its answers and the ordinary under-answer retry
re-asks it with the sibling it overwrote (`packages/core/.../synthesis.py`;
tests in `test_synthesis_wire.py` and `test_synthesis_node_passes.py`); the
user re-ran alecmfg alone into run A's folder on run A's pins (the four
finished fields replayed byte-identically; process_caps re-asked exactly
`gdaltkrb` + `gdohgsgl` in one retry request) — 126 of 126 dumps, 22,162
records. The two fields were then pulled, added to the population
(`judged_population.py … --singletons 4000` so the old population stays a
strict subset: +448, −0), judged (J16–J19, Sonnet), verified (packet 09, Opus,
62 items, 9 rows changed; `verify_packets.py --only … --first-packet 9`),
ingested, and paired; the stage-2 sample gained its 35 missing rows (two Opus
graders) — 900 of 900 paired. Two INV-1 flags (blackadvtech, pradeepmetals
process_caps) are `unknown_answer_ids` only: a mangled id echo, recovered by
the retry pass.

**Mechanical (contract excluded):** retried records 152 (was 82), retry
requests 46 (42), first-pass 1,550; focal-form-absent **0** (was 33);
identical-synthesis records **297** (was 899); sibling-mention share **80.9%**
(was 72.6%, UP); output tokens +32%, mean paragraph ≈459 chars (was 322);
cost ≈$85 (was ≈$79); own-designation drops 0.

**Judged population:** `checks/judged_population.py --singletons-from-baseline`
(new flag: stratum B drawn from the baseline's JUDGED singletons so every
singleton pairs) = 7,133 cluster + 71 baseline-retried + 2,850 singletons =
10,054 (+1 orphan) on the first pass, 10,502 (+1) once alecmfg's two fields
were added; `checks/judge_jobs.py` → 66 Sonnet jobs, then 4 more (J16–J19);
`checks/verdict_coverage.py` (new) → 10,503/10,503 rows, 0 malformed, 0
duplicates. Two jobs died on the 64k output-token cap (agents narrating
records into their replies) and were relaunched with a terse-output
instruction. Verification: 8 Opus packets, 824 items (3 J3, 199 majors, 604
random passes, 18 calibration rows), **77 rows changed** (J2 major→minor 42,
J6 major→minor 26, J1 major→pass 19, J6 major→pass 13, J3 major→pass 3, 15
pass→fail minor for the Waev parent voice in process_caps); the rulings that
emerged are in `JUDGE_PROMPT_TEMPLATE.md`. Ingested 10,055.

**Paired result (10,467 pairs — the FULL population, alecmfg included; baseline → this run):**

| readout | baseline | this run |
|---|---|---|
| records with any fail | 3.1% (326) | 3.4% (355) — 236 newly passing, 265 newly failing |
| records with a major | 2.0% (208) | **1.5% (152)** |
| J7 agent laundering / J4 party / J1 faithfulness | 128 / 70 / 187 | **56 / 46 / 132** |
| J2 under-claim / J6 serviceability | 107 / 234 | **186** / 230 |
| J3 entity swaps | 2 | **0** |
| identical-evidence twins that flip | 1 of 214 | 5 of 214 |
| mixed clusters | 30 | 34 |

(The 10,019-pair readout before alecmfg's two fields: any-fail 3.2% → 3.4%,
majors 2.0% → 1.5%, J7 120 → 51, J2 107 → 178 — same reading.) alecmfg's
two fields themselves: material_caps 87 rows, 2 fails; process_caps 361 rows,
25 fails after verification — the same shapes as the rest of the run (own
listings hedged, case-study titles read both ways, client-requirement table
rows read as delivered). Per field before alecmfg (any-fail / major):
conformity 43/417 = 10.3% / 26; equipments 18/686 = 2.6% / 9; industries
23/988 = 2.3% / 12; material_caps 47/1,139 = 4.1% / 14; process_caps 94/2,668
= 3.5% / 34; products 117/4,157 = 2.8% / 52.

**Stage 2 (answerability, the eval's field-specific second stage —
`docs_local/field_requirements_survey_20260911/READOUT_PAIRED_20260911T223222.md`,
900 paired records, Opus; 865 on the first pass):** capacity absent/contradicted
28.1% → 10.0%, frame 29.2% → 6.4%, party 13.0% → 6.3%; fully clean per field
conformity 94→130, equipments 110→114, industries 97→121, material_caps
68→127, process_caps 77→120, products 28→103.

**Reading.** The shared block removed laundering (J7 −58%, J4 −36%, majors
2.0% → 1.5%, J3 → 0) and made paragraphs carry the frame and the capacity;
it added one under-claim class: the "capacity unstated" sentence fires on
the subject's OWN listings (bare bullets under "Our Capabilities:", "we
provide the following services:", grade headings on own materials pages, own
badges) — 160 records (84 major), the whole of the J2 rise. Secondary:
frame named but the WRONG page (≈62 minor J1; flat nav strings, gallery hubs);
the closer restating a hedge as unconditional; paragraphs opening with wire
vocabulary ("The focal_form …", 67 rows noted); sibling inconsistency up
(twin flips 5/206). Probes: FZ-P3 and FZ-P1 fixed; MW-P1 5 fail / 37 pass
(was a 31-record class); AG-P8 document reduction gone; AL-P8 5/22 and HW-P5
10/29 chunk- or request-dependent; TD-P3 (Waev) untouched.

## STATE (2026-09-11, run `20260911T003500` — the focus run, judged on 10,467 records, paired against 20260905T213127)

The first run on the focal-form statics (pv `TC4hc7…`, published 2026-09-11
00:33), the own-scoped trigger, the first-answer tie and the eager-loop exit
fix. Same 18 subjects, same 22,162 records (search replayed, the fold is
unchanged), so every record pairs with its baseline twin on identical
evidence. TAXONOMY.md changed between the runs (J7 added, J1 split, severity
per record), so `taxonomy_version` differs and the whole population re-judged.

**Mechanical (all 18 subjects):** delivery 22,162/22,162, every invariant
passes — **INV-3 passes on all 18 contract cards** (17 of 18 failed before;
the loop fix worked). Records re-asked by the trigger **82** (was 1,593),
retry requests **42** (was 450), first-pass requests 1,598 (same);
own-designation drops **0** of 2,484; scoped designation preservation 99.2%
of tokens, 2,708 of 2,734 records fully; sibling-mention share **72.6%**
(16,078 of 22,162; was 72.9%), mean chars naming vs not **350.3 vs 245.9**
(was 359.8 vs 232.8), mean paragraph 321.7 chars (was 325); identical-
synthesis records 899 in 405 within-request clusters (was 1,327); focal-form-
absent 33; stage cost ≈$79 (32.4M input tokens; was ≈$96).

**Judged population (design doc §7):** every record in an identical-snippet-
set cluster (2,635 clusters, 7,467 records) + 3,000 singletons (all 1,050
singletons the baseline run retried, plus 1,950 field-proportional at seed
20260911) = 10,467 records, 70 Sonnet judge jobs from `JUDGE_PROMPT_TEMPLATE.md`
(run copy in `history/runs/20260911T003500/JUDGE_PROMPT.md`), 128 verdict
files, 0 malformed rows. Verification (`checks/verify_packets.py`,
`checks/apply_corrections.py`): 831 rows re-read — both J3 fails, all 189
majors, 623 random passes, and every J2/J6 fail of the five slices whose
judges flagged the document rule — 23 rows changed (17 J2/J6 under-claims
upgraded minor→major, 3 reseller "offers X" rows overturned to pass under the
dealing rule, 3 passes turned to major fails). Ingested 10,467; 11,695
records stay pending by design (`checks/paired_readout.py` for the pairing).

**Paired result (10,463 pairs on identical evidence, baseline → this run):**

| readout | baseline | this run |
|---|---|---|
| records with any fail | 6.7% (703) | **3.1% (326)** — 563 newly passing, 186 newly failing |
| records with a major | 1.3% (141) | 2.0% (208) — see the rubric caveat |
| the baseline-retried singletons (1,050), any fail | 26.5% | **5.9%** |
| identical-evidence twins that flip pass/fail | 19 of 210 | **1 of 214** |
| clusters with a failing member / mixed clusters | 136 / 45 | 93 / 30 |
| J3 entity swaps | 0 | 2 |
| J5 mis-scoping | 59 | 3 |
| J1 fails / containment breaches / grounded-import notes | 497 / 323 / — | 187 / 27 / 210 |
| J6 fails / J7 fails (new) / J2 fails | 162 / — / 97 | 234 / 128 / 107 |

Per field this run: conformity 12.3% any-fail (51/416; 41 major),
material_caps 3.8% (47/1,224), industries 3.7% (37/988), equipments 3.5%
(24/684), products 2.9% (120/4,126), process_caps 1.6% (47/3,029).

**Rubric caveat.** The like-for-like signals are any-fail, J3/J4/J5, twin
flips and mixed clusters. The major count is NOT like-for-like: J7 did not
exist (128 rows), the J1 split moved 210 page-imports from fail to
pass-with-note, severity is now per record, and verification upgraded 17
document-rule under-claims to major. Read "majors up" as "the rubric now
names laundering and under-claim harder", not as a regression.

**The retry axis is gone.** The population the old trigger re-asked failed at
26.5% on the baseline and 5.9% here; only 82 records were re-asked this run.

**Top defect classes this run (verified by reading):**
1. *The document rule over-applied* — 65 verified J2+J6 under-claims where the
   paragraph wrote "offers a document / no dealing beyond a title" for the
   subject's OWN product-catalog bullets ("ready products you can purchase",
   agstech), case-study titles that name the work ("Custom CNC Machining of
   Aluminum Brackets", alecmfg), materials-catalog grade headings
   (`## LOW ALLOY 4145: ASTM A29`, howcogroup) and certificate download links
   (anchor, howcogroup). A one-paragraph statics issue; the conformity field's
   12.3% is mostly this.
2. *J7 agent laundering* — 128 rows: generic explainer/listicle copy and
   application lists read as the subject's own dealing (fze, blackadvtech,
   tanfel, lucasmilhaupt, howcogroup); the D11 laundering-rules run's target.
3. *Parent voice dropped* — taylordunn's Waev supplier code and 3TG
   statement voiced as Taylor-Dunn (24 J4 fails across four fields, 9 major
   in conformity; new probe TD-P3); steelcraft's Allegion Declare labels (1).
4. *Rep-agency principal default* — mathewsco products, 31 rows (MW-P1).
5. *Upstream scrape fusion* — adjacent cells/bullets concatenated into fake
   compound entities (decimal "Hard Coat Barrel Plating", "Cadmium Chromate";
   pradeepmetals "PED 2014/68/EU,AD2000"), faithfully described by the model.

**Probes:** 55 evaluated, 36 all-pass (incl. AC-P1/P2/P3 dealer stock, AG-P4
reseller software, AL-P5 client gripper, BA-P1 listicle, FZ-P1/P2, TF-P4 the
retry closer, SC-P4 KD frames), 19 with a fail (AB-P4/P5, AG-P6, AL-P3 7/50,
AN-P3, BA-P2, DM-P2/P4, FZ-P3/P4/P5/P6/P8, HW-P2/P3, MW-P1 5/6, PM-P1, ST-P2,
TD-P2 24/24 — the predicted cookie-policy/legal-vocabulary upstream leak, still
reaching synthesis, not a synthesis defect).

**D10 (fold clause-clip): the watch numbers did not move (72.6% vs 72.9%,
350 vs 360 chars), so the design's condition is met literally — but the harm
the watch proxied did not materialise: J3 2 of 10,467, twin flips 1 of 214,
mixed clusters down a third. Recommendation to the user: do not build the
clause-clip; spend the next run on the laundering rules (D11) and the
document-rule wording. User decision pending.**

## STATE (2026-09-10 — Step 1 of the synthesis/grounding redesign BUILT, awaiting the focus run)

Built 2026-09-10 per `docs_local/SYNTHESIS_GROUNDING_REDESIGN_2026-09-10.md`
§7/§10, nothing run yet: the six synthesis statics carry the focal-form
paragraph (unpublished until the user publishes; pins still 18a3fd59…); the
under-enumeration trigger demands the record's OWN designations only, cut at
the chunk's sibling forms, and the retry-vs-first comparator keeps the first
answer on ties; the recursive base's eager loop keeps going while a pass
embeds new ids (the INV-3 root cause); TAXONOMY.md gained J7 and the batch of
edits listed in CANDIDATE_DIMENSIONS.md (so `taxonomy_version` changed —
every cached verdict re-judges on the next run, which the pv change would
have emptied anyway); `JUDGE_PROMPT_TEMPLATE.md` is now tracked; the new
watches are in the scorecards. **Next:** the user publishes the six statics
and launches the 18 census subjects from `mfg_extraction_test.ipynb`, stop
after synthesis, full dump; then `checks/run_eval.py --run <id> --pull`;
judge every record in an identical-evidence cluster plus a matched
3,000-record singleton sample (Sonnet), paired against 20260905T213127
(any-fail 6.4%, major 1.1%, retried 19.8% vs 5.4%, mixed clusters 46 of 138,
twin flips 8.7%), with a two-subject same-pv A/A alongside; then the D10
decision (fold clause-clip) on the sibling-mention share and paragraph
length.

## STATE (2026-09-06, run `20260905T213127` — COMPLETE, 100% judged)

18 subjects × 7 fields on the flattened snippets+chunk-text wire. **22,162
records, 22,157 distinct content keys, all judged, 0 pending** (contract_products
copies excluded — they replay products' answers). 186 slices across 107 work
orders; 22,162 verdict rows, 0 malformed, 0 unknown keys. The five repeated
content keys are the by-design collision (a group whose evidence AND answer are
byte-identical in two chunks yields one key); four of the five were judged blind
by two different agents and all five agree exactly on all six dimensions.

**Headline (record-level, because one defect commonly trips two dimensions):**

| population | records | any fail | with a major |
|---|---|---|---|
| all | 22,157 | 6.4% | 1.1% |
| retried by the pipeline | 1,593 | 19.8% | 3.1% |
| not retried | 20,564 | 5.4% | 1.0% |

Per field, records with any fail: conformity 11.5% (94/819), material_caps 7.3%
(171/2,346), equipments 7.0% (141/2,019), industries 6.5% (155/2,389),
process_caps 6.1% (355/5,849), products 5.8% (508/8,735).

**The retry finding is the run's biggest result.** The synthesis stage re-asks
records whose first answer under-enumerates designations; those records fail at
roughly 3.7× the base rate, and containment breaches at 3.8× (9.4% vs 2.5%).
The gap holds in every field and is widest on equipments (28.6% vs 6.0%) and
industries (30.5% vs 5.1%). Three distinct retry drifts are documented as C10 in
`CANDIDATE_DIMENSIONS.md`: hedges collapse, the document rule retreats, and a
closing template launders feedstock into a supplied product.

**Watch numbers (not gates):** location coverage 96,636/106,213 mentions located
= 91.0%; designation preservation proxy 61.7% of tokens and 73.0% of records —
the proxy is noisy by construction (sibling grades, footer digits and image
filenames all nominate falsely, and it missed a real drop, "squeeze casting"),
so J2's judged designation clause is the answer, not this number.

**A pipeline defect the eval found, outside synthesis:** INV-3 fails on 17 of
18 contract_products cards, 510 diverging (chunk, group) pairs. The contract
node replays the group answers only and never sees the retry state, so wherever
`resolve_under_enumeration` kept a different answer the two diverge. The fix
belongs to the contract node.

**Judge population:** Fable judged 2,487 rows before the account's monthly spend
limit was hit, Sonnet the remaining 19,670 under the user's "Sonnet for smaller
tasks" rule; every row's `judge` string carries the model. Fable grades harder
on every dimension (any-major 2.5% vs 2.0%, containment 4.0% vs 2.9%, J5 2.6%
vs 0.2%), so cross-field comparisons that mix the two are only as good as that
gap. 1,908 rows were re-read and marked `verified` at verification, and roughly
90 rows were re-graded — the calibration decisions behind those corrections are
in the run's `JUDGE_PROMPT.md` and in `CANDIDATE_DIMENSIONS.md`.

The spend limit was hit three times during the fan-out; every cut was resumed
from the partial verdict files with no rework (`reconcile.py` writes a RESUME
NOTE naming the exact missing record indices).

**Eval-set growth from this run:** seven new candidate dimensions (C6–C12), 37
new probes, and a `target` field added to all 48 probes that lacked one — a
probe without `target` cannot attach, which is why 9 pre-existing probes on
in-run subjects scored nothing this pass. Pending TAXONOMY.md edits are listed
at the foot of `CANDIDATE_DIMENSIONS.md`; they must wait for the next
statics republish, which empties the verdict cache anyway.

## Standing open questions the eval should keep pressure on

- Synthesis A/A noise floor: unmeasured; one same-`pv`/same-`ud` re-dispatch
  (≈$2.50) would calibrate every trend judgment made here.
- The `loc=0` arm (synthesis without location text) has never run.
- Evidence thickness (64.4% single-snippet records on 20260905T213127) drives
  both cost and collapse; it is upstream of this stage but bounds what judgment
  can expect of it.
- **Is the under-enumeration retry worth its cost?** It re-asked 1,593 records
  (450 of 2,048 requests) and those records fail at 19.8% against a 5.4% base.
  The retry is supposed to recover dropped designations; measure what it
  recovers against what it degrades before keeping it as is.
