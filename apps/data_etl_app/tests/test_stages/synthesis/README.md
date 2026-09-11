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
