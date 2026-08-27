# Synthesis-stage evaluation

The permanent, evolving evaluation of the pipeline's **synthesis stage** — the
LLM pass that writes one evidence-grounded paragraph per group record (a focal
form plus entries of {snippet, location}), sitting between mention collection
and the grounding/screening tail. Built 2026-08-26 from the defect history of
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
| `TAXONOMY.md` | J1–J6 dimensions + per-field extensions; its hash is `taxonomy_version` | yes |
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
`--run` to use the newest dump directory.

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
  party attribution, J5 claim scoping, J6 field serviceability) come from the
  ledger; the denominator is that run's judged coverage, printed beside every
  rate.
- **Probes** are named regression tests over known cases; failures are
  reported by id.

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

## STATE (2026-08-26, first judgment pass — PARTIAL)

Run `20260825T194457` is snapshotted, prepared, and **partly judged: 546 of
2,133 records**. The pass stopped because the account hit its **monthly spend
limit** mid-flight, not because of any harness or data problem — 11 of 16
judge agents were cut off; 8 verdict files landed and all validated (0
malformed rows, 0 unknown keys, 0 duplicates; partial files were cut cleanly
at line boundaries by the shell-append protocol).

**Fully judged (0 pending):** steelcraft equipments (84), industries (81),
material_caps (97); alecmfg equipments (36).
**Partly judged:** alecmfg products 143/365, conformity 32/40, material_caps
23/77; steelcraft products 50/663.
**Unjudged:** alecmfg industries (110), alecmfg process_caps (255),
steelcraft conformity (146), steelcraft process_caps (179).

**To resume:** run the CLI (no `--pull` needed, the snapshot is on disk) — the
work orders it writes contain ONLY the 1,587 unjudged records — then judge per
RUNBOOK.md. Nothing already judged is re-judged: the content-keyed cache was
verified live (three fields went 84/81/97 → 0 pending after ingestion).

The 546 verdicts were migrated when this folder adopted the sibling-stage
layout: every row's stored `content_key` reproduced exactly under its old
value before being re-keyed to `taxonomy_version` (546/546 verified, 0
orphaned; rows carry `migrated_from_taxonomy`).

Five candidate defect classes came out of this pass — `CANDIDATE_DIMENSIONS.md`,
with regression probes SC-P6/7/8 and AL-P5/6/7 already seeded. **Do not
promote them into TAXONOMY.md mid-pass**: that changes `taxonomy_version` and
invalidates the 546 cached verdicts. Promote in one batch between runs.

## Standing open questions the eval should keep pressure on

- Synthesis A/A noise floor: unmeasured; one same-`pv`/same-`ud` re-dispatch
  (≈$2.50) would calibrate every trend judgment made here.
- The `loc=0` arm (synthesis without location text) has never run.
- Evidence thickness (70% single-entry records) drives both cost and collapse;
  it is upstream of this stage but bounds what judgment can expect of it.
