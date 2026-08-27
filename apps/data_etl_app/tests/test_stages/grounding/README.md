# Grounding evaluation instrument

A standing, per-field, continuously growing evaluation of the pipeline's four
**grounding stages** — freehand grounding (products, contract_products,
equipments), initial grounding, out-of-vocabulary (OOV) grounding, and recursive
descent (industries, material_caps, process_caps, conformity_attestations) —
including their **declines**. Screening is never judged here; each tag's
screening verdict is carried only as severity context. Established 2026-08-26 on
the back of the full hand census of run `20260825T194457`
(`pipeline_v3_evidence/2026-08-25_run_194457_full_end_to_end/`), which is this
instrument's baseline row.

**To run it:** after a full pipeline run, ask Claude to
*"run the grounding eval on run `<run_id>`"*. The session follows
`RUNBOOK.md`: a deterministic code pass, a **full census** by judge
agents (one per subject × field dump — user decision 2026-08-26: full
enumeration every run, no sampling), a personal verification pass, then the
report, ledger append, and the evolution step. The deterministic half alone is
runnable by anyone:

```
.venv/bin/python checks/run_eval.py --run <run_id>
.venv/bin/python checks/run_eval.py --run <run_id> --merge-judgments
```

## The folder

| path | what it is |
|---|---|
| `TAXONOMY.md` | the versioned defect codes and binding judgment rules |
| `RUNBOOK.md` | the run protocol: phases, judge-agent prompt, verification, first-contact, evolution |
| `config/common.yaml` | criteria every field shares: enforcement flags, judged metrics, cross-field watch items |
| `config/fields/*.yaml` | per-field watch items, recall sets, judge cautions |
| `expectations/<subject>_com/` | per-manufacturer expectation inventories — `subject.yaml` (profile, coverage) plus one file per field (expected entities + traps); all 8 sample subjects |
| `checks/run_eval.py` | the code half: deterministic checks, judgment merge, tiers, ledger |
| `history/runs/<run_id>/` | per-run: `metrics.json`, `DETERMINISTIC.md`, `judgments/*.jsonl`, `verification_log.md`, `REPORT.md` |
| `history/ledger.json` / `history/LEDGER.md` | the longitudinal table, one row per (run, subject, field) |
| `tests/test_grounding_eval_harness.py` | pytest coverage of the instrument's own code against a fixture dump |

The layout matches the sibling stage instruments (`../search/`, …) — same
`RUNBOOK.md` / `config/` / `expectations/` / `checks/` / `history/` / `tests/`
vocabulary, so one protocol reads across all of them.

## What the numbers mean

**Deterministic (structural — never judged, never prose):**

- `gate_violations` — tags emitted despite a `failed`/`violated` outcome in their
  own rule list (with the shipped sub-count).
- `membership.violations` — in-vocab or descent tags that are not an ontology
  label (name or altLabel) of the run's pinned vocabulary.
- `dropped_options.false_drops` — B1 drops whose option is a decorated display
  form (`X (also: …)`) of a real label.
- `sentinel_leaks`, `duplicate_tag_rows`, `twins.divergence_rate`,
  `descent.screened_share`, `tag_churn.clusters` (normalized-key collisions among
  surviving tags), `own_name_in_tags` (context only), per-stage ops
  (requests/tokens/retries/latency), and the dump census (missing files).

**Judged (assigned by reading, per `TAXONOMY.md`):**

- `clean_share` = (D direct + N normalized) / tag instances.
- defect mix X (wrong axis) / V (vague) / B (bridge) / P (wrong party) /
  **F (fabricated — the headline safety count)**.
- `shipped_defects` — defective tags that passed screening on a grounded row.
- `decline_sound_share` (DS vs DF false-decline vs DR self-refuting), plus the
  per-subject **recall sets** (e.g. conformity: UL 1784, ANSI A250.8-2003 …) —
  named-entity presence checks that survive the noise floor.
- `descent_defect_share` (H-BRIDGE + H-FORCED + H-STOP over hops).
- recall gaps against the subject inventory (expected strong/moderate entities
  no tag covers).

## The noise floor, and when a movement is real

Grounding diverges on **12.9–18.7% of rows on byte-identical input** at
temperature 0 with a fixed seed. Phase A recomputes this each run from the free
A/A instrument (products vs contract_products share freehand payloads; comparable
only when their `ud=` digests match) and derives a **two-sigma band** for share
metrics: a flipped row changes an item's clean/defective bit at most half the
time, so with per-row divergence *r* and *n* judged items,

```
band = 2 · sqrt(2 · q · (1 − q) / n),   q = r / 2
```

(≈ ±5 percentage points at n = 200, r = 0.15). A run-over-run movement inside
the band is reported as **within noise**, never as a finding. Single-record
phenomena are tracked as frequencies across runs, never per-run pass/fail. A
watch item closes only after ~3 consecutive clean runs (more for rare items).

## Outcome tiers (per subject × field)

- **PASS** — every enforced deterministic check clean, no judged regression
  beyond the band, no active watch-item hit.
- **WATCH** — an unenforced violation is present, a watch item fired, or a
  judged metric regressed beyond the band.
- **FAIL** — an **enforced** deterministic check violated (enforcement flags
  live in `config/common.yaml`; a check is enforced only after its known
  upstream fix has landed).
- **BASELINE/…** — the first evaluated run for that subject × field.

Judged numbers steer WATCH but never auto-FAIL a field on a single record —
that is the noise floor's veto.

## How the eval set evolves

Every run ends with the Phase D evolution step (playbook): judge agents propose
new defect patterns; the session presents them to the user; accepted ones become
watch items (`class: emerged`) with provenance, or taxonomy changes (version
bump — counts are comparable only within a version). New subjects get
first-contact inventories; existing inventories absorb corrections. Nothing is
silently added.

## Boundaries

- Screening soundness, search precision/recall, synthesis quality: out of scope
  (context only). The census showed each fails differently; they deserve their
  own instruments.
- This is **not** the Phase-6 ground-truth rebuild. Judgment records are keyed
  (run, subject, field, chunk bounds, group_id) precisely so they can seed that
  GT layer later.
- Extraction runs are driven from the user's notebook, never from here.
- Explanation text never gates anything (the census meta-finding: explanations
  are post-hoc justifications, not decision traces).
