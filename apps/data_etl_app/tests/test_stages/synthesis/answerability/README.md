# Answerability survey — the synthesis eval's stage 2

The field-specific second stage of the synthesis eval. Six per-field grader
agents read a fixed sample of records and mark, axis by axis (A–H of the
requirements matrix, `docs_local/SYNTHESIS_FIELD_REQUIREMENTS_2026-09-11.md`),
whether the paragraph CARRIES, leaves ABSENT or CONTRADICTS what the downstream
reader needs — a reading the J-rubric (`../TAXONOMY.md`) does not give. Moved
here from `docs_local/field_requirements_survey_20260911/` on 2026-09-14 (the
docs_local tree is local-only since then); the design record is
`docs_local/SYNTHESIS_GROUNDING_REDESIGN_2026-09-10.md` §17, §19–§21, §36.

## Files

- `BRIEF.md` — the grader's brief (what each axis asks, the marks).
  `BRIEF_LABELS.md` — the addendum for runs of the label wire (run X1 only).
- `sample.py` — draws the baseline sample from run 20260911T003500's judged
  orders (150 per field: every fail + stratified passes) →
  `sample_<field>.jsonl`, `verdict_index.jsonl`, `SAMPLE_SUMMARY.txt`.
- `sample_runA.py <run> [<baseline>]` — the SAME records carrying a new run's
  paragraphs → `runA_<run>/sample_<field>.jsonl` (+ `MISSING_<run>.txt`);
  `rebuild_sample_runA.py` rebuilds it from full work orders when a later
  `run_eval.py --pull` thinned the run's pending files.
- Graders write `results_<field>.jsonl` (baseline, top level) and
  `runA_<run>/results_<field>.jsonl`.
- `readout.py` → `READOUT.md` (baseline); `readout_paired.py <run>` →
  `READOUT_PAIRED_<run>.md` (paired vs the baseline, per axis and stratum);
  `label_truth_readout.py <run>` (label-wire runs only).
- `tryout/` — the prompt tryout tooling: `run_tryout.py`, `run_tryout_pass2.py`,
  `run_tryout_c16.py`, `run_tryout_p3.py` (the production call shape: gpt-4.1,
  strict schema, seed; arms ≥5 repeats, read as a mode fraction) and the repeat
  probes `repeat_mw2.py`, `repeat_oneanddone.py`; `run_tryout_b.py` (2026-09-14:
  production requests REBUILT from the evidence snapshot, no Mongo — `--check`
  proves the rebuild against a pulled request; N repeats per arm, production
  call shape), `field_sections.py` (the run-B field sections of record: `arms`
  writes the per-field system texts, `statics` would apply them — never run),
  `tryout_b_check.py` (parse/coverage/length of the outputs); judge notes `JUDGE_*.md`
  (`pass2/JUDGE_RUN_B_BRIEF.md` + six `JUDGE_RUN_B_<field>.md` = the run-B tryout);
  `pass2/` holds the arms' system texts (`*_system.txt`), the pulled production
  requests (`req_*_user.txt`) and the outputs. `pass2/label_system.txt` is the
  run X1 static (md5 `5f7ba520…`), tracked so the experiment stays reproducible.

## Data policy

`*.jsonl`, `*.json` and `*.txt` here are ignored by explicit rules in the repo
`.gitignore` (they carry site text; the harness tree un-ignores data by
default); the scripts, briefs, readouts, judge notes and the prompt arms
(`*_system.txt`) are tracked. The tryout inputs the scripts name under
`docs_local/synthesis_shape_tryout_20260909/` are local-only.

## Running

From the repo root: `.venv/bin/python apps/data_etl_app/tests/test_stages/synthesis/answerability/<script>`.
The scripts find the harness by their own path and the repo root by `.git`.
