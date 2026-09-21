# Step 2 tryout (grounding / freehand / screening prompts against today's)

Tries the Step 2 prompt families (design draft `STEP2_DESIGN_DRAFT_2026-09-20.md`, local-only)
against today's on REAL records of a finished run — by default the pre-Step-2 baseline
20260915T024255 — in the production call shape (gpt-4.1, the notebook's `GPTModelParams`, a
strict schema generated from the catalog, temperature 0, seed 12345, a fresh nonce per call),
N repeats per arm read as a mode. First run 2026-09-21 (Phase 4 of the 2026-09-14 survey drafted
the harness; the Step 2 catalogs are its first real arms).

- `run_grounding_tryout.py` — rebuilds a chunk's requests from the run's dumps (records = the
  synthesis block's focal forms and paragraphs, packed 25 per request as production does; the
  vocabulary = the repo ontology rendered by `render_concept_outline`, dash-line shape for the
  new call) and runs the arms. `--list` prints chunks and group counts; `--write` writes the
  rebuilt request texts; outputs under `out/<tag>/` (gitignored) with a `.usage.json` per call.
  Arms: `a` today's two grounding calls; `b` the Step 2 one-call grounding (`phrase_grounding`
  catalogs); `fa`/`fb` today's vs the reworded freehand grounding (equipments, products,
  contract products; today's text snapshotted under `arms/today/`, byte-identical to the
  published version); `sa`/`sb` today's screening vs the Step 2 unit screening
  (`phrase_unit_screening` catalogs), BOTH on the same mode candidates of a grounding arm
  (`--screen-from`, default `b` / `fb`), so the screening comparison isolates the prompt.
- `subject_names.json` — the 18 subjects' display names, read once from the baseline's stored
  screening requests (the dumps do not carry them); the screening request's first line.
- `check_tryout.py` — the mechanical readout (`out/CHECK.md|json`): parse success, coverage,
  membership (reroutes, folds), quotes found in the record, multi-record entries, stability
  (mean pairwise Jaccard, records identical in every repeat), mode overlap between arms, the
  freehand "candidate named in record" proxy, screening acceptance / evidence-distance /
  failed-rule distributions and the sa-vs-sb agreement matrix, unit coverage, cost per arm.
- `sample_for_judges.py` — draws the judged sample (`out/judge/`): single-arm pairs, proposals,
  unnamed freehand candidates, screening disagreements and "inferred" acceptances, plus a
  slice of agreed rows; packets of 40 for Sonnet judges (brief: `JUDGE_BRIEF.md`).
- `judge_readout.py` — joins the judges' verdicts (`out/judge/verdicts/*.jsonl`) back to the sample:
  codes by arm and field, arm b's quote verdicts, each screening arm's confusion against the judged
  truth, the evidence-distance calibration (`out/judge/READOUT.md`). First run 2026-09-21: report in
  `docs_local/grounding_gap_survey_2026-09-14/STEP2_TRYOUT_2026-09-21.md`.

Guard: `out/` holds model outputs and rebuilt requests (scraped site text); never `git add` it —
its own `.gitignore` ignores everything inside. `arms/today/` and `subject_names.json` are
tracked (prompt text and names only).
