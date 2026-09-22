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
- `outline_variants.py` + `--outline dash today defs2 layered sandwich` on the runner (the OUTLINE
  tryout, 2026-09-21, after `docs_local/grounding_gap_survey_2026-09-14/VOCABULARY_FORMAT_ASSESSMENT_2026-09-21.md`):
  arm `b`'s round-5 prompt in the CACHEABLE layout — the vocabulary moves into the system message
  (instructions, then the outline), the user message is the nonce and the records — with the
  vocabulary in one of five shapes: `dash` (round 5's, the reference in the new position), `today`
  (production's, no definitions), `defs2` (definitions on depth 1–2 only), `layered` (flat lists under
  depth headings + "prefer the deeper option"), `sandwich` (dash + the three decisive sentences repeated
  after the records). Files `out/<tag>/b-<variant>_<k>.json`; every `.usage.json` carries the provider's
  `cached_tokens`; one call per (field, variant) is sent first and alone to warm the cache. Each shape's
  prompt substitutions (the outline paragraph, the option-meaning phrase, the layered GR-M2a note) are
  recorded in `outline_variants.py` and applied by string replacement — no catalog is written.
  `check_tryout.py` reads each outline arm like arm b, adds a `depth` readout (choices by depth,
  non-leaf choices, presence vs parent-child vs cross-branch flips) and each arm's mode against
  round 5's arm b (`mode_vs_round5_b`, the judge sample); `usage` prices cached tokens at the cached rate.
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
