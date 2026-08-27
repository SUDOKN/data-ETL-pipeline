# Search-stage evaluation

A permanent, per-field, evolving evaluation of the SEARCH stage only — the
first LLM pass that extracts verbatim surface forms from ~5k-token windows of
scraped site text (plus brute round 0 on concept fields, plus recursive
rounds if ever re-enabled). Downstream stages (mentions, synthesis, grounding,
screening, descent) are out of scope except as instruments.

Fields evaluated per run: industries, process_caps, material_caps, equipments,
conformity_attestations, products (products' search output also feeds
contract_products — one physical request, never double-counted).

- **RUNBOOK.md** — the protocol the assistant executes on "run the search eval",
  including the standing agent fan-out authorization and gating policy
  (recall-only; user decision 2026-08-26).
- **METRICS.md** — every number the instrument reports, the flaw it tracks,
  its baseline, and which ones gate.
- **TAXONOMY.md** — judgment codes per field + shared rollup + audit format.
- **EXPECTATIONS_SCHEMA.md** — the evolving eval set's contract (span-witnessed
  entries, candidate→confirmed lifecycle with user veto).
- **expectations/** — per-(subject, field) inventories for all 8 corpus
  subjects, seeded 2026-08-26 from the sample scraped texts.
- **config/** — thresholds, prices, per-field sweep lexicons, born-from baselines.
- **checks/** — the mechanical battery (see each module's docstring):
  pull.py (Mongo → raw request docs), loading.py, masking.py, mechanical.py,
  scorecard.py, run_eval.py, aa_probe.py.
- **history/** — append-only `metrics.jsonl` + per-run scorecards, summaries
  and judgment audit files. `history/runs/*/raw/` holds site text and is
  gitignored.
- **tests/** — pytest for the harness code itself (pure functions + fixtures;
  no Mongo, no dumps needed). Collected by the normal repo-wide pytest run.

Origins: formalizes `search_self_eval.py` + `search_metrics.py`
(pipeline_v3_evidence/2026-08-22_run_223715_location_stage/) and the flaw
ledger assembled 2026-08-26 from all twelve v3 runs' evidence.
