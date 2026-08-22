# Mention-stage under-answer investigation (2026-08-22, run 20260822T061410)

Scripts read the run's mention-collection requests out of Mongo (`pull_mention_requests.py` → `mention_requests.json`,
not committed: it holds the site text) and the sample texts in `apps/.../knowledge/sample_scraped_texts/`.

- `group_analysis.py` / `group_analysis_output.txt` — per-request skip ratio (forms with an exact hit that got no mention)
  vs near-duplicate density, group size; the worst groups. 91% of with-hit forms answered; 2/183 answers returned fewer forms than sent.
- `replay_experiment.py` / `replay_results.txt` — deterministic replays (gpt-4.1, temperature 0, seed 12345, same response_format)
  of the two worst groups with controlled variants: exact, reversed, one-form-per-normalize-key, skipped-only, halves,
  capitalized-only. Verdict: the 2/30 steelcraft answer is a non-reproducible eager stop (exact replay → 27/27);
  the 3/21 alecmfg answer is deterministic and independent of near-duplicates (listing/heading occurrences skipped).
- `backfill_prototype.py` / `backfill_prototype_output.txt` — the mechanical backfill rule run on the real 2/30 window:
  35 unaccounted hits → 35 code mentions (sentence-clipped line, code page, fixed location marker, source=floor_scan).
