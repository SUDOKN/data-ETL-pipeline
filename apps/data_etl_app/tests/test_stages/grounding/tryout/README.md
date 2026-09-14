# Grounding-shape tryout (Phase 4 of the 2026-09-14 grounding-gap survey)

Tries the drafted three-list grounding prompt (option-major: `matched`, `proposed`,
`unmatched`) against today's two-call shape on REAL records of a finished synthesis run,
in the production call shape (gpt-4.1, the notebook's `GPTModelParams`, a strict schema,
temperature 0, seed 12345, a fresh nonce per call), N repeats per arm read as a mode.

- `run_grounding_tryout.py` — rebuilds a chunk's grounding request from the run's partial
  dump (records = the synthesis block's focal forms and paragraphs, packed 25 per request
  as production does; options = the repo ontology rendered by `render_concept_outline`)
  and runs the arms. `--list` prints the chunks and group counts; `--write` writes the
  rebuilt request text for inspection; outputs under `out/<tag>/` (gitignored).
- `arms/<field>_three_list_system.txt` — arm `b`'s system prompt, the draft the user
  approved in chat. Arm `a` uses the published prompts under `final_texts/assembled/`.
- `check_tryout.py` — the mechanical readout: parse success, coverage violations,
  membership drops, proposals, citation completeness, per-record stability across repeats,
  and the two arms' mode overlap. Everything that needs reading goes to judges.

Guard: `out/` holds model outputs and rebuilt requests (scraped site text); never
`git add` it — its own `.gitignore` ignores everything inside.
