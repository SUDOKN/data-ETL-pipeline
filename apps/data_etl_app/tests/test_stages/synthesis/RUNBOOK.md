# RUNBOOK — running the synthesis-stage evaluation

The protocol the assistant follows whenever the user says some form of **"run
the synthesis eval"** (typically after a full pipeline run). It exists so any
future session executes the same evaluation without re-deriving it. The user's
standing decisions are baked in (2026-08-26): **exhaustive judgment with a
content-keyed cache**, the dump instrument fixed so full runs carry the
synthesis block, all 8 corpus subjects seeded.

**Standing authorization: spawn as many agents as the protocol needs.** One
judge agent per (subject, field), split at request boundaries for large
fields, plus verification passes — this fan-out is the user's explicit
instruction, renewed by every "run the synthesis eval" request. Do not
hesitate; do not ask permission per agent. (If the account is over its spend
limit, agents die mid-flight; that is safe — see §6.)

## Definitions (plain language)

- **Record**: one group (all spellings of one extracted phrase) as synthesis
  meets it — an opaque `record_id`, one **focal form** (its most frequent
  member spelling), and its **snippets**.
- **Snippet**: one distinct evidence passage, copied verbatim from the site,
  in which the entity is named. Since the 2026-09-05 wire port there is no
  per-snippet location on the wire: the snippets are the fenced evidence, and
  the model reads them against the **chunk text** (below).
- **Chunk text**: the whole chunk of scraped site text the record's snippets
  came from (~20k tokens), placed at the top of the request. The statics say it
  PLACES the snippets (heading, table, list, surrounding words) and never adds
  claims; the snapshot stores each chunk once under `evidence_snapshots/<run>/
  chunk_text/`, and every work order names the file per `chunk_bounds`.
- **Location** (code): the heading or table header row each mention sits
  under, computed by the fold after the fact (`fold.groups[].mentions[]
  .location`). The model never sees it. The harness tracks the located share
  as a WATCH number (never a gate) and hands each record's distinct locations
  to the judge as pointers into the chunk text.
- **Co-pack**: the records that rode in one request (at most the packing cap
  of snippets, groups never split — 50 through run 20260912T225723, 10 from
  the 2026-09-12 packing change on; the custom id's `|gs=` says which).
  Identity swaps and evidence bleed happen inside a co-pack, so J3 needs
  this context. A record answered by the under-answer **retry** pass
  (`retried: true`) has the retry request as its co-pack — the work order's
  `request_custom_id` already points there.
- **Twin**: one group synthesized separately in both chunks. Twins get
  independent paragraphs and may diverge — always key by chunk bounds.
- **Replayed**: a request answered from Mongo's cache because its custom id
  (including the `ud=` evidence digest) was unchanged. Replayed records are
  byte-identical, which is exactly what makes the judgment cache work.

## The protocol

1. **Mechanical pass + evidence snapshot** (deterministic, ~a minute):
   `.venv/bin/python checks/run_eval.py --run <run_id> --pull`
   from this directory. `--pull` needs the repo `.env` Mongo URI and must be
   done promptly — a scoped delete erases the wire evidence, and no dump
   carries the snippets or the chunk text. This writes
   `history/runs/<run_id>/<subject>__<field>.json` scorecards, appends
   `history/metrics_scoreboard.csv`, and emits **work orders** under
   `history/runs/<run_id>/pending/` holding only records the ledger has not
   judged. Any invariant FAIL is a pipeline or instrument break — investigate
   before judging anything. **Pull BEFORE any stored-request delete**: a
   second draw of the same subjects (the notebook's delete block live) erases
   the previous run's wire evidence from Mongo; run 20260913T170246 was
   pulled first for exactly that reason.

2. **Sanity-read the mechanical output** before judging: verify one flagged
   record per field (a focal-form-absent flag, an identical-synthesis cluster)
   against its work-order evidence. A metric you have not spot-checked is not
   yet a number (locked lesson: regex sweeps failed 3× in both directions on
   this pipeline's prose).

3. **New subject this run?** Add its `subject_name` to
   `expectations/<subject>/subject.yaml` and to `KNOWN_SUBJECT_NAMES` in
   `checks/loading.py` (read it off the run's `business_desc` dump), and seed
   field probe files if absent.

4. **Judge fan-out.** One general-purpose agent per work order (Sonnet by
   the user's standing rule; the model name goes into every `judge` string);
   split above ~250 records into slices of ~150–200, **never splitting a
   `request_custom_id` across agents** (co-packed context is what J3 needs).
   The agent prompt is **`JUDGE_PROMPT_TEMPLATE.md`** (harness root, tracked)
   with its `{...}` fields filled — copy the filled text to
   `history/runs/<run_id>/JUDGE_PROMPT.md` first, so the run's judgment stays
   reproducible, and add that run's calibration rulings to the template at
   the end of the run (the template is the accumulated rulings). It contains,
   in this order:
   1. `TAXONOMY.md`'s dimensions section (J1–J5, J7) — paste or give the path.
   2. That field's `## <field>` extension section (J6).
   3. Its slice of the work order (path + explicit index range), and the
      work order's `chunk_texts` map — the agent opens a chunk file only to
      place a snippet or to classify an unsupported claim (containment breach
      vs fabrication); it never judges from the chunk text alone.
   4. The output contract: JSONL, one verdict object per record, schema in
      TAXONOMY.md §Output, written to
      `history/runs/<run_id>/verdicts/<subject>__<field>[__partN].jsonl`;
      echo `content_key`, `taxonomy_version`, `pv`, `evidence_sha256`
      unchanged; every record in the slice gets a row — no sampling (a skipped
      record stays pending forever).
   5. The discipline reminders: judge by reading, quote what convicts,
      `locations` are code pointers (not something the model saw), the
      `designations_dropped` list is a nomination for J2's designation clause
      (own designations only, since 2026-09-10) and must be verified against
      the snippets, a page-supported claim about the focal entity is a
      grounded import (J1 pass + note), not a breach, `unclear` is honest,
      probes apply only to records they name.
   Tell agents to build the file with **shell appends in batches** — if an
   agent dies mid-run its partial file is still valid JSONL.
   Tooling (2026-09-11): `checks/judged_population.py --run <id> --baseline
   <prev>` writes the judged subset work orders + `SLICES.json` (cluster
   members + the baseline-retried singletons + a random singleton sample);
   `checks/judge_jobs.py --run <id>` packs slices into ≤190-record jobs and
   writes one instruction file per job under `pending/judged/jobs/` from the
   run's `JUDGE_PROMPT.md`, so an agent prompt is just "read J<nn>.md and
   follow it". Run ~15 agents concurrently, launch one per completion (the
   harness caps concurrent subagents at 20; Sonnet rate limits — server-side
   429s and the account's session limit — cut agents off mid-slice).
   **Resume mode (2026-09-13):** relaunch a cut-off job with the instruction
   to read the `out` file's existing content keys and APPEND only the
   missing records, never truncating; the append-only discipline makes this
   safe — `checks/verdict_coverage.py --run <id> --jobs` shows 0 duplicates
   and names the jobs still pending. Normalise a resumed agent's judge string
   if it dropped the `agent:` prefix (coverage reports it as `bad_schema`).

5. **Verify before accepting** (non-negotiable): for every agent, re-read
   against the work order **every J3 fail, every `major` fail** (they are
   rare), 5 random passes, and any headline rate that looks surprising.
   Correct rows before ingestion; mark re-checked rows `"verified": true`.
   Tooling (2026-09-11): `checks/verify_packets.py --run <id> [--calibration-file
   <verdict file>]...` builds the packets (J3 fails, majors, 5 random passes per
   file, plus every J2/J6 fail of any slice a judge flagged); Sonnet verifiers
   write full replacement rows to `verify/corrections_<k>.jsonl`;
   `checks/apply_corrections.py --run <id>` merges them and prints the
   transitions. A judge that flags its OWN already-written rows as wrong
   (it may not rewrite an appended file) names their content keys in its
   reply: put them in an extra packet with
   `checks/extra_packet.py <run> <packet number> <verdict file> <keys…>`
   (numbered after the built packets; `verify_packets.py` without `--only`
   deletes existing packets, so build the main packets first) and hand it
   to a verifier like any other. Then `checks/paired_readout.py --run <id> --baseline <prev>`
   for the identical-evidence pairing; add `--by-subject` for the per-subject
   and per-field FLIP counts (records whose verdict differs between the two
   sides) — the number the A/A floor is quoted in (2026-09-12: mathewsco
   8.2%, tanfel 3.0% any-fail flips on identical inputs) and the number every
   variance lever is read against; `checks/request_mode_readout.py --run <id>`
   shows whether the fails cluster by request (the per-request mode: 22% /
   51% / 81% of failing records in majority-failing requests on the cap-50
   draws and the cap-10 run of 2026-09-12/13); with two or more judged draws
   of the same subjects on the same pins, `checks/medoid_readout.py --runs
   <a> <b> <c>` prints the three-way floor (pairwise disagreement), the split
   and unanimous-fail counts, and what an N-sample text medoid would have
   scored (2026-09-13: 6.4% pairwise floor, 8 of 1,449 records fail in every
   draw, the text medoid no better than a single draw).
   Since 2026-09-13 every answer carries four LABELS (`doer`, `doer_name`,
   `capacity`, `dealing_words`; work orders show them to the judge):
   `checks/label_readout.py --run <id> [--baseline <second draw>]` prints
   their distributions and, between two draws on identical evidence, the
   per-record label flips — the mechanical counterpart of the verdict flip.
   Two of the original census's own numbers were corrected exactly this way —
   the step is a measurement, not a formality.

6. **Ingest and report:**
   `.venv/bin/python checks/run_eval.py --run <run_id> --finalize`
   ingests `verdicts/*.jsonl` into `history/judgments/`, re-scores, and writes
   `history/runs/<run_id>/REPORT.md`. Malformed trailing lines (from a killed
   agent) are counted and skipped, never fatal. Partial coverage is safe and
   expected: judged records cache, the rest stay pending for the next pass —
   record the split in the README's STATE block.
   Then write the user a plain-language summary: per field, judged fail rates
   **with numerators and denominators**, new defects with one verbatim example
   each, probe outcomes, and what was added to the eval set.

7. **Grow the set (mandatory last step).** Every NEW defect class becomes:
   - a probe in `expectations/<subject>/<field>.yaml` (status `active`,
     `motivated_by` this run) — cache-safe, probes are not hashed; and
   - an entry in `CANDIDATE_DIMENSIONS.md` when no existing dimension names it.
   Every fixed-flaw class that regressed: flip its probe's note and tell the
   user. **Promoting a candidate into TAXONOMY.md changes `taxonomy_version`
   and invalidates that field's cached verdicts** — batch promotions between
   runs, never mid-pass, and say so in the report.

## Cost discipline

Unchanged records replay byte-identically and hit the cache; a normal
follow-up run re-judges only records whose synthesis, evidence, prompt version
or taxonomy changed. If `pending` is unexpectedly large on a no-change run,
suspect an upstream `ud=` drift (a search prompt edit or a fold change
re-digests every synthesis request) before suspecting the cache. A republish
of the synthesis statics changes every `pv=` and therefore empties the cache
for that run by design (2026-09-05 was such a run: 0 hits, 22,162 pending) —
that is the window in which TAXONOMY.md can be edited at no extra cost.
