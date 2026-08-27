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
  member spelling), and its **entries**.
- **Entry**: one distinct evidence passage — a verbatim `snippet` from the
  site plus a `location` sentence saying where it sits. Entries are ALL the
  model saw; it cannot see the site.
- **Co-pack**: the records that rode in one request (≤50 entries, groups never
  split). Identity swaps and evidence bleed happen inside a co-pack, so J3
  needs this context.
- **Twin**: one group synthesized separately in both chunks. Twins get
  independent paragraphs and may diverge — always key by chunk bounds.
- **Replayed**: a request answered from Mongo's cache because its custom id
  (including the `ud=` evidence digest) was unchanged. Replayed records are
  byte-identical, which is exactly what makes the judgment cache work.

## The protocol

1. **Mechanical pass + evidence snapshot** (deterministic, ~a minute):
   `.venv/bin/python checks/run_eval.py --run <run_id> --pull`
   from this directory. `--pull` needs the repo `.env` Mongo URI and must be
   done promptly — a scoped delete erases the wire evidence, and a full-run
   dump does not carry entries. This writes
   `history/runs/<run_id>/<subject>__<field>.json` scorecards, appends
   `history/metrics_scoreboard.csv`, and emits **work orders** under
   `history/runs/<run_id>/pending/` holding only records the ledger has not
   judged. Any invariant FAIL is a pipeline or instrument break — investigate
   before judging anything.

2. **Sanity-read the mechanical output** before judging: verify one flagged
   record per field (a focal-form-absent flag, an identical-synthesis cluster)
   against its work-order evidence. A metric you have not spot-checked is not
   yet a number (locked lesson: regex sweeps failed 3× in both directions on
   this pipeline's prose).

3. **New subject this run?** Add its `subject_name` to
   `expectations/<subject>/subject.yaml` and to `KNOWN_SUBJECT_NAMES` in
   `checks/loading.py` (read it off the run's `business_desc` dump), and seed
   field probe files if absent.

4. **Judge fan-out.** One general-purpose agent per work order; split above
   ~250 records into slices of ~150–200, **never splitting a
   `request_custom_id` across agents** (co-packed context is what J3 needs).
   Each agent prompt must contain, in this order:
   1. `TAXONOMY.md`'s dimensions section (J1–J5) — paste or give the path.
   2. That field's `## <field>` extension section (J6).
   3. Its slice of the work order (path + explicit index range).
   4. The output contract: JSONL, one verdict object per record, schema in
      TAXONOMY.md §Output, written to
      `history/runs/<run_id>/verdicts/<subject>__<field>[__partN].jsonl`;
      echo `content_key`, `taxonomy_version`, `pv`, `evidence_sha256`
      unchanged; every record in the slice gets a row — no sampling (a skipped
      record stays pending forever).
   5. The discipline reminders: judge by reading, quote what convicts,
      locations count as evidence, `unclear` is honest, probes apply only to
      records they name.
   Tell agents to build the file with **shell appends in batches** — if an
   agent dies mid-run its partial file is still valid JSONL.

5. **Verify before accepting** (non-negotiable): for every agent, re-read
   against the work order **every J3 fail, every `major` fail** (they are
   rare), 5 random passes, and any headline rate that looks surprising.
   Correct rows before ingestion; mark re-checked rows `"verified": true`.
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
suspect an upstream `ud=` drift (a mention/location prompt edit re-digests
every synthesis request) before suspecting the cache.
