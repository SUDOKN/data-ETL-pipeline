# Warn / raise audit — follow-ups

Opened 2026-08-27 from the audit in `INVENTORY.md`. Two items fixed the same
day (see "Done" below); two deferred by decision, recorded here.

---

## OPEN 1 — extraction items get exactly one pass, by construction

**Want:** retry logic on extraction queue items.

**Why it is needed:** all four queue bots ack the SQS message from a `finally:`
block, so the delete runs on the exception path too:

- `apps/data_etl_app/src/data_etl_app/bots/new_extract_queue_bot.py:376-379`
- `apps/data_etl_app/src/data_etl_app/bots/gt_extract_queue_bot.py:370-373`
- `apps/data_etl_app/src/data_etl_app/bots/new_scrape_queue_bot.py:235-236`
- `apps/data_etl_app/src/data_etl_app/bots/gt_scrape_queue_bot.py:232-233`

There is no retry, no dead-letter queue, and no reliance on the visibility
timeout for redelivery. One transient failure — an OpenAI 5xx, a Mongo blip, a
network drop — permanently removes that subject from the queue. An
`ExtractionError` row is written, so the loss is auditable after the fact, but
nothing re-drives it.

**Consequence today:** every retry mechanism *inside* the pipeline
(`RESPONSE_PARSE_ERROR_CAP = 3`, `MAX_UNPRODUCTIVE_PASSES = 3`, the per-stage
under-answer retry) sits underneath a queue layer with zero retries. The
in-pipeline budgets only ever get spent within a single pass of a subject.

**Not yet decided:** where the retry lives (bot-level re-drive vs. letting the
visibility timeout redeliver vs. a real DLQ), how many attempts, and whether a
retried subject resumes from its deferred state or restarts. The `finally:`
delete is the thing to change; everything else is open.

---

## OPEN 2 — NAICS code triple dropped silently in the TTL generator

**Deferred 2026-08-27 by decision — not being looked at now.**

`apps/data_etl_app/src/data_etl_app/knowledge/ttlgenerator3_new.py:283-292`
wraps `float(code)` in a bare `except Exception: pass`. A non-numeric NAICS
code (e.g. `"31-33"`, a sector range) loses its `hasNAICSCodeValue` triple
while `RDF.type`, `hasPrimaryNAICSClassifier` and `hasNAICSTextValue` are all
still added. The result is partially populated RDF with no signal anywhere that
a triple went missing.

Cheapest fix when it comes up: log the dropped code and keep going, so the gap
is at least countable.

---

## Done 2026-08-27

- **Orchestrator error recording.** The loop over the nine real extraction
  fields in `manufacturer_extraction_orchestrator.py` had no error handling at
  all, while the three prerequisite fields above it each recorded an
  `ExtractionError` naming themselves. A field failure therefore reached the
  bot as one bare `general_processing` row with no way to tell which field
  died. Now recorded per-field and **re-raised** — the subject still stops
  exactly where it did before. *Isolating the nine fields from each other is a
  separate, still-open decision.*
- **Phrase-contract guards no longer disable themselves silently.** The three
  `if sent_ids is None: return <unvalidated>` early exits in
  `phrase_blocks_contract.py` (lines 303, 469, 618) now warn first. Every
  request we build carries a block, dummies included, so if one of these ever
  fires it means the renderer stopped emitting the fence and the hold has been
  switched off across every stage — previously with nothing in the log.
