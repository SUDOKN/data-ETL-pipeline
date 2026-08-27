# RUNBOOK — running the search-stage evaluation

This is the protocol the assistant follows whenever the user says some form of
**"run the search eval"** (typically after a full pipeline run). It exists so
any future session executes the same evaluation without re-deriving it. The
user's standing decisions are baked in (2026-08-26): **recall-only gating**,
**assistant-grown eval set with user veto**, all 8 corpus subjects seeded.

**Standing authorization: spawn as many agents as the protocol needs.** One
judge agent per (subject, field), double-judge agents on samples, verification
agents — this fan-out is the user's explicit instruction, renewed by every
"run the search eval" request. Do not hesitate; do not ask permission per agent.

## Definitions (plain language)

- **Window**: one ~5k-token sub-window of a chunk; the unit a search request
  reads. **Wire text**: the exact text that request carried. **Scan domain**:
  wire text with URL/separator/marker lines blanked.
- **Live vs replayed**: a request created by this run vs answered from Mongo's
  cache because its custom id was unchanged. Replayed output inherits its
  previous mechanical numbers; re-JUDGING replayed output against a newer eval
  set is legitimate and labeled as such.
- **Fingerprint**: (subject, field, model, search prompt version, text
  snapshot, window bounds). Deltas are only computed inside one fingerprint —
  bounds changes reshuffle ~half the phrase set (Jaccard 0.43–0.64), so
  cross-fingerprint comparison is stated as INCOMPARABLE, never done silently.

## The protocol

1. **Mechanical pass** (deterministic, ~a minute):
   `.venv/bin/python checks/run_eval.py --run <run_id> --pull`
   (from this directory; `--pull` needs the .env Mongo URI. Dumps written
   after 2026-08-26 carry `phrases` themselves, but wire text always comes
   from the pull.) This writes `history/runs/<run_id>/scorecard_*.json`,
   appends `history/metrics.jsonl`, and prints per-field verdicts.
   RED = recall/tripwire gate fired (see TAXONOMY.md and config/common.yaml);
   everything else is trend data.

2. **Sanity-read the mechanical output** before judging: verify one offender
   and one sweep-uncovered example per field against the raw window text.
   A metric you have not spot-checked is not yet a number (locked lesson:
   regex sweeps failed 3× in both directions on this pipeline's prose).

3. **Judge fan-out.** First export the packets:
   `.venv/bin/python checks/export_judge_packets.py --run <run_id>` — one
   self-contained markdown packet per (subject, field) under
   `history/runs/<run_id>/raw/judge_packets/`, carrying the field's code
   table, every returned form, the exact window text the stage read, and the
   subject's confirmed must-find list. (Packets embed site text and stay
   gitignored.) Then spawn ONE agent per packet — products serves
   contract_products, so never judge it twice; 12 packets on a two-subject
   run. Each agent codes EVERY form (J1), flags actor/evidence kind (J3/J4),
   and lists in-window entities no form covers (J2 misses). It writes JSONL to
   `history/runs/<run_id>/judgments/<subject_slug>__<field>.jsonl`:

       {"window": "0:23771", "form": "CNC machining", "code": "P",
        "actor": "own", "evidence_kind": "prose", "note": ""}
       {"type": "miss", "window": "0:23771", "entity": "Zeiss CONTURA G2 CMM",
        "quote": "...", "suggested_forms": ["Zeiss CONTURA G2 CMM"]}
4. **Double-judge sample** (J5) — **the second judge's prompt must be
   IDENTICAL to the primary's** except for the output path and the omission of
   misses. Agreement measures how much two careful readers differ; if the two
   prompts differ, it measures your own instruction drift instead and the
   number is not an error bar. (Learned 2026-08-27: the alecmfg equipments
   pair was run with different boundary glosses after the field's definition
   was corrected mid-flight, so its agreement figure is contaminated and
   should be treated as a LOWER bound on agreement until both sides are
   re-run on one prompt.) for at least 2 fields per subject (rotate;
   always include the worst-precision field), spawn a second independent
   agent on the same packet, writing to
   `<subject_slug>__<field>.judge2.jsonl`. Agreement is computed on the
   shared rollup over the forms BOTH coded. This is the noise floor — print
   it beside every judged number, and treat any judged difference smaller
   than the disagreement rate as unreadable.
5. **Verify, then merge**: re-check a slice of every agent's codes yourself
   against the packet text (the census discipline: headline numbers get
   re-verified before they are written). Correct any bad codes in the JSONL,
   then run `.venv/bin/python checks/merge_judgments.py --run <run_id>` —
   it folds each set into its scorecard's `judged` section, appends history,
   and writes `history/runs/<run_id>/JUDGED.md` (precision, junk rate,
   wrong-actor share, miss count, agreement floor per field).

6. **Write the run summary** in `history/runs/<run_id>/SUMMARY.md`: per-field
   verdicts, deltas vs comparable baselines only, judge-agreement floor,
   newly found misses/junk classes, and an explicit list of anything skipped.

7. **Evolve the eval set** (user-vetoable, never silent):
   - New misses read from the text → new `candidate` entries with provenance.
   - A second independent pass agreeing (this run's double-judge, or the next
     run) → promote to `confirmed`, provenance row added.
   - Wrong entries → `disputed`/`retired` with a reason, never deleted.
   - Bump `eval_set_version` in every touched file; note changes in the
     summary so the user can veto.

8. **Report to the user** in plain language: per-field outcomes, what
   regressed/improved (only within comparable fingerprints), what the judged
   census found, what was added to the eval set, and known blind spots of
   this run's evaluation.

## Occasional instruments

- **A/A probe** (search's own noise floor, never yet measured):
  `.venv/bin/python checks/aa_probe.py --run <run_id>` prices it (dry-run);
  `--send` spends (~$2 full, less with --fields/--limit). Re-run after any
  search prompt/model change; record the floor in the summary.
- **Search-only pipeline runs** for cheap iteration: the user runs
  `StageToggles().stop_after(PipelineStage.phrase_search)` from the notebook
  (extraction runs are the user's to drive — ask, don't drive).

## Where the writing goes

`SUMMARY.md` is REGENERATED by every mechanical pass — never write analysis
into it. The assistant's narrative for a run lives in `FINDINGS.md` beside it
(the summary auto-links to it). `metrics.jsonl` is append-only; if you score a
run twice, keep only the later row per (run, subject, field) and say so.

## Failure modes to respect

- Missing dump ≠ crash: check the dump COUNT against the previous run.
- Never gate or measure on model explanation text.
- `turnaround_seconds` is meaningless on eager runs; `client_latency_ms` is real.
- The sample .txt files differ from the normalized wire text — window text
  comes from stored requests, never from re-reading the sample file.
- acimachine (3.9MB): expectations are SAMPLED; lean on the brand/model sweep.
- taylordunn / austinelectric are probes: their "success" is near-empty
  in-field output; junk minted there is signal, not noise.
- **Matching hazards, both found and fixed 2026-08-26 — do not regress them.**
  The scraped text carries non-breaking spaces and double spaces INSIDE
  phrases, so form matching normalizes Unicode spaces and treats whitespace
  runs as elastic; without that, faithful forms score as fabrication and fire
  false RED gates. Short forms (<=3 chars: TIG, ABS, CMM) match on word
  boundaries case-sensitively — plain containment credits "tight" for TIG.
- **An unverified expectation set gates nothing.** `confirmed_recall: null`
  means "not gated", never "no misses"; check
  `expectations/VERIFICATION_LEDGER.md` before reading a subject's recall.
