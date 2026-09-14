# Agent playbook — how the grounding eval is executed

This file is standing instructions to the Claude session running the evaluation. It
exists so every run is executed the same way regardless of which session runs it.
The user's decisions of 2026-08-26 that shaped it: **full census every run** (no
sampling), all four grounding stages including descent and declines, everything
persisted in this folder.

Agent count is uncapped. Spawn as many as the run needs — the unit of ownership is
one (subject, field) dump per judge agent. Do not split one dump across agents:
merged counts require one owner applying one taxonomy to a whole dump.

## The four phases of one evaluation run

### Phase A — deterministic pass (code only, no agents)

```
.venv/bin/python checks/run_eval.py --run <run_id>
```

Runs every structural check over `packages/logs/extraction_dumps/<run_id>/`, writes
`history/runs/<run_id>/metrics.json` and `history/runs/<run_id>/DETERMINISTIC.md`, and prints
the judge-agent worklist (one line per (subject, field) dump with item counts).
Optional `--subject` / `--field` filters restrict it. Phase A must complete before
agents are spawned — its item enumeration is what the agents judge.

### Phase B — the judge agents (full census)

Spawn **one judge agent per (subject, field) dump**, in parallel, background. Every
agent gets the same prompt template (below), instantiated with its dump. Each agent
judges **every item** in its dump — every tag instance, every decline, every descent
hop; no sampling — and writes one JSONL file per the format in `TAXONOMY.md`.

Prompt template (fill the ⟨brackets⟩; keep the rest verbatim so judging stays
uniform):

> You are a census judge for the SUDOKN grounding evaluation. Your dump:
> `packages/logs/extraction_dumps/⟨run_id⟩/⟨subject⟩__⟨field⟩.json`.
>
> Read, in order: (1) `apps/data_etl_app/tests/test_stages/grounding/TAXONOMY.md` —
> your codes and binding judgment rules; (2) the eval-set files
> `config/common.yaml`, `config/fields/⟨field⟩.yaml`, and
> `expectations/⟨subject_slug⟩/⟨field⟩.yaml` — the field's watch items and the
> subject's expectation inventory; (3) `packages/logs/extraction_dumps/HOW_TO_READ_A_DUMP.md`
> Parts 5–7 if the dump shape is unfamiliar.
>
> Then judge EVERY item in the dump — every tag instance in `freehand_grounding` /
> `in_vocab_grounding` / `oov_grounding` `tags` maps, every `declined` entry, every
> hop in `lvl_by_lvl_itps` — one JSONL line each, exactly the TAXONOMY.md record
> format, into `history/runs/⟨run_id⟩/judgments/⟨subject⟩__⟨field⟩.jsonl`. Judge by
> reading the record (focal form, forms, synthesis); open the scraped source text
> `apps/data_etl_app/tests/test_stages/sample_scraped_texts/⟨subject_domain⟩.txt` (moved from `knowledge/` on 2026-08-28; the markdown-format scrapes sit beside it under `sample_scraped_markdowns/`)
> whenever existence or attribution is in doubt. Never let a rule explanation decide
> a code. Tag every item that bears on a watch item with that item's id.
>
> When done, append a summary block to your file's end as a single JSON line with
> `"item": "summary"`: counts per code, the 3–5 most consequential defects with
> group_ids, recall gaps against the subject inventory (expected entities with
> strength strong/moderate that no tag covers), and — separately — any defect
> PATTERN you saw that no existing watch item names (candidate new watch items,
> with 2+ examples each).

Judge agents must be strong readers; use the default model, not a smaller one, for
judging. (Inventory-building agents may be smaller; judging is the precision path.)

### Phase C — verification and merge (the running session, personally)

The census proved verification is not optional: four of the coordinator's own
numbers were corrected by it. For each judge report:

1. Re-verify every headline claim against the dump itself (open the rows named).
2. Spot-check ≥10 judgments per dump, weighted toward F, DF, DR, and H-* codes —
   the rare, consequential codes are where a mis-calibrated judge does damage.
3. Demand the control before accepting a cause (sibling field, sibling subject,
   previous run) and check the consequence (did it ship?) before grading severity.
4. Log every correction in `history/runs/<run_id>/verification_log.md` — corrections are
   data about the instrument.

Then merge: `checks/run_eval.py --run <run_id> --merge-judgments` folds the JSONL files into
`metrics.json` (judged block), recomputes outcome tiers, and appends the ledger.

### Phase D — report and evolution

1. Write `history/runs/<run_id>/REPORT.md`: per (subject, field) — outcome tier, the
   judged and deterministic metrics against the ledger's previous rows, watch-item
   statuses, and verbatim examples for anything that moved. Plain language,
   every term defined; the reader has not seen this instrument's internals.
2. Update watch-item `status` maps in `config/fields/*.yaml` for this run.
3. **Evolution step** — present to the user, never silently apply:
   - candidate new watch items from the judges' summaries (with examples),
   - any taxonomy boundary that two judges drew differently (candidate v-bump),
   - any expected-entity or trap corrections to subject inventories.
   Accepted changes are written with provenance (`first_seen: <run_id>`).
4. If the run includes a subject with no inventory or a placeholder one, run the
   **first-contact protocol** below before Phase B, not after.

## Noise-floor discipline (binding on every comparison)

Grounding diverges on **12.9–18.7% of rows** on byte-identical input at temperature
0 (measured, run 190359 and 194457). Therefore:

- Never report a run-over-run movement in a judged rate as real unless it exceeds
  the current noise band; report it as "within noise" otherwise.
- Recompute the free A/A instrument every run that has both product fields
  (`products` vs `contract_products` share freehand payloads when their `ud=`
  digests match) and record it in `metrics.json` — the band is measured, not
  assumed.
- Single-record watch items (e.g. the FE→DE entity swap) are tracked as frequencies
  across runs, never as per-run pass/fail.
- A watch item closes only after being absent for enough consecutive runs that the
  noise floor cannot explain the absence (rule of thumb: 3 clean runs for
  population items; more for rare single-record items).

## First-contact protocol (new subject enters the eval)

When a run contains a subject with no `expectations/<subject_slug>/` directory, or
whose `subject.yaml` is marked `placeholder: true`:

1. Spawn one inventory agent per subject (smaller model acceptable) with the
   **inventory spec** below.
2. Review its output personally against spot-reads of the text before Phase B uses
   it; expectation errors poison recall metrics silently.
3. Where the text exceeds what the pipeline can read (`max_tokens_per_chunk` ×
   `max_chunks`, ≈40k tokens ≈ 160–200k chars after legal-page exclusion), the
   inventory covers only the readable prefix and must say so in `coverage_note`.

### Inventory spec (given verbatim to inventory agents)

> Read the scraped site text at ⟨path⟩ (first ⟨N⟩ characters only, if capped) and
> write **eight files** into
> `apps/data_etl_app/tests/test_stages/grounding/expectations/⟨subject_slug⟩/`
> (slug = the domain with dots as underscores, e.g. `alecmfg_com`).
>
> `subject.yaml` — the profile and provenance:
>
> ```yaml
> subject: ⟨domain⟩
> source_text: ⟨repo-relative path⟩
> text_chars_read: ⟨int⟩
> coverage_note: "full text"  # or "first N chars; pipeline chunk cap"
> built: ⟨date⟩
> built_by: "inventory agent, taxonomy v1"
> placeholder: false
> profile: >
>   2–3 sentences, plain language: what the company makes, for whom, and what kind
>   of site this is (catalog, job-shop portfolio, case studies, bilingual, …).
>   Say plainly if the scrape itself is degenerate or dominated by boilerplate.
> ```
>
> Then one file per field — `material_caps.yaml`, `process_caps.yaml`,
> `industries.yaml`, `conformity_attestations.yaml`, `products.yaml`,
> `contract_products.yaml`, `equipments.yaml` — each shaped:
>
> ```yaml
> subject: ⟨domain⟩
> field: material_caps
> expected:
>   - name: "Stainless Steel"
>     evidence: "short verbatim quote or tight paraphrase locating it"
>     strength: strong          # strong | moderate | weak
> traps:
>   - name: "Lead"
>     kind: homonym             # homonym | third_party | client_work | marketing_idiom
>                               # | doc_title | wrong_axis | wrong_actor | negated
>     note: "appears only inside 'lead time'"
> ```
>
> Write all seven field files even when a field is empty (`expected: []`) — a
> judge agent reads exactly one of them, and a missing file is indistinguishable
> from an unbuilt inventory.
>
> Field meanings — these are the axes; getting them wrong poisons the eval:
> `material_caps` = materials the SUBJECT works/processes; `process_caps` =
> manufacturing processes the subject itself performs (not a certification lab's
> tests, not installation-site work); `industries` = sectors the subject SERVES
> (its customers' sectors — the subject's own activity is NOT an industry served);
> `conformity_attestations` = certifications/standards compliance claimed FOR the
> subject (a standard merely mentioned, or a parent company's or lab's credential,
> is a trap); `products` = the subject's own catalog items; `contract_products` =
> items made to a customer's order/spec; `equipments` = machines the subject owns
> or operates (products it SELLS are the classic trap).
>
> Selectivity: 8–30 `expected` per field ranked by evidence strength (empty list is
> correct when the text shows nothing for a field); every trap you can find, each
> with its `kind`. Strength: `strong` = the site states it outright as the
> subject's own; `moderate` = clearly implied by worked examples; `weak` =
> plausible but thin. Quote evidence tightly (≤ 25 words). Note attribution
> hazards: parent-company sections, customer testimonials, case studies about a
> client's product, reseller/distributor language.

## Rules carried from the census (do not relearn these)

- Pair records with the **chunk bounds** in the key; group_id alone silently drops
  twins (this once hid 7 real defects).
- `products` and `contract_products` share every phrase stage through synthesis —
  never sum their dumps as independent evidence of upstream stages.
- The descent stage's `requests` list contains non-dispatched entries
  (`note: usage_unavailable`); count only entries with usage as calls.
- A missing dump is not always a crash — compare the dump file count against
  subjects × fields before reading anything else.
- Dump-computed counters are not comparable across runs when the counting code
  changed between them; recompute uniformly from the instrument's own code.
- Illustrating a finding with verbatim examples is a measurement step, not a
  presentation step — pull the quotes before believing the number.
