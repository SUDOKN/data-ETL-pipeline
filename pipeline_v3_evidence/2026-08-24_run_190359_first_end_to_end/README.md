# Run 20260824T190359 — the first end-to-end v3 run (Phase 3 review gate)

Subjects: `steelcraft.com`, `alecmfg.com`. Both **FAILED**, at the same stage,
for the same reason. Everything before that point ran.

Reproduce: `python3 pipeline_v3_evidence/2026-08-24_run_190359_first_end_to_end/analyze_run.py`
(output committed as `analyze_run_output.txt`). Log read alongside:
`apps/data_etl_app/src/data_etl_app/scripts/latest_extraction_error.txt` (2,360 lines).

## What actually ran

Only the tail was live. `created_at` on the stored requests:

| stage | created | status this run |
|---|---|---|
| `llm_phrase_search` | 2026-08-23T04:45 | replayed |
| `llm_phrase_mention_collection` | 2026-08-24T02:07 | replayed |
| `llm_phrase_synthesis` | 2026-08-24T02:07 | replayed |
| `llm_phrase_freehand_grounding` / `initial` / `oov` | 2026-08-24T19:03 | **LIVE** |
| `llm_phrase_relationship_screening` | 2026-08-24T19:03 | **LIVE** |
| `llm_phrase_recursive_tagging` | 2026-08-24T19:03 | **LIVE** |

Correct behaviour (the `|ud=` digest chain replays an unchanged payload), but it
means no upstream number in these dumps is a new measurement.

## Fields completed before the crash

- steelcraft: addresses, both binaries, products, contract_products, equipments,
  conformity_attestations, industries → **crashed on `process_caps`**
- alecmfg: addresses, both binaries, products, contract_products, equipments →
  **crashed on `conformity_attestations`**

## The crash

`ConceptInitialGroundingNode` (in-vocab grounding). The model returned options
that are not vocabulary labels; `parse_record_grounding_result` turns each into a
violation and `raise_for_violations` aborts the whole subject.

- steelcraft `process_caps`: record `g21dwglm` chose `Testing`
- alecmfg `conformity_attestations`: 6 violations across 4 records — `RoHS` ×3, `REACH` ×3

One record out of hundreds kills every remaining field for that manufacturer.
`record_response_parse_error_capped` nulls `response`/`batch_id` first, so the
request is armed to be re-asked — **this is the genuine mid-run failure that gives
the 29d2167 recursive-resume fix its first live verification.**

Root cause is a threadbare vocabulary, not a rogue model. On steelcraft
`conformity_attestations` the in-vocab pass produced exactly ONE label
(`UL Product Certification`, 6 records); the other 32 distinct labels all landed in
the out-of-vocabulary pass. Faced with `RoHS`/`REACH` and a near-empty menu, the
model reached instead of declining.

## Findings, ranked

### 1. Freehand grounding is 78% / 90% reproducible on identical input (NEW)

`products` and `contract_products` share every prompt version except screening,
and their freehand-grounding requests carry **identical `|ud=` payload digests**
(23/23 steelcraft, 13/13 alecmfg). Same prompt, same payload, same model,
`temperature=0`, `seed=12345` — two calls. A free A/A test:

| subject | groups compared | identical candidate sets | decline-vs-tag flips |
|---|---|---|---|
| steelcraft | 547 | 425 (**77.7%**) | 41 |
| alecmfg | 353 | 318 (**90.1%**) | 23 |

A record yields a tag in one call and nothing in the other ~7% of the time.
This bounds every downstream number; no A/B smaller than ~10-22% is readable.

### 2. Grounded tags are never normalized — 206 near-synonyms for "doors and frames"

steelcraft `products`: 559 groups → 279 grounded → **206 distinct final tags**,
99% of which contain "door" or "frame". `DW Series Drywall frames` and
`DW Series drywall frames` both survive — a case-only duplicate.

The aggregation fold normalizes *search forms* into groups; nothing normalizes the
*grounded tags* coming out. `FGR-Q2` is explicitly scoped to one record ("distinct
from every other product category **named for the same record**"), so cross-record
duplication is unchecked by design.

`FGR-Q1` (must be a category, not an individual item) is also self-reported
"satisfied" on model/series names with no reasoning:

    tag 'SZ Series Falcon flush doors'   (focal_form: 'Full height, visible edge seams')
      FGR-Q1 satisfied: "'SZ Series Falcon flush doors' is a determinate product category."

### 3. The equipment search returns products, not equipment

steelcraft `equipments`: 75 groups → **1 grounded** (`engraining and staining machine`).
**68 of the 75 groups are the same group_ids the products search found** (group_id
is a hash of the normalized key, so this is exact, not a guess) — despite a
separate search prompt (`EwEy…` vs `i3Dy…`).

Grounding declined all 74 correctly ("Doors are finished goods, not production
machines"), so precision was saved downstream — but the site paid for mention
collection, synthesis, grounding and screening on 74 known-bad entities.

alecmfg `equipments` is healthy by contrast: 34 groups → 30 grounded.

### 4. SCR-2 on the contract screen accepts circular evidence

SCR-2 asks whether the work is customer-directed. On steelcraft (a catalogue door
manufacturer) 190 tag-verdicts satisfied it, of which:

- **46 (24%) are circular**: "The doors are manufactured by Steelcraft, indicating customer-directed work."
- ~37% lean on a standard or rating: "meeting broad fire rating requirements … indicates these are made to customer or project requirements"

alecmfg (a real contract shop) shows 3% circular — the failure is subject-dependent.
The same rule fails *correctly* elsewhere in the same run:

    [tornado doors] SCR-2 failed: "…does not specify that they are made to customer
    specification, order, or brand, nor that they are contract or customer-directed work."

Net: 153 steelcraft groups pass BOTH the pure-product and contract screens.
The screens are 59% concordant on steelcraft, 88% on alecmfg.

### 5. Evidence thickness barely moved

2,104 groups, mean **2.12** mentions per group, median 1, **68.1% single-mention**.
v2's relationship stage measured ~1.7. The split bought ~25%; the collapse the
redesign targeted is only partly relieved. Confirms the parked Phase 5 question.

### 6. What worked

- **Screening works and is the thing that separates products from contract_products.**
  Verdicts carry per-rule explanations that quote the synthesis.
- **The manufacturer-name rider landed**: 96–100% of syntheses name the subject, now
  sanctioned. `record_own_name_hits` no longer discriminates anything — its
  threshold needs redefining or the lint retiring.
- **The re-key is sound.** Group ids are global (a group appearing in both chunks
  carries one id); no `not_synthesized` rows anywhere; `no_mentions` rows are
  dump-visible with `forms`/`key` populated (`focal_form` is null because it is
  elected during synthesis, which never ran for an empty bundle).
- **No retry pass fired.** Zero `>retry>1>` custom ids in any dump — every group
  request was answered in full first time. The retry code is still unverified live.
- **One warning in 2,360 log lines** ("dropping explanation volunteered beside 1 unit").

## Cost

$11.93 at gpt-4.1 list rates for both subjects, 2.30M in / 0.92M out. The LIVE
portion (grounding + screening + descent) is ~$5.65; the rest was billed on
earlier runs. Top stages: freehand grounding $2.94, synthesis $2.59,
mention collection $2.47, screening $2.11.

## What was done about it (2026-08-25)

Three fixes, built and green the day after this run. Suites 1,281 passed / 8
deselected; pyright and ruff deltas both 0.

- **A1 — the parse-error retry budget is now actually spent.**
  `record_response_parse_error_capped` promises CAP re-dispatches (CAP+1
  attempts), but the failure that arms them is raised inside
  `embed_request_ids` at the TOP of the recursive convergence loop, so the
  exception escaped before the dispatch at the BOTTOM could spend one. This run
  surfaced on attempt 1 of 4. The error is now held for the rest of the pass and
  re-raised only if the pass armed nothing; `RepeatedParseFailure` is never held.
  Completes `29d2167`, which made the re-dispatch reachable but not reached.
- **B1 — a non-vocabulary option is dropped, not raised.** It lands on the new
  `RecordGroundingEntry.dropped_options` and shows in the dump beside the
  verdict. The OOV pass runs on every record and sees the label absent from
  `already_identified`, so a real vocabulary gap is still recorded where it
  belongs — `RoHS` and `REACH` were never the in-vocab pass's to record.
- **The stale `mentions` promise fixed** in 21 catalogs and 5 skeletons —
  `evidence_source` is now "the record's focal form and synthesis". All 21 tail
  prompts re-rendered and published 2026-08-25T19:43:45Z.

The two findings this run raised that are NOT yet acted on are the tag
normalization gap (finding 2), the equipment search precision failure
(finding 3) and SCR-2's circular evidence (finding 4).

## Next

1. Re-run (the 21 prompts were published 2026-08-25T19:43:45Z). Note this will NOT verify A1 live — a
   `pv` change re-keys every tail request and orphans the stale rows, the same
   trap that closed the window last time. Live evidence needs two runs at the
   same prompt state.
2. The in-vocab pass for `conformity_attestations` earns 1 label in 130 groups.
   Either the vocabulary grows or the pass is skipped for that field.
3. The tail record shape is still the v2 map; synthesis already sends an array.
   Decided, unbuilt.
