# Pipeline v2 flip — review digest (Phase 2 gate)

**Branch `new-ground-truth-v2` · 2026-08-21 · working tree uncommitted on `0ec8c8c`.**
This is the review-gate packet for the Phase 2 flip (slice-C cascade). It is a
point-in-time review aid; [PIPELINE_V2_PLAN.md](PIPELINE_V2_PLAN.md) stays the source of
truth for state and history.

Status: **suite 984 passed** · **pyright net −5 vs HEAD baseline** (after fixes below) ·
**prompt pins check clean**.

---

## The inversion, as landed

v1 screened one identified entity per phrase and then let grounding fan out to tags that
never faced relationship scrutiny. v2 grounds first — candidate enumeration under
candidate-only rules — then consolidated screening vets **every** candidate's
relationship, and recursive descent runs on passed in-vocab survivors without
re-screening. Chain order below is read directly from
`apps/data_etl_app/src/data_etl_app/services/extraction_pipeline_factory.py`, not from
the plan.

```
v1 concept:  Prefill → Search → Recursive search → Relationship
             → Screening → In-vocab grounding → Descent → Reconcile

v2 concept:  Prefill → Search → Recursive search → Relationship
             → In-vocab grounding → OOV grounding (NEW) → Screening
             → Descent → Reconcile

v2 keyword:  Prefill → Search → Recursive search → Relationship
             → Freehand grounding → Screening → Reconcile
```

The OOV node runs serially after in-vocab and has an OFF pass (metadata node `None` →
zero requests embedded, empty map published) — toggled per run via
`create_pipelines(oov_grounding_enabled=…)`.

---

## Three deviations that need your sign-off

Everything else follows the approved plan. These three depart from the sketch or keep
something the plan retired, each deliberately; the flip review is where they get
ratified or reversed.

### Deviation 1 · storage shape

Stored grounding entries are `RecordGroundingEntry {tags, explanation}` with a
correlation validator (tags-empty ⇔ explanation-present), not the sketch's bare
`{record_id: {tag: rules}}` map. The bare map would have lost declination explanations
at storage — reopening the no-recorded-reason hole that `no_candidate_explanation`
closed in v1. Lives in `packages/core/src/core/models/extraction_schemas/grounding.py`.

### Deviation 2 · sentinel label guard kept

The sentinel **contract** is dead (wire arms, catalog field, assembly branch,
stop_reason all deleted), but `is_sentinel_grounding_label` and the two label constants
survive as a guard in `get_deepest_concepts_and_oov`. Rationale: the descent trail is
the one result path screening never vets, and a v1-habit "none of the above" echo must
not persist as a discovered OOV label — the bug that bit twice. Narrow by design;
delete it only if you decide the risk is gone.

### Deviation 3 · V2-suffixed names stay

Models and stats keep their `*V2` suffixes (e.g. `llm_phrase_extraction_results_v2.py`,
`Manufacturer` fields typed `*ResultsV2`). Folding them into unsuffixed canonical names
is deferred as a cosmetic slice so the flip diff stays reviewable. Debt is recorded in
the plan's STATE.

---

## Contract-by-contract walkthrough

### Record identity & masking — F1, F2 *(verified in code)*

`packages/core/src/core/utils/record_id_util.py`: `record_id_for_phrase` derives `r` +
7 base36 chars of sha256(phrase), purely — no stored minting step.
`RecordIdCollisionError` names both phrases on collision. `assign_record_ids` /
`mask_relationship_records` / `phrases_by_record_id` are the only entry points. Masking
is key-only: mention `form`s still carry the phrase wording; only the output grouping
key is replaced.

**Check:** is the 7-char id length acceptable as the permanent format? The collision
error's remedy is "raise the length", which changes every downstream id.

### Relationship stage — F9, F13, F14 *(carried from journal)*

`packages/core/src/core/services/pipeline_nodes/multi_stage/llm_phrase_relationship_node_service.py`
parses `records: [{phrase, mentions: [{form, page, account}], synthesis}]` to the masked
record map. Phrase-axis hold is unchanged from v1 — identity is set here; masking is
applied **after** the hold and merge. `records_with_mentions` filters the empty-mentions
records (the honest not-found branch) out of every downstream stage while stats keep
them.

### Grounding — one shared service — F3, F4, F7 *(carried from journal)*

`packages/core/src/core/services/pipeline_nodes/multi_stage/llm_grounding_node_service.py`
serves in-vocab, OOV, and freehand; the stages differ only in what rides beside the
records and whether labels are vocabulary-held. In-vocab: hard membership check at
parse — a non-vocab label fails the response (casing drift is repaired to the
vocabulary's own spelling with a warning). Empty candidates are an explicit declination
and must carry an explanation; silent empties fail. Violations are collected
whole-response, then raised once. `build_record_payloads` holds the mask — the phrase
never rides to grounding.

### OOV pass — F5, F6 *(structure verified)*

`packages/core/src/core/models/pipeline_nodes/multi_stage/base/llm_phrase_oov_grounding_node.py`
(`ConceptOovGroundingNode` per field) runs serially after in-vocab with the full vocab
outline plus per-record `already_identified` (sorted) in context. OOV mints that restate
a vocab label re-route in-vocab during reconcile — never raise. The toggle is run-config
+ optional metadata node, deliberately not a StageToggle.

### Screening — F8 *(carried from journal)*

`packages/core/src/core/services/pipeline_nodes/multi_stage/llm_relationship_screening_node_service.py`:
per-candidate verdicts with `passed` derived by `passed_implied_by`, never reported by
the model. Held on both axes: the record-id axis via the shared hold, the candidate axis
via `hold_candidates_to_sent_records` against the request's own records payload — an
unjudged candidate raises, a fabricated verdict raises, and candidate echo is
exact-match (repair would be silent misattribution). Deposition-only: records and
candidates, never chunk text. Candidates come from `candidates_for_screening` (in-vocab
∪ OOV, casefold-deduped preferring the in-vocab spelling) in
`pipeline_v2_derivations.py`.

### Descent & reconcile — F10 *(carried from journal)*

Descent seeds from `descent_seed_tagging_results` (passed in-vocab tags, tag-major, the
shape v1's descent machinery already consumes) and stops on empty options — the sentinel
stop_reason is retired, `false_child` kept. Minted sibling proposals are still allowed
(RGR-M3), so descent parses hold ids exactly but not labels. Reconcile assembles
`ConceptsFound` per F10: concept `in_vocab` = survived screening then deepened
post-descent; `out_of_vocab` = passed OOV candidates; keywords = passed freehand
candidates with `in_vocab` empty by construction and `dedupe_equivalent_keywords`
applied at the union.

### Replay integrity — F12 *(carried from journal)*

Every downstream `get_request_custom_id` (relationship, in-vocab, OOV, screening,
freehand, descent) appends `|ud=<12-hex digest>` of the group's own upstream-derived
payload (`packages/core/src/core/utils/request_custom_id_util.py`). Descent ids derive
theirs through the same evidence walk request creation uses, so id and payload cannot
disagree. This closes the stale-replay hazard where A/Bs on an upstream prompt read
fabricated downstream numbers.

### Dumps *(carried from journal)*

One row per record with statuses `no_mentions` / `no_candidates` / `screened_out` /
`grounded` / `screening_dropped`; the OOV key is omitted when the pass never ran, so
never-asked and asked-found-nothing stay distinguishable.

---

## Verification — what the 2026-08-21 session measured

**Found & fixed:** the pyright aggregate ("net negative") hid three real regressions:
the keyword relationship nodes (`ContractProduct`/`Equipment`/`PureProduct`) still
annotated `next_node` as their v1 successor (`*RelationshipScreeningNode`) while the
factory now wires the v2 one (`*FreehandGroundingNode`). All three re-annotated at the
source; the factory and all three nodes are now pyright-clean, and the fix also cleared
`contract_product_relationship_node.py`'s standing error. Suite re-run: 984 passed.

The delta was measured against a clean worktree at HEAD (`0ec8c8c`) with an
`extraPaths` config so baseline sources resolve to themselves rather than the
editable-install paths — 177 errors baseline, 172 in the working tree over the same
touched-file set, before the fix. Remaining new errors are fresh instances of the
codebase's established covariant-override style on new or edited node files; fixing
them means base-class signature redesign, not annotation repair, so they were left
deliberately:

| File | New errors | Kind |
|---|---|---|
| `llm_phrase_oov_grounding_node.py` | 7 | covariant overrides of the `create_batch_requests` family — identical in kind to the 6–7 every sibling node already carries |
| `llm_phrase_freehand_grounding_node.py` | +1 | same style, one added override |
| `llm_phrase_initial_grounding_node.py` | +1 | same style, one added override |
| `concept_relationship_screening_node.py` | +1 | `get_chunk_candidates_by_record` narrows its base signature |
| `keyword_relationship_screening_node.py` | +1 | same method, keyword side |

**Prompt state:** `assemble_prompts.py check` passes clean — every prompt byte-matches
its catalog render and its published S3 version. The plan's standing blocker (4 OOV
prompts + merged `product_phrase_freehand_grounding` unpublished) was stale: all five
carry S3 version ids in their catalogs' `published` blocks. Nothing stands between the
flip review and a measurement run.

---

## Fork ledger — where each decision landed

| Fork | Decision | Landed in |
|---|---|---|
| F1 | Content-derived record_id, collision raises | `record_id_util.py` |
| F2 | Key-masking only | `mask_relationship_records` + grounding payload builder |
| F3 | Sentinel retired; empty candidates = declination + explanation | wire builders + grounding parse; guard remnant is Deviation 2 |
| F4 | In-vocab hard membership at parse | `llm_grounding_node_service.py` |
| F5 | OOV context = full vocab + pass-1 results; vocab matches re-route | OOV node + reconcile |
| F6 | OOV toggle = run config, never StageToggle | `create_pipelines(oov_grounding_enabled=…)`, optional metadata node |
| F7 | Attribution clauses out, match strictness kept | v2 catalogs (promoted slice B) |
| F8 | Screening deposition-only, both axes held | `llm_relationship_screening_node_service.py` |
| F9 | Empty-mentions records skip grounding + screening | `records_with_mentions` |
| F10 | Results semantics; keywords adopt ConceptsFound | reconcile nodes + `pipeline_v2_derivations.py` |
| F11 | GT pivots to v2 shapes after Phase 4 | v1 instrument retired (slice A); primitives kept |
| F12 | Upstream-content digest in custom_id | `request_custom_id_util.py`, all downstream stages |
| F13 | Placeholders stripped; context-append kept | relationship statics (0.1) |
| F14 | Mention text field = `account` | wire + stored relationship shapes |
| F15 | No downstream-stage info in prompts | all v2 skeletons + catalogs |
| F16 | Rule ids renumber from 1, no v1 continuity | v2 catalogs; joins via catalog_version |
| F17 | RGR granularity self-report removed | recursive catalogs; GT v2 drops the requirement |

---

## What was deleted

- v1 node services (initial/freehand grounding, v1 relationship and screening parse
  paths); the `_v2` siblings collapsed into canonical file names.
- v1 wire builders, `SentinelWireEntry`, `NO_CANDIDATE`/`JUDGED`, and the `_v2` wire
  dispatchers — canonical `response_model_for` now dispatches the record-keyed builders.
- v1 stats/metadata/results classes, v1 relationship wire shapes,
  `partition_by_search_round`, `merge_stage_repairs`.
- `RuleCatalog.sentinel_tag` + validator + the assembly sentinel branch.
- The v1 GT instrument (slice A): 25 files, 224 tests — `audits.py`, `rule_tree.py` and
  their tests kept as Phase-5 primitives; binary GT untouched.
- 18 v1 phrase catalogs, including the two split product-freehand catalogs (now one
  merged catalog serving both product fields).

---

## Open items & the path from here

- **This review.** Ratify the three deviations, or name what to change.
- **Phase 3 (observability):** dump/trail rows joined via the stored triple;
  subject-name lint over `form`/`account`/synthesis (a `subject_name_lint.py` util
  already exists to build on).
- **Phase 4 (measurement gate):** alecmfg + steelcraft via your notebook against the
  material-caps baselines (56% boundary precision, 45% context coverage); relationship
  `max_phrases_per_request` 20–30; OOV on vs off. GT rebuild waits on this.
- **Standing watch items:** M2 match rules; brute word-boundary TODO;
  descent-attribution leak channel (accepted, measure in Phase 4);
  `OUTCOME_GLOSS_BY_KIND` not_triggered gloss (closed at cutover per slice B — confirm
  during review).
