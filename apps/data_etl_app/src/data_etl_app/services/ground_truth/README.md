# Ground truth rebuild — master plan & journal

**Branch `new-ground-truth` · this file is the single source of truth for progress.**
The design docs in this folder are the specs; this README is the plan, the fork
ledger, and the append-only journal. A fresh session resumes by reading: (1) the STATE
block, (2) the fork ledger, (3) the step it says is next.

| Spec | Instrument | Fields |
|---|---|---|
| [LLM_PHRASE_GT_MODEL_DESIGN.txt](LLM_PHRASE_GT_MODEL_DESIGN.txt) | **ACTIVE — phrase-stage audit trail (run-scoped, user-authored)** | all 7 phrase fields: `products`, `contract_products`, `equipments`, `industries`, `conformity_attestations`, `material_caps`, `process_caps` |
| [BINARY_GT_ANNOTATION_DESIGN.md](BINARY_GT_ANNOTATION_DESIGN.md) | Binary classification — **PARKED 2026-08-14** | `is_manufacturer`, `is_product_manufacturer`, `is_contract_manufacturer` |
| [KEYWORD_GROUND_TRUTH_DESIGN.md](KEYWORD_GROUND_TRUTH_DESIGN.md) | Keyword entity inventory — Layer B superseded by the phrase instrument; Layer A (durable inventory) deferred | `products`, `contract_products`, `equipments` |
| [CONCEPT_GROUND_TRUTH_DESIGN.md](CONCEPT_GROUND_TRUTH_DESIGN.md) | Concept assertions — audit half superseded by the phrase instrument | `industries`, `conformity_attestations`, `material_caps`, `process_caps` |

---

## STATE

- **Phase:** P — LLM phrase-GT instrument (models → services; routes excluded from this plan)
- **Step in flight:** P1.1 — audit primitives (core)
- **Last completed:** P0′ plan pivot + all design forks resolved (2026-08-14)
- **Next action:** build P1.1, pause for the user's audit. The user audits every substep
  against their design and coding style — substeps stay one-file-plus-test sized.
- **Blockers:** none for P1. X2 (legacy disposition) still gates any retirement of the
  old GT collections/services.
- **Kernel:** dissolved as a separate phase — see "2026-08-14 pivot" below for what was
  absorbed and what is parked.

*Update this block at every substep completion. Journal rows are append-only, at the
bottom of this file.*

---

## How this plan runs

1. **Every substep is a pause point.** Substeps are sized to be reviewable in one
   sitting — typically one file plus its test. Stop after any of them.
2. **Done means:** code written, `pytest` green, STATE block updated, journal row
   appended. Nothing is "done pending tests."
3. **Every step ends in a REVIEW gate** with a specific question for the user. The
   next step does not start until that question is answered.
4. **Resumption protocol:** read STATE → read the step's substeps → check the journal
   for the last row → continue. No other context required.
5. **Tests run with the repo's existing conventions:** `pytest` from the repo root;
   anything needing live AWS/network is marked `integration` (deselected by default,
   per [pytest.ini](pytest.ini)).

## The architecture this plan commits to

**2026-08-14 pivot.** The user authored
[LLM_PHRASE_GT_MODEL_DESIGN.txt](LLM_PHRASE_GT_MODEL_DESIGN.txt) — a self-contained,
run-scoped audit document over every phrase stage (relationship, screening, oov
grounding, in-vocab grounding) — and it jumped the queue ahead of the binary branch.
The 2026-08-13 kernel plan (K1–K8) dissolved into it:

- **Absorbed into P-steps:** snapshot witness (K3 → identifiers), provenance capture +
  completeness tripwire (K4 → explicit run identity, P1.4), authorship fields
  (K5 → inline `author_email`/`at`/`source` on audit entries — no separate
  `Authorship` model).
- **Parked, deliberately:** evidence spans, span validation, span math (K1/K2/K6) —
  the design anchors nothing by offset; missed phrases are validated by
  substring-containment in chunk text, the old service's convention. `Split` (X4) is
  not in the v1 document. Roles (K5.1/K5.3, X6) wait for the routes phase. Legacy
  archive tooling (K7) waits for X2.
- **Parked branches:** binary (BIN-4/BIN-5 open, unscheduled). The keyword/concept
  *audit* halves are superseded by this instrument; the keyword Layer A durable
  inventory is deferred with eyes open (see I1 verdict — no cross-run accumulation in
  v1, the annotator re-judges new runs).

**What survives from the 2026-08-13 simplicity review:** sharing only where semantics
fit one sentence; field-type-agnostic core models (`packages/core` cannot import
`data_etl_app` — verified, and the field enums live app-side); no document base
classes. Placement follows X5: reusable embedded models in `packages/core`, the
Beanie Document and field-enum binding in `apps/data_etl_app`.

### The design's settled semantics (do not re-litigate; ask before deviating)

Decisions from the 2026-08-13/14 review sessions, recorded with their reasons:

1. **Self-contained document.** The doc embeds a full copy of the run's stage outputs
   plus the whole extraction-metadata block. Reason: `Manufacturer` keeps one result
   slot per field, overwritten each run — this document is the only surviving record
   of the run it audits. Self-containment includes pinning the text: witness
   (`scraped_text_file_version_id`, sha256, char_len) lives in identifiers.
2. **Run-config-keyed identity (I1 → resolved).** Any version change = a different
   ground truth. Identity is **explicit version fields** in the unique index — the
   user chose explicit fields over a fingerprint *"because I want to know what was
   the change, not just if there was a change."* A field-set tripwire test (K4.2
   pattern) fails when the metadata models gain a field, forcing a conscious decision
   about whether it joins the identity.
3. **No cross-run accumulation in v1 — accepted cost.** A new run means the annotator
   re-judges from scratch; it is on the annotator to check what carried over. The
   fold is per-document. Revisit only if annotation hours demand it.
4. **Catalog-driven template inflation.** The annotation template inflates from the
   pinned catalog and is hydrated with the LLM's report — never built from the report
   alone. Unreported guards materialize as `reported: false` with the implied
   outcome, so silence is auditable. Future reportable children flow through
   automatically (the wire schema is generated from the same catalog).
5. **Nested storage, whole-document writes.** The tree shape mirrors the catalog and
   is written whole on submission — no incremental deep updates. Flattening for
   analytics is compute (model methods), not storage.
6. **Stored `passed` is a cache, never editable.** A validator asserts stored
   `passed == fold(rules through catalog combinators)`. Flipping a condition to
   failed forces fail; the reverse does not force pass — only the fold decides.
7. **Audit entries:** `agree | agree-but | disagree`. `agree-but` = correct but
   incomplete (recall flag) and **requires `corrected_text`** (the addendum);
   `disagree` = incorrect and requires the correction. Entries carry
   `author_email`, `at` (timestamp), `source`; array order is submission order and
   both are kept.
8. **Same-author resubmission pops their previous entry** (stack-top, the old
   service's convention). Recorded cost: the pre-judge flip history is lost; the
   one-line fix (a `superseded` list) exists if flip-rate is ever wanted.
9. **Judge responses are immutable siblings.** A more-capable-LLM judge's response is
   generated once, stored next to the human audit slot, never regenerated. Judge
   entries carry their own provenance (model, judge prompt version); human
   acceptance is recorded as `proposed_by: judge` + `endorsed_by: human` — never
   copied into a human-authored entry, so endorsement stays distinguishable from
   independent agreement.
10. **Fold: latest wins** per node across authors; untouched nodes (empty audits) are
    *unreviewed*, not agreement — the fold only consumes reviewed nodes.
11. **`missed_phrases` lives in the document** beside `extracted_phrases` — the
    search-recall surface. Human-asserted phrases carry human derivations only
    (no `llm_result`), validated by substring presence in the chunk text.
12. **Document granularity mirrors storage exactly** (the user's "it's exactly
    what's stored"): one document per (subject, field_type, run-config);
    inside, a chunk-key map (`"start:end"`, same keys as
    `chunked_extraction_stats`) → per-chunk `extracted_phrases` (phrase-major, with
    `search_round`) and `missed_phrases`.
13. **Build order is the user's:** models first, then services/utilities, routes
    excluded from this plan. Tests ride along every substep.

---

# Phase P — the phrase-GT instrument

Core files under `packages/core/src/core/models/ground_truth/` and
`packages/core/src/core/services/ground_truth/`; app files under
`apps/data_etl_app/src/data_etl_app/db_models/` and `services/ground_truth/`.
(`core/models/ground_truth/` exists and is empty — a leftover `__pycache__` only.)

---

### P1 · Models — the document, bottom-up

| Sub | Do | Done when |
|---|---|---|
| P1.1 | **Core** `models/ground_truth/audits.py`: `AuditVerdict` (`agree`/`agree_but`/`disagree`), `TextFieldAudit {type, corrected_text, author_email, at, source}` with the requiredness validator (`corrected_text` required for `agree_but` and `disagree`), plus the judge sibling model `JudgeAudit` (content + judge model + judge prompt version + `at`; written once) and the `proposed_by`/`endorsed_by` fields that keep endorsement distinguishable. | Models + validators import cleanly; tests for every requiredness branch |
| P1.2 | **Core** `models/ground_truth/rule_tree.py`: recursive `AuditedRule {rule_id, kind, outcome, explanation, reported, audits, judge_audit?, sub_rules}` and `AuditedSection {section_id, combinator, applied_rules}`. `reported=False` marks catalog-synthesized nodes (unreported guards, notes). Combinator values come from the catalog's own vocabulary — this file declares no combinator enum. | Recursion round-trips through pydantic serialization; tests |
| P1.3 | **Core** `models/ground_truth/stage_blocks.py`: `RelationshipGT`, `ScreeningLLMCopy {passed, identified_entity, no_candidate_explanation, sections}`, `HumanScreeningDerivation {identified_entity, audits, sections}`, `ScreeningGT`, `OovGroundingGT`, `InVocabGroundingGT` (level map mirroring `IterativeGroundingResult`: `{lvl → [{parent_group_id, group_id, stop_reason?, applied_rules, audits}]}`), `ExtractedPhraseGT {search_round, stages…}`, `MissedPhraseEntry` (human derivations only). Structural `passed` validator (catalog-free part). | Models exist; tests incl. a concept-shaped and a keyword-shaped instance |
| P1.4 | **Core** `models/ground_truth/run_identity.py`: `ExplicitRunIdentity` — the enumerated per-node version fields (`prompt_version_id`, `catalog_version`, `llm_model`, model-params hash, node caps) for every node of `LLMPhraseExtractionMetadata` + `KeywordExtractionMetadata` + `ConceptExtractionMetadata`, plus base `chunk_strat` and `ontology_version_id`; `from_metadata()` constructor. **Tripwire test**: a recorded snapshot of the metadata models' field sets; the test fails when a field appears that identity has not consciously included or excluded (follow `tests/test_services/test_prompt_provenance.py` conventions). | Model + constructor + tripwire green |
| P1.5 | **App** `db_models/llm_phrase_ground_truth.py`: `LLMPhraseGroundTruth(Document)` — subject, field_type (app enum), `scraped_text_file_version_id`, witness (sha256, char_len), `run_identity`, the full embedded metadata copy, `chunks: {chunk_key → {extracted_phrases, missed_phrases}}`, `created_at`; unique index over (subject, field_type, text version, run_identity dotted fields). `db_seed_indices` re-point; wire-schema regen stays the user's workflow. | Document + index defined; round-trip test; no existing test breaks |

**PAUSE — REVIEW P1.** *Question for the user:* does the `ExplicitRunIdentity` field
list match your bar for "worth storing in identifiers" — specifically, are the
node batching caps (`max_phrases_per_request`, `max_pairs_per_request`, recursive
round caps) in or out of the unique index?

---

### P2 · Services and utilities

| Sub | Do | Done when |
|---|---|---|
| P2.1 | **Core** `services/ground_truth/catalog_template_inflation.py`: catalog → inflated section/rule skeleton; hydrate from stored stats (chunk → round → phrase); synthesize unreported guards (`reported=False`, implied outcome); include note children as context nodes. Share the traversal with `catalog_wire_schema` where it does not contort either. | Inflation of a real catalog + real stats round-trips; unreported-guard test |
| P2.2 | **App** `services/ground_truth/llm_phrase_gt_template_service.py`: assemble a full GT document template from a `Manufacturer`'s stored results — copies, witness computation, `ExplicitRunIdentity.from_metadata`, empty audit slots. | Template builds for one keyword and one concept field from a live-shaped fixture |
| P2.3 | **Core** `services/ground_truth/audit_submission_validation.py`: the contract — requiredness rules; human derivations checked against the pinned catalog (outcomes in `outcome_vocab` for the rule's kind, `all`-sections complete on fresh derivations, rule ids exist at that `catalog_version`); entity-change ⇒ fresh sections; missed-phrase substring check; same-author pop; append ordering. | The test file reads as the contract; every rejection branch covered |
| P2.4 | **Core** `services/ground_truth/gt_fold.py`: latest-wins effective view; effective `passed` via the combinator fold; per-document computed-truth summary and per-rule agreement rollups (the drift diagnosis this instrument exists for). | Fold tests: both toggle directions, multi-author latest-wins, unreviewed-node exclusion |
| P2.5 | **App, deferrable** `services/ground_truth/judge_service.py`: generate the judge response once per slot, immutable write, full judge provenance. Build only when the user says the judge experiment starts. | Marked deferred unless activated |

**PAUSE — REVIEW P2.** *Question:* the fold's per-rule agreement rollups — which
slices do you want first (per rule id across subjects, per field, per catalog
version), so the rollup shape serves the first real analysis instead of a guess?

---

### P3 · Routes — explicitly OUT of this plan

Recorded so it is not forgotten: template GET, submission POST, computed-truth GET;
blinding/role gating (X6) re-enters here. Planned in its own session when P1–P2 are
audited and stable.

**Untouched until X2 resolves:** the old binary/keyword/concept GT services, routes,
and collections — including the stale legacy-convention
[concept_ground_truth_new.py](../../db_models/concept_ground_truth_new.py) — keep
running; callers unaffected.

---

## Fork ledger

**RESOLVED** = verdict recorded · **OPEN** = decide at the step named · **PARKED** =
deliberately unscheduled.

| Id | Question | Scope | Status | Verdict |
|---|---|---|---|---|
| X1 | Multi-annotator machinery | all | **RESOLVED** 08-12 | Support N; entries append in submission order; effective view = latest wins (see PH-4) |
| X3a | Binary annotation surface | binary | **RESOLVED** 08-12 | Full scraped text (unchanged; binary parked) |
| X5 | Placement | all | **RESOLVED** 08-13 | Reusable models in `packages/core`; Beanie Documents app-side — applied to P1 |
| X6 | Annotator/adjudicator roles | all | **RESOLVED** 08-13, **parked to P3** | Extend `UserRole`; only matters at the route plane |
| X7 | How much to share | all | **RESOLVED** 08-13 | One-sentence-explainable shared pieces only; no base Documents |
| I1 | Provenance in the identity key? | phrase fields | **RESOLVED** 08-14 | **Yes — run-config-keyed audit documents** (user's verdict): explicit version fields in the unique index; truth computed per-doc by the fold; cross-run accumulation deliberately deferred (annotator re-judges; accepted cost) |
| PH-1 | Template built from report vs catalog | phrase | **RESOLVED** 08-14 | Catalog-driven inflation, hydrated with the report; unreported guards materialize `reported=false` |
| PH-2 | Judge response handling | phrase | **RESOLVED** 08-14 | Immutable sibling, generated once; judge provenance + `proposed_by`/`endorsed_by`; never adopted as human-authored |
| PH-3 | `agree-but` payload | phrase | **RESOLVED** 08-14 | Requires `corrected_text` (the addendum) |
| PH-4 | Fold semantics | phrase | **RESOLVED** 08-14 | Latest wins per node; empty audits = unreviewed, excluded |
| PH-5 | Search-recall surface | phrase | **RESOLVED** 08-14 | `missed_phrases` in-document, per chunk, substring-validated |
| PH-6 | Identity mechanism | phrase | **RESOLVED** 08-14 | Explicit version fields (user: "I want to know what was the change") + field-set tripwire |
| PH-7 | Same-author resubmission | phrase | **RESOLVED** 08-14 | Pop retained (old-service convention); flip-history loss recorded as accepted; `superseded` list is the one-line fix if ever wanted |
| PH-8 | Cross-run reuse of judgments | phrase | **RESOLVED** 08-14 | None in v1 — re-judge from scratch; on the annotator to check carry-over |
| X2 | Legacy collections: migrate vs archive+wipe | all | **OPEN** | Rec: archive-dump then wipe; needs the K7-style counts before anything retires |
| X4 | dev/test split | all | **OPEN** | Not in the v1 phrase document; decide at consumer/scoring time |
| BIN-4 / BIN-5 | Binary guideline wording / bundling | binary | **PARKED** | Branch parked 08-14 |
| KW-F1 | Blind-highlight pass | keyword | **PARKED** | Prefill-from-LLM is intentional (template UX); anchor bias accepted; calibration idea kept on record |
| KW-F3 | Products family only vs both | keyword | **SUPERSEDED** | The phrase instrument covers all phrase fields structurally |
| CON-2 | Explicit negatives with rule reasons | concept | **ABSORBED** | Rule-level audits are exactly this |
| CON-5 / CON-6 | Uncertain scoring / keyword fields on assertions | concept | **PARKED** | Scoring-time decisions |

---

## Journal (append-only, newest last)

| Date | Entry |
|---|---|
| 2026-08-12 | **P0 — chartered.** README repurposed from a stale duplicate of the binary design into master plan + journal. Locked: API-first (UI deferred, CLI harness stand-in), binary-first order, README-as-journal with memory mirror. X1 and X3a resolved; I1 recorded. |
| 2026-08-13 | **K0 — base models drafted for review** ([KERNEL_MODELS_DESIGN.md](KERNEL_MODELS_DESIGN.md)), at the user's request to settle the provenance question on models rather than prose. Contains: a concrete definition of "identity keying" (the unique index, using the existing `binary_gt_unique_idx` as the worked example), a direct answer to "you can't tell whether a prompt edit was unrelated" (nothing ever classifies an edit — reuse joins on phrase+text, and the unmatched remainder IS the work), the `EvidenceSpan` / `ReviewedSpan` / `Split` / `TextSnapshotWitness` / `RunProvenance[T]` / `Authorship` models, and §6's A-vs-B decision. New recon fact: **this system has no run identity at all** — no `run_id` or `batch_id`, and `Manufacturer` keeps one result slot per field, overwritten each run — so a pv-keyed truth would key to something not retained, while the metadata block is the only run identity the codebase has. K1 blocked until §6 is answered. |
| 2026-08-13 | **Plan expanded to substep granularity + simplicity review.** User push-back: binary is a different kind of instrument from keyword/concept; the common base must stay small and one-sentence-explainable. Kernel narrowed from 10 shared elements to 7; answer vocabularies, phrase anchoring, and document base classes explicitly de-scoped (X7). Phrase anchoring demoted to a Tier 2 layer built inside the keyword branch. X5 (placement) and X6 (extend `UserRole`) resolved. Recon findings folded in: versioned S3 download already exists; `LLMPhraseExtractionMetadata` already carries per-node prompt/catalog versions so provenance captures rather than re-lists; `packages/core` cannot import the app, so kernel primitives are field-type agnostic; the model-side structured-evidence check was removed 2026-08-11. I1 analysis written. Next: K1.1. |
| 2026-08-14 | **P0′ — pivot to the user-authored phrase-GT instrument; every design fork resolved.** [LLM_PHRASE_GT_MODEL_DESIGN.txt](LLM_PHRASE_GT_MODEL_DESIGN.txt) reviewed over two sessions; review verdicts that shaped it: the wire emits FLAT `AppliedRule` rows and every catalog child rule is a never-reported `note` (the tree lives in the catalog — hence PH-1 catalog inflation, which also future-proofs reportable children since the wire schema generates from the same catalog); stored stats are chunk-keyed (`"0:1000"` in `chunked_extraction_stats`) then round→phrase-major — the document mirrors that exactly (settled semantics #12). Resolutions this session: I1 → run-config-keyed (§6 answered as B for this instrument, with the fold as the computed-truth half); PH-1…PH-8; identity by EXPLICIT version fields + tripwire (user wants the *what* of a change visible, not just its existence); placement per X5. Kernel dissolved: K3/K4/K5-shapes absorbed into P1, spans (K1/K2/K6) parked — nothing in the design anchors by offset; binary branch parked. Old GT trio + stale `concept_ground_truth_new.py` untouched pending X2. Build order is the user's: P1 models → P2 services; routes excluded (P3 stub). The user audits every substep. Next: P1.1. |
