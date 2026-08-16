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
- **Phase P1 + P2: COMPLETE and type-clean (2026-08-15).** All ten built substeps
  user-audited; P2.5 (judge) parked; P2 gate resolved (below); pyright basic over
  every file from this build: 0 errors.
- **Next action:** P3 — routes (template GET, submission POST, computed-truth GET,
  X6 role gating) — explicitly OUT of this plan; plan it in its own session.
  X2 (legacy GT disposition) also remains open.
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
   about whether it joins the identity. **Extended 08-15 (user):** model params are
   stored EXPLICITLY too ("what was the mismatch?"), and **the whole identity IS the
   unique index** — any parameter two runs can differ on keys their ground truths
   apart; a run is quite literally identified by its identity. Nothing in
   `ExplicitRunIdentity` is exempt from keying; only the index *mechanics* (dotted
   paths vs a validated-cache digest) remain open, decided at P1.5.
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
9. **Judge logic PARKED entirely (2026-08-15).** v1 is human-annotation only: no
   `JudgeAudit` model, no `proposed_by`/`endorsed_by` fields, no judge service. The
   PH-2 design stays on record for if the experiment is ever revived: immutable
   sibling generated once, own provenance (model, judge prompt version), acceptance
   as `proposed_by: judge` + `endorsed_by: human`, never adopted as human-authored.
   A live-debate companion (judge defends the original extraction, annotator argues)
   was considered and rejected the same day: persuasion-gating disagreements would
   bias the GT toward the audited model, and the rationale signal is captured far
   cheaper by `corrected_text` on disagreements.
10. **Fold: latest wins** per node across authors; untouched nodes (empty audits) are
    *unreviewed*, not agreement — the fold only consumes reviewed nodes.
11. **`missed_phrases` lives in the document** beside `extracted_phrases`. A missed
    phrase is a **full positive extraction claim on the happy path** (user, 08-15):
    the screening derivation is required, must identify an entity, and must apply
    the catalog to satisfaction (every condition `satisfied`, no guard `violated`,
    at least one condition), and every grounding derivation must do the same — a
    phrase that would fail screening cannot be recorded, since it changes nothing
    downstream. This narrows the surface from search-recall to extraction-recall.
    Human-asserted phrases carry human derivations only (no `llm_result`),
    validated by case-insensitive whole-word presence in the chunk text
    (`word_regex` — the old service's actual convention; "substring" in earlier
    notes was a paraphrase, corrected 08-15 when P2.3 read the code). The
    happy-path constraint is `MissedPhraseEntry`'s alone — on an extracted phrase,
    a human derivation may legitimately conclude "fail" against an LLM pass.
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
| P1.1 | **Core** `models/ground_truth/audits.py`: `AuditVerdict` (`agree`/`agree_but`/`disagree`), `TextFieldAudit {type, corrected_text, author_email, at, source}` with the requiredness validator (`corrected_text` required for `agree_but` and `disagree`, forbidden on `agree`). ~~Judge sibling model~~ — judge logic parked 2026-08-15 (settled semantics #9). | **DONE 2026-08-15** — models + validators import cleanly; 13 tests cover every requiredness branch |
| P1.2 | **Core** `models/ground_truth/rule_tree.py`: recursive `AuditedRule {rule_id, kind, outcome, explanation, reported, audits, sub_rules}` and `AuditedSection {section_id, combinator, applied_rules}`. `reported=False` marks catalog-synthesized nodes (unreported guards, notes). Combinator values come from the catalog's own vocabulary — this file declares no combinator enum. | **DONE 2026-08-15** — recursion round-trips through pydantic serialization; 11 tests |
| P1.3 | **Core** `models/ground_truth/stage_blocks.py`: `RelationshipGT`, `ScreeningLLMCopy {passed, identified_entity, no_candidate_explanation, sections}`, `HumanScreeningDerivation {identified_entity, audits, sections}`, `ScreeningGT`, `GroundingDerivation`/`TagGroundingGT`/`OovGroundingGT`, `InVocabGroundingGT` (level map mirroring `IterativeGroundingResult`: `{lvl → [{parent_group_id, group_id, stop_reason?, sections, audits}]}` — sections per PH-9), `ExtractedPhraseGT {search_round, stages…}`, `MissedPhraseEntry` (human derivations only). Structural `passed` validator (catalog-free part). | **DONE 2026-08-15** — 20 tests incl. a concept-shaped and a keyword-shaped instance round-tripping |
| P1.4 | **Core** `models/ground_truth/run_identity.py`: `ExplicitRunIdentity` — the enumerated per-node version fields (`prompt_version_id`, `catalog_version`, `llm_model`, model-params hash, node caps) for every node of `LLMPhraseExtractionMetadata` + `KeywordExtractionMetadata` + `ConceptExtractionMetadata`, plus base `chunk_strat` and `ontology_version_id`; `from_metadata()` constructor. **Tripwire test**: a recorded snapshot of the metadata models' field sets; the test fails when a field appears that identity has not consciously included or excluded (follow `tests/test_services/test_prompt_provenance.py` conventions). | **DONE 2026-08-15** — model + constructor + tripwire green (18 tests) |
| P1.5 | **App** `db_models/llm_phrase_ground_truth.py`: `LLMPhraseGroundTruth(Document)` — subject, field_type (union of the two app enums), `scraped_text_file_version_id`, witness (sha256, char_len), `run_identity`, `identity_digest` (validated cache, PH-11 b), the full embedded metadata copy, `chunks: {chunk_key → ChunkGT}`, `created_at`; unique index over (subject, field_type, text version, **identity_digest**), declared as a constant the seeder imports. `db_seed_indices` re-pointed; wire-schema regen stays the user's workflow. | **DONE 2026-08-15** — Document + index defined; round-trip green; no existing test breaks (876 passed) |

**PAUSE — REVIEW P1: both questions RESOLVED 08-15.** (1) Caps in the unique
index? — answered early by the user during the P1.4 audit: the whole identity is
the unique index (PH-11), so caps and explicit params are all in. (2) Index
mechanics — **the user chose (b)**: a unique index on a canonical
`identity_digest` stored as a validated cache of the identity (the `passed`
pattern); explicit fields answer "what was the mismatch", the digest enforces
"identified by its identity". Option (a), whole-subdoc compound keys, was
rejected for field-order sensitivity, multi-KB `response_format` index keys, and
Mongo's 32-field compound cap ruling out full dotted-path flattening.

---

### P2 · Services and utilities

| Sub | Do | Done when |
|---|---|---|
| P2.1 | **Core** `services/ground_truth/catalog_template_inflation.py`: catalog → inflated section/rule skeleton; hydrate from stored stats (chunk → round → phrase); synthesize unreported guards (`reported=False`, implied outcome `not_violated`); include note children as context nodes. Traversal sharing with `catalog_wire_schema` ends at `walk_rules`/`rules_by_id` — more would contort both. | **DONE 2026-08-15** — 20 tests: real equipment+industry catalogs round-trip, unreported-guard synthesis, mixed-provenance descent nodes, loud corruption branches |
| P2.2 | **App** `services/ground_truth/llm_phrase_gt_template_service.py`: assemble a full GT document template from a `Manufacturer`'s stored results — deep metadata copy, witness from caller-supplied text, `ExplicitRunIdentity.from_metadata`, empty audit slots, catalogs via `build_rule_catalog_lookup` with a hard catalog-version pin check per node. | **DONE 2026-08-15** — template builds for `equipments` and `industries` from live-shaped fixtures against the real deployed catalogs |
| P2.3 | **Core** `services/ground_truth/audit_submission_validation.py`: the contract — requiredness rules; human derivations checked against the pinned catalog (outcomes in `outcome_vocab` for the rule's kind, `all`-sections complete + `ordered` exactly-one-chosen on fresh derivations, rule ids exist at that `catalog_version`); entity/tag-change ⇒ fresh sections + disagree coherence; missed-phrase word-boundary check (`word_regex`, the old service's ACTUAL convention — not bare substring); same-author stack-top pop; append ordering. | **DONE 2026-08-15** — 26 tests, the file reads as the contract; every rejection branch covered |
| P2.4 | **Core** `services/ground_truth/gt_fold.py`: latest-wins effective view; effective `passed` via the combinator fold (reproduces the stored cache on pure LLM copies); per-document computed-truth summary (view models live in the service — compute, not storage, per #5) and per-rule agreement rollups keyed by rule id, per document, composable into any cross-document slice. | **DONE 2026-08-15** — 16 tests: both toggle directions, multi-author latest-wins, unreviewed-node exclusion, rollup tallies + overrides |
| P2.5 | ~~`judge_service.py`~~ — **PARKED 2026-08-15** with the rest of the judge logic (settled semantics #9). Revisit only if the judge experiment is ever activated. | n/a |

**PAUSE — REVIEW P2: RESOLVED 08-15.** The user's verdict on the rollup slices:
keep it as **one groupby** — per-document rollups keyed by rule id compose into
any slice via the document's identity fields, and *how to interpret the collected
truths is decided later, post-annotation*. No aggregation helper is built now;
none is owed until real annotated documents exist.

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
| PH-2 | Judge response handling | phrase | **PARKED** 08-15 | Judge logic skipped entirely for v1 (human annotation only); the 08-14 design (immutable sibling, judge provenance, `proposed_by`/`endorsed_by`) stays on record; live-debate variant rejected — see settled semantics #9 |
| PH-3 | `agree-but` payload | phrase | **RESOLVED** 08-14 | Requires `corrected_text` (the addendum) |
| PH-4 | Fold semantics | phrase | **RESOLVED** 08-14 | Latest wins per node; empty audits = unreviewed, excluded |
| PH-5 | Search-recall surface | phrase | **RESOLVED** 08-14 | `missed_phrases` in-document, per chunk, substring-validated |
| PH-6 | Identity mechanism | phrase | **RESOLVED** 08-14 | Explicit version fields (user: "I want to know what was the change") + field-set tripwire |
| PH-7 | Same-author resubmission | phrase | **RESOLVED** 08-14 | Pop retained (old-service convention); flip-history loss recorded as accepted; `superseded` list is the one-line fix if ever wanted |
| PH-8 | Cross-run reuse of judgments | phrase | **RESOLVED** 08-14 | None in v1 — re-judge from scratch; on the annotator to check carry-over |
| PH-9 | Grounding rule container: sections vs flat rules | phrase | **RESOLVED** 08-15 | **Sections everywhere** (user's pick, recommended): grounding catalogs have real sections (attribution / match_qualification / ordered matching), so P2.1 inflation is one traversal for every stage and the ordered-preference context reaches the annotator; the design txt's two "?" spots both resolve to `list[AuditedSection]` |
| PH-10 | Missed-phrase happy path | phrase | **RESOLVED** 08-15 | **Enforced structurally** (user's call): a missed phrase is a full positive extraction claim — required screening derivation with an identified entity, catalog applied to satisfaction (conditions `satisfied`, no guard `violated`, ≥1 condition), groundings likewise; narrows PH-5's surface from search-recall to extraction-recall — see settled semantics #11 |
| PH-11 | Params in identity: hash vs explicit; index coverage | phrase | **RESOLVED** 08-15 | **Explicit params, whole identity = unique index** (user, P1.4 audit): `model_params` stored whole so the mismatch itself is readable; every identity field keys — "a run is quite literally identified by its identity". Index mechanics (dotted paths vs validated-cache digest) open at P1.5 — see the P1 gate |
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
| 2026-08-15 | **Judge parked; P1.1 built.** Session opened on the user's question: is a *live debate companion* (judge defends the original extraction, annotator must convince it) worth building for downstream prompt-optimization/fine-tuning/RL signal? Assessment recorded: persuasion-gating disagreements would bias the GT toward the audited model; the rationale signal is mostly recoverable from `corrected_text`; RL-scale data is unrealistic at this annotation volume. User verdict: **skip judge logic entirely for v1 — human annotation only.** PH-2 → PARKED (design on record), P2.5 parked, `judge_audit?` dropped from the P1.2 shape, settled semantics #9 rewritten. Then P1.1 built: [audits.py](../../../../../../packages/core/src/core/models/ground_truth/audits.py) — `AuditVerdict` (underscore value `agree_but` chosen over the design txt's hyphen; queryable identifier) + `TextFieldAudit` (requiredness validator: `corrected_text` non-blank for `agree_but`/`disagree`, forbidden on `agree`; `at` explicit with no default per the `ConceptCorrectionLog` convention; `source` plain `str` in core since the enum is app-side). 13 tests in `packages/core/tests/test_models/test_ground_truth_audits.py`; full suite 807 passed. Awaiting the user's P1.1 audit; then P1.2. |
| 2026-08-15 | **P1.1 audited and approved by the user ("that's good"); P1.2 built.** [rule_tree.py](../../../../../../packages/core/src/core/models/ground_truth/rule_tree.py): recursive `AuditedRule` + `AuditedSection`. Shape decisions: `kind: RuleKind` and `combinator: Combinator` are IMPORTED from `rule_catalog` (single vocabulary, nothing re-declared — a removed kind/combinator would fail old-doc loads loudly, the tripwire taste); `outcome`/`explanation` structurally optional with two catalog-free validators — `reported=True` ⇒ both present (wire rows always carry them), `kind="note"` ⇒ never reported + no outcome (mirrors `RuleNode.check_reporting_matches_kind`); unreported guards = `reported=False` + implied outcome + no explanation; `reported` has NO default so inflation code must decide consciously; `outcome` stays plain `str` per the `AppliedRule` precedent (vocab is catalog data, checked at P2.3 against the pinned `catalog_version`). 11 tests incl. 3-level round-trip with an audit on a leaf; full suite 818 passed. Awaiting the user's P1.2 audit; then P1.3. |
| 2026-08-15 | **P1.2 audited and approved ("looks good"); PH-9 resolved; P1.3 built.** New code facts from recon: `passed_implied_by` (`applied_rule_validation.py:100`) is kind-driven — condition ≠ "satisfied" blocks, any reported guard blocks, empty report blocks — so the structural `passed` cache validator is catalog-free (kind lives inline on `AuditedRule`; the "satisfied" literal is mirrored with a cross-reference comment). Grounding catalogs have REAL sections (industry IGR: attribution/all, match_qualification/all, matching/**ordered** over 4 preferences) → PH-9 asked and resolved: **sections everywhere**. [stage_blocks.py](../../../../../../packages/core/src/core/models/ground_truth/stage_blocks.py): `RelationshipGT`, `ScreeningLLMCopy` (branch validators mirror `ScreeningVerdict`: no-candidate ⇒ no reported rules + explanation required; entity ⇒ reported rules exist + no no_candidate_explanation; passed == reported-rows-only fold — synthesized unreported guards deliberately do NOT reject), `HumanScreeningDerivation`, `ScreeningGT`, `GroundingDerivation` (tag plays identified_entity's role, per the design txt's own likening), `TagGroundingGT` (llm_result=None = human-asserted tag, must carry ≥1 derivation), `OovGroundingGT` (per-phrase tag map), `InVocabNodeGT`/`InVocabGroundingGT` (recorded flattening: direct-vs-iterative og-tag split and altLabel indirection collapse into one sections container per node — flagged in the plan, not vetoed), `ExtractedPhraseGT` (optional stage blocks; required combinations are app knowledge), `MissedPhraseEntry` (human-only, provenance mirrors audit convention, substring check deferred to P2.3). 20 tests incl. keyword-shaped and concept-shaped instances round-tripping; full suite 838 passed. Awaiting the user's P1.3 audit; then P1.4. |
| 2026-08-15 | **PH-10 — missed-phrase happy path, user's call during the P1.3 audit.** `MissedPhraseEntry` upgraded from optional stages to a full positive extraction claim: `screening` now REQUIRED with a non-null `identified_entity`, and a catalog-free `_happy_path_problem` fold rejects any condition ≠ `satisfied` (incl. `not_triggered` and unfilled `None`), any guard `violated`, or zero conditions — applied to the screening sections and every grounding derivation. `violated` is imported from `ONLY_REACHABLE_OUTCOME_BY_REPORT_WHEN`, not re-declared. Verified before building: both grounding catalog families carry conditions (equipment FGR: 4, industry IGR: 2), and preferences/quality never gate (`passed_implied_by` reads only condition+guard) — so preferences with `chosen` pass through, with a test pinning that. Consequence recorded in settled semantics #11: the surface narrows from search-recall to extraction-recall; the constraint is MissedPhraseEntry's alone — extracted-phrase derivations may still conclude "fail". 9 new tests (29 in the file); full suite 847 passed. P1.3 audit continues; then P1.4. |
| 2026-08-15 | **P1.3 (incl. PH-10) audited and approved ("sounds good"); P1.4 built.** [run_identity.py](../../../../../../packages/core/src/core/models/ground_truth/run_identity.py): `NodeIdentity` (llm_model NAME, `model_params_hash` = sha256 over sort_keys-canonical JSON — a hash is acceptable for params alone because the full copy lives in the document's embedded metadata, so "what changed" stays answerable; prompt_version_id; catalog_version, None for catalog-less search/relationship; the three caps as named optional fields) + `ExplicitRunIdentity` (chunk_strat embedded whole, ontology_version_id, 4 required shared nodes, 3 optional grounding nodes with a family validator: initial+recursive paired, freehand exclusive) + `from_metadata()` serving all three metadata classes by attribute. Consciously EXCLUDED and recorded in the tripwire: `created_at` (when, not what) and `prompt_name` (addressing; the version id pins the bytes). Tripwire covers all 10 metadata models, failing with instructions to place new fields consciously. 18 tests; full suite 865 passed. Recon fact: `GPTModelParams` is a flat multiple-inheritance composite — params live directly on the instance. Awaiting the user's P1.4 audit; then P1.5, then the P1 REVIEW gate (caps in the unique index?). |
| 2026-08-15 | **PH-11 — P1.4 amended during its audit (user's two calls).** (1) `model_params` now stored EXPLICITLY in `NodeIdentity`, hash deleted — "I would like to know: what was the mismatch?" Model-level equality stays order-insensitive on dict-valued params (dict equality is), with a test pinning that; order sensitivity is purely an index-key concern. (2) **The whole identity IS the unique index** — any differentiating parameter keys; this ANSWERS the P1 gate's original question (caps: in). The gate's remaining question is index mechanics: Mongo's 32-field compound cap rules out full dotted-path flattening, so it's whole-subdoc compound keys (field-order sensitive, KB-scale response_format schemas in keys) vs a canonical `identity_digest` validated-cache (the `passed` pattern). 19 tests; full suite 866 passed. P1.4 audit continues; then P1.5. |
| 2026-08-15 | **P1.4 approved with option (b); P1.5 built — P1 COMPLETE.** Core: `ExplicitRunIdentity.canonical_digest()` (sha256 over recursively-sorted canonical JSON — dict-order noise cannot split index keys, tested) and `ChunkGT` in stage_blocks (core so the P2.4 fold can traverse documents). App: [llm_phrase_ground_truth.py](../../db_models/llm_phrase_ground_truth.py) — `LLMPhraseGroundTruth(Document)` with FOUR self-proving validators: digest == canonical_digest (the index key is a validated cache), run_identity == from_metadata(metadata) (identity is a validated PROJECTION of the embedded copy — it can never quietly disagree), field family == metadata family (the metadata union discriminates on each family's required grounding nodes), chunk keys match `^\d+:\d+$`. `field_type` is `KeywordTypeEnum \| ConceptTypeEnum` — no new 7-value enum to drift. Unique index (subject, field_type, text version, identity_digest) declared as a constant in the db_model, imported by `db_seed_indices.py` (new `create_llm_phrase_ground_truth_indexes`, wired into `seed_all_indexes`); Document registered in `APP_DOCUMENT_MODELS`. **Test-infra discovery:** Beanie 2.0 `init_beanie` unconditionally runs `buildInfo` I/O, but construction + `model_validate` need only `cls._document_settings` — new `tests/test_db_models/conftest.py` replicates the synchronous `Initializer.init_settings` step for all app documents, so document contracts are testable offline. 8 document tests + 2 digest tests; full suite 876 passed. Awaiting the user's P1.5 audit; then P2.1. |
| 2026-08-15 | **P1.5 approved ("looks good"); P2.1 built — first service.** [catalog_template_inflation.py](../../../../../../packages/core/src/core/services/ground_truth/catalog_template_inflation.py): `inflate_applied_rules` (the primitive: walk catalog sections, hydrate by rule id, synthesize silence), `inflate_screening_llm_copy` (doubles as an integrity check — a stored verdict whose `passed` disagrees with its own rules cannot inflate), `inflate_tag_groundings`, `inflate_in_vocab_grounding` (reuses `get_phrase_trails` for per-phrase regrouping), `inflate_chunk` (chunk → round → phrase; phrase membership + search_round from relationship stats). Judgment calls recorded: unreported guard ⇒ `not_violated` constant, unreported preference ⇒ outcome None (silence means another branch won); empty report ⇒ full silent skeleton ONLY on screening's no-candidate branch, `sections=[]` on zero-row descent nodes, raise on zero-rule tags; mixed-provenance descent nodes partition rows by which catalog knows them (direct maps can carry IGR rows — `iterative_tagging.py`'s own comment) and concatenate both catalogs' sections, nothing dropped; orphan grounding phrases warn+skip mirroring `extraction_dump_util`; loud InflationError on unknown ids, missing always-rules in non-empty reports, out-of-vocab outcomes, conflicting duplicates. Real-catalog tests load the app's JSON as data (no app import). 20 tests; full suite 896 passed. Awaiting the user's P2.1 audit; then P2.2. |
| 2026-08-15 | **P2.1 approved ("go ahead"); P2.2 built.** [llm_phrase_gt_template_service.py](llm_phrase_gt_template_service.py): `build_llm_phrase_gt_template(manufacturer, field_type, scraped_text, catalog_lookup=None)` → unsaved `LLMPhraseGroundTruth`. Decisions: catalogs via the existing `build_rule_catalog_lookup()` ((stage, field_type) → catalog, injectable); **hard catalog-version pin check per metadata node** — stored `catalog_version` ≠ deployed catalog hard-fails naming both versions (inflating against a catalog the run didn't use would mis-slot rules; remedy = re-run or check out matching catalogs; the resume-invariant taste); **witness computed from caller-supplied text** (fetching the pinned S3 version is route/notebook business — a wrong text produces a witness that provably disagrees with the object it claims to pin); **metadata deep-copied** (`model_copy(deep=True)` — the document is the surviving record, tested `is not` + nested container identity); the `chunk_stats` (keyword) vs `chunked_extraction_stats` (concept) naming split handled per family. Beanie shim moved from `tests/test_db_models/conftest.py` up to the app tests root conftest (Manufacturer fixtures need it too). Recon facts: `KeywordExtractionStats` requires `results`, `ConceptExtractionStats` requires `results` + `brute_search`. 5 tests against the REAL deployed catalogs; full suite 901 passed. Awaiting the user's P2.2 audit; then P2.3. |
| 2026-08-15 | **P2.2 approved ("go ahead"); P2.3 built — the submission contract.** [audit_submission_validation.py](../../../../../../packages/core/src/core/services/ground_truth/audit_submission_validation.py): `append_text_audit` (PH-7 stack-top pop), `submit_screening_derivation`, `submit_grounding_derivation` (tag plays entity; human-asserted tags always fresh; sentinel tags rejected), `submit_missed_phrase`. The contract: one submission one author + a derivation with no audit anywhere asserts nothing; keep-the-entity ⇒ mirror the LLM copy's tree with NO SILENT EDITS (any outcome/explanation diff needs a rule audit) and no disagree entity audit; replace-the-entity ⇒ latest entity audit is disagree with corrected_text == replacement, sections fresh against the catalog's silent skeleton (shape check REUSES `inflate_applied_rules(synthesize_all_on_empty=True)` as the canonical template), all-sections complete + ordered ladders exactly-one-chosen (the wire's own cardinality); entity None only against a no-candidate copy — rejection is expressed through rule outcomes, the fold decides passed; missed phrases replace by (author, phrase) — keyed assertions, not stacked opinions. **Correction recorded:** old-service missed-phrase presence is `word_regex` + IGNORECASE (case-insensitive whole word), not bare substring — code read during recon, semantics #11 amended, "weld" does not ride in on "welding" (tested). 26 tests, all passing first run; full suite 927. Awaiting the user's P2.3 audit; then P2.4. |
| 2026-08-15 | **P2.3 approved ("go ahead"); P2.4 built — P2 COMPLETE (P2.5 parked).** [gt_fold.py](../../../../../../packages/core/src/core/services/ground_truth/gt_fold.py): `effective_passed` (the combinator fold over outcomes, human-authored and reported alike: every evaluated condition `satisfied`, no guard `violated`, ≥1 condition evaluated — reproduces the stored cache on pure LLM copies, tested on both branches; settled #6's two toggle directions tested exactly); `effective_text` / `effective_screening` / `effective_tag_groundings` / `effective_in_vocab` (latest wins per surface, settled #10; unreviewed surfaces fold the LLM copy with `reviewed=False`); `compute_document_truth` (view models live in the service file — compute, not storage, settled #5; missed phrases surface as human-asserted positives, reviewed by construction per PH-10); `rule_agreement_rollup` → `{rule_id: {agree, agree_but, disagree, overridden}}` PER DOCUMENT — unreviewed rules excluded entirely (matching-by-default is not agreement, tested), overrides counted where the effective outcome differs from the LLM's, and per-doc rollups sum trivially into any cross-document slice since identity carries subject/field/catalog versions. Constants `CONDITION_HOLDS_OUTCOME`/`GUARD_FIRED_OUTCOME` promoted public in stage_blocks (no third "satisfied" literal). 16 tests; full suite 943. Awaiting the user's P2.4 audit + the P2 gate answer (first rollup slice). P3 (routes) remains excluded — own session. |
| 2026-08-15 | **P2 gate resolved by the user + type-hygiene pass — PHASE P1+P2 CLOSED.** Gate verdict: one groupby, per-doc rollups as built, interpretation of collected truths decided post-annotation; no aggregation helper owed yet. Then a pyright pass (basic mode, venv interpreter — `.venv/bin/pyright --pythonpath .venv/bin/python`, matching Pylance) over every file from this build: **226 diagnostics → 0 errors / 0 warnings**, full suite still 943 green. Real fixes (5, all Optional-narrowing in sources): the no-candidate guard folded into the entity-replacement branch of `submit_screening_derivation` (narrowing `replacement` to `str`); `submit_grounding_derivation`'s `fresh` flag inlined so pyright narrows `llm_sections` in the else-branch; `effective_text`'s disagree branch asserts `corrected_text` (the model guarantees it); `_missed_truth` helper asserts the PH-10-guaranteed entity; conftest uses `getattr(model, "Settings")` (`type[Document]` has no such attribute statically). Test hygiene: splat-fixture dicts annotated `dict[str, Any]`; asserts before Optional access; `# type: ignore` ONLY on deliberately-invalid inputs whose ill-typedness IS the test (missing `at`, extra field, bad combinator); `_rule` helper typed `RuleKind`; one dict-literal `llm_result` replaced with a real `ScreeningLLMCopy`. Working tree note: this branch also carries the earlier uncommitted phrase-trail rework — nothing committed this session. **Next session: P3 routes.** |
