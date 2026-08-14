# Ground truth rebuild — master plan & journal

**Branch `new-ground-truth` · this file is the single source of truth for progress.**
The three design docs in this folder are the specs; this README is the plan, the fork
ledger, and the append-only journal. A fresh session resumes by reading: (1) the STATE
block, (2) the fork ledger, (3) the step it says is next.

| Spec | Instrument | Fields |
|---|---|---|
| [BINARY_GT_ANNOTATION_DESIGN.md](BINARY_GT_ANNOTATION_DESIGN.md) | Binary classification | `is_manufacturer`, `is_product_manufacturer`, `is_contract_manufacturer` |
| [KEYWORD_GROUND_TRUTH_DESIGN.md](KEYWORD_GROUND_TRUTH_DESIGN.md) | Keyword entity inventory | `products`, `contract_products`, `equipments` |
| [CONCEPT_GROUND_TRUTH_DESIGN.md](CONCEPT_GROUND_TRUTH_DESIGN.md) | Concept assertions | `industries`, `conformity_attestations`, `material_caps`, `process_caps` |

---

## STATE

- **Phase:** Tier 1 kernel · not started
- **Step in flight:** K0 — base-model design review
- **Last completed:** K0 draft written (2026-08-13)
- **Next action:** user reviews
  [KERNEL_MODELS_DESIGN.md](KERNEL_MODELS_DESIGN.md) and answers its §6 — identity
  Option A (provenance as metadata), Option B (provenance in the key), or the middle
  option. **K1 does not start until §6 is answered**, because the answer changes what
  the models are.
- **Blockers:** K1 blocked on §6. X2 (legacy disposition) must resolve before B2.
- **Kernel freeze:** not yet (K8)

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

Decided 2026-08-13. **Binary is a different kind of instrument from keyword and
concept, and the plan does not pretend otherwise.** Sharing is allowed only where the
semantics are obvious in one sentence.

```
Tier 1 — KERNEL (all three instruments)          ← this plan builds it, steps K1–K8
  packages/core/src/core/…  pure, tiny, field-type agnostic
  spans · span validator · snapshot witness · provenance capture · span math

        ├── BINARY branch (own chat/phase)        ← first consumer, proves the kernel
        │
        └── Tier 2 — PHRASE LAYER (keyword + concept only)
              built inside the KEYWORD branch, lifted to shared code only when
              CONCEPT actually reuses it — never speculatively
                    ├── KEYWORD branch
                    └── CONCEPT branch
```

**Deliberately NOT shared** (recorded so it is not "fixed" later by mistake):

- **Answer vocabularies.** Binary answers `yes | no | insufficient_evidence` about a
  proposition; keyword and concept answer `confirmed | rejected | uncertain` about a
  candidate. Same arity, different meaning. Each instrument declares its own 3-line
  enum. Forcing one enum would buy nothing and cost clarity.
- **Phrase anchoring.** Only keyword and concept are phrase-anchored. It is Tier 2,
  and Tier 2 is not built until a second consumer exists.
- **Document base classes.** No abstract `GroundTruthDocument`. The three instruments'
  Beanie Documents are independent; they share *field types*, not inheritance.

**Hard constraint discovered 2026-08-13:** `packages/core` never imports
`data_etl_app` (verified: zero matches), and the field-type enums
(`ConceptTypeEnum`, `KeywordTypeEnum`, `BinaryClassificationTypeEnum`,
`GroundTruthSource`) live app-side in `models/types_and_enums.py`. Therefore **every
kernel primitive is field-type agnostic** — it never names a field enum. Instruments
bind the concrete enums in their own app-side Documents. This is a constraint the
architecture already enforces, and it happens to keep the kernel honest.

## What the kernel actually shares

Narrowed from the ten candidate elements after the 2026-08-13 simplicity review.
Each row must pass: *can its semantics be stated in one sentence?*

| # | Shared element | One-sentence semantics | Step |
|---|---|---|---|
| S1 | `EvidenceSpan` / `ReviewedSpan` | A character range in the scraped text, plus the text that was there. | K1 |
| S2 | Span validator | Given the text and a span, prove the quote is really there. | K2 |
| S3 | Snapshot witness | Pin the exact text version, and be able to prove later it hasn't changed. | K3 |
| S4 | `RunProvenance` | Everything needed to reproduce the run that suggested this. | K4 |
| S5 | Authorship + append-only entry | Who decided, when, and what they did — never overwritten. | K5 |
| S6 | Span math | Which spans fall inside which ranges. | K6 |
| S7 | `Split` (dev\|test) | Which subjects headline numbers may come from. | K1 |

Everything else in the three specs — task leases, adjudication, bands, trays,
sampling strata, brute sweep, scoring — is instrument-specific and lives in a branch.

---

# Tier 1 — the kernel · steps K1–K8

Target: `packages/core/src/core/` for pure code, tests in `packages/core/tests/`.
Two substeps (K5.1, K5.3) are app-side and say so.

---

### K1 · Vocabulary — the words all three instruments share

**Semantics:** plain data, no behavior. New file
`packages/core/src/core/models/ground_truth/primitives.py`.

| Sub | Do | Done when |
|---|---|---|
| K1.1 | `EvidenceSpan {start, end, quote}` and `ReviewedSpan {start, end}` as pydantic models. Half-open ranges (`[start, end)`) — stated in the docstring, since every later comparison depends on it. Validator: `end > start`, `start >= 0`, non-empty quote. | Models import cleanly; field-type agnostic (no app imports) |
| K1.2 | `Split` enum (`dev` \| `test`) with a docstring recording *why*: anchor-mfg and steelcraft are dev by construction, headline numbers come from test only. | Enum exists |
| K1.3 | Unit tests: valid construction, each rejection case, half-open boundary behavior. | `pytest packages/core` green |

**PAUSE — REVIEW K1.** *Question for the user:* are these the right primitive shapes,
and is `quote` on the span (denormalized, a second witness) worth its storage cost
versus deriving it from the text every time? (Recommendation: keep it — it is the
witness that catches a snapshot swap, and all three specs assume it.)

---

### K2 · The span validator — one pure function

**Semantics:** given text and a span, is the quote really there? New file
`packages/core/src/core/services/ground_truth/span_validation.py`.

**Context that changed on 2026-08-13:** this is *new* code, not a shared util. The
model-side structured-evidence check this was supposed to share
([applied_rule.py:14-23](packages/core/src/core/models/extraction_schemas/applied_rule.py#L14-L23))
was removed on 2026-08-11 because a model asked for a span the source does not contain
has no compliant move. That reasoning does not transfer to humans: a human quoting by
text selection cannot fabricate a span. The human-side rule stands on its own.

| Sub | Do | Done when |
|---|---|---|
| K2.1 | `validate_span(text, span) -> None` — raises when `text[start:end] != quote`. Typed exceptions, not bare `ValueError`. | Function + error classes exist |
| K2.2 | `locate_quote(text, quote) -> int` — for spans submitted without an offset. Exactly one occurrence → its offset; zero → `QuoteNotFound`; more than one → `AmbiguousQuote` (never guess). | Function exists |
| K2.3 | Unit tests, explicitly including: non-ASCII text (see the unicode-escape corruption history), a quote occurring twice, quotes at index 0 and at EOF, empty text, quote longer than text. | `pytest packages/core` green |

**PAUSE — REVIEW K2.** *Question:* on `AmbiguousQuote`, should the API return the
candidate offsets so a client can disambiguate, or just reject? (Recommendation:
return them — it costs one list and turns a dead end into a UI affordance later.)

---

### K3 · Snapshot and witness

**Semantics:** pin the exact text, and be able to prove it later. New file
`packages/core/src/core/services/ground_truth/text_snapshot.py`.

**Good news from recon:** versioned download already exists —
`download_scraped_text_from_s3_by_subject_unique_id(subject_unique_id, version_id)` in
[scraped_text_file_util.py:161](packages/infra/src/infra/utils/aws/s3/scraped_text_file_util.py#L161),
alongside `get_latest_version_id_by_subject_unique_id`. This step is a thin wrapper
plus hashing, not new S3 work.

| Sub | Do | Done when |
|---|---|---|
| K3.1 | `TextSnapshotWitness {subject_unique_id, scraped_text_file_version_id, sha256, char_len}` + `compute_witness(subject, version_id, text)` — pure. | Model + function exist |
| K3.2 | `verify_witness(text, witness) -> None` — raises `SnapshotMismatch` on sha or length disagreement. Pure. | Function exists |
| K3.3 | `load_pinned_snapshot(subject, version_id, witness=None)` — async; downloads by pinned version, verifies when a witness is supplied. | Function exists |
| K3.4 | Tests: pure ones by default (hash stability, mismatch detection, unicode length semantics — chars not bytes); the S3 round-trip marked `integration`. | `pytest packages/core` green; integration test passes when run explicitly |

**PAUSE — REVIEW K3.** *Question:* `char_len` — characters or bytes? (Recommendation:
characters, because every offset in every spec is a Python string index. Worth stating
once, here, because getting it wrong silently corrupts every span.)

---

### K4 · Provenance capture — the comprehensiveness guarantee

**Semantics:** everything needed to reproduce the run that suggested this. New file
`packages/core/src/core/models/ground_truth/provenance.py`.

**This is the step that delivers the user's stated goal** ("provenance as comprehensive
as possible, to ensure reproducibility"). Recon found the pipeline already carries it:
[`LLMPhraseExtractionMetadata`](packages/core/src/core/models/extraction_results/llm_phrase_extraction_results.py#L124)
holds per-node `prompt_version_id` (S3 version), `catalog_version`, `llm_model`,
`model_params`, plus `chunk_strat` and `ontology_version_id` on the base. So
`RunProvenance` **captures existing models rather than re-listing fields** — re-listing
is how a provenance block silently goes stale.

| Sub | Do | Done when |
|---|---|---|
| K4.1 | `RunProvenance` composing the existing extraction metadata model(s) plus the run reference and the pinned `scraped_text_file_version_id`. Generic over the metadata type so single-stage (binary) and multi-stage (keyword/concept) both fit without a union of hand-written fields. | Model exists; round-trips a real metadata instance |
| K4.2 | **Completeness tripwire test** — fails if a field is added to a source metadata model and is not reachable through `RunProvenance`. Follow the existing convention in `apps/data_etl_app/tests/test_services/test_prompt_provenance.py`. | Test exists and fails when a field is hidden |
| K4.3 | Tests: construction from each metadata shape; serialization round-trip. | `pytest` green |

**PAUSE — REVIEW K4.** *Question:* this is where the I1 fork gets its teeth. With
provenance this complete and mechanically enforced, does capturing it as **metadata**
satisfy the reproducibility goal — or do you still want provenance in the identity
key? (Analysis in the ledger under I1. Recommendation: metadata; the kernel is
unblocked either way, and the decision is only load-bearing at keyword kickoff.)

---

### K5 · Authorship, roles, append-only

**Semantics:** who decided, when, and what they did — never overwritten.

| Sub | Do | Done when |
|---|---|---|
| K5.1 | **App-side:** extend `UserRole` in [db_models/user.py](apps/data_etl_app/src/data_etl_app/db_models/user.py) with `ANNOTATOR` and `ADJUDICATOR`. Existing registration/auth unchanged. | Enum extended; no existing test breaks |
| K5.2 | **Core:** `DecisionLogEntry {author_email, at, op, payload}` — plain data, append-only by convention (nothing mutates it). Docstring records that corrections supersede rather than edit. | Model exists |
| K5.3 | **App-side:** a role-gate dependency (`require_role(...)`) in `dependencies/`, reusing the existing user-auth dependency. | Dependency exists with tests |
| K5.4 | Tests for both. | `pytest` green |

**PAUSE — REVIEW K5.** *Question:* should `ADJUDICATOR` imply `ANNOTATOR`, or be a
disjoint role? (Recommendation: imply — with one human today, requiring two role
grants is friction with no benefit; the distinction that matters is recorded per
*annotation*, not per person.)

---

### K6 · Span math for scoring

**Semantics:** which spans fall inside which ranges. New file
`packages/core/src/core/services/ground_truth/span_math.py`.

This is the small pure kernel behind three separate promises in the specs: score only
inside what a human read, derive per-chunk truth for any chunking, and price the
`max_chunks` truncation.

| Sub | Do | Done when |
|---|---|---|
| K6.1 | `intersect(a, b)`, `contains(outer, inner)`, `overlaps(a, b)` over half-open ranges; `total_covered(spans)` with overlap merging. | Functions exist |
| K6.2 | `is_beyond_window(span, window)` — the evidence-beyond-window measurement both the binary and concept specs promise. | Function exists |
| K6.3 | Tests: adjacency vs overlap (the classic half-open off-by-one), empty intersections, unsorted and overlapping input to `total_covered`. | `pytest packages/core` green |

**PAUSE — REVIEW K6.** *Question:* does `chunk_bounds` parse cleanly into a range here?
It is a string key in the pipeline (`chunk_map` keys); if its format is not a plain
`"start:end"`, K6 needs a parser and that is worth seeing before it is written.

---

### K7 · Legacy disposition tooling — build now, run when X2 resolves

**Semantics:** be able to archive the old collections before anything is dropped.

| Sub | Do | Done when |
|---|---|---|
| K7.1 | Script under `apps/data_etl_app/src/data_etl_app/scripts/` that JSON-dumps `binary_ground_truths`, `keyword_ground_truths`, `concept_ground_truths` to `batch_data/gt_archive/<date>/`. Read-only; no deletes anywhere in it. | Script exists, dry-run prints counts |
| K7.2 | The keyword-F2 query: count docs with non-empty `corrections` in each collection — the evidence that decides whether any real human labor exists to preserve. | Query runs; counts recorded in the journal |
| K7.3 | Run K7.2 against the dev DB and record the numbers in the journal. **Do not delete anything.** | Journal row with counts |

**PAUSE — REVIEW K7.** *Question:* with the counts in hand, X2 becomes decidable —
archive-and-wipe, or is there labor worth migrating? This is the gate for B2.

---

### K8 · Kernel freeze gate

| Sub | Do | Done when |
|---|---|---|
| K8.1 | Full suite green from repo root; confirm no `data_etl_app` import leaked into `packages/core`. | `pytest` green; grep clean |
| K8.2 | Write the kernel's own short README (what each primitive is for, one line each). | File exists |
| K8.3 | User review of the whole kernel surface. | Sign-off |
| K8.4 | Journal row **"KERNEL v1 FROZEN"**; update the `gt-master-plan` memory. After this, kernel API changes need a journal-logged decision. | Journal + memory updated |

**PAUSE — REVIEW K8. This is the branching point.** After K8 the plan forks; the next
chat can be the binary branch, and keyword/concept can proceed independently later.

---

# Beyond the branch point — sketches only

Deliberately not expanded to substeps: doing so now would be guessing, and each
branch's kickoff re-reads its spec against a kernel that actually exists.

**Binary branch (B).** B1 fork closeout (BIN-4 guideline wording, BIN-5 bundling) ·
B2 three collections + indexes + schema regen (**needs X2**) · B3 guideline files ·
B4 annotator plane with blinding enforced by response models · B5 adjudicator + admin
planes · B6 consumer plane and metrics · B7 CLI annotation harness · B8 caller
migration and legacy retirement · B9 pilot batch.

**Tier 2 phrase layer + keyword branch (K).** K0 kickoff confirms I1 and closes
KW-F1/KW-F3; the phrase-anchoring primitive is built here as *keyword code*, and only
lifted into the kernel if the concept branch genuinely reuses it.

**Concept branch (C).** Kickoff closes CON-2, CON-5, CON-6; heaviest instrument
(ontology trays, brute sweep, depth semantics, ancestor-credit scoring).

**Cross-instrument closeout.** Test-split onboarding, second-annotator tooling, and a
UI decision informed by real annotation-hours data from the CLI-harness era.

---

## Fork ledger

**RESOLVED** = verdict recorded · **OPEN** = decide at the step named · **INTERPRETED**
= working interpretation, confirm at kickoff.

| Id | Question | Scope | Status | Verdict |
|---|---|---|---|---|
| X1 | Multi-annotator machinery | all | **RESOLVED** 08-12 | Support N + append-only + provisional/adjudicated from day one; run with 1; agreement tooling later |
| X3a | Binary annotation surface | binary | **RESOLVED** 08-12 | Full scraped text; the model's window may be smaller; offsets let eval down-scope |
| X5 | Kernel placement | all | **RESOLVED** 08-13 | Pure primitives in `packages/core`; Beanie Documents and routes stay in `apps/data_etl_app` |
| X6 | Annotator/adjudicator identity | all | **RESOLVED** 08-13 | Extend `UserRole` with `ANNOTATOR`, `ADJUDICATOR` |
| X7 | How much to share | all | **RESOLVED** 08-13 | Binary is its own kind; kernel stays tiny and one-sentence-explainable; answer vocabularies, phrase anchoring, and document base classes deliberately NOT shared |
| X2 | Legacy collections: migrate vs archive+wipe | all | **OPEN** | Rec: archive-dump then wipe. Decidable after K7.3 produces counts. Blocks B2 |
| I1 | Does "phrase-level with full provenance" put provenance in the identity key? | kw + concept | **OPEN** — analysis below | Rec: comprehensive provenance as *metadata*; identity stays snapshot-keyed. Decide at K0; does not block the kernel |
| X4 | dev/test split on all three instruments | all | OPEN | Rec: yes everywhere. `Split` ships in K1 regardless; adoption decided per branch |
| BIN-4 | Guidelines: human-language vs catalog rules | binary | OPEN | Rec: human-language, catalog wording withheld |
| BIN-5 | Bundle all three questions per sitting | binary | OPEN | Rec: bundle by default |
| KW-F1 | Blind-highlight pass | keyword | OPEN | Rec: calibration-only during pilot |
| KW-F3 | Products family only vs both | keyword | OPEN | Rec: both |
| CON-2 | Explicit negatives with catalog-rule reasons | concept | OPEN | Rec: yes, rule id optional |
| CON-5 | How "uncertain" scores | concept | OPEN | Rec: excluded from numerator and denominator, reported as abstention |
| CON-6 | Keyword fields onto the assertion model later | concept | OPEN | Rec: defer, block nothing |

### I1 — the analysis (raised 2026-08-12, sharpened 2026-08-13)

The user's goal: *provenance as comprehensive as possible, for reproducibility* — with
the intuition that if any of it changes, that is logically a different ground truth.

**The two concerns are separable, and only one of them is free.**

*Comprehensiveness* costs nothing and is delivered by K4: every judgment record embeds
the full `RunProvenance`, with a tripwire test that fails when a new metadata field
escapes capture. Nothing is lost, and any past judgment can be replayed exactly.

*Identity-keying* is the expensive half. Ask which provenance fields actually change
**what is true about the text**:

| Provenance field | Does a change alter the truth? |
|---|---|
| `scraped_text_file_version_id` | **Yes** — different text, different truth. Already in the key. |
| `ontology_version_id` | Partly, for concept only: it changes the available *vocabulary*, not the facts. Concept spec §8 handles it with a revalidation sweep that turns confirmed-OOV into in-vocab. |
| `prompt_version_id`, `catalog_version`, `llm_model`, `model_params`, `chunk_strat` | **No** — these change what the *model said*, not what is true. |

So exactly one truth-bearing field exists, and it is already in the key. Putting the
rest in the key means a prompt edit invalidates a human's judgment about whether
"we operate 14 progressive stamping presses" proves manufacturing — which it plainly
does not. The keyword spec measured the cadence: **twelve prompt edits in the week it
was written**. Under identity-keying, nothing ever accumulates.

The real intuition behind the fork is still valid and is handled separately: the
*candidate set* a human judged **is** run-dependent, and that is exactly what a
per-run overlay records (keyword Layer B, concept `candidate_ref`) — full provenance,
run-scoped, without fragmenting the durable truth. And where a *policy* change really
should invalidate a label, `policy_flags` + re-adjudication handles it by re-deciding
a slice rather than re-collecting a corpus.

**Recommendation: option A** — phrase-anchored judgment records carrying comprehensive
provenance, snapshot-keyed durable truth. Option C (phrase records primary, snapshot
rollup folded) is nearly the same design, since truth is folded from the append-only
records either way; if the fold is what appeals, C and A converge. Option B is the one
that defeats the purpose.

---

## Journal (append-only, newest last)

| Date | Entry |
|---|---|
| 2026-08-12 | **P0 — chartered.** README repurposed from a stale duplicate of the binary design into master plan + journal. Locked: API-first (UI deferred, CLI harness stand-in), binary-first order, README-as-journal with memory mirror. X1 and X3a resolved; I1 recorded. |
| 2026-08-13 | **K0 — base models drafted for review** ([KERNEL_MODELS_DESIGN.md](KERNEL_MODELS_DESIGN.md)), at the user's request to settle the provenance question on models rather than prose. Contains: a concrete definition of "identity keying" (the unique index, using the existing `binary_gt_unique_idx` as the worked example), a direct answer to "you can't tell whether a prompt edit was unrelated" (nothing ever classifies an edit — reuse joins on phrase+text, and the unmatched remainder IS the work), the `EvidenceSpan` / `ReviewedSpan` / `Split` / `TextSnapshotWitness` / `RunProvenance[T]` / `Authorship` models, and §6's A-vs-B decision. New recon fact: **this system has no run identity at all** — no `run_id` or `batch_id`, and `Manufacturer` keeps one result slot per field, overwritten each run — so a pv-keyed truth would key to something not retained, while the metadata block is the only run identity the codebase has. K1 blocked until §6 is answered. |
| 2026-08-13 | **Plan expanded to substep granularity + simplicity review.** User push-back: binary is a different kind of instrument from keyword/concept; the common base must stay small and one-sentence-explainable. Kernel narrowed from 10 shared elements to 7; answer vocabularies, phrase anchoring, and document base classes explicitly de-scoped (X7). Phrase anchoring demoted to a Tier 2 layer built inside the keyword branch. X5 (placement: `packages/core`, Documents in app) and X6 (extend `UserRole`) resolved. Recon findings folded in: versioned S3 download already exists; `LLMPhraseExtractionMetadata` already carries per-node prompt/catalog versions so `RunProvenance` captures rather than re-lists; `packages/core` cannot import the app, so kernel primitives are field-type agnostic; the model-side structured-evidence check was removed 2026-08-11, so K2 is new code and the specs' "same standard as the model" claim is stale. I1 analysis written. Next: K1.1. |
