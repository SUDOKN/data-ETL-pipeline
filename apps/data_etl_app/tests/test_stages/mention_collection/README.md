# Mention-stage evaluation

> **STALE — target stage retired 2026-09-03.** The mention-collection LLM
> stage this instrument judged was merged into the synthesis stage (the
> location-stage merge): mention collection is pure code in the aggregation
> fold, and the location task became per-entry verbatim CONTEXT QUOTES on the
> synthesis wire, audited mechanically by `core/utils/context_check.py` (the
> fabrication rate rides the synthesis dump's `context_check` block). The
> location-quality judgments here have no live stage to run against; the
> golden corpus and taxonomy are kept as history. Location-register quality
> is now the **synthesis eval's** business.

The permanent, evolving evaluation of the **mention-collection stage** — a
sibling of `search/`, `grounding/` and `synthesis/`, following their layout and
protocol vocabulary. Trigger: the user says **"run the mention eval"** → follow
`RUNBOOK.md`.

## What this stage is, and what this instrument judges

Between search and synthesis the pipeline does four things. Given the candidate
phrases search produced, plain code finds **every place each one occurs**
(`floor_scan` / `collect_window`), clips each occurrence to the sentence or line
holding it (a **snippet**), asks a model to describe **where that passage sits**
(the **location**), and buckets forms whose spelling normalizes alike into
**groups** — the records everything downstream reads (`fold_document`).

So the contract judged here is: *given the forms it was handed*, did the stage

1. find every occurrence, at the right spans, and no phantom ones,
2. clip snippets that carry enough sense to be evidence,
3. describe each snippet's context correctly,
4. group spellings correctly, and
5. deliver every answer it was asked for — at what cost?

**Out of scope: whether the right forms were sent.** That is the search eval's
question (`search/`), and its metric battery already owns fabrication, casing
splits, window consistency and brute health. Numbers this instrument computes
that belong to that question — zero-hit forms, empty groups, junk-form
composition — are **exported, never judged or gated here** (`BOUNDARY.md`).
Synthesis and the grounding tail are downstream and equally out of scope.

## Why a golden corpus, and why it needs no pipeline runs

The mechanical half of this stage is **deterministic**: given a text and a form,
where that form occurs is a fact, not an opinion. So most of this stage's ground
truth can be written down **from the texts alone** — no run required. That is
what `goldens/` holds, and it is why this instrument can cover all 20 corpus
subjects while only two of them have ever been through a v3 run.

Labels are anchored the way `search/` anchors its entities: a verbatim quote plus
an advisory offset, against a sha256-pinned text snapshot in
`../sample_scraped_texts/`. Offsets are advisory because the pipeline reads a
*normalized, page-trimmed* copy of the text; quotes survive that, offsets do not.

## The five label kinds

| kind | question it answers | how it is produced |
|---|---|---|
| **occurrence** | every span where a form really appears | computed, then sampled by a reader |
| **snippet** | what passage should carry that occurrence, and is a sentence/line enough | judged |
| **location** | what kind of text, what it belongs to, **whose words** | judged — the never-measured one |
| **grouping** | which forms are one thing and which are not | judged |
| **inheritance** | when a short form matches inside a longer one, is crediting it sound | judged (new; see below) |

## Decentralization — the stage's newest and least-understood behaviour

Until 2026-08-27 a short phrase found inside a longer one was suppressed:
in *"Our steel doors and frames set the industry standard"*, only
`doors and frames` was credited, and `doors` was filed as searched-but-never-
found. `bbfc42b` **reversed** that (D8): the sentence is now an occurrence of
`doors and frames`, of `doors` **and** of `frames`, each its own record.

The reason is strong — containment was hiding **34% of all occurrences** and was
the sole cause of every empty record in the corpus (`ISO 9001` looked missing
because the page writes `ISO 9001:2015`). The cost is that a generic word now
inherits the evidence of every specific thing containing it: the pipeline's own
dump comment records `door` going **60 → 157 entries**. Whether each inherited
occurrence is sound evidence for the generic record is a judgment nothing has
ever made, and it is cheap to enumerate (any occurrence whose matched span is
longer than the form). Hence the **inheritance** label kind.

## Gating

**TRIPWIRE** (exact, hard-fail) only on mechanical facts — delivery, span
integrity, banned location content, identity. **BAND** (warn) on anything a
model authored, calibrated to *measured* variation; the Location stage's own
reproducibility floor is unmeasured, so `checks/aa_probe.py` measures it before
any location band gates. **TREND** (report-only) for cost, shape and volume.

## Layout

```
README.md              this file — charter + STATE
RUNBOOK.md             the protocol followed on "run the mention eval"
TAXONOMY.md            the judgment contract; its hash is taxonomy_version
GOLDEN_LABELS_SCHEMA.md  what a golden label is and how it is promoted
BOUNDARY.md            metrics computed here but owned by the search eval
EVAL_PLAN.md           the design record (superseded in part; read its REVISION)
config/common.yaml     thresholds, prices, enforcement flags, noise inputs
goldens/<slug>/        golden labels: subject.yaml + one file per field
checks/                deterministic code (see checks/README or module docs)
history/               per-run results; metrics.jsonl is the cross-run trend
tests/                 the harness's own tests (they guard this code, not the pipeline)
```

Per-run data under `history/**` is gitignored by the shared rule; the trend file
and per-run `*.md` analyses are the tracked exceptions. `goldens/` **is** the
instrument and is tracked.

## The document set

| file | what it is |
|---|---|
| `RUNBOOK.md` | the protocol followed on "run the mention eval" |
| `TAXONOMY.md` | the judgment contract; its sha256 is `taxonomy_version` |
| `METRICS.md` | every number reported, what flaw it tracks, its baseline |
| `GOLDEN_LABELS_SCHEMA.md` | what a golden label is; the promotion rule |
| `goldens/SEEDING_BRIEF.md` | the procedure handed to seeding agents |
| `goldens/VERIFY_BRIEF.md` | the second pass that promotes a label |
| `goldens/VERIFICATION_LEDGER.md` | corpus state, per subject, with corrections |
| `BOUNDARY.md` | what is exported to the sibling evals, and what they are owed |
| `CANDIDATE_DIMENSIONS.md` | defect classes staged before entering the taxonomy |
| `EVAL_PLAN.md` | the design record (read its REVISION block first) |

## STATE

**2026-08-27 — instrument BUILT and exercised; golden corpus SEEDED; nothing
judged yet.**

- **Instrument:** 255 tests green across all four stage harnesses together,
  pyright 0, ruff clean. `checks/run_eval.py` runs end-to-end on
  `20260824T020729`: **14 of 14 fields OK, no tripwire fired**, 4,828 judgeable
  items enumerated into work orders. See
  `history/runs/20260824T020729/FINDINGS.md`.
- **Corpus:** 20 subjects, 140 field files, **14,713 occurrence labels covering
  67,592 occurrences**, 25,264 quotes all re-verified verbatim against their
  pinned snapshots, 253 must-not-occur labels, 171 substring traps, plus 142
  adversarial negatives. `checks/validate_goldens.py --quotes` reports 0 errors.
  **All labels are `candidate`, so the corpus gates nothing yet** — the second
  pass in `goldens/VERIFY_BRIEF.md` is the next work.
- **Not measured by anyone, ever:** location correctness (S2∧S3∧S4), snippet
  extent (S1), grouping (G1–G4), inheritance dilution (I1), and this stage's own
  reproducibility floor (`checks/aa_probe.py` is unbuilt).

⚠ **The stage was rewritten the same day this instrument was built**
(`bbfc42b`: D8 reversed, D21 `collapsed`; `482b0c0`: `doc_span`, the fold stored
in `extraction_runs`). **No run exists on the current code.** Every baseline
here is the "before" side, `nested_occurrences` is 0 by construction, and the
first run of the new code should confirm five specific predictions listed at the
end of `history/runs/20260824T020729/FINDINGS.md`. Check
`fold.collapse_compounds` in the dump header before reading anything else.
