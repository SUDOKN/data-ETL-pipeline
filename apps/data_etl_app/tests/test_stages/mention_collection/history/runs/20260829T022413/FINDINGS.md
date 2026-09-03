# Mention-stage eval — run `20260829T022413`, Phase A

The first run of the current code. Every earlier baseline in this folder is the
"before" side of the 2026-08-27 rewrite (`bbfc42b` reversed D8; `482b0c0` added
`doc_span`), so nothing here is comparable to it except the six predictions the
previous FINDINGS.md wrote down in advance. All six are now settled.

`fold.collapse_compounds` is `True` in the dump header — the run really is on
the new code. Checked first, per protocol.

## Headline

| | |
|---|---|
| fields | 140 (20 subjects x 7) |
| status | **138 OK, 2 RED**, 0 BLIND |
| mentions | 126,810 (includes the `contract_products` duplicate) |
| distinct snippets | 38,849 |
| groups | 33,690 |
| nested occurrences (the new I1 population) | **34,528** |
| judgeable items | **84,885 — 0 judged** |

Judged coverage is zero: no reader has been through any of it yet. Every
location number in this instrument remains unmeasured.

## The predictions, settled

Compared on `alecmfg_com` + `steelcraft_com` only — the two subjects the old run
covered. Different `config_digest` on each side by construction; these are two
different quantities, and the comparison is legitimate only because it is
measuring exactly the change that made them different.

| # | prediction | old | new | verdict |
|---|---|---|---|---|
| 1 | mentions rise ~34% | 8,540 | 12,943 | **confirmed, understated** (+52%) |
| 2 | empty groups collapse toward zero | 148 | 0 | **confirmed** (6 across all 20) |
| 3 | `nested_occurrences` becomes non-zero | 0 | 4,403 | **confirmed** |
| 4 | `groups_over_50_entries` grows | 2 | 10 | **confirmed** |
| 5 | a `collapsed` status appears | 0 | 18 | **confirmed** (201 across all 20) |
| 6 | `doc_span` on every mention | — | 34,530/34,530 | **confirmed** |

**The number that says the change did what it meant to:** groups went 3,309 ->
3,311 and distinct snippets 3,571 -> 3,571. Same snippets, same groups, 52% more
occurrences attributed inside them. That is the decentralization mechanism
stated in one line — and it is why I1 is now the question this instrument
exists to answer.

Largest group on the full run: **193 entries**, against synthesis's packing
limit of 50, with **149 groups over that line** (was 2 on the old two-subject
run). This is the number handed to the synthesis eval.

## RED 1 — a one-character mention-id mis-echo, and the retry that only half ran

`lucasmilhaupt_com__products` and `lucasmilhaupt_com__contract_products`.
Two REDs, one event.

The model was sent mention id `m78n7d33` and answered `m78n7t33` — **a single
character changed, `d` -> `t`**. The id matched nothing, so the fold recorded an
unknown answer id and left that snippet uncoloured on the first attempt.

The snippet is a repeated sentence, appearing in windows 0 and 3:

> But you can further insure minimum resistance by using a close joint
> clearance, to keep the layer of filler metal as thin as possible.

The mention id is content-derived and behaved correctly — the same sentence in
two windows legitimately shares one id. This is not an id collision.

### The part that matters more than the mis-echo

Same run, same chunk (`48459:138167`), **identical dump timestamp to the
microsecond**:

| | described | not_described | retried | unknown_answer_ids |
|---|---|---|---|---|
| `products` | 685 | 0 | **1** | 1 |
| `contract_products` | 684 | **1** | **0** | 1 |

Both fields saw the mis-echo. **Only `products` retried.** So the three affected
mention rows carry a real location in `products` and `(location not described)`
in `contract_products`.

**This falsifies the shared-identity invariant.** `TAXONOMY.md` and
`check_shared_identity` both assert that `contract_products` is a byte-copy of
`products` through this stage, sharing one physical request. It is — on 19 of 20
subjects. It is not on the retry path, and the retry path is exactly where the
LLM half is least deterministic.

### The tripwire could not see it — now fixed

`check_shared_identity` compared `(mention_id, span, group_id)` and nothing
else. The divergence is entirely in the `location` text, so the check passed
while the two folds genuinely differed. **A tripwire that asserts a byte-copy
must compare the bytes that can differ.** The tuple now carries `location` and
`location_source`, and the gate fires on this run.

## Instrument defect found and fixed before any judging — the work orders sampled

`build_work_orders` enumerated one item per `mention_id`. A mention id is
content-derived, so one id covers a passage **wherever it recurs**, and the
model describes it once PER WINDOW — correctly giving a different description
for each place. Keying by id alone judged the first and discarded the rest.

| | |
|---|---|
| occurrences | 92,280 |
| distinct `mention_id` (what was enumerated) | 28,260 |
| distinct `(mention_id, chunk, window)` = one model claim | **36,441** |
| windows yielding >1 location for one mention | **0** |

**8,181 model claims — 22.5% — never reached a reader**, against a standing
decision that judgment is exhaustive and never sampled.

The same blind spot was in `check_location_content`: the URL / banned-opener /
cross-reference scans are EXACT gates, and they too saw one location per
mention id, so a violation in any other window went undetected.

**The subtler half.** A claim may legitimately cover several occurrences on
several pages — the prompt asks for it ("A passage may occur at more than one
place ... Describe it once, covering where it recurs"). Measured on
ableengineering: one snippet, 13 occurrences, **8 pages, 6 descriptions**, e.g.

> Sentence of prose under the main content area on **both the Boeing and Airbus
> fixed wing replacement parts pages**, in the site's own copy.

The old work order handed a judge that snippet with `occurrences: 13`, ONE page
and ONE description. Judging a recurrence-covering sentence against a single
representative page manufactures a false S3 failure — the exact error class
`goldens/VERIFY_BRIEF.md` was written to prevent. Each item now carries every
occurrence its claim covers (`pages`, `occurrence_doc_spans`, `forms_covered`)
and is annotated `spans_multiple_pages`.

Judgeable items: 76,704 -> **84,885**.

## Corpus state

Unchanged by this run and still gating nothing: all 14,713 occurrence labels are
`candidate`, so `check_occurrences` examined zero labels. The golden comparison
contributed nothing to the statuses above, and will not until the verification
pass promotes labels to `confirmed`.

The difference-reporting fix landed before this pass (`goldens.py`): a form that
search SENT and this stage collected zero times is no longer swallowed into the
"never sent" bucket, and an under-count is now reported instead of being
absorbed by the trimmed-text leniency. Both are non-gating and both are dormant
until labels gate.

## What is still unmeasured

Everything judged. Location correctness (S2 and S3 and S4), snippet extent (S1),
grouping (G1-G4) and inheritance dilution (I1) have no verdicts. The I1
population exists for the first time — 34,528 nested occurrences — and nothing
is known about its dilution rate. This stage's own reproducibility floor is
still unmeasured (`checks/aa_probe.py` unbuilt), so no location number may gate
and no location delta below ~15% relative may be reported as a result.
