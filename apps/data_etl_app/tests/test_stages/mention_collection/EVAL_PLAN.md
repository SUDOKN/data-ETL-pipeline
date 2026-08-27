# Mention-stage evaluation — design plan

**Status: PROPOSED 2026-08-26, revised same day. Nothing below is built.**
Revision: search-stage quality is OUT OF SCOPE — a parallel session owns the
search stage and its own evaluation. This eval takes the sent-forms list as a
given input and judges only what the mention stage does with it. This file is
the plan of record; open questions at the bottom await the user.

---

## 1. Scope — what "the mention stage" means here

The stage under evaluation is everything between "search returned candidate
forms" and "synthesis reads bundles":

1. **Form pooling** — the chunk-wide union of search forms + brute-search
   casings, occurrence-filtered per sub-window (`get_chunk_forms` /
   `forms_occurring_in_window`). The pooling *mechanism* is mention-owned;
   the forms' *content* is not.
2. **Mechanical collection** — the pure-code scan
   (`floor_scan` / `collect_window`): whole-word matching, longest-span
   containment, sentence/line clipping, casing rescue.
3. **The LLM Location stage** — one location description per distinct snippet,
   plus the retry pass (`llm_phrase_mention_collection` requests).
4. **The aggregation fold** — grouping by `normalize()`, `group_id` minting,
   the dump's `fold` block (`fold_document` / `build_fold_dump`).

**The scope contract:** given the sent forms, did the stage (a) collect every
occurrence, exactly and only at whole-word spans, (b) clip sense-preserving
snippets, (c) describe each snippet's location correctly, (d) group spellings
correctly, and (e) deliver everything it was asked, at what cost? Whether the
*right forms* were sent is the search eval's question. Synthesis and
everything downstream are out.

**Explicitly handed to the search-stage eval** (computed here where the dump
carries them, exported, never judged or gated here): form-quality judgments
(the V/B/P/M/G/C/S/U rubric), window recall probes, the *interpretation* of
`zero_hit_forms` and paraphrase-class empty groups (search claimed a phrase
the text lacks), search drift/consistency metrics, junk-form families
(document titles, wrong-field forms, ≥6-word fragments), and per-field
contamination shares (e.g. steelcraft equipments 68/75 product-shaped groups).

**Per-field independence:** every layer is parameterized by (subject, field).
A failure in one field never blocks another field's numbers. `products` and
`contract_products` share one mention identity by construction — the eval
treats `products` as canonical and asserts the two dumps' mention data are
byte-identical (that assertion is itself a Layer-0 invariant).

**Upstream-drift guard (comparison discipline):** mention metrics are only
trended on *paired* identities — mentions matched by `mention_id`, requests by
`ud=` digest, groups by `group_id` — the method the 012721→020729 A/B proved.
When search output changed between runs, unpaired deltas are attributed
upstream and reported as context, never as mention-stage change. Without this,
search drift (measured ~45–55% phrase-set churn under geometry/prompt change)
would masquerade as mention-stage movement.

## 2. Prerequisites (code changes owed before the eval can run on full runs)

1. **Carry the `fold` block into full-run dumps.** Today only `stop_after`
   partial dumps call `build_fold_dump` (`partial_run_dump.py`); the full-run
   writer (`extraction_dump_util.py`) emits the group spine only. Verified on
   `20260825T194457`: zero `fold` blocks in all 14 files. Coordination note:
   the dump writer is shared code and a parallel session works the search
   stage — exact-anchor edits only, per the shared-tree precedent.
2. *(Nice-to-have)* a sha256 of the **trimmed** subject text in the dump
   header, so offline re-scans can prove they hold the right text.

## 3. Folder layout (this directory)

```
tests/test_stages/mention_collection/
  EVAL_PLAN.md            ← this file
  README.md               ← charter + how to run (written at build time)
  AGENT_PROTOCOL.md       ← the judgment-pass playbook (census method)
  taxonomy.md             ← defect codes: M- mention/snippet, L- location,
                            G- grouping, D- delivery axes (no form axis — search's)
  expectations/           ← per (subject, field) anchor files, evolving
  checks/                 ← python library (ported from pipeline_v3_evidence)
    dump_reader.py  invariants.py  metrics.py  anchors.py
    ledger.py  report.py  run_eval.py (CLI: layers 0+1+3)
  test_mechanical_golden.py   ← offline golden tests (normal suite)
  test_run_invariants.py      ← Layer-0 over a real run (marker: integration,
                                driven by MENTION_EVAL_RUN_ID)
  ledger/metrics.jsonl    ← append-only, git-tracked
  reports/<run_id>.md     ← generated per eval, git-tracked
  reports/<run_id>_judgments/  ← raw agent verdicts, re-derivable numbers
```

Suggested sibling for the parallel effort: `tests/test_stages/phrase_search/`
(so the two evals never collide in one directory).

## 4. The four layers

### Layer 0 — deterministic invariants (pytest, hard gates)

Everything checkable from the dump alone. Seeded from the fixed-flaw register
(§6.1) so every past fix has a tripwire:

- Delivery: `not_described == 0` after retry; `unknown_answer_ids == 0`
  (nonce-shaped ids their own class); `windows_with_undescribed`, `retried`
  tracked.
- Scan integrity: every mention's `form` occurs whole-word in its own
  `snippet`; spans inside window bounds; same snippet ⇒ same `mention_id`;
  no duplicate mentions at one absolute span.
- Bookkeeping: `zero_hit_forms` and empty groups **computed and exported**
  (search's numbers to interpret); the only gated part is that every empty
  group is *classifiable* into the six known benign causes — an
  unclassifiable empty means mention-stage bookkeeping broke.
- Location text: URL-bearing == 0; banned openers == 0; cross-references == 0;
  empty locations == 0; `location_source` accounting.
- Page exclusion (observable effect): mentions on legal URLs == 0;
  `windows_with_excluded_pages` tracked.
- Identity: `pv=` in every custom_id matches `extraction_metadata`;
  products == contract_products byte-identity; config digest extracted
  (pv per stage, chunk geometry, normalizer_version, verb_fold,
  snippet_radius, page_exclusion_version) for like-for-like trending.
- Deep verify (optional, when Mongo holds the run): re-run `collect_window`
  on the wire texts; recomputed mention set == the dump's, exactly —
  including code-derived `page` correctness.

### Layer 0b — offline mechanical golden corpus (normal suite, no run needed)

Frozen (text excerpt, form list) fixtures → `collect_window`/`fold_document`
→ golden outputs, for all 8 sample subjects. Grows deliberately with
adversarial fixtures: tables, testimonials, headings, possessives (`'s`),
TM/(R) marks, hyphenation/line breaks, Spanish text (exists in production),
short forms, page-barrier and continued-page cases, snippet_radius > 0.
This is the LLM-free half of "continuous expansion", and how the 6 subjects
with no run data participate immediately.

### Layer 1 — expectation checks (the evolving per-field eval set)

`expectations/<subject>__<field>.yaml`, each record with provenance
(run, judge, date) and status (active / retired + reason):

- `occurrence_anchors`: given form F is among the sent forms for subject S,
  its group must show exactly/at-least K mentions with expected pages —
  conditional on the form being sent, so search drift cannot fire them
  falsely. (Deterministic once conditioned; near-Layer-0 strength.)
- `location_anchors`: specific snippets whose correct location attribution is
  known and load-bearing — the "More from Allegion" parent-company class,
  customer testimonials, table cells in delivered-project write-ups. The
  party-attribution regression tests that style metrics cannot see.
- `grouping_anchors`: form pairs that must (or must never) share a group —
  the wrong-merge tripwire made concrete as cases accumulate.
- `watch`: known polyseme groups (`lead`, `production`, `part`) whose
  occurrence loads are tracked, never judged — sense burden is screening's
  by design (D14); the count is context for downstream evals.
- `deltas_pending`: judged verdicts awaiting user adjudication.

Anchor misses report for adjudication; deterministic violations
(a location anchor asserting a party name, a grouping ban) can hard-fail.

### Layer 2 — agent judgment passes (the not-regex part)

Run by Claude spawning as many agents as the dump warrants, under
`AGENT_PROTOCOL.md`. Ground rules from the hand census: **judgments by
reading, regex only enumerates**; one shared taxonomy so counts merge; the
lead session re-verifies headline claims against the dumps; explanation text
is never trusted.

Standing passes, all mention-owned:

- **Location-correctness sweep** (the centerpiece — never measured; only
  style has been): stratified by snippet class (bare form / heading /
  testimonial / third-party section / table cell / prose / list entry),
  each location judged on the three mandated elements: kind-of-text right?
  belongs-to right? whose-words right? Plus a boilerplate-informativeness
  verdict: is "in the site's own copy" (99.5% of the attribution slot)
  *correct* boilerplate or *wrong* (a testimonial or parent-company section
  labeled as site copy)?
- **Snippet-extent judgment**: does the sentence/line clip preserve enough
  sense for synthesis (the r=0 adequacy question), and does it ever cut
  mid-sense or bleed across page barriers?
- **Grouping eyeball**: every multi-form group whose members differ beyond
  case/plural/verb-fold — the wrong-merge probe (0 found to date; the dump's
  `forms` list exists precisely to make this visible at a glance).
- **Containment-attribution sample**: occurrences owned by a longer form —
  is the longer owner the right reading at that span?
- **Empty-group classification audit**: verify the Layer-0 six-class
  assignment on a sample (the *causes* that are search defects are exported
  to the search eval, not judged here).
- **Latent-flaw probe rotation** (§6.4): 1–2 per eval.

Judgments are keyed to (subject, field, group key or mention_id +
content digest), so verdicts carry forward across runs when content is
unchanged — replayed stages cost zero re-judging. New defect classes get a
taxonomy code + counting rule; that plus anchor growth and golden-corpus
growth is how the eval set evolves.

### Layer 3 — ledger + trend report

`ledger/metrics.jsonl`: one row per (run, subject, field, metric) with config
digest, value, n, gate type, outcome. Gate types: **TRIPWIRE** (exact,
mechanical facts only), **BAND** (warn — anything LLM-authored; calibrated to
*measured* variation only — the Location stage's own A/A churn is unmeasured
and is an early probe), **TREND** (report-only). Report generator emits
`reports/<run_id>.md` per field: values, delta vs previous like-config run
(paired identities only, per §1), delta vs baseline, gate outcomes, new
taxonomy codes, proposed expectation updates.

## 5. Baselines to seed at build time

From `20260824T020729` (verified byte-exact mention stage of full run
`20260825T194457`):

| metric | baseline |
|---|---|
| mentions (run total) | 6,085; snippets 3,345; described 3,345/3,345 |
| not_described / retried / unknown ids | 0 / 0 / 0 |
| zero-hit forms / empty groups (exported) | 0 / 115, all containment-class |
| banned openers / URL-bearing / cross-refs | 0% / 0% / 0 |
| median location length | 154 ch (p90 224); location share of payload 61.1% |
| "whose words" slot filled | 100% — 99.5% the boilerplate "site's own copy" (informativeness unjudged) |
| party carry-through (bounds-keyed pairing) | 66%; 7 mild (Falcon) losses, 0 Allegion-class |
| thickness (context, upstream-shaped) | mean 2.27 mentions/group, 67.1% single-mention |
| provenance mix (pooling/rescue bookkeeping) | llm_round_1 3,216 / brute 8 / unmatched 85 |
| casing-rescued mentions (prior run) | 722 (11.9%) |
| stage cost | 942,984 in / 234,723 out tokens ≈ $3.76 (top cost stage) |
| window re-send share of stage input | ~26% |
| latency | client p50 7.9 s, p90 15.6 s |

Location correctness has NO baseline — the first Layer-2 sweep creates it.

## 6. The flaw register (what the numbers track)

### 6.1 Improved — each a Layer-0 tripwire

| flaw (was) | tripwire |
|---|---|
| LLM collector satisficing (19% occurrences lost) | mechanical scan; deep-verify recompute == dump |
| under-answer collapse (0/47 described) | not_described == 0 after retry |
| nonce echoed as mention_id | nonce-shaped unknown ids == 0 |
| URLs in locations (39–83%) | URL-bearing == 0 |
| boilerplate openers (92.7%) | banned openers == 0 |
| cross-mention references (2.7%) | == 0 |
| legal-page junk (80k tokens wasted) | legal-URL mentions == 0 |
| overlap duplicate mentions (297) | duplicate absolute spans == 0 |
| word-boundary hits (`Steel` in `Steelcraft`) | form whole-word in own snippet, 100% |
| heading paraphrase losing party names | location anchors (Allegion-class == 0) |

### 6.2 Remaining — tracked with direction (mention-owned)

Location verbosity (154 ch median, 61.1% of payload, ↓ wanted); attribution
slot boilerplate (99.5% — informativeness to be judged, then banded);
chunk-boundary pooling gap (`metal` vs `sheet metal` attribution
inconsistency — pooling scope is a mention-stage design property); window
re-send cost (26% of stage input tokens — packing lever unbuilt); Location
retry singleness (one retry pass; whatever it misses is defaulted); no
truncation salvage on Location responses (a `max_completion_tokens` cut fails
the whole group). Context rows, upstream-shaped, reported not owned: evidence
thickness (67% single-mention — drives 51% twin divergence downstream),
polyseme load, coverage cap (max_chunks=2 — % of document seen).

### 6.3 Emerged — the mechanism

Every eval's Layer-2 sweep can mint new taxonomy codes; the report counts
codes added per run. Past emergent classes (nonce echo, heading paraphrase)
surfaced by reading output — the sweep stays reading-based.

### 6.4 Latent — probe rotation

Wrong merges in `normalize()` (0 so far); containment misattribution;
excluded-page false positives (URL rule dropping a real content page —
observable here as missing mentions; shared infra, flagged not owned);
mention_id/group_id hash collisions (cheap duplicate check); short-form
casing silent declines (`Abs` vs `ABS`); Location-stage A/A churn (unmeasured
— needed before location bands can gate); snippet_radius > 0 never exercised;
continued-page-header rare path; Location behavior on windows opening
mid-page.

## 7. Workflow per full pipeline run

1. User runs the pipeline (notebook) and gives the run id + arm mapping.
2. `checks/run_eval.py <run_id>` → Layers 0, 1, 3 → ledger rows + report
   skeleton. (Minutes, no LLM.)
3. Claude spawns Layer-2 agents per `AGENT_PROTOCOL.md`, sized to the paired
   delta; verdicts merged, headline claims re-verified by the lead session.
4. Report finalized: per-field outcomes in chat + `reports/<run_id>.md`.
5. Expectation/taxonomy updates applied; disputed verdicts queued for the
   user; retired anchors keep their reason.

## 8. Corpus expansion to all 8 subjects

Only alecmfg + steelcraft have v3 mention data. The other six enter via one
`stop_after(PipelineStage.mention_collection)` run each (user-driven),
followed by a seeding pass that writes their first expectation files.
acimachine (4 MB) is capped by max_chunks=2 to ~40k tokens of coverage —
recorded, not fought.

## 9. Decisions (user, 2026-08-26 — do not re-litigate)

- **Gate posture**: tripwires hard-fail, judged metrics banded. ACCEPTED.
- **Judgment budget**: **census-scale FULL REREAD every run** — the user's
  explicit choice over paired-delta sampling. Every eval reads all groups,
  snippets, and locations. Carried-forward verdicts are kept only as a
  judge-stability cross-check, never as a substitute for reading.
- **Mongo**: the harness may read Mongo (wire texts, deep verify). Other
  agents are also building Mongo utilities — coordinate/swap when a shared
  util lands.
- **Dump exhaustiveness is owned elsewhere**: the user has other agents/tasks
  making the dump exhaustive (which covers the fold-block-in-full-dump gap).
  This harness does NOT build that fix; until it lands, the reader falls back
  to a paired `stop_after(mention_collection)` partial dump with matching
  mention identity (pv + ud), and it validates the fold schema version on
  every load in case the dump shape changes under it.
- **Six missing subjects**: the user authorized Claude to drive the seeding
  runs directly (an explicit exception to the runs-via-user-notebook rule for
  this task; the prompt-publish racing hazard with parallel agents still
  applies — verify pinned prompt state immediately before dispatch).
- **Handoff**: agreed — a boundary-metrics note for the search-stage eval
  will be written (kept in this folder; the user relays the pointer).

Remaining open (asked 2026-08-26): sequencing/execution details of the six
seeding runs; whether byte-identical mention replays also get the full reread
or a reduced judge-stability sample; own minimal Mongo reader now vs waiting
for the shared util; commit policy for the built harness.
