# Ninth v3 run analyzed: 20260824T012721 — the mention-stage Location rewrite

The six `3_phrase_mention_collection` statics were published and re-run. The Task now asks for **one
compact sentence carrying three things** — (a) what kind of text it is, (b) what it belongs to, (c) whose
words they are — plus an explicit ban on opening with a reference to the mention itself ("This passage",
"This sentence", …) or with "appears / is found / is located / sits". Same two subjects, `loc=1`, 50
entries per request, `gpt-4.1`, temperature 0, seed 12345.

Because the mention `pv=` changed, the `ud=` digest of every synthesis request changed too, so **both
stages re-ran** — this is not a synthesis-only A/B like the previous three.

Scripts: `location_rewrite_ab.py` / `location_rewrite_ab_output.txt`, run from the repo root.

## Read this first: the run is 17 of 20 dumps

**Three fields produced no dump at all** — `alecmfg/industries`, `steelcraft/products` and
`steelcraft/contract_products` (which dies with `products`, whose synthesis request it shares). That is
**773 records and 2,192 mentions, 36% of the record count** of the complete run `20260824T010654`.

Neither loss is caused by the Location rewrite. Both trace to the **failed run 45 minutes earlier**
(`20260824T012354`), and to two defects that turn its failure into a permanent one. See *Two defects* below.

Every number in this README is computed on the **17 fields both runs share**, with one version of the code
on both sides, so the comparison itself is sound. The pairing is exact: 1,872 mentions matched by
`mention_id`, all with identical spans and identical snippets; 1,246 synthesis records matched by
`group_id`, all with identical focal forms. The fold did not move.

## Verdict in one line
**The rewrite did what it was asked to do — the opener is gone (94% → 0%), attribution is universal, and
nothing else regressed — but it is 25% shorter, not the 35–40% hoped for, and it paraphrases away a
section heading that names a third party, which is the one thing the previous session fixed.**

## RIGHT

1. **The opener ban is total: 92.7% → 0.0%.** Not one of the 1,872 locations opens with a
   self-reference, and not one opens with a banned verb in its first four words. The old run's opening
   two words were `this passage` (1,226), `this sentence` (863), `this phrase` (284), `this line` (171);
   the new run's are `sentence of` (501), `bullet point` (464), `entry in` (99), `table cell` (70) — the
   kind of text, exactly as asked.

2. **"Whose words" is now carried everywhere: 1,869 of 1,872.** Measured with one regex on both runs,
   the old locations carried an explicit attribution in **12%** of cases and the new ones in **100%**.
   (The plan's recorded baseline of 34% came from code that was not kept; 12% is what the shipped
   measurement gives on the same dumps. Either way the direction and the size of the move are not in
   doubt.)

3. **Every rule that had to survive, survived.** 0 URLs (the address ban holds), 0 cross-references to
   other mentions (down from 1), 0 empty locations, and the one-sentence ask is honoured — the median
   location has 1 sentence-ender and the maximum is 2, against a max of 4 and 35 multi-sentence
   locations before.

4. **Delivery is clean on both stages.** Mention: 3,893 of 3,893 entries have a usable location, 0
   without (the old run had 1). Synthesis: 1,360 of 1,360 records synthesized, 0 not-synthesized, 0
   retries, 0 unknown ids. The `ud=` checks land exactly as the design predicts — all 93 shared mention
   requests have an **identical** `ud=` (the stage reads the same windows and forms; only `pv=` moved),
   and all 61 shared synthesis requests have a **changed** `ud=` (the locations they digest are what
   changed).

5. **The focal-form lint flags 0 in both runs**, on 700 and 385 entity-shaped records. No entity was
   renamed.

6. **Cost fell.** Mention $2.86 → $1.89 and synthesis $2.43 → $1.53 — but those totals are not
   comparable, because the new run is missing 36% of the records. Per paired mention the input is
   genuinely smaller; see below.

## WRONG

1. **The saving is 25%, not the 35–40% the plan targeted.** On the 1,872 paired mentions, location text
   goes **384,517 → 289,956 characters, −24.6%**. Median length 192 → 152, p90 258 → 202, max 487 → 337.
   Locations are still **59.9%** of the (location + snippet) payload, down from 66.0%. Killing a
   boilerplate opener that was 27% of the text while *adding* a third mandated element nets out to
   roughly a quarter — which in hindsight is the arithmetic one should have expected.

2. **The attribution slot is spent almost entirely on boilerplate.** Of the 1,872 new locations, **1,857
   say "in the site's own copy"** and 49 mention a customer. The element is being satisfied, but it is
   carrying almost no information — it distinguishes nothing when 99% of a manufacturer's website is the
   manufacturer's own copy.

3. **THE REAL REGRESSION — the rewrite paraphrases away a heading that names a third party.** Party
   survival into the synthesis went 60% → 50%, and 7 records whose location named a party (Allegion,
   Falcon) no longer do. Read individually, five of the seven are harmless and two are not:

   | records | what changed | verdict |
   |---|---|---|
   | 2 (`steel doors and frames`, `Factory-applied … primer`) | the old location enumerated several pages, one of them "SZ Series Falcon Flush Doors"; the new one names a single page | **harmless** — page enumeration, not attribution; Falcon is a Steelcraft sub-brand |
   | 3 (`LEED rating system`, `Declare Labels`, `Living Building Challenge`) | the old location **restated the content** ("explaining Allegion's provision of Declare labels"); the new one describes location only | **correct** — the static says *"Describe location only. Do not judge, summarize, or restate."* The old locations were breaking that rule. 2 of the 3 syntheses still name Allegion, read off the snippet |
   | **2 (`LEED Credits`, `CalGreen Building Standards`)** | old: *"a line under the **'More from Allegion'** section"*; new: *"a line in a list of links or resources at the end of the sustainability section"* | **REGRESSION** |

   The two in the last row are **the exact pair the previous session identified as the only genuine
   attribution defects and fixed** (run `002404`: both records were made to name Steelcraft as publisher
   and surface the "More from Allegion" provenance). The rewrite has undone that fix — not in synthesis,
   which is unchanged, but upstream in the location, which no longer contains the provenance for
   synthesis to preserve. The new syntheses now read *"This shows that Steelcraft offers…"*.

   **The cause is a specific instruction.** The static asks for *"the section or heading it falls under
   … named in your own words"*. Applied to a heading that **is** the provenance — "More from Allegion" —
   paraphrasing destroys it. The model quotes headings verbatim elsewhere ('Features and Benefits',
   'Steelcraft Declare Labels'), so this is the paraphrase instruction firing inconsistently, not a
   capability limit.

   **Proposed fix (not built):** tell the static to give a section or heading name **verbatim, in
   quotes**, and to paraphrase only the *page subject*. That keeps the compaction (the page subject is
   the long part) and restores the provenance.

## Two defects — why three fields have no dump

The run 45 minutes earlier, `20260824T012354`, failed on both subjects: `steelcraft` on a parse error and
`alecmfg` on a `429 — no credits remaining` part-way through `industries`. Run `012721` was the retry, and
it inherited the wreckage.

### Defect A — a duplicate `record_id` in a synthesis answer kills the whole field

`packages/core/src/core/models/extraction_schemas/synthesis.py:142` raises `ValueError` when the model
answers the same `record_id` twice. It fired on
`steelcraft.com>products>llm_phrase_synthesis>chunk>91562:158790>group>0` with
`Duplicate record_id 'gn8gmdzc'`, killing `products` **and** `contract_products`.

This contradicts the hold's own contract, written in the docstring of `parse_synthesis_group_result`:
*"Held EXACTLY: unknown ids are dropped and reported, missing ids are simply absent."* A duplicated id is
the same class of model misbehaviour as an unknown id — it should be dropped and reported, not fatal.
The error is recorded on the request in Mongo (`response_parse_errors`) and **the response is not stored**,
which is what hands the problem to Defect B.

### Defect B — a stored request with no response is never re-asked, and the field dies in silence

`get_missing_req_ids` (`base_llm_extraction_node.py:148`) asks only whether a request *document exists* —
its own inline comment says `# maybe complete maybe not`. `are_all_requests_complete` asks whether it has
a *response*. So a row that exists **without** a response is never "missing", is never re-created, and is
never re-dispatched — while the completeness check stays False forever.

In `base_llm_recursive_extraction_node.execute` the consequence is:

```
missing_req_ids == ∅            → break out of the while loop
are_all_requests_complete()     → False
                                → the `if` body is skipped
                                → the method returns              # file ends at line 191; there is no else
```

**No exception, no log, no dump, and `next_node` is never called.** The field vanishes and the sweep
reports success.

This is what stranded both remaining fields, and Mongo shows it exactly — no new request documents were
created at `01:27` for either:

| field | rows left without a response by run `012354` | cause |
|---|---|---|
| `steelcraft/products` | 1 of 25 (`chunk>91562:158790>group>0`) | Defect A |
| `alecmfg/industries` | 9 of 9 | the `429 — no credits` |

Both are **deterministic on every future run** until the rows are cleared, because the ids are content-derived
and regenerate identically.

## What this run does NOT tell us

- Nothing about `alecmfg/industries` or `steelcraft/products` — 36% of the records, and `products` is the
  field where third-party brand names are densest, so the attribution finding above rests on the
  attestations and equipments records alone.
- The `loc=1` vs `loc=0` A/B is still unrun. Every run so far is the same arm.
