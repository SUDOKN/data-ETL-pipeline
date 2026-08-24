# Tenth v3 run analyzed: 20260824T020729 — the heading-verbatim fix

The six `3_phrase_mention_collection` statics were published (02:03:41Z) with one change to the
*What it belongs to* bullet: the section or heading is now **quoted exactly as the text writes it**, and
only the page subject is paraphrased — *"Never paraphrase a heading: it can name a party or a source that
nothing else on the page names."* Same two subjects, `loc=1`, 50 entries per request, `gpt-4.1`,
temperature 0, seed 12345.

Scripts: `heading_verbatim_ab.py` / `heading_verbatim_ab_output.txt`, run from the repo root.

## This is a clean A/B, despite a delete

The stored batch requests were deleted before this run, which threatened to confound it — a re-run of
search would have changed the phrase sets the mention stage digests. **It did not happen.** The delete was
scoped and left search untouched:

- all 96 `llm_search` and 6 single-stage request rows survive in Mongo from `2026-08-23 04:45`, answered;
- the run's `llm_phrase_search` stage carries `started_at = 2026-08-23T04:45:00` against the mention and
  synthesis stages' `02:07:29` — it replayed, it did not re-run;
- **all 37 shared mention `ud=` digests are identical** to `20260824T012721`, and all 20 shared synthesis
  `ud=` changed (the locations moved). Only `pv=` differs on the mention stage.

So every difference below is the prompt. **20 of 20 dumps, field set identical to the last complete run.**
The three fields lost by `012721` are back.

## Verdict in one line
**The fix landed on exactly what it targeted and cost almost nothing — the two regressed records have
their provenance back, no party name is lost anywhere, and the price is +6.9% location text — but the
FE→DE entity swap recurred, which retires the previous session's "REPAIRED" verdict on it.**

## RIGHT

1. **The target records are fixed.** Both records read the heading again, and the synthesis follows:

   | | `012721` | `020729` |
   |---|---|---|
   | location | "Line in **a list of links or resources** at the end of the sustainability section…" | "Line in the **'More from Allegion'** section at the end of the sustainability page…" |
   | `LEED Credits` synthesis | "…This shows that Steelcraft offers a document or resource…" | "…Steelcraft, **in the 'More from Allegion' section**…, lists 'LEED Credits' as a resource…" |
   | `CalGreen Building Standards` | same unscoped phrasing | same repair |

2. **Party survival recovered and nothing was lost.** Of the 1,246 shared records, those whose location
   names a party went 18 → 20, and those carrying it into the synthesis went **9 → 12 (50% → 60%)**.
   **Records that lost a party name: 7 → 0.**

3. **Nothing the earlier rewrite won was given back.** The opener ban still reads **0.0%** of 2,805
   locations (no self-reference, no banned verb); "whose words" is still carried by **100%**; 0 URLs,
   0 cross-references, 0 empty locations. The one-sentence discipline is fractionally looser — 2 of 2,805
   locations now have more than two sentence-enders, against 0 before.

4. **Delivery is clean and cost is as forecast.** 2,133/2,133 records synthesized, 0 not-synthesized,
   0 retries, 0 unknown ids. Mention $2.81 + synthesis $2.44 = **$5.25**, against the ~$5.30 estimate.

## WRONG

1. **The price of quoting headings is +6.9%.** On the 1,872 paired mentions, location text goes
   289,956 → **309,901 characters**; median 152 → 154, p90 202 → 224. Locations are 61.1% of the
   (location + snippet) payload, up from 59.9%. Cheap for the provenance it buys, but it is a real
   give-back against the rewrite's −24.6%.

2. **THE FE→DE ENTITY SWAP IS BACK, and the heading fix did not cause it.** The focal-form lint flags
   1 record in 700 (was 0 in 385): `steelcraft/equipments`, focal form and sole snippet both
   `FE Series Double-Egress Frames`, synthesis says **`DE Series Double-Egress Frames`**.

   A sweep of every steelcraft record whose focal form names an `<X> Series`, counting the focal form,
   the forms, the snippets **and the locations** as legitimate evidence, gives **0 of 65** in `012721`
   and **1 genuine of 182** here. (A first pass reported 9 and 19; it scanned snippets but not locations,
   and a neighbouring series legitimately appears there — `DW & K Series drywall frames`. Four further
   `020729` hits are regex artefacts capturing the preceding word: "these Series", "or … Series",
   "frame Series". Do not repeat either mistake.)

   **The heading fix is not the cause.** The FE and DE records are twins in the same request, and their
   locations are **byte-identical to each other in BOTH runs** — before the fix and after it. The location
   is not the variable; the model simply collapses the two, and in `020729` it emitted the *same synthesis
   string* for both records.

   **This retires a verdict.** The plan recorded the swap as **REPAIRED** by run `002404`. Across five
   observations of the same record it is: `195031` right, `200044` wrong, `002404` right, `012721` right,
   `020729` **wrong**. Two in five. It is **stochastic and unfixed**, and the earlier "repaired" reading
   was a single lucky sample. The lint is what caught it, both times.

## What this run does NOT tell us

- **Nothing about the recursive resume fix.** The publish minted new ids, so the 10 stranded rows were
  never consulted. Its verification remains the 5 tests in `test_recursive_node_eager_dispatch.py`.
- **`products` and `industries` are descriptive only.** They had no `012721` baseline, so for them this
  run spans two prompt changes at once. Every number above is computed on records present in both runs.
- The `loc=1` vs `loc=0` A/B is still unrun. Every run so far is the same arm.
