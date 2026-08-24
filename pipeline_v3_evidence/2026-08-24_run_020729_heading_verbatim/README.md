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
- **all 93 shared mention request ids carry an identical `ud=`** against `20260824T012721`
  (this line first said 37 — recounted by the supplement below), and all 20 shared synthesis
  `ud=` changed (the locations moved). Only `pv=` differs on the mention stage.

So every difference below is the prompt. **20 of 20 dumps, field set identical to the last complete run.**
The three fields lost by `012721` are back.

## Verdict in one line
**The fix landed on exactly what it targeted and cost almost nothing — the two regressed records have
their provenance back, no Allegion-class party loss remains (the supplement finds 7 mild Falcon
page-enumeration losses on the full pairing), and the price is +6.9% location text — but the
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
   *Corrected by the deep supplement (§8 below): this pairing dropped 114 records; on the full pairing
   7 records did lose a party name — all the mild Falcon page-enumeration class — and survival is
   47% → 66%. The Allegion class stays fixed.*

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

---

# Deep supplement (same day, second pass): `deep_supplement.py` / `deep_supplement_output.txt`

An independent recount of the A/B's structural claims plus what the first pass did not measure.
Run from the repo root; pyright 0 errors, ruff clean.

## Corrections to the first pass

1. **93, not 37.** The mention stage shares **93 request ids** between the runs (93 → 126), every one
   with an identical `ud=` (92 distinct digests — the products/contract_products window pair shares one).
   The "37" in the first write-up matches nothing in the dumps; the committed A/B script itself printed 93.
2. **The pairing dropped 114 records.** `synth_rows` keyed records (subject, field, group_id) — but
   **242 group_ids appear in BOTH chunks of a field**, so the dict silently kept only the last chunk's
   record. Bounds-keyed pairing gives **1,360** shared records (= every record `012721` produced).
3. **On the full pairing, 7 records DID lose a party name** — all `steelcraft/conformity_attestations`
   chunk 0, all losing exactly `Falcon`, all the page-enumeration class: the old location listed every
   product page carrying the bullet ("… for INPACT System Integrated Door Solution **and SZ Series Falcon
   Flush Doors** …"); the compact new style names fewer pages ("on the page about INPACT Series hospital
   doors"), so the sub-brand word rode out of the list. The claim's own provenance survives; what shrank
   is the sibling-page enumeration. Party survival on the full pairing: **16/34 = 47% → 19/29 = 66%** —
   the direction of the verdict stands, the absolute "nothing was lost" does not.

## New measurements

4. **Identical-synthesis collapse census** (two records of one request sharing a byte-identical synthesis
   string): **12 (`002404`) / 23 (`010654`) / 1 (`012721`) / 14 (`020729`)**. A **standing** synthesis
   behavior, not the heading fix's — `012721`'s 1 is only its missing `steelcraft/products` dump (the
   field holding 10–16 of the pairs every full run). 8 of this run's 14 pairs were already identical in
   `010654`. **The FE→DE pair is the only one whose shared string is WRONG for a record**; the rest are
   composite sentences naming both entities ("… include a Paladin Door & Frame with Schlage LM9300 Levers
   or Von Duprin WS-T Exit Devices …") reused verbatim across co-packed records — satisficing, and 13 of
   14 are invisible to the focal-form lint because the shared string contains both focal forms.
   **Tripwire candidate: flag identical synthesis strings within one request** — see §10, which
   sizes it and kills the pre-emptive alternative.
5. **Twin census.** Reconstructing the packer (soft cap 50, verified against every chunk's
   `group_requests`), **130 confusable same-request focal-form pairs** — multi-token names differing in
   one confusable token, or edit distance ≤ 2 (`FE/DE Series`, `Type 304/316 Alloy`, `TAS 201/202/203`,
   `ICC 500-2014/-2020`, `2/5 Day Door Express`, the 12-member `System Set D1…S4` family = 66 pairs).
   93 of the 130 sit in `steelcraft/products`. This is the population the twin fix must protect;
   1 invention among them this run.
6. **Own-name identification is steady and the heading fix inflates it.** Hits: alecmfg 1,635 → 1,618 →
   **1,780**, steelcraft 2,255 → 2,154 → **2,477** across `002404` / `010654` / `020729`, with 99–100% of
   records naming the manufacturer throughout — verbatim headings ('STEELCRAFT PRODUCTS') inject the name.
   Payload, not a defect, post-ban.
7. **The document-listing rule held under the new locations**: on the 28 paired records whose snippet
   evidence is a document listing, scoped 19 → 20, unscoped 'provides this resource' **0 → 0**.
8. **Mention-stage delivery clean** (the first pass only checked synthesis): 3,345/3,345 snippets
   described, 0 not-described, 0 retried, 0 unknown ids, 0 zero-hit forms; 96 replayed `llm_search`
   request rows confirmed in the dumps.
9. **Latency**: client p50 7.9–10.0 s, p90 ~16 s, max 24.6 s; turnaround p50 189–203 s (batch queue).
   17 transparent client-level OpenAI retries in the log (~8% of 221 fresh requests — normal).
10. **Paired synthesis text +1.9%** (mean 381 → 389 ch on the 1,360 pairs) — the first pass's
    "371 → 363" compared different record sets. Thin single-entry records steady at 69% → 70%.

## §10 — Sizing the collapse: no PRE-emptive filter is affordable

Added after the first supplement pass, when the illustration work turned up the mechanism.

**Every collapse has one signature.** Call two records in the same request **thin twins** when each has
exactly ONE evidence entry and their location lists are byte-identical. Across the four runs, **49 of the
50 collapses are thin twins** — but the signature fires 837–1,557 times per run:

| run | thin twins | + confusable names | collapses | thin-twin recall | precision | confusable recall |
|---|---|---|---|---|---|---|
| `002404` | 840 | 75 | 12 | 92% | 1% | 0% |
| `010654` | 840 | 75 | 23 | 100% | 3% | 0% |
| `012721` | 837 | 3 | 1 | 100% | 0% | 0% |
| `020729` | 1,557 | 74 | 14 | 100% | 1% | **7%** |

**This kills the OWED item as written.** "Separate near-identical focal forms in the packer" cannot be
built on any signature measured here: thin-twinness over-treats by ~100× (1,557 pairs to prevent 14), and
narrowing it with a confusable-name test catches **1 of 14** — because **FE→DE is the atypical collapse**.
The other 13 are pairs of *different* things named in one true sentence:

```
'Paladin Door & Frame'          →  "Steelcraft's site states that certified Tornado assemblies
'Schlage LM9300 Levers'         →   include a Paladin Door & Frame with Schlage LM9300 Levers
'Von Duprin WS-T Exit Devices'  →   or Von Duprin WS-T Exit Devices as latching hardware."
'latching hardware'             →  (one sentence, four records)
```

**What is actually worth building, and what already exists:**

1. **The exact post-hoc tripwire** — flag identical synthesis strings within one request. Costs nothing,
   is exact by construction, measures a real quality axis (undifferentiated records). Dump counter, in
   the posture of the existing lints — NOT a retry trigger.
2. **The harmful subclass is already covered.** A record whose own name is absent from the shared sentence
   is exactly what `focal_form_lint` tests, and it flagged FE→DE **both times it occurred**. There is no
   detection gap to close — the earlier "13 of 14 invisible to the lint" reading was backwards: the lint
   correctly ignores 13 accurate-but-duplicated records. What is missing is a *fix*, not a detector.
3. **The mechanism is evidence thickness, not name similarity.** The same FE/DE pair appears in both
   chunks of the document:

   | chunk | evidence | lint | outcome |
   |---|---|---|---|
   | `0:91562` | 1 bare nav-menu entry each, identical locations | **flags FE** | **fused — FE described as DE** |
   | `91562:158790` | FE 3 entries, DE 2 entries, real prose | clean | **both correct** |

   Given anything to tell them apart, the model tells them apart. So the promising direction is
   **thickening or disambiguating single-entry records**, not re-packing by name similarity.
