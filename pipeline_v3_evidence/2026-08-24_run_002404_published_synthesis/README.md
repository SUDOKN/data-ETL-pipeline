# Seventh v3 run analyzed: 20260824T002404 — the synthesis statics as PUBLISHED

The first run of the two fixes built on 2026-08-23 after the `195031`-vs-`200044` A/B: the **own-name ban
retired** from the six synthesis statics (the manufacturer's name now identifies the party rather than
being masked to `the manufacturer`) and the **party-preservation rule** added (say whose dealing it is;
do not promote a third party's credential into the manufacturer's). Same two subjects, `loc=1`, 50 entries
per request, `gpt-4.1`, temperature 0, seed 12345.

**This is a clean A/B.** All 95 synthesis requests carry an identical `ud=` digest and an identical id set
across `20260823T200044` and this run — only `pv=` differs (six new prompt version ids confirm the publish).
The mention state is the same one both earlier runs replayed. Every difference below is the prompt and
nothing else.

Scripts: `synthesis_ab.py` / `synthesis_ab_output.txt`, run from the repo root.

> **Counting caveat** (unchanged from the previous run): `products` and `contract_products` share every
> phrase stage through synthesis, so their dump files are byte-identical apart from `field_type`. Every
> number below skips `contract_products`: **2,133 records, 95 requests**.

## Verdict in one line
Both fixes landed and the headline defect is gone — the **FE→DE entity swap is repaired**, the invented-
`Series` sweep goes **2 → 0**, and both Allegion records now name the right party *and* explicitly decline
to claim the credential — at **no cost** ($2.42 both runs) and **−2.6% output**. Two things came back that
the plan did not predict: the shipped focal-form lint now fires **13 times, all false**, because the new
prose re-inflects the form it used to quote; and the set-aside floor **collapsed on one narrow class** —
phrases whose only evidence is a document title in a Downloads list (floor fired on 23 of 74, now 1 of 74).

## RIGHT

1. **The FE→DE swap is fixed.** The record whose focal form and only snippet both read
   `FE Series Double-Egress Frames` — and which `200044` returned describing `DE Series Double-Egress
   Frames`, a real and different product from the same 50-entry request — now describes FE correctly in
   both the `equipments` and `products` branches. The `<X> Series` invention sweep over all 1,250 steelcraft
   records goes **2 flagged → 0** (after removing the known ™-stripping false positives, 9 raw → 0 real).
   This was the one defect on record that nothing downstream could see, and it is gone.
2. **Party preservation works, on exactly the cases it was written for.** Both Allegion records improved:
   - `LEED Credits` — was *"the manufacturer provides information about LEED Credits **in relation to its
     products**"*; now *"**Steelcraft's** sustainability page lists 'LEED Credits' … under the **'More from
     Allegion'** section. This shows that Steelcraft provides information about LEED Credits, but **does not
     state that Steelcraft itself awards or certifies** LEED Credits."*
   - `CalGreen Building Standards` — the same shape, same explicit disclaimer.

   The old wording read as though Steelcraft held the credential. The new one names the publisher, names
   the real owning party, and declines the stronger claim. Records naming Allegion at all: 38 → 43.
3. **Perfect delivery, unchanged.** 2,133 records sent, 2,133 synthesized, in both runs.
   `not_synthesized=0`, `retried=0`, `retry_requests=0`, `unknown_answer_ids=0` across all 95 requests.
   The 3.2 retry path has still never had to fire.
4. **Free.** $2.42 both runs. Input 449,126 → 451,484 tokens (+0.5%), output 189,669 → 189,195 (−0.2%),
   max response 4,174 → 4,030 against the 20,000 cap, mean latency 9.3 s → 8.8 s.
5. **The name is real, not invented.** "Alec Model" appears 62 times in the source text
   (`knowledge/sample_scraped_texts/alecmfg.com.txt`, "Alec Model Co."). The stage is using the company's
   actual name, not a paraphrase of the domain.
6. **The set-aside floor did not weaken overall.** 2.3% → 2.6% of all records (48 → 56) — see WRONG 2 for
   the one class where it did.
7. **Third-party attribution held.** 9.1% → 9.0% of records attribute a dealing to a customer, client,
   supplier or other party rather than silently crediting the manufacturer.

## WRONG

1. **The shipped focal-form lint now cries wolf: 2 flagged → 13, and all 13 are false.**
   700 entity-shaped records in each run, so the base is identical. The cause is the prompt, not the model:
   the old statics *quoted* the focal form (*"'Precision Fixturing & Machining' is described as…"*), the new
   ones write it into the sentence's grammar (*"Alec Model **performs precision fixturing and machining**"*).
   `focal_form_absent` tests for a contiguous substring after collapsing punctuation to spaces, so:
   - **8 of 13** are a `&` or `,` re-read as the word "and" — the inserted token breaks the match
     (`Precision Fixturing & Machining`, `Surface Finishing & QA`, `M4, M6 fasteners`).
   - **5 of 13** are re-ordering or an inserted word splitting the form
     (`Mineral Board (optional)` → "**optional mineral board** core";
     `Steelcraft Hurricane products` → "Steelcraft **describes its** Hurricane products").

   Precision went from 1-real-of-2 to **0-real-of-13**. The lint would still catch a true swap — a wrong
   name is still absent — and 13 rows is still cheap to read by eye, but the rates quoted in
   `focal_form_lint.py`'s docstring are now stale, and the connector hole is worth closing.
   The naive whole-corpus check moved the same way: 24 → 38.
2. **The set-aside floor collapsed on document listings: 23 of 74 → 1 of 74.**
   The 45% → 64% jump in dealing assertions on thin evidence (section 6) is *mostly* a measurement artifact
   — the old prompt wrote "offered by the manufacturer" (verb before subject, which the section-6 regex
   misses) where the new one writes "Steelcraft offers" — and sampling 12 of the 98 newly-asserting records
   confirms most are the same claim in the active voice. But one class genuinely changed verdict: a phrase
   whose only evidence is a document title in a Downloads or Literature list.

   | focal form | old | new |
   |---|---|---|
   | `Allegion EHPA Tornado Systems Data Sheet` | "the entries do not show any specific dealing by the manufacturer with it" | "indicating that **Steelcraft provides this data sheet** as a resource" |
   | `Falcon SZ Series` | "do not show any specific dealing" | "indicating that **Steelcraft provides this resource**" |
   | `Steelcraft Portfolio` | "do not show any specific dealing" | "showing that **Steelcraft provides this portfolio**" |

   Each new sentence is *literally true* — the document really is on Steelcraft's site — so this is not
   fabrication. The risk is downstream: these records sit in `products` and `process_caps`, and a phrase
   like `Falcon SZ Series` (a **Falcon** product, another Allegion brand) or `Steelcraft Portfolio` (not a
   product at all, a PDF) now carries an affirmative sentence where it used to carry a "no dealing shown"
   verdict. The floor was accidentally protecting against a **search-stage precision** problem — document
   titles being extracted as products at all, consistent with the 37% products precision measured on
   2026-08-22. **Consequence unproven:** screening is in `stages_disabled` and gets re-keyed in 3.3, so
   nothing has yet read these records. Fork for the user — see the plan's RESUME HERE.

## Numbers

| | 20260823T200044 | 20260824T002404 |
|---|---|---|
| synthesis requests / records | 95 / 2,133 | 95 / 2,133 |
| requests with changed `ud=` | — | **0** (clean A/B) |
| synthesized / not / retried / unknown | 2,133 / 0 / 0 / 0 | 2,133 / 0 / 0 / 0 |
| input / output tokens | 449,126 / 189,669 | 451,484 / 189,195 |
| est. cost (gpt-4.1) | $2.42 | $2.42 |
| mean / median synthesis chars | 389 / 328 | 379 / 323 (**−2.6%**) |
| records naming the manufacturer | 112 / 2,133 (5%) | **2,133 / 2,133 (100%)** |
| invented `<X> Series` (steelcraft) | **2** | **0** |
| focal-form lint flagged / real | 2 / 1 | **13 / 0** |
| set-aside floor, all records | 48 (2.3%) | 56 (2.6%) |
| set-aside floor, document listings | **23 / 74** | **1 / 74** |
| non-manufacturer attribution | 9.1% | 9.0% |

## Questions this run answers

- *Does output length fall once the read-back check is removed?* **No — −2.6%.** The +28% measured on
  2026-08-23 is not attributable to that instruction; it belongs to the rewrite's other content.
- *Does `own_name_hits_in_syntheses` going non-zero mean a violation?* **No.** It is now an identification
  counter: 100% of records name the manufacturer, which is what retiring the ban asked for. The metric
  needs renaming or retiring, not fixing.
- *Is the party-preservation rule strong enough for the Allegion case?* **Yes, on both test records** —
  and it recovered the "More from Allegion" provenance the old run dropped.

## What was built off this run (2026-08-24, unpublished at time of writing)

1. **The focal-form lint gained a second match path** (`core/utils/focal_form_lint.py`). A verbatim
   occurrence still satisfies it; failing that, every *distinctive* token of the form — connectives
   dropped, plurals folded, whole tokens only, order not required — must appear. Re-measured over all
   three synthesis runs it flags **exactly 1 row per 700 entity-shaped records**, and on `200044` that one
   row IS the FE→DE swap:

   | run | contiguous only | with the token path |
   |---|---|---|
   | 20260823T195031 | 3 flagged, 0 real | 1 flagged, 0 real |
   | 20260823T200044 | 2 flagged, 1 real | **1 flagged, 1 real (the swap)** |
   | 20260824T002404 | 13 flagged, 0 real | 1 flagged, 0 real |

2. **The document-listing rule was added to the synthesis statics** — the fix for WRONG 2. Applied to all
   six, which keeps the "one static, six pins" invariant. **Known overreach, to be evaluated at the
   end-to-end run:** the measured need is `products` (22) and `process_caps` (1); the other four measured
   0 flips. `conformity_attestations` is the field where the rule could actively harm — a certificate PDF
   in a downloads list *is* evidence of the attestation — so if the full pipeline shows attestations being
   withheld, split that static rather than weakening the rule.

The 23 records that lost the floor, for the A/B on the next run — 22 `products`, 1 `process_caps`, all
single-entry, in two shapes:

- **the focal entity IS a document** (search extracted a title as a product): `Steelcraft Portfolio`,
  `INPACT Door Systems Brochure`, `INPACT Door Systems Data Sheet`, `Steelcraft Frame Nomenclature`,
  `Steelcraft Rapid Program Data Sheet`, `5 Day Express Door Data Sheet`, `3 Day Express Frames Data
  Sheet`, `2 Day Rapid Data Sheet`, `Steelcraft 5-day Express Door Sell Sheet`, `Steelcraft 3-day Express
  Frame Sell Sheet`, `Kansas City Door & Frame Stock Sheet`, `Kansas City Modification Sheet`,
  `Grouting WS Strikes` (process_caps);
- **the focal entity is merely NAMED INSIDE a document title**: `Falcon SZ Series` (a *Falcon* product,
  another Allegion brand), `Allegion EHPA Tornado Systems Data Sheet`, `High Definition Door`,
  `FT Series (Thermal Break)`, `Integral Kerfed Frame`, `Windstorm Solutions`, `2 Day Door Express`,
  `3 Day Frame Express`, `5 Day Door Express`.
