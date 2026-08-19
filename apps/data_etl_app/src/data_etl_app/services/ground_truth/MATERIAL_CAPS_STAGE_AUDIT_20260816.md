# material_caps stage audit — alecmfg.com, run 20260816T030947

Manual audit of a single stage-gated extraction, judged by Claude (Opus 5) against the
source text, not by the pipeline's own grounding stages. Written 2026-08-16.

**Dump:** `packages/logs/extraction_dumps/20260816T030947/alecmfg_com__material_caps__partial.json`
(gitignored; the dump is the evidence base for every number here)
**Source:** `knowledge/sample_scraped_texts/alecmfg.com.txt` — 142,826 chars, 71 pages,
37 of them scraper-marked `[duplicate]`.

Source-drift check: all **116/116** extracted phrases occur verbatim in the local sample,
and the chunk bounds (`0:98612`, `82980:142870`) line up with its length. The local sample
is the same snapshot the run used, so recall claims below are sound.

## Run shape

| | |
|---|---|
| Stages run | `phrase_search`, `recursive_search`, `relationship` |
| Stages skipped | `freehand_grounding`, `initial_grounding`, `iterative_grounding`, `reconcile`, `screening` |
| Model (all 3 stages) | gpt-4.1, temp 0, seed 12345 |
| Chunking | `max_chunks=2`, `max_tokens_per_chunk=20000`, `overlap=0.15`, `search_divisor=4` |
| Requests | 19 (8 search, 8 recursive, 3 relationship) |
| Tokens | 131,454 in / 12,171 out |
| Wall time | 173 s |

Coverage is complete — the two macro chunks union to the whole document, with an
overlap region at `82980:98612`. No text went unsearched.

---

## Verdict per stage

| Stage | Verdict | Headline number |
|---|---|---|
| `phrase_search` (round 1) | **Good recall, mediocre precision** | 99 phrases, 63.6% in-boundary |
| `recursive_search` (round 2) | **Failing — net negative** | 12 phrases, **0% in-boundary** |
| `brute` | **Failing — unsafe matcher** | 5 phrases, 2 usable, 2 substring false positives |
| `relationship` | **Correct but severely under-collected** | 45.2% of distinct contexts captured |

---

## Stage 1 — `phrase_search`

### Recall: strong

The site's canonical material lists are captured **completely**. The `/services/`
"Material Options" block enumerates 12 metals (Aluminum, Stainless Steel, Brass, Copper,
Titanium, Magnesium, Zinc, Nickel, Mild Steel, Alloy Steel, Tool Steel, Inconel) and
8 plastics (ABS, Nylon, Polycarbonate, POM, PTFE, PEEK, PVC, HDPE) — **all 20 extracted**.
Every case-study alloy designation is captured too: 316L, 6061-T6, 6061-T651, AL7075,
EN AW 7075-T6, EN AW-7475-T7351, EN AW-6082 T6, Ti6Al4V, C1100, ASTM A36, 4140.

A 90-pattern lexicon sweep (alloys, polymers, composites, depositing coatings, stock forms)
returned **4** uncovered terms, all false positives of the lexicon itself — `2024` (a year),
`420` and `360` (dimensions), `casting` (a process). A page-level read of the material-dense
pages found nothing further.

**No material was missed that matters.** Recall against the prompt's stated boundary is
effectively 100%, and the boundary-vs-ontology bucket is empty here: there is no material
the site names that the prompt's boundary excludes.

### Precision: the weak half

Classifying all 116 rows against the prompt's own `What qualifies` / `Rejection` / `Extent`
rules:

| Category | n | % | Meaning |
|---|---:|---:|---|
| MATERIAL | 65 | 56.0% | correct — a material designation |
| PART | 27 | 23.3% | an artifact name with a material embedded — `Extent` violation |
| PROCESS | 16 | 13.8% | a process naming no deposited substance — `Rejection` violation |
| CONSUMABLE | 5 | 4.3% | a real substance, but not one the work is made from |
| FALSEPOS | 2 | 1.7% | no material present at all |
| SPEC | 1 | 0.9% | a standard designation (`ASTM B221`) |

Two systematic leaks, both traceable to prompt wording:

1. **`Extent` is not enforced.** 27 rows are part names, not materials — `Aluminum Mounting
   Brackets`, `Stainless Steel Valve Body`, `copper heat sink`, `steel plates`,
   `titanium components`, `optical sensor ring`. The rule says "extract the designation,
   not the statement made about it", but the model treats a part name containing a material
   word as itself qualifying. Note the bare designations were *also* extracted, so these
   are pure duplicates of information already present.

2. **Anodizing is out of boundary but keeps being extracted.** 6 rows (`anodizing`,
   `Clear anodizing`, `Natural anodizing`, `Type II matte black anodizing`, `Clear anodized
   (Type II)`, `Clear anodized (Type II, Class 1)`). The qualifying rule admits "a process
   or treatment **named after a substance** it deposits". Anodizing is named after *anode* —
   it names no substance, so it does not qualify under the rule as written. Contrast
   `epoxy coated` (correctly qualifies) and `Nickel-based duplex process` (correctly
   qualifies — names nickel). If anodized aluminium oxide is wanted, the rule needs to say so;
   right now the model is over-reading it.

---

## Stage 2 — `recursive_search`: **0 for 12**

Round 2 added 12 phrases across both chunks. **Not one is in boundary.**

| Phrase | Why it fails |
|---|---|
| `optical emission spectrometry (OES)` | an analysis technique |
| `passivation`, `blasted`, `medical-grade cleaning` | processes naming no substance |
| `bar stock`, `extrusion stock`, `dowel`, `vacuum fixture`, `threaded inserts`, `copper heat sink` | part/stock forms |
| `alcohol`, `triple-filtered water`, `lint-free cloths` | cleaning consumables |

This is precisely the failure its own prompt warns against — *"It is normal for nothing to
remain… Do not lower the bar to return something."* The model lowered the bar anyway. Round 1
had already achieved full recall, so round 2 had nothing legitimate left to find and
manufactured 12 rejects instead.

It also costs the most input tokens of any stage (40,395) for 109 output tokens.

**Recommendation:** for `material_caps`, either set `max_rounds=0` or make the empty result
the strongly-preferred default. Every downstream stage now has to spend tokens rejecting
these 12.

---

## Stage 2b — `brute`: the matcher is unsafe

5 rows came from brute ontology-label matching:

| Phrase | Verdict |
|---|---|
| `Chromium` | **good** — genuine catch the LLM missed ("Enhanced with elements like chromium and nickel") |
| `Urethane` | **good** — the site offers urethane casting |
| `Foam` | out of scope — shock-absorbent *packing* foam |
| `Lead` ×2 | **false positive — 52 occurrences, zero are the metal** |

`Lead` is the important one. All 52 hits are substring matches inside **leading**,
**leadership**, **lead time**, **lead developer**, *"lead with integrity"*, **heavy-lift**.
The matcher is not applying word boundaries, and even with them `lead time` would still hit.

Short ontology labels that are also common English words (`lead`, `foam`, `tin`, `iron`)
need word-boundary matching at minimum, and realistically a stop-context list.

---

## Stage 3 — `relationship`

### Is the relationship correct? Mostly yes.

Read across the corpus, the relationship prose is **accurate, well-attributed, and
appropriately hedged**. The best example is the one at the top of the dump — for
`316L stainless steel` the model correctly identifies that the text describes a *client's
previous supplier's* material, and explicitly declines to claim the manufacturer uses it:

> "The manufacturer references 316L stainless steel as the material used by a client's
> previous suppliers… but does not explicitly state it uses 316L itself in its own production."

That is exactly the neutral-journalist behaviour the prompt asks for, and it is the single
most valuable thing this stage produces. Attribution was checked against the source:
`Joe Vin` (author bio), `U.S. Oil & Gas Client`, `Swedish Electric Truck Startup` all exist.
**No fabricated sources.**

Three defects, in descending severity:

**1. Under-collection of mentions — the main finding.** See next section.

**2. The 6 null rows are a DUMP provenance bug, not a pipeline defect.**

*(Corrected after first writing — the initial reading of this finding had the mechanism
backwards. The numbers below are unchanged; the cause and the fix are not.)*

Deterministic: **6/6** null relationships are case-variant duplicates, and **6/6**
case-variant duplicates produced exactly one null. In every case the survivor is the
capitalised variant and the null is the lowercase one:

| null row | surviving sibling |
|---|---|
| `aluminum` | `Aluminum` |
| `aluminum alloy` | `Aluminum Alloy` |
| `polycarbonate` | `Polycarbonate` |
| `stainless steel` | `Stainless Steel` |
| `titanium` | `Titanium` |
| `aluminum alloy EN AW-6082 T6` | `Aluminum alloy EN AW-6082 T6` |

**The pipeline is behaving correctly.** `get_relationship_candidates()` already applies
`dedupe_case_insensitive` (`core/utils/label_dedupe_util.py`, kept from the reverted grouping
work) at `llm_phrase_relationship_node_service.py:258`. That helper sorts and keeps the first
variant — uppercase sorts before lowercase — which is exactly the survivor pattern observed.
So the relationship stage was only ever *sent* `Aluminum`; `aluminum` was deliberately
collapsed before the request.

The bug is in the dump. `extraction_dump_util.py:489-494` drives its rows from the union of
the raw **search** rounds and then does `relationship_flat.get(phrase)`, so a phrase that was
deduped away before the request is indistinguishable from a phrase the stage saw and had
nothing for. The run note asserts the stronger claim outright:

> "A stage absent from a row's keys did not run; an explicit null means it ran and had
> nothing for that phrase."

For these 6 rows that is false. **Fix belongs in the dump:** either drive relationship rows
from the deduped candidate list, or mark collapsed variants explicitly (e.g.
`"relationship_deduped_into": "Aluminum"`) so a reviewer can tell "never asked" from
"asked, nothing found". Until then every dump under-reports relationship coverage and any
GT instrument built on these rows inherits the same blind spot.

Cross-chunk variants are a separate, still-open matter: dedup runs per chunk, so
`Copper`/`copper` and `Aluminum Mounting Brackets`/`aluminum mounting brackets` still reach
the final merge as distinct phrases (see Cross-chunk duplication below).

**3. One attribution error from nav-link adjacency.** For `copper heat sink` (chunk 2) the
model attributes *"This case study highlights our expertise in copper heat sink machining…"*
to the **U.S. Oil & Gas Client case study**. The sentence is on the copper-heat-sink page
(`@105564`); "U.S. Oil & Gas" appears 172 chars earlier as a `Next →` nav link. The model
read the nav link as a section header. Low frequency, but it inflates apparent evidence.

### Quote fidelity — 89.4%, and the misses are mostly benign

189 quoted spans ≥20 chars; 169 verbatim-present.

- **7** are ellipsis truncations (`'…components...'`) — grounded, but they drop the trailing
  qualifiers the prompt explicitly asks to keep.
- **10** add a terminal period the source lacks (table cells and bullets). Cosmetic.
- **3 are re-assembled table rows** and ground to nothing as written:
  `"Sealing: Nickel-based duplex process >96 hr salt spray resistance"`,
  `"Micro-blasting: White alumina 800 Uniform matte texture Sa ≈ 1.5 μm"`,
  `"18°C / 60 minutes 18.2 ± 0.3 μm black layer."`
  The scraper flattens tables into `\n\n\t\n\n`-separated cells; the model reconstructed the
  `Step | Parameters | Result` rows into readable prose. **This is semantically faithful and
  arguably better than verbatim** — it restores a binding the scraper destroyed. The real
  defect is upstream in table flattening, not in the model.
- Title-case normalisation inside quotes (`…Housing For French UAV Client` vs the source's
  `for`) recurs and should be considered a fidelity bug if quotes are ever used as anchors.

### Own-name leakage: 2/110, both already flagged

`SLA/SLS printed prototypes` and `high-temperature alloys` leak "Alec". The pipeline caught
both (`relationship_own_name_hits: 1`), so detection works. 1.8% leak rate on gpt-4.1 is far
below the gpt-4o-mini rates recorded in the stage-3 naming A/Bs.

---

## The mention question: **no, one mention is not enough — and it is a real defect**

Your read was right, and it is not explained away by the prompt's legitimate de-dup clause.

Raw occurrence counts overstate the problem, because much of this corpus is the same heading
repeated across 71 pages. So occurrences were clustered into **distinct contexts** (unique
containing line, per chunk). Against that measure:

- distinct contexts available across the 110 rows: **303**
- `Mention:` records actually written: **137**
- **context coverage: 45.2%**

The shape of the failure is the important part:

| distinct contexts | rows | avg contexts | avg mentions | coverage |
|---|---:|---:|---:|---:|
| 1 | 70 | 1.0 | 1.00 | **100.0%** |
| 2–3 | 22 | 2.2 | 1.68 | 77.1% |
| 4–9 | 10 | 5.2 | 1.70 | 32.7% |
| 10+ | 8 | 16.6 | 1.62 | **9.8%** |

**Average mentions written is ~1.7 no matter how many contexts exist.** The model is not
de-duplicating; it is satisficing — it writes one or two records and stops. Coverage collapses
precisely as the phrase gets more important.

Crucially, the de-dup clause *is* being honoured correctly where it applies. 11 phrases occur
7–10 times in a single repeated heading and correctly collapse to one mention
(`Aluminum Mounting Brackets` 10 occurrences → 1 context → 1 mention;
`optical sensor ring` 8 → 1 → 1). So the model can tell repeats from distinct contexts. It
just will not write more than ~2 records.

Worst cases — all high-value generic materials, i.e. exactly the phrases that will carry the
ontology grounding:

| phrase | chunk | distinct contexts | mentions | missed |
|---|---|---:|---:|---:|
| `Aluminum` | 0 | 34 | 2 | 32 |
| `Metal` | 0 | 14 | 1 | 13 |
| `copper` | 2 | 14 | 1 | 13 |
| `steel` | 0 | 13 | 2 | 11 |
| `anodizing` | 0 | 14 | 5 | 9 |
| `aluminum` | 2 | 10 | 1 | 9 |
| `copper heat sink` | 2 | 8 | 1 | 7 |
| `titanium alloy` | 2 | 5 | 1 | 4 |
| `Aluminum Alloy` | 0 | 5 | 1 | 4 |
| `Stainless Steel` | 0 | 5 | 2 | 3 |

What actually gets lost is substantive, not redundant. For `Aluminum Alloy` the single
recorded mention omits *"aerospace-grade aluminum alloy with anodized surface treatment for
corrosion resistance"*, *"pre-stressed… sheets were selected to minimize deformation"*, and
*"three aluminum alloy components"* — three different facts about how the material is used.
For `titanium alloy` it omits the Ti6Al4V identification, the α-β classification, and the
machining-difficulty discussion. The prompt's own standard — *"collect them in enough detail
to verify the phrase later on without needing the original text"* — is not met for any
phrase above 3 contexts.

**Recommendation:** the prompt asks for exhaustiveness in prose but gives the model no
counting discipline. Ask it to state the number of distinct contexts it found before writing
the records, and cap by relevance rather than letting it stop at the first hit. The
per-request phrase budget (`max_phrases_per_request=50`) is also worth testing lower —
chunk 0 packed 74 phrases into 2 requests and produced 5,044 output tokens, which is where
the satisficing pressure most likely comes from.

---

## Cross-chunk duplication

13 phrases (case-insensitive) appear in both chunks — 11% of rows are duplicated work from
the `82980:98612` overlap. Their relationship records **diverge**: different mentions,
different attributions, sometimes different verdicts.

The sharpest case is `Lead`. Chunk 0 gets it right:

> "No direct mention of 'Lead' as a material or in any other context… The text has no direct
> mentions or states no clear relationship."

Chunk 2, same model, same prompt, same seed, invents evidence from a string match:

> "In the Swedish Electric Truck Startup case study, under 'Sample Lead Time', the text
> lists: 'Delivery required within 20 calendar days after design confirmation'."

The "no clear relationship" escape hatch works, but not reliably — given a bogus phrase the
model will sometimes reach for any string match rather than reject. Three more pairs
(`Copper`/`copper`, `Stainless Steel Valve Body`/`stainless steel valve body`,
`Aluminum Mounting Brackets`/`aluminum mounting brackets`) differ only by case across chunks
and will collide at merge exactly as the within-chunk collisions did.

---

## Priority of fixes

1. **Fix the dump's null semantics** — mark phrases collapsed by `dedupe_case_insensitive`
   instead of emitting a bare `null` that the run note defines as "ran and found nothing".
   Cheap, and it stops every future audit (and any GT instrument built on these rows)
   from mis-reading deduped variants as stage failures.
2. **Disable `recursive_search` for `material_caps`** (or raise its bar hard) — 0/12
   in-boundary and the most expensive stage by input tokens.
3. **Word-boundary the brute matcher** and stop-list short ontology labels — `Lead` is
   pure noise being paid for through every downstream stage.
4. **Give the relationship stage counting discipline** — the 45.2% context coverage is the
   biggest quality gap, and it hits the highest-value phrases hardest.
5. **Tighten `Extent` and settle anodizing** in the search prompt — 37% of rows are parts,
   processes, or consumables that grounding must now reject.

Items 1–3 are mechanical. Item 4 needs a prompt A/B (weak model, temperature 0, fixed seed,
per the standing rule). Item 5 needs a boundary decision from you before any prompt edit.

---

## Addendum — windowing the relationship stage (2026-08-17)

Measurements taken to evaluate narrowing the relationship window and adding an aggregator.

**Windows produce complementary evidence, not redundant evidence.** For the 13 phrases seen
by both macro chunks, comparing the quoted spans each window produced:

| | |
|---|---|
| spans from window 0 | 16 |
| spans from window 1 | 23 |
| union | 32 |
| shared between them | 7 (**21.9%**) |
| union vs better single window | **1.39×** |

78% of the evidence is window-specific. Running a phrase over more windows genuinely surfaces
new mentions rather than re-finding the same top hit.

**Narrower windows are cheaper on input, not dearer.** Simulated over this document
(27,767 tokens, 116 phrases, `max_phrases_per_request=50`):

| window | windows | requests | phrase-slots | input tokens |
|---:|---:|---:|---:|---:|
| 20,000 (current) | 2 | 4 | 138 | 65,162 |
| 10,000 | 4 | 6 | 184 | 57,571 |
| 7,500 | 5 | 6 | 214 | 45,251 |
| **5,000** | **7** | **7** | **254** | **38,691** |
| 3,000 | 11 | 11 | 320 | 41,887 |

Actual current run: 3 requests, 53,187 input tokens. At 5,000-token windows input drops
**~27%**, because a phrase group stops re-sending the full 20k chunk text. At 5k every window
holds ≤44 phrases — under the 50 cap — so batching collapses to one request per window
instead of chunk 0's current 2 requests each carrying the whole chunk.

**Output would rise ~2.2×.** Mentions cost ~81 output tokens each; full coverage of all 303
distinct contexts implies ~24,600 output tokens against today's 11,129.

**The design tension to settle first:** asymmetric chunking was adopted on the stated premise
that "one wide relationship window removes same-phrase cross-chunk disagreement". Narrowing
the relationship window deliberately gives that property up — the `Lead` contradiction
(window 0 correctly reports no mention, window 1 invents one from "Sample Lead Time") is
exactly the failure mode it was meant to prevent, and more windows means more of it. An
aggregation stage is therefore not optional garnish; it is what pays back the coherence debt
the narrowing takes on.
