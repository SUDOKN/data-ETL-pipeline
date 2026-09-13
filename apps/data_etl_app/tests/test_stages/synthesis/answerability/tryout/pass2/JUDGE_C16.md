# JUDGE_C16 — the list rule on the production model, against requests that carry the defect

Judged 2026-09-12. Arms: **OLD** = `runA_system.txt` (published), **THREE** = OLD + the three
validated edits of JUDGE_PASS2 §10.1, **C16** = THREE + the list rule. Model gpt-4.1, temperature 0,
seed 12345. 107 records × 3 arms = 321 paragraphs, every one read.

## Headline

C16's list rule **works, and it costs nothing on the party control**. It takes the target defect from
25 to 0 on the two tanfel requests whose listings carry an explicit introducing verb, and it does this
across 39 records of a sales representative without a single party error, a single laundering
possessive, or a single represented maker's capability moving to the representative — the cost that
sank edit (1) on the weak model. The clause "What the item takes from that sentence is the dealing,
never the doer", published together with THREE's doer clause, is what holds the party.

It is nevertheless **REVISE**, for one identified reason: on the third request the list is a bare
capabilities index with no verb in it, the model reads that as "a list of what the manufacturer sells,
represents, or carries", and the capacity paragraph's *mandatory* hedge fires on all 26 of those
records — the same 26 OLD hedges. The list rule never contradicts that paragraph, so the paragraph
wins. One replacement sentence in the capacity paragraph closes it (§6).

**THREE is PUBLISH.** On the production model, on requests that carry the defect, it removes 32 of
OLD's 51 over-hedges, fixes both of OLD's party slides on the representative, and introduces no party,
laundering or supplied-dealing regression anywhere. Its three residues are all shape, all listed in §4.

## 1. Method

- **Files.** Requests `pass2/req_tanfel_g{13,10,15}_user.txt` (Tanfel, process_caps) and
  `../synthesis_shape_tryout_20260909/1/A_user.txt` (Mathews & Company, the party control). Outputs
  `pass2/c16_out_<req>_<arm>_41.json`. Fences stripped; record ids identical across all three arms in
  every request (15 / 20 / 33 / 39), so every comparison is record-for-record.
- **Reading, not counting.** Every record of every arm was read against its own snippets and against
  the passage the snippets come from in the request's site text. Mechanical counts appear only where
  they were confirmed by reading; where the regex and the reading disagreed (g10 THREE, g15 THREE) the
  reading governs and the discrepancy is stated.
- **Secondary evidence.** The gpt-4.1-mini runs were scanned and read where they might change a
  conclusion. They do not: the weak model writes the word "capacity" **zero times in all 12 files**,
  including on the sales representative where hedging is correct. That confirms JUDGE_PASS2 §10 — this
  edit's target is unobservable on the weak model, in either direction.

### 1a. Is the subject a maker or a representative? — the frame ruling applied

**Tanfel (g13, g10, g15): a maker that presents every listing in this chunk as its own.** The site
text settles it. The homepage says "TANFEL IS A LEADING PROVIDER OF CUSTOM METAL PARTS … We focus on
stamping, extrusion, casting and CNC machining", "Tanfel now makes high-quality Thread Plug Gages",
and heads a block "## Our Capabilities" whose items are "Our Capabilities: Stamping" and so on. Every
snippet family in these three requests sits on a page under Tanfel's own `CAPABILITIES` breadcrumb:

| snippet family | page it sits on | heading above it |
|---|---|---|
| `CAPABILITIES: X` | tanfel.com/products | `## CAPABILITIES` |
| `Metal: X` | tanfel.com/metal/ | `## Metal` (breadcrumb Home › CAPABILITIES › Metal) |
| `Additional Capabilities: X` | tanfel.com/additional-capabilities | `## Additional Capabilities` |
| `Related Products: X` | each capability page | `## Related Products` |
| `- Anodizing`, `- EDM cutting` | tanfel.com/products/aluminum-extrusion/ | "Here is an overview of **Our** Aluminum Extrusion Capabilities/Services:" |
| `- UV hard coating`, `- EMI, RFI and ESD shielding` | tanfel.com/products/membrane-switches | `#### Capabilities`, under "**Our** membrane switches … are manufactured with strict process controls" |
| `- Precision CNC-Machining`, `- Heat Sinks` | industry pages (medical, lighting, music) | `## Capabilities` |

Tanfel does source globally ("Global Sourcing", "Our Worldwide Network Of Quality ISO Certified
Facilities"), but it never attributes a listed item to another company: there is no line card, no
represented maker's page, no third-party name anywhere in these three requests. **Frame-decides
therefore licenses "offers" / "lists among its capabilities" on every listing record in g13, g10 and
g15, and "capacity unstated" on none of them.** The membrane-switch page's own first person licenses
more than that — "manufactures" — for that page's items only.

**Mathews & Company (rep): a representative, and nothing else.** "Mathews & Company only represent
companies who are interested in a long term relationship"; the Kansas office "was established in 1962
as a sales representative's organization … We specialize in selling related products". Each Kansas
Product Line entry is a principal's copy, pronouns and all ("They have in house engineering and
tooling", "We can apply Pressure Sensitive Adhesives"). **Hedging is right on all 39 records, and a
principal's capability must never become Mathews's own.**

### 1b. What counts as the target defect

On a Tanfel listing record: any clause saying the capacity, the role, or who makes/performs the thing
is unstated, appended to a dealing the frame has already licensed. It is counted whether the arm also
credited the offering (THREE's and C16's form, "Tanfel offers X … but the capacity is not specified")
or refused to (OLD's form, "X is a category associated with Tanfel … the capacity is unstated"); the
split is reported. Hedges on the representative, and declines on explainer passages, are **correct**
and are never counted as the defect.

## 2. Per-request grade tables

Counts are records. "OWN listings" excludes explainer, testimonial and general-process records, which
are graded on their own rows.

### g13 — Tanfel, 15 records (all 15 are own listings; `go74pqr8` also carries a materials sentence)

| axis | OLD | THREE | C16 |
|---|---|---|---|
| frame named | 15 | 15 | 15 |
| frame accurate | 15 | **3** (12 call an alt-text list entry "a section heading") | 15 |
| dealing credited as the frame licenses, clean | 1 | 1 | **14** |
| dealing credited **with** a capacity hedge (over-fire) | 0 | **13** | 0 |
| dealing refused, capacity hedged (full over-fire) | **14** | 1 | 0 |
| **target over-hedge, total** | **14** | **14** | **0** |
| dealing supplied beyond the frame | 0 | 0 | 0 |
| party correct | 15 | 15 | 15 |
| laundering possessive | 0 | 0 | 0 |
| wire vocabulary ("snippet") | 15 | 14 | **1** |
| unsupported claim | 0 | 0 | 0 |
| focus kept (no byte-identical sibling) | 15 | 15 | 15 |
| mean words | 82.6 | 60.0 | **49.0** |

### g10 — Tanfel, 20 records (1–11 own listings, 12 a customer's words, 13–20 explainer/process)

| axis | OLD | THREE | C16 |
|---|---|---|---|
| frame named | 20 | 20 | 20 |
| frame accurate | **20** | 19 (`g9awklq1` called a testimonial) | 19 (same) |
| own listings credited clean (of 11) | 0 | 6 | **11** |
| **target over-hedge (of 11)** | **11** | **5** | **0** |
| own-list item correctly credited from the introducing sentence | 0 | 0 | **2** (`g5s94jt5`, `gsqiy4oj`) |
| explainer correctly declined (of 8) | 8 | 8 | 8 |
| restating closer | 0 | 0 | **6** |
| party correct | 20 | 20 | 20 |
| laundering possessive | 0 | 0 | 0 |
| wire vocabulary | 20 | 10 | 7 |
| focus kept | 20 | 20 | 20 |
| mean words | 65.2 | 64.7 | 68.5 |

### g15 — Tanfel, 33 records (26 bare capabilities-index entries; 1, 3, 4 page/blog records; 5–8 technique mentions)

| axis | OLD | THREE | C16 |
|---|---|---|---|
| frame named | 33 | 33 | 33 |
| frame accurate | 33 | 33 | 33 |
| own listings credited clean (of 26) | 0 | **26** | 0 |
| **target over-hedge (of 26)** | **26** | **0** | **26** |
| vacuous "does not provide further detail" clause | 0 | **24** | 0 |
| dealing supplied beyond the frame | 0 | 0 | 0 |
| party correct | 33 | 33 | 33 |
| laundering possessive | 0 | 0 | 0 |
| wire vocabulary | 33 | 32 | 31 |
| focus kept | 33 | 33 | 33 |
| mean words | 58.1 | 51.2 | 57.4 |

### rep — Mathews & Company, 39 records (the party control; every record is another party's doing)

| axis | OLD | THREE | C16 |
|---|---|---|---|
| frame named and accurate | 39 | 39 | 39 |
| **party correct** | **37** | **39** | **39** |
| correctly hedged where the frame does not license the offering | 37 | 39 | 39 |
| capacity clause replaced by naming the maker (better still) | 0 | 0 | **2** (`ghu2cklc`, `g7zeb1wf`) |
| dealing supplied beyond the frame | **9** | 0 | 0 |
| laundering possessive ("their capabilities" as Mathews's) | 0 | 0 | 0 |
| a represented maker's capability credited to Mathews | 0 | 0 | **0** |
| wire vocabulary | 39 | **0** | **0** |
| focus kept | 39 | 39 | 39 |
| mean words | 77.1 | 60.2 | 63.3 |

### The one number that decides the pass

| | OLD | THREE | C16 |
|---|---|---|---|
| target over-hedges, g13 + g10 + g15 | **51** | **19** | **26** |
| …of which g13 + g10 (lists with an introducing verb) | 25 | 19 | **0** |
| …of which g15 (a bare capabilities index, no verb) | 26 | 0 | **26** |
| party errors on the 39-record representative | 2 | **0** | **0** |

The hedge is a per-request habit, not a per-record judgement: within one request the model picks one
template and applies it to every listing. That is why the C16 defect is 26-or-0 and never in between,
and why a single sentence can be expected to move it.

## 3. Better / same / worse

| request | THREE vs OLD | C16 vs THREE |
|---|---|---|
| g13 (15) | better 13, same 2, worse 0 | **better 14, same 1, worse 0** |
| g10 (20) | better 6, same 14, worse 0 | better 7, same 7, **worse 6** |
| g15 (33) | better 26, same 7, worse 0 | better 0, same 7, **worse 26** |
| rep (39) | better 9, same 30, worse 0 | better 2, same 37, worse 0 |
| **all 107** | **better 54, same 53, worse 0** | **better 23, same 52, worse 32** |

Ties on the dealing axis are broken by the dealing; frame precision, length and vocabulary are logged
but never outrank a truth change.

### g13 — THREE vs OLD: better (13 of 13 shown as 5)

- `g1hma18g` OLD "Casting Finishes **is a category or type of product or service associated with
  Tanfel** … the capacity is unstated" → THREE "**Tanfel lists Casting Finishes among the related
  products and capabilities it offers**". The dealing arrives; the hedge stays.
- `ghmgct1t` OLD "**do not … show any specific dealing by Tanfel** beyond listing it" → THREE "**Tanfel
  offers or features Metal Casting as part of its offerings**".
- `gq9hkhwi` same shift on Carbide; 111 → 55 words.
- `gaqogzxh` THREE keeps the focal entity distinct from its "/ Springs" neighbour while crediting.
- `g5eo8ybu` OLD "Springs **is a product or capability associated with** Tanfel" → THREE "Tanfel offers
  or features Springs".

**g13 — THREE vs OLD: worse (0).** Reason class of the 12 frame slips: *frame misdescribed* (§4c).

### g13 — C16 vs THREE: better (14 of 14 shown as 5)

- `g1hma18g` THREE "…**but the capacity—whether as a manufacturer, supplier, or service provider—is not
  specified**" → C16 "**These placements indicate that Tanfel offers Casting Finishes as part of its
  product and service capabilities.**" 92 → 36 words, hedge gone, frame named from the three real
  section names.
- `gylk3k14` THREE "**but the snippets do not specify the exact capacity**" → C16 "**Tanfel offers …
  as part of its product and service offerings**", and the frame is corrected from "a section heading"
  to "a heading … and in the 'CAPABILITIES' and 'Related Products' lists" (both verified in the text).
- `gczn9kgc`, `gbmkfcf5`, `gq9hkhwi`: identical correction, each naming its own list.
- `go74pqr8` C16 alone notices the axis mismatch — "the snippets … refer to **wire forming and
  springs, not forming in general**" — and correctly attributes the loose sentence to "**the
  properties of D2 tool steel**" (verified: "- D2 tool steel. Very high wear resistance for punches,
  blades, and forming tools.").

**g13 — C16 vs THREE: worse (0).**

### g10 — THREE vs OLD: better (6 of 6 shown as 5)

- `gg3980lf` OLD "…**but do not specify the capacity beyond offering the service**" → THREE "**Tanfel
  offers Quality Control & Inspection Services as part of its services.**"
- `gj2yrwhk`, `gx3qhtxr`: same removal on Global Sourcing and Engineering & Design Support.
- `g8ln07ym` OLD "does not specify the capacity beyond offering the support" → THREE names the real
  heading, "**under 'Our Advantage'**".
- `ge8yqftc` likewise, and correctly reads the bullet as an advantage rather than a service.

**g10 — THREE vs OLD: worse (0),** with one frame regression inside a better record (`g9awklq1`, §4d).

### g10 — C16 vs THREE: better (7 of 7 shown as 5)

- `grix8k7b` THREE "**though the capacity (such as whether Tanfel manufactures directly or sources) is
  not specified**" → C16 "**Tanfel offers MIM … as one of its own manufacturing capabilities**".
- `gg7ny00v`, `gqeu2tyr`, `gv1v3wzh`, `gmv00sa1`: the same removal across the MIM family.
- `g5s94jt5` **the list rule earning its keep**: OLD and THREE both say the passages "**do not specify
  Tanfel's own dealing with anodizing**"; C16 reads the bullet under Tanfel's own introducing sentence
  ("Here is an overview of Our Aluminum Extrusion Capabilities/Services: … - Finish: Anodizing, …")
  and writes "**Tanfel offers anodizing as part of its finishing services for aluminum extrusion
  products**". Correct, and only C16 gets it.
- `gsqiy4oj` same: "**EDM cutting is one of the operations Tanfel offers**", from "- Secondary
  operations: Machining, Grinding, EDM cutting, …" under the same sentence — while still reading the
  turning sentence as a general explanation. The one record where C16 separates the two correctly.

### g10 — C16 vs THREE: worse (6 of 6 shown as 5) — reason class: *restating closer*

- `gdookuiw` C16 ends "**Therefore, the snippet shows only a general description of aluminum extrusion
  manufacturing, not a specific dealing by Tanfel.**" after having said exactly that.
- `g4ipunv6`, `gqjg9w4i`, `gvuln6xj`, `g9rh2w1v`: word-for-word the same closer with the entity
  swapped. (`gindgrfr` is the sixth.)

### g15 — THREE vs OLD: better (26 of 26 shown as 5)

- `gxu47gb7` OLD "…**but the capacity—whether as manufacturer, supplier, or another role—is not
  specified**" → THREE "**Thread Plug Gages is listed as a metal-related capability or offering by
  Tanfel**".
- `gk3csh03` OLD hedges UV hard coating; THREE "**UV hard coating is a capability offered by Tanfel as
  part of its membrane switch and overlay product offerings**" — correct, the bullet sits under
  `#### Capabilities` on Tanfel's own membrane-switch page.
- `gfb0aiqo` identical correction for EMI, RFI and ESD shielding.
- `gm0e12ot`, `gp6smhba`: the Additional Capabilities entries credited instead of hedged.

**g15 — THREE vs OLD: worse (0),** with 24 vacuous clauses logged at §4e.

### g15 — C16 vs THREE: worse (26 of 26 shown as 5) — reason class: *over-hedge on own listings*

- `gxu47gb7` THREE clean → C16 "**The dealing is that Tanfel offers Thread Plug Gages for metal; the
  capacity is not specified in the snippet.**"
- `gk3csh03` THREE clean → C16 "…offers UV hard coating as a capability for its membrane switches …
  **the capacity is not specified in the snippet**" — on a page that says "Our membrane switches …
  **are manufactured** with strict process controls".
- `gfb0aiqo`, `gm0e12ot`, `gj3yttvy`: same clause, same cause.

C16 is better than THREE here only on wording, not on truth: "The dealing is that Tanfel **offers** X"
is a stronger crediting than THREE's passive "X **is listed** … by Tanfel", and C16 names the exact
industry page on `gku1flax` where THREE says "such as". Neither outweighs the hedge.

### rep — THREE vs OLD: better (9 of 9 shown as 5)

- `g4qrt3w2` **party fixed**: OLD "**Mathews & Company offers to apply** Pressure Sensitive Adhesives"
  → THREE "Mathews & Company offers or represents the application … **but does not state that it
  performs this service itself**". The "We" belongs to the kpl05 principal's entry.
- `gulv78zg` **party fixed**: OLD "**Mathews & Company offers to supply rapid prototype machined
  parts**" → THREE "…**does not state that it performs the machining itself**".
- `gjpkpgsp` OLD "**it represents or supplies access to this equipment**" → THREE "**does not state
  that it operates such equipment itself**".
- `gvoll26g`, `gbyy3ii7`: same removal of "supplies access".
- Across all 39, "the snippet …" 39 → 0.

### rep — C16 vs THREE: better (2), same (37), worse (0)

- `ghu2cklc` THREE "…**the capacity is that of a representative or distributor**" → C16 "Mathews &
  Company … **lists or represents smooth forged billet as a product offered by Ellwood City Forge**".
  Naming the maker settles the capacity instead of declaring it open. Both keep the party.
- `g7zeb1wf` the same on rough turned bar.
- The 37 "same" records are the pass's central negative result: **C16 hedges every one of them**, and
  in every one the doer stays with the principal. No represented maker's capability becomes Mathews's.

## 4. Regressions, exhaustively by record id

### C16 vs THREE

**4a. Over-hedge on own listings — 26, all in g15.** The capacity clause appended to a dealing the
frame has already licensed:

`gsj3z25c` `gffrcucn` `gud2hu64` `g8x90n25` `gq36rc0k` `gxu47gb7` `gdbukhyn` `ghllw04a` `g89d1iqh`
`gj9j0gnd` `gnabckk1` `gifzt4q3` `gi1e71a7` `g7ae933f` `ggh91ucl` `g3bi3j4r` `gvv3q5ze` `gg6psrhg`
`gm0e12ot` `gp6smhba` `gfmtr7f0` `gj3yttvy` `gm9hoa1m` `gku1flax` `gk3csh03` `gfb0aiqo`

Cause, read from the paragraphs themselves: C16's list rule fires (every one says "The context is a
list of what Tanfel offers", "The dealing is that Tanfel offers X"), and then the **unamended capacity
paragraph** fires on top of it. g15's lists are bare indices with no verb in them (`## Metal`,
`## Additional Capabilities`, `## Capabilities`), so the model classes them under "a list of what the
manufacturer sells, represents, or carries … when it says nothing about who made the things listed",
where the published prompt *requires* the hedge. The list rule gives the dealing but never says the
capacity clause is thereby discharged, so the two instructions stack. g13 and g10, whose lists are
introduced by "Our Capabilities", "Our Aluminum Extrusion Capabilities/Services" and "Our Services",
show 0 — the same rule, the same model, a verb in the introducing words.

**4b. Restating closer — 6, all in g10.** `gdookuiw` `g4ipunv6` `gindgrfr` `gqjg9w4i` `gvuln6xj`
`g9rh2w1v`. Each ends "Therefore, the snippet shows only a general description of …, not a specific
dealing by Tanfel", repeating the sentence before it and breaching the Task paragraph's existing
"do not close by restating what the synthesis has already said". Not a truth regression; it is the
main cause of g10's length rise (64.7 → 68.5 mean words).

**4c. Inherited from THREE, not introduced:** the "testimonial" frame on `g9awklq1` (§4d).

**4d. What did NOT regress in C16 — the classes this tryout was run to test.**
Wrong party **0/107**. A represented maker's capability credited to the representative **0/39**.
Laundering possessive **0/107**. Dealing supplied beyond the frame **0/107** (no "makes",
"manufactures" or "produces" written from a bare item anywhere; "manufacturing capabilities" appears
only where Tanfel's own page says "Our Capabilities"). Fabricated frame **0 new**. Byte-identical
siblings **0/107** in every arm.

### THREE vs OLD

**4e. Vacuous no-information clause — 24, all in g15.** `gsj3z25c` `gffrcucn` `gud2hu64` `g8x90n25`
`gq36rc0k` `gxu47gb7` `gdbukhyn` `ghllw04a` `g89d1iqh` `gj9j0gnd` `gnabckk1` `gifzt4q3` `gi1e71a7`
`g7ae933f` `ggh91ucl` `g3bi3j4r` `gvv3q5ze` `gg6psrhg` `gm0e12ot` `gp6smhba` `gfmtr7f0` `gj3yttvy`
`gm9hoa1m` `gku1flax`. Each ends "but the snippet alone does not provide further detail" (or, on
`gsj3z25c`, "do not specify further details about their use or features"). It replaces OLD's capacity
hedge with a clause that says nothing at all. Better than the hedge — it makes no false claim — but it
is a dropped sentence, and C16 removes it.

**4f. Frame misdescribed — 12, all in g13.** `gylk3k14` `gzco65b9` `gugo8jsx` `ghmgct1t` `gczn9kgc`
`gbmkfcf5` `g5eo8ybu` `g3kr36dq` `g6qut1hd` `gvkahl2q` `goc21ko6` `gq9hkhwi`. Each calls a caption /
alt-text list entry ("Metal: Lathe / Turning") "**a section heading**". OLD does this on 2 records
(`gylk3k14`, `gzco65b9`, where the text does show `## Industrial Metrology: …` and it is correct).
C16 fixes all 12.

**4g. Fabricated frame — 1.** `g9awklq1`: THREE and C16 both call "We understand the unique
requirements in the music industry and can help you with your prototype needs up to large production
and supply chain management" **a testimonial**. It is Tanfel's own copy on tanfel.com/industries/music/,
directly under "Tanfel's music career is not found on stage…". OLD reads it correctly as "a sentence
on an industry page". A customer's words are misattributed to a customer that does not exist. This is
the only unsupported claim found in 321 paragraphs, and it is introduced by THREE. (`gonlxrb1`, which
all three arms call a testimonial, genuinely is one: "We have been using Tanfel as a supplier of custom
fabrications for 7 years.")

**4h. Wire-vocabulary ban only half-holds on the production model — not a regression, a failure to
take.** THREE's and C16's ban on "snippet" is obeyed perfectly on the representative request (OLD
39/39 → 0/39 in both arms) and on g13 in C16 (1/15), and **ignored** on g15 (THREE 32/33, C16 31/33)
and on g13 in THREE (14/15). The edit was validated at 0/111 on the weak model; on gpt-4.1 it is
request-dependent. Publishing it costs nothing — it never makes a record worse — but it should not be
recorded as settled.

**4i. What did NOT regress in THREE.** Wrong party 0/107 (and OLD's 2 slides are fixed). Laundering
0/107. Dealing supplied beyond the frame 0/107 (OLD's 9 "supplies access to this equipment" /
"offers to apply" are removed). Byte-identical siblings 0/107. Over-hedge introduced on the
representative where hedging is right: 0 — THREE keeps the hedge in every one of the 39, phrased as a
doer negation ("does not claim to perform it itself") rather than with the word "capacity", which is
why a word count of "capacity" on the rep arm (OLD 37, THREE 2) misreads as a loss and is not one.

## 5. Length

| request | OLD | THREE | C16 | THREE vs OLD | C16 vs THREE |
|---|---|---|---|---|---|
| g13 | 82.6 (67–111) | 60.0 (47–92) | **49.0 (36–97)** | −27% | −18% |
| g10 | 65.2 (45–106) | 64.7 (33–114) | 68.5 (33–145) | −1% | +6% |
| g15 | 58.1 (45–128) | **51.2 (37–110)** | 57.4 (40–121) | −12% | +12% |
| rep | 77.1 (49–133) | **60.2 (41–111)** | 63.3 (47–114) | −22% | +5% |
| **all 107** | **69.9** | **58.4** | **60.4** | **−16%** | **+3%** |

THREE is shorter than OLD in every request, mostly by deleting OLD's habit of re-quoting every snippet
inside the paragraph. C16 is shorter still on g13 (the cleanest arm in the pass: 49 words, one
sentence of place and one of dealing) and longer elsewhere for two identifiable reasons — the restating
closer (§4b, g10) and the added "the capacity is not specified" sentence (§4a, g15). Both disappear
under the §6 revision, which should put C16 at or below THREE everywhere. No arm is too long to read;
C16's 145-word maximum (`gsqiy4oj`) is the one record where the extra length buys a real distinction.

## 6. Verdicts

### THREE — **PUBLISH**

The three edits were recommended in JUDGE_PASS2 §10.1 on weak-model evidence that could not exercise
their target. This tryout exercises it, on the production model, on requests that carry the defect,
with a 39-record representative as the control. The result holds in both directions:

- **The doer clause pays and costs nothing.** Party 39/39 on the representative, against OLD's 37/39,
  and it removes OLD's 9 supplied dealings ("represents or **supplies access to** this equipment",
  "Mathews & Company **offers to apply** Pressure Sensitive Adhesives"). It does not over-hedge Tanfel:
  0 party errors and 0 declined own-listings across 68 Tanfel records.
- **The first-person rule holds its carve-out.** `ghu2cklc` and `g7zeb1wf` — the two records new2 broke
  in the previous pass — are correct in both new arms: the "We specialize in a variety of custom
  forgings" stays with Ellwood City Forge.
- **The wire-vocabulary ban is free but not settled** (§4h): 0/39 on the representative, 31–32/33 still
  on g15. Publish it; do not record it as measured.
- **Incidentally, it moves the target too:** over-hedges 51 → 19 on the three tanfel requests, entirely
  in g10 and g15.

Residues (§4e–4g) are three, all shape or frame, none a truth claim about a party: 24 vacuous
clauses, 12 misnamed "section heading"s, 1 fabricated "testimonial". The first two are removed by C16;
the third is worth the two-sentence fix below whichever arm ships.

### C16 — **REVISE**, then re-run these same four requests

What the tryout was run to find out is answered: **the list rule does not move the doer.** Zero party
errors, zero laundering possessives, zero represented capabilities credited to the representative
across 39 control records, and 25 of 25 over-hedges removed wherever the introducing words carry a
verb. "What the item takes from that sentence is the dealing, never the doer" is the sentence that
makes edit (1) safe, and it should survive into whatever ships.

It is not publishable as it stands because on a bare capabilities index it leaves the defect exactly
where OLD had it (26 records, §4a) — the list rule and the capacity paragraph both fire, and nothing
tells the model which governs. One blocking replacement, two non-blocking.

#### B1 — blocking. The capacity clause must not fire on the company's own offering words.

In the paragraph on what the manufacturer does with the entity, replace:

> Where they show a dealing but leave its capacity open, as a list of what the manufacturer sells,
> represents, or carries does when it says nothing about who made the things listed, say the dealing as
> shown and that the capacity is unstated; do not supply one.

with:

> Where they show a dealing but leave its capacity open, say the dealing as shown and that the capacity
> is unstated; do not supply one. A list leaves the capacity open when the words that introduce it put
> the making with someone else, or name no one at all, as a list of what the manufacturer sells,
> represents, or carries does when it says nothing about who made the things listed. It does not leave
> the capacity open when the words introducing it are the manufacturer's own words for its own
> offerings, capabilities, or services, whether those words stand in a sentence above the list or in the
> heading or page title the list sits under: there the dealing those words give is the whole answer, and
> no further clause about an unstated capacity belongs in the statement.

Then close the stacking at its other end, by appending to the list rule itself, after "What the item
takes from that sentence is the dealing, never the doer.":

> Having taken the dealing from such a sentence, do not then add that the capacity is unstated: that
> sentence has settled it.

(The two together are what the evidence asks for. The first is the cause — g15's lists have a heading
and no verb, so the model reaches for "sells, represents, or carries"; the second stops the two
instructions stacking even where the first is read narrowly.)

#### B2 — non-blocking. A decline is said once, not twice.

In the Task paragraph, after "Say where it sits once, and do not close by restating what the synthesis
has already said.", add:

> This holds when the statement declines a dealing as much as when it gives one: say once that the
> passage shows no dealing with the focal entity, in the sentence that places the passage, and do not
> add a closing sentence that says it again.

(Fixes §4b's six records. Without it C16 spends its length budget restating declines.)

#### B3 — non-blocking. Do not invent a speaker.

In the Task paragraph's list of places a passage can sit, extend "a customer's words" to:

> a customer's words — only where the text shows someone other than the company speaking, since the
> company's own we or our on its own page is the company speaking and never a customer's words —

(Fixes §4g. One record here, but it is the only false claim in the pass and it is the kind that a
reader cannot detect from the statement alone.)

#### After these

Re-run g13, g10, g15 and rep on gpt-4.1 at temperature 0, seed 12345 and read all 107 again. The
expected result is g15's 26 over-hedges going to 0 with g13 and g10 staying at 0, the representative's
39 hedges and 39 correct parties unchanged, g10's 6 closers gone, and mean length at or below THREE in
every request. If g15's 26 do not move, the cause is not this paragraph and the next thing to test is
whether the heading alone — with no sentence anywhere on the page — is enough of an introducing word
for the model, which this pass could not separate.

#### What to watch on any production run

The party, on any subject that shows another company's line. C16 is clean on 39 such records here, but
39 records of one representative is one site. And the wire-vocabulary ban (§4h), which the weak model
made look settled and the production model does not.

---

# 7. Arm c16b — B1 (both halves), B2 and B3 applied

Judged 2026-09-12, same four requests, gpt-4.1, temperature 0, seed 12345. `c16b_system.txt` diffs
against `c16_system.txt` in exactly the three places §6 asked for, verbatim. All 107 records read
against THREE and C16.

## 7.0 Headline

**B2 and B3 worked completely. B1 did not, and it broke the party control.**

The two cheap fixes are clean: the six restating closers are gone (6 → 0) and the fabricated
testimonial is gone, with the one real testimonial still correctly named. c16b is also the shortest arm
in the pass by a wide margin (45.7 mean words against THREE's 58.2), and g13 is the best 15 records any
arm has produced.

B1 failed in both directions at once. On g15 it moved nothing: the same 26 records carry the same
hedge. On the sales representative it moved everything: the capacity-open answer vanished from all 39
records and **37 of them now end "as part of the product line Mathews & Company offers"** — because
B1's own words licensed it. The page is titled "# Kansas Product Line": the representative's own words,
for its own product line, standing as "the heading or page title the list sits under". A line card
satisfies B1's carve-out verbatim. That is the one class this programme is not allowed to regress.

## 7.1 Per-request grade tables (THREE / C16 / c16b)

### g13 — 15 records

| axis | THREE | C16 | c16b |
|---|---|---|---|
| frame accurate | 3 | 15 | **15** |
| dealing credited clean | 1 | 14 | **14** |
| **target over-hedge** | **14** | **0** | **0** |
| dealing supplied beyond the frame | 0 | 0 | 0 |
| party correct | 15 | 15 | 15 |
| laundering | 0 | 0 | 0 |
| wire vocabulary | 14 | 1 | 1 |
| restating closer | 0 | 0 | 0 |
| unsupported claim | 0 | 0 | 0 |
| focus kept | 15 | 15 | 15 |
| mean words | 60.0 | **49.0** | 50.0 |

### g10 — 20 records

| axis | THREE | C16 | c16b |
|---|---|---|---|
| frame accurate | 19 | 19 | **20** |
| own listings credited clean (of 11) | 6 | 11 | **11** |
| **target over-hedge (of 11)** | **5** | **0** | **0** |
| list item credited from the introducing sentence | 0 | 2 | **2** |
| explainer correctly declined (of 8) | 8 | 8 | 8 |
| **restating closer** | 0 | **6** | **0** |
| **fabricated frame ("a testimonial")** | **1** | **1** | **0** |
| party correct | 20 | 20 | 20 |
| wire vocabulary | 10 | 7 | **2** |
| focus kept | 20 | 20 | 20 |
| mean words | 64.7 | 68.5 | **55.5** |

### g15 — 33 records

| axis | THREE | C16 | c16b |
|---|---|---|---|
| frame accurate | 33 | 33 | 33 |
| own listings credited clean (of 26) | **26** | 0 | 0 |
| **target over-hedge (of 26)** | **0** | **26** | **26** |
| vacuous "no further detail" clause | 24 | 0 | **0** |
| party correct | 33 | 33 | 33 |
| laundering | 0 | 0 | 0 |
| wire vocabulary | 32 | 31 | 29 |
| focus kept | 33 | 33 | 33 |
| mean words | 51.2 | 57.4 | **45.5** |

### rep — 39 records, the party control

| axis | THREE | C16 | c16b |
|---|---|---|---|
| frame accurate | 39 | 39 | 39 |
| **capacity-open answer present where the frame requires it** | **39** | **37 + 2 by naming the maker** | **2** |
| **capacity-open answer lost** | **0** | **0** | **37** |
| …of which the other party is still named in the statement | — | — | 14 |
| …of which no party is named and the dealing reads as Mathews's | — | — | **21** |
| …of which a principal's doing is asserted as Mathews's own | — | — | **2** |
| **party correct** | **39** | **39** | **37** |
| laundering possessive | 0 | 0 | **0** |
| dealing supplied beyond the frame | 0 | 0 | **21** |
| wire vocabulary | 0 | 0 | 0 |
| focus kept | 39 | 39 | 39 |
| mean words | 60.2 | 63.3 | **39.1** |

### The pass in one table

| | OLD | THREE | C16 | c16b |
|---|---|---|---|---|
| target over-hedges, g13 + g10 + g15 | 51 | 19 | **26** | **26** |
| …g13 + g10 (lists introduced by a verb or a possessive) | 25 | 19 | **0** | **0** |
| …g15 (a bare-noun capabilities index) | 26 | 0 | 26 | 26 |
| lost hedge on the representative (of 39) | 0 | 0 | 0 | **37** |
| party errors on the representative | 2 | 0 | 0 | **2** |
| supplied dealing on the representative | 9 | 0 | 0 | **21** |
| restating closers | 0 | 0 | 6 | 0 |
| fabricated frames | 0 | 1 | 1 | 0 |
| mean words, all 107 | 69.9 | 58.2 | 60.4 | **45.7** |

## 7.2 Better / same / worse — c16b vs THREE

| request | better | same | worse |
|---|---|---|---|
| g13 | **14** | 1 | 0 |
| g10 | **8** | 12 | 0 |
| g15 | 0 | 7 | **26** |
| rep | 2 | 0 | **37** |
| **all 107** | **24** | **20** | **63** |

## 7.3 The three questions this arm was run to answer

### (1) On the representative, did the capacity-open answer vanish — and is the party carried by other words?

**It vanished on 37 of 39, and the party survives on only 16 of them.** The regression is real, and it
splits three ways.

**(a) The party is genuinely carried by other words — 16 records.** Here a reader holding only the
statement can still tell whose doing it is, because another party is named inside it:

- `gjpkpgsp` "'9-axis Swiss machines' are included in **the equipment list of a company that does
  precision machining**, as part of the product line Mathews & Company offers." (Same shape on
  `gvoll26g`, `gkxhdeph`, `ghhnmcs9`, `g63nk11h`, `gbyy3ii7`, `gb7lm6fw`, `g8nedk2z`.)
- `ghu2cklc` "…is listed among the custom forgings that **Ellwood City Forge specializes in**, but the
  passage is about Ellwood City Forge and **does not show any dealing by Mathews & Company beyond
  listing this information**." This is the **best reading of this record in the whole pass** — better
  than THREE's "the capacity is that of a representative or distributor" and better than C16's. Same
  on `g7zeb1wf`.
- `g8c93upg` "'Aluminum Die Casting' is described as being produced by **an American manufacturer**…"
  (also `gef1saje`, `gpa4z2mo`); `gcsfu74l` and `ga39any9` name STADCO Precision; `gpoeco15` names Sun
  Microstamping.

Even in these 14 of 16 the required hedge is gone, but no reader is misled.

**(b) No party is named and the dealing reads as the representative's own — 21 records.** These are the
regression. The source sentence's doer is a principal's "They" or "We"; c16b deletes it and leaves only
Mathews:

- `gwnanq35` "'Die Cutting' is listed among the services offered (along with Water Jet Cutting,
  Slitting, …) **as part of the product line Mathews & Company offers**." The source is "**They** offer
  Die Cutting Water Jet Cutting…" inside the kpl05 principal's entry. THREE: "…**but does not claim to
  perform it itself**." C16: "…**the capacity in which it does so is not specified**." c16b: nothing.
  A downstream reader takes die cutting as a service Mathews offers. Same shape on `grrakqm9`,
  `gl1imbur`, `gnwwkljf`, `gcwci3hl`, `gdt982tg`.
- `g8xwhmip` "'tapping' is listed as one of the secondary operations included with stampings, **as part
  of the product line Mathews & Company offers**" — the operations belong to the kpl02 principal.
  Same on `g1pc2e6i`, `gkuvt5np`, `gxensd0m`.
- `gmqxyi1d` "'floor molding' is listed as one of the processes available for castings ranging from
  1 lb to 10,000 lbs, **as part of the product line Mathews & Company offers**." Same on `gluqsr68`,
  `gptrnivb`, `gv3i8pzu`.
- Also `gbqwv8mr`, `g8rnu8jo`, `glsv1e4i`, `gbkrl7iv`, `g64ggddx`, `g4qrt3w2`, `glrjcsaw`.

**(c) A principal's doing asserted as Mathews's own — 2 records, a straight party error.**

- `gsawivwk` "**Mathews & Company states it can supply rapid prototype machined**, as part of its
  product line." The "We can supply your rapid prototype machined" sits inside the kpl03 entry and is
  the principal's first person; THREE's doer clause is what catches it, and B1 overrides the catch.
- `gulv78zg` "'rapid prototype machined' is stated as something **Mathews & Company can supply**."

OLD made exactly these two errors; THREE and C16 fixed both; c16b brings them back.

**The cause is B1's own text.** The Kansas Product Line page is titled "# Kansas Product Line" — the
representative's own words for its own product line, standing as "the heading or page title the list
sits under". B1 then says the capacity is *not* open there. The carve-out as written cannot tell a
company's capabilities index from a company's line card, because both are the company's own words for
its own lines. Nothing about the entries enters the test.

### (2) On g15, why did the 26 over-hedges not move?

**Not because the model misread the page — because B1 leaves the discharge to the model's judgement,
and on a bare-noun heading with a one-snippet record it declines.** Three facts settle it:

1. **The site text is byte-identical across g13, g10 and g15** (md5 `e31d6998…` for all three). Same
   pages, same headings, same "## Our Capabilities" and "#### Our Services" available to all three
   requests. So nothing about the page explains a 0/0/26 split.
2. **The model names the frame correctly in every one of the 26.** "…an entry in the metal capabilities
   list on **Tanfel's 'Metal' capabilities page**", "…in the **'Additional Capabilities' list** on
   Tanfel's website", "…the **list of capabilities for Membrane Switches** on Tanfel's 'Membrane
   Switches' product page". It reads the list as Tanfel's own capabilities, credits "**Tanfel offers X
   as a capability**" — and then adds "**but the capacity is not specified in the snippet**" anyway.
3. **Where the carve-out is cited it is obeyed.** g13's 15 records recite it: "…in the 'Related
   Products' list, the 'CAPABILITIES' list, and the 'Metal' section on Tanfel's website, **each of
   which presents offerings or capabilities**. These placements show that Tanfel offers Casting
   Finishes…" — with the same headings that g15 hedges under. g10's cite the possessive directly: "On
   the **'Our Capabilities'** page…", "in the **'Our Services'** lists…", "under **'Our Advantage'**".

The difference is what each record hands the model. g13's index records carry three snippets apiece
(`Related Products: X`, `CAPABILITIES: X`, `Metal: X`) and g10's carry a snippet with the possessive in
it (`Our Capabilities: MIM - Metal Injection Molding`). **24 of g15's 26 carry a single bare entry** —
`Metal: Carbide`, `Additional Capabilities: Plastic Products`, `- UV hard coating` — under headings
that are bare nouns with no verb and no possessive: `## Metal`, `## CAPABILITIES`, `## Additional
Capabilities`, `#### Capabilities`. B1 asks whether those are "the manufacturer's own words for its own
offerings, capabilities, or services". A bare category label is a judgement call, and on a one-snippet
record the model calls it the other way — then locks that template for the request (`gsj3z25c` carries
three snippets and is hedged with the rest, so the template, not the evidence, governs once set).

**B1 is therefore too weak in one place and too strong in another, and the two failures have one
root:** it tests the *words above the list* and never the *entries in it*. Tanfel's bare `## Metal`
index fails a test it should pass; Mathews's `# Kansas Product Line` passes a test it should fail.

### (3) Did B2 remove the closers and B3 the testimonial?

**Both, completely.**

- **B2 — yes, 6 → 0.** `gdookuiw`, `g4ipunv6`, `gindgrfr`, `gqjg9w4i`, `gvuln6xj`, `g9rh2w1v` each now
  end on the single placing sentence: "This passage explains the process in general terms and **does
  not specifically attribute the activity to Tanfel.**" C16's "Therefore, the snippet shows only a
  general description of …, not a specific dealing by Tanfel" is gone from all six, and no new closer
  appears anywhere in the 107. g10's mean length falls 68.5 → 55.5 words on this alone.
- **B3 — yes, and it cuts only the false one.** `g9awklq1` was "a testimonial" in THREE and C16; c16b:
  "on the 'Audio - Music' industry page, **Tanfel states that it can help** with prototype needs up to
  large production and supply chain management" — correct, it is Tanfel's own page copy. `gonlxrb1`,
  the genuine one, is still named as one and named more precisely: "In the **customer testimonial
  section on the main page, a customer states**…" (verified: the homepage heading is "## What customers
  say"). Unsupported claims across 107 records: **0**, the only arm in the pass to reach it.

## 7.4 Regressions in c16b, exhaustively by record id

### 7.4a Lost hedge on the representative — 37 (the blocking class)

Every rep record except `ghu2cklc` and `g7zeb1wf`, which need no hedge because they name the maker:

`gbqwv8mr` `gpa4z2mo` `ga39any9` `gsawivwk` `gulv78zg` `g8c93upg` `gef1saje` `g8rnu8jo` `glsv1e4i`
`g8xwhmip` `g1pc2e6i` `gkuvt5np` `gxensd0m` `gbkrl7iv` `g64ggddx` `gwnanq35` `grrakqm9` `gl1imbur`
`gnwwkljf` `gcwci3hl` `gpoeco15` `gdt982tg` `g4qrt3w2` `glrjcsaw` `gmqxyi1d` `gluqsr68` `gptrnivb`
`gv3i8pzu` `g8nedk2z` `gcsfu74l` `gjpkpgsp` `gvoll26g` `gkxhdeph` `ghhnmcs9` `g63nk11h` `gbyy3ii7`
`gb7lm6fw`

### 7.4b Of those, no party named — the dealing reads as the representative's own — 21

`gbqwv8mr` `g8rnu8jo` `glsv1e4i` `g8xwhmip` `g1pc2e6i` `gkuvt5np` `gxensd0m` `gbkrl7iv` `g64ggddx`
`gwnanq35` `grrakqm9` `gl1imbur` `gnwwkljf` `gcwci3hl` `gdt982tg` `g4qrt3w2` `glrjcsaw` `gmqxyi1d`
`gluqsr68` `gptrnivb` `gv3i8pzu`

### 7.4c Wrong party — a principal's doing asserted as the representative's own — 2

`gsawivwk` `gulv78zg`

### 7.4d Over-hedge on own listings — 26, all g15, unchanged from C16

`gsj3z25c` `gffrcucn` `gud2hu64` `g8x90n25` `gq36rc0k` `gxu47gb7` `gdbukhyn` `ghllw04a` `g89d1iqh`
`gj9j0gnd` `gnabckk1` `gifzt4q3` `gi1e71a7` `g7ae933f` `ggh91ucl` `g3bi3j4r` `gvv3q5ze` `gg6psrhg`
`gm0e12ot` `gp6smhba` `gfmtr7f0` `gj3yttvy` `gm9hoa1m` `gku1flax` `gk3csh03` `gfb0aiqo`

### 7.4e What did not regress

Laundering possessive **0/107**. Byte-identical siblings **0/107**. Restating closers **0/107**.
Fabricated frames **0/107**. Unsupported claims **0/107**. Wire vocabulary: rep 0/39, g13 1/15,
g10 2/20, g15 29/33 — unchanged in kind from THREE and C16 (§4h). Frame accuracy is the best of the
four arms: 107/107, including the 12 "section heading" slips THREE made and the "testimonial" both
earlier arms made.

## 7.5 Length

| request | THREE | C16 | c16b | c16b vs THREE |
|---|---|---|---|---|
| g13 | 60.0 (47–92) | 49.0 (36–97) | 50.0 (42–84) | −17% |
| g10 | 64.7 (33–114) | 68.5 (33–145) | **55.5 (31–122)** | −14% |
| g15 | 51.2 (37–110) | 57.4 (40–121) | **45.5 (34–82)** | −11% |
| rep | 60.2 (41–111) | 63.3 (47–114) | **39.1 (24–88)** | **−35%** |
| **all 107** | 58.2 | 60.4 | **45.7** | **−21%** |

c16b is the shortest arm everywhere, and on g13 and g10 the shortening is pure gain — B2 deleting a
repeated sentence, and the crisper frames. On the representative the −35% is **not** a gain: it is
mostly the deleted hedge clause. A rep record that has lost its "does not claim to perform it itself"
is shorter and wrong, not shorter and better; length on that request should be read only alongside
§7.4a.

## 7.6 Verdict on c16b: **REVISE**

c16b holds the two cheap wins outright and should never be re-litigated: B2 and B3 are verified at 0
closers and 0 fabricated frames with no cost anywhere, and they are the only sentences in the pass that
improved every request they touched. **Keep them.**

B1 must come out in its present form. It is not a partial success to be tuned: it changed nothing
where it was aimed (g15, 26 → 26) and did all its work where it was not (the representative, 0 → 37
lost hedges, 0 → 2 party errors). One sentence causes both.

### R1 — blocking. The carve-out must read the entries, not the page title.

Replace, in the capacity paragraph:

> It does not leave the capacity open when the words introducing it are the manufacturer's own words
> for its own offerings, capabilities, or services, whether those words stand in a sentence above the
> list or in the heading or page title the list sits under: there the dealing those words give is the
> whole answer, and no further clause about an unstated capacity belongs in the statement.

with:

> It does not leave the capacity open when the company's own words present the list as what the company
> itself does, offers, or holds, and nothing in the list or around it puts any item with another party.
> An index of the company's own work is such a presentation even where its heading is a bare noun with
> no verb and no our in it — a page or section that gathers kinds of work under the company's
> capabilities, services, or products is the company saying these are its own — and there the dealing
> that gathering gives is the whole answer, with no further clause about an unstated capacity. But
> where the entries name other companies, describe their doings, or speak in their words, the list
> leaves the capacity open for every item in it, however the page above it is titled: a page that
> carries a company's line, product line, or represented makers is not that company saying it does
> these things.

The first half is what g15's 26 need: it names the bare-noun index and removes the judgement call.
The second half is what the representative's 37 need: it puts the entries, not the title, in charge,
and it names the line card explicitly.

### R2 — blocking, one sentence, and cheap insurance for the doer clause.

The doer clause survived B1 on 16 rep records and lost on 23. Pin the precedence in the list rule,
after "Having taken the dealing from such a sentence, do not then add that the capacity is unstated:
that sentence has settled it.":

> This never overrides the rule above about whose doing it is: where the item, its entry, or the
> passage around it puts the doing with another party, that party keeps it and the capacity stays
> open, whatever the page is called.

### After these

Re-run the same four requests. The target is g13 0, g10 0, **g15 0**, and the representative back to
39 hedges and 39 correct parties with c16b's frames and lengths. If g15 still holds at 26 after R1's
first half, stop editing the capacity paragraph: the remaining cause is that a one-entry record gives
the model nothing to place, and the fix belongs upstream in what a record carries, not in the prompt.

---

# 8. Recommendation across the four production-model arms

**Ship C16 — THREE plus the list rule alone — and fold in B2 and B3 from this pass after one
confirming run. Do not ship B1 in any form yet.**

| | over-hedge (68 tanfel records) | rep hedges kept (of 39) | rep party (of 39) | closers | fabricated frames | mean words |
|---|---|---|---|---|---|---|
| OLD (published) | 51 | 37 | 37 | 0 | 0 | 69.9 |
| THREE | 19 | 39 | **39** | 0 | 1 | 58.2 |
| **C16** | **26**, and **0** on verb-introduced lists | 39 | **39** | 6 | 1 | 60.4 |
| c16b | 26 | **2** | 37 | **0** | **0** | **45.7** |

**Why not c16b.** It is the most attractive arm on every axis except the one that decides: it loses the
capacity-open answer on 37 of 39 representative records, hands a principal's doing to the
representative twice, and leaves 21 more where a downstream reader has no way to tell whose doing it
is. Every previous pass has ruled that class blocking, and it buys nothing for it — g15 is unchanged.

**Why not THREE alone.** THREE is the safe arm and it is genuinely safe: 0 regressions in four arms,
party 39/39, 51 → 19. But 19 of the 25 over-hedges that C16 removes are live majors on the
production model, and C16 removes them at a measured cost of zero on the control. Shipping THREE alone
is leaving a validated fix on the table to avoid a risk this pass has now tested and not found.

**Why C16.** It is the only arm that removes the target where the frame plainly licenses the offering —
**25 → 0 on g13 and g10** — while holding everything THREE holds: party 39/39 on a 39-record
representative, 0 laundering, 0 supplied dealing, 0 byte-identical siblings. On g15 it neither helps
nor harms: 26 over-hedges, the same 26 the published prompt writes today, so shipping it cannot make
any record worse than production is now. Its two residues are the six restating closers and the
inherited "testimonial", and this pass has already found and verified the exact sentences that remove
both.

**The sequence.**

1. Publish C16 (`three_system.txt` + the list rule, i.e. the current `c16_system.txt`) as the static.
   Expected on a production run: the own-listing over-hedge drops wherever a listing sits under an
   introducing verb or a possessive, and does not move on bare capabilities indices; no change on
   parties; a small length rise from the restating closers.
2. Fold B2 and B3 into it as arm `c16c` and run these same four requests once. Both are Task-paragraph
   sentences that never touch the capacity paragraph; c16b measured them at 6 → 0 closers and 1 → 0
   fabricated frames with no cost. One run confirms they behave the same without B1 beside them.
3. Only then take up the capacity paragraph again, with R1 and R2 of §7.6, and judge it on the same
   four requests plus at least one further representative or distributor — 39 records of one site is
   thin evidence for a rule that, on this pass, turned out to be decided by a page title.

**What this pass established that the weak model could not.** The list rule's feared cost — a
represented maker's capability becoming the representative's — does **not** occur on the production
model when the rule is paired with THREE's doer clause: 0 in 39 control records, twice
(C16 and, on its 16 party-carrying records, c16b). What does occur, and what no weak-model arm could
have shown, is that a rule keyed on the words above a list will read a line card and a capabilities
index the same way. The entries, not the heading, are where the party lives.
