# JUDGE_P3 — R2 and R1+B2+B3 on the production requests that carry C21 and C22

Judged 2026-09-12. Arms: **PUB** = `c16_system.txt`, the static now in production; **P3A** = PUB + R2
(the doer-precedence sentence appended to the list rule); **P3B** = P3A + R1 (the capacity clause
rewritten with a carve-out that reads the entries and names the bare-noun index) + B2 (a decline said
once) + B3 (never invent a customer speaker). `diff`ed and confirmed: PUB→P3A is R2 alone; P3A→P3B is
R1 + B2 + B3, verbatim from JUDGE_C16 §7.6.

Two call shapes were run. **Strict** (`_41s`) is the PRODUCTION call shape — `GPTModelParams` as the
notebook builds them plus `SYNTHESIS_RESPONSE_SCHEMA` attached by the synthesis node; this is the
primary evidence. **Loose** (`_41`) is a direct completion with no wire schema; secondary. Both:
gpt-4.1, temperature 0, seed 12345. 135 records × 3 arms × 2 modes = **810 paragraphs, every one read**.

## Headline

**Neither R2 nor R1 can be validated on this evidence, and R1 must not ship.**

1. **The tryout does not reproduce production on C21.** On the 17 verified J1+J6+J7 majors of
   `mathewsco` the production run wrote a flat *"so the snippet shows Mathews & Company offering X"*.
   The PUB arm — same static, same request, same model, same params — **hedges all 17, in both call
   shapes**. The defect this tryout was built to fix is not present in the tryout's control arm, so
   nothing measured here about C21 transfers.
2. **The two Mathews requests are the same request.** `req_mathewsco_g2_user.txt` and
   `synthesis_shape_tryout_20260909/1/A_user.txt` differ by exactly three lines: a `request nonce`
   line, a blank line after it, and one blank line at 847. `diff` is otherwise empty. They are
   therefore an **A/A pair**, and the arms diverge across it violently: strict P3A writes a capacity
   clause on 33/39 records of `mw2` and on **0/39** of `rep`; strict P3B hedges 37/39 of `mw2` and
   **launders 35/39** of `rep`. Same prompt, same input, opposite party verdicts.
3. **R1 (P3B) re-breaks the party control exactly as B1 did.** On `rep`, P3B writes *"The context is a
   list of product lines Mathews & Company offers, so the page shows Mathews & Company offering X"* on
   35 of 39 strict records (31 of 39 loose) — the production laundering sentence, restored verbatim in
   shape. This is the same failure JUDGE_C16 §7.4 recorded for c16b, on the same 39 records.
4. **Nothing fixes C22.** Own-listing over-hedge across the 46 tanfel own-listing records, strict:
   **PUB 42, P3A 42, P3B 43.** P3A does not remove the hedge, it converts it into a stronger *decline*
   ("does not … show any dealing by Tanfel beyond its inclusion in the list") on 24 g15 records,
   including 4 that PUB had written cleanly. P3B softens the clause but keeps it on 23.
5. **The one place P3B looked like a fix does not survive the production call shape.** In loose mode
   P3B takes g15's capacity clause 26 → **0** and writes the ideal answer. In strict mode the same arm
   on the same request writes it on 22. The wire schema flips the result.

**Recommendation: ship none of them. Keep PUB. Do not re-litigate R1 on a Mathews-only control.**

## 1. Method

- **Files.** Requests: `pass2/req_mathewsco_g2_user.txt` (mw2, 39 records), `pass2/req_tanfel_g13_user.txt`
  (15), `pass2/req_tanfel_g15_user.txt` (33), `pass2/req_tanfel_c8_user.txt` (9), and
  `../synthesis_shape_tryout_20260909/1/A_user.txt` (rep, 39). Outputs `pass2/p3_out_<req>_<arm>_41s.json`
  (strict, primary) and `..._41.json` (loose, secondary). All 30 files parsed; record ids identical
  across arms in every request, so every comparison is record-for-record.
- **Reading, not counting.** Every paragraph was read against its own snippets and against the passage
  the snippets sit in inside the request's site text. Regex appears only as a nomination and only where
  the reading confirmed it; two of the pre-read's mechanical counts were wrong in the same direction
  (below) and the reading governs.
- **Corrections to the mechanical pre-read.** (a) "the published static on mw2 shows 37 records with an
  unhedged `Mathews & Company offers/has …`" — false. PUB's sentence is *"Mathews & Company offers X as
  part of its Kansas product line; **the capacity is not specified**"*: the hedge is there, after the
  semicolon, on 36 of 39. (b) "P3A takes both to 0" — P3A on `rep` has no capacity *word*, but its
  sentence is *"Mathews & Company **lists or represents** X"*, which carries the party correctly; it is
  not a lost hedge, it is a better one. (c) g15 "p3a 1" counts capacity words only and misses that P3A
  replaced the hedge with a flat decline on 24 records, which is worse, not better.
- **Frame ruling applied** (JUDGE_C16 §1a, unchanged): Tanfel is a maker and every listing family in
  g13/g15/c8 sits under its own CAPABILITIES / Related Products / Metal breadcrumb, so "offers" is
  licensed and "capacity unstated" is the defect. Mathews & Company is a representative and nothing
  else, so hedging or naming the principal is right on all 39 and a principal's doing must never become
  Mathews's own.
- **Taxonomy.** J1 faithfulness, J2 under-claim, J4 party attribution, J6 field serviceability, J7 agent
  laundering, as in `apps/data_etl_app/tests/test_stages/synthesis/TAXONOMY.md`.

## 2. Production versus tryout on the PUB arm

### 2a. The comparison is only valid on 58 of the 63 prod records

`prod_g13.json` carries, for five ids, a paragraph that cites page text **absent from
`req_tanfel_g13_user.txt`** and present only in `req_tanfel_c8_user.txt`: `gaqogzxh` and `g5eo8ybu`
("leading supplier for custom springs", "antenna springs"), `g3kr36dq` and `goc21ko6` ("corrosively
machines away", "Photo chemical machining"), `gq9hkhwi` ("cemented carbide", "Large Carbide"). Each
string greps 0 in the g13 request and 1 in the c8 request. Those production paragraphs were written from
a different chunk packing of the same record, not from this request, and are excluded. `prod_mw2` is
missing three of the request's ids (`gulv78zg`, `gpoeco15`, `g4qrt3w2`), leaving 36. Comparable:
mw2 36, g13 6, g15 10, c8 6 = **58**.

### 2b. Agreement on the full reading (party + capacity + frame)

| request | comparable | same reading | frame same | party same | capacity same |
|---|---|---|---|---|---|
| mw2 | 36 | **2** (5.6%) | 36 | 36 | **2** |
| g13 | 6 | **2** | 6 | 6 | 2 |
| g15 | 10 | **8** | 10 | 10 | 8 |
| c8 | 6 | **2** | 6 | 6 | 2 (same *defect class* 6) |
| **all** | **58** | **14 (24%)** | 58 | 58 | 14 |

The frame and the named party never disagree. **The whole disagreement is the capacity clause, and it
runs one way: the tryout hedges where production did not.**

### 2c. On the 17 C21 majors the tryout reproduces nothing

Production, `gwnanq35`: *"…This is part of the product line Mathews & Company offers, so the snippet
shows Mathews & Company offering Die Cutting."* — J1+J6+J7 major.
PUB tryout, strict, same record: *"…Mathews & Company offers Die Cutting as part of its Kansas product
line; **the capacity is not specified**."*
PUB tryout, loose: *"…The dealing shown is Mathews & Company offering Die Cutting; **the capacity is not
specified**."*

All 17 verified majors (`gbqwv8mr` `gpa4z2mo` `g8c93upg` `gef1saje` `glsv1e4i` `g8xwhmip` `g1pc2e6i`
`gkuvt5np` `gxensd0m` `gbkrl7iv` `g64ggddx` `gwnanq35` `grrakqm9` `gl1imbur` `gnwwkljf` `gcwci3hl`
`gdt982tg`) carry the hedge in the PUB arm in **both** call shapes. **PUB-tryout reproduces PUB-production
on 0 of 17.** The strict shape is closer in wording (the coordinator's 0.57 similarity against 0.40) but
not in reading: the extra similarity is the restored *"On the Kansas Product Line page, 'X' is described
as…"* opening, not the party sentence.

The converse also holds on the tanfel side of g13: production wrote `gaqogzxh`, `g5eo8ybu`, `g3kr36dq`,
`goc21ko6`, `gq9hkhwi` as clean, detailed offerings with no hedge — but from a richer chunk, so they
prove only that the same record reads differently depending on what is packed with it.

### 2d. Where the tryout DOES reproduce production: C22 on g15

| record | production | PUB strict tryout |
|---|---|---|
| `gj9j0gnd` | "Tanfel lists Lathe / Turning as one of its metal-related capabilities, **but the capacity — whether as manufacturer, supplier, or another role — is not specified**" (J2+J6 major) | "Tanfel offers Lathe / Turning as a capability, **but the capacity (such as whether Tanfel manufactures or sources it) is not specified**" |
| `gifzt4q3` | same shape (J2+J6 major) | same shape |
| `ggh91ucl` | same shape (J2+J6 major) | same shape |
| `gp6smhba` | same shape (J2 minor) | same shape |
| `g3n0t5p9` `go8yx79u` `glj3j97j` `gqwbdj9a` | correct decline on the blog explainer | correct decline |
| `gm9hoa1m` `gku1flax` | hedged (J2+J6 minor) | **clean** — "This shows that Tanfel offers Precision CNC-Machining as a capability for these industries" |

**So: this tryout path exercises C22 and does not exercise C21.** Read §3–§5 with that asymmetry in
mind; the arms can still be compared against each other, but any C21 conclusion drawn here is
unsupported, and this is the reason the C21 arms cannot be validated.

### 2e. The A/A finding that governs everything else

`req_mathewsco_g2_user.txt` and `synthesis_shape_tryout_20260909/1/A_user.txt` are byte-identical apart
from a `request nonce (ignore): …` line, the blank line after it, and one blank line at 847
(`diff` output is those three lines and nothing else). `mw2` and `rep` are therefore the **same request
run twice per arm per mode** — four independent A/A samples of 39 records:

| arm | mode | mw2 capacity-hedged | rep capacity-hedged | rep flat-laundered |
|---|---|---|---|---|
| PUB | strict | 36 | 35 | 0 |
| PUB | loose | 37 | 35 | 0 |
| P3A | strict | 33 | **0** (all 39 "lists or represents") | 0 |
| P3A | loose | 39 | 37 | 0 |
| P3B | strict | 37 | 4 | **35** |
| P3B | loose | 36 | 9 | **31** |

PUB is stable across the nonce. **P3A and P3B are not.** P3A's whole rep behaviour (the "lists or
represents" template) appears on one instance of the request and not on the other; P3B launders on the
`rep` instance in both modes and hedges on the `mw2` instance in both modes. A one-line nonce decides
whether 31–35 records get the party right. No party conclusion from a single 39-record Mathews request
is safe, and JUDGE_C16 §8's warning ("39 records of one representative is one site") understates it —
it is not even one measurement.

## 3. Per-request grade tables — STRICT (primary)

"Own listings" excludes explainer/blog records, which are graded on their own rows.

### mw2 — Mathews & Company, 39 records (the C21 request; every record another party's doing)

| axis | PUB | P3A | P3B |
|---|---|---|---|
| frame named and accurate | 39 | 39 | 39 |
| **party correct** | **39** | **39** | **39** |
| capacity-open answer given | 36 | 33 | 37 |
| principal named as the doer instead of hedging | 3 | 6 | 2 |
| flat "Mathews & Company offers X" with no hedge and no party | **0** | **0** | **0** |
| dealing supplied beyond the frame | 0 | 0 | 0 |
| laundering possessive | 0 | 0 | 0 |
| **wire vocabulary ("the snippet")** | **0** | **37** | **0** |
| unsupported claim | 0 | 0 | 0 |
| focus kept (no byte-identical sibling) | 39 | 39 | 39 |
| mean words | 55.9 | 58.7 | 59.7 |

### rep — the same request, nonce changed (the A/A twin)

| axis | PUB | P3A | P3B |
|---|---|---|---|
| frame named and accurate | 39 | 39 | 39 |
| **party correct** | **39** | **39** | **4** |
| **a principal's doing asserted as Mathews's own** | **0** | **0** | **35** |
| capacity-open answer given | 35 | 0 (carried by "lists or represents") | 4 |
| dealing supplied beyond the frame | 0 | 0 | 0 |
| laundering possessive | 0 | 0 | 0 |
| wire vocabulary | 0 | 0 | 0 |
| focus kept | 39 | 39 | 39 |
| mean words | 59.5 | 52.5 | 58.4 |

### g13 — Tanfel, 15 records (all own listings)

| axis | PUB | P3A | P3B |
|---|---|---|---|
| frame named and accurate | 15 | 15 | 15 |
| own listing credited clean | 0 | 3 | 1 |
| **target over-hedge — capacity clause** | **15** | 0 | 0 |
| **target over-hedge — flat "no further dealing / beyond listing"** | 0 | **12** | **14** |
| **target over-hedge, total** | **15** | **12** | **14** |
| party correct | 15 | 15 | 15 |
| laundering possessive | 0 | 0 | 0 |
| **wire vocabulary** | 15 | 12 | **15** |
| unsupported claim | 0 | 0 | 0 |
| mean words | 49.3 | 64.7 | **80.9** |

### g15 — Tanfel, 33 records (25 own listings incl. 20 bare-noun index entries; 8 blog/explainer)

| axis | PUB | P3A | P3B |
|---|---|---|---|
| frame named and accurate | 33 | 33 | 33 |
| own listing credited clean (of 25) | 4 | 1 | 2 |
| **target over-hedge — capacity clause** | **21** | 0 | **23** |
| **target over-hedge — flat decline** | 0 | **24** | 0 |
| **target over-hedge, total (of 25)** | **21** | **24** | **23** |
| explainer correctly declined (of 8) | 8 | 8 | 8 |
| party correct | 33 | 33 | 33 |
| **wire vocabulary** | 25 | 29 | **0** |
| unsupported claim | 0 | 0 | 0 |
| mean words | 50.9 | 49.0 | **44.6** |

### c8 — Tanfel, 9 records (6 own "Related Products" cross-links, 2 rich pages, 1 resource table)

| axis | PUB | P3A | P3B |
|---|---|---|---|
| frame named and accurate | 9 | 9 | 9 |
| rich-page records credited correctly (`g1hma18g`, `guzaeduj`) | 2 | 2 | 2 |
| resource table correctly declined (`gffoqmzp`) | 1 | 1 | 1 |
| **target over-hedge on the 6 cross-links** | **6** (flat decline) | **6** (capacity clause) | **6** (flat decline) |
| party correct | 9 | 9 | 9 |
| wire vocabulary | 7 | 7 | 7 |
| mean words | 78.0 | 81.0 | 69.8 |

### The two numbers that decide the pass — STRICT

| | PUB | P3A | P3B |
|---|---|---|---|
| **tanfel own-listing over-hedge (46 records: g13 15 + g15 25 + c8 6)** | **42** | **42** | **43** |
| **Mathews party errors, mw2 (39)** | **0** | **0** | **0** |
| **Mathews party errors, rep (39, same request)** | **0** | **0** | **35** |

### The same table — LOOSE (secondary)

| | PUB | P3A | P3B |
|---|---|---|---|
| tanfel own-listing over-hedge (46) | 45 | 41 | **20** |
| …of which g15 (25) | 25 | 21 | **0** |
| Mathews party errors, mw2 (39) | 0 | 0 | 0 |
| Mathews party errors, rep (39) | 0 | 0 | **31** |
| mean words, all 135 | 58.9 | 63.5 | 60.8 |

P3B's g15 result inverts between the two call shapes on the same request: **0 over-hedges loose, 23
strict**. That is the single reason the loose evidence cannot be used to publish R1.

## 4. Better / same / worse — STRICT

Ties on the dealing axis are broken by the dealing; where no truth changes, wire vocabulary and length
decide, as in JUDGE_C16 §3.

| request | P3A vs PUB | P3B vs P3A |
|---|---|---|
| mw2 (39) | better 3, same 2, **worse 34** | **better 35**, same 2, worse 2 |
| rep (39) | **better 30**, same 9, worse 0 | better 4, same 2, **worse 33** |
| g13 (15) | better 3, same 0, **worse 12** | better 1, same 1, **worse 13** |
| g15 (33) | better 1, same 8, **worse 24** | **better 24**, same 9, worse 0 |
| c8 (9) | better 2, same 7, worse 0 | better 0, same 3, **worse 6** |
| **all 135** | **better 39, same 26, worse 70** | **better 64, same 17, worse 54** |

### 4a. mw2 — P3A vs PUB: worse (34; reason class *wire vocabulary*)

R2 adds no truth on this request — both arms hedge — and P3A pays for it by opening the dealing
sentence with the banned word on 37 of 39 records.

- `gwnanq35` PUB "Mathews & Company offers Die Cutting as part of its Kansas product line; the capacity
  is not specified." / P3A "**The snippet shows** Mathews & Company offering Die Cutting … in an
  unstated capacity."
- `g8xwhmip`, `g1pc2e6i`, `gkuvt5np`, `gxensd0m` — the four secondary-operation records, identical
  substitution.
- `gl1imbur` P3A "**The snippet shows** Mathews & Company offering Slitting …".
- `gvoll26g` P3A "**The snippet shows** Mathews & Company offering access to CNC Machining …".
- `gbyy3ii7` P3A "**The snippet shows** Mathews & Company offering access to precision grinding …".

### 4b. mw2 — P3A vs PUB: better (3; reason class *doer named*)

- `ga39any9` P3A "…show Mathews & Company listing machining as part of its Kansas Product Line and
  **representing principals that perform machining, but do not show Mathews & Company performing
  machining itself**." PUB: "Mathews & Company offers machining-related services … and represents
  companies with machining capabilities; the specific capacity is not always stated."
- `g8rnu8jo` P3A names `rowleyspring.com` and `hudson-technologies.com` as the makers on the line card;
  PUB says only "part of its Kansas and California product lines".
- `gpoeco15` P3A "…**representing a principal that specializes in Injection Molding**".

### 4c. rep — P3A vs PUB: better (30; reason class *cleaner dealing, shorter*)

R2's doer clause produces a uniform, correct template with no capacity clause needed:
- `glsv1e4i` P3A "Mathews & Company **lists or represents** laser cutting as part of its Kansas product
  line." (PUB: "…indicating it offers or arranges for this service, with the capacity unstated.")
- `gwnanq35`, `grrakqm9`, `gl1imbur`, `gnwwkljf` — same substitution, −20 words each.
- `gcsfu74l` P3A "…and **presents information about companies it represents that perform precision
  machining**."

This is the best 39 records any arm produced in the pass. It is also the result that **does not
reproduce on the byte-identical `mw2` instance**, where the same arm writes "The snippet shows … in an
unstated capacity" instead — see §2e.

### 4d. rep — P3B vs P3A: worse (33; reason class *party laundered, R1's carve-out*)

R1's "an index of the company's own work … even where its heading is a bare noun" reads the Kansas
Product Line page as Mathews's own index and hands it the principals' doings:
- `gwnanq35` P3B "**The context is a list of product lines Mathews & Company offers, so the page shows
  Mathews & Company offering Die Cutting.**" The entry reads "**They** offer Die Cutting Water Jet
  Cutting, Slitting, Extrusions, Transfer, Injection and Compression Molding" — "They" is a principal.
- `g8c93upg` P3B "…so the page shows Mathews & Company offering Aluminum Die Casting, with the capacity
  left unstated." The entry: "**This American manufacturer** of aluminum die casting has position
  itself to compete against off shore manufacturer…".
- `gvoll26g` P3B "…so the page shows Mathews & Company offering CNC Machining **as part of its precision
  machining capabilities**." The entry: "**Their** equipment list includes … CNC Machining …". This one
  also supplies a capability ("its precision machining capabilities") the site never gives Mathews.
- `gbkrl7iv` P3B "…so the page shows Mathews & Company offering Rubber Fabrication." Entry describes a
  principal "with manufacturing and being a distributor of elastomeric rubber".
- `glrjcsaw` P3B "…so the page shows Mathews & Company offering Sand Castings."

### 4e. g15 — P3B vs P3A: better (24; reason class *credit restored, wire vocabulary gone*)

- `gj9j0gnd` P3A "…**The snippet only lists the term and does not provide further information or show
  any dealing by Tanfel beyond its inclusion in the list.**" / P3B "…**indicating that Tanfel offers
  Lathe / Turning as part of its metal-related capabilities**, but the capacity is not further
  specified in this listing."
- `gifzt4q3`, `ggh91ucl` — the other two verified J2+J6 majors, same substitution.
- `gnabckk1`, `gg6psrhg` — same.

P3B is right to credit the offering. It still appends the clause, so the J2 fail survives at reduced
severity; it is not the fix R1 was written to be.

### 4f. g15 — P3A vs PUB: worse (24; reason class *over-decline on the company's own index*)

- `gk3csh03` PUB **clean**: "UV hard coating is listed as a capability under the 'Capabilities' section
  on Tanfel's 'Membrane Switches' product page. This shows that Tanfel offers UV hard coating as a
  capability for its membrane switches and related products." / P3A "…**does not provide further
  information or show any dealing by Tanfel beyond its inclusion in the list of capabilities**."
- `gfb0aiqo`, `gm9hoa1m`, `gku1flax` — three more records PUB wrote cleanly and P3A declines.
- `gxu47gb7`, `gdbukhyn` — PUB hedged, P3A declines outright.

A flat "no dealing" is a worse downstream outcome than a hedged offering: the hedged form still mints a
weak tag, the decline mints none.

### 4g. g13 — P3A vs PUB and P3B vs P3A: worse (12 and 13; reason classes *weaker credit* and *explicit
"beyond listing" + wire vocabulary + length*)

- `gq9hkhwi` PUB "These placements show that **Tanfel offers Carbide** as part of its capabilities, but
  the capacity is not specified." / P3A "…**Tanfel lists Carbide among its offerings**, but the snippets
  do not provide further detail about the nature of the offering or Tanfel's specific dealing with it."
  / P3B "**The snippets for Carbide appear as entries** … but the snippets do not provide further detail
  about **what Tanfel does with Carbide beyond listing it** as a capability and related product."
- `gczn9kgc`, `ghmgct1t`, `g5eo8ybu`, `gvkahl2q` — identical three-way pattern.
- P3B opens all 15 g13 paragraphs with "**The snippets for** X appear as…" and runs 80.9 mean words
  against PUB's 49.3 (+64%) for no added truth.

### 4h. c8 — P3B vs P3A: worse (6; reason class *decline restored*)

- `gcweyxfl` (verified J2+J6 major) P3A "…Tanfel **presents** Metal Fused Filament Fabrication **as a
  related product**, but the specific capacity … is not stated." / P3B "…the snippets provided show
  only its mention as a related product and **do not show any further dealing by Tanfel** with Metal
  Fused Filament Fabrication beyond listing it."
- `ghjv3mc5`, `glqit6d2`, `gylk3k14`, `gzco65b9`, `gugo8jsx` — same.

## 5. Regressions, exhaustively by record id (STRICT unless marked)

### 5a. P3B, `rep` — a principal's doing asserted as Mathews & Company's own — 35 (BLOCKING)

`gbqwv8mr` `gpa4z2mo` `ga39any9` `gsawivwk` `gulv78zg` `g8c93upg` `gef1saje` `g8rnu8jo` `glsv1e4i`
`g8xwhmip` `g1pc2e6i` `gkuvt5np` `gxensd0m` `gbkrl7iv` `g64ggddx` `gwnanq35` `grrakqm9` `gl1imbur`
`gnwwkljf` `gcwci3hl` `gdt982tg` `g4qrt3w2` `glrjcsaw` `gmqxyi1d` `gluqsr68` `gptrnivb` `gv3i8pzu`
`g8nedk2z` `gjpkpgsp` `gvoll26g` `gkxhdeph` `ghhnmcs9` `g63nk11h` `gbyy3ii7` `gb7lm6fw`

Loose mode, same class, 31: the list above minus `gpa4z2mo` `ga39any9` `gsawivwk` `g8rnu8jo`. Of the 35,
four soften with a trailing "with the capacity left unstated" (`gpa4z2mo` `g8c93upg` `gef1saje`
`g8nedk2z`) and are still party errors — the sentence has already said Mathews offers it.

PUB and P3A on the same request: **0**.

### 5b. P3A, `mw2` — wire vocabulary in the dealing sentence — 37

`ghu2cklc` `g7zeb1wf` `gbqwv8mr` `gpa4z2mo` `ga39any9` `gsawivwk` `g8c93upg` `gef1saje` `g8rnu8jo`
`glsv1e4i` `g8xwhmip` `g1pc2e6i` `gkuvt5np` `gxensd0m` `gbkrl7iv` `g64ggddx` `gwnanq35` `grrakqm9`
`gl1imbur` `gnwwkljf` `gcwci3hl` `gpoeco15` `gdt982tg` `glrjcsaw` `gmqxyi1d` `gluqsr68` `gptrnivb`
`gv3i8pzu` `g8nedk2z` `gcsfu74l` `gjpkpgsp` `gvoll26g` `gkxhdeph` `ghhnmcs9` `g63nk11h` `gbyy3ii7`
`gb7lm6fw`

PUB on the same request: 0. P3B: 0. The static bans the word; P3A restores it on 95% of the request.

### 5c. P3A, `g15` — own index entry declined outright — 24

`gffrcucn` `gud2hu64` `g8x90n25` `gq36rc0k` `gxu47gb7` `gdbukhyn` `ghllw04a` `g89d1iqh` `gj9j0gnd`
`gnabckk1` `gifzt4q3` `gi1e71a7` `g7ae933f` `ggh91ucl` `g3bi3j4r` `gvv3q5ze` `gg6psrhg` `gm0e12ot`
`gp6smhba` `gfmtr7f0` `gm9hoa1m` `gku1flax` `gk3csh03` `gfb0aiqo`

Of these, **four were clean in PUB** and are therefore new defects, not inherited ones: `gm9hoa1m`
`gku1flax` `gk3csh03` `gfb0aiqo`. Loose mode, same class, 23.

### 5d. P3B, `g15` — capacity clause retained on the company's own index — 23 (R1's stated target, unmoved)

`gsj3z25c` `gffrcucn` `gud2hu64` `g8x90n25` `gq36rc0k` `gxu47gb7` `gdbukhyn` `ghllw04a` `g89d1iqh`
`gj9j0gnd` `gnabckk1` `gifzt4q3` `gi1e71a7` `g7ae933f` `ggh91ucl` `g3bi3j4r` `gvv3q5ze` `gg6psrhg`
`gm0e12ot` `gp6smhba` `gfmtr7f0` `gm9hoa1m` `gku1flax` (23 ids)

Two of these (`gm9hoa1m` `gku1flax`) were clean in PUB. In LOOSE mode this list is **empty** — the same
arm, the same request, the wire schema the only difference.

### 5e. P3B, `g13` — "beyond listing it" plus wire vocabulary on every record — 14 / 15

Decline clause: `g1hma18g` `gylk3k14` `gzco65b9` `ghmgct1t` `gczn9kgc` `gaqogzxh` `gbmkfcf5` `go74pqr8`
`g5eo8ybu` `g3kr36dq` `g6qut1hd` `gvkahl2q` `goc21ko6` `gq9hkhwi` (clean: `gugo8jsx`).
Wire vocabulary: all 15.

### 5f. P3A, `g13` — weaker credit plus residual decline — 12

`g1hma18g` `ghmgct1t` `gczn9kgc` `gaqogzxh` `gbmkfcf5` `go74pqr8` `g5eo8ybu` `g3kr36dq` `g6qut1hd`
`gvkahl2q` `goc21ko6` `gq9hkhwi` (clean: `gylk3k14` `gzco65b9` `gugo8jsx`).

### 5g. P3B, `c8` — cross-link decline restored — 6

`glqit6d2` `gcweyxfl` `ghjv3mc5` `gylk3k14` `gzco65b9` `gugo8jsx`. PUB has the same 6; P3A converts them
to capacity clauses, which is the same defect class.

### 5h. What did NOT regress anywhere

Laundering possessives **0/810**. Byte-identical siblings **0/810**. Fabricated frames or invented
speakers **0/810** — B3 has nothing to fix in this pass, since no arm called Tanfel's or Mathews's own
first person a customer's words. Unsupported claims 0, with one exception: loose P3B on `rep`
`ghu2cklc`/`g7zeb1wf` writes "**the capacity is that of a sales representative**", which supplies a
capacity the instruction forbids supplying (it is grounded in the chunk text, so a grounded import
rather than a fabrication). Frame accuracy 135/135 in every arm and both modes — the 12 "section
heading" slips and the fabricated "testimonial" of earlier passes are absent from all three arms. B2's
restating closers: 0 in every arm, including PUB, so B2 also has nothing to fix here.

## 6. Length

### STRICT (primary)

| request | PUB | P3A | P3B | P3A vs PUB | P3B vs P3A |
|---|---|---|---|---|---|
| mw2 (39) | 55.9 (43–112) | 58.7 (37–121) | 59.7 (38–122) | +5% | +2% |
| rep (39) | 59.5 (39–119) | **52.5 (37–102)** | 58.4 (44–108) | −12% | +11% |
| g13 (15) | **49.3 (38–85)** | 64.7 (48–91) | 80.9 (64–123) | **+31%** | **+25%** |
| g15 (33) | 50.9 (36–98) | 49.0 (38–122) | **44.6 (31–103)** | −4% | −9% |
| c8 (9) | 78.0 (39–212) | 81.0 (46–181) | **69.8 (40–155)** | +4% | −14% |
| **all 135** | **56.5** | 56.7 | 58.7 | **+0.4%** | **+3.5%** |

### LOOSE (secondary)

| request | PUB | P3A | P3B |
|---|---|---|---|
| mw2 | 58.5 | 56.5 | 72.7 |
| rep | 57.1 | 65.2 | 65.0 |
| g13 | 69.7 | 92.1 | 60.8 |
| g15 | 52.8 | 53.5 | **36.6** |
| c8 | 73.2 | 75.9 | 80.3 |
| **all 135** | **58.9** | 63.5 | 60.8 |

Reading: overall length is flat and tells nothing. The two places it moves are both diagnostic, not
cosmetic. **g13 +31% then +25%** is each arm spending more words to say less — P3A swaps "Tanfel offers
X" for "Tanfel lists X among its offerings, but the snippets do not provide further detail about … its
specific dealing", and P3B prefixes every paragraph with "The snippets for X appear as entries in lists
of …" and closes by naming the list a second time. **g15 P3B −12% against PUB** is the one honest
shortening in the pass: the clause it drops ("whether as a manufacturer, supplier, or another role") is
the one that should go. No arm is too long to read; the 212-word maximum (`guzaeduj`, PUB, the Stamping
page) buys real content. Nothing in the length reading changes any verdict.

## 7. Verdicts

### PUB — **KEEP** (it is the only arm with no blocking regression in either mode)

Not because it is good. It carries the C22 defect on 42 of 46 tanfel own-listing records in strict mode
and 45 in loose, including all three of `g15`'s verified J2+J6 majors and both of `c8`'s. But it is the
only arm that is *stable*: it writes the same reading on both instances of the byte-identical Mathews
request, in both call shapes, with 0 party errors in all four samples, 0 wire vocabulary on 78 Mathews
records, and 0 laundering possessives anywhere. Publishing over it needs an arm that beats it on the
target without costing the party, and neither candidate does.

### P3A (PUB + R2) — **REVISE**

R2 is a sentence about precedence, and on this evidence it has no measurable effect on the thing it
governs. Its target — a principal's doing surviving the list rule — never fires, because PUB already
gets the party right on all 78 Mathews records. What R2 *does* do is destabilise the phrasing: on one
instance of the request it produces the pass's best template ("Mathews & Company lists or represents
X", 39/39 correct, shortest), and on the byte-identical other instance it produces "**The snippet**
shows Mathews & Company offering X in an unstated capacity" on 37 of 39 — the banned word, on a request
where PUB never uses it. On tanfel it converts the capacity hedge into a flat decline on 24 g15 records
(4 of them clean in PUB) and into a weaker credit on 12 g13 records: 42 → 42 over-hedges, no gain.

If R2 is to be kept at all it must be pinned to a vocabulary the model already uses, not to the wire
words. Exact generic sentence to test in its place, appended to the list rule after "What the item takes
from that sentence is the dealing, never the doer.":

> This never overrides the rule above about whose doing it is: where the item, its entry, or the words
> around it put the doing with someone else, that party keeps it and the capacity stays open, however
> the page or the section is titled. Name that party in the statement wherever the passage names it,
> and where it does not, say that the passage does not say who performs it.

(Changed from R2: "the passage around it" → "the words around it"; "whatever the page is called" →
"however the page or the section is titled"; and a second clause that gives the model somewhere to put
the party, which is what it reaches for the wire word to do.)

### P3B (PUB + R2 + R1 + B2 + B3) — **DO NOT PUBLISH**

R1 fails in both directions again, for the second consecutive pass, and the failure is the same one
JUDGE_C16 §7.6 diagnosed and R1 was written to prevent:

- **Where it was aimed it barely moves.** g15's own index: 21 over-hedges → 23. The clause changes from
  "the capacity … is not specified" to "the capacity is not further specified in this listing" and
  survives. R1's first half — "An index of the company's own work is such a presentation even where its
  heading is a bare noun with no verb and no *our* in it" — is read by the model as licence to say
  "Tanfel offers X" **and still** append the capacity clause, exactly the stacking R1's second half was
  supposed to end. g13 gets worse (15 → 14, but as explicit "beyond listing it", with wire vocabulary on
  15/15 and +64% length); c8 unchanged at 6.
- **Where it was not aimed it destroys the party.** 35 of 39 rep records assert a principal's doing as
  Mathews & Company's own, and the sentence it writes — "*The context is a list of product lines Mathews
  & Company offers, so the page shows Mathews & Company offering X*" — is the production C21 defect
  restored in shape. R1's second half ("a page that carries a company's line, product line, or
  represented makers is not that company saying it does these things") names this exact page and does
  not stop it; the first half names the bare-noun index and wins.
- **And it is unstable.** The same arm on the same request hedges 37/39 under one nonce and launders
  35/39 under another; its one success (loose g15, 26 → 0) disappears under the production call shape.

B2 and B3 are carried along with R1 in this arm and cannot be judged here: PUB writes **0** restating
closers and **0** invented speakers across all 135 records in both modes, so neither sentence had
anything to fix. They remain unmeasured, not refuted. They should be tested on their own, on a request
that carries their targets — not on these five.

### Which arm should ship

**PUB. Ship nothing from this pass.**

| | tanfel over-hedge (46) | rep party errors (39) | mw2 party errors (39) | wire vocabulary (135) | stable across the A/A twin |
|---|---|---|---|---|---|
| **PUB** | **42** | **0** | **0** | **47** | **yes** |
| P3A | 42 | 0 | 0 | **85** | no |
| P3B | **43** | **35** | 0 | 22 | no |

### What this pass established

1. **This tryout path cannot exercise C21.** PUB reproduces 0 of the 17 production majors in either call
   shape. Any future C21 arm must first be shown to reproduce the defect in its control arm, or the
   comparison is between two clean arms and means nothing.
2. **The Mathews party result is not a measurement.** `mw2` and `rep` are the same request. Two arms
   give opposite party verdicts across a nonce. Before R1, R2 or any successor is judged on the party
   axis again, the control needs **at least two genuinely different representative or distributor
   sites**, and each arm needs **at least two runs per request** so the A/A spread is visible in the
   table rather than hidden between two files that were believed to be different requests.
3. **The call shape changes the answer.** g15 P3B: 0 over-hedges loose, 23 strict. Every future tryout
   must run the strict shape; loose results are not evidence about production.
4. **C22 has not been moved by any prompt sentence tried in three passes** (B1, R1, R2). g15's bare-noun
   index and c8's "Related Products" cross-links both give the model a one-entry record with nothing to
   place, and JUDGE_C16 §7.6 already named the next step: *"stop editing the capacity paragraph: the
   remaining cause is that a one-entry record gives the model nothing to place, and the fix belongs
   upstream in what a record carries, not in the prompt."* The g13 finding of §2a is the direct
   evidence for that — the same record ids (`gaqogzxh`, `g5eo8ybu`, `g3kr36dq`, `goc21ko6`, `gq9hkhwi`)
   that every arm here hedges were written by production, from a chunk that carried the page body,
   as clean detailed offerings with no hedge at all. **The record's packing, not the prompt's wording,
   is what decides the over-hedge.**
