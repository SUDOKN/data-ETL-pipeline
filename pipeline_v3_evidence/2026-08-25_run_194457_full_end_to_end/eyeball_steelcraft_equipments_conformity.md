# Eyeball review: steelcraft.com equipments + conformity_attestations, run 20260825T194457

Reviewed 2026-08-25 against the previous run 20260824T190359 (same upstream search/fold/synthesis
group_ids; tail prompts changed between runs — the evidence_source wording fix, 'mentions' →
'focal form and synthesis'). Files:

- `packages/logs/extraction_dumps/20260825T194457/steelcraft_com__equipments.json`
- `packages/logs/extraction_dumps/20260825T194457/steelcraft_com__conformity_attestations.json`
- `packages/logs/extraction_dumps/20260824T190359/steelcraft_com__equipments.json`
- `packages/logs/extraction_dumps/20260824T190359/steelcraft_com__conformity_attestations.json`

Counting note: these dumps hold **rows per chunk**; a group that spans both chunks has two rows
that ground independently. All counts below say whether they are row-level or distinct-group.
The task brief's numbers ("11 of 75 grounded, 9 distinct tags"; "32 → 61 tags") are reproducible
only under a group-level reconciliation that discards a group when its other chunk's row declined;
raw row-level counts are given here and the reconciliation is called out where it matters.

---

## TASK 1 — equipments

### Headline counts (measured)

| | old run 20260824T190359 | new run 20260825T194457 |
|---|---|---|
| rows (both chunks) | 84 | 84 |
| distinct group_ids | 75 | 75 |
| rows status=grounded | **1** | **15** |
| distinct groups grounded | **1** (`gfv70csp`) | **15** |
| distinct final tags (passed screening) | 1 | **10** |
| rows declined (`no_candidates`) | 83 | 69 |

All 15 grounded rows sit in `freehand_grounding.tags` and every offered tag passed screening
(SCR-1/SCR-2 both satisfied on all of them). 4 of the 15 grounded groups (`g31tt6mn`,
`gbubf8pp`, `gd10mzgh`, `gec77azf`) also have a **declined** row in the other chunk — dropping
those (a "conflict means declined" merge) yields exactly the brief's 11 groups / 9 tags.

68 of the 75 equipment group_ids are also group_ids in the same run's products dump
(`steelcraft_com__products.json`); 12 of the 15 grounded groups are in that overlap set, and the
other 3 (`g400k534` "Hurricane doors", `gbf5c0f9` the door-type list, `gfv70csp` the finishing
process) are products/process by inspection.

### The 15 grounded rows, verbatim

Tag explanations quoted are the FGR-E1 (evidence) rule verbatim; the other rules (Q1–Q4) are
boilerplate variations of the same inference on every row.

1. **`g04h532n`** (chunk 0:91562) — focal_form `hollow metal steel doors` → tag
   **`hollow metal door manufacturing machine`**.
   FGR-E1: *"The focal form 'hollow metal steel doors' and synthesis state that Steelcraft
   manufactures and offers a full line of hollow metal steel doors. This implies the use of
   machines that manufacture hollow metal doors."*
   **Verdict: regression.** The synthesis is a product listing ("provides a full line of hollow
   metal steel doors"). The tag is an invented machine category deduced from the product's
   existence — no machine is evidenced anywhere.

2. **`gdxy3axk`** — `T Series` → **`flush door manufacturing machine`**.
   FGR-E1: *"...Steelcraft manufactures and offers T Series doors, which are flush doors with
   temperature rise cores, implying the use of machines to manufacture flush doors."*
   **Verdict: regression** (product line).

3. **`g31tt6mn`** — `LS Series Stainless Steel Doors` → **`stainless steel door manufacturing
   machine`**. FGR-E1: *"...these doors are a product offered by Steelcraft, implying the use of
   machines to manufacture stainless steel doors."* The same group's chunk-2 row is
   `no_candidates`. **Verdict: regression** — the explanation itself says "a product offered".

4. **`g92f1qbb`** — `Hollow Metal Steel Flush Doors` → **`hollow metal door manufacturing
   machine`**. FGR-E1: *"...these doors (specifically the Steelcraft L Series) are a product
   offered by Steelcraft, implying the use of machines to manufacture hollow metal doors."*
   **Verdict: regression.**

5. **`gbubf8pp`** — `L Series` → **`flush door manufacturing machine`** (chunk-2 row declined).
   FGR-E1: *"...the L Series is a line of hollow metal steel flush doors manufactured and offered
   by Steelcraft, implying the use of machines to manufacture flush doors."*
   **Verdict: regression.**

6. **`gd10mzgh`** — `A14 Series Entrance Doors` → **`entrance door manufacturing machine`**
   (chunk-2 row declined). FGR-E1: *"...Steelcraft manufactures and offers entrance doors,
   implying the use of machines to manufacture entrance doors."* **Verdict: regression.**

7. **`gec77azf`** — `SZ Series Falcon Flush Doors` → **`flush door manufacturing machine`**
   (chunk-2 row declined). FGR-E1: *"...these are a product offered by Steelcraft, implying the
   use of machines to manufacture flush doors."* **Verdict: regression.**

8. **`g97lpxgl`** — `L Series flush doors` → **`flush door manufacturing machine`**.
   FGR-E1: *"...Steelcraft manufactures and offers L Series flush doors, implying the use of
   machines to manufacture flush doors."* **Verdict: regression.**

9. **`gbf5c0f9`** — `flush, stile and rail, severe weather, acoustical, stainless steel and
   blast resistant doors` → **six tags at once**: `flush door manufacturing machine`,
   `stile and rail door manufacturing machine`, `severe weather door manufacturing machine`,
   `acoustical door manufacturing machine`, `stainless steel door manufacturing machine`,
   `blast resistant door manufacturing machine` (all passed screening).
   FGR-E1 (flush, representative): *"The focal form ... and synthesis state that Steelcraft
   manufactures and offers flush doors, implying the use of machines to manufacture flush
   doors."* The source is the "BUILT TO LAST" about-page sentence listing door types.
   **Verdict: regression, worst case** — one marketing list fabricates six machine categories.

10. **`ged4kjbr`** — `T Series temperature rise core doors` → **`flush door manufacturing
    machine`**. FGR-E1: *"...Steelcraft manufactures and offers T Series temperature rise core
    doors, which are flush doors with a specific safety feature, implying the use of machines to
    manufacture flush doors."* **Verdict: regression.**

11. **`g400k534`** — `Hurricane doors` → **`hurricane door manufacturing machine`**.
    FGR-E1: *"...Steelcraft manufactures and offers hurricane doors and frames, implying the use
    of machines to manufacture hurricane-resistant doors."* **Verdict: regression.**

12. **`g6on13ul`** — `Steelcraft Hurricane products` → **`hurricane door manufacturing
    machine`**. FGR-E1: *"...Steelcraft manufactures and offers hurricane products (doors)
    designed for severe weather protection, implying the use of machines to manufacture
    hurricane-resistant doors."* **Verdict: regression.**

13. **`gf47v6x5`** — `SZ Series Flush doors` → **`flush door manufacturing machine`**.
    FGR-E1: *"...these are square edge flush doors offered by Steelcraft, implying the use of
    machines to manufacture flush doors."* **Verdict: regression.**

14. **`gexjsgp9`** — `SZ doors` → **`flush door manufacturing machine`**.
    FGR-E1: *"...SZ doors are offered by Steelcraft, implying the use of machines to manufacture
    flush doors."* The synthesis is about *sales channels* ("targeted at Distributor over the
    counter sales"). **Verdict: regression.**

15. **`gfv70csp`** (chunk 91562:158790) — `engraining and staining process` →
    **`engraining and staining machine`**.
    FGR-E1: *"The focal form is 'engraining and staining process', and the synthesis states
    Steelcraft describes its exclusive engraining and staining process as simulating a wide
    variety of wood finishes. This implies the use of a machine or system that performs
    engraining and staining on steel doors."*
    **Verdict: plausibly genuine.** This is the GRAINTECH finishing operation Steelcraft itself
    performs on its doors — a real in-house production process, so a machine/system performing it
    is a fair equipment inference. It is also the **only** group the old run grounded, with an
    almost identical rationale, so it is a carry-over, not a gain of this run.

### Quantified verdict

- **Genuine equipment: 1 of 15** (`gfv70csp`, the carry-over). **Leaked products: 14 of 15.**
- Distinct final tags: 10. **9 of the 10 are fabricated `<door type> manufacturing machine`
  categories** derived purely from product listings; only `engraining and staining machine`
  has process evidence behind it.
- The uniform signature of the leak: FGR-E1 satisfied via *"manufactures and offers X, implying
  the use of machines to manufacture X"* — a product→machine existence inference, present
  verbatim (modulo the door type) on all 14 bad rows, and the screening stage rubber-stamps it
  with the mirrored *"which necessarily involves the use of X manufacturing machines"*.

### Before/after on the same group_ids

For every one of the 14 leaked groups the old run declined with the correct, crisp reason.
Representative old-run declines (all `no_candidates`):

- `g04h532n`: *"The record discusses hollow metal steel doors as a product manufactured and
  offered by Steelcraft. Doors are finished goods, not production machines. There is no mention
  or implication of a production machine category."*
- `gbf5c0f9`: *"The record lists various types of doors ... as products offered by Steelcraft.
  These are finished goods, not production machines. No production machine category is
  evidenced."*
- Same wording pattern on `gdxy3axk`, `g31tt6mn`, `g92f1qbb`, `gbubf8pp`, `gd10mzgh`,
  `gec77azf`, `g97lpxgl`, `ged4kjbr`, `g400k534`, `g6on13ul`, `gf47v6x5`, `gexjsgp9`.

Old run: **74 of 75 groups declined, 1 grounded** (`gfv70csp`). New run: **15 grounded**. Same
upstream records, only the tail prompts differ (the 'mentions' → 'focal form and synthesis'
evidence_source fix), so the flip is attributable to the prompt change: the model previously held
the line "doors are finished goods, not production machines" and now walks the
product→machine-implication path on 14 of the same records.

### Spot-check of 5 still-declined groups (new run) — all correct

- `ggigei4o` `L Series door`: *"...there is no mention or implication of any production machine
  category. The synthesis focuses on the features and applications of the door itself..."* Correct.
- `gvro0daq` `H SERIES FLUSH DOORS`: *"...indicate a product offering, but do not mention or
  imply any production machine category."* Correct.
- `gjrirs9y` `K Series frames`: *"...does not mention or imply any production machine category."*
  Correct.
- `g1fbtuje` `DE Series Double-Egress Frames`: *"...describe a type of frame, not a production
  machine."* Correct.
- `glfts2vg` `T Series Flush Doors`: *"...does not mention or imply any production machine
  category."* Correct.

Note the incoherence this exposes: `L Series door` (declined) vs `L Series` and `L Series flush
doors` (grounded); `T Series Flush Doors` (declined) vs `T Series` (grounded) — identical
evidence class, opposite verdicts, i.e. the new prompt does not fail deterministically; it fails
~20% of the time on this evidence class (consistent with the known freehand reproducibility
noise floor, but here with a large systematic shift on top: 1 → 15).

---

## TASK 2 — conformity_attestations

### Headline counts (measured)

| | old run | new run |
|---|---|---|
| rows | 154 | 154 |
| distinct group_ids | 130 | 130 |
| grounded rows | 40 | **70** |
| `no_candidates` rows | 96 | 67 |
| `screened_out` rows | 10 | 9 |
| `no_mentions` rows | 8 | 8 |
| distinct final tags, row-level (passed screening) | **36** | **71** |
| same, counting only conflict-free groups | 30 | 64 |

(The brief's 32 → 61 is the same phenomenon under its own merge rule; every accounting shows
roughly a doubling.) Group-level status transitions old→new: 30 stayed grounded, **32 flipped
no_candidates→grounded**, 45 stayed declined, 4 grounded→no_candidates, 1 grounded→screened_out,
5 screened_out→no_candidates, 5 stayed screened_out, 5 stayed no_mentions.

### All 71 distinct final tags of the new run, with classification

Classes: **(a)** real certification / standard-compliance claim, **(b)** duplicate or
near-duplicate of another tag (named), **(c)** not actually a conformity attestation.
One verbatim example explanation each (OGR-E1 unless noted). All are `oov_grounding` except
`UL Product Certification` (`in_vocab_grounding`).

| # | tag | class | example explanation (verbatim) / duplicate-of |
|---|---|---|---|
| 1 | ADA compliance | a | `gvmqwjxa`: "The focal form is 'ADA compliant', and the synthesis states Steelcraft claims its product is ADA compliant." |
| 2 | ANSI A115 (Locations) compliance | a | `gc0wz3jn`: "'SDI Membership communicates compliance with industry standards including ANSI A115 (Locations).'" |
| 3 | ANSI/DHI A115 Compliance | b → #2 (same A115 standard) | `g9g20hgj`: "...products are designed to meet ANSI/DHI A115 for hardware locations." |
| 4 | ANSI/DHI A115 compliance | b → #3 (case-only, same group `g9g20hgj`, other chunk) | "...hardware preparation locations meet ANSI/DHI A115..." |
| 5 | ANSI A250.10-2011 Compliance | a | `gdt7tk74`: "...the factory-applied baked-on rust inhibiting primer meets or is in accordance with ANSI A250.10-2011." |
| 6 | ANSI A250.10-2011 compliance | b → #5 (case-only, same group, other chunk) | — |
| 7 | ANSI/SDI A250.10 compliance | b → #5 (same standard, edition dropped) | `g4te0r3w`: "'Steelcraft claims compliance with ANSI/SDI A250.10 (Prime Paint) for its products.'" |
| 8 | ANSI A250.3-2007 (R2011) compliance | a | `gst4vkuh`: "'finish paint is tested in accordance with ANSI A250.3-2007 (R2011)...'" |
| 9 | ANSI/SDI A250.3 compliance | b → #8 | `gj6noivo`: "'compliance with industry standards including ANSI/SDI A250.3 (Finish coatings)'." |
| 10 | ANSI A250.6-2003 compliance | a | `gqb6u9sj`: "'hardware preparations and reinforcements meet ANSI A250.6-2003.'" |
| 11 | ANSI/SDI A250.6 compliance | b → #10 | `gtrd4i6v`: "'compliance with industry standards including ANSI/SDI A250.6 (Hardware reinforcing)'." |
| 12 | ANSI A250.8-2017 (SDI-100) compliance | a | `gpt9k5hx`: "'door construction exceeds ANSI A250.8-2017 (SDI-100)'..." |
| 13 | ANSI/SDI A250.8 compliance | b → #12 | `g4v94pgj`: "'Steelcraft claims compliance with ANSI/SDI A250.8 as part of its industry standards...'" |
| 14 | ANSI/SDI A250.13 compliance | a (new, real windstorm standard) | `golae5un`: "'Steelcraft products are in accordance with ANSI/SDI A250.13...for Protection of Building Envelopes.'" |
| 15 | ANSI/SDI A250.4 compliance | a | `g09kg190`: "'Steelcraft claims compliance with ANSI/SDI A250.4 (Physical endurance) for its products.'" |
| 16 | SDI/A250.4 compliance | b → #15 | `gcrgn5qh`: "'INPACT Series hospital doors are tested to SDI/A250.4 for 1 million cycles and a 350 lb. cart impact.'" |
| 17 | ANSI/BHMA A156.3 2014 compliance | a | `grpmqkvd`: "'hardware meets ANSI/BHMA A156.3 2014.'" |
| 18 | ANSI compliance | c (vacuous — "ANSI" is a body, not a standard) | `g5du1gjw`: "'Steelcraft designs the FP14 Paladin Series to meet ANSI compliance.'" |
| 19 | ASTM A666 Compliance | a-borderline (a stainless *material* spec, i.e. a material conformity claim, ×2 groups incl. `g34j30vh`) | `gj48ol44`: "...the synthesis states Steelcraft manufactures products in compliance with these standards." |
| 20 | ASTM B117-18 compliance | a (test performed per standard) | `gfzufxqj`: "'salt spray testing is conducted in accordance with ASTM B117-18.'" |
| 21 | ASTM C1363 compliance | a | `gigvsqpn`: "'frames are tested to ASTM C1363 for Thermal Performance.'" |
| 22 | ASTM D2794-93R19 compliance | a | `g1fxbscj`: "'Impact test D2794-93R19 is performed.'" |
| 23 | ASTM D3359-17 compliance | a | `gftkrhco`: "'film adhesion testing is conducted in accordance with ASTM D3359-17.'" |
| 24 | ASTM D4585-D4585M-18 compliance | a | `gcd704bo`: "'condensation testing (humidity) is conducted in accordance with ASTM D4585-D4585M-18.'" |
| 25 | ASTM E152 Compliance | a (legacy fire-test designation; ×2 groups) | `gdtm8odl`: "...doors ... comply with neutral pressure testing (ASTM E152 and UL-10B)." |
| 26 | ASTM E152 compliance | b → #25 (case-only, same group, other chunk) | — |
| 27 | ASTM E413 Compliance | c (reference-only: standard used to *calculate* STC, no compliance claim) | `g79fvy1u`: "...Steelcraft references ASTM E413 as the standard for calculating STC ratings." |
| 28 | California Proposition 65 Compliance | a | `g0x2ae26`: "...Steelcraft provides information or warnings in accordance with California PROP65..." |
| 29 | California Proposition 65 compliance | b → #28 (case-only, same group, other chunk) | — |
| 30 | Class A (3-hour) fire rating | a (rating claim) | `gx2sx6zi`: "...Steelcraft manufactures products with a fire rating up to Class A (3-hour)." |
| 31 | Declare Label | a | `gq6j3dms`: "'Steelcraft...offers Declare labels for products to support the Living Building Challenge.'" |
| 32 | Living Building Challenge Declare label | b → #31 | `g8uwl1vh`: "'Steelcraft, through Allegion, provides Declare labels for products to support the Living Building Challenge.'" |
| 33 | FEMA P-320 compliance | a (canonical P-320) | `gjsypzsu`: "'Steelcraft offers Paladin Series glass light steel tornado doors that meet FEMA P-320...standards for tornado shelters.'" |
| 34 | FEMA 320 (2021) compliance | b → #33 | `g1tuv25u`: "'Steelcraft's FP14 Paladin Series frames are labeled by Intertek for compliance with FEMA 320 (2021)'." |
| 35 | FEMA P-320 (2021) Compliance | b → #33 | `g2wg48rx`: "...Steelcraft offers products labeled as compliant with FEMA P-320 (2021)." |
| 36 | FEMA P-361 compliance | a (canonical P-361) | `gjsypzsu`: "'meet FEMA P-320 & P-361/ICC500-2014 standards for tornado shelters.'" |
| 37 | FEMA 361 compliance | b → #36; also weak evidence — grounded on the generic-material sentence "'steel is the only readily available door material that passes the FEMA 361 and ICC 500 tornado test'" (`gevpuivl`), the very sentence declined on `gy3fqrk6` | — |
| 38 | FEMA P-361 (2021) Compliance | b → #36 | `g56nm2k6`: "...Steelcraft manufactures products that comply with FEMA P-361 (2021)." |
| 39 | FEMA 320/361 Compliance | b → #33+#36 (combined tag) | `gc4kxua6`: "...products are listed and labeled by Intertek and UL showing compliance with FEMA 320/361." |
| 40 | FEMA 361/320 guidelines compliance | b → #33+#36 | `g0czhfr5`: "'Steelcraft designs the FP14 Paladin Series to meet FEMA 361/320 guidelines.'" |
| 41 | FEMA Tornado Shelter Standards Compliance | b → #33+#36 (umbrella) | `gcwytcti`: "...Paladin PW Series flush doors and frames assemble as a series to meet FEMA tornado shelter standards." |
| 42 | FEMA tornado shelter standards compliance | b → #41 (case-only, same group, other chunk) | — |
| 43 | Florida Building Code (FBC) compliance | a | `gpifkwta`: "'Steelcraft products are in accordance with the Florida Building Code (FBC).'" |
| 44 | HVHZ protocol compliance | a | `gtpy7xmu`: "'products tested to HVHZ protocols (coastal Palm Beach/Dade).'" |
| 45 | Hurricane certification | c-lean (vague umbrella of #44/#46/#65) | `gsmsgovf`: "'Steelcraft offers a wide variety of hurricane certified doors and frames.'" |
| 46 | Miami-Dade County NOA hurricane approval | a (×2 groups) | `gxy9ebbi`: "...Steelcraft manufactures products approved for hurricanes by NOA Dade County Florida." |
| 47 | ICC 500-2014 Compliance | a (canonical) | `gghuz13m`: "...products are listed and labeled by Intertek and UL showing compliance with ICC500-2014." |
| 48 | ICC 500-2014 compliance | b → #47 (case variant, different group `gjsypzsu`) | — |
| 49 | ICC500-2014 code compliance | b → #47 | `gdb7lj7u`: "'FP14 Paladin Series is designed to meet requirements of ICC500-2014 code.'" |
| 50 | ICC 500-2020 compliance | a | `gxqkgqxg`: "...Steelcraft offers products labeled as compliant with ICC 500-2020." |
| 51 | ICC 500 Label | b/c → #47 family ("label availability" info, not a distinct attestation) | `g8d55g4v`: "...Steelcraft provides information about ICC500 label availability for its products." |
| 52 | International Building Code compliance | a | `gwzrhkqo`: "...designs and labels its fire-rated and tornado-resistant doors to meet the requirements of the International Building Code." |
| 53 | International Fire Code compliance | a | `gyie246f`: "...Steelcraft's fire doors are designed to meet the International Fire Code." |
| 54 | Intertek Listing and Labeling | a (third-party listing) | `ghktbo72`: "...Steelcraft's products are listed and labeled by Intertek." |
| 55 | Intertek (WHI) fire door labeling | a (distinct: WHI fire labeling service at KC service center) | `gzjo14al`: "...Steelcraft provides Intertek (WHI) fire labeling services at its Kansas City Service Center." |
| 56 | Intertek labeling for FEMA 320/361 compliance | b → #54 + #33/#36 | `geuxnx4j`: "'products are tested and Intertek labeled showing compliance with ICC500-2014 and FEMA 320/361.'" |
| 57 | Intertek labeling for ICC500-2014 compliance | b → #54 + #47 (same group `geuxnx4j`) | — |
| 58 | NFPA 80 Standard for Fire Doors and Other Opening Protectives Compliance | a | `g1oiy03h`: "...fire doors are designed to meet NFPA 80 Standard for Fire Doors and Other Opening Protectives." |
| 59 | NFPA 80 compliance | b → #58 (same group, other chunk) | — |
| 60 | SDI 117 (Tolerances) compliance | a | `ggwezw0w`: "'SDI Membership communicates compliance with industry standards including SDI 117 (Tolerances).'" |
| 61 | SDI Membership | c-lean (association membership, not itself an attestation; old run kept it too) | `g334dpzz`: "'SDI Membership communicates compliance with industry standards including...'" |
| 62 | Steel Door Institute membership | b → #61 | `gtuz1auj`: "...Steelcraft is a member of the Steel Door Institute and is certified to its standards." |
| 63 | Steel Door Institute Certification | a (×2 groups) | `gtuz1auj`: "'Steelcraft is SDI Certified through regular audits to ensure manufacturing, performance, and quality standards set by the Steel Door Institute.'" |
| 64 | Steel Door Institute certification | b → #63 (case-only) | `gvq3fx4u` "SDI Certified" |
| 65 | South Florida Building Code (SFBC) compliance | a | `g2n191sl`: "'Steelcraft manufactures products that comply with the South Florida Building Code (SFBC)'." |
| 66 | TDI Non-impact compliance | a | `g7y5p5ap`: "'Steelcraft provides information for verifying TDI Non-impact compliance for its products.'" |
| 67 | UL 10C positive pressure fire test compliance | a | `gwhsll1u`: "...frames and doors are listed for installations requiring compliance to positive pressure standards (UL-10C)." |
| 68 | USL-10C Compliance | b → #67 (site's own verbatim string — the phrase "Compliance with ASTM A666, ASTM E152, UL-10B, and USL-10C" is found in the text, mention_count 1 — carrying what is almost certainly the site's typo for UL-10C into the ontology-facing tag) | `gj48ol44` |
| 69 | UL Product Certification | a (the only in-vocab grounding; 3 groups: `ghktbo72`, `gy6skn0z`, `gz00zxei`) | `gz00zxei`: "...Steelcraft offers labeling with UL fire ratings, which evidences that Steelcraft provides UL Product Certification for its products." |
| 70 | UL-10B Compliance | a (×3 groups) | `gh8ye3mm`: "...Steelcraft references UL-10B as a standard their products meet or are associated with." |
| 71 | UL-10B compliance | b → #70 (case-only, same group `gdtm8odl`, other chunk) | — |

**Classification totals:** (a) real ≈ 38 tags; (b) duplicate/near-duplicate = 29 tags
(8 case-only pairs + 21 naming/edition/combined variants); (c) not-an-attestation or vacuous = 4
(`ANSI compliance`, `ASTM E413 Compliance`, `Hurricane certification`, `ICC 500 Label`;
`SDI Membership`+its dup are borderline-c but were present in the old run too).

Case-only duplicate mechanism, precisely: 8 pairs differ **only in capitalization**
(A250.10-2011, A115, E152, Prop 65, FEMA tornado umbrella, ICC 500-2014, SDI Certification,
UL-10B). Six of the eight are the *same group grounded independently in each chunk*, each chunk
choosing a different casing — cross-chunk rows are not reconciled and final tags dedupe only on
the exact string.

### Is 32→61 (36→71 row-level) real recall gain or duplication?

**Both, roughly half and half — and the gained half is mostly genuine.**

- **Real recall gain:** the 32 groups that flipped no_candidates→grounded are dominated by real
  standards the old run wrongly declined: the ANSI A250.x technical-data-manual family
  (A250.3/6/10/13, SDI 117, A115, A156.3), the paint/finish ASTM test suite (B117, C1363, D3359,
  D4585, E152), FBC/SFBC/HVHZ, the explicit FEMA P-320/P-361/ICC 500-2014 designations, Declare
  labels, ADA. Distinct real-world concepts covered went from ~29 (old) to ~40 (new).
- **Duplication:** 29 of the 71 tags are duplicates — the FEMA family alone spawns 10 tags for 2
  standards + an umbrella, ICC 500-2014 spawns 5, and the same standard is repeatedly tagged
  both as `ANSI A250.x-year` and `ANSI/SDI A250.x`. This is the OPTION-axis/no-canonicalization
  hole: OOV tags are free-text and nothing normalizes them across groups or even across the two
  chunks of one group.
- **Precision loss is small but real:** 4 tags in class (c), plus `FEMA 361 compliance` grounded
  on a generic "steel as a material passes" sentence that the same run declined on another group.

### Losses vs the old run (concepts, not names): 2

- **`UL 1784 compliance`** — gone. `gzzyuc5m` "UL 1784 (Air Leakage)", new-run oov decline:
  *"The synthesis shows Steelcraft claims compliance with UL 1784 (Air Leakage), which is a
  standard, not a certification, accreditation, registration, or compliance attestation."*
  A self-contradictory rationale (a compliance claim against a standard IS the field's subject).
- **`ANSI A250.8-2003 (SDI 100)`** (the 2003 edition; 2017 survives) — `gunf6db9` declined with
  confused scope reasoning: *"...this is not out-of-vocabulary; it is not present in the provided
  vocabulary, but the task is to identify only what is not already covered, not to list all
  standards mentioned."*

The bogus *"standard, not a ... compliance attestation"* decline template appears on 5 rows
(`gv188n3y`, `gzzyuc5m`, `gyr66wyt`, `guhv8wxy`, `gwt80v25`); three of the five concepts survive
via other groups, two do not. On `guhv8wxy` the synthesis literally says *"This shows that
Steelcraft claims compliance with ANSI UL 10C (Fire) for its products"* and the decline still
fires — the decline contradicts its own evidence.

### Screen and decline sampling (all 9 screened_out + 12 declined examined)

All 9 `screened_out` rows, with SCR-1 fail reasons (verbatim, abridged):

1. `gdvz67y2` LEED — "a mention in passing, not a substantive activity." **Correct.**
2. `gjqgyja7` LEED rating system — "do not show Steelcraft's own direct dealing." **Correct.**
3. `g09kbuzu` LEED Credits — documentation offering only. **Correct.**
4. `g7913xrc` CALGreen Building Code — "offers documentation related to." **Correct.**
5. `g82dbwh7` ASTM E330 — "does not show Steelcraft engaging in any activity ... beyond
   referencing it." **Correct on the synthesis as written** (reference-only).
6. `g4pqpn4g` ANSI/ASTM E330 — same. **Correct.**
7. `g1eet2qw` TAS 203 — same; **borderline** — the mentions sit under "Approvals and specified
   industry standards" on the hurricane-door page, so a compliance reading was available, but
   the synthesis itself only says "references", and the screen follows the synthesis.
8. `gshm03um` Fire Rated — "'Fire Rated' is a general property, not a specific certification."
   **Correct** (the old run had bent this into in-vocab `UL Product Certification`, which the
   new run still gets from 3 other groups).
9. `gkywodxh` FEMA — CE course + service-center mention. **Correct.**

12 declined (`no_candidates`) rows sampled: `g57wxjaw` "LEED certified" (site explains the LEED
process, does not claim it — correct), `g5uwzc6o` EPDs (explains what EPDs are — correct),
`gae6sn77` "Clean Air Gold Certification" (site says it "supports CALGreen", never claims the
cert — correct, despite the certification-sounding name), `ghnq0ga5` "SDI Certification"
("provides information ... does not state its own certification status" — correct for that row;
the claim is captured from `gtuz1auj`/`gvq3fx4u`), `gy3fqrk6` ICC 500 tornado test (generic
steel-material claim — correct), `gxzoa8rc` FEMA P-361 (best-practices reference — correct),
`gadi41jw` ICC 500 (reference — correct), `gprjb7bo` TAS 201 and `giadco5s` ASTM E1886/E1996
(reference-only per synthesis — defensible; the TAS 201/202/203 + E1886/E1996 windstorm test
family ends up entirely absent from final tags, the one plausibly-real recall gap, though the
Miami-Dade NOA and HVHZ tags carry the substance), `gdw2sfeo` TDI Impact ("beyond what is
already covered" — wrong-ish: only TDI *Non*-impact is covered; minor miss), `guhv8wxy` ANSI UL
10C and `gwt80v25` ICC 500-2014 (bogus-template declines, concepts survive elsewhere).

**Screen verdict:** the screen itself is not dropping real certifications — its 9 kills are 8
clean and 1 borderline. The losses happen one stage earlier, in the oov-grounding declines with
the "standard, not an attestation" template.

---

## Overall verdicts

1. **Equipments: clear regression.** Same upstream, prompt-only delta, and grounding flipped
   from 74/75 correct declines + 1 correct ground to 14 fabricated `<product> manufacturing
   machine` tags + the same 1 correct ground. 9 of the 10 final tags are fabrications. The new
   evidence wording correlates with the model adopting a product→machine existence inference
   that the old prompt's declines explicitly rejected, and relationship screening provides no
   backstop (SCR-1/SCR-2 rubber-stamp the same inference).
2. **Conformity: net win with cleanup owed.** The tag growth is ~half real recall (≈29 → ≈40
   distinct real concepts; the A250.x/ASTM finish-test/FBC/FEMA-P designation families are
   genuine) and ~half duplication (29/71 tags are case, edition, naming, or combined-tag
   variants — no OOV canonicalization, and the two chunks of one group ground independently and
   disagree on casing). Two real concepts were lost (UL 1784, A250.8-2003) to a
   self-contradictory "standard ≠ attestation" decline template that fired on 5 rows.
