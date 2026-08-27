# Census — steelcraft.com / `products` — run 20260825T194457

Dump: `packages/logs/extraction_dumps/20260825T194457/steelcraft_com__products.json`
Subject: Steelcraft — catalogue manufacturer of hollow metal steel doors and frames.

**Coverage: complete. 680 rows, 454 grounded tag instances, 264 declines, 454 screening verdicts
(413 passed / 41 failed → 826 rule outcomes on passed verdicts). Every item was read and coded by
hand, in 26 grounding batches and 8 decline batches. Nothing was sampled or extrapolated.**

Row statuses: grounded 361 · screened_out 38 · no_candidates 264 · no_mentions 17.
559 distinct group_ids; 121 appear in both chunks, 115 of those as codeable row pairs.

---

## 1. Count tables

### 1.1 Grounded tag instances (row × tag), n = 454

| Code | Meaning | Count | Share |
|---|---|---:|---:|
| D | DIRECT — synthesis names/describes the tagged entity as the subject's | 152 | 33.5% |
| N | NORMALIZED — canonical/reworded form of what the synthesis says | 149 | 32.8% |
| V | VAGUE — too generic to be usable | 60 | 13.2% |
| X | AXIS — real entity, wrong field (component, material, finish, process, custom work) | 43 | 9.5% |
| B | BRIDGE — needs an inference the synthesis does not state | 31 | 6.8% |
| P | PARTY — real entity, wrong actor (supplier's / third party's) | 19 | 4.2% |
| **F** | **FABRICATION** | **0** | **0.0%** |

**Zero fabrications.** Every tagged entity is traceable to text in its own record. The failure
mode of this field is not invention — it is *mis-typing* (X), *over-generalising* (V) and
*inferring* (B).

Split by final screening outcome:

| Code | passed (n=413) | failed (n=41) |
|---|---:|---:|
| D | 142 | 10 |
| N | 146 | 3 |
| V | 58 | 2 |
| X | 36 | 7 |
| B | 29 | 2 |
| P | **2** | 17 |

The screen is a good *party* filter (17 of 19 P tags rejected) and almost no filter at all for
axis/vagueness/bridge: 123 of the 134 V+X+B tags survive it.

### 1.2 Declines, n = 264

| Code | Meaning | Count | Share |
|---|---|---:|---:|
| OK | nothing groundable; decline correct | 141 | 53.4% |
| SCOPE | correctly declined, belongs to another field (materials, processes, attestations) | 52 | 19.7% |
| TEMPLATE | declined by an argument that misfires | 35 | 13.3% |
| TWIN | the same group grounded in its other chunk | 26 | 9.8% |
| LOST | a real in-scope entity was evidenced and wrongly declined | 10 | 3.8% |

The decline path is **much healthier than the grounding path**: 73% of declines are correct
(OK + SCOPE). The SCOPE bucket is worth noting on its own — 52 declines are correct *here* and
would be positives for `material_caps` / `process_caps` / `conformity_attestations`
(cores, galvannealed steel, polystyrene, tack welding, hinge preps, ANSI/ASTM/UL compliance).

The 10 LOST declines: `g3lg06wn` "Steelcraft Paladin Door", `g6ti62rq` "FP Series Frames",
`gqcdc4xl` "Falcon SZ Series" (×2, both chunks), `ge91wq09` "High Definition Door" (×2, both
chunks), `gh0ww3q3` "acoustical door assemblies", `girndbt9` "acoustically rated doors and
frames", `g8som4hv` "Tornado-Resistant Doors", `grf93fgl` "CE Series doors". All are the
subject's own product lines. All were declined for the same reason: *the record only offers a
document / a benefit claim about them*. None cost the final output anything (each category is
grounded from some other row), but they show the grounding stage applying a **dealing test that
belongs to screening**.

### 1.3 Rule outcomes on the 413 PASSED verdicts (826 outcomes)

| Code | SCR-1 (substantive dealing) | SCR-2 (subject is the maker/seller) |
|---|---:|---:|
| R REAL | 148 (35.8%) | 189 (45.8%) |
| C CIRCULAR | 31 (7.5%) | 168 (40.7%) |
| G GENERIC | 165 (40.0%) | 6 (1.5%) |
| I INVERTED | 69 (16.7%) | 16 (3.9%) |
| M MIRROR | 0 | 34 (8.2%) |
| **non-R total** | **265 (64.2%)** | **224 (54.2%)** |

Read plainly: **only about a third of SCR-1 outcomes and under half of SCR-2 outcomes cite
evidence that actually tests the rule.** SCR-1's dominant failure is G — it accepts a standard,
a rating, an application, a market or bare availability in place of a described making/selling
dealing. SCR-2's dominant failure is C — "the products are described as Steelcraft's own, so
Steelcraft manufactures and sells them", with no first-party marker quoted from the record.

The 69 SCR-1 `I` outcomes are the sharpest finding: SCR-1a says explicitly that a product
"only serviced, repaired, installed, tested, inspected, or certified, or used as an input, tool,
or piece of equipment" does not satisfy the condition — and 69 passed SCR-1 outcomes cite
precisely one of those involvements (a bare menu listing, a certification, an installation
instruction, a component inclusion) as the thing that satisfies it.

### 1.4 The 41 FAILED verdicts

| Code | Count |
|---|---:|
| OK (correct rejection) | 39 |
| WRONG (real product wrongly rejected) | 2 |

The 2 WRONG: `gyx2id7a` and `gzirp14k`, both quoted in §3.

---

## 2. Twin disagreement

115 group_ids produced a codeable row in *both* chunks. My judgments differ on **52 of 115
(45%)**:

| Kind of disagreement | Pairs |
|---|---:|
| One chunk grounds, the other declines | 26 |
| Both ground, but emit a different tag set | 25 |
| Same tag set, different code from me | 1 |
| Identical | 63 |

Two of the 26 ground/decline flips are *screening* flips on byte-identical evidence
(`gudlkmfn`, `gd64cgkv` — §3.6). The 25 tag-set disagreements are not noise around a stable
answer, they are systematic naming instability:

```
gbubf8pp   A: doors                          B: L Series doors
glfts2vg   A: T Series flush doors           B: flush doors
g31tt6mn   A: LS Series Stainless Steel Doors B: stainless steel doors
gcafg2sk   A: flush doors                    B: SZ Series square edge flush doors
gp63oj6g   A: A14 Series steel doors         B: full glass doors
gj1fwnwq   A: windstorm-resistant door assemblies
           B: steel doors for STC and healthcare applications
gq7wtoss   A: steel doors, drywall frames, casing-ready frames   B: doors, door frames
gg35z4qb   A: fire-rated steel doors, fire-rated frames
           B: DE Series double egress frames, SL Series square edge flush doors
gxy55kvr   A: borrowed lights                B: borrowed light frames
gw51sl2z   A: transoms                       B: transom sidelight frames
gl4f5xc4   A: top and bottom caps for doors (FAILED)
           B: flush doors with top and bottom caps (PASSED)
g793uv47   A: fire-rated doors with ceramic glass (PASSED)  B: ceramic glass (FAILED)
```

The same group, on the same website text, oscillates between the head noun, the series name,
the accessory, and the accessory-as-door-variant. `gl4f5xc4` is the clearest: rejected as an
accessory in one chunk, admitted in the other simply because the tag was written as a door
variant instead.

---

## 3. The ten most consequential errors (verbatim)

### 3.1 An input is admitted while its byte-twin is rejected — `gzx9ch70` vs `gg19x8di` (chunk A)
`gzx9ch70` TAG **"steel face sheets"** — PASSED:
> SCR-2[satisfied]: The synthesis says 'Steelcraft uses steel face sheets in its door
> construction,' indicating Steelcraft manufactures and sells steel face sheets **as part of
> its doors**.

`gg19x8di` TAG "steel door faces" — FAILED, same page, same sentence family:
> SCR-2[failed]: The synthesis shows that steel door faces are a component used in Steelcraft's
> doors, not a standalone product category that Steelcraft manufactures and sells as its own.

The passing explanation states the disqualifying fact ("as part of its doors") and passes anyway.

### 3.2 Commodity fasteners as a product — `gsabhkuk` (both chunks, both PASSED)
> SCR-2[satisfied]: The synthesis says 'Steelcraft provides screws for these purposes in its
> frame products,' indicating Steelcraft **supplies these screws as part of its manufactured
> products**.

Meanwhile `gf4juknn` "wedge-lock corner clips", identical evidence shape, FAILED:
> SCR-2[failed]: ... the record does not state that Steelcraft manufactures or sells wedge-lock
> corner clips as a standalone product category. The clips are a feature of the frames.

### 3.3 Paint admitted as a catalogue product — `glrpgdac`, `gl4iv9nm`, `gllrfpo3` (chunk A)
`gl4iv9nm` TAG **"industry standard tested paint for doors"** — PASSED:
> SCR-2[satisfied]: The phrase 'applied to any of Steelcraft's trusted door solutions' and
> 'Steelcraft's team will work with customers' shows Steelcraft itself applies and offers this
> paint **as part of its product**.

`gfc7e10i` "finish paint colors" FAILED on the same page; `gxqtqolt` "FINISH PAINT" was declined
outright. Three outcomes for one page section.

### 3.4 Shipping crates as a manufactured product — `gelggini` (chunk B, PASSED)
> E1[satisfied]: The focal form is 'Custom crates', and the synthesis states 'custom crates are
> available to minimize risk of damage during transit.' This evidences the product category of
> custom crates.
> SCR-2[satisfied]: ... Steelcraft provides custom crates for shipping from its Kansas City
> Service Center, showing Steelcraft itself manufactures and sells them.

### 3.5 A colour swatch as a manufactured product — `g2vf2pxf` (chunk B, PASSED)
> SCR-1[satisfied]: The synthesis states Steelcraft provides GRAINTECH swatches upon request,
> which is substantive.
> SCR-2[satisfied]: Steelcraft is the party providing GRAINTECH swatches, indicating it
> manufactures and sells them.

### 3.6 Two screening verdicts reversed between chunks on identical text
`gudlkmfn` "fire doors" — chunk A FAILED:
> SCR-2[failed]: The synthesis only describes the regulatory standards that fire doors must
> meet, but does not state that Steelcraft manufactures or sells fire doors as its own product.

chunk B PASSED, on the same NFPA-80 sentence:
> SCR-2[satisfied]: The synthesis attributes this information to 'Steelcraft's own copy under
> 'FIRE-RATED STEEL DOORS'', showing Steelcraft manufactures and sells fire doors.

`gd64cgkv` "fire-resistance-rated frames" flips the same way — chunk A: "only describes a
requirement"; chunk B: "discusses these frames **as part of Steelcraft's product line**", a
clause that appears nowhere in the record.

### 3.7 The subject's own series read as a third-party comparison — the 2 WRONG rejections
`gyx2id7a` (chunk B), TAG "drywall frames" — FAILED:
> SCR-1[failed]: The synthesis states that the MU Series provides backbend returns similar to
> the profile of DW or K Series drywall frames, indicating that DW or K Series drywall frames
> are referenced only as a design comparison, **not as a product Steelcraft offers**.

`gzirp14k` (chunk B) fails identically on KS Series. DW, K and KS are Steelcraft's own frame
series. The screen treated an internal product-to-product comparison as an outside reference.

### 3.8 Outside-the-record world knowledge, stated openly — `gpp58w24` (chunk A, PASSED)
> E1[satisfied]: The focal form ('Steelcraft's products') and synthesis state that Steelcraft's
> products are designed for institutional, commercial, and industrial markets, which, **given
> the context of the company**, refers to steel doors and frames.

FGR-E1b forbids deriving a category from markets served. The rule reports *satisfied* while its
explanation describes the exact violation. (The chunk-B twin of this group declined it.)

### 3.9 A product category minted from a standard's scope sentence — `g7o5ubog` (chunk B, PASSED ×2)
> E1[satisfied]: The synthesis states 'standard commercial steel doors and frames' as the
> category for which Steelcraft's products are designed to **exceed industry standards**.

"standard commercial steel doors" / "standard commercial steel frames" are lifted from the title
text of ANSI A250.8-2017 (SDI-100), which FGR-E1b names as a forbidden source.

### 3.10 Frame corners as a product category — `gojzgzho` (chunk B, PASSED)
> TAG 'MU Series multi-use flush frame corners'
> SCR-2[satisfied]: The synthesis attributes the manufacture of these frame corners to
> Steelcraft, indicating it manufactures and sells them.

Runner-up, same family: `gju2rxse` TAG **"doors with Schlage or Von Duprin hardware"** (PASSED) —
a Steelcraft product category named after two Allegion hardware brands.

---

## 4. Unexpected patterns (named, counted, quoted)

Excluded as already known: near-duplicate door/frame tag explosion; products/contract screens
co-admitting.

**U1 — The passing explanation states the disqualifying fact. (16 SCR-2 outcomes, coded I.)**
A recurring shape: SCR-2 quotes evidence containing "as part of its doors/frames/products" or
"uses X in its construction", then concludes the subject manufactures and sells X. Instances:
`gzx9ch70`, `gsabhkuk` (×2 chunks), `guuoe1u8`, `g2qbp64a`, `g7ttduh2`, `gwjvjhxy`, `g0hmowgv`,
`gxtosca1`, `gmd9gvdg`, `go8wkisj`, `grzmso99`. Verbatim, `g2qbp64a`:
> SCR-2[satisfied]: Steelcraft is described as offering these core systems **as part of its
> doors**, indicating it manufactures and sells them.

**U2 — Proprietary series/trade names leak into the persisted tag. 58 passed D/N tags.**
FGR-Q1a says to "carry none of what singled it out into the category name". Passed tags
violating it include `L Series doors`, `T Series flush doors`, `LS Series Stainless Steel Doors`,
`FP14 Paladin Series tornado safe frames`, `E6 embossed panel doors`, `H16 Series doors`,
`HE16 Series doors`, `GRAINTECH doors`, `INPACT Series Door Systems`, `PW14 Paladin Door System`,
`MU Series multi-use flush frame corners`, `KS Drywall Series frames`, `DE Series frames`,
`FS Series frames`, `KS Series frames`, `MS Series frames`. These are catalogue SKU lines, not
ontology categories; a curator must strip or drop every one.

**U3 — Logistics is mined as product evidence, and simultaneously rejected. 30 passed tags vs 17 declines.**
30 passed tag instances have no cited dealing other than expedited shipping, stock inventory or
express-program eligibility — `5-Day Express Doors`, `kwik-pak doors`, `kwik-pak frames`,
`stock package small parts`, `configured doors`, `configured frames`, `door stock`, `frame stock`,
`stock doors`, `stock frames`, plus bare `doors`/`frames` from six shipping rows. Verbatim,
`gj7m55fz`:
> SCR-2[satisfied]: The synthesis shows Steelcraft is offering expedited shipping for select
> doors, **implying** Steelcraft is the manufacturer and seller of these doors.

The same programs are declined 17 times elsewhere ("2 Day Door Express", "3 Day Frame Express",
"EXPRESS PROGRAMS", "quick ship stock", "standard 5-day stock shipping", "Express Service from
the factory", "Custom frame Express shipping", …), each time as "a shipping option, not a
product category".

**U4 — A glossary section is read as a catalogue. 5 passed tags.**
The site's FRAME ELEVATIONS glossary defines terms; the pipeline converts each definition into
an offering. `grgonped` verbatim:
> SYN: A bullet point ... defines 'Transom' as a door frame with a transom bar and glass, panel,
> or louver above the door opening. This shows that Steelcraft **describes and offers** transom
> frames as part of their product range.

Same for `gnpsqqvp` (ceiling height frames), `gfhsmf4o` (transom sidelight frames), `g25lsvyz`
(sidelight frames), `gns4slcz` (borrowed light frames). "describes" and "offers" are fused in the
*synthesis*, and both screening rules then cite that fused sentence.

**U5 — Documentation-only evidence is admitted 5 times and rejected 40+ times.**
Passed on a download-list entry alone: `g7arnny3` "FT Series frames (Thermal Break)",
`g92a0lvv` "integral kerfed frames", `gwroacwz` "FS/KS/MS Series frames" (×3),
`gwt7n3hj` "sound transmission control doors"/"…frames" (×2). Rejected on the identical shape:
`ghn78zyl`, `g3ycp174`, `g6ti62rq`, `gqcdc4xl`, `ge91wq09`, `grf93fgl`, `g55omy8v`, plus ~35
pure-document declines. Two of the rejections are LOST (§1.2).

**U6 — Grounding declines on ownership, which FGR-E1a explicitly forbids. 35 TEMPLATE declines.**
E1a: "Whose product category it is has no bearing here. A candidate evidenced by a customer's,
supplier's, or any other third party's dealings is still identified and recorded." Yet
`goauvpap` verbatim:
> DECLINED: The focal form 'Allegion hardware' and the synthesis describe hardware made by
> Allegion, which is used with Steelcraft's products, but **do not evidence a product category
> manufactured or supplied by Steelcraft**.

Same for LCN Closers, LCN Magnetic Holders, Schlage LM9300 Levers, FireLite fire-rated glass
(×2), swing clear hinges, concealed closers, concealed vertical rods, EMA anchors, wire/Masonry-T
anchors, stud anchors (×2), compression anchors (×2), standard/heavy-weight hinges (×5). ~20 of
the 35 declare a genuine trade product category to be "not a product category". The outcome is
right; the reasoning is doing screening's job at the grounding stage, which is why the same
entities get *grounded* whenever the sentence is phrased differently (anchors ×2, screws ×2,
hardware preps).

**U7 — Component / feature-compound minting. 65 passed tags (29 B + 36 X).**
Passed tags naming a part, an input, a finish, an option, or a feature rather than a catalogue
product: `mullions`, `dividers`, `sill sections`, `transom bars`, `door stops`, `frame hospital
stops`, `stainless steel stiffeners`, `door panels`, `core systems for steel doors`,
`component frame material`, `stick system components`, `open frame components`, `frame open
sections`, `closed sections for frames`, `galvannealed steel components`, `frame lead-lining
clips`, `lead-lining clips`, `glass light kits`, `flush lite kits`, `anchors`, `frame anchors`,
`weld-in anchors`, `screws`, `hardware preps`, `door lights`, `GRAINTECH finishes` (×2),
`GRAINTECH swatches`, `wood-look steel door finishes`, `hand-stained embossed door finishes`,
plus feature-compounds like `frames with factory countersunk holes`, `frames designed for screw
attachment at the sill`, `stainless steel doors with seamless and interlocking edge`,
`stainless steel doors with reinforcement components`, `flush doors with top and bottom caps`,
`doors with lights`, `doors with factory-installed exit hardware`, `frames with 5-3/4" jamb
depth`, `two-piece frames`.

**U8 — One sentence split into parallel "categories". 11 tags across 4 rows.**
`gyti9a4s`/`gv4ywzln`/`gf5i4wta` turn "STC 46 on Singles and 43 on Pairs" into three product
categories (`sound-rated door and frame assemblies`, `sound-rated single door assemblies`,
`sound-rated door and frame assemblies for pairs of doors`). `gn2yad8w` splits "H, HE
(embossed), TH (temperature rise)" into three; `gwroacwz` splits "FS, KS, & MS Series" into
three; `gztju3ft` splits "Frame and weld-in anchors" into two and defends the split with a
false claim:
> FGR-Q2[satisfied]: 'Weld-in anchors' is distinct from 'frame anchors' as a product category,
> **as both are listed separately in the focal form**.

They are not: the focal form is the single phrase "Frame and weld-in anchors".

**U9 — FGR-Q1 adjudicates a string other than the one persisted. 29 instances.**
The determinacy rule quotes and approves a term that differs from the tag actually written out,
e.g. `gbubf8pp` approves "'L Series doors' refers to a determinate product category" while the
persisted tag is `doors`; `gslypwhc` approves "'A14 Series door'" and persists `doors`;
`gos8cty5` approves "'F/MU'" and persists `steel door frames`; `gt2n55a0` approves
"'INPACTTM Series'" and persists `doors`. In 29 cases the quality gate is not evaluating the
string that reaches the database.

**U10 — Identical SCR-2 explanation byte-copied across every tag in a row. 4 of 51 multi-tag rows.**
`gq7wtoss` (3 tags), `gn2yad8w` (3 tags), `g5mzx46v` (3 tags), `g6s91w57` (2 tags) each carry one
SCR-2 sentence repeated verbatim for every candidate, so the per-candidate actor test is not
being performed per candidate.

**U11 — Word-sense conflation: caught twice, missed once.**
Caught: `gpk94tty` separates "lead" the metal from "lead times"; `grue4dtg` separates "clear
coat" from "clear opening width". Missed: `g5da8x1s` grounds `flush doors` partly on
"Recessed Dezigner™ Glass Trim creates a clean edge, **flush with the door surface**" — the
adverbial sense of "flush", not the door style.

**U12 — Own-name saturation is high but harmless here.** `record_own_name_hits` averages ~2 per
row and reaches 7; every synthesis names Steelcraft. No tag leaked the company name.

---

## 5. Precision estimate

Final output = the 413 passed tag instances (226 distinct strings).

| Reading | Kept | Precision |
|---|---:|---:|
| **Permissive** — keep every D and N (any correctly-typed door/frame category, series names tolerated) | 288 / 413 | **69.7%** |
| **Ontology-grade** — additionally drop the 58 passed D/N tags carrying a proprietary series or trade name (FGR-Q1a) | 230 / 413 | **55.7%** |
| Distinct-string view, permissive | 149 / 226 | 65.9% |

**A curator would keep roughly 56–70% of what this field currently emits; my working estimate
is ~60%.** The discard is not random: 58 V (bare `doors`/`frames`/`Severe Weather products`/
`stock`/`configured` tags), 36 X (components, finishes, custom work, shipping crates), 29 B
(inferred variants), 2 P (screws) — and then 58 SKU-line names that need stripping before they
can be used as categories.

Recall is better than precision: only 10 declines (3.8%) are false negatives, and every one of
them names a category that some other row supplies anyway. **The problem in this field is
over-admission, not under-admission** — and the screen, which exists to correct that, currently
removes 41 of 454 tags (9%) and cites rule-testing evidence in only about a third of the
outcomes it passes.

---

## Method note

Per-item codes are on disk in the session scratchpad (`codes_tags.txt`, 454 lines;
`dec_final.txt`, 264 lines; `twin_diffs.txt`, 52 pairs). Every code was assigned by reading the
item; scripts were used only to enumerate, to join twins, and to total. Two mechanical counts —
the 29 Q1/tag mismatches and the 4 byte-identical SCR-2 reuses — were computed by string
comparison and then eyeballed, and are labelled as such above.

Judgment calls worth knowing about, since they move the headline numbers:
- Bare `doors` (23 instances) and bare `frames` (21) were coded **V**. For a subject that makes
  only doors and frames these carry no discriminating information. If a curator accepts coarse
  head nouns, permissive precision rises from 69.7% to about 80%.
- Tags naming custom/made-to-order work (`custom doors` ×2, `custom frames`) were coded **X**,
  since `products` is the catalogue field and SCR-2c explicitly excludes items made to a
  customer's specification.
- SCR-2 was coded **R** when the explanation quotes a first-party ownership marker from the
  record ("Steelcraft's own copy", "our", "its product line", a quoted manufacture/offer verb)
  and **C** when it only asserts "the synthesis attributes X to Steelcraft". That line is the
  single largest driver of the 168 C count.
