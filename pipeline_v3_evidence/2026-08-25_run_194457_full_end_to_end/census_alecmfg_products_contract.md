# Census — alecmfg.com `products` vs `contract_products`
Run `20260825T194457`. Dumps: `packages/logs/extraction_dumps/20260825T194457/alecmfg_com__products.json`, `..._contract_products.json`.

**Coverage: complete. Nothing sampled, nothing extrapolated.** Every item in both dumps was read and coded by hand:
142 + 135 = **277 grounded tag instances**, 226 + 233 = **459 declines**, 142 + 135 = **277 screening verdicts**
(89 + 118 = 207 passed verdicts carrying 178 + 354 = **532 individual rule outcomes**; 53 + 17 = 70 failed verdicts).
Both dumps share the same 381 rows and — verified programmatically — **byte-identical `record.synthesis` on all 381 rows**.

## Coding rules I applied (stated so the counts merge)
- **V (VAGUE)** = the tag's head noun is a filler (`part(s)`, `component(s)`, `product(s)`) and its modifier is evaluative, temporal or shop-wide (`precision`, `production`, `end-use`, `high-precision`, `machined`, `CNC`, `lightweight`). Modifiers naming a **material**, an **industry/application** or a **function** are discriminating → not V. V outranks D/N per the severity order, so a directly-quoted but contentless tag is coded V.
- **P (PARTY)** covers the client's own product, a supplier's product, and bought-in hardware/consumables.
- **X (AXIS)** covers processes, finishes, materials/stock, tooling/fixtures, equipment, facilities, documentation and machined *features* tagged as products.
- Rule codes: **R** cites specific on-point record content; **C** the evidence *is* the verdict restated (typically "…, indicating Alec Model manufactures and sells them as its own product"); **G** boilerplate that would fit any candidate; **I** the explanation's own cited content contradicts the verdict; **M** repeats the preceding rule's fact without doing the new rule's work.
- Failed verdicts judged against subject ground truth (alecmfg is a pure contract shop): **WRONG** = a real contract entity dropped.

---

## 1. Grounded tag instances

| code | products | contract |
|---|---|---|
| D DIRECT | 81 | 76 |
| N NORMALIZED | 1 | 1 |
| B BRIDGE | 12 | 19 |
| V VAGUE | 19 | 19 |
| X AXIS | 18 | 12 |
| P PARTY | 11 | 8 |
| F FABRICATION | 0 | 0 |
| **total** | **142** | **135** |

Restricted to the tags that **survived screening** (what actually lands in the DB):

| code | products (89 passes) | contract (118 passes) |
|---|---|---|
| D | 57 | 69 |
| N | 1 | 0 |
| B | 5 | 19 |
| V | 19 | 16 |
| X | 4 | 9 |
| P | 3 | 5 |

The products screen removes most axis/party junk (18 X → 4, 11 P → 3). The contract screen removes almost none (12 X → 9, 8 P → 5) and lets **every one of its 19 bridge inferences through**.

## 2. Declines

| code | products | contract |
|---|---|---|
| SCOPE (out-of-field by nature: process / material / finish / tooling / equipment / facility / document / feature) | 154 | 154 |
| OK | 71 | 76 |
| TWIN (grounded on the same group's other chunk row) | 1 | 1 |
| LOST (real entity wrongly declined) | 0 | 2 |
| TEMPLATE | 0 | 0 |
| **total** | **226** | **233** |

**SCOPE is 68% and 66% of all declines.** Two thirds of what the search stage hands the product field is not a product at all. The grounding stage is doing the field-boundary work the search stage should have done.

The 2 contract LOSTs:
- `gcocizy4` — synthesis names *"aluminum alloy sheet metal structural housing for a French UAV client"*; the contract dump declined: *"The focal form ('aluminum alloy sheet metal processing') and synthesis describe a manufacturing process, not a product category."*
- `gx4yuxoe` — *"a Swedish electric truck startup urgently required a batch of metal structural parts"*; declined: *"describe the client's urgent requirement for these parts, not Alec Model's own dealing with them."*

## 3. Rule outcomes on PASSED verdicts

### products (89 passes, 178 rule outcomes)
| rule | R | C | G | I | M | total |
|---|---|---|---|---|---|---|
| SCR-1 | 70 | 0 | 2 | 17 | 0 | 89 |
| SCR-2 | 0 | 51 | 0 | 21 | 17 | 89 |
| **all** | **70** | **51** | **2** | **38** | **17** | **178** |

**SCR-2 never produced a single real (R) justification in 89 passes.** 57% of them are circular, 24% inverted, 19% mirrors of SCR-1. Machine check: **40 of the 89 passed explanations literally contain the phrase "as its own" / "its own product"** — the verdict restated as its own evidence.

### contract_products (118 passes, 354 rule outcomes)
| rule | R | C | G | I | M | total |
|---|---|---|---|---|---|---|
| SCR-1 | 85 | 3 | 6 | 24 | 0 | 118 |
| SCR-2 | 75 | 0 | 42 | 1 | 0 | 118 |
| SCR-3 | 21 | 3 | 13 | 0 | 81 | 118 |
| **all** | **181** | **6** | **61** | **25** | **81** | **354** |

**SCR-3 is inert: 81/118 (69%) are pure mirrors of SCR-1** — "Alec Model is the manufacturer, as stated in the synthesis." Only 21 do independent actor work. SCR-2 is the healthiest rule in either dump (75/118 R) because "made for a client" is written plainly in most syntheses.

### FAILED verdicts
| | products | contract |
|---|---|---|
| OK | 53 | 13 |
| WRONG | 0 | 4 |

The products screen's 53 rejections are **all correct** — it is a good filter and a bad justifier. The contract screen's 4 WRONG rejections are its recall holes: `gm5pj38k` "metal parts", `gh749siu` "die cast components", `ggy3moja` "cylindrical parts", `ghxs6flt` "precision-machined components" — each failed for *"does not specify that these are made to customer order or specification"* at a shop where everything is.

## 4. Twin and cross-dump disagreement

**Twins within a dump** (14 group_ids appear on two chunk rows), identical in both dumps:
- status disagreement: **2/14** — `g7cxds0d` (no_candidates | no_mentions), `gs7w5hrz` (no_candidates | grounded)
- tag-set divergence on the same group: **2/14** — `gqtfttqp` → `precision parts` vs `high-precision aluminum parts` + `titanium alloy parts`; `gs7w5hrz` → nothing vs `power module prototype components`

**Cross-dump, same row, same record, identical grounding prompt+payload — 381 rows:**

| products → contract | rows |
|---|---|
| no_candidates → no_candidates | 214 |
| grounded → grounded | 74 |
| screened_out → grounded | 32 |
| no_mentions → no_mentions | 16 |
| screened_out → no_candidates | 12 |
| no_candidates → grounded | 11 |
| screened_out → screened_out | 9 |
| grounded → no_candidates | 7 |
| grounded → screened_out | 5 |
| no_candidates → screened_out | 1 |

- **68 rows (18%) get a different status between the two dumps; 55 of those are "one dump grounded it, the other did not."**
- **31 rows diverge at the GROUNDING stage, which the two dumps run with identical prompt and identical payload** (12 + 19 one-sided declines). Grounding is not a function of its input.
- Of the 120 rows grounded in **both** dumps, **16 (13%) emitted a different tag string** for the same record — e.g. `gmckxdkp`: `aerospace components` vs `3D printed parts`; `ghx1ugh2`: `sheet metal components with ventilation slots` vs `aluminum alloy sheet metal housings`; `glqxl4dh`: `titanium alloys` (X) vs `titanium alloy components` (B).
- Total grounding-stage non-determinism on identical input: **47 of 151 active rows = 31%.**
- The 32 `screened_out → grounded` rows are the split working as intended. The 5 `grounded → screened_out` are it working backwards.

## 5. Ten most consequential errors (verbatim)

1. **`g39j249n` [products] — quote contains the refutation.** Tag `aluminum alloy sheet metal housings`, SCR-2: *"The phrase 'Alec Model manufactures such housings for client testing purposes' shows Alec Model itself manufactures and sells these housings."* The quoted clause says "for client testing purposes."
2. **`gr9whppn` [products] — the calibration case, unchanged.** SCR-2: *"The synthesis indicates Alec Model received technical documentation for the purpose of manufacturing titanium components, showing Alec Model manufactures and sells them as its own."* Receiving the client's drawings is the definition of contract work.
3. **`gucarjqf` [products] — client's system tagged as shop's product.** Tag `thermal systems`, SCR-2: *"The synthesis directly attributes the role of technical manufacturing partner for thermal systems to Alec Model, showing that Alec Model itself manufactures and sells thermal systems as its own."* "Manufacturing partner" is contract language read as ownership.
4. **`g9lrwix5` [products] — silence taken as evidence.** Tag `EV battery enclosures`, SCR-2: *"The statement is general and not tied to a specific client or custom project, indicating Alec Model manufactures and sells EV battery enclosures as its own product."* The record says only *"ready to be a trusted manufacturing partner"*. SCR-2b forbids exactly this inference; identical wording also passed `gdyr4892` (aluminum gearbox housings).
5. **`gcsepkc1` [both] — raw stock as product, in both dumps.** Tag `steel plates` from *"all steel plates were cut and pre-machined in-house from ASTM A36"*; products SCR-2: *"attributes the cutting and pre-machining of steel plates to Alec Model itself, not to a third party or as custom-only work, indicating Alec Model manufactures and sells steel plates as its own product."* The same ASTM A36 sentence's other half, `ASTM A36 plate` (`geznnmz3`), was declined by both dumps as a material.
6. **`g3j37tvk` [both] — documentation as product, in both dumps.** Tag `weld maps`, contract SCR-1: *"'Weld map' is required documentation provided by Alec Model, which is substantive."* From the same four-item documentation list, `WPS/PQR`, `dimensional inspection` and `material certs` were all declined by both dumps.
7. **`g62t40sd` [products] — prospective work passed, guard never fired.** Tags `custom end-effectors` and `mounting plates`, SCR-1: *"The synthesis notes Alec Model is engaged in further component development for robotic arm systems, including custom end-effectors, indicating substantive activity."* The record says *"ongoing discussions."* The contract screen failed both on precisely that reading.
8. **`guwh9llb` [contract] — the material the shop consumes, sold as its contract output.** Tag `aluminum alloy sheets`, SCR-1: *"The synthesis describes Alec Model 'selecting and using aluminum alloy sheets in its manufacturing process to meet client requirements,' which is a substantive activity involving aluminum alloy sheets."* The products dump declined the same row: *"'aluminum alloy sheets' is a material, not a product category."*
9. **`gncq1afn` / `gqb67z6x` [contract] — bought-in hardware as contract product.** `threaded inserts` SCR-1: *"The synthesis specifies that Alec Model manufactures baseplates that include threaded inserts (helicoils), showing substantive involvement."* The cited evidence is about baseplates. `helicoils` passed on the same reasoning. The products screen rejected both correctly.
10. **`g7bdbo9s` / `g8p9945z` [contract] — a surface finish and a machined surface as contract products.** `as-machined surface` SCR-1: *"The synthesis says Alec Model delivers 'as-machined' surfaces when required by the client, which is substantive."* `bearing surface` SCR-1: *"'bearing surfaces' were CNC milled within target tolerances as part of a fabrication process, which is substantive."* The contract dump's own FGR-Q1 on this row admits it is *"a type of machined feature."*

## 6. Unexpected patterns the taxonomy does not name

**U1 — The file-upload flood (15 rows × 2 dumps = 30 decline instances).** The quote form's accepted upload types reached the product field as candidates: `stp`, `igs`, `dwg`, `zip`, `stl`, `Step`, `iges`, `sldprt`, `catpart`, `jpeg`, `png`, `pdf`, `dxf`, plus `project files` and `Design File`. All correctly declined, all wasted calls. This is a search-stage recall defect, not a grounding defect.

**U2 — "It's only a document" (11 rows × 2 dumps = 22 declines).** Case-study *titles* are declined on the ground that the shop only published a document about the thing: `grd8rfp0` — *"The synthesis states Alec Model offers a document (a case study) about this valve body, but does not show any further dealing with the valve body itself beyond the document. No product category is evidenced."* Same shape for `gtfqlgr3`, `getq6kfv`, `gk3g7d6v`, `guo4j7su`, `gcofs2ew`, `gtctd9fk` and their chunk-2 twins. Harmless here only because every entity was recovered from the case-study body rows.

**U3 — Focal-form gatekeeping (6 declines: 1 products, 5 contract).** The decline names the correct product and then refuses it for not being the focal form: `gy1tfp30` — *"The actual product is the bracket, but that is not the focal form here."* Also `gwcy93ro`, `gwd5yds7`, `gym7nm9q`, `gxozzsqg`, `gtwrhlkm`. This contradicts FGR-E1, which asks whether the record's focal form **and synthesis** evidence the entity.

**U4 — Same-sentence splits.** One sentence's list items receive opposite treatment across rows:
- *"bolt hole positions, slot widths, bearing surfaces — were all CNC milled"*: `bolt hole positions` declined by both; `slot widths` → products passed `machined slot components`; `bearing surfaces` → contract passed `bearing surface`.
- *"SLS and SLA rapid prototyping machines, as well as multi-axis CNC machines and lathes"*: `SLS` → contract passed `SLS rapid prototypes`; `SLA rapid prototyping machines` failed both; `lathes` failed products / declined contract; `multi-axis CNC machines` declined by both.
- *"weld map, WPS/PQR, dimensional inspection, and material certs"*: `weld maps` passed both, other three declined by both.
- Surface-finish list: `Smooth Machining` → products passed `smooth machining surface finish`; `Polished`, `Fine Machining`, `Unfinished (as-machined)` declined by both.
- `thin rib structures` (`gyl7ph01`) passed the contract screen; `thin ribs` (`g1ankxao`) was declined by both as a feature.
- `vertical machining centers` (`gjknclnq`) was grounded in products; `HS-V55LS models` (`gxczt6yp`), the same machines named in the same sentence, was declined by both.

**U5 — Feature-as-product (9 passes in products, 28 in contract).** Machined features, surfaces and sub-features become product categories: `counterbore`, `bearing surface`, `ground pads`, `ventilation grids`, `thin rib structures`, `heat fins`, `finned structures`, `machined interfaces`, `installation bases`, `as-machined surface`, and the ad-hoc constructions `components with tolerance-critical bores`, `components with through-holes`, `sheet metal components with ventilation slots`, `machined slot components`.

**U6 — SCR-G1 is dead in the products screen (0 appearances in 142 verdicts; 2 in contract).** At least 7 products passes rest on aspirational or prospective evidence — `custom end-effectors`, `mounting plates` ("ongoing discussions"), `EV battery enclosures`, `aluminum gearbox housings`, `complex thermal management systems` ("ready to be"), `sensor brackets` ("quoted a batch run and preparing to supply"), `high-precision parts` (from a section literally headed "Future Collaboration"). None triggered the guard. Meanwhile the *grounding* stage declined five other prospective items (`vertical panel brackets`, `cable tray bases`, `pre-series production`, `product family`, `modules for a new inverter control platform`) for exactly that reason — the guard lives upstream, by accident.

**U7 — Material-list → invented product line (11 bridges).** A bare "Material Options" bullet becomes a product: `Brass` → `brass components`, `Mild Steel` → `mild steel parts`, `ABS` → `ABS parts`, `Nylon` → `nylon parts`, `Plastics` → `plastic parts`, `Nickel` → `nickel parts`, `EN AW-7475-T7351` → `components made from EN AW-7475-T7351 aerospace aluminum`. All passed screening. Their justification is one of two literal templates, each appearing 4× verbatim in the contract dump: *"These services are offered to customers, indicating customer-directed work."* and *"Alec Model itself offers these manufacturing services."* Meanwhile 12 chemically identical rows (`Aluminum`, `Copper`, `Zinc`, `Inconel`, `Magnesium`, `Alloy Steel`, `Polycarbonate`, `POM`, `PTFE`, `PEEK`, `PVC`, `HDPE`) were declined by both dumps. The split is arbitrary.

**U8 — The record can carry the party error upstream.** `g4mrl2az` / `g237yzhv` syntheses assert *"Alec Model manufactured gripper modules"* when the underlying case study is about a bracket for a client's gripper. Both dumps then faithfully ground and pass `gripper modules`. The same client's gripper is correctly refused one row over: `g5u33m0u` — *"attributes the development of the end-of-line robotic gripper to the client, not Alec Model."*

## 7. The key question, in counts

**Of the products screen's 89 passes, how many are defensible as alecmfg's OWN catalogue products? Zero.**

I judged all 89 individually. Every one is customer-directed work (client case studies, custom brackets, client prototypes, per-client housings and heat sinks), a wrong-axis object (4: `smooth machining surface finish`, `traceable label sets`, `weld maps`, `steel plates`), a wrong-party object (3: `gripper modules` ×2, `thermal systems`), or a contentless generic (19 V). The three closest calls — `EV battery enclosures`, `aluminum gearbox housings`, `complex thermal management systems` — all come from one sentence, *"ready to be a trusted manufacturing partner for…"*, which is aspirational **and** explicitly contract-partner framing.

- **Products precision against the field as defined (own catalogue products): 0/89 = 0%.**
- Products precision against a looser standard (a real product-shaped entity the shop actually works on, ignoring whose it is): 82/89 = 92%.
- Ground truth says this list should be **near-empty**. It has 89 entries. The 53 correct rejections show the screen *can* see the distinction — `g9q94yhy`, `g8rkxb1m`, `gzm91gmm`-class rows were rejected with clean reasoning like *"The components are made for a client (semiconductor equipment), indicating they are custom-manufactured to client requirements, not as Alec Model's own product."* It simply fails to apply that reading to the other 89.

**What does the contract screen miss?**
- 4 WRONG rejections (§3) — real contract entities failed for lack of an explicit customer-order statement.
- 2 LOST declines at grounding (§2).
- 12 rows where products screened_out and contract found no candidates at all, plus 7 where products grounded and contract declined — 19 rows where the contract dump has nothing to say about a record the products dump did engage with.
- **Contract precision: 69/118 = 58% strictly correct** (specific, right axis, right party). 85/118 = 72% if contentless-but-true generics count. **14/118 = 12% are wrong-axis or wrong-party** (fixtures, jigs, raw sheet, packaging, labels, weld maps, counterbores, surfaces, purchased helicoils, the client's gripper modules and thermal systems). **19/118 = 16% are unevidenced material→product or feature→product inferences.**

## 8. What the numbers say
The screening split is real and it works in the direction it was built for: 32 rows correctly move from products-rejected to contract-accepted, and the products screen correctly rejects 53 candidates with no false rejections. Three things spoil it.

1. **The products screen has no positive test for ownership.** Its SCR-2 produced 0 real justifications in 89 passes; 40 of them restate the verdict verbatim. It passes anything a synthesis attributes to Alec, and every contract job is attributed to Alec.
2. **The contract screen has no axis or party test.** SCR-3 is 69% mirror text, so tooling, raw stock, packaging, documentation and the client's own assemblies walk through on SCR-2's "…for a client" alone.
3. **Grounding is not reproducible.** 31% of active rows behave differently across two runs of the same prompt on the same payload. Any A/B on the screening rules measured at this scale is reading noise unless the effect is larger than ~30% of active rows.
