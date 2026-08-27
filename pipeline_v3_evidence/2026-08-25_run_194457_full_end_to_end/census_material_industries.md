# Census — `material_caps` + `industries`, run 20260825T194457

Exhaustive, hand-assigned. **Every** item in the four dumps was enumerated to a compact line and read
individually; no sampling, no regex/keyword coding. Totals classified:

| dump | rows | tag instances | declines | verdicts | grounding-rule instances | screening-rule instances |
|---|---|---|---|---|---|---|
| steelcraft_com__material_caps | 108 | 99 | 103 | 99 | 198 | 200 |
| alecmfg_com__material_caps | 92 | 64 | 90 | 64 | 128 | 128 |
| steelcraft_com__industries | 82 | 120 | 50 | 120 | 240 | 240 |
| alecmfg_com__industries | 127 | 110 | 120 | 110 | 220 | 221 |
| **total** | **409** | **393** | **363** | **393** | **786** | **789** |

## Coding conventions used (so counts merge)

* `D` the tag names the material/sector as the synthesis states it. `N` the tag is the vocabulary's
  normalised/parent form of something specifically named (`A60 Galvannealled steel`→`Steel`,
  `hotels`→`Commercial Construction`). `B` an inference step not present in the synthesis (includes
  grounding on **negated/comparative** evidence). `V` the tag is real but so generic or so non-categorical
  it carries no information (`Metal`, `Polymer`, `Chemicals`, `Honeycomb`, `Thermal Systems`).
* `X` real but wrong field. For `industries` this splits into **own-activity** (the subject's own
  manufacturing/services tagged as an industry served) and **wrong-sector** (a real sector, but not the
  one evidenced). Both sub-counts are reported.
* `P` real but another actor's (customer's wall/building, a supplier's component, the *client's own*
  customer sectors).
* Rule outcomes on PASSED verdicts: `R` the explanation points at record evidence that answers the rule.
  `M` MIRROR — it restates the grounding stage's inference instead of testing it. `I` INVERTED — the
  evidence it cites argues *against* the outcome it records. (`C`/`G` were looked for and never found:
  0 circular, 0 generic across all 393 verdicts.)

---

## 1. Tag-instance codes, per dump, split by grounding stage

### steelcraft_com__material_caps — 99 instances (77 in-vocab, 22 OOV)

| code | in-vocab | OOV | total |
|---|---|---|---|
| D DIRECT | 43 | 16 | **59** |
| N NORMALIZED | 12 | 0 | **12** |
| B BRIDGE | 13 | 1 | **14** |
| V VAGUE | 4 | 2 | **6** |
| P PARTY | 4 | 3 | **7** |
| F FABRICATION | 1 | 0 | **1** |
| X AXIS | 0 | 0 | **0** |

### alecmfg_com__material_caps — 64 instances (61 in-vocab, 3 OOV)

| code | in-vocab | OOV | total |
|---|---|---|---|
| D | 38 | 3 | **41** |
| N | 13 | 0 | **13** |
| V | 7 | 0 | **7** |
| B | 1 | 0 | **1** |
| X | 1 | 0 | **1** |
| P | 1 | 0 | **1** |
| F | 0 | 0 | **0** |

### steelcraft_com__industries — 120 instances (73 in-vocab, 47 OOV)

| code | in-vocab | OOV | total |
|---|---|---|---|
| D | 24 | 37 | **61** |
| N | 24 | 0 | **24** |
| B | 11 | 1 | **12** |
| X | 10 | 3 | **13** |
| P | 3 | 3 | **6** |
| V | 0 | 3 | **3** |
| F | 1 | 0 | **1** |

`X` breakdown: **6 own-activity** (`Manufacturing` from "manufactures … in the United States";
`Energy` from its own LEED energy reporting; `Construction and Infrastructure` + `Building Products
Distribution` from its own 1933 founding; `Commercial Construction` + `Metal Building Manufacturing`
from the division it *sold* in 1959) and **7 wrong-sector** (4× `Industrial Machinery and Equipment`
for industrial *buildings*; 2× `Healthcare Equipment` for hospitals/labs; `Food and Beverage` for
restaurants).

### alecmfg_com__industries — 110 instances (97 in-vocab, 13 OOV)

| code | in-vocab | OOV | total |
|---|---|---|---|
| D | 61 | 5 | **66** |
| X | 18 | 4 | **22** |
| P | 9 | 0 | **9** |
| B | 6 | 2 | **8** |
| N | 3 | 0 | **3** |
| V | 0 | 2 | **2** |
| F | 0 | 0 | **0** |

`X` breakdown: **21 own-activity**, **1 wrong-sector** (`Consumer Electronics` for a UK industrial
power-module client). 16 of the 18 `Industrial Machinery and Equipment` instances are Alec's own
services (Manufacturing Services, production, prototyping, engineering, Manufacturing, CNC Machining,
3D printing, international manufacturing, OEM, 3D printing manufacturer, manufacturing applications,
machine shop, precision manufacturing solutions, precision machining, precision machining fields);
the other 5 own-activity X are `Additive Manufacturing` ×2, `Packaging`, `Sheet Metal Fabrication`,
`International Manufacturing`.

**Combined `industries` own-activity total: 27 of 230 tag instances (11.7%).**

## 2. Decline codes

| dump | OK | SCOPE | TWIN | LOST | TEMPLATE | total |
|---|---|---|---|---|---|---|
| steelcraft matcaps | 89 | 13 | 0 | 1 | 0 | 103 |
| alec matcaps | 82 | 6 | 2 | 0 | 0 | 90 |
| steelcraft industries | 32 | 13 | 5 | 0 | 0 | 50 |
| alec industries | 109 | 8 | 3 | 0 | 0 | 120 |
| **total** | **312** | **40** | **10** | **1** | **0** | **363** |

`SCOPE` = correctly declined because the focal form is a process/finish/property/profession, not a
material or a sector. `TWIN` = this row declined while the *same group_id* in the other chunk grounded
a tag; in 7 of the 10 the declining half is the **correct** half (`defense`, `CNC Machining`).
The single `LOST`: steelcraft matcaps `g4rno1vt` "Prime Paint", where the OOV pass declined `Paint`
— a material the same stage emitted one row later.

## 3. Rule outcomes

### On PASSED verdicts

| dump | verdicts | SCR-1 R / M | SCR-2 R / M / I |
|---|---|---|---|
| steelcraft matcaps | 78 | 57 / 21 | 57 / 20 / 1 |
| alec matcaps | 63 | 41 / 22 | 41 / 22 / 0 |
| steelcraft industries | 105 | 59 / 46 | 59 / 43 / 3 |
| alec industries | 93 | 55 / 38 | 55 / 13 / 25 |
| **total** | **339** | **212 / 127** | **212 / 98 / 29** |

`M` is assigned whenever the tag was reached by an inference (`N`/`B`/`V`/`X`) and the screening
explanation simply repeats that inference rather than testing it — screening never independently
re-derives a sector mapping in this run. `I` is assigned where SCR-2 cites, as proof that the subject
*serves* a sector, evidence that it *is in* that sector or that the **client** is.

### On FAILED verdicts

| dump | verdicts | OK | WRONG |
|---|---|---|---|
| steelcraft matcaps | 21 | 20 | 1 |
| alec matcaps | 1 | 1 | 0 |
| steelcraft industries | 15 | 12 | 3 |
| alec industries | 17 | 7 | **10** |
| **total** | **54** | **40** | **14** |

Raw outcome tallies (mechanical): SCR-G1/violated fires exactly twice in the whole set (steelcraft
matcaps `gttgtpgg`, correctly killing a 1959 divestiture; alec industries `gwc8e349`, correctly
killing a prospective "discussions expected next quarter"). It does **not** fire on steelcraft
`ggd507mb`, which grounds an industry on the company's 1933 founding and passes.

## 4. Emitted despite a FAILED rule in its own applied-rules list

**Exactly 2, both in steelcraft_com__material_caps; 0 in the other three dumps.**

1. `gwjvjhxy` → `Stainless Steel Stiffener`, `OGR-N1/failed` ("'Stainless Steel' is already identified
   … a stiffener made of stainless steel does not constitute a new material") — tag emitted, screening
   passed it. (The calibration case; confirmed.)
2. `gpk94tty` c0 → `Lead`, `IGR-E1/failed` ("the synthesis refers to 'lead time' … not the material
   lead. No material is evidenced") — tag emitted anyway, then `IGR-M1/chosen` records "No material is
   evidenced; 'lead' here refers to time, not the element." Screening caught it.

Both are the same code-level defect: a rule list containing a non-satisfied outcome does not suppress
the tag. The `Lead` case is the more alarming one because the failing rule is the *evidence* rule.

## 5. Duplicate census (distinct tags that pass screening)

| dump | distinct passed tags | near-duplicates of another | of which pure casing/plural |
|---|---|---|---|
| steelcraft matcaps | 26 | **10** | 4 |
| alec matcaps | 33 | **2** | 0 |
| steelcraft industries | 40 | **19** | 0 |
| alec industries | 37 | **10** | 0 |

* steelcraft matcaps: `Mineral board`/`Mineral Board`; `Galvannealed Steel`/`Galvannealed steel`/
  `Galvanized Steel`; `Stainless Steel`/`Stainless Steel Stiffener`; `Honeycomb`/`Paper honeycomb`/
  `Kraft paper`.
* alec matcaps: `Mild Steel`/`Carbon Steel` only. (`Metal`/`Alloy` are two redundant umbrellas but
  distinct vocabulary nodes; not counted.)
* steelcraft industries: `Education`/`Education Construction`/`Educational Facilities`;
  `Healthcare`/`Healthcare Institutions`/`Healthcare and Medical Devices`;
  `Institutional`/`Institutional Buildings`/`Institutional Construction`;
  `Correctional Facilities`/`Correctional Facilities Construction`;
  `Storm Shelter Construction`/`Storm-Resistant Construction`; `Restaurants`/`Food Service`;
  `Building Construction`/`Construction and Infrastructure`; `Architecture`/`Architectural Design`.
* alec industries: `Automation Equipment`/`Industrial Automation`; `Medical Devices`/`Healthcare and
  Medical Devices`; `Electronics`/`Consumer Electronics` (co-emitted for the *same* client);
  `Packaging`/`Packaging Machinery`; `Industrial Machinery and Equipment`/`International Manufacturing`.

Nearly all of the industries duplication is manufactured by the OOV stage: it repeatedly re-words a
concept the in-vocab stage already tagged and asserts `OGR-N1/satisfied` on the reworded string.

## 6. Twin disagreements (same group_id, two chunks)

| dump | groups in both chunks | disagreeing | of which substantive (both chunks had mentions) |
|---|---|---|---|
| steelcraft matcaps | 23 | 8 | 4 |
| alec matcaps | 11 | 3 | 1 |
| steelcraft industries | 11 | 7 | 7 |
| alec industries | 17 | 9 | 5 |

Substantive ones, verbatim:

* `g2n7t6j4` (steelcraft ind): c0 **declined** ("refer to defense against weather and corrosion, not
  the Defense industry"); c1 **emitted `Defense`**.
* `grxyx2pu` (alec ind): c0 → `Industrial Machinery and Equipment`; c1 **declined** ("'CNC machining'
  is a process, not an industry, market, or sector"). The two chunks reach opposite verdicts on the
  single most consequential judgement in the field.
* `gjjkomsh` (alec ind): c0 → `Industrial Machinery and Equipment`; c1 → OOV `Industrial Automation`.
* `gqc7n8x2` (alec ind): c0 → `Packaging` (own shipping cartons); c1 → `Medical Packaging`.
* `g08pf7po` (alec ind): `Electronics` vs `Consumer Electronics`. `gaqs60zp`: `Medical Devices` vs
  `Surgical Instruments`.
* `gejskp3y`/`gbnktffb` (alec ind, c1) **pass** `Food Packaging`+`Personal Care Products` on exactly
  the evidence that made c0's `g750pce1` **fail** them.
* steelcraft ind: `gkupwy1b` `Food and Beverage`+`Restaurants` vs `Commercial Construction`+`Food
  Service`; `ge7rrlz5` `Commercial Construction`+`Healthcare` vs `Healthcare and Medical Devices`;
  `g2pjgtvu` `Commercial Construction`+`Education` vs `Primary and Secondary Education`;
  `gp3z6cum`/`gnx0rrto` `Industrial Construction` vs `Industrial Machinery and Equipment`;
  `gsnhjyee` `Institutional Construction` vs `Institutional Buildings`.
* steelcraft matcaps: `gudf1k9m` casing flip; `gzhgrai9` `Paper honeycomb` vs `Honeycomb`;
  `g793uv47` `Ceramic`+`Glass` (both pass) vs `Ceramic` (fails)+`Ceramic glass` (passes).
* alec matcaps: `glqxl4dh` c0 declined ("offers a document (case study) about titanium alloy component
  machining … no further dealing"), c1 grounded `Titanium Alloy`.

## 7. Ten most consequential errors

1. **alec `industries`, 16 instances** — the shop's own services become an industry it serves.
   `grxyx2pu`: *"'CNC Machining' is not an option, but it is a key process within 'Industrial Machinery
   and Equipment', which generalizes the activity."* SCR-2 then ratifies it: *"showing Alec Model itself
   operates in the industrial machinery and equipment sector."* Same for `production`, `prototyping`,
   `engineering`, `machine shop`, `precision machining`.
2. **alec `industries`, 10 failed verdicts** — every real customer sector reached through "a client in
   sector X approached us" is killed. `gk4srhwq`: *"The synthesis shows Alec Model had dealings with an
   electronics manufacturer as a client, but does not state Alec Model itself serves, supplies, or
   operates in the electronics sector."* Also `gtd40bfo` (Rail Equipment), `gegssrji` (Surgical
   Instruments), `go32ceas` (Rail Technology), `gcb3tyfp` (Automotive), `grpeucy4` (UAS),
   `gqaot2mw` (Surgical Robotics). SCR-2 asks "is the subject in this sector?" when the field asks
   "does the subject serve it?" — the two halves of §7.1 and §7.2 are the *same* inverted rule.
3. **steelcraft `industries` `gquhdebd`** — focal form "the United States"; the in-vocab pass correctly
   declines it as geography, and the OOV pass emits **`Manufacturing`**, which passes: *"Steelcraft
   itself is the manufacturer, as stated in 'Steelcraft manufactures hollow metal doors and frames in
   the United States.'"*
4. **steelcraft `industries` `ggd507mb`** — the company's **1933 founding** as a distributor yields
   `Construction and Infrastructure` **and** `Building Products Distribution`, both passing, with
   SCR-G1 silent: *"Steelcraft itself is described as a building products distribution company at its
   founding."*
5. **steelcraft `industries` `g2n7t6j4` c1** — `Defense` grounded on *"tough defense against harsh
   weather and corrosion"*, with the rule text conceding the error and emitting regardless: *"This
   evidences the concept of defense as a function, but not as the Defense industry. However, since
   'Defense' is an option … it can be generalized to the Defense sector."* Screening caught it; the
   twin chunk had declined it outright.
6. **steelcraft `matcaps` `g8rspbxx`** — `Wood` **passes** for a hollow-metal door maker, off the
   customer's wall framing: *"SCR-2/satisfied: The synthesis attributes the provision of frames designed
   for installation with wood stud anchors to Steelcraft."* The one surviving `P` in the whole
   material set, and the one `I` in that dump.
7. **steelcraft `matcaps` `gpk94tty` c0** — `Lead` emitted with `IGR-E1/failed` in its own list (§4).
8. **steelcraft `industries` `gkyezhd5`** — `Data Centers` **failed** (*"Steelcraft only identifies data
   centers as a use case … does not serve, supply, or operate in the data center sector"*) while
   `hotels`, `schools`, `hospitals`, `restaurants`, `detention centers` from the **identical** "Typical
   applications" bullet lists all passed. Same for `g677l1sa` `Architecture and Design Services` (fail)
   vs `g2kp8p3a` `Architecture` (pass) on the same literature sentence.
9. **steelcraft `matcaps` `gzhgrai9`** — `Paper honeycomb` invented for an unspecified "honeycomb cell"
   by importing another record: *"in the context of door construction, 'honeycomb cell' typically refers
   to a paper-based (often kraft paper) honeycomb core. This is further supported by the similar 'kraft
   honeycomb' in another record."* Cross-record leakage inside a per-record grounding rule.
10. **steelcraft `industries`, 4 instances** — `Industrial Machinery and Equipment` stands in for
    *industrial buildings*: `ga6oezx4`, `gnx0rrto`, `gp3z6cum`, `ghhsw862` (*"'Industrial Machinery and
    Equipment' generalizes industrial buildings"*), all passing, all producing a machinery-sector claim
    for a door manufacturer. The twin chunk emits `Industrial Construction` for the same phrase.

## 8. Unexpected patterns the taxonomy does not name

1. **SCR-2 is the wrong test for `industries` (29 instances).** It asks whether the activity is
   attributable to the manufacturer itself. For a *served-industries* field that rewards own-activity
   tags (25 in alec, 3 in steelcraft, +1 party case in steelcraft matcaps) and, in negative form, kills
   customer sectors (10 wrong FAILs in alec). Nothing else in this run comes close to this in impact.
2. **Case-study-as-mere-document (6 rows / 12 declines, alec matcaps).** The subject's own case study
   is read as a publication rather than as work performed. `gq3repg6`: *"Alec Model offers a document
   (the case study) about the High-Precision Stainless Steel Valve Body for Semiconductor Equipment,
   but do not show any further dealing with the valve body itself beyond the document."* Also
   `g0q4zt9t`, `glqxl4dh`, `g34c7ffu`, `grk15wlx`, `gj3ggbex`. In `glqxl4dh` the twin chunk grounds it.
   The defect originates in the **synthesis**, which writes its own "does not show own dealing"
   conclusion; grounding is faithful to a bad record.
3. **OOV declines *because the term is not in the vocabulary* (2).** Self-defeating for the stage whose
   whole job is out-of-vocabulary capture. `g4rno1vt`: *"'Paint' is a mixture or coating, but the
   vocabulary does not include 'Paint' as a material … No new material can be identified."* `g6o40t3d`:
   *"The vocabulary does not include 'gypsum' or 'drywall' as a material."*
4. **OOV emits an in-vocabulary term while `OGR-N1` reports satisfied (2).** alec matcaps `gj6o4dlw`
   `Titanium` — *"The vocabulary lists 'Titanium Alloy' … but not 'Titanium' as a pure metal"*, yet
   `Metal > Pure Metal > Non-Ferrous Metal > Titanium` exists and the in-vocab stage picked it exactly
   on another row. steelcraft matcaps `gevtj19j` `Concrete` — *"Concrete is present in the vocabulary,
   but not in the already-identified results for this record, so it is a new identification"*, which
   reads the novelty rule as record-local rather than vocabulary-local.
5. **Grounding on negated or comparative evidence (8, all steelcraft matcaps).** `Wood` ×4 (from "no
   wood door … has passed FEMA", from GRAINTECH *simulating* ash/walnut), `Metal` ×2, `Plastic`
   (from "compared to traditional vinyl separators"), `Fiber Glass` (from "steel outperforms
   fiberglass"). **Screening caught 8 of 8** — this is the one place the party/substance rules
   demonstrably work.
6. **`IGR-M1` ("exact match") claimed on non-matches (6, steelcraft industries).** `coastal regions`,
   `Building Envelopes`, `industry standards`, `the industry`, `commercial quality`, `Distributor over
   the counter sales` all record *"The option 'Commercial Construction' exactly matches"*. The M1/M2
   distinction (exact vs. generalisation) carries no information in this dump, so nothing downstream
   can use it to rank confidence.
7. **`dropped_options` swallowing correct out-of-vocab proposals (8).** steelcraft matcaps 5
   (`Polystyrene` ×4, `Fiberglass` ×1), alec matcaps 2 (`Magnesium`, `Zinc`), alec industries 1
   (`Industrial Automation`). Seven were recovered by the OOV pass in the same row; the exception is
   `gk69uwdk`, where `Fiberglass` was dropped and the OOV pass then declined it as already-in-vocab —
   the proposal is lost between two stages, each behaving correctly on its own terms.
8. **The most frequent tag in each `industries` dump is the least informative.** `Commercial
   Construction` is 42 of 105 passing steelcraft instances (40%); `Industrial Machinery and Equipment`
   is 18 of 93 alec instances (19%), 16 of them own-activity. A single generic node absorbs the field.
9. **Zero `C` (circular) and zero `G` (generic) rule outcomes in 393 verdicts.** The screening
   explanations are always specific to their row; the failure mode is not laziness, it is repeating the
   grounding stage's inference (127 `M` on SCR-1, 98 on SCR-2). Screening as built is a *restatement*
   pass, not an independent check.
10. **`Lead` calibration confirmed but not as described.** On alecmfg it was declined at both stages in
    both chunks (4 correct declines). On steelcraft it was **emitted twice** and killed by screening
    both times — once with a failed evidence rule (§4), once legitimately (frame lead-lining clips,
    where the lead lining itself is referred to Republic Doors).

## 9. Curator-keep estimate

| dump | passing instances kept | passing distinct tags kept | note |
|---|---|---|---|
| steelcraft matcaps | 68 / 78 (**87%**) | ~19 / 26 (**73%**) | drop `Chemicals`, `Polymer`, `Honeycomb`, `Wood`, `Stainless Steel Stiffener`, 2 casing dups |
| alec matcaps | 54 / 63 (**86%**) | ~29 / 33 (**88%**) | drop `Metal`, `Alloy`, `Plastic` (redundant umbrellas), `Foam` (shipping) |
| steelcraft industries | 83 / 105 (**79%**) | ~17 / 40 (**43%**) | after collapsing 19 near-dups into 8 concepts and dropping 6 own-activity + 3 pseudo-sectors |
| alec industries | 58 / 93 (**62%**) | ~22 / 37 (**59%**) | 21 own-activity tags dropped outright; +10 real sectors must be *restored* from wrongly-failed verdicts |

`material_caps` is close to shippable on both subjects — the residue is genericity and casing, not
falsehood. `industries` is not: on alecmfg the field currently reports the shop's own processes as
customers and rejects a third of its actual customers, and the two failures share one root cause, the
SCR-2 self-attribution test.
