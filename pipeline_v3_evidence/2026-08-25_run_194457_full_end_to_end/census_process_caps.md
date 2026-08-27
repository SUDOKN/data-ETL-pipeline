# Behavioural census — `process_caps`, run 20260825T194457

Dumps: `packages/logs/extraction_dumps/20260825T194457/steelcraft_com__process_caps.json`,
`.../alecmfg_com__process_caps.json`.
Vocabulary: `apps/data_etl_app/src/data_etl_app/knowledge/ontology/process_caps.json` (673 labels incl. altLabels;
**no** `Inspection`, `Testing`, `Welding`, `Cutting`, `Manufacturing`, `Prototyping` concept).

**Coverage: complete.** Every one of the 396 grounded tag instances (153 steelcraft + 243 alecmfg), all 493 declines
(213 + 280) and all 396 screening verdicts was read and coded by hand. Nothing sampled, nothing extrapolated,
no regex/keyword assignment.

## Method

A script enumerated each tag instance with its full record synthesis, every grounding rule (`IGR-*`/`OGR-*`) and its
screening verdict into compact text; each item was then read and coded individually, in batches of ~20, with a
running ledger. Codes follow the shared taxonomy. Two clarifications I applied consistently:

* **D vs N vs B.** `D` when the tag label (or a direct morphological variant: "beveled edges" → *Edge Beveling*,
  "tack welds" → *Tack Welding*) is stated in the synthesis; `N` when the tag is a vocabulary synonym/parent that
  requires a mapping step ("3D printing" → *Additive Manufacturing*, "welding" → *Thermal Welding*); `B` when an
  inferential step not present in the synthesis is required ("compliance with a standard that covers finishing" →
  *Surface Finishing*).
* **P sub-buckets.** `LAB` = certification/standards testing (UL, Intertek, TAS, ASTM, ANSI/SDI, ICC, ASTM C1363
  hot-box). Paint-line QC that a coating shop plausibly runs in-house (salt spray, humidity, crosshatch adhesion,
  impact) was **not** counted as LAB. `INSTALL` = work at the building site (field assembly, anchoring the frame to
  the wall, field hinge conversion). `SUPPLIER` = done by an upstream vendor (stainless mill finishes, outsourced
  anodising). `DISTRIBUTOR` = the record hedges "Steelcraft **or its distributor's** fabrication shop".
  `CUSTOMER` (added; alecmfg only) = the process is performed by alecmfg's client, not by alecmfg.

Decline codes were assigned by this precedence, stated so the numbers are reproducible:
dedupe ("already identified/covered") → `OK`; else group grounded in the other chunk → `TWIN`; else definitional
misfire on an operation-bearing phrase → `TEMPLATE`; else correct out-of-field call (installation/design/business)
→ `SCOPE`; else a real in-scope process was dropped → `LOST`; else `OK`.

---

## 1. Grounded tag instances — code distribution

### steelcraft.com (153 instances, 138 passed screening)

| code | all | passed | in-vocab stage | OOV stage |
|---|---:|---:|---:|---:|
| D direct | 53 | 53 | 15 | 38 |
| N normalized | 22 | 16 | 22 | 0 |
| B bridge | 24 | 23 | 9 | 15 |
| V vague | 5 | 5 | 3 | 2 |
| X axis | 10 | 10 | 1 | 9 |
| **P party** | **38** | **31** | **16** | **22** |
| F fabrication | 1 | 0 | 0 | 1 |
| **total** | **153** | **138** | **66** | **87** |

### alecmfg.com (243 instances, 231 passed screening)

| code | all | passed | in-vocab stage | OOV stage |
|---|---:|---:|---:|---:|
| D direct | 117 | 117 | 86 | 31 |
| N normalized | 28 | 28 | 28 | 0 |
| B bridge | 14 | 14 | 11 | 3 |
| V vague | 20 | 20 | 5 | 15 |
| X axis | 46 | 42 | 0 | 46 |
| **P party** | **10** | **7** | **4** | **6** |
| F fabrication | 8 | 3 | 8 | 0 |
| **total** | **243** | **231** | **142** | **101** |

The two dumps fail in opposite ways. Steelcraft's damage is **wrong-actor** (25% of instances); alecmfg's is
**wrong-axis** (19%) plus **vague** (8%), almost entirely produced by the OOV stage.

## 2. P (wrong actor) — exact sub-buckets

| bucket | steelcraft (all / passed) | alecmfg (all / passed) |
|---|---:|---:|
| LAB (certification body) | 13 / 13 | 0 / 0 |
| INSTALL (building site) | 12 / 11 | 0 / 0 |
| DISTRIBUTOR | 11 / 5 | 0 / 0 |
| SUPPLIER | 2 / 2 | 1 / 1 |
| CUSTOMER (client performs it) | 0 / 0 | 9 / 6 |
| **total** | **38 / 31** | **10 / 7** |

**The prior "~37% of steelcraft's process tags are lab-or-installation work" is roughly 2× too high.**
Exact: LAB+INSTALL = **25 / 153 = 16.3%** of all instances, **24 / 138 = 17.4%** of instances that survived
screening. All four P buckets together are 24.8% / 22.5%. On distinct surviving labels, LAB+INSTALL is 21 of 86
(24%). The 37% figure is only reachable if distributor-hedged and supplier tags are folded in *and* the count is
taken over OOV-only tags.

LAB tags (13): *Severe windstorm resistance testing, Missile Impact Testing ×2, Uniform static air pressure
testing ×2, Cyclic Wind Pressure Testing ×2, Neutral pressure fire testing, Positive pressure fire testing,
Physical endurance testing, Thermal performance testing, Product Testing, Assembly Testing*.
INSTALL (12): *Tornado Door Installation (screened out), Compression Anchor Installation, Tab/Lock Corner Assembly,
Corner Clip Installation ×2, Field Assembly, Mechanical Joining ×3 (KD field assembly, EMA masonry anchors, sill
attachment), Lead lining integration, Field modification ×2*.
DISTRIBUTOR (11): the three `FRAME ELEVATIONS` sentences, 5 of which passed screening and 6 of which failed.

## 3. OOV vs in-vocab split

| | steelcraft | alecmfg |
|---|---|---|
| in-vocab instances | 66 (43%) | 142 (58%) |
| OOV instances | 87 (57%) | 101 (42%) |
| OOV distinct labels | 71 | 96 |
| OOV singleton labels | 60 | 92 |

**alecmfg singleton-OOV curator-discard: exactly 61 of 92 = 66%** (X 42, V 13, P-CUSTOMER 6), not the estimated
75–80%. The named examples are confirmed discards (`Shipment` X, `CMM Reporting` X, `Photographic Documentation` X).
The remaining 31 singletons are real, usable processes (`Micro-blasting`, `Helicoil installation`, `laser
finishing`, `First Article Inspection`, `optical emission spectrometry (OES)`, `Probing`, `cleaning`…).
If you additionally discard singletons whose own `OGR-N1` admits an already-identified parent (redundant
refinements such as `Progressive Drilling` under `Drilling`), the figure rises into the 75–80% band — that is
probably what the earlier estimate was measuring: **33 of 101 alecmfg OOV tags (22 of 87 for steelcraft) carry an
`OGR-N1` explanation that names an already-identified parent and were emitted anyway.**

Steelcraft's singleton OOV is far healthier: 17 of 60 discard (28%) — its OOV stage is doing genuine work
(edge beveling, stitch welding, countersinking, silencer preparation), and its failure mode is the actor, not the axis.

## 4. Screening

Mechanical rule outcomes recorded in the dumps:

| | steelcraft | alecmfg |
|---|---|---|
| SCR-1 satisfied / failed | 146 / 7 | 238 / 5 |
| SCR-2 satisfied / failed / not_triggered | 138 / 8 / 7 | 231 / 7 / 5 |
| verdicts passed / failed | 138 / 15 | 231 / 12 |

My per-rule judgement on **passed** verdicts (R real, C circular, G generic, I inverted, M mirror):

| | SCR-1 | SCR-2 |
|---|---|---|
| steelcraft (138) | R 88, C 48, G 2 | R 41, M 55, I 27, G 15 |
| alecmfg (231) | R 199, C 28, G 3, I 1 | R 194, M 26, I 9, G 2 |

`M` (mirror) is the dominant steelcraft SCR-2 pattern: the rule quotes the record's own canned attribution clause
("The dealing shown is Steelcraft's own: …"), which the record-writer emits for every row regardless of actor, so
SCR-2 has no independent evidence at all. **55 of 138 steelcraft SCR-2 passes are mirrors and a further 27 are
inverted** — i.e. 59% of steelcraft's actor checks are not actually checking the actor.

**Failed** verdicts: steelcraft 9 OK / **6 WRONG**; alecmfg 12 OK / 0 WRONG. All 6 wrong failures are the same
mechanism (§7, U5).

## 5. Declines (493)

| code | steelcraft (213) | alecmfg (280) |
|---|---:|---:|
| OK | 157 | 266 |
| TEMPLATE | 13 | 3 |
| TWIN | 22 | 9 |
| SCOPE | 18 | 0 |
| LOST | 3 | 2 |
| stage split | IV 121 / OOV 92 | IV 123 / OOV 157 |

The decline path is in far better shape than the grounding path. Notably *correct*: `rolling`/`facing` idioms
rejected (4 declines), `Grouting WS Strikes` recognised as a document title, the Republic Doors referral for
lead-lined doors, "eliminating the need for continuous profile welding" recognised as an absence, and every
`Zeiss CONTURA G2 CMM`/`Laser Interferometer` equipment row.

LOST (5): `SUA (set-up and welded) for installation as a pre-welded unit` (factory welding read as an installation
option); `light cutouts` (IV pass dropped the decorated `Machining (also: …)` option); `3-sided frame welding`
(dropped `Welding`, never generalised to `Thermal Welding`); alecmfg `welding` ×2 (both stages declined the central
welding row — IV dropped the non-existent `Welding` label, OOV said it was "already present in the vocabulary under
'Joining' and its subtypes" and emitted nothing).

## 6. Twin disagreements

Steelcraft: 24 group_ids appear in both chunks; **6 produce identical tag sets, 18 disagree (75%)**.
alecmfg: 21 in both chunks; 12 identical, **9 disagree (43%)**.

Steelcraft disagreements (chunk 0 → chunk 1):

| group_id | chunk 0 | chunk 1 |
|---|---|---|
| gxensd0m | Joining | — |
| gwjtq9ok | Epoxy filling of mechanical interlock edges | Adhesive Bonding |
| gzynov8n | Mechanical Joining | Hardware preparation |
| g7wr25ls | Painting | Coating |
| ga8154tz | Thermal Welding | Unit Frame Welding |
| g2tkvw2x | Face Welding, Thermal Welding | — |
| g19xvdk9 | Die-Mitering | Die-Mitered Corner Connection |
| g52bewq4 | Continuous Profile Welding | — |
| gbi46g9v | Anchor Preparation | Special Anchor Preparation |
| geywziq8 | Lamination | Adhesive Bonding |
| g5g6hx3k | Compression Anchor Installation | Compression Anchor Preparation |
| gip8ccwz | — | Tab/Lock Forming |
| gly2pdyf | — | Silencer Preparation |
| ga9t0e4v | Anchor Installation Preparation | Adjustable Base Anchor Preparation |
| gnqs5x6u | — | Field modification |
| gxu9w9vr | Fabricating | — |
| god9k1b4 | — | Coating |
| gyakxc89 | — | Hardware preparation |

alecmfg: gwbw12t0 (4 tags → 0), gsawivwk (3 → 0), gcct95un (3 → 1), gbe7vg55 (1 → 0), g63nk11h (1 → 0),
g5s94jt5 (1 → 0), gkab3bnm (Sheet Metal Fabrication → Sheet Metal Processing), go8oeek9 (0 → Fabricating),
g4kvkhr6 (0 → Labeling).

Two shapes: **label drift on the same evidence** (Painting/Coating, Lamination/Adhesive Bonding,
Die-Mitering/Die-Mitered Corner Connection, Sheet Metal Fabrication/Sheet Metal Processing — 8 pairs) and
**presence/absence** (a chunk yields nothing for a group the other chunk tagged — 10 steelcraft, 6 alecmfg).

## 7. The ten most consequential errors (verbatim)

1. **alecmfg `gx0r03vp` — homonym: engineering drawings tagged as the metal-forming process `Drawing`.**
   Record: *"Alec Model conducted drawing review and communication as part of its optimization measures … a lead
   time of 20 working days from drawing approval to dispatch."*
   `IGR-M1/chosen`: *"'Drawing' as a process is directly referenced and matches the option."* Passed screening.
   `Drawing` is L3 under *Forming* (Bar/Cold/Deep/Tube/Wire Drawing). Steelcraft's identical `drawing` row
   (`gx0r03vp`, same group id) correctly declined — the same word, opposite outcome, in the same run.

2. **alecmfg `gsqiy4oj` — idiom: "cutting-edge machines" grounds `Precision Cutting`.**
   `SCR-1/satisfied`: *"The synthesis describes Alec Model's CNC factory being 'equipped with nine cutting-edge
   machines' and discusses the technical challenge of cutting titanium alloy, which is substantive activity in
   precision cutting."* Passed. (The same dump correctly rejects `rolling` as *"only used in idiomatic
   expressions"*.)

3. **alecmfg `gsiaalak` — `Surface Preparation` from *material testing*.**
   `IGR-M2/chosen`: *"'Material testing' is not a specific process in the options, but 'Surface Preparation'
   generalizes the described activities."* Passed. The sibling row `goovx13d` does the same for *material prep*.
   `Surface Preparation` is functioning as a catch-all sink for anything containing "material" or "prep".

4. **alecmfg `g08pdmj1` — a tag whose own evidence rule failed was still emitted.**
   `IGR-E1/failed`: *"The focal form is 'specialty anodizing coordination' … There is no mention of urethane
   casting."* The instance `Urethane Casting` is nevertheless present in `in_vocab_grounding.tags`; only screening
   removed it. Same shape at `g0v71p5s` (`Polishing`, where the record says polishing *was not permitted*), and at
   steelcraft `gu8yq7hw`/`gsjmyp84` where `OGR-N1/failed` ("already identified") did not suppress the OOV tag —
   and there screening **passed** it.

5. **alecmfg `g08pdmj1` — outsourced anodising attributed to the subject.**
   Record: *"Alec Model coordinated specialty anodizing and performed insert installation."*
   `SCR-2/satisfied`: *"The synthesis states Alec Model coordinated the anodizing, indicating Alec Model's own
   involvement in arranging or managing the process for its client."* "Arranging" is the definition of the wrong
   actor; SCR-2 says so and passes anyway.

6. **steelcraft `gi269rxj` — Intertek's testing credited to Steelcraft.**
   Record: *"the frames are tested and Intertek labeled, showing compliance with ICC500-2014 and FEMA 320/361."*
   `SCR-2/satisfied`: *"The synthesis attributes the testing and labeling to Steelcraft's own frames, indicating
   Steelcraft is responsible for ensuring the testing occurs."* Tag `Product Testing` kept. Ownership of the
   product is being read as performance of the test; this single argument shape produces all 13 LAB tags.

7. **steelcraft `g9acvx6i` — `Field Assembly` kept as a production capability.**
   `OGR-K1/satisfied`: *"Field assembly is a manufacturing/assembly process involving the assembly of components at
   the installation site."* `SCR-2/satisfied`: *"'Steelcraft manufactures frames that can be assembled in the
   field,' directly attributing the process to Steelcraft."* The same dump's `goslfq8s` decline states the correct
   rule: *"installation itself is not a manufacturing process performed on the work by the manufacturer; it is a
   field operation by the customer or installer."*

8. **steelcraft `gk7atzku` vs `gxyzr92k`/`gvxwiivi` — the identical sentence passes and fails.**
   One sentence ("Steelcraft **or its distributor's** fabrication shop cuts components to length, notches and/or
   miters them, assembles and welds them…") produced 11 tags across four groups.
   `gk7atzku` SCR-2/satisfied: *"Steelcraft is one of the parties that may perform this: 'Steelcraft or its
   distributor's fabrication shop cuts components to length.'"* (3 tags kept)
   `gxyzr92k` SCR-2/failed: *"the process is attributed to 'Steelcraft or its distributor's fabrication shop,' so
   it is not confirmed that Steelcraft itself performs notching."* (3 tags dropped)
   Net: 5 kept, 6 dropped, on byte-identical evidence.

9. **steelcraft `g04y5dft` / `gxfzf496` — the steel mill's finish tagged as Steelcraft's process.**
   *"'#2B - Smooth, rolled mill finish' … described as smooth and unpolished"* → `Surface Finishing`;
   *"'#4 Brushed Satin - Polished with one directional grain'"* → `Polishing`, `IGR-M1/chosen`: *"Polishing with a
   directional grain is exactly matched by the option 'Polishing'."* Both are mill finishes on purchased stainless
   sheet.

10. **steelcraft `g52bewq4` — a process the record says is *eliminated* is grounded.**
    Record: *"the miter includes four corner tabs designed with concealed connection, eliminating the need for
    continuous profile welding."*
    `OGR-E1/satisfied`: *"the synthesis states that frames are designed to eliminate the need for this process,
    indicating it is a recognized manufacturing process. The process itself is evidenced as something that could be
    performed."* Screening caught it (`SCR-2/failed`), but the grounding stage explicitly reasoned from an absence.

Runner-up worth one line: steelcraft `gp00fhdu` declines `factory countersunk holes` because *"the options do not
list countersinking or hole making as a standalone process"* — `Hole Making` **is** in the vocabulary and was
chosen two rows earlier (`gxu7fdzo`).

## 8. Unexpected behaviours the taxonomy does not name

| # | pattern | count | example |
|---|---|---|---|
| U1 | Homonym / idiom capture in grounding | 2 (both alecmfg) | `Drawing` from "drawing review"; `Precision Cutting` from "cutting-edge machines". The *decline* path rejects idioms correctly 4× (`rolling` ×2, `facing` ×2) — the two paths disagree about the same phenomenon. |
| U2 | Tag emitted although its own grounding rule **failed** | 4 | alec `IGR-E1/failed` ×2 (both later screened out); steel `OGR-N1/failed` ×2 (both **passed** screening: `Factory assembly of jamb and head components`, `Finish painting of doors`). |
| U3 | Record disclaims attribution, tag emitted anyway | 4 | *"not necessarily Alec Model's own activity, but it implies…"* (`g992y12e`, kept); *"the entries do not show Alec Model's dealing with the Laser Interferometer beyond mentioning it"* (`gyzdckky`); *"does not specify Alec Model's direct dealing with this treatment"* (`gnt6ehxi`); *"likely provision of this manufacturing process"* (`grhljfj5`, kept). |
| U4 | Hallucinated vocabulary citation | 2 | steel `gpc22jbj` `OGR-N1`: *"The vocabulary includes 'Inspection, testing, and measurement' as a general category"* — it does not (0 of 673 labels). The `gxtop8gn` decline asserts the same non-existent category in the negative. |
| U5 | Screening literalism kills correct vocabulary generalisations | 6 wrong failures | `SCR-1/failed`: *"The synthesis does not mention or describe thermal welding; it only discusses tack welding and stitch welding."* `Thermal Welding` is chosen 18× in steelcraft: 11 pass, 5 fail wrongly (2 more fail correctly) on the same kind of evidence; `Mechanical Joining` from factory-installed glass light kits fails the same way. |
| U6 | Documentation deliverables harvested as capabilities (alecmfg) | 11 instances | `CMM Reporting`, `Dimensional Reporting`, `Inspection Reporting` ×2, `manufacturing reporting`, `packaging documentation`, `Photographic Documentation`, `Document Control`, `batch traceability documentation`, `DFM Reporting`, `Traceability Implementation`. |
| U7 | Project-timeline / business-phase rows harvested (alecmfg) | 8 | `Prototype Manufacturing`, `Pilot Batch Production`, `Production Ramp-Up`, `DFM optimization` (one 4-row *Project Timeline* table produced all four), plus `Production Capacity Expansion`, `Production Scheduling`, `Shipment`, `full-cycle manufacturing execution`. |
| U8 | CAM / workholding / parameter techniques harvested (alecmfg) | 21 | `multi-axis interpolation`, `TCP control`, `Specialized Fixturing`, `Vacuum Fixturing` ×2, `precision fixturing`, `Precision 5-axis workholding`, `Toolpath Generation`, `Optimized Toolpath Programming`, `CAM programming`, `toolpath design`, `Programming (Manufacturing)`, `Tool Setup`, `cutting parameter optimization`, `Process Parameter Optimization`, `Process Parameter Control`, `bending sequence optimization`, `structural optimization`, `Process Design`, `Production Process Optimization`, `Internal Tolerance Control`. The IV stage declines these correctly ("a machining technique or method, but not a distinct manufacturing process"); the OOV stage grounds them. |
| U9 | Same-row stage contradiction: IV declines on a definitional argument, OOV grounds the same phrase | 13 steel + 3 alec (the TEMPLATE declines) | `g0804bhv`: IV *"It refers to the result of manufacturing, not the process itself"* → OOV emits `Edge Beveling`. |
| U10 | Near-duplicate / case-collision labels in the final set | ≥14 | steel `Hardware Preparation` vs `Hardware preparation` (3 rows), `Anchor Preparation` vs `Special Anchor Preparation` vs `Stud Anchor Preparation` vs `Compression Anchor Preparation` vs `Adjustable Base Anchor Preparation` vs `Adjustable Base Anchor Installation` vs `Anchor Installation Preparation` (7 labels, one concept); alec `Prototyping`/`Rapid prototyping`/`rapid prototyping (non-additive)`/`Prototype Build`/`Thermal Component Prototyping`, `CMM Inspection` ×3. |
| U11 | Parenthetical-disambiguator tag names | 3 | `Probing (for measurement and alignment)`, `rapid prototyping (non-additive)`, `Laser Interferometry (for inspection/validation)`, `Programming (Manufacturing)`, `Manufacturing (General)` — the model is annotating the label because the axis is unclear to it. |
| U12 | `dropped_options` beyond the known 14 | 36 rows total | steelcraft 14 (`Testing` ×9, `Machining (also: …)` ×2, `Welding` ×2, `Extruding (also: Extrusion)` ×1 — the 3 known false drops are all here, both `Machining` and the `Extruding` one). **alecmfg adds 22 more rows** (`Inspection` ×15, plus `Prototype Manufacturing`, `Welding`, `Testing`, `Cutting`, `Manufacturing`, `Prototyping`, `Design for Manufacturing`) — all genuine vocabulary gaps, no false drops, but 22 rows is beyond the known 14 and should be re-checked against that note. |
| U13 | Vocabulary-gap declines concentrate on one axis | 24 declines | Every `Testing`/`Inspection` drop is the missing generic concept. It costs steelcraft nothing real (the tests are lab work anyway) but costs alecmfg genuine capabilities that only survive through the OOV stage. |

## 9. Curator-keep estimate

Keepable = `D`+`N`+`B` (a real process the subject performs, however reached). Discard = `F`+`X`+`P`+`V`.

| | steelcraft | alecmfg |
|---|---|---|
| instances surviving screening | 138 | 231 |
| keepable instances | 92 (67%) | 159 (69%) |
| **distinct labels surviving** | **86** | **127** |
| **keepable distinct labels** | **60 (70%)** | **66 (52%)** |
| discard distinct labels | 26 (30%) | 61 (48%) |

alecmfg looks better per instance and worse per label because its keeps are a handful of high-frequency in-vocab
tags (`CNC Machining` 11×, `Anodizing` 7×, `Five Axis Machining` 7×, `Packing` 6×) while nearly every discard is a
singleton OOV label. A curator working the *label* list — which is what reaches the KG — would throw away roughly
**a third of steelcraft's and half of alecmfg's** surviving process capabilities. For steelcraft the thrown-away
third is overwhelmingly other people's work (lab, installer, distributor, mill); for alecmfg it is documents,
project phases, CAM settings and fixtures.
