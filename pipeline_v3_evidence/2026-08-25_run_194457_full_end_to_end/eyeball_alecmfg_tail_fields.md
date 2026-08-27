# Eyeball review: alecmfg.com tail fields (conformity_attestations, industries, process_caps, material_caps)

Run `20260825T194457` — the first fully completed end-to-end v3 run. These four fields had
never been reviewed for this subject (the previous run crashed at conformity_attestations).
Dumps: `packages/logs/extraction_dumps/20260825T194457/alecmfg_com__{conformity_attestations,industries,process_caps,material_caps}.json`.

Subject ground truth: alecmfg.com (Alec Model) is a Chinese contract precision-machining shop
(CNC milling/turning, 5-axis, sheet metal, prototyping) serving aerospace, semiconductor,
medical, automotive/EV, rail, automation clients, machining aluminum, titanium, stainless,
copper, brass, plastics.

## Counting note

"Final tags" below = distinct tag strings with `screening.passed: true` on any row. My counts
run slightly above the caller's headline numbers (industries 37 vs 35, process 127 vs 123,
grounded groups 70/184/57 vs 67/179/55) because groups duplicated across the two chunks can
carry different verdicts per chunk; the downstream merge evidently reconciles a few. Conformity
(44→12→10) and materials (33 tags) match exactly. Row-level status counts:

| field | rows | distinct groups | grounded | no_candidates | screened_out | no_mentions |
|---|---|---|---|---|---|---|
| conformity_attestations | 44 | 44 | 12 | 14 | 14 | 4 |
| industries | 127 | 110 | 80 | 15 | 15 | 17 |
| process_caps | 271 | 250 | 196 | 51 | 8 | 16 |
| material_caps | 92 | 81 | 62 | 14 | 1 | 15 |

---

## 1. conformity_attestations — 10 final tags

### The 10 tags, with screening explanations and verdicts

**1. IATF 16949** (in-vocab; rows `gqsg6v12` "IATF 16949:2016 Certification", `gi1mz7ar` "IATF 16949:2016") — **REAL, correct.**
> SCR-1: "The synthesis states Alec Model 'has officially achieved IATF 16949:2016 Certification' and offers a downloadable copy of its certification. This is a substantive statement about holding the certification."
> SCR-2: "The certification is attributed directly to Alec Model, and a downloadable copy is offered as evidence."

**2. ISO 13485** (in-vocab; `g2arruit` "ISO 13485", `gfu8hpda` "ISO 13485:2016") — **REAL, correct.**
> SCR-1: "The synthesis states: 'Alec Model has obtained ISO 13485 certification, as stated in multiple locations including site navigation, news, and case study pages.' ... This is substantive information about the certification and Alec Model's activity regarding it."
> SCR-2: "The synthesis explicitly attributes the certification to Alec Model: 'The manufacturer's own dealing is the acquisition of ISO 13485 certification for its quality management systems.'"

**3. ISO 14001** (in-vocab; `gbnfzc0c`, `gbdqg9sd` "ISO 14001:2015") — **REAL, correct.**
> SCR-1: "The synthesis states: 'Alec Model has obtained ISO 14001 certification, as stated in site navigation, news, and company news listings.'"
> SCR-2: "The synthesis says: 'The manufacturer's own dealing is the acquisition of ISO 14001 certification.' This shows Alec Model itself holds the certification."

**4. REACH compliance** (OOV; `g3ld8yip` "RoHS, REACH, and ISO 10993-5 compliant") — **defensible.** Case-study claim about the shop's own finishing:
> SCR-1: "The synthesis states Alec Model demonstrates 'medical-grade finishing that is RoHS, REACH, and ISO 10993-5 compliant.' This is a substantive claim of compliance."
> SCR-2: "The compliance is attributed to Alec Model's own finishing, not to a third party."

**5. ISO 10993-5 compliance** (OOV; same row `g3ld8yip`) — **defensible**, same evidence and rules as #4.

**6. ASTM B221 material standard** (OOV; `gfo1ehdm` "ASTM B221") — **borderline.** "The entry shows that Alec Model provides material certification per ASTM B221 for 6061-T651 aluminum" — supplying mill certs to a spec is a real attestation practice, but ASTM B221 is a *material* spec, not a certification the shop holds.
> SCR-2: "Alec Model is the party providing the material certification per ASTM B221."

**7. ASTM B221** (OOV; `gre2hbvl` "6061-T6 aluminum (ASTM B221, extrusion stock)") — **wrong, and a duplicate of #6.** Evidence is only "Alec Model uses 6061-T6 aluminum (ASTM B221, extrusion stock)" — a material-capability fact, not an attestation. Two final tags ("ASTM B221" and "ASTM B221 material standard") name the same standard: OOV minting has no cross-row dedup, and `gre2hbvl`'s OGR-N1 claims "'ASTM B221' is not covered by ... already-identified results" even though `gfo1ehdm` had minted it (the "already identified" context is per-row only).

**8. ASTM A36 material standard** (OOV; `g0pakh0c`) — **wrong axis.** "Alec Model uses ASTM A36 plate in its fabrication" = a material capability (and it IS also tagged Carbon Steel in material_caps), not a conformity attestation.

**9. Class 100,000 cleanroom standard compliance** (OOV; `gryanocs`) — **weak-positive.** "all packaging was completed in a cleanroom rated at Class 100,000 or better" for one client project; an operational fact, marginal as an attestation.

**10. D-U-N-S Number registration** (OOV; `g4s7rmka`) — **marginal.** Real (news article: obtained a D-U-N-S Number 2025-07-01), but the in-vocab pass itself declined it on definitional grounds ("a business identifier, not a certification, accreditation, registration, standard, or compliance") and the OOV pass re-admitted it under OGR-K1 "This fits the meaning of a registration". A D-U-N-S number is not a conformity attestation.

**Verdict on the 10:** 3 solid certifications (IATF 16949, ISO 13485, ISO 14001), 2 defensible
compliance claims (REACH, ISO 10993-5), 5 that a curator would drop or merge (2× ASTM B221,
ASTM A36, D-U-N-S, and arguably Class 100,000). The vocabulary being threadbare pushed 7 of 10
through OOV, and OOV's admission bar (OGR-K1) is generous.

### What OOV recorded across ALL rows (oov_grounding.tags, 13 rows)

EN 10204 3.1 material certification (`g88e76ph`), ISO 7599 AA10 anodizing standard
(`gihnws2r`), ISO 10993-5 compliance (`gi6cowdt`, `g3ld8yip`), RoHS compliance (`ga6sjlqp`,
`glx2w9gi`), REACH compliance (`g3ld8yip`, `glx2w9gi`), CE-MDR documentation compliance
(`gc2q6oxd`), ASTM B221 material standard (`gfo1ehdm`), ASTM B221 (`gre2hbvl`), ASTM A36
material standard (`g0pakh0c`), D-U-N-S Number registration (`g4s7rmka`), ISO 7 cleanroom
standard compliance (`gsj6pe0p`), Class 100,000 cleanroom standard compliance (`gryanocs`).

The RoHS/REACH/ISO 10993-5 dropped_options path (new this run) worked on all 4 rows that hit it
(`gi6cowdt`, `ga6sjlqp`, `g3ld8yip`, `glx2w9gi`): non-vocabulary in-vocab answers landed in
`dropped_options` and the run survived — the exact failure that killed run 20260824T190359.

### Declined/screened-out rows — lost real certifications?

All 14 screened_out rows reviewed; the ones that matter:

- **ISO 9001 — the one likely real loss.** Three rows screened out correctly (all client-side:
  `gpcrxlbl` "aligns its inspection reports with clients' ISO9001 compliance protocols",
  `gd2y03ba` "routines compatible with the client's ISO9001 and CE requirements", `ggj60n3y`
  "meets a client's ISO9001 supplier requirements"). But row `g5zt67xs` "ISO 9001:2015"
  (no_candidates): "ISO 9001:2015 is listed in the 'Certification Statistics' section under the
  heading about Alec Model obtaining ISO 13485 and ISO 14001 certifications." A row header in
  the shop's own certification-statistics table is decent evidence they claim it; both grounding
  passes declined ("Listing a standard in statistics without stating acquisition or compliance
  does not evidence certification"). Conservative-by-design, but a curator would probably credit
  ISO 9001 here.
- **RoHS compliance — lost by row-fate asymmetry.** The same sentence that admitted REACH and
  ISO 10993-5 ("medical-grade finishing that is RoHS, REACH, and ISO 10993-5 compliant",
  `g3ld8yip`) names RoHS, but on that row the OOV pass tagged only REACH + ISO 10993-5 and
  omitted RoHS (no recorded reason). RoHS's own carrying rows (`ga6sjlqp`, `glx2w9gi`) are
  "addresses client requirements" framings that rightly failed SCR-2. Net: REACH in, RoHS out,
  on identical evidence. A tag's fate depends on which row happens to carry it.
- **ASME** (`g79b6qz8` "ASME-certified welders"): SCR-2 failed — "The certification is
  attributed to the welders, not to Alec Model itself." Technically right (personnel cert), but
  these are the shop's own welders fabricating its offshore-skid frames; a curator might keep
  "employs ASME-certified welders". Debatable loss.
- **EN 10204 3.1** (`g88e76ph`): SCR-2 failed — "only shows Alec Model fulfilling client
  requirements for EN 10204 3.1 certification, not that Alec Model itself holds or issues the
  certification." **Inconsistent with #6:** "provides material certification per ASTM B221"
  passed SCR-2 while "fulfills client requirements for EN 10204 3.1 material certification"
  failed it — nearly the same posture, opposite outcomes.
- Correctly dropped: CE / CE-MDR (client requirements), ISO 7 cleanroom (client requirement,
  and consistent with Class 100,000 passing since that row shows the shop's own activity),
  ISO 7599 AA10 (client anodizing spec), "IATF" bare (redundant with IATF 16949),
  Regulatory Approval (supports clients' approval processes; SCR-1 failed).
- no_candidates rows all correct declines: EU Compliance, EU-compliant documentation, EU quality
  standards, conformity statement, CMM inspection, anodizing cert, packaging photos, traceable
  label set, Type II Class 1, EN AW-7475-T7351, CE technical documentation (a customer
  testimonial about the customer's CE work), REACH (`gutmbbfq` — see anomaly below).

No ITAR/AS9100 anywhere in the dump — expected: the shop is Chinese ("described as a Chinese
supplier with full compliance with EU quality standards", `g53bh8zu`); the site apparently
never claims them. The field is capturing the shop's real certification story (IATF/13485/14001
+ compliance claims), minus ISO 9001.

### Anomalies

- **Verdict/rationale contradiction** on `gutmbbfq` "REACH" (oov_grounding.declined): "...the
  prompt only asks for what the vocabulary misses, and REACH is not in the vocabulary, so it
  should be identified. However, since the instructions say to only return what the vocabulary
  misses, and REACH is not in the vocabulary, it qualifies. Correction: REACH should be returned
  as a candidate." — the model argued itself into tagging and still landed as a decline (it
  emitted no candidate). Harmless here (REACH passed via `g3ld8yip`) but the decline text
  contradicts its verdict.
- **Repo ontology snapshot diverges from the live vocabulary.** The in-vocab tag "Regulatory
  Approval" (`g03qqd2d`) appears nowhere in
  `apps/data_etl_app/src/data_etl_app/knowledge/ontology/certificates.json` (58 nodes, version
  `mZgLXCU1...` — same id the dump header pins), yet the run's brute search log says "Brute
  search found 4:{'ASME', 'IATF 16949', 'IATF', 'Regulatory Approval'} concepts in text" — so it
  IS a live concept and the dropped_options guard did not leak. The repo JSON export is stale or
  partial relative to the graph the run used. Worth knowing before using these files as the
  reference vocabulary in future reviews.
- **"Not chosen" units still become tags** (seen in process_caps, same parser): see §3
  anomalies; the wire contract lets a unit whose own rules say "not chosen" through to
  screening.

---

## 2. industries — 37 screening-passed tags (caller headline: 35)

### The full list (tag ← contributing focal_forms)

Solid, correctly evidenced industries-served (**21**): Aerospace (10 rows incl. `g2sicie9`
"aerospace", `gil8hay0` "aerospace CNC machining"), Automotive, Automotive Components,
Automation Equipment, Commercial Vehicles ("Electric Truck Startup", "logistics vehicles"),
Consumer Electronics, Electric Vehicles, Electronics, Healthcare and Medical Devices,
Industrial Automation (OOV), Industrial Electronics, Industrial Robotics, Medical Devices,
Offshore Marine, Offshore Oil and Gas ("Offshore Skid Systems"), Power Electronics, Rail
Equipment, Semiconductor, Semiconductor Equipment, Surgical Instruments, Unmanned Aerial
Systems.

Wrong-axis / junk (**9**): Industrial Machinery and Equipment, Additive Manufacturing,
International Manufacturing, Sheet Metal Fabrication, Packaging, Medical Packaging,
Maintenance Repair and Overhaul (MRO), Marine Equipment, Construction and Infrastructure.

Borderline (**7**): Tool and Die, Thermal Systems, Research and Development Services, Systems
Integration Services, Food Packaging, Personal Care Products, Packaging Machinery.

### Judged examples (verbatim)

**a. `gtl7g9tq` "Manufacturing Services" → Industrial Machinery and Equipment — WRONG (own
services as industry).** Synthesis: "The entries show that Alec Model provides manufacturing
services, described in the site's own copy as 'Full-Spectrum Manufacturing Services for
Small-Scale Production and Prototyping'..." Grounding: "[IGR-M2/chosen] 'Manufacturing
Services' is not an option, but the activities are best generalized under 'Industrial Machinery
and Equipment'." This is the M2 axis-mismatch hypernym leak, and it is the dominant failure of
this field: **18 distinct rows** feed this one tag, all of them the shop's own activities —
'production', 'prototyping', 'engineering', 'Manufacturing', 'CNC Machining', '3D Printing',
'machine shop' ("We're more than a machine shop"), 'OEM', 'precision machining', 'international
manufacturing', 'Industrial Automation', 'manufacturing applications', '3D printing
manufacturer', 'Manufacturing Services', 'precision manufacturing solutions', 'precision
machining and prototyping fields'. None is an industry the shop serves.

**b. `gmckxdkp` "3D Printing" → Additive Manufacturing (OOV) — WRONG (technology as
industry).** Synthesis: "Alec Model offers 3D Printing as a service, described as
revolutionizing prototyping..." OGR-K1: "The synthesis describes Alec Model's provision of 3D
printing services as a business activity, which constitutes an industry/sector." A capability
restated as a sector.

**c. `gqc7n8x2` "packaging" → Packaging (chunk 1) and Medical Packaging (chunk 2) — WRONG
(logistics activity as industry).** Chunk-1 synthesis: "Alec Model provides packaging as part
of its manufacturing and delivery process... a quotation from a customer praising 'perfect
packaging, zero defects.'" Chunk-2 synthesis: "Alec Model completed packaging in a cleanroom
rated at Class 100,000 or better for a North American medical device client..." → IGR-M1: "The
option 'Medical Packaging' exactly matches the identified industry of packaging for medical
devices." The shop packs its parts; it does not serve the packaging industry. Note the same
group got different tags in the two chunks — cross-chunk duplication multiplies wrong tags.

**d. `gze30u4x` "OEMs" → Maintenance, Repair, and Overhaul (MRO) (OOV) — WRONG (expo attendee
list as industry).** Synthesis: "...participating in events such as the Hong Kong International
Aero Engine Expo 2025, which attracted engineers, project leads, and buyers from major
aerospace OEMs and MROs..." OGR-E1 admits the evidence is "The explicit mention of 'MROs' ...
as a distinct group Alec Model interacts with". Who attended a trade show is not an industry
served. (The same row also passes Aerospace — harmless, since Aerospace has 9 other rows.)

**e. `gesrfab0` "infrastructure" → Construction and Infrastructure — WRONG (material-blurb
boilerplate).** Synthesis: "Alec Model offers CNC machining services using mild steel, which is
described as ideal for infrastructure and automotive parts due to its toughness and
affordability, according to the site's own copy." A generic material-page sentence, not a
customer. Same family: `g7vb5gk7` "marine environments" → Marine Equipment ("using Inconel,
which is described as perfect for marine environments"), `gvl2wnpb` "manufacturing of cutting
and machining tools" → Tool and Die ("using tool steel, which is characterized as being
typically used in the manufacturing of cutting and machining tools"). Three industry tags
minted from what-material-X-is-good-for marketing copy.

**f. `gcd6u9wc` "international market" → International Manufacturing (OOV) — JUNK.** Synthesis:
"Alec Model has optimized production processes... to increase its share in the international
market." OGR-K1: "The phrase 'increase its share in the international market' refers to a
market, and 'providing manufacturing services' refers to an industry. Together, these describe
an industry/market sector." Rubber-stamp reasoning over a marketing phrase.

**g. `gzl08q9p` "Precision Manufacturing" → Semiconductor Equipment + Unmanned Aerial Systems —
CORRECT tags, shaky routing.** Synthesis: "...case-study listings for high-precision stainless
steel valve bodies for semiconductor equipment and aluminum alloy sheet metal housings for a
French UAV client..." The tags are right because the synthesis smuggled in two client domains;
the focal form itself ('Precision Manufacturing') is another own-activity phrase.

**h. `g0ztxttl` "medical care" → Healthcare and Medical Devices + Automotive + Consumer
Electronics — CORRECT.** Synthesis: "Alec Model's certifications demonstrate its ability to
provide solutions that meet strict requirements in fields such as medical care, automobiles,
and consumer electronics." Legitimate multi-tag row.

**i. `guwbxyai` "sheet metal fabrication factory" → Sheet Metal Fabrication (OOV) — WRONG
AXIS.** Synthesis: "Alec Model's production capabilities include a dedicated sheet metal
fabrication factory..." Own capability; belongs in process_caps (where Sheet Metal
Processing/Fabrication already passed), not industries.

**j. Screening inconsistency on identical evidence — the packaging-lines client.** `g750pce1`
"automation integrator specializing in high-speed packaging lines for the food and personal
care sectors" FAILED: "[SCR-2/failed] The synthesis only shows Alec Model was approached by a
client in this sector, not that Alec Model itself serves, supplies, or operates in it." But the
same client, same facts, on `gejskp3y` "packaging lines" and `gbnktffb` "food and personal care
sectors" (chunk 2) PASSED Food Packaging + Personal Care Products (+ OOV Packaging Machinery)
because those syntheses end "The manufacturer's own dealing is providing services to clients
involved in packaging lines." Screening outcomes track synthesis *framing*, not facts. Same
pattern: `grpeucy4` "French UAV manufacturer" failed ("does not specify any further action by
Alec Model") while `gszdggqw` "UAV" passed on the same case study; `gqaot2mw` "Danish surgical
robotics developer" failed while `gaqs60zp` "Surgical Robotics" passed. In this run every such
client industry happened to also pass via a better-framed row, so nothing was lost — but that
is luck, not design.

**Known past failure check:** no third-party firm names in the final tags (Robovision DK, SNCF
appear in syntheses only; `gvphoil8` "Mold Industry" — a certification-statistics table row
header — was screened out correctly). The firm-name leak did not recur.

**Verdict:** ~21/37 solid, ~9 wrong-axis (own technology/activity or boilerplate), ~7
borderline. The single biggest lever: forbid IGR-M2 from generalizing the subject's own
manufacturing activities into "Industrial Machinery and Equipment" (18 rows → 1 bad tag).

---

## 3. process_caps — 127 screening-passed tags (caller headline: 123)

271 rows / 250 groups → 196 grounded rows; only 8 rows screened_out. 38 tags are in-vocab; 89
are OOV-minted, 86 of them singletons. The head of the frequency table is a real machine shop;
the tail is a flood of one-off case-study narrative details.

### Top 30 by supporting-row count

17 CNC Machining · 12 Surface Finishing · 10 Machining · 9 Five Axis Machining · 9 Anodizing ·
8 Additive Manufacturing · 8 Packing · 7 Precision Machining · 6 Sheet Metal Processing · 6
Surface Preparation · 4 Labeling · 4 Fabricating · 3 Precision Cutting · 3 Bending · 3
Assembly · 3 Drilling · 3 CMM Inspection (OOV) · 2 Injection Molding · 2 Prototyping (OOV) · 2
Polishing · 2 Vacuum Fixturing (OOV) · 2 Inspection Reporting (OOV) · 2 Stress Relieving · 1
Rapid prototyping (OOV) · 1 Sheet Metal Fabrication · 1 Urethane Casting · 1 Die Casting · 1
CNC Milling · 1 CNC Turning · 1 Milling.

Judgment on the top 30: 26 are real machining-shop capabilities. Doubtful: **Packing** (8 rows
— 'packaging', 'packed', 'protective cases', 'Protective Packaging', 'export-ready packaging',
'packaging validation photos', 'Labeling & Packing'...) and **Labeling** (4 rows, incl.
`gsj9ocjb` 'shockwatch labels' — "used shockwatch labels in the packaging of parts") are
shipping logistics; they ARE ontology processes ('Packing (also: packaging)'), so the pipeline
is faithful to the vocabulary, but 12 rows of packing/labeling evidence for a machining shop is
inflation of case-study delivery narratives. **Inspection Reporting** and **CMM Inspection**
are QA activities (real ones — "each housing was inspected via CMM for hole concentricity,
flange flatness, and ventilation slot dimensions", `gcm77eie`), reasonable to keep as
capabilities. Die Casting / Injection Molding / Urethane Casting are genuinely offered services
("The site's own copy, under the manufacturing services section, describes Die Casting as
suitable for high-volume production...", `gef1saje`).

### 20 random others (seed 194457, from the 97 below top-30)

Die Casting*, Functional Testing, Ground Pad Quality Control, Helicoil inspection, Inspection
routine setup, Micro-blasting, Photographic Documentation, Plasma Cutting, Process Design,
Production Scheduling, Progressive Drilling, Reaming, Selective Laser Sintering, Toolpath
Generation, custom high-precision bending mold fabrication, final inspection, laser finishing,
precision fixturing, prototype manufacturing, toolpath design. (*tie-order put Die Casting past
rank 30 in this draw.)

Judgment: keep Plasma Cutting, Reaming, SLS (`geo4oolf`: "acquiring SLS (Selective Laser
Sintering) rapid prototyping machines"), Micro-blasting, Progressive Drilling, laser finishing,
Helicoil-related (as one "Helicoil/insert installation" concept), precision fixturing (merged
with Vacuum Fixturing/Specialized Fixturing/Precision 5-axis workholding). Drop or merge the
rest: QA-as-capability (Functional Testing, Ground Pad Quality Control, final inspection,
Inspection routine setup, Photographic Documentation), business ops (Production Scheduling,
Process Design, prototype manufacturing dup), CAM minutiae (Toolpath Generation, toolpath
design).

### The three failure families in the tail, with verbatim evidence

**(1) QA/testing/documentation inflated into capabilities** (~30 of the 89 OOV tags):
Inspection Reporting, CMM Inspection, CMM Reporting, Batch Inspection, First Article
Inspection, full inspection, final inspection, inspection, Quality Inspection, Quality Control,
Quality Assurance, Quality Assurance (QA), Ground Pad Quality Control, Secondary verification,
Hardness Testing, torque testing ("Alec Model provided insert torque tests as part of the
documentation for a project", `g0jh2z5v`), Form-fit testing ("delivered fully machined
prototypes for form-fit testing", `grn7jhzt` — the *client* does the testing), Factory
Acceptance Testing, Assembly Testing, Assembly Fit Testing, Assembly Verification, Functional
Testing, Material Testing, optical emission spectrometry (OES), Dimensional Reporting, Surface
Finish Measurement, Photographic Documentation, Document Control, packaging documentation,
batch traceability documentation, manufacturing reporting, Inspection routine setup, Helicoil
inspection, Internal Tolerance Control. The OOV gate rule OGR-K1 rubber-stamps every one, e.g.
`g78soen3` 'Shipment': "[OGR-K1/satisfied] Shipment is a manufacturing process, as it is an
operation performed on the work to deliver it to the customer." And `gfray0tp` 'Production
Capacity': "[OGR-K1/satisfied] Production capacity expansion is a manufacturing process, as it
involves operations to increase the ability to produce goods."

**(2) One-off case-study narrative details minted as standalone capabilities**: multi-axis
interpolation ("used 'multi-axis interpolation' to achieve less than 0.02 mm positional
tolerance", `g2jvcbw8`), TCP (Tool Center Point) control ("'TCP (Tool Center Point) control
with ±3 μm positional accuracy'", `gh6nnuu2`), cutting parameter optimization (from 'tuned
cutting parameters', `gg6gluyi`), bending sequence optimization, Process Parameter
Optimization, Process Parameter Control, Process Control, structural optimization, DFM
optimization, Design for Manufacturing (DFM) Reporting, high-speed drilling with progressive
pilot holes, Precision 5-axis workholding, Optimized Toolpath Programming, CAM programming.
These are real facts but machining minutiae, not catalog-level capabilities.

**(3) Business/lifecycle activities**: Shipment, Production Scheduling, Production Capacity
Expansion, Production Ramp-Up, pilot batch production, full-cycle manufacturing execution,
Manufacturing (General) (`gffb8be5` 'manufacturing' — vacuous), Prototype-to-Verification
Support, Traceability Implementation, Document Control.

**Product-attribute leak:** `gh000pnf` 'Unfinished (as-machined)' → OOV tag **"as-machined
finish"** — a surface-finish *option* (literally the absence of a process) passed as a process
capability.

**Cross-axis mis-grounding (in-vocab):** `gx0r03vp` 'Drawing' → in-vocab **Drawing** (the
metal-forming process). Synthesis: "Alec Model conducted drawing review and communication...
a lead time of 20 working days from drawing approval to dispatch". IGR-M1: "'Drawing' as a
process is directly referenced and matches the option." Engineering drawings ≠ deep drawing.
Clear false positive.

### Duplication / near-synonym clusters (counted as separate final tags)

- CNC Machining / Machining / Precision Machining / High-Precision CNC Machining / CNC Milling
  / Milling / CNC Turning / Five Axis Machining — 8 tags, ~3 concepts.
- Prototyping / Rapid prototyping / rapid prototyping (non-additive) / prototype manufacturing
  / Prototype Build / Thermal Component Prototyping / Prototype-to-Verification Support — 7
  tags, 1–2 concepts.
- **Quality Assurance vs Quality Assurance (QA)** — exact duplicate differing by a
  parenthetical, both OOV-minted (`gnri5f7e`, `giws1998`); `giws1998`'s OGR-N1 even asserts
  "'Quality Assurance (QA)' as a process is not present in ... already-identified results"
  (per-row scope again).
- Toolpath Generation / toolpath design / Optimized Toolpath Programming / CAM programming — 4
  tags, 1 concept.
- Vacuum Fixturing / precision fixturing / Specialized Fixturing / Precision 5-axis workholding
  — 4 tags, 1 concept.
- Sheet Metal Processing / Sheet Metal Fabrication / Precision Fabrication / Fabricating;
  Sheet Bending / Bending; Packing / Protective foam packaging / packaging documentation.
- Casing inconsistency in OOV mint: 'as-machined finish', 'sealing', 'cleaning', 'inspection',
  'final inspection', 'full inspection', 'toolpath design' lowercase vs Title Case elsewhere —
  exact-string final-tag identity means casing variants would not merge.

### Screening did real work where it fired

The 8 failed-only tags are all correct kills, e.g. **Hardening** ← `gpcn7qru` 'hardness
validation': "The synthesis only mentions 'hardness validation' (testing), not the process of
hardening (altering material properties)"; **Thermal dissipation testing**: "the need ...
attributed to the client, with Alec Model involved in supplying the housings"; **Laser
Interferometry**, **Epoxy Coating**, **Fit-Check Testing**, **Dimensional Inspection**,
**Lifecycle validation testing**, **sampling** — all client-attributed or testing-vs-process
confusions. The problem is not screening quality; it is that SCR only checks substance +
attribution, so anything the shop genuinely did — including shipping a box — passes.

**Curator keep-as-is estimate:** of 127 tags, ~37 of the 38 in-vocab (drop 'Drawing') plus
~15–20 of the 89 OOV survive as-is; after merging synonym clusters, **roughly 50–55 tags ≈
40–45% keep-as-is**. The multi-row head (count ≥ 2, 23 tags) is ~90% good; the singleton OOV
tail is ~75–80% discard-or-merge.

---

## 4. material_caps — 33 final tags

### Verdicts

Real machined-material capabilities with direct evidence (**27**): ABS, Acetal, Alloy Steel,
Aluminum, Aluminum Alloy, Brass, Carbon Steel, Copper, HDPE, Inconel, Magnesium (OOV), Metal,
Mild Steel, Nickel, Nylon, PVC, Peek, Plastic, Polycarbonate, Stainless Steel, Steel, Teflon,
Ti-6Al-4V, Titanium, Titanium Alloy, Tool Steel, Urethane, Wrought Aluminum Alloy, Zinc (OOV).
(That's 29 including the two OOV; see splits below.)

Wrong or dubious (**4**): Foam, Alumina, Nickel Alloy, Alloy.

### Quoted evidence (≥6)

**a. `gcbdetlp` et al. → Wrought Aluminum Alloy — STRONG.** Seven contributing rows, all
specific grades: 'EN AW 7075-T6 (EN 10204 3.1)', 'EN AW-7475-T7351 bar stock', '6061-T651
aluminum', '6061-T6 aluminum', 'extrusion stock', '6061-T6 extrusion plates', 'AL7075
aluminum'. Exactly what a precision shop's material list should look like.

**b. `gj6o4dlw` 'high-temperature alloys' → Alloy + OOV Titanium — REAL.** "Alec Model states
in the event highlights section of its page about the Hong Kong International Aero Engine Expo
2025 that it produces CNC machined parts in titanium and high-temperature alloys for engine and
structural use." ('Alloy' itself is a low-value parent tag; also fed by `g505quiy`
'aerospace-grade materials' via "The closest generalizing option is 'Alloy.'" — M2-style
vagueness.)

**c. `glgldo7r` 'Magnesium' → OOV Magnesium — REAL, and the new B1 path working.** "Alec Model
lists magnesium as a material option for small-scale production and prototyping and includes it
in the 'Selecting Materials' section for CNC machining services." Magnesium is genuinely absent
from the 158-node material ontology; the in-vocab pass answered 'Magnesium' anyway, the new
guard put it in `dropped_options` instead of crashing, and the OOV pass recorded it. Same story
for **`gd979y9n` 'Zinc'** ("lists zinc as a material option... in the 'Selecting Materials'
section"). Both are menu-listing evidence — the shop claims them; a curator keeps both (weak
but genuine claims). This is the designed rescue: dropped → OOV-covered → screened PASS.

**d. `g06pxo08` 'foam' → Foam — WRONG.** "all parts were packed in protective cases with
shock-absorbent foam and shockwatch labels before shipping. This shows Alec Model using foam as
a packaging material for shipping their manufactured parts." Packaging consumable, not a
machined material. ('Foam' is an ontology label, so brute search planted it and grounding took
the exact match at face value.)

**e. `gzp19xys` 'White alumina 800' → Alumina — WRONG-ISH.** "Alec Model lists 'White alumina
800' as a parameter for the 'Micro-blasting' step." A blasting abrasive (process consumable);
the shop does not machine alumina parts.

**f. `g1d61ryy` 'Nickel-based duplex process' → Nickel Alloy — DUBIOUS.** "Alec Model lists
'Nickel-based duplex process' as a parameter for the 'Sealing' step." IGR-M1: "'Nickel-based
duplex process' refers to a process involving nickel alloys, and the closest matching option is
'Nickel Alloy' as there is no more specific match." A surface-treatment chemistry inferred into
a material capability. (Elemental 'Nickel', `goo79est`, is separately fine: "lists Nickel as a
material option... in the 'Selecting Materials' list under 'Materials Compatible With CNC
Machining'.")

**g. `geznnmz3`/`g0pakh0c` → Carbon Steel — REAL.** "all steel plates cut and pre-machined
in-house from ASTM A36 using CNC plasma and mill" — and note the same ASTM A36 fact ALSO became
a conformity_attestations final tag (cross-field leak, see §5).

### Known past failures checked

- **'Lead' substring false positive: did NOT recur.** Both 'lead' groups exist (`gpk94tty`,
  14 + 3 mentions — 'lead times', 'lead development engineer', 'Project Lead') and both
  grounding passes declined on both chunk copies: "all uses are either about project timing,
  job titles, or leadership, not the material Lead." The containment rule plus
  synthesis-then-ground chain handled it.
- **Case-collisions:** no wrong merges seen in the 33 tags' `forms` lists (e.g. `gnqgk3b5`
  correctly merges 'Plastic'/'Plastics'/'plastic'/'plastics'). The 15 no_mentions rows are the
  familiar containment casualties (PVC/PEEK/PTFE/Teflon/Acetal/HDPE bare forms swallowed by
  their parenthesized long forms — cf. HOW_TO_READ_A_DUMP Q8), and every one of those concepts
  still surfaced via its long-form group. No evidence lost.
- **Screening:** the single screened_out row is right: `gzwo5h79` 'epoxy coated' → Epoxy FAIL —
  "not that Alec Model itself performs the epoxy coating. It is possible that this requirement
  is for a supplier or subcontractor."

**Verdict:** 29/33 real with solid evidence, 4 wrong/dubious (Foam, Alumina, Nickel Alloy,
Alloy). Best field of the four.

---

## 5. Cross-field observations

1. **Material specs became conformity attestations.** ASTM A36 and ASTM B221 (twice) are final
   conformity tags on uses-this-stock evidence, while the same facts correctly feed Carbon
   Steel / Wrought Aluminum Alloy in material_caps. The conformity OOV pass needs a
   material-spec exclusion (its own in-vocab pass already articulates it: "which is a material
   specification, not a certification..." — then OOV re-admits it).
2. **Process capabilities became industries.** Sheet Metal Fabrication, Additive Manufacturing,
   'Industrial Machinery and Equipment' (18 own-activity rows), International Manufacturing —
   all shop activities surfacing as industries-served. Reverse direction: industries'
   'Packaging'/'Medical Packaging' are the same packing activity that (legitimately, per
   ontology) feeds process_caps 'Packing'.
3. **Packaging shows up in all four fields** from the same case-study sentences: process_caps
   'Packing'/'Protective foam packaging'/'packaging documentation' (8+ rows), material_caps
   'Foam', industries 'Packaging'/'Medical Packaging', conformity 'Class 100,000 cleanroom
   standard compliance' (packaging step). One delivery narrative, four fields of tags.
4. **Material-page marketing blurbs mint industries**: mild steel→Construction and
   Infrastructure, Inconel→Marine Equipment, tool steel→Tool and Die.
5. **Per-row "already identified" scope causes cross-row duplicates**: 'ASTM B221' vs 'ASTM
   B221 material standard' (conformity), 'Quality Assurance' vs 'Quality Assurance (QA)'
   (process). OOV dedup never sees sibling rows, and final-tag identity is the exact string.
6. **Screening verdicts track synthesis framing, not facts** (industries §2j; conformity
   EN 10204-vs-ASTM B221). Where the synthesizer wrote "the manufacturer's own dealing is X",
   SCR-2 passes; where it wrote "the client required X", it fails — on the same underlying
   event. This run lost nothing important to it (except RoHS), but it is the mechanism most
   likely to flip tags between reruns given the known grounding noise floor.
7. **Systemic root causes ranked by yield:** (a) OGR-K1 is a rubber stamp — tightening "is this
   a *capability/industry/attestation*, not an activity/attribute/consumable" would kill ~40
   bad process tags, ~5 bad industry tags, ~4 bad conformity tags at one stroke; (b) IGR-M2
   own-activity generalization (18 industry rows); (c) no cross-row tag normalization/dedup for
   OOV mints.
