# Eyeball review: steelcraft.com process_caps + material_caps (run 20260825T194457)

First-ever review of these two fields on this subject (previous runs crashed at process_caps
grounding). Sources:

- `packages/logs/extraction_dumps/20260825T194457/steelcraft_com__process_caps.json` (189 rows, 2 chunks)
- `packages/logs/extraction_dumps/20260825T194457/steelcraft_com__material_caps.json` (108 rows, 2 chunks)
- Vocabularies: `apps/data_etl_app/src/data_etl_app/knowledge/ontology/process_caps.json` (521 concepts),
  `material_caps.json` (158 concepts)

"Final tag" here = the reviewer rule given for this audit: per row, screening-passed tags if
`screening` is present, else all grounding tags. Rows are per-chunk; the same `group_id` can
appear in both chunks (counts below are deduped by group where stated). Note the rule excludes
recursive descendants (e.g. `Wet Painting`), which never appear as screening keys — descent
quality is judged separately in §3.

Status distribution — process: 130 grounded / 43 no_candidates / 10 no_mentions / 6 screened_out.
Material: 68 / 14 / 11 / 15.

---

## Headline verdicts

1. **Material output is good; process output is flooded with unanchored OOV micro-tags.**
   process_caps: 89 distinct final tags, only **17 in-vocab, 72 OOV**. material_caps: 26 distinct
   final tags, 11 in-vocab, 15 OOV. The process OOV mass is dominated by (a) 15 testing tags that
   are mostly certification-lab activity, (b) an "X Installation / X Preparation / X Modification"
   family minted from product-feature nouns, (c) case-variant duplicates of the same concept.
2. **The new dropped_options path caught two distinct causes.** 11/14 process drops are genuine
   vocabulary gaps (`Testing` ×9, `Welding` ×2 — neither exists as a concept name). **3/14 are
   FALSE drops of in-vocab concepts caused by the model echoing the decorated menu label**
   (`"Extruding (also: Extrusion)"`, `"Machining (also: Material Removal Process, Subtractive
   Process, subtractive manufacturing)"` ×2) — `Extruding` (alt `Extrusion`) and `Machining` (those
   exact altLabels) ARE in the vocabulary. This is the known OPTION-axis echo hole, now surfacing
   as false drops instead of fake OOV.
3. **Recursive descent is mostly one hop and mostly defensible, but every Painting→Wet Painting
   hop (8 groups) rests on an unverified "liquid" claim**, and two descents are clear
   world-knowledge bridges (`Surface Finishing→Surface Preparation` on a mill finish;
   `Polymer→Plastic→Thermoplastic(→PVC)` on "extrusion implies thermoplastic", with the PVC leaf
   reached on a material the site explicitly says it does NOT use).
4. **Screening rejections are individually right on material (all 15 correct) but inconsistent on
   process**: the identical sentence "Steelcraft or its distributor's fabrication shop …" passed
   SCR-2 in two rows and failed it in two others; SCR-1 literalism kills the in-vocab
   generalization (`Thermal Welding`) while keeping the OOV specific (`Tack Welding`), i.e.
   screening systematically strips ontology-anchored tags in favor of unanchored ones.
5. **'Lead' is handled correctly this time** (contrast with the old material-caps audit's brute
   substring false positive): both the 'lead' row (pure "lead time" homonym) and the 'Lead' row
   (13 mentions, mostly "STEELCRAFT LEAD TIMES") were screened out.
6. **Case-collision duplicates exist in the final tag set** (4 pairs), including one where the SAME
   group got `Mineral board` from chunk 0 and `Mineral Board` from chunk 1.

---

## 1. Final output quality

### 1a. process_caps — 89 distinct final tags (138 tag-instances; g = distinct groups)

In-vocab (17): Thermal Welding 11g, Mechanical Joining 6g, Painting 5g, Surface Finishing 5g,
Adhesive Bonding 4g, Coating 4g, Fabricating 3g, Lamination 3g, Joining 2g, Labeling 2g,
Cut to Length Tubing 1g, Fusion Welding 1g, Hole Making 1g, Notching 1g, Packing 1g, Polishing 1g,
Surface Coating 1g.

OOV (72): Knock-Down Frame Preparation 5g; Adjustable Base Anchor Installation 2g, Adjustable Base
Anchor Preparation 2g, Die-Mitered Corner Connection 2g, Field modification 2g, Hardware
preparation 2g, Hinge Preparation 2g, Missile Impact Testing 2g; and 60 singletons: 3-Sided Frame
Welding, Anchor Installation Preparation, Anchor Preparation, Assembly Testing, Back-to-Back
Stiffener Assembly, Backbend Return Forming, Compression Anchor Installation, Compression Anchor
Preparation, Condensation (humidity) testing, Corner Clip Installation, Countersinking, Countersunk
Hole Preparation, Cyclic Wind Pressure Testing, Cyclic wind pressure testing, Die-Mitering, Door
Hardware Preparation, Door and Frame Modification, Door and frame stock modification, Door cutout
modification, Edge Beveling, Edge Interlocking, Edge Seam Filling, Epoxy filling of mechanical
interlock edges, Face Welding, Factory assembly of jamb and head components, Fiberglass batting
insertion, Field Assembly, Film Adhesion Testing, Finish Paint Testing, Finish painting of doors,
Frame Reinforcing, Glass Installation Preparation, Glass Light Kit Installation, Hardware
Preparation, Honeycomb core insertion, Hospital Stop Fabrication, Impact testing, Knock-Down
Preparation, Lead lining integration, Light Cutout Modification, Modification for lights and
louvers, Neutral pressure fire testing, Paint surface testing, Physical endurance testing, Positive
pressure fire testing, Product Testing, Reinforcement Channel Installation, Salt spray testing,
Series Assembly, Severe windstorm resistance testing, Silencer Preparation, Special Anchor
Preparation, Steel Stiffener Welding, Stitch Welding, Stud Anchor Preparation, Tab/Lock Corner
Assembly, Tab/Lock Forming, Tack Welding, Thermal Separator Installation, Thermal performance
testing, Uniform Static Air Pressure Testing, Uniform static air pressure testing, Unit Frame
Welding, Wood Grain Simulation.

**Verdict classes** (each judged against its own synthesis):

**CORRECT — real Steelcraft factory capability, well evidenced (~35 tags).** The strongest are the
welding, painting/coating, lamination, bonding, modification-services and fire-labeling families.
Representative quotes:

- `Thermal Welding` (gcmoqg9c): *"Steelcraft's own copy, in a bullet point under 'Chino Products &
  Services' … lists local, custom frame manufacturing and welding as a service."* Steelcraft's own
  service center performing welding — correct (`Fabricating` on the same row also correct).
- `Painting` (gxqtqolt 'finish paint'): *"Steelcraft offers a wide selection of standard finish
  paint colors and can create customized colors for hollow metal projects, but frames are not
  available with factory finish paint."* Explicitly their own factory painting of doors — correct.
- `Lamination` (geywziq8): *"the continuous bonding of the laminated core to steel face sheets
  provides an attractive, flat door, free of face welding marks."* Core construction is the
  factory's own operation — correct.
- `Adhesive Bonding` (gs3wqxc7): *"Steelcraft's LS Series stainless steel doors use stainless steel
  stiffeners that are epoxy-glued to eliminate surface imperfections associated with welding."*
  Correct.
- `Hole Making` (gxu7fdzo): *"lists function hole preps on doors as one of the doors and frames
  modifications offered at the facility"* (Chino Regional Manufacturing Center) — correct, and one
  of the few times a modification grounded into the ontology instead of an ad-hoc OOV name.
- `Labeling` (g06z8r2z): *"Steelcraft offers Intertek (WHI) fire labeling as a product and service
  at its Kansas City regional service center"* — applying labels is theirs, correct (contrast the
  testing behind the label, §Testing below).
- `Packing` (gelggini): *"Steelcraft offers custom crates to minimize risk of damage during
  transit"* — correct.
- `Steel Stiffener Welding` (g13su6xl): *"steel stiffened core construction using 20 gauge hat
  section stiffeners located 6 inches apart and welded 5 inches on center"* — the operation is real
  and theirs; only the ad-hoc OOV name (instead of descending under Thermal Welding) is a defect.

**WRONG — certification-lab testing attributed to Steelcraft (15 testing tags).** All follow the
same template: the site lists a test *standard* its products comply with; OOV grounding converts
compliance into a performed process; screening waves it through with explicit attribution
laundering. Quotes:

- `Product Testing` (gi269rxj): synthesis: *"the frames are tested and Intertek labeled, showing
  compliance with ICC500-2014 and FEMA 320/361."* The named tester is **Intertek**. Screening
  SCR-2: *"The synthesis attributes the testing and labeling to Steelcraft's own frames, indicating
  Steelcraft is responsible for ensuring the testing occurs."* "Responsible for ensuring" is not
  "performs" — misattributed.
- `Missile Impact Testing` (gm60lp3f): synthesis: *"'Large Missile Impacts' is listed as a test
  standard (TAS 201, ASTM E1886/E1996, ANSI A250.13, and ICC 500) … it references compliance with
  these test standards for its products."* Listing a standard ≠ operating a missile cannon;
  windstorm certification testing is third-party lab work. Wrong (same for `Uniform static air
  pressure testing`/`Uniform Static Air Pressure Testing`, `Cyclic ...` ×2, `Severe windstorm
  resistance testing`, `Neutral/Positive pressure fire testing`, `Physical endurance testing`,
  `Thermal performance testing`, `Assembly Testing`).
- The six FINISH-PAINT test tags (`Salt spray testing`, `Condensation (humidity) testing`,
  `Impact testing`, `Film Adhesion Testing`, `Paint surface testing`, `Finish Paint Testing`) are
  the most defensible of the family — e.g. gr3w75mq: *"salt spray testing is performed in
  accordance with ASTM B117-18"* — but the voice is passive throughout; whether Steelcraft's lab or
  the paint supplier's performs them is not evidenced. At best "possible", not established.
- Root cause is upstream: `Testing` does not exist in the process vocabulary (confirmed by scan; no
  `Testing`/`Inspection` top-level concept), so every test mention that survives screening becomes
  a bespoke OOV string. 15 of the 72 OOV tags (21%) are this one vocabulary gap.

**WRONG — installation-site / customer activity (5–7 tags).**

- `Field modification` (gnqs5x6u): synthesis: *"Steelcraft's patented universal hinge preparations
  allow for easy field conversion from standard-weight .134" … to heavyweight .180" hinges."*
  The conversion happens in the field, by the installer; Steelcraft only makes the prep. Screening
  passed it on: *"Steelcraft is the party providing the preparations that enable this field
  modification"* — enabling ≠ performing. Wrong (both groups; the correct capture of the same
  evidence is the `Hinge Preparation` tag, which also passed — so the record double-counts).
- `Field Assembly` (g9acvx6i): *"frames can be supplied as KD (knock-down) for field assembly or
  welded prior to installation"* — field assembly is what the *customer* does with a KD frame.
  Wrong (the right tag from the same evidence, `Knock-Down Frame Preparation`, exists 5×).
- `Compression Anchor Installation` (g5g6hx3k): *"Steelcraft … installs them using compression
  anchors"* is actually contradicted by the row's own framing: *"Steelcraft manufactures and
  supplies frames that use compression anchors for installation"* — installation into drywall
  happens on site. Wrong axis; `Compression Anchor Preparation` (the factory half) also exists as
  a separate tag from another group.
- `Mechanical Joining` on gr38dyhk: focal form is literally *"Installed into existing masonry with
  EMA anchors"* — a site activity. Passed. Wrong instance of an otherwise fine tag.

**INFLATED — product-feature nouns re-cast as processes (~15 tags).** e.g. `Edge Interlocking`
(g02ag0dq: the entire evidence is *"Steelcraft offers seamless and interlocking edge as a feature
and benefit"*), `Die-Mitered Corner Connection` alongside `Die-Mitering` (same concept, two rows,
two names), `Reinforcement Channel Installation`, `Thermal Separator Installation` (the separator
is a purchased polymer extrusion), `Back-to-Back Stiffener Assembly`, `Tab/Lock Forming` +
`Tab/Lock Corner Assembly`, `Corner Clip Installation`, `Series Assembly` (gd6ez4x2: doors
*"assemble as a series to meet FEMA tornado shelter standards"* — describes a certification
configuration, not a process; even the row's own in-vocab pass declined with *"Assembly here refers
to installation, not a manufacturing operation"*, then OOV minted a tag anyway). Individually
several are real factory operations by inference, but none of these names is anchored, and the
Preparation/Installation family alone contributes 14 tags where 2–3 ontology concepts
(hardware preparation ≈ Hole Making/Machining; anchor prep; KD prep) would do.

**Fake OOV — the concept exists in vocabulary (≥3 tags).** `Countersinking` (gp00fhdu; vocabulary
has `Counter Sinking` L5 under `Hole Making` — the in-vocab pass even claimed *"the options do not
list countersinking or hole making as a standalone process"*, which is false: another row grounded
to `Hole Making` from the same outline), `Countersunk Hole Preparation` (same concept again),
`Edge Beveling` (vocabulary has `Bevelling` L5 under Sawing). The OOV novelty rule (OGR-N1) was
also observed passing tags whose own N1 check FAILED: g u8yq7hw `Factory assembly of jamb and head
components` (*"[OGR-N1/failed] 'Joining' is already identified … Thus, this is not a new
process."*) and gsjmyp84 `Finish painting of doors` (N1 failed against `Painting`) were both still
emitted and screened in — **a failed N1 does not suppress the tag**.

**Missing vs. expectation:** `Roll Forming` is in the vocabulary and is certainly how frames are
made, but no phrase evidenced it — not a pipeline error, a site-text gap. `Machining` was reached
by the model twice and lost both times to the echo-drop (§2). `Extruding` likewise (though there
the OOV pass correctly concluded the extrusion is a purchased component). No forming/stamping tag
of any kind survived except OOV `Backbend Return Forming` and `Tab/Lock Forming`.

### 1b. material_caps — 26 distinct final tags

In-vocab (11): Steel 21g/26r, Stainless Steel 8g, Epoxy 5g, Galvanized Steel 3g, Glass 2g,
Fiber Glass 2g, Polymer 2g, Urethane 2g, Ceramic 1g, Chemicals 1g, Wood 1g.
OOV (15): Polystyrene 3g, Mineral board 2g, Type 304 Stainless Steel 2g, and singletons Ceramic
glass, Galvannealed Steel, Galvannealed steel, Glazing compound, Honeycomb, Kraft paper, Mineral
Board, Paint, Paper honeycomb, Rust inhibiting primer, Stainless Steel Stiffener, Type 316
Stainless Steel.

**CORRECT (the large majority).**

- `Steel` (gzejq7ow, 131 mentions): *"Steelcraft manufactures and provides steel doors and frames,
  including hollow metal steel doors … steel is the only readily available door material that
  passes FEMA 361 and ICC 500 tornado tests."* Unimpeachable.
- `Stainless Steel` (g9q94yhy c1): *"LS Series stainless steel doors are available in thicknesses
  up to 12 gauge and are made of 100% stainless steel."* Correct; `Type 304 Stainless Steel` /
  `Type 316 Stainless Steel` OOV variants are genuinely finer than the vocabulary (which stops at
  `Stainless Steel` L5) and well evidenced (*"available in either 304 or 316 alloy"*).
- `Galvanized Steel` (gf6vp26a): *"galvanized steel is used for superior strength and resistance to
  corrosion"* — correct; `Galvannealed Steel`/`Galvannealed steel` OOV pair is real material detail
  (A60 galvannealed) but duplicated by case and already ~covered by the Galvanized Steel tag +
  descent.
- `Epoxy` (gezvzngj): *"premium standard door construction combines … strong epoxy filled interlock
  edges"* — correct. BUT one of its 5 groups (god9k1b4) is bad evidence: the in-vocab pass tagged
  the *"Factory-applied baked-on rust inhibiting primer"* as Epoxy on pure world knowledge —
  *"[IGR-E1] 'primer' in this context is typically an epoxy or similar resin"* — the composition is
  never stated. Same M2-style unverified bridge, now at initial grounding on the material axis.
  (The same evidence in chunk 1 was handled correctly: IVG declined, OOV `Rust inhibiting
  primer`.)
- `Polystyrene` (3 groups, OOV): *"polystyrene is an optional core material providing enhanced
  thermal performance"* — correct, and a genuine vocabulary gap (no Polystyrene concept; scan
  confirms), recovered cleanly by the drop→OOV path all 4 times.
- `Fiber Glass` (gx7o9mbh): *"steel stiffened core construction uses … 1 pound fiberglass batting
  between stiffeners"* — correct.
- `Glass` (g8oawnf1): *"Paladin PW Series flush doors and frames can be equipped with
  factory-installed Glass Light Kits"* — factory glazing; correct, though the glass itself is
  supplier product (screening correctly rejected `Glass` on the Pilkington row, see §5).
- Core/consumable OOV tags `Mineral board`, `Kraft paper`, `Paper honeycomb`, `Honeycomb`, `Paint`,
  `Rust inhibiting primer`, `Glazing compound` — all evidenced as materials Steelcraft builds into
  or applies to its own doors; correct in substance, though `Honeycomb`/`Paper honeycomb`/`Kraft
  paper` are three names for one core material family, and `Paper honeycomb`'s evidence is
  explicitly inferential (*"the synthesis does not explicitly state the material … 'honeycomb
  cell' typically refers to a paper-based … core"* — OGR-E1 satisfied on world knowledge).

**WRONG (4 tags).**

- `Wood` (g8rspbxx): synthesis: *"C Series frames are typically welded and installed as part of the
  wall framing sequence using wood or steel stud anchors."* The wood is the *wall stud* the
  customer's building provides; Steelcraft processes no wood. Screening passed it (*"C Series
  frames can be installed using wood stud anchors, showing substantive involvement with wood"*)
  while four other Wood rows were correctly rejected as simulation/comparison — inconsistent, and
  this one is the error.
- `Chemicals` (gdlqieed): from *"fire rating capabilities include the use of ceramic glass and
  special glazing compounds"* — generalizing a glazing compound to `Chemicals` is vacuous
  (everything is chemicals), and the sibling `chemicals` rows (HPD disclosure; harsh-chemical
  environments) were rightly rejected. The specific OOV `Glazing compound` from the same row
  suffices.
- `Ceramic` (g793uv47 chunk 0): passed as generalization of "ceramic glass"; the SAME group in
  chunk 1 FAILED Ceramic (*"It does not describe Steelcraft processing ceramic itself, only ceramic
  glass"*) while passing OOV `Ceramic glass`. Cross-chunk contradiction; chunk 1's verdict is the
  right one. (Note the vocabulary places `Glass` L2 *under* `Ceramic`, so the generalization is at
  least tree-consistent — but the chunk-1 reasoning is better.)
- `Stainless Steel Stiffener` (gwjvjhxy): a component, not a material; its own OGR-N1 FAILED
  (*"a stiffener made of stainless steel does not constitute a new material distinct from
  'Stainless Steel'"*) yet the tag was emitted and screened in — same failed-N1 leak as
  process_caps.

---

## 2. In-vocab menu quality — the 14 dropped_options rows (process) + 5 (material)

process drops: `Testing` ×9 (gm60lp3f, gbg2dgtk, gm6xg1tp, g21dwglm, gxtop8gn, gmc0idrt, gpc22jbj,
gi269rxj, g5f1yv5n), `Welding` ×2 (ga8154tz, g9mbhrcx), `Extruding (also: Extrusion)` ×1
(gnwwkljf), `Machining (also: …)` ×2 (gll66lbk, gmh447et).
material drops: `Polystyrene` ×4 (g5ixmdyt ×2 chunks, guygclxa, gqqnh6vi), `Fiberglass` ×1
(gk69uwdk).

Five rows read closely, with verdicts:

1. **gxtop8gn 'tested to ASTM C1363 for Thermal Performance' → dropped `Testing`.** Synthesis:
   *"frames are tested to ASTM C1363 for Thermal Performance."* The model's reach was
   semantically right — this record describes testing and nothing else; the vocabulary simply has
   no Testing/Inspection concept anywhere (confirmed by full-name scan). Its own declination even
   says so: *"there is no matching or generalizing option for 'Testing' or 'Inspection, testing,
   and measurement' in the provided outline."* **Vocabulary gap**, not a record that shouldn't
   ground. (Whether Steelcraft or a lab performs it is then screening's problem — see §1.)
2. **ga8154tz 'welded for installation as a complete unit' → dropped `Welding`.** Synthesis:
   *"Steelcraft specifies and supplies frames welded for installation as a complete unit."*
   Generic `Welding` truly does not exist — the tree jumps from `Thermal Joining` to `Thermal
   Welding`/`Mechanical Welding` subtypes. Reach was semantically right; interestingly the same
   group in chunk 0 grounded the same evidence to `Thermal Welding` and the same model here
   reached for bare "Welding" instead — menu-choice instability, then the OOV pass minted `Unit
   Frame Welding`. Gap + instability.
3. **gnwwkljf 'extrusion' → dropped `Extruding (also: Extrusion)`.** **FALSE drop:** `Extruding`
   (L3 under Forming, altLabel `Extrusion`) IS in the vocabulary; the model answered with the
   decorated menu string (the menus render options as `Name (also: alt1, …)` — cf. IGR-M2 texts
   like *"generalized by the option 'Joining (also: Assembly)'"*) and the membership check
   rejected the decoration. Outcome accidentally acceptable — the OOV pass then correctly declined
   (*"'Extrusion' in this context refers to a polymer component used in the product, not the
   process of extruding"* — the separator is purchased) — but the mechanism is a bug: a validator
   that strips ` (also: …)` before matching would have tagged `Extruding`, which screening should
   then have rejected on supplier grounds. Two wrongs made a right here.
4. **gll66lbk 'doors and frames modifications' → dropped `Machining (also: Material Removal
   Process, Subtractive Process, subtractive manufacturing)`.** **FALSE drop, with real loss.**
   Synthesis: *"the facility offers a wide variety of doors and frames modifications, including
   light cutouts, function hole preps on doors, and reinforcing and welding of stock frames."*
   Light cutouts and hole preps ARE machining, performed by Steelcraft's own Chino center; the
   dropped string is exactly `Machining`'s name + its three altLabels. The row kept `Thermal
   Welding` and OOV `Door and Frame Modification`, but the ontology-anchored `Machining` tag was
   lost to the echo format. Same story on gmh447et 'light cutouts' (ended with only OOV `Light
   Cutout Modification`).
5. **gk69uwdk 'Steel stiffened core' (material) → dropped `Fiberglass`.** Synthesis mentions
   *"filled with 1 pound fiberglass batting between stiffeners."* The vocabulary spells it `Fiber
   Glass`; the model's one-word `Fiberglass` failed exact membership. Near-miss spelling drop —
   softer than the decoration bug but same strict-matcher root. Net loss on this row (OOV then
   declined citing *"'Fiber Glass' is already present in the vocabulary"* — so the fiberglass
   evidence in this record produced nothing), though two other rows captured `Fiber Glass`.

Summary: reach was semantically right in 13/14 process cases (the Testing/Welding family are real
gaps worth adding as generic concepts; the Machining/Extruding cases were right reaches mangled by
echo format). The Polystyrene drops are a clean gap correctly recovered via OOV. **Action items:
(a) strip `(also: …)` decoration (and normalize spacing/case) in the dropped-option membership
check; (b) consider adding a generic `Testing`/`Inspection` concept and a generic `Welding`
node.**

## 3. Recursive descent quality (8 descent lineages sampled)

Descents observed (leaf ≠ initial tag): Painting→Wet Painting (8 groups), Coating→Painting→Wet
Painting (3), Steel→Galvanized Steel (5), Steel→Stainless Steel (1), Adhesive Bonding→Lamination
(1), Mechanical Joining→Mechanical Welding (2), Surface Finishing→Coating (2), Surface
Finishing→Surface Preparation (1), Polishing→Mechanical Polishing (1), Polymer→Plastic→
Thermoplastic (1), Polymer→…→PVC (1).

1. **Painting → Wet Painting (g7wr25ls, primer): OVERREACH, mild.** RGR-E1: *"The process described
   involves applying a liquid primer (paint) that is then baked on, which is a form of Wet
   Painting."* The record says "factory-applied baked-on rust inhibiting primer" — **"liquid" is
   supplied by the model**, and a baked-on primer could equally be powder coat. Plausible for
   hollow-metal doors, but it is exactly an unverified world-knowledge subset claim. All 8 Wet
   Painting leaves (primer ×3, Prime Paint, finish paint, Custom colors, finish painting on doors,
   Clear coat) repeat the same "liquid" insertion.
2. **Steel → Galvanized Steel (gwripvot 'A60 Galvannealled steel'): GOOD.** *"'Galvannealed' is a
   zinc-coated steel, which is a form of galvanized steel"* — that is definitionally true and the
   focal form itself carries the evidence. (5 groups, all with explicit galvanized/galvannealed
   focal forms.)
3. **Steel → Stainless Steel (gzejq7ow 'steel'): GOOD.** Leaf evidenced verbatim in the synthesis:
   *"…flush, stile and rail, severe weather, acoustical, stainless steel, blast resistant…"*.
4. **Adhesive Bonding → Lamination (geywziq8): GOOD.** *"'continuous bonding of the laminated core
   to steel face sheets' directly describes a process where multiple layers … are bonded"* — the
   leaf term is in the text.
5. **Surface Finishing → Surface Preparation (g04y5dft '#2B - Smooth, rolled mill finish'):
   OVERREACH, clear.** RGR-M1: *"a mill finish that prepares the surface for further finishing
   (such as painting), which exactly matches the definition of 'Surface Preparation'."* A 2B mill
   finish is produced **at the steel mill** (it arrives on the purchased sheet); re-reading a
   finish *option Steelcraft offers* as a *preparation process Steelcraft performs* because the
   surface is "typically painted" is a pure axis-bridge. Not screened (descendants never are).
6. **Polishing → Mechanical Polishing (gxfzf496 '#4 Brushed Satin'): OVERREACH, moderate.**
   *"'brushed satin' and 'one directional grain' indicate a surface finish achieved by mechanical
   abrasion"* — true as world knowledge, absent from the record; and like #2B, a #4 sheet finish
   is commonly bought from the supplier. The initial `Polishing` tag itself passed screening on
   "Steelcraft offers this finish", which is the same supplier ambiguity.
7. **Mechanical Joining → Mechanical Welding (gr38dyhk 'Installed into existing masonry with EMA
   anchors'): OVERREACH by evidence-switching.** The grounded concept was anchor installation; the
   descent justifies itself from a *different clause* (*"frames may be 'welded for installation as
   a complete unit'"*). Each clause is real, but the lineage now claims the installation record
   evidences welding. (Also note the ontology places Mechanical Welding under Mechanical Joining,
   so the pipeline now holds welding under two parents; gqmi0inf did the same descent redundantly
   alongside its own Thermal Welding tag.)
8. **Polymer → Plastic → Thermoplastic (g62m60n9 'polymer extrusion') and → PVC (grmn3z4h 'vinyl
   separators'): the textbook M2-style bridge.** Thermoplastic step: *"extrusion is a standard
   process for thermoplastics … matches this exactly"* — inference from process to material class,
   not stated in the record. The PVC leaf is worse: it descends on *"traditional vinyl
   separators"*, which the synthesis says Steelcraft does **not** use (*"more durable … compared to
   traditional vinyl separators"*), and the RGR-M1 even admits *"even if only as a comparator."*
   Screening caught `Plastic` at the grounded level (*"The mention is only comparative"* — SCR-1
   failed) but the PVC/Thermoplastic leaves live below screening's reach.

Net: 4/8 sampled lineages GOOD (all where the leaf term is verbatim in the record), 4/8 carry
unverified world-knowledge subset claims — same signature as the known M2 axis-mismatch leak, now
in RGR clothing. The failure predictor is simple: **descent is sound when the leaf's name/synonym
appears in the synthesis, and unsound when RGR-E1 has to assert a property ("liquid", "mechanical
abrasion", "thermoplastics are extruded") the record never states.**

## 4. Case-collisions / duplicates; 'Lead'

Case-variant pairs in the FINAL tag set (would collide under case-insensitive keying):

| field | pair | rows |
|---|---|---|
| process | `Uniform static air pressure testing` (guwzesvm) / `Uniform Static Air Pressure Testing` (g21dwglm) | 2 groups, same concept |
| process | `Cyclic Wind Pressure Testing` (gbg2dgtk) / `Cyclic wind pressure testing` (gvutc2fd) | 2 groups |
| process | `Hardware preparation` (gyakxc89, gzynov8n-c1) / `Hardware Preparation` (gfcdnera) | 3 groups |
| material | `Mineral board` (gudf1k9m-c0, gsnioy91) / `Mineral Board` (gudf1k9m-c1) | **same group gudf1k9m emitted both casings from its two chunks** |
| material | `Galvannealed Steel` (g7ymhht5) / `Galvannealed steel` (g6un3wkb) | 2 groups |

Near-duplicates beyond case: `Die-Mitering`/`Die-Mitered Corner Connection`;
`Knock-Down Preparation`/`Knock-Down Frame Preparation`; `Countersinking`/`Countersunk Hole
Preparation`; the 6-member anchor-prep family; the 5-member door-modification family;
`Honeycomb`/`Paper honeycomb`/`Kraft paper`; `Ceramic glass` vs `Ceramic`+`Glass`. Whatever merges
rows into the manufacturer record must be case-insensitive AND ideally OOV-normalizing, or the
record will carry these as distinct capabilities. (No sign of the OLD bug of case-collision
nulling rows — both members of every pair are intact rows here.)

**'Lead': handled correctly.** Two rows, both screened out. gpk94tty-c0 ('lead', 2 mentions):
IGR-E1 itself failed — *"the synthesis refers to 'lead time' (the time required to fulfill an
order), not the material lead"*. gpk94tty-c1 ('Lead', 13 mentions): synthesis separates the one
real lead mention (*"frame lead-lining clips are available for easy integration of lead where
radiation transmission is a concern"*) from the *"STEELCRAFT LEAD TIMES"* noise; screening then
rejected: *"This only shows Steelcraft providing clips for lead, not processing lead itself."*
Defensible (arguably conservative — lead-lined door assemblies are a real product line — and note
the cross-field tension: process_caps PASSED OOV `Lead lining integration` from the same clips
evidence). No substring false positive this run.

## 5. Screening review — every rejected row

**material_caps — 15 screened_out rows (13 groups): ALL CORRECT.**

- gpiy53tu `Glass`/Pilkington: *"[SCR-2/failed] … not that Steelcraft itself processes or
  manufactures the glass. The activity is limited to offering a product that includes glass made
  by Pilkington."* Correct — supplier product.
- ggkmcm9f `Chemicals` ×2: c0 *"does not describe Steelcraft processing or using chemicals—only
  disclosing their presence"* (HPD page); c1 *"only describes the frames being used in
  environments with chemicals."* Both correct.
- gi3d8x03 `Metal`,`Wood` ('metal sheathing'): *"Steelcraft does not offer tornado-resistant doors
  made with metal sheathing over wood."* Correct — a negated competitor construction.
- gttgtpgg `Metal` ('metal'): *"[SCR-G1/violated] The activity described is not current but
  historical: 'Steelcraft sold its metal building division'"* (1959). Correct; the one SCR-G1
  firing observed, and apt.
- gpk94tty `Lead` ×2 — see §4. Correct.
- gevtj19j `CMU`,`Concrete` + gmlglbwf `Concrete` + g4m0wwkh `CMU block` + g3xbobq0 `Concrete`:
  all *"frames are designed for installation with …"* — wall materials at the site, correctly
  rejected every time.
- g4wdctzp-c1 `Wood`, g7td62v7 `Wood` ('ash'), gjsaok6u `Wood` ('walnut'): *"describes simulation,
  not processing of wood itself"* (GRAINTECH engrained finishes). Correct.
- gftxv4rf `Fiber Glass` ('fiberglass'): *"fiberglass is listed as a material that steel
  outperforms"* — comparison only. Correct.

**process_caps — 6 screened_out rows: 5 correct, 1 wrong-for-the-right-rule; plus 5 grounded rows
with partial failures showing two systematic problems.**

- g2643vtg `Tornado Door Installation`: *"[SCR-2/failed] While Steelcraft provides guidance, the
  synthesis does not state that Steelcraft itself performs … installation."* Correct.
- g52bewq4 `Continuous Profile Welding`: *"[SCR-2/failed] Steelcraft's frames are designed to
  AVOID continuous profile welding, not that Steelcraft performs or offers it."* Correct — and it
  had to catch OOV grounding's absurd OGR-E1 (*"The process itself is evidenced as something that
  could be performed"*).
- guuoe1u8 `Hardware preparation` ('Hardware Preps' in Technical Data Guides): *"only shows
  Steelcraft documenting or listing hardware preparation."* Correct on this record (the concept
  survives from better records).
- gxyzr92k 'notched' + gvxwiivi 'mitered' (`Mechanical Joining`,`Notching`,`Thermal Welding` all
  failed): *"[SCR-2/failed] The synthesis attributes the process to 'Steelcraft or its
  distributor's fabrication shop', so it is not confirmed that Steelcraft itself performs …"*
  Defensible in isolation, **but the same sentence passed SCR-2 in gk7atzku ('cut to length' →
  Cut to Length Tubing, Notching, Thermal Welding all PASSED: "Steelcraft is one of the parties
  that may perform this") and gqmi0inf ('assembled and welded' → passed)**. Four rows, one
  sentence, opposite verdicts — screening is a coin-flip on shared-attribution evidence.
- ga8154tz-c0 `Thermal Welding` ('welded for installation as a complete unit'):
  *"[SCR-1/failed] … does not mention or describe thermal welding specifically."* WRONG rule
  application: welding of steel frames IS thermal welding; SCR-1 read literally demands the
  ontology label appear in prose. The same literalism produced the partial failures on g5b12ru0
  ('tack welds': Thermal Welding failed, OOV `Tack Welding` kept), g5bp2k12 ('stitch welded'),
  g2tkvw2x ('face welding'), g88efzja ('reinforcing and welding of stock frames' — Thermal
  Welding failed while OOV `Frame Reinforcing` passed!), gjf582rr (Mechanical Joining failed,
  `Glass Light Kit Installation` kept). **Systematic effect: SCR-1 strips the in-vocab
  generalization and keeps the ad-hoc OOV specific — the exact opposite of what a
  vocabulary-anchored KG wants.** Fortunately Thermal Welding survives via 11 other groups, but
  the mechanism will bite on thinner subjects.

## Appendix — counting notes

- Task brief said 4 (process) / 12 (material) screened-out; actual status counts are 6 / 15 rows
  (15 rows = 13 distinct groups; ggkmcm9f and gpk94tty each appear in both chunks).
- process rows with `in_vocab_grounding` shapes: 57 tags-only, 1 tags+dropped_options,
  13 declined+dropped_options, 108 declined, 10 empty (no_mentions).
- Cross-chunk instability beyond casing: gzynov8n got `Mechanical Joining` (c0) vs OOV `Hardware
  preparation` (c1) for the same group; ga8154tz got screened-out `Thermal Welding` (c0) vs passed
  OOV `Unit Frame Welding` (c1); g793uv47 passed `Ceramic` (c0) and failed it (c1).
- Recursive-descent leaves (`Wet Painting` 8g, `Mechanical Welding` 2g, `Surface Preparation`,
  `Mechanical Polishing`, `Thermoplastic`, `PVC`, `Galvanized Steel` 5g, `Stainless Steel` 1g,
  `Lamination` 1g, `Coating` 2g) are never screening keys — whatever assembles the record must
  decide whether unscreened descendants inherit their ancestor's screening verdict.
