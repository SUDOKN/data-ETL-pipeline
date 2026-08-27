# Census — steelcraft.com / `contract_products` (run 20260825T194457)

Dump: `packages/logs/extraction_dumps/20260825T194457/steelcraft_com__contract_products.json`

**Method.** Every item enumerated by script into compact lines, then read and coded by hand, in
batches, against the shared taxonomy. No regex/keyword classification was used to assign any code.
Coverage is complete: **465/465** grounded tag instances, **258/258** declines, **465/465**
screening verdicts, **1,395/1,395** screening rule explanations (657 on the 219 passed verdicts
coded individually; the 738 on the 246 failed verdicts rolled up into the verdict-level OK/WRONG
call, as the taxonomy specifies). 680 rows, 559 distinct groups, 121 twinned.

Rule texts used as the yardstick: `product_phrase_freehand_grounding.json` (FGR-E1/E1a/E1b,
FGR-Q1/Q1a/Q1b, FGR-Q2) and `product_phrase_screening_contract.json` (SCR-1 substantive,
SCR-2 customer-directed, SCR-3 manufacturer-is-the-actor, SCR-G1 guard). Note the live catalog has
**SCR-3, not SCR-2b/2c**; it is reported separately below.

---

## 1. Grounded tag instances (465, one code each)

| code | meaning | n | % |
|---|---|---:|---:|
| N | NORMALIZED | 201 | 43.2% |
| D | DIRECT | 143 | 30.8% |
| V | VAGUE | 47 | 10.1% |
| B | BRIDGE (unstated inference step) | 31 | 6.7% |
| X | AXIS (wrong field) | 24 | 5.2% |
| P | PARTY (wrong actor) | 16 | 3.4% |
| F | FABRICATION (entity absent from record) | 3 | 0.6% |
| | **total** | **465** | |

Grounding is largely sound (74% D+N). Coding conventions applied consistently:

* **V** = the tag fixes nothing beyond Steelcraft's whole business — bare `doors` (26) / `frames` (18),
  plus `door stock`, `frame stock`, `small parts`, `door and frame products`, `frame components`.
  44 of the 47 V's are bare `doors`/`frames`.
* **X** = finishes and paint (`finish paint colors`, `GRAINTECH finishes`, `wood-look steel door
  finishes`, `hand-stained embossed door finishes`, `GRAINTECH swatches`), preps
  (`hardware preps` ×3, `door preps`), materials (`steel face sheets`, `steel door faces`,
  `component frame material (stick sections)`, `galvannealed steel components`,
  `stainless steel components for frames`, `ceramic glass`), drawings (`elevations for frames`,
  `storefront elevations`), packaging (`custom crates`) and a shipping programme (`5-Day Express Doors`).
* **P** = third-party or sister-brand hardware and competitor product: `exit devices` ×2 (Von Duprin),
  `door hinges` (IVES), `wall magnet hold-opens` (LCN), `zero thresholds`, `gasketing`,
  `heavyweight hinges` ×2, `heavy weight hinges`, `standard weight hinges` ×2, `fire-rated glass`
  (Technical Glass Products' FireLite), `lead-lined doors and frames` (Republic), `veneered wood
  doors` + `solid wood doors` (competitor products used as a foil), `steel products` (a general
  steel-recycling claim). **All 16 P instances were screened out** — see §5.
* **F** = 3 instances, all where the explanation itself supplies the entity from world knowledge.

Of the 219 **passed** tags, the grounding code was already defective in **51**: V 23, B 13, X 12, F 3.

---

## 2. Declines (258, one code each)

| code | n |
|---|---:|
| OK | 160 |
| SCOPE (belongs to another field) | 39 |
| TWIN (grounded in the other chunk) | 30 |
| TEMPLATE (misfiring definitional argument) | 16 |
| LOST (real entity wrongly declined) | 13 |
| **total** | **258** |

* **SCOPE 39** — materials (`A60 Galvannealled steel`, `Type 304 Alloy`, `fiberglass batting`,
  core materials), processes (`continuously welded edges`, `stitch welded and filled`, `epoxy-glued`,
  `3-sided frame welding`, `Special anchor preps`, `light cutouts`, `function hole preps on doors`),
  standards (`ANSI A250.8-2017`, `ASTM A666`, `UL-10C`) and labelling services. Right call here,
  real entity for another field.
* **TWIN 30** — the same `group_id` was grounded in the other chunk: `core systems`, `stud anchors`,
  `Adjustable base anchors`, `steel face sheets`, `Honeycomb cell`, `latching hardware`,
  `wedge-lock corner clips`, `GRAINTECH Doors`, `FT Series (Thermal Break)`, `DW & K Series Frames`,
  `Hardware Preps`, `temperature rise door`, `Fire Ratings`, `top and bottom caps`, …
* **TEMPLATE 16** — the "X is a component / accessory / feature, not a product category" line applied
  to things the pipeline grounded elsewhere in the same dump: `Frame anchors` (grounded as
  `frame anchors` #102), `jamb compression anchors` (grounded as `compression anchors` #137),
  `DW Series: adjustable base anchors` (grounded #121), `wood or steel stud anchors` /
  `wire or Masonry-T anchors` / `EMA (existing masonry) anchors` (grounded as `stud anchors` #432,
  `EMA anchors` #451), `Optional Polystyrene cores` (grounded as `door cores` #131/#410),
  `frame open sections` (grounded #375), `steel faces` (grounded as `steel door faces` #130),
  `Frame Lead-lining clips` (grounded as `lead-lining clips` #392) — "not a **main** product
  category" is a criterion the catalogue does not contain.
* **LOST 13** — Steelcraft product lines declined because the record shows them only as a document
  title: `Steelcraft Paladin Door`, `FP Series Frames`, `C & CK Series frames`, `CE Series doors`,
  `Falcon SZ Series` ×2, `High Definition Door` ×2, `Integral Kerfed Frame` ×2,
  `Tornado-Resistant Doors`, plus `acoustical door assemblies` and `acoustically rated doors and
  frames`. The same shape **was** grounded elsewhere (`FT Series (Thermal Break) frames`, #427),
  so the rule is applied inconsistently. 86 of the 258 declines turn on a document/literature title.

---

## 3. Screening — the headline count

### Passed verdicts (219). Every rule outcome coded.

| | R real | C circular | G generic | I inverted | M mirror |
|---|---:|---:|---:|---:|---:|
| **SCR-1** (substantive) | 184 (84.0%) | 34 (15.5%) | 1 (0.5%) | 0 | 0 |
| **SCR-2** (customer-directed) | **17 (7.8%)** | **88 (40.2%)** | **100 (45.7%)** | **14 (6.4%)** | 0 |
| **SCR-3** (manufacturer is the actor) | 218 (99.5%) | 0 | 0 | 0 | 1 (0.5%) |

**SCR-2 is unsound on 202 of 219 passes (92.2%).** The earlier ~87% sampled estimate is confirmed
and slightly exceeded as an exact count. Split of the 202: 88 circular ("Steelcraft offers/manufactures
them, therefore customer-directed"), 100 generic (a standard, rating, application, market or option
list substituted for the rule's test), 14 inverted (the quoted evidence says the opposite).

SCR-1's 34 C's are all the same move: the explanation itself says the record only *lists* the item
("'Full Flush' is a subcategory under the 'DOORS' heading … indicating a substantive offering"),
which is exactly what SCR-1's text excludes ("rather than only naming it, listing it").

### Failed verdicts (246)

| | n |
|---|---:|
| OK (correct rejection) | 241 |
| WRONG (real contract work rejected) | 5 |

The five WRONGs: **#407 `custom doors` (gxtosca1)** — the identical tag from the identical group
passed in the other chunk; **#127 `configured doors` (gjnfujbx)** — the identical tag passed at
#425/#426; **#113 `doors` (g4rsm98q)** — record says "Steelcraft's team will work with customers to
craft just the right shade … applied to any of Steelcraft's trusted door solutions", and the sibling
paint tags passed on that same sentence; **#396/#397 `stainless steel doors` / `stainless steel
frames` (gvpawofc)** — record says "a complete offering of **standard and custom** Stainless Steel
Doors and Frames" and the custom half was ignored.

### Rule totals across all 465 verdicts

SCR-1 satisfied 459 / failed 6. SCR-2 satisfied 219 / failed 240 / not_triggered 6.
SCR-3 satisfied 219 / **failed 0** / not_triggered 246.

---

## 4. Twin behaviour

* 559 distinct groups, **121 twinned** (appear in both chunks), 680 rows.
* **58 of 121 twinned groups (47.9%) get a different `status` in their two chunks** —
  23 grounded↔screened_out, 19 no_candidates↔screened_out, 11 grounded↔no_candidates,
  3 no_candidates↔no_mentions, 2 no_mentions↔screened_out.
* **61 of 121 (50.4%) produce different tag sets** from the same group.
* **7 groups produce an opposite screening verdict on a byte-identical tag string**
  (10 tag instances): `gppz0xa5` (`flush doors`, `door frames`), `g04h532n`
  (`hollow metal steel doors`), `giqbyr4z` (`steel doors`, `steel frames`), `gxtosca1`
  (`custom doors`), `gd64cgkv` (`fire-resistance-rated frames`), `gj5jxusm`
  (`temperature rise doors`), `g6wkbd44` (`stock doors`, `stock frames`).
* 30 declines are groups grounded in the other chunk (§2).

`g6wkbd44` is the sharpest case: **`stock doors` passes in one chunk and fails in the other**,
and the failing explanation is the correct one.

---

## 5. The ten most consequential errors (verbatim)

1. **gj1fwnwq — fabricated entity, then admitted as contract work.** Focal form is the heading
   `STEELCRAFT PRODUCTS`; the record never says "doors".
   E1: *"Since Steelcraft is a manufacturer of steel doors and frames, and the context is about their
   own products for these applications, 'steel doors' is directly evidenced."*
   SCR-2: *"Steel doors are described as Steelcraft's **own products**, indicating they are made to
   customer order or specification."* (twin instance #306 `steel door frames` identical). **F + I.**
2. **gpp58w24 — same fabrication, opposite twin.** E1: *"…designed for institutional, commercial, and
   industrial markets, **which, in the context of the site, refers to doors**."*
   SCR-2: *"the products are designed for specific markets and applications, indicating
   customer/project-driven work."* The other chunk of this group **declined** the same focal form.
3. **gaj63gns — a possessive read as customer direction.** SCR-2: *"The phrase **'our steel doors'**
   indicates these are made for customers, showing customer-directed work."* (and #14 for frames).
4. **g6wkbd44 — stock inventory admitted as contract.** SCR-2: *"Steelcraft offers **stock doors** as
   a product category, available for customer order and quick shipment."* The same group, same tag,
   fails in the other chunk with *"describes Steelcraft's stocking and supply of **standard stock
   doors**, not that they are made to customer specification."*
5. **g3wpgwui — "standard" read as "custom".** SCR-2: *"Mullions are manufactured as **standard
   components**, indicating they are made to customer order/specification."* (and #369 `dividers`).
6. **gabn2r4c / g7mf691z — an express *stock* shipping programme admitted 4×.** SCR-2:
   *"The doors are offered to meet quick-turn deadlines for customers, indicating customer-directed
   work"*; *"offers small parts for doors and frames as a product category for customer order and
   quick shipment."* Meanwhile `5-Day Express Doors` (ghkxl8zg) and the same-programme rows #173–#180
   were correctly failed.
7. **giqbyr4z — industry typicality substituted for evidence.** SCR-2: *"The synthesis refers to
   these as Steelcraft's products, implying they are made to customer order or specification,
   **as is typical for contract work in this industry**."* Twin fails the same tag.
8. **gy7ol6ey — catalogue copy declared to mean contract work.** SCR-2: *"…the context of
   **'available' and 'offering' in manufacturer product copy typically refers to items made to
   customer order or specification**."*
9. **g8c4kqup — an OEM relationship invented.** SCR-2: *"…components manufactured by Steelcraft for
   their frames, which are **typically specified by customers or for OEM use** in construction
   projects, implying customer-directed work."* Nothing in the record mentions any customer or OEM.
10. **gxtosca1 — the one genuinely contract item, decided both ways.** Pass (#126): *"'custom doors
    may be produced,' which implies that Steelcraft manufactures doors to customer specification."*
    Fail (#407): *"There is no indication that these custom doors are made to customer specification
    or as contract work."* Same group, same tag string.

Honourable mention (grounding, not screening): `gruh87jt` grounds **`exit devices`** from
*"Von Duprin Exit Devices"*, `g4qoeu67` grounds **`door hinges`** from *"IVES Hinges"*, `gvtgsdqv`
grounds **`wall magnet hold-opens`** from *"LCN SEM Wall magnet Hold-opens"*, and `gzbj6iqw` grounds
**`lead-lined doors and frames`** off a referral to *Republic Doors and Frames*. Every one is another
company's product presented as a Steelcraft product category.

---

## 6. Unexpected patterns (not named by the taxonomy)

1. **SCR-3 is inert — it rejected nothing, 0/465.** It is `satisfied` on all 219 passes and
   `not_triggered` on the other 246 (SCR-2 short-circuits first). The 16 PARTY grounding errors were
   all caught by **SCR-2**, on the reasoning "Steelcraft doesn't *make* the hinges", i.e. the
   customer-direction screen is doing the wrong-actor screen's job. The screen designed to catch
   third-party attribution has never fired in this dump.
2. **"Standard/stock" evidence admitted as contract — 14 instances (all the I's).** The word
   *standard*, *stock* or *own* appears in the very sentence quoted as proof of customer direction.
   This is the most mechanically detectable failure in the dump.
3. **Application/standard substitution — 100 instances (45.7% of passes).** A fire rating, an
   ANSI/FEMA/ICC standard, a wall condition, a market segment or an option list is offered where
   the rule asks who the items belong to. Nine of these are literally *"designed to meet ANSI
   A250.8-2017 … which are customer-driven specifications"*.
4. **Catalogue designations carried into tag names — 72 instances (56 distinct tags)**, e.g.
   `T Series doors`, `SZ Series Falcon Flush Doors`, `FN Series Three-Sided Flush Frames`,
   `INPACT Series door systems`, `E6 embossed panel doors`. FGR-Q1a explicitly forbids carrying "its
   maker, its designation, its catalogue reference" into the category name, yet **FGR-Q1 is reported
   `satisfied` on every one of them**.
5. **Compound tags bundling two categories — 11 instances**, e.g. `hurricane doors and frames`,
   `lead-lined doors and frames`, `sound-rated door and frame assemblies`, `Paladin door and frame
   assemblies`, `small parts for doors and frames`, `lights for doors and frames`. FGR-Q2 is reported
   satisfied on all of them, usually with "No other product category is named in this record".
6. **Document titles as the whole evidence base — 86 of 258 declines.** The search stage keeps
   feeding literature/data-sheet/brochure/programme names into grounding, which then spends a full
   round on `System Set D1…S4`, `FBC Lookup-Anchors (Excel)`, `Steelcraft Portfolio`, etc. Thirteen
   of these declines are LOST product lines (§2) and one such title (`FT Series (Thermal Break)`)
   **was** grounded — the rule is being applied both ways.
7. **The "not a *main* product category" criterion** appears 16 times (§2, TEMPLATE) and exists
   nowhere in the catalogue. It is the reason `Frame Lead-lining clips` is declined in one place and
   `lead-lining clips` grounded in another.
8. **Feature-and-component records promoted to product categories.** 31 B-coded tags come from
   records whose focal form is a construction detail — `7 gauge hinge reinforcement` →
   `door hinge reinforcements`, `four corner tabs` → `FN Series Three-Sided Flush Frames`,
   `epoxy filled mechanical interlock edges` → `tornado-resistant flush doors`. The decline path
   rejects exactly this shape 100+ times; the grounding path accepts it 31 times.
9. **Only one M in 657 rule outcomes** (#457 SCR-3, which infers the maker from a similarity
   comparison). Mirror-of-grounding is essentially absent here — the screening stage does not reuse
   the grounding argument; it invents a fresh, weaker one.

---

## 7. Verdict — how many of the 219 passed contract tags are genuinely contract work?

**Roughly 11 of 219 (5%), and on a strict reading 6.**

Only **17** of the 219 passes (7.8%) rest on evidence that actually tests customer direction, and
those 17 fall into three tiers:

* **Genuinely customer-directed product work — 6:** `custom doors` (gxtosca1 #126, gmd9gvdg #408),
  `custom frames` (go8wkisj #250), `stainless steel doors` with *custom sizing, lites and finishes*
  (g5rn2wq0 #409), `configured doors` / `configured frames` (ggcgvlsd #425/#426).
* **Defensible customer-directed *work*, but the tag is the wrong object — 5:** service-centre
  modification work tagged as bare `doors` / `frames` (ghh68g9x #122/#123, gqsuh3j9 #423), and
  factory assembly to a customer's hardware selection tagged `door systems` / `door assemblies`
  (gaphbp8p #209, gmvn8c99 #216).
* **Real customer-directed activity on the wrong axis — 6:** custom paint colours (#110/#111/#112/#115,
  all X-coded finish tags), `door preps` (#219, a process), `stick sections` (#351).

The other **202 of 219 (92.2%)** are catalogue products admitted on circular, generic or actively
contradicted reasoning. This matches the ground-truth intuition exactly: a catalogue hollow-metal
door maker has a handful of custom/configure-to-order lines and essentially no contract manufacturing,
and the field is currently returning its entire product catalogue instead.

The single highest-value fix is at SCR-2, not at grounding: **grounding is 74% clean, screening is
8% clean.** A rejection clause that fails any candidate whose cited evidence contains "standard",
"stock", "own", "offers", "product line" or a bare standard/rating reference would remove 202 of the
219 passes and cost at most 5 (the WRONG rejections in §3 show the model can already reason
correctly when the record says "custom").
