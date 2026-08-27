# Eyeball review: products vs contract_products split — run 20260825T194457

Reviewed 2026-08-25. Files: `packages/logs/extraction_dumps/20260825T194457/{steelcraft_com,alecmfg_com}__{products,contract_products}.json`.
The two fields are a deliberate A/A pair upstream (identical search/mention/synthesis groups, identical freehand-grounding prompt+payload);
only the relationship-screening rules differ (`product_phrase_screening_pure_product.2026.08.8` vs `product_phrase_screening_contract.2026.08.8`).

**Counting convention used throughout:** a *group* is a distinct `group_id`; a group appearing in both chunks contributes two
independent rows (each chunk has its own synthesis, grounding and screening). A group "passes a screen" when **any** of its rows
has `status: "grounded"`. My deduped counts differ slightly from the headline numbers in the task brief (e.g. I count 316
steelcraft-products passing groups and 226 distinct passed tags vs the brief's 302/209) — the direction and magnitude of every
finding is unaffected by the convention.

---

## 0. Headline verdict

The two screens do **not** jointly encode catalogue-vs-contract. Each screen's **FAIL reasoning is consistently sound**; each
screen's **PASS reasoning is unreliable on the subject it should be rejecting**:

- On steelcraft (catalogue maker), the contract screen's SCR-2 passes are overwhelmingly **circular** — 13 of the 15 explanations
  I read invent customer direction from ordinary catalogue evidence ("offers", "our", "product subcategory").
- On alecmfg (contract shop), the products screen's SCR-2 passes are the **mirror-image failure** — all 12 sampled explanations
  flip "manufactures X *for a client*" into "manufactures and sells X *as its own*", directly contradicting catalog note SCR-2c.
- The same screen applies the same rule correctly on other rows of the same file (see §4), so the failure is a per-row coin flip,
  not a mis-specified rule text.

---

## 1. Split sanity

Group-level overlap (pass = any row grounded):

| subject | total groups | pass BOTH | only products | only contract | neither |
|---|---|---|---|---|---|
| steelcraft_com | 559 | **164** | 152 | 11 | 232 |
| alecmfg_com | 367 | **73** | 12 | 43 | 239 |

For a catalogue maker, 164 of the contract screen's 175 passing groups (94%) also pass products; for a contract shop, 73 of
the products screen's 85 passing groups (86%) also pass contract. Each screen leaks most of the other's population.

Of the "neither" groups, most are `no_candidates` (freehand grounding produced no tag on either side); steelcraft has 26
screened_out only on products / 39 only on contract among them, alecmfg 21 / 10.

### 1a. Ten steelcraft groups passing BOTH, read side by side (seed 42)

Sampled: `gyvmrpsx, g7kzxypv, g1fbtuje, gdixzrbo, gce3xpwt, gbgynksb, g8txr5fh, g7hjnqux, gp63oj6g, g6gp2upp`.
In **all ten**, the PRODUCTS verdict is the right one — these are Steelcraft catalogue lines (Paladin door & frame, A14 Series,
DE Series, DW & K Series drywall frames, FT thermal-break frames, MU Series, B Series steel-stiffened doors...). The CONTRACT
pass is carried every time by a fabricated SCR-2 inference. Representative quotes:

- `g7kzxypv` (focal `'Full  glass architectural entrance doors'`): products SCR-2 quotes the synthesis verbatim
  ("Steelcraft manufactures full glass architectural entrance doors"); contract SCR-2 satisfied with
  *"The synthesis shows that Steelcraft manufactures these doors as a product category for customer order."* — the
  "product category for customer order" clause appears nowhere in the synthesis; it is invented.
- `gp63oj6g` (`A14 Series`): contract SCR-2 satisfied: *"The synthesis describes options, lights, hardware preps, and fire
  rated products, indicating these doors are made to customer/project specification."* Catalogue options read as contract work.
- `g6gp2upp` (`steel stiffened core`): contract SCR-2 satisfied: *"...doors with steel stiffened cores, which are specified
  for customer needs (impact resistance)."* A product feature read as customer direction.

**Within-run instability on the same group:** three of the ten (`g1fbtuje`, `g7hjnqux`, `gp63oj6g`) appear in both chunks and
the contract screen decided **opposite ways on near-identical evidence**. E.g. `g7hjnqux` (MU Series): chunk `0:91562`
contract SCR-2 *satisfied* ("offers multi-use flush frames as a product category for customer order") while chunk
`91562:158790` contract SCR-2 *failed* — correctly — with *"The synthesis describes MU Series multi-use flush frames as a
standard product manufactured by Steelcraft, not that they are made to customer specification or as contract work."*
The rule can fire correctly; whether it does is chance.

### 1b. Fifteen steelcraft contract SCR-2 "satisfied" explanations, read and classified (seed 7, of 219 total)

My classification: **13 circular / 1 standards-compliance-as-evidence / 1 borderline-real.** Far above the ~9% the regex
suggested — the regex only catches one phrasing family; the drifted phrasings are equally circular. All 15 verbatim (group,
tag, explanation):

| # | group / tag | class | SCR-2 explanation (verbatim) |
|---|---|---|---|
| 0 | `gk97t7ce` / knock-down steel door frames | circular | "KD frames are supplied by Steelcraft for various series, indicating they are made to customer order or specification." |
| 1 | `gbubf8pp` / doors | circular | "The doors are manufactured and offered with various features and options, indicating customer-directed work." |
| 2 | `ggsd1wi5` / DE Series double egress frames | circular | "Factory preparation for field-installed silencers is a customer-directed feature." |
| 3 | `gdtm8odl` / square edge flush doors | **standards-compliance** | "Doors are described as being listed for specific compliance requirements, which are customer-driven." (the focal form is `neutral pressure testing (ASTM E152 and UL-10B)`) |
| 4 | `giqbyr4z` / steel doors | circular | "The synthesis refers to these as Steelcraft's products, implying they are made to customer order or specification, as is typical for contract work in this industry." |
| 5 | `ghynixys` / Full Glass doors | circular | "The doors are described as being for 'high-use exterior entrances where large full glass lights are required,' indicating customer-driven specification." |
| 6 | `gkrbgfq8` / masonry door frames | circular | "The frames are described as Steelcraft's masonry frames, indicating they are made to customer order or specification." |
| 7 | `g1fbtuje` / double egress frames | circular | "The frames are specified for particular applications, indicating they are made to customer order/specification." |
| 8 | `g9kdi5qh` / storm resistant doors | circular | "Offering storm resistant doors as a product subcategory implies they are made to customer specification or order." |
| 9 | `ghh68g9x` / doors | **borderline real** | "The synthesis specifies that modifications such as light cutouts and function hole preps on doors are offered, which are typically performed to customer order." (made-to-order modification is genuine customization, though "typically" is still world-knowledge, not the record) |
| 10 | `gj5jxusm` / temperature rise doors | circular | "The doors are described as being offered by Steelcraft, indicating they are made to customer order or specification." |
| 11 | `glz04e0o` / embossed doors | circular | "'Offers' indicates these doors are provided to customer order or specification." |
| 12 | `gdfpjxig` / multi-use flush frames | circular | "Frames are described as being designed for specific installation and technical requirements, which are customer-driven." |
| 13 | `gn2ezvzg` / doors | circular | "Manufacturing and providing doors indicates they are made to customer order or specification." |
| 14 | `gaj63gns` / steel doors | circular | "The phrase 'our steel doors' indicates these are made for customers, showing customer-directed work." |

Corpus-level corroboration (all 219 SCR-2 satisfied in the file): 160 (73%) contain an inference verb
(indicating/implying/suggesting/typically); of the 59 without one, the ones I read are circular by other phrasings — e.g.
*"The synthesis shows that Steelcraft offers storm resistant doors as a product category for customer order."* (the literal
words "product category" used as evidence FOR contract work), *"The phrase 'our steel frames' shows these are made for
customers."* The new boilerplate *"as a product category for customer order"* appears 15× this run, 0× last run — phrasing
drift, same circularity. **Effectively ~90% of steelcraft contract SCR-2 passes are unearned.**

---

## 2. Alecmfg products screen (85 passing groups; 12 read, seed 11)

**Verdict: 12/12 are contract work or bare capability descriptions leaking through; zero genuine catalogue evidence.**

The flagrant ones — the synthesis itself names the client, and SCR-2 still concludes "sells as its own":

- `gr9whppn` (`titanium components`): synthesis — *"a North American medical device client provided Alec Model with detailed
  3D models and specification sheets for a series of titanium components."* This is the textbook SCR-2c fail (made to a
  customer's design/drawing). SCR-2 satisfied: *"The synthesis indicates Alec Model received technical documentation for the
  purpose of manufacturing titanium components, showing Alec Model manufactures and sells them as its own."*
- `gqtfttqp` chunk `96974:127736` (`parts`): synthesis — *"Alec Model manufactures high-precision aluminum parts for a German
  EV client, and titanium alloy parts for a North American medical device client"*. Both tags passed; SCR-2: *"The synthesis
  specifies Alec Model manufactures these parts for clients, indicating Alec Model itself manufactures and sells them."*
  "For clients" cited as evidence of selling as its own.
- `g3tramcs` (`ultra-precision valve body assemblies`): *"a client urgently needed 30 ultra-precision valve body assemblies"*
  → passed.
- `gxw7zsvu` (`aluminum housings`): housings for *"a French UAV client"* → passed.
- `gr4ze85l` (`aerospace-grade battery trays`), `gta3j33f` (`sensor rings`, laparoscopic-arm prototype case study),
  `gvdjybet` (`welded frames`, offshore-skid case study), `gzvsqor5` (`field-facing components` "for long-term partners"),
  `gxensd0m` (`assemblies`, customer testimonials) — all client-project case-study material, all passed.
- Capability-as-product: `gy6pveem` (`plastic parts` — "manufactures plastic parts using injection molding", the arrangement
  unstated; SCR-2b should have withheld), `gcdzrgs3`/`gcct95un` (`prototypes` — a service deliverable; SCR-2 even
  rationalizes *"not ... as custom-only work"* against a synthesis that says "to exact specifications" for clients).

**Mecha Hand:** the string `mecha` (any casing) occurs **nowhere** in either alecmfg dump, nor in the repo's sample scraped
text (`apps/data_etl_app/src/data_etl_app/knowledge/sample_scraped_texts/alecmfg.com.txt` — only "mechanical"). The run's
scraped text covers everything (2 chunks, `0:96974` + `96974:127736` = full trimmed text; nothing truncated by `max_chunks`).
So there is **no Mecha Hand evidence available to the pipeline** — either that line lives on unscraped pages or the premise
is wrong. None of alecmfg's 85 products passes is attributable to it; the passes are pure screen leakage.

---

## 3. Tag quality

### 3a. Steelcraft products: 226 distinct passed tags, 199 (88%) contain door/frame

**15 flush-door-containing tags this run** (last run 14):

`L Series flush doors`, `L Series hollow metal steel flush doors`, `SL Series flush doors`, `SL Series square edge flush
doors`, `SZ Series Falcon flush doors`, `SZ Series square edge flush doors`, `T Series flush doors`, `flush doors` (x21),
`flush doors with top and bottom caps`, `full flush doors`, `hollow metal steel flush doors`, `hurricane rated flush doors`,
`square edge flush doors`, `standard flush doors`, `tornado-resistant flush doors`.

Curator judgment: **12–13 of the 15 merge into `flush doors`.** The seven Series-prefixed tags are SKU naming, not categories;
`full`, `standard`, `with top and bottom caps`, and `hollow metal steel` are construction descriptors of the same category.
Defensible survivors besides the parent: `square edge flush doors` (a real spec distinction) and the two severe-weather
variants (`hurricane rated` / `tornado-resistant`) — though those overlap the separately-passed severe-weather tags. So 15
tags → ~3 curated categories.

The 27 non-door/frame tags are mostly **components and accessories of doors** (`steel face sheets`, `screws`, `anchors`,
`mullions`, `transoms`, `sill sections`, `lead-lining clips`, `stainless steel stiffeners`, `stock package small parts`) —
inputs, not sold products; SCR-1a ("used as an input" does not satisfy) is not being enforced on the products side either.

### 3b. Alecmfg: all 75 distinct passed products tags, non-products flagged

Full list with my flags (S=service/capability, D=documentation/deliverable, G=generic/vacuous, C=one-off client part from a
case study — a contract product, not a catalogue product; unflagged = would be a plausible product category *if* the
sell-as-own evidence existed, which it does not):

- G: `assemblies`, `components`, `end-use parts`, `machined parts`, `metal parts`, `parts`\*, `precision parts`,
  `high-precision parts`, `production parts`, `samples`, `validation units` (\*as tags `high-precision aluminum parts` etc.)
- S: `smooth machining surface finish` (a finish option — group `glst7nct`, SCR-2: "The synthesis attributes the offering of
  this surface finish to Alec Model"), `prototypes`, `functional prototypes`, `SLA/SLS printed prototype`,
  `thermal component prototypes`, `power module prototype components`
- D: `weld maps` (group `g3j37tvk` — synthesis: *"lists 'Weld map' as required documentation"* in a client case study; SCR-2:
  "Alec Model is the party providing weld maps as documentation, indicating it supplies these as its own"),
  `traceable label sets` (group `g4rvsq9g`, same pattern)
- C (client-project parts): `CNC machined aluminum baseplates with threaded inserts`, `EV battery enclosures`,
  `aerospace-grade battery trays`, `aluminum alloy sheet metal housings`, `aluminum alloy sheet metal structural housings`,
  `aluminum baseplates`, `aluminum bracket`, `aluminum brackets`, `aluminum gearbox housings`, `aluminum housings`,
  `aluminum mounting brackets`, `cnc machined aluminum baseplates`, `custom end-effectors`, `custom-machined aluminum
  brackets`, `gripper modules`, `parts for orthopedic surgical guides`, `precision optical sensor rings`, `precision sensor
  rings`, `sensor brackets`, `sensor rings`, `titanium alloy parts`, `titanium components`, `ultra-precision valve body
  assemblies`, `vacuum chamber components for etching equipment`, `welded frames`, `copper heat sinks`, `heat sinks`,
  `heat sink structures`, `thermal support frames`, `thermal systems`, `complex thermal management systems`
- Remainder (`CNC aluminum parts`, `CNC machined parts`, `aerospace components`, `die cast components`, `plastic parts`,
  `nylon parts`, `nickel parts`, `steel plates`, `cylindrical parts`, `finned structures`, `housings`, `mounting brackets`,
  `mounting plates`, `machined slot components`, `components with through-holes`, `components with tolerance-critical
  bores`, `sheet metal components with ventilation slots`, `complex 5-axis components`, `lightweight CNC components`,
  `medium-to-large aluminum parts`, `high-precision metal parts`, `field-facing components`): material/process capability
  descriptions. **Not one of the 75 tags is backed by catalogue-product evidence.** A correct products list for alecmfg is
  empty or near-empty.

---

## 4. Disagreement direction

### Steelcraft — products passed, contract failed (152 groups; 5 read, seed 3)

All five contract rejections are **sound**, e.g.:

- `gj7m55fz` (`doors`, 5-Day Express program): SCR-2 failed — *"The synthesis only describes expedited shipping for select
  doors, not that the doors are made to customer specification, order, or branding. There is no evidence of contract or
  customer-directed work."*
- `gs2v6d9e` (`stick system components`): SCR-2 failed — *"There is no indication that these stick system components are made
  to customer specification or as contract work; it only states that Steelcraft manufactures them for a range of
  requirements."*
- Same sound pattern on `gzx9ch70` (steel face sheets), `gxklblyf` (elevations), `g7ttduh2` (stainless steel components).
  (Side finding: those three products-side passes are component/feature tags that SCR-1a should arguably have stopped.)

### Steelcraft — contract passed, products failed (11 groups; 5 read)

**3 of 5 are not screen disagreements at all**: on `ghnqk1d8`, `g1tgnl9d`, `g0tuadio` the products-side row is
`no_candidates` — freehand grounding, on a byte-identical A/A payload, produced a tag on the contract run and declined on the
products run (the known ~80–90% grounding noise floor). Of the 2 real disagreements:

- `gfc7e10i` (`finish paint colors`): both sides defensible — contract SCR-2: *"Steelcraft will create customized colors to
  complement hollow metal projects"* (genuine customization); products SCR-2 failed: *"options or features provided for
  Steelcraft's products, not a standalone product category"*.
- `gf3f1ec2` (`door preps`): likewise defensible both ways ("a la carte hardware options are available to order door preps
  for hardware not offered" → made-to-order service; products fail calls it "a service or customization, not a product").
- The contract-side passes on the noise-artifact groups are the usual circularity (`ghnqk1d8`: *"The features described
  (square lock and hinge side, epoxy filled interlock edges, etc.) are provided to customer specification."* — catalogue
  construction features).

### Alecmfg — contract passed, products failed (43 groups; 5 read, seed 3)

All five are genuine contract work and **both** screens reason correctly:

- `g7fos1xy` (`aluminum alloy sheet metal structural housing`): products SCR-2 failed — *"The housings were produced for a
  client and for specific testing applications, indicating they were made to client requirements, not as Alec Model's own
  product."* Contract SCR-2 satisfied on the same evidence. **This is the exact evidence pattern (client-directed housings for
  the French UAV client) that the products screen PASSED in `gxw7zsvu` and `gqtfttqp`** — proof the products-side failure is
  per-row nondeterminism, not rule text.
- Same correct双-verdict on `gx2v9lof` / `gvbxfxhv` ("listed as a 'client requirement'"), `gqb67z6x` (helicoils — products:
  "does not establish that Alec Model manufactures and sells helicoils as its own product category"), `gyl7ph01` (thin rib
  structures — products SCR-1 failed: process optimization, no product).

### Alecmfg — products passed, contract failed (12 groups; 5 read)

- 2 of 5 (`gy1tfp30` aluminum brackets, `gxkkr8q3` precision sensor rings) are again **grounding-noise artifacts** (contract
  side `no_candidates`), and both products passes are flagrant leaks — `gy1tfp30`'s own synthesis: *"Alec Model supplied a
  custom-machined aluminum bracket for a U.S. industrial automation client"* → SCR-2 satisfied.
- `ghxs6flt` (`precision-machined components`): contract fail sound; the products pass rests on *"Alec Model is committed to
  becoming a trusted, globally recognized manufacturer of precision-machined components"* — an aspiration statement that
  guard SCR-G1 (aspirational-only) should have caught.
- `gh749siu` (die cast components), `gm5pj38k` (metal parts): capability descriptions; contract SCR-2 fails are sound
  ("does not specify that these are made to customer order or specification"); the products passes ignore SCR-2b (arrangement
  unstated).

**Conclusion for task 4:** rejection reasoning is sound on both screens (10/10 steelcraft-direction-1, 5/5 both alecmfg
directions where a real screen decision exists); acceptance reasoning is where both screens fail. Jointly they do not encode
catalogue-vs-contract — on ~86–94% of each subject's passing set the two screens agree, and the disagreement margin is
substantially grounding noise (5 of the 10 minority-direction groups I read were `no_candidates` on the other side, not
screen verdicts).

---

## 5. Freshness delta vs run 20260824T190359

**The two runs are NOT an identical-prompt pair.** Between them, three product-stack catalogs were republished
(uploaded 2026-08-25T19:43:45Z, the "21 tail prompts"):

- `product_phrase_freehand_grounding` 2026.08.2 → **.3** (pv `ptDyfJ2dWtGG` → `rehZYa3AuSAh`)
- `product_phrase_screening_pure_product` 2026.08.7 → **.8** (pv `zyH0ZHzUOzw8` → `w7sy3FgE_JGO`)
- `product_phrase_screening_contract` 2026.08.7 → **.8** (pv `ZWqrISsYKh26` → `5R7AENoIq5nI`)

The diffs are wording-only: `evidence_source` "the record's mentions and synthesis" → "the record's focal form and synthesis"
(the stale-`mentions` fix), and SCR-2a/3a "never by name" → "the manufacturer in question is NAMED at the top of the request".
Search/mention/synthesis pins and the scraped-text `s3_version_id` (`phz2pjEpuynH`) are identical.

Given that, the tag deltas (steelcraft products 206→209, contract 138→126; alecmfg products 79→74, contract 96→98) are within
what the ~80–90% grounding noise floor alone produces, and nothing in my samples looks like a behavioral change:

- Steelcraft contract SCR-2 satisfied: 233 → 219; inference-verb rate 85% → 73%. Same failure, same magnitude. The only new
  thing is phrasing drift: the boilerplate *"as a product category for customer order"* appears 15× this run, 0× last —
  which is why a fixed regex now undercounts the circularity (my reading: ~87% of a random 15, vs ~9% by regex).
- Alecmfg products SCR-2 satisfied: 91 → 89. The contract-work-as-products leak existed at the same size last run.
- The SCR-2a "NAMED at the top" fix did not change acceptance behavior on either side in any sample I read.

**Verdict: noise plus a benign wording republish; no real behavioral change. The two acceptance-side failures (contract
SCR-2 circularity; products SCR-2 ignoring SCR-2c/2b on client-directed evidence) are stable, pre-existing, and are the
dominant error source in this A/A pair — ahead of grounding nondeterminism, which accounts for about half of the
minority-direction "disagreements".**

---

### Suggested next steps (from the evidence, not built)

1. Contract SCR-2 needs an anti-circularity note mirroring SCR-2b: "that the manufacturer offers, supplies, or describes the
   thing — or lists options for it — does not show it is made to a customer's order; the record must state the customer
   direction." Every one of the 13 circular explanations would fail that test.
2. Products SCR-2c (client-directed ≠ own product) is being ignored in the pass direction; it is currently a non-reportable
   note — consider promoting it to a reportable guard so the model must attest to it per tag.
3. Any split metric on this A/A pair should first drop groups where the twin's row is `no_candidates`, or grounding noise
   pollutes the disagreement measurement.
