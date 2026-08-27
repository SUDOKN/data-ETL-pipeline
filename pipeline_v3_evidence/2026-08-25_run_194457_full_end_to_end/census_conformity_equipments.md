# Behavioral census — `conformity_attestations` + `equipments`, run 20260825T194457

Scope: 4 dumps in `packages/logs/extraction_dumps/20260825T194457/`.
Totals reconciled against the dumps before coding: 318 rows, 170 grounded tag instances,
338 decline strings, 170 screening verdicts.

**Method.** Every item was enumerated by script into compact lines, then read and coded by
hand. Nothing was coded by regex or keyword match. Coverage is 100%: all 170 tag instances,
all 170 screening verdicts, and all 338 decline strings were individually read, except that
104 of the 338 declines are *cascade* declines (the row yielded a tag at the other grounding
stage, so no entity was lost). Those 104 were coded `OK` structurally, from the row's tag
outcome rather than from re-reading each string; that is stated wherever it affects a count.
The remaining 234 decline strings were read individually.

Taxonomy as issued. Grounded tag: one code, severity `F > P > X > B > V > N > D`.
Decline: one code from `OK / LOST / TEMPLATE / TWIN / SCOPE`. Rule outcome on a passed
verdict: `R / C / G / I / M`; on a failed verdict `OK / WRONG`.

**One taxonomy decision worth stating,** because it sets the fabrication number. For
`equipments`, a *process the subject performs* → machine (`Die Casting` → `die casting
machine`) is coded **B (BRIDGE)**: the inference step is unstated but the machine category
is real and the subject demonstrably operates one. A *product the subject sells* → machine
(`T Series` doors → `flush door manufacturing machine`) is coded **F (FABRICATION)**: the
tag names a machine category that exists nowhere in the record and, for most of these
strings, nowhere in industry. That line is what separates the alecmfg control from the
steelcraft regression.

---

## 1. Count tables

### 1.1 Grounded tag instances, by code and dump

| Dump | stage | N tags | D | N | B | V | X | P | F |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|
| steelcraft `equipments` | freehand | 20 | 0 | 0 | 1 | 0 | 0 | 0 | **19** |
| alecmfg `equipments` | freehand | 34 | 6 | 6 | 21 | 0 | 0 | 1 | 0 |
| steelcraft `conformity` | in-vocab | 6 | 5 | 0 | 0 | 0 | 1 | 0 | 0 |
| steelcraft `conformity` | oov | 82 | 61 | 1 | 5 | 5 | 8 | 1 | 1 |
| alecmfg `conformity` | in-vocab | 13 | 5 | 0 | 0 | 1 | 1 | 6 | 0 |
| alecmfg `conformity` | oov | 15 | 4 | 0 | 0 | 0 | 6 | 5 | 0 |
| **All four** | | **170** | **81** | **7** | **27** | **6** | **16** | **13** | **20** |

### 1.2 Declines, by code and dump

| Dump | strings | OK | LOST | TEMPLATE | TWIN | SCOPE |
|---|--:|--:|--:|--:|--:|--:|
| steelcraft `equipments` | 69 | 65 | 0 | 0 | 4 | 0 |
| alecmfg `equipments` | 3 | 3 | 0 | 0 | 0 | 0 |
| steelcraft `conformity` | 212 | 180 | 12 | 14 | 6 | 0 |
| alecmfg `conformity` | 54 | 50 | 4 | 0 | 0 | 0 |
| **All four** | **338** | **298** | **16** | **14** | **10** | **0** |

Steelcraft conformity decline arithmetic, for audit: 140 in-vocab + 72 oov = 212.
73 in-vocab strings are cascade-OK (oov then grounded the row); 5 oov strings are cascade-OK
(in-vocab already grounded); 67 rows declined at both stages = 134 strings. On the 14
TEMPLATE rows only the oov string carries the misfiring argument, so those rows contribute
14 TEMPLATE + 14 OK. 44 fully-correct terminal rows contribute 88 OK. 78 + 14 + 88 = 180 OK.

### 1.3 Screening — per-rule totals

Passed verdicts, rule outcomes:

| Dump | verdicts | passed | failed | SCR-1 R | SCR-1 G | SCR-1 C | SCR-2 R | SCR-2 M |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| steelcraft `equipments` | 20 | 20 | 0 | 1 | 0 | **19** | 1 | **19** |
| alecmfg `equipments` | 34 | 33 | 1 | 32 | 1 | 0 | 32 | 1 |
| steelcraft `conformity` | 88 | 79 | 9 | 72 | 7 | 0 | 72 | 7 |
| alecmfg `conformity` | 28 | 13 | 15 | 9 | 4 | 0 | 13 | 0 |
| **All four** | **170** | **145** | **25** | **114** | **12** | **19** | **118** | **27** |

No `I (INVERTED)` outcomes were found on any passed verdict in these four dumps.

Failed verdicts (25 total): **25 OK, 0 WRONG**. Every screening rejection in these four dumps
was correct. Breakdown: alecmfg conformity 15 (14 of them `SCR-2 failed` — third-party
attribution, exactly the right rule firing); steelcraft conformity 9 (8 X-code tags and 1
V-code tag); alecmfg equipments 1 (`5-axis CNC milling machine` from a client-requirements
table). Screening is the healthiest stage in the run and it is the only thing standing
between the steelcraft equipment fabrications and the database — where it fails completely
(20/20 pass, see §3).

---

## 2. `conformity_attestations` — the definitional-refusal template

The argument "*… which is a standard, not a certification, accreditation, registration, or
compliance attestation*" and its close variants ("*a legal requirement*", "*a reference to
passing a test*", "*only a rating, not a specific compliance or certification*", "*a material
specification*") fire **21 times**, all in steelcraft, all at the oov stage, **0 times in
alecmfg**.

**Split: 14 TEMPLATE (misfire) / 7 OK (correct).**

TEMPLATE — 14 instances. Every one refuses a synthesis that explicitly says the subject
claims compliance, is listed, or is third-party labelled:

| # | gid | focal form | what the synthesis actually said | recovered elsewhere? |
|---|---|---|---|---|
| 1 | `gy3fqrk6` | ICC 500 tornado test | "passes the FEMA 361 and ICC 500 tornado test" | yes → `ICC 500-2014 compliance` |
| 2 | `gwhsll1u` | positive pressure standards (UL-10C) | "**listed for installations requiring compliance to** … UL-10C" | yes → ch1 `UL 10C positive pressure fire test compliance` |
| 3 | `gv188n3y` | ANSI A250.8-2017(SDI-100) | "construction **exceeds** ANSI A250.8-2017" | yes → `gpt9k5hx` |
| 4 | `guhv8wxy` | ANSI UL 10C (Fire) | "**claims compliance with** ANSI UL 10C (Fire)" | yes → `gwhsll1u` ch1 |
| 5 | `gzzyuc5m` | **UL 1784 (Air Leakage)** | "**claims compliance with** UL 1784 (Air Leakage)" | **NO — net loss** |
| 6 | `gwyilzfb` | **ASTM A653** | "**claims compliance with** ASTM A653" | **NO — net loss** |
| 7 | `gyr66wyt` | Intertek…ICC 500-2020 | "**labeled by Intertek** signifying compliance to ICC 500-2020" | yes → `gxqkgqxg` ch1 |
| 8 | `gwt80v25` | ICC 500-2014 | "**labeled by Intertek** … ICC 500-2014" | yes |
| 9 | `gwa1w830` | FEMA 361 (2021) | "**labeled by Intertek** … FEMA 361 (2021)" | yes → `g56nm2k6` |
| 10 | `gwzrhkqo` | International Building Code | "designs its products to meet the requirements of the IBC" | yes → ch1 |
| 11 | `gyie246f` | International Fire Code | "designs its fire doors to meet the … IFC" | yes → ch1 |
| 12 | `geb3di4i` | **positive pressure fire-ratings** | "positive pressure fire-ratings are available … 20 minutes to three hours" | **partial only** |
| 13 | `ggr6w2el` | **Sound-rated to STC 46 / 43** | "its products **are sound-rated to STC 46** on Single and 43 on Pairs" | **NO — net loss** |
| 14 | `gvmqwjxa` | ADA-compliant | "Steelcraft offers **ADA-compliant** glass lights" | yes → ch1 `ADA compliance` |

Instance 7 is the sharpest: a third-party conformity label issued by Intertek is the
paradigm case of a conformity attestation, and the template rejects it as "a standard".

OK — 7 instances, correctly refused: `gzzyl98p` (fire rated configuration — an advisory),
`gje6lt1q` (fire rating — generic capability talk), `gd64cgkv` (fire-resistance-rated frames —
a requirement statement), `g9gmzob3` (temperature rise ratings — explains a rating system),
`gf2pv6vh` (STC rating — explains a rating system), `gxtpos3t` (Type 304 Alloy),
`gldvpidw` (Type 316 Alloy).

### 2.1 Real attestations LOST (zero passing verdict anywhere in the dump)

Lost to the TEMPLATE (4):

1. **UL 1784 (Air Leakage)** — `gzzyuc5m`. *(known loss vs the 20260824T190359 run — confirmed)*
2. **ASTM A653** (galvanized/galvannealed steel; "All Tornado products are Galvanealled") — `gwyilzfb`.
3. **STC 46 / STC 43 sound rating** — `ggr6w2el`, and again at `gly133qp` under a non-template argument.
4. **GRAINTECH positive-pressure fire ratings, 20 min – 3 hr** — `geb3di4i` (partially covered by the generic UL-10C tag).

Lost to the **drop-orphan** path — the in-vocab pass records the term in `dropped_options`,
and the oov pass then refuses it with a self-contradicting sentence (2):

5. **ANSI A250.8-2003 (SDI 100)** — `gunf6db9`. *(known loss vs the previous run — confirmed)*
6. **ANSI A250.6-1997** — `gt20luat`.

Lost to other reasoning (4):

7. **TDI Impact** — `gdw2sfeo`. The *identical* sentence shape for TDI **Non-impact**
   (`g7y5p5ap`, "instructs users to select 'Steelcraft Mfg.' for TDI Non-impact") was grounded
   and passed screening. Same page, same subsection, opposite outcome.
8. **TAS 201** — `gprjb7bo`, described under the heading "Approvals and specified industry standards".
9. **TAS 202** — `gwfr852u`, same heading. (TAS 203 was grounded but then screened out, so the whole TAS family is absent.)
10. **ISO 9001** — alecmfg, `g5zt67xs`. See §5; this is the single worst loss in the four dumps.

### 2.2 Near-duplicate census

steelcraft conformity: **88 tag instances → 78 distinct strings → ~40 distinct real attestations.**

| family | tag instances | distinct strings | distinct real things |
|---|--:|--:|--:|
| FEMA (P-320 / P-361 / "tornado shelter standards" / 320-361 / Intertek-labelled variants) | 12 | 11 | 2 |
| ANSI/SDI A250.x (13, 8-2017, 8, 6-2003, 6, 10-2011, 10, 4, 3-2007, 3, SDI/A250.4) | 12 | 11 | 7 |
| ICC 500 (2014 ×3 spellings, 2020, Label, Intertek-labelled) | 6 | 6 | 2 |
| Steel Door Institute (Certification ×2, certification, membership, SDI Membership) | 5 | 4 | 2 |
| Intertek / UL (UL Product Certification ×3, Intertek Listing and Labeling, Intertek (WHI) fire door labeling, + 2 Intertek-labelled-for-X) | 7 | 5 | 2 |
| UL-10B (`compliance` + `Compliance` ×2) | 3 | 2 | 1 |
| ASTM E152 (`compliance` + `Compliance` ×2) | 3 | 2 | 1 |
| UL-10C (`UL 10C positive pressure fire test compliance`, `USL-10C Compliance`) | 2 | 2 | 1 |
| ASTM E330 (`ASTM E330 compliance`, `ANSI/ASTM E330 compliance`) | 2 | 2 | 1 |
| LEED | 3 | 1 | 1 |
| ASTM A666, Prop 65, NFPA 80, Miami-Dade NOA, ANSI/DHI A115 (each a case/wording pair) | 11 | 8 | 5 |

Pure case-only duplicates (`X compliance` vs `X Compliance`, chunk 0 vs chunk 1): **7 pairs.**
The chunk-0 pass lowercases the trailing noun, the chunk-1 pass capitalises it — a systematic
per-chunk stylistic split, not a semantic difference.

alecmfg conformity: 28 instances → 20 distinct strings → **6 distinct real attestations**
(IATF 16949, ISO 13485, ISO 14001, REACH, ISO 10993-5, Class 100,000 cleanroom).
Repeats: `ISO 9001` ×3, `ISO 13485` ×2, `ISO 14001` ×2, `IATF 16949` ×2, `RoHS compliance` ×2,
`REACH compliance` ×2, `ISO 10993-5 compliance` ×2, plus `ASTM B221` / `ASTM B221 material standard`.

---

## 3. `equipments` — the exact fabrication count

### 3.1 steelcraft — 19 of 20 tag instances are fabrications (95%)

The equipment search returned products: **68 of the 75 distinct group_ids in the equipment
dump also appear in the products dump**, and **12 of the 15 grounded equipment rows are
product groups**. Grounding then converted the product into a machine via an explicit
"…implying the use of machines to manufacture X" step, and screening passed **20 of 20**.

**FABRICATION count: 19 tag instances across 14 rows, 9 distinct fabricated categories.**

| fabricated tag | instances | group_ids |
|---|--:|---|
| `flush door manufacturing machine` | 8 | `gdxy3axk` `gbubf8pp` `gec77azf` `g97lpxgl` `gbf5c0f9` `ged4kjbr` `gf47v6x5` `gexjsgp9` |
| `hollow metal door manufacturing machine` | 2 | `g04h532n` `g92f1qbb` |
| `stainless steel door manufacturing machine` | 2 | `g31tt6mn` `gbf5c0f9` |
| `hurricane door manufacturing machine` | 2 | `g400k534` `g6on13ul` |
| `entrance door manufacturing machine` | 1 | `gd10mzgh` |
| `stile and rail door manufacturing machine` | 1 | `gbf5c0f9` |
| `severe weather door manufacturing machine` | 1 | `gbf5c0f9` |
| `acoustical door manufacturing machine` | 1 | `gbf5c0f9` |
| `blast resistant door manufacturing machine` | 1 | `gbf5c0f9` |
| **total F** | **19** | |

The single non-fabrication is `engraining and staining machine` (`gfv70csp`, code **B**),
derived from a genuine Steelcraft *process* — "its exclusive engraining and staining process"
— and it is the only row in the dump a curator should keep.

One row, `gbf5c0f9`, produced **six fabrications from one sentence** by splitting a product
list into six parallel "X door manufacturing machine" categories, and `FGR-Q3` (the
no-duplicate rule) was satisfied six times on the grounds that each is "distinct from other
door manufacturing machines listed".

Comparison to 20260824T190359: that run declined 74 of 75 with "Doors are finished goods,
not production machines". This run declines 69 and grounds 15 rows into 19 fabrications and
1 bridge. The regression is real and is located in the freehand grounding prompt, not in
search (search was already returning products in both runs).

### 3.2 alecmfg — the healthy control, 0 fabrications

34 tag instances: **6 D, 6 N, 21 B, 1 P, 0 F.** The record names actual machines in six
cases (`CNC machines`, `lathes`, `SLS and SLA rapid prototyping machines`, `vertical
machining centers`, `HS-V55LS models`, `double-column CNC center`) and the rest are
process→machine conversions of processes alecmfg genuinely performs. 33 of 34 passed
screening; the one rejection (`g0xp0zkc`, `5-axis CNC milling machine`) was correct — the
record says outright "The entry does not show Alec Model's own dealing with 5-axis CNC
milling, but rather lists it as a client requirement."

**Duplicate sprawl on the healthy side, though:** `CNC machining center` fires **8 times**
across 8 different groups (`grxyx2pu` ×2 chunks, `gtf9bvz0`, `golfnj4j`, `glp6gvmo`,
`gdouo7gb`, `g8hs76tb`, `g5ukjsnf`), and the machining-centre concept is split across six
near-synonymous tags: `CNC machining center`, `multi-axis CNC machining center`, `5-axis CNC
machining center`, `5-axis machining center`, `5-axis simultaneous CNC machining center`,
`double-column CNC machining center`. `FGR-Q3` only checks for duplicates *within* a record,
so it can never catch this. 34 instances → 23 distinct strings → ~13 distinct machine categories.

---

## 4. Twin disagreements

Rows whose group_id appears in both chunks and whose outcome differs.

**steelcraft `equipments`** — 9 group_ids in both chunks, **4 disagree**, all in the same
direction: chunk 0 fabricated, chunk 1 correctly declined.

| gid | chunk 0 | chunk 1 |
|---|---|---|
| `g31tt6mn` | `stainless steel door manufacturing machine` | DECLINED — "a finished door product, not a production machine" |
| `gbubf8pp` | `flush door manufacturing machine` | DECLINED |
| `gd10mzgh` | `entrance door manufacturing machine` | DECLINED |
| `gec77azf` | `flush door manufacturing machine` | DECLINED |

In all four the **decline is the correct answer** and the grounding is the fabrication. The
same group, given a slightly shorter chunk-1 synthesis, flips to the right verdict — which
locates the failure in the grounding prompt's sensitivity to synthesis verbosity, not in the
record.

**alecmfg `equipments`** — 2 group_ids in both chunks, **0 disagree**.

**steelcraft `conformity`** — 24 group_ids in both chunks, **17 disagree**, of three kinds:

- *Ground vs decline* (9): `gshm03um`, `gvmqwjxa`, `gpt9k5hx`, `gqb6u9sj`, `gwhsll1u`,
  `gwzrhkqo`, `gxqkgqxg`, `gyie246f`, `gh8ye3mm` (the last is an artifact — the chunk-0 row
  is an empty `no_mentions` row).
- *Case / wording only, same entity* (7): `gcwytcti`, `gvq3fx4u`, `g9g20hgj`, `gdtm8odl`,
  `gdt7tk74`, `g0x2ae26`, `g1oiy03h`. Chunk 0 emits `… compliance`, chunk 1 emits
  `… Compliance`. Every one of these becomes a duplicate row downstream.
- *Genuine facet split* (1): `gtuz1auj` → `Steel Door Institute Certification` (ch0) vs
  `Steel Door Institute membership` (ch1). Both are true; neither chunk got both.

**alecmfg `conformity`** — 0 group_ids appear in both chunks, so 0 disagreements.

---

## 5. The ten most consequential errors

1. **ISO 9001 is absent from alecmfg entirely.** `g5zt67xs` (ch1), synthesis: *"ISO 9001:2015
   is listed in the 'Certification Statistics' section under the heading about Alec Model
   obtaining ISO 13485 and ISO 14001 certifications."* Declined: *"Listing a standard in
   statistics without stating acquisition or compliance does not evidence certification."*
   The three other `ISO 9001` tags (`gpcrxlbl`, `gd2y03ba`, `ggj60n3y`) are all the **client's**
   ISO 9001 and were all correctly screened out. Net result: alecmfg's flagship certification
   has **zero passing verdicts** in the dump.

2. **A self-refuting decline that argues itself into the opposite conclusion and declines
   anyway.** `gutmbbfq` (REACH), oov decline, verbatim: *"However, the prompt only asks for
   what the vocabulary misses, and REACH is not in the vocabulary, so it should be identified.
   However, since the instructions say to only return what the vocabulary misses, and REACH is
   not in the vocabulary, it qualifies. **Correction: REACH should be returned as a candidate.**"*
   — and the field is still a decline string, not a tag.

3. **The drop-orphan path loses five real ANSI standards.** `gunf6db9`, in-vocab:
   *"No vocabulary option was chosen: this pass discarded 'ANSI A250.8-2003 (SDI 100)'"*, with
   `dropped_options: ['ANSI A250.8-2003 (SDI 100)']`. The oov pass then says:
   *"this is not out-of-vocabulary; it is not present in the provided vocabulary, but the task
   is to identify only what is not already covered"* — a sentence that contradicts itself in
   its first clause. Identical text at `gt20luat`, `gpt9k5hx` ch1, `gqb6u9sj` ch1, `gv188n3y` ch1.
   8 in-vocab drops in steelcraft, 3 rescued by oov, **5 orphaned**.

4. **19 fabricated machines pass screening 20/20.** `gbf5c0f9` alone yields six. `SCR-1`
   verbatim: *"Offering a product of this type implies substantive activity related to its
   manufacture, which involves the use of stainless steel door manufacturing machines"* —
   the screening rule's evidence is the grounding's own inference (coded C, 19×).

5. **A third-party conformity label rejected as "a standard".** `gyr66wyt`: the synthesis says
   frames *"are labeled by Intertek signifying compliance to ICC 500-2020"*; the decline says
   *"this is a standard, not a certification, accreditation, registration, or compliance
   attestation."*

6. **`Intertek` → `UL Product Certification`.** `ghktbo72`, in-vocab. The group's focal form is
   *Intertek*; the in-vocab pass emits `UL Product Certification`, justified by
   *"'listed and labeled by UL' evidences UL Product Certification"* — reading past its own
   focal form to the other body named in the sentence. The oov pass on the same row then
   correctly emits `Intertek Listing and Labeling`, so the row carries two bodies' certifications
   under an Intertek identity.

7. **A source typo becomes a standard.** `gj48ol44` → `USL-10C Compliance`, with
   `OGR-K1: "USL-10C is a named standard for fire testing"`. No such standard exists; the
   source text says "USL-10C" where it means UL-10C. Nothing in the rule chain normalises it,
   and both screening rules pass it.

8. **The same sentence grounded and declined in the same chunk.** *"steel is the only readily
   available door material that passes the FEMA 361 and ICC 500 tornado test"* →
   `gevpuivl` grounds `FEMA 361 compliance` (passed), `gy3fqrk6` declines *"this is a reference
   to passing a test, not a certification"*. Neither is defensible on its own terms: the claim
   is about steel as a material class, not about Steelcraft's products.

9. **TDI Impact lost while TDI Non-impact is kept.** `g7y5p5ap` ("select 'Steelcraft Mfg.' for
   TDI Non-impact") → grounded, passed. `gdw2sfeo` ("select 'Steelcraft Mfg.' for TDI Impact")
   → declined. Same page, same subsection, same sentence template.

10. **Material specs treated as conformity attestations in alecmfg, correctly refused in
    steelcraft.** `gfo1ehdm`/`gre2hbvl` → `ASTM B221`, `g0pakh0c` → `ASTM A36 material
    standard`, all three passed screening. In steelcraft the identical shape is refused:
    `gxtpos3t` *"This is not a certification … but a material specification."* The two subjects
    got opposite policies in the same run. `g4s7rmka` → `D-U-N-S Number registration` is the
    same axis error (a business identifier, not a conformity attestation) and also passed.

---

## 6. Unexpected behaviours the taxonomy does not name

1. **Self-refuting decline** — the decline text reasons to the opposite conclusion and still
   declines. **2 instances**: `gutmbbfq` ("Correction: REACH should be returned as a
   candidate"), and the 5 drop-orphan oov strings sharing "this is not out-of-vocabulary; it
   is not present in the provided vocabulary" (`gunf6db9`, `gt20luat`, `gpt9k5hx` ch1,
   `gqb6u9sj` ch1, `gv188n3y` ch1) — 6 strings across 2 distinct malformations.

2. **Per-chunk casing split** — chunk 0 emits `X compliance`, chunk 1 emits `X Compliance`, for
   the same entity from the same group. **7 group-level pairs** in steelcraft conformity
   (`gcwytcti`, `gvq3fx4u`, `g9g20hgj`, `gdtm8odl`, `gdt7tk74`, `g0x2ae26`, `g1oiy03h`), each
   producing a spurious duplicate.

3. **Empty carrier rows** — rows with `focal_form: None`, `mention_count: 0`, empty synthesis
   and `status: no_mentions`, occupying a row slot and a group_id. **12 instances**: 8 in
   steelcraft conformity (`gf81aaop` ×2, `gsvyao6z`, `gh8ye3mm`, `gtrr9wd7` ×2, `gk0kbqa7`,
   `gewjdk7y`), 4 in alecmfg conformity. One of them (`gh8ye3mm`) registers as a twin
   disagreement purely because its other chunk grounded.

4. **Focal-form / tag identity crossover** — the tag names an entity other than the group's own
   focal form, read from a neighbouring clause. **1 instance**: `ghktbo72` (`Intertek` →
   `UL Product Certification`). This defeats group identity: the group can no longer be keyed
   by what it is about.

5. **Source-typo laundering** — a malformed standard designation in the source is emitted as a
   tag and then *justified* by a rule explanation asserting it is a recognised standard.
   **1 instance**: `USL-10C Compliance` (`gj48ol44`).

6. **Cross-record duplicate blindness** — `FGR-Q3` / `OGR-N1` check only *within* a record, so
   the same category re-fires from every group that mentions it. Worst case
   `CNC machining center` **×8** (alecmfg equipments, 8 distinct groups); in steelcraft
   conformity the FEMA family fires **12 times** for **2** real standards.

7. **Cross-subject policy divergence in one run** — material designations are conformity
   attestations for alecmfg (`ASTM B221`, `ASTM A36`, 3 tags, all passed) and are not for
   steelcraft (`Type 304 Alloy`, `Type 316 Alloy`, `EN AW-7475-T7351`, all declined). Same
   prompt, same run, opposite policy. **3 tags vs 3 declines.**

8. **Grounding from a client-requirements table** — the record explicitly disclaims the
   subject's own dealing and the grounder tags anyway, leaving screening as the only defence.
   **13 instances** (12 alecmfg conformity P-coded tags + `g0xp0zkc` in alecmfg equipments).
   Screening caught 12 of 13. It caught 0 of 19 on steelcraft equipments.

9. **Verbosity-sensitive flipping** — the four steelcraft equipment twin disagreements all flip
   from fabrication (long chunk-0 synthesis) to correct decline (short chunk-1 synthesis) with
   no change in the underlying evidence. **4 instances.**

---

## 7. Curator-keep estimate per dump

| Dump | tag instances | keep as-is | keep after dedup | keep rate |
|---|--:|--:|--:|--:|
| steelcraft `equipments` | 20 | **1** (`engraining and staining machine`, `gfv70csp`) | 1 | **5%** |
| alecmfg `equipments` | 34 | 33 | **~13** distinct machine categories | 97% correct / 38% distinct |
| steelcraft `conformity` | 88 | 67 | **~40** distinct real attestations | 76% correct / 45% distinct |
| alecmfg `conformity` | 28 | 9 | **6** distinct real attestations | 32% correct / 21% distinct |

Reasoning. *steelcraft equipments*: 19 of 20 are fabricated machine categories that a curator
must delete; the field's true answer for this subject is close to empty and the previous run
was nearly right. *alecmfg equipments*: substantively correct throughout, but 34 instances
collapse to roughly 13 categories once the six machining-centre synonyms and the eight
`CNC machining center` repeats are merged. *steelcraft conformity*: subtract the 9 X-coded and
5 V-coded tags, the 1 P (`ANSI/BHMA A156.3 2014` — a Von Duprin hardware standard), the 1 F
(`USL-10C`), and the 5 B-coded regulatory-requirement bridges (NFPA 80 ×2, IFC, FEMA 361,
ICC 500 Label) → ~67 defensible instances, ~40 distinct after merging the FEMA, ICC 500,
A250.x, SDI and Intertek/UL families and the 7 casing pairs. *alecmfg conformity*: only 9 of
28 name an attestation alecmfg itself holds, and those 9 are 6 distinct things — and the list
is missing ISO 9001 and ITAR entirely.

The two fields fail in opposite directions and both failures are recoverable at the prompt
layer. `equipments` on steelcraft is a **precision collapse** caused by one licensed inference
("offering product X implies machines that make X") that grounding applies and screening
mirrors rather than tests. `conformity_attestations` is a **recall collapse** caused by two
independent refusal paths — the "standard, not a certification" template (14 misfires, 4 net
losses) and the drop-orphan hand-off between the in-vocab and oov passes (5 orphans, 2 net
losses) — plus a duplicate layer that roughly doubles the row count without adding facts.
