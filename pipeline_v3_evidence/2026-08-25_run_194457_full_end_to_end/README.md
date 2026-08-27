# Run 20260825T194457 — the first COMPLETED end-to-end v3 run

Subjects: `steelcraft.com` (727.8s), `alecmfg.com` (683.4s). **Both completed all
7 multi-stage fields** — the first time either subject has finished. 14 dumps,
2,780 groups. Log: `apps/data_etl_app/src/data_etl_app/scripts/latest_extraction_logs.txt`
(15,811 lines).

Reproduce: `python3 pipeline_v3_evidence/2026-08-25_run_194457_full_end_to_end/analyze_run.py`
(output committed as `analyze_run_output.txt`). Record-level eyeball reviews (4
subagent reports, each independently verified against the dumps):
`eyeball_steelcraft_equipments_conformity.md`, `eyeball_steelcraft_process_material.md`,
`eyeball_alecmfg_tail_fields.md`, `eyeball_products_contract_split.md`.

## What was live

Tail only, as designed: initial/OOV/freehand grounding, recursive tagging and
screening created 2026-08-25T19:44+ under exactly the **21 published pv ids**
(2 freehand, 4 initial, 4 OOV, 4 recursive, 7 screening; one pv per
(field, stage) everywhere). Search replayed from 08-23T04:45, mention
collection + synthesis from 08-24T02:07. Live tail cost **$10.36** of the
$18.84 the by-stage rollup shows at gpt-4.1 list rates (the rest was billed on
earlier runs). Live wall-clock 23.5 min for both subjects.

Note on dump timing metadata: every request's `created_at` is the orchestrator
run-start (19:44:57.791 for steelcraft), so `turnaround_seconds` measures
"since run start", not latency — `client_latency_ms` is the real per-request
number (the retry: 1,550 ms). `run.time_span` spans back to the oldest
replayed request (~63h); the sweep summary's per-subject seconds are the real
durations.

## The three 2026-08-25 fixes, verified live

### B1 — dropped_options: fired 56 times, saved the run, zero losses to the OOV net

Both of run 190359's fatal signatures happened AGAIN and were absorbed:
steelcraft process_caps chose `Testing` (g21dwglm, same record), alecmfg
conformity chose `RoHS`/`REACH`/`ISO 10993-5`. 56 drops across 6 dumps
(53 rows after chunk-dedup; a few rows dropped 2–3 options), every one
WARNING-logged with record id.
Classification against the ontology (`analyze_run.py` §3):

- **53 correct** — the option genuinely isn't a vocabulary label. The big
  clusters are real ontology gaps: the process vocabulary (521 labels) has NO
  generic `Inspection` (13 drops), `Testing` (10), `Welding` (3),
  `Cutting`/`Manufacturing`/`Prototyping` — only leaf types like `Spot
  Welding`, `Plasma Cutting`. The certificates vocabulary (58 labels) lacks
  `RoHS`, `REACH`, ISO and every ANSI/SDI standard; materials lacks
  `Polystyrene`, `Fiberglass`, `Magnesium`, `Zinc`; industries lacks
  `Industrial Automation`.
- **3 FALSE drops** (steelcraft process_caps): the model echoed the menu's
  DISPLAY form — `Machining (also: Material Removal Process, Subtractive
  Process, subtractive manufacturing)` ×2, `Extruding (also: Extrusion)` ×1 —
  and exact-match missed the bare vocab name. This is the known OPTION-axis
  echo hole (see memory `echo-surfaces-and-phrase-indexing`), now with a
  measured live cost: 2 of the 3 records kept nothing in-vocab, and the
  `extrusion` record's OOV also declined, so a true `Extruding` grounding was
  fully lost. Fix: strip a trailing `(also: …)` before the vocabulary
  membership test (or match display forms), keep the drop only if the bare
  label still misses.
- **The OOV net catches nearly everything dropped** (§4): of the 53 dropped
  rows, 45 got an OOV concept from the same record — 33 by direct name match,
  the rest as sensible coinages (`insert torque tests`→`torque testing`,
  `Large Missile Impacts`→`Missile Impact Testing`, `quality
  control`→`Quality Control`). 8 declined in OOV too: alecmfg `welding` (a
  real loss — the record evidences welding), steelcraft `extrusion` (the
  false-drop row, fully lost), 5 steelcraft ANSI-standard variants (their
  near-identical siblings got "…compliance" coinages — the A/A noise, not a
  systematic hole), and `Fiberglass` on a record whose drop was itself dubious
  evidence.
- The explanation-synthesis trap fix held: every drop-to-zero row carries a
  valid declination naming the drops; no entry validator failures.

### The retry port — FIRST LIVE FIRING, clean

`steelcraft.com>material_caps>llm_phrase_oov_grounding>retry>1>group>0>chunk>91562:158790`:
the model answered 45 of 46 records in a group request; the node embedded 1
retry, dispatched it, got the answer in 1.5s, and the pass completed ("1 of 46
record(s) came back unanswered; embedding 1 retry request(s)"). This is the
machinery `29d2167` + the 2026-08-25 completion built — never before seen live
(190359 had zero retries). A1's specific parse-error-hold path was NOT
exercised (zero parse failures in the whole run) — still only unit-tested.

### The mentions prompt fix — landed, but see the equipment regression

Explanations citing a `mention(s)` evidence base fell **62.5% → 4.4%**
(and the remaining 4.4% is ordinary English — "the synthesis mentions…",
"the vocabulary does not mention…" — not the stale field). 84.1% now cite the
focal form / synthesis explicitly. The promise and the delivery finally match.

## NEW FINDING — the equipments E1 anchor regression (steelcraft 1 → 11 grounded)

Same upstream records (replayed, identical group_ids), new tail prompt:
steelcraft equipments went from 1/75 grounded (190359 — grounding correctly
declined 74 product-shaped groups) to **11/75 grounded, 9 fabricated tags**.
Every new grounding is a product record inflated through an inference bridge,
verbatim in the explanations:

    "Steelcraft manufactures and offers a full line of hollow metal steel
     doors. This implies the use of machines that manufacture hollow metal
     doors."  → tag `hollow metal door manufacturing machine`

`flush door manufacturing machine` (×7 groups), `hurricane door manufacturing
machine`, `stainless steel door manufacturing machine`, … — none of these
machines is named anywhere in the text; the tag is an invented category. All
11 passed screening (screening judges the relation, and the relation IS
implied — it cannot rescue an invented entity).

Mechanism: the mentions fix changed FGR-E1's anchor from "named, described, or
clearly implied by **what the mentions say**" to "…clearly implied by **the
record's focal form and synthesis**". The "clearly implied" escape hatch
existed before, but anchored to verbatim mention snippets; anchored to an
abstractive synthesis ("Steelcraft manufactures X"), it licenses
product⇒machinery existence inference. 1→11 with one systematic pattern is
far outside the decline-flip noise.

The control: **alecmfg equipments is excellent** (30/34 grounded, 22 tags) —
`Die Casting`→`die casting machine`, `CNC Turning`→`CNC lathe`,
`'HS-V55LS models'`→`vertical machining center`. Process→machine conversion
(the shop's own named activity) is sound; product→machine is fabrication. The
fix should make FGR-E1 for equipments require the equipment be named or
described as something the subject operates/owns/uses — and explicitly forbid
inferring machinery from the products it sells.

## Re-measured findings from 190359

| finding | 190359 | 194457 | verdict |
|---|---|---|---|
| freehand A/A reproducibility (steelcraft) | 77.7% | 81.0% (43 decline-flips) | unchanged noise floor |
| freehand A/A reproducibility (alecmfg) | 90.1% | 86.7% (31 flips) | unchanged |
| steelcraft product tags (near-dup explosion) | 206 | 209, 92% door/frame, 14 "flush door" variants | unchanged, unaddressed |
| equipments group overlap with products | 68/75 | 68/75 (replayed, by construction) | unaddressed upstream |
| SCR-2 circular on steelcraft (regex) | 24% | 9% by regex — but hand-reading 15 gives **~87% circular**; the boilerplate drifted to "product category for customer order" (15× this run, 0× last), which the regex misses | WORSE than the regex says; regex retired as a measure |
| SCR-2 circular on alecmfg (regex) | 3% | 11% regex; hand-read: products-screen SCR-2 inverts evidence (see below) | the subject-shape claim from 190359 was an artifact of the regex |
| mean mentions per group | 2.12 (2,104 grps) | 2.27 (2,780 grps, all fields) | thickness unchanged (~67% single-mention) |
| own-name lint discriminates | 96–100% | 84–100% | dead; retire or re-threshold |

## Funnel (full table in analyze_run_output.txt §2)

Never-before-seen fields completed: steelcraft process_caps 165→116 grounded→83
tags, material_caps 85→53→24; alecmfg conformity 44→12→10, industries
110→67→35, process_caps 250→179→123, material_caps 81→55→33. steelcraft
conformity_attestations grew 32→61 final tags. Record-level quality verdicts
for all of these live in the four eyeball reports.

## The steelcraft conformity 32 → 61 growth, decomposed

Only 8 tags are shared verbatim with 190359 — but ~20 of the 24 "lost" old
tags are re-worded twins of new ones (`ADA Compliance`→`ADA compliance`,
`Miami-Dade County Notice of Acceptance (NOA) Hurricane Approval`→`Miami-Dade
County NOA hurricane approval`, `TDI Non-impact`→`TDI Non-impact compliance`).
The growth is roughly half **real recall gain** (7 ASTM standards, NFPA 80,
ANSI/BHMA A156.3, ANSI A115, HVHZ, Florida Building Code, `Steel Door
Institute membership`…) and half **coinage churn**: the new set carries 8–9
FEMA variants for two real concepts (FEMA P-320, P-361), 3 ICC 500 variants,
~10 ANSI A250.x variants. One apparent real loss: `UL 1784 compliance`
(present in 190359, absent now). OOV coins its own tag wording per record;
nothing normalizes across records or runs — this is finding "grounded tags are
never normalized" wearing its conformity costume, and it dominates the
field's output quality.
## Own-eyes spot checks (Claude, this session)

- **steelcraft industries** (57 grounded → 38 tags): mostly plausible served
  industries (commercial/institutional construction, healthcare, education,
  corrections, hospitality). Two diseases: coinage sprawl (Education /
  Educational Facilities / Primary and Secondary Education / Higher Education /
  Education Construction / Student Housing all coexist; Institutional ×3;
  Correctional ×2) and loose in-vocab picks that echo the M2 axis-mismatch
  pattern — `Industrial Machinery and Equipment` ← "commercial and industrial
  buildings", `Manufacturing` ← "the United States", `Healthcare and Medical
  Devices` ← "hospitals". `Commercial Construction` ×34 is correctly the
  dominant tag.
- **alecmfg equipments** (30/34 grounded, 22 tags): the healthiest field in
  the run. Process→machine conversions are sound (`Die Casting`→`die casting
  machine`, `CNC Turning`→`CNC lathe`) and a model number resolves to its
  category (`HS-V55LS models`→`vertical machining center`). Residual
  near-dup tags: `5-axis CNC machining center` / `5-axis machining center` /
  `multi-axis CNC machining center` / `5-axis simultaneous CNC machining
  center`.
- **products/contract both-pass overlap**: steelcraft 302 pass products, 164
  pass contract, **151 pass both** (was 153 in 190359 — stable); alecmfg 85 /
  116 / **73 both**. The two screens still do not partition catalogue vs
  contract work; they mostly co-admit.
- **material_caps OOV normalization glimpse** (steelcraft): `'304'` and
  `'Type 304 Alloy'` both → `Type 304 Stainless Steel` — OOV coinage CAN
  normalize when the concept has a canonical name; the churn happens where no
  canonical name exists (FEMA/ANSI "compliance" phrasings).

## Agent report 1 of 4 — products/contract split (verified)

Full report: `eyeball_products_contract_split.md`. Claims I re-verified
verbatim against the dumps before accepting:

- **SCR-2 circularity is ~87%, not 9%.** The 9% was regex myopia: this run's
  dominant boilerplate is "manufactures these doors as a product category for
  customer order" (15×, 0× in 190359), which the old circular-marker missed.
  Of 15 hand-read SCR-2 satisfactions on steelcraft: 13 circular, 1
  standards-as-evidence, 1 borderline-real (made-to-order light cutouts).
- **The products screen inverts evidence on alecmfg.** gr9whppn synthesis:
  "client provided Alec Model with detailed 3D models and specification
  sheets for a series of titanium components" → products SCR-2: "showing Alec
  Model manufactures and sells them as its own" (verified verbatim). 12/12
  sampled alecmfg products passes are contract work or capability text; the
  correct alecmfg catalogue-products list is ≈ empty ("Mecha Hand" never
  appears in the scraped text — only "mechanical…" substrings).
- **Rejections are sound on both screens in every sample; acceptance is where
  both fail.** The products screen rejects client-spec housings in one group
  ("made to client requirements, not as Alec Model's own product") and passes
  the identical evidence pattern in two others — grounding/screening
  nondeterminism on the acceptance side.
- 94% of steelcraft's contract passes and 86% of alecmfg's products passes
  are leakage from the other field. ~5 of 10 minority-direction disagreements
  were not screen decisions at all — one arm's freehand grounding had
  declined (`no_candidates`), the A/A noise again.
- Steelcraft flush-door tags: 15 variants, a curator merges 12–13.
- Screening catalogs went .7→.8 in the republish (evidence_source + a "NAMED
  at the top" note); SCR-2 satisfied counts moved 233→219 / 91→89 — wording
  churn, no behavioral change on the failure modes.

(The agent's group-level split counts differ slightly from §"own-eyes"
overlap numbers — 164/152/11 vs 151/151/13 — from a methodology difference in
what counts as a pass; direction and magnitude identical.)

## Agent report 2 of 4 — steelcraft equipments + conformity (verified)

Full report: `eyeball_steelcraft_equipments_conformity.md`. Verified claims:

- **Equipments regression, refined: 15 grounded rows, 14 leaked products, 1
  genuine** (gfv70csp, the only group the old run grounded too). My earlier
  11/9 undercounted because of a dedup artifact — see "per-chunk twins" below.
  9 fabricated `<door type> manufacturing machine` tags; one marketing list
  (gbf5c0f9) alone yields 6. Screening mirrors the same inference and passes
  everything. Old run on identical group_ids: 74/75 declined.
- **Conformity growth re-judged at row level: 36→71 distinct tags — ≈38 real,
  29 duplicates, 4 non-attestations.** Distinct real-world concepts ~29→~40:
  half genuine recall (the flipped declines are overwhelmingly real standards
  the old run missed), half string duplication. FEMA family: 10 tags for 2
  standards.
- **A new decline template kills real attestations**: "…which is a standard,
  not a certification, accreditation, registration, or compliance
  attestation" — 10 row-instances (verified), firing even against a synthesis
  that says "Steelcraft **claims compliance** with ANSI UL 10C". Cost: UL 1784
  and ANSI A250.8-2003, both present in 190359, lost this run.
- Windstorm test family (TAS 201/202/203, ASTM E1886/E1996) entirely absent —
  declined as reference-only per the syntheses; plausible real gap to revisit.

## Agent report 3 of 4 — steelcraft process_caps + material_caps (verified)

Full report: `eyeball_steelcraft_process_material.md`. Verified claims:

- **OGR-N1 outcomes are recorded but NOT enforced** (verified verbatim,
  gwjvjhxy): the OOV tag `Stainless Steel Stiffener` carries `OGR-N1 failed`
  ("does not constitute a new material distinct…") yet the tag was emitted,
  screened (SCR-1/2 satisfied) and survives to final output. Also seen twice
  in process_caps. A failed novelty rule should gate the tag — code fix.
- **Descent is unscreened and bridges on world knowledge** (verified,
  grmn3z4h): screening killed the top-level `Plastic` tag (comparison
  material — the site touts REPLACING "traditional vinyl separators") but the
  recursive tree under the passing `Polymer` tag still descends via
  "Vinyl is a well-known type of plastic (polyvinyl chloride, PVC)" — an
  RGR-E1 world-knowledge hop on negative evidence. Every Painting→Wet
  Painting hop (8 groups) inserts an unevidenced "liquid". 4/8 sampled
  lineages are bridges; 4/8 are sound. The M2 axis-mismatch leak
  (memory: `m2-axis-mismatch-hypernym-leak`) is alive inside descent, and
  screening never sees descendants.
- **Process tags are flooded with lab/installation/product-noun activity**:
  15 testing tags mostly certification-lab work (`Product Testing` from
  "tested and Intertek labeled"), Field Assembly/Field modification are
  installation-site, ~15 product-feature nouns recast as processes. Core
  welding/painting/lamination/modification tags are correct. material_caps
  is largely correct (4 wrong of 26: `Wood` from customer wall studs,
  `Chemicals`, `Ceramic` (chunk-split verdict), the stiffener above).
- **Screening favors ad-hoc OOV tags over ontology-anchored ones**: SCR-1
  literalism kills in-vocab `Thermal Welding` ("does not mention thermal
  welding, only tack welding") while keeping OOV `Tack Welding`.
- `Lead` is CORRECT this run: both casings screened out ("lead time"
  homonym) — the 20260816 substring false positive did not recur.

## Cross-cutting mechanism, verified and MEASURED: per-chunk twin divergence

A group whose mentions span both chunks is dispatched to the tail TWICE (once
per chunk) and gets two independent verdicts. This is the twin fork the user
DECIDED to accept in `29d2167` ("accept + the identical-synthesis tripwire…
never a retry trigger") — the acceptance was justified on synthesis evidence
(twins synthesize identically; collapse looked cosmetic). **This run adds the
downstream cost, which was invisible then: 412 of 2,780 groups (15%) are
twins, and 209 of the 412 (51%) diverge in status or final tag set between
their two chunks.** Per dump: steelcraft process_caps 75%, conformity 71%,
contract_products 57%; alecmfg industries 53%. Verified instances: g31tt6mn /
gbubf8pp grounded in chunk 0, `no_candidates` in chunk 1; `Ceramic` passed
one chunk, failed the other; `Mineral board`/`Board`, and all 8 case-only
conformity duplicate pairs, are twins. The freehand A/A noise (~13–19%) plus
OOV coinage wording is the engine; the twin dispatch is what turns it into
contradictions and duplicate tags inside a single run. Every analysis keyed
"one row per group" (including this folder's funnel) silently picks a chunk.

This is NEW EVIDENCE on a settled fork, not a re-litigation: the accepted
cost was "same synthesis rendered twice"; the measured cost is "half the
twins contradict themselves downstream". Whether the decided map→array
record-shape change also collapses twin dispatch (one record per group per
tail request set instead of per chunk) is exactly the fold-point to put to
the user — the re-key it forces is the same one either way.

## Agent report 4 of 4 — alecmfg conformity/industries/process/material (verified)

Full report: `eyeball_alecmfg_tail_fields.md`. Verified claims and nuances:

- **material_caps is the best field of the run**: 29/33 tags real with direct
  evidence (7 specific wrought-aluminum grades, Ti-6Al-4V, C1100). 4 bad:
  `Foam` (packing foam), `Alumina` (blasting abrasive), `Nickel Alloy` (a
  sealing-process parameter), vague `Alloy`. `Zinc`/`Magnesium`: B1 →
  OOV → passed — real menu-listed capabilities the ontology lacks. The
  `Lead` false positive did not recur (both homonym groups declined).
- **conformity (10 tags)**: 3 solid certs (IATF 16949, ISO 13485, ISO 14001),
  2 defensible (REACH, ISO 10993-5), 5 curator-drops incl. ASTM B221 twice
  (per-row OOV dedup → duplicate labels) and D-U-N-S (in-vocab declined it
  definitionally; OOV re-admitted it). **ISO 9001 lost — but faithfully**
  (verified g5zt67xs): the synthesis itself says only "lists ISO 9001:2015 in
  its certification statistics… no explicit statement of obtaining"; both
  grounding passes correctly follow the synthesis. The under-claim happens at
  synthesis, not grounding. **RoHS lost by row-fate asymmetry** (captured in
  drops on other rows; its own rows failed SCR-2).
- **A decline that argues against itself** (verified gutmbbfq): the OOV
  explanation reasons "…REACH is not in the vocabulary, so it should be
  identified. However, since the instructions say to only return…" and lands
  as a decline — the reproducibility noise caught mid-sentence.
- **industries (37 passed)**: ~21 solid served industries (Aerospace,
  Automotive, Medical Devices, Semiconductor, EV, Rail, UAS), ~9 wrong-axis.
  Dominant failure: 18 own-activity rows (`CNC Machining`, `machine shop`,
  `prototyping`) → `Industrial Machinery and Equipment` via IGR-M2
  generalization — the same M2 hypernym leak, now on the subject's OWN
  activity axis (steelcraft shows the same tag ×4 from "industrial
  buildings"). No firm-name leak this run. Screening verdicts track synthesis
  FRAMING ("was approached by" fails; "provided services" passes — same client).
- **process_caps (127 passed tags)**: the head (count≥2, 23 tags) is ~90%
  real; the 86 singleton OOV tags are ~75–80% curator-discard — QA/testing/
  documentation inflated into capabilities (`CMM Reporting`, `Photographic
  Documentation`, `Shipment` — OGR-K1 rubber-stamps "shipment is a
  manufacturing process"), case-study minutiae, business ops. Keep-as-is
  ≈ 40–45%. Dup clusters: 8 machining variants, 7 prototyping variants.
- **Cross-field leakage is real**: ASTM specs leak materials→conformity; own
  processes leak→industries; one packaging narrative tags all four fields.
- Caveat the agent raised, checked: the repo `ontology/*.json` files are
  SNAPSHOTS (a live brute-search concept `Regulatory Approval` is absent from
  the 58-node certificates snapshot). This does not disturb §3's drop
  classification — a drop happens precisely because the LIVE parser's
  membership list lacked the label; the repo files only corroborated. The 3
  FALSE drops stand on the strongest evidence: bare `Machining` was accepted
  in the same run where its decorated form was rejected.

## Steelcraft process flood — own count confirming the agent

Of 89 steelcraft process tags: 18 testing/inspection-flavored (Intertek lab
work: missile impact, salt spray, cyclic wind pressure ×2 casings…) + 15
installation/anchor-prep tags (Field Assembly, Compression Anchor
Installation…) = **37% of the field is lab or installation-site activity**,
not Steelcraft's own manufacturing processes. The screening rules for
process_caps do not currently encode "performed BY the subject in
production" strongly enough to stop either family.
