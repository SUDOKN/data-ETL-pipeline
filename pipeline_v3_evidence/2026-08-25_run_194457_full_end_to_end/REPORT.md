# Run 20260825T194457 — what worked, what didn't, and what to fix

> **If you want this in plain English with every term defined and worked
> examples, read [`PLAIN_ENGLISH_REPORT.md`](PLAIN_ENGLISH_REPORT.md) instead.**
> This file is the dense version and uses the census shorthand.

The first end-to-end completion of pipeline v3 on both subjects, censused by
hand. **Every judgment below was made by reading the record, not by matching
text**: 6 agents plus me classified ~6,900 items (grounding tags, declines,
screening rule outcomes, descent hops) against one shared taxonomy, and I
re-verified every headline claim against the dumps myself. Corrections I made
to agent claims — and to four of my own numbers — are in
`census_verification_log.md`. Per-dump detail is in the seven `census_*.md`
files.

---

## 1. What worked

**The run itself, completely.** Both subjects finished all 7 multi-stage fields
(steelcraft 727.8s, alecmfg 683.4s), 14 dumps, zero exceptions.

**Delivery was flawless.** 381 live LLM calls; the log's dispatch count matches
the dumps exactly on every live stage (freehand 95/95, initial 47/47, oov 48/48,
screening 87/87, descent 104/104). Zero parse errors, zero unknown-id responses,
zero requests left unanswered.

**All three 2026-08-25 fixes did their jobs.**
- **B1** fired 56 times and absorbed both of the previous run's fatal crash
  signatures (the *same* record `g21dwglm` answered `Testing` again; alecmfg
  answered RoHS/REACH again). 53 of 56 drops are genuine vocabulary gaps, and
  the OOV pass re-captured 45 of the 53 affected rows.
- **The under-answer retry fired live for the first time** (steelcraft
  material_caps OOV, 1 of 46 records unanswered → re-asked → answered in 1.5 s).
- **The mentions fix landed**: explanations citing a non-existent `mentions`
  field fell 62.5% → 4.4%.

**Screening's rejection side is the strongest component in the pipeline.**
Counted, not sampled: 25/25 correct rejections in conformity+equipments,
241/246 in steelcraft contract, 312 correct declines in material+industries,
0 wrong rejections in all of alecmfg process_caps. It also correctly catches
negated evidence ("frames are designed to **avoid** continuous profile
welding") — the exact pattern descent walks straight through.

**The decline path generally beats the grounding path.** Across every field the
declines are better-reasoned than the acceptances: they correctly reject
idioms, file formats, document titles, and equipment-in-a-product-field.

**Best fields:** alecmfg material_caps (84% clean instances), alecmfg
equipments (33/34 correct, real process→machine conversions), alecmfg
process_caps rejections (0 wrong). Descent's acronym/designation resolution is
excellent throughout (`SLS`→Selective Laser Sintering, `EN AW-6082 T6`→Wrought
Aluminum Alloy, `A60 galvannealed`→Galvanized Steel).

---

## 2. What didn't — the counted picture

### Grounding: 65% clean, 35% defective (2,005 instances hand-classified, 11 dumps)

| code | meaning | count | share |
|---|---|---:|---:|
| D + N | direct or correctly normalized | 1,304 | **65%** |
| X | wrong axis (own activity as industry served, process as product, …) | 189 | 9% |
| V | too vague to use | 188 | 9% |
| B | bridge — inference the synthesis never states | 166 | 8% |
| P | wrong actor (lab, supplier, customer, installer, distributor) | 125 | 6% |
| F | fabricated — entity absent from the record | 33 | 2% |

Clean share per dump: alecmfg material 84%, steelcraft contract 74%, steelcraft
material 72%, steelcraft industries 71%, **steelcraft products 66%**, alecmfg
industries 63%, alecmfg process 60%, alecmfg products 58%, alecmfg contract 57%,
steelcraft process 49%, **steelcraft equipments 5%**. (conformity is reported by
keep-rate instead: steelcraft 76%, alecmfg 32%; alecmfg equipments 33/34.)

**33 of the 33 fabrications are concentrated in two places** — 19 in steelcraft
equipments, 8 in alecmfg process_caps, 3 in steelcraft contract, 1 each in three
others. **steelcraft products has zero fabrications**: with an identical stage
and a sibling prompt, its failure mode is over-admission, not invention — paint,
colour swatches, shipping crates, screws, mullions and frame corners all reach
final output as "products". Invention needs a contaminated upstream (equipments)
or an open-ended OOV pass (alecmfg process); over-admission needs only a weak
screen.

The two subjects fail in **opposite** ways on process_caps: steelcraft's dominant
error is wrong-actor (38 P = 24.8%; lab 13 + installation 12), alecmfg's is
wrong-axis (46 X, all from the OOV pass).

### Screening's acceptance side is where the pipeline breaks

| screen | passed verdicts | sound | unsound |
|---|---:|---:|---:|
| steelcraft **products** SCR-1 | 413 | 148 (36%) | 265 (64%) — 165 generic, 69 inverted, 31 circular |
| steelcraft **products** SCR-2 | 413 | 189 (46%) | 224 (54%) — 168 circular, 34 mirror, 16 inverted |
| steelcraft contract SCR-2 | 219 | 17 (7.8%) | **202 (92.2%)** — 88 circular, 100 generic, 14 inverted |
| alecmfg **products** SCR-2 | 89 | **0** | 51 circular, 21 inverted, 17 mirror |
| steelcraft process SCR-2 | 138 | 41 | 97 — 55 mirror, 27 inverted, 15 generic |
| material + industries SCR-2 | 212 R | 212 | 127 mirror + 29 inverted |

**SCR-3 is inert: 0 failures in 465 verdicts** (219 satisfied, 246
not_triggered). SCR-2 short-circuits it, so the wrong-actor screen never runs.

**Verdicts:** roughly **6 of 219** steelcraft contract passes are genuine
contract work; **0 of 89** alecmfg products passes are defensible catalogue
products. Both screens' rejections are sound — the failure is entirely on the
acceptance side.

### Four defects with no backstop

1. **Descent (`iterative_grounding`) is 42% defective and 99.3% unvetted.**
   54 of 128 hops are wrong, and 134 of 135 descent tags never appear in any
   screening verdict — because `PipelineStage` ranks screening at 8 and descent
   at 9. The stage making the most specific claims is the only one nothing
   checks. Full detail and 8 named bridge families in `census_descent.md`.
2. **A failed rule never gates its own tag — 6 cases, 3 shipped.** 3 ×
   `IGR-E1 failed` (evidence) were caught by screening only incidentally; 3 ×
   `OGR-N1 failed` (novelty) reached final output, because screening tests
   evidence and actor but never novelty.
3. **steelcraft equipments: 19 of 20 tag instances are fabrications** — 9
   invented `<door type> manufacturing machine` categories, none named anywhere
   in the text, all passed by screening. Caused by the mentions fix softening
   FGR-E1's anchor from mention snippets to the abstractive synthesis.
4. **SCR-2 is aimed backwards for `industries`**, and one rule causes both
   halves of the failure: it kills genuine customer sectors (`Unmanned Aerial
   Systems` from a real UAV client, `Surgical Robotics`, `Rail Equipment`,
   `Semiconductor Packaging` — "the client approached Alec Model … no evidence
   Alec Model itself serves") while ratifying ~25 own-activity tags precisely
   because those *are* attributable to the subject.

### Reproducibility and duplication

- **Grounding is irreproducible on 12.9% (alecmfg) / 18.7% (steelcraft) of rows**
  given byte-identical prompt and payload (verified: 381/381 and 680/680
  identical syntheses). No A/B below that is readable.
- **51% of twin groups (209 of 412) get contradictory verdicts** between their
  two chunks; 10 (group, tag) pairs get literally opposite pass/fail.
- **Tag coinage churn dominates output quality**: conformity 32→61 tags is half
  real recall and half rewording (8–10 FEMA variants for 2 standards); 8
  distinct welding labels for one capability; 19 of 40 industries tags are
  near-duplicates.

---

## 3. Expected vs unexpected

**Expected — reproduced at the predicted magnitude:** the A/A noise floor
(81%/87%, was 78%/90%); the near-duplicate explosion (206→209 product tags);
the equipment search returning products (68 of 75 group_ids); vocabulary gaps
in process/certificates/materials; the FE→DE entity swap recurring (1 of 6
observations); twin divergence existing at all (the accepted fork).

**Unexpected — none of these were predicted:**

1. **A prompt fix caused a regression in a different field.** The mentions fix
   re-anchored a shared rule template and only `equipments` — the one field
   whose upstream is contaminated with products — converted it into fabrication.
2. **SCR-3 has never rejected anything** (0 in 465).
3. **SCR-2 is inverted for an entire field**, causing both its false positives
   and its false negatives.
4. **Descent runs after screening**, making the terminal LLM stage unvetted.
5. **6 tags shipped despite a failed rule in their own rule list.**
6. **Grounding hallucinates the vocabulary's contents** — 3 explanations cite
   options that do not exist (`"the vocabulary includes 'Inspection, testing,
   and measurement'"`; it contains no `Inspection`, `Testing` or `Measurement`
   anywhere in 673 labels) — while reaching the *correct* verdict.
7. **A decline that argues against itself**: "…REACH is not in the vocabulary,
   so it should be identified. However, since the instructions say to only
   return…" → declined.
8. **`IGR-M1` outcome `chosen` whose text reads "No material is evidenced."**
9. **The dump reports 216 descent requests for 104 real calls** (2.08x), so any
   volume or cost figure taken from it for that stage is wrong.
10. **The identical-synthesis tripwire built in `29d2167` is absent from
    full-run dumps** — it only writes on stop-after-synthesis runs, so the
    instrument is blind on the runs that ship.
11. **`record_own_name_hits` is saturated at exactly 100.0%** (3,155/3,155), not
    the 84–100% a sample suggested.
12. **Marketing idiom read as capability evidence**: "Alec Model uses
    **'cutting-edge'** machines" cited as proof of `Precision Metal Cutting`.
13. **The determinacy gate judges a string that is not the one persisted** — 31
    `FGR-Q1` checks in steelcraft products validate a specific form
    (`'L Series doors'`, `'H Series Hurricane Rated Flush Doors'`) and then
    store a general one (`'doors'`). The tag that ships was never the tag the
    rule approved. Same root as the 58 series/trade names that DID leak into
    tags: normalization happens at persist time, silently and inconsistently.
14. **69 SCR-1 passes cite exactly what SCR-1a excludes** (listing, certified,
    installed, used-as-input), and 35 grounding declines are made on ownership
    grounds that FGR-E1a explicitly forbids — rules being applied in reverse.

**The meta-finding.** Four independent observations (6, 7, 8, plus three failed
regexes of my own) say the same thing: **rule explanations are post-hoc
justifications, not traces of the decision.** Any metric computed from
explanation text measures phrasing, not behaviour. This is precisely why the 6
failed-rule tags shipped — the code trusted a field the model writes as prose.
My own regexes failed three times this session in both directions: 10x under on
SCR-2 circularity, ~69x over on self-refuting declines, 2x over on
lab/installation attribution.

---

## 4. Fixes, ranked by (value / effort)

**Code, no prompt change, no re-dispatch:**
1. **Gate tag emission on the tag's own rule outcomes.** One guard covering both
   grounding stages; removes the 6 emitted-despite-failed-rule tags, 3 of which
   ship today. Add a test per stage.
2. **Strip the decorated display form before the vocabulary membership test**
   (`Machining (also: Material Removal Process, …)`). Recovers the 3 false
   drops, one of which (`Extruding`) lost a true grounding outright.
3. **Carry the synthesis block's counters into the full-run dump**, and stop
   listing non-dispatched descent nodes in `requests` (or mark them so they are
   not counted as calls).

**Architecture — the decision to make:**
4. **Screen the descendants.** Either move `iterative_grounding` above
   `screening` or add a second screening pass over descent output. 42% defective
   and 99.3% unvetted is not a defensible terminal state. Cheapest variant:
   screen only hops whose RGR-E1 does not quote the record.
5. **Bundle the one tail re-dispatch**: map→array record shape + twin-dispatch
   collapse + the prompt fixes below. Twin divergence at 51% makes the
   record-shape change a correctness fix, not just cleanliness.

**Prompts (all re-key the tail — do them in the same bundle as 5):**
6. **SCR-2 per field.** For `industries`, invert it: being approached by a
   client in sector X *is* evidence of serving X, and the subject's own
   processes are not industries. For products/contract, add explicit
   anti-circularity ("X is manufactured by S" never proves customer direction)
   and ban standards/ratings as evidence of customer direction. Expect this
   single change to move more numbers than anything else on the list.
7. **Equipment FGR-E1**: require the machine to be named or described as
   operated/owned/used; forbid inferring machinery from products sold. Removes
   19 of 20 fabrications.
8. **Descent RGR-E1 must quote** the child concept's discriminating feature, and
   descent needs an explicit "stop at the parent" outcome — families 1 and 2 in
   the descent census (Wet Painting's inserted "liquid", Decorative Anodizing's
   inserted "aesthetic") exist only because the vocabulary forces a distinction
   the text never makes.
9. **Fix the conformity "standard, not a certification" template** (14 misfires
   of 21; cost UL 1784, ANSI A250.8-2003, ASTM A653, STC 46/43, TDI Impact,
   TAS 201+202) and relax SCR-1 literalism so a correct ontology generalization
   is not killed for lacking its label verbatim.

**Design work, not a quick fix:**
10. **Tag normalization across records, chunks and runs** — the single largest
    output-quality lever, connecting to the reconciler's scored best-first
    design.
11. **Vocabulary gaps → an ontology decision**: generic `Inspection`, `Testing`,
    `Welding`, `Cutting`; the certificates family; `Polystyrene`/`Zinc`/
    `Magnesium`/`Fiberglass`; `Industrial Automation`.
12. **Retire or re-threshold `record_own_name_hits`** (dead at 100.0%).
13. **Equipment search precision** (68/75 groups are products) and evidence
    thickness (67% single-mention) stay parked for Phase 5.
