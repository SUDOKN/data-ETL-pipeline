# Verification log — every agent claim I re-checked against the dumps

Six agents hand-classified ~6,900 judgments. I re-verified the sharpest claims
myself. Numbers held up; two mechanism attributions did not, and one of my own
regex probes failed in the opposite direction from the earlier SCR-2 failure.

## Verified exactly, no change

| claim | source | my check |
|---|---|---|
| steelcraft equipments: 15 grounded rows, 20 tag instances, 19 fabricated, 9 fabricated categories | conformity+equipments agent | exact; only `engraining and staining machine` survives |
| steelcraft equipments twins: 4 of 9 flip | same | exact, and all 4 flip the same direction (chunk 0 grounded → chunk 1 declined) |
| conformity drop-orphans: 8 drops, 3 rescued by OOV, 5 orphaned | same | exact |
| **SCR-3 is inert: 0 failures in 465 verdicts** | steelcraft contract agent | exact — 219 satisfied, 246 not_triggered, **0 failed** |
| 7 groups return opposite screening verdicts on a byte-identical tag | same | exact — 10 (group, tag) pairs across 7 groups, incl. `stock doors` |
| alecmfg products/contract share byte-identical records | alecmfg agent | exact — 381/381 rows, synthesis string identical |

## Corrected

**1. The equipments twin flip is NOT "synthesis-verbosity sensitivity."**
The conformity+equipments agent proposed that mechanism. Reading the four
flipping pairs, synthesis length moves in both directions (138→168, 747→892,
350→183, 165→186) and the two syntheses say substantively the same thing
("LS Series Stainless Steel Doors are a product offered by Steelcraft" vs
"Steelcraft offers LS Series Stainless Steel Doors"). The flip is **freehand
grounding being bistable on equivalent evidence**: FGR-E1's "clearly implied"
clause licenses both "Steelcraft makes doors → machines exist" and "doors are
finished products, no machine is named", and the model picks one per call.
This matters for the fix: the defect is in the rule's inference license, not in
synthesis quality, so rewriting E1 fixes it and improving synthesis does not.

**2. "31% grounding non-determinism" overstates it; the true row-level figure
is 12.9% / 18.7%.** The alecmfg agent's raw count (31 rows diverging at
grounding) is right; the percentage is not. Computed at row level over the A/A
pair (products vs contract_products — identical prompt, identical payload,
verified identical syntheses):

| subject | rows compared | decline↔ground flips | both grounded, different tags | total divergent |
|---|---:|---:|---:|---:|
| alecmfg | 365 | 31 (8.5%) | 16 of 120 (13.3%) | **47 (12.9%)** |
| steelcraft | 663 | 50 (7.5%) | 74 of 377 (19.6%) | **124 (18.7%)** |

This independently reproduces the group-level A/A figure (81.0% / 86.7%
identical) from a different direction, so **the noise floor is now measured
twice: ~13% (alecmfg) and ~19% (steelcraft) of grounding outcomes are
irreproducible on byte-identical input.** No A/B on this stage below that is
readable.

**3. My own regex over-flagged, mirroring the SCR-2 failure in reverse.**
Hunting "self-refuting declines", a reversal-marker regex (however | correction |
should be identified | but this is …) returned **69 candidates**. Reading them,
essentially all are correct, well-reasoned declines using ordinary contrastive
English ("However, a D-U-N-S Number is a business identifier, not a
certification" — right). **One is genuinely self-refuting**: `gutmbbfq`, whose
OOV explanation reasons "…REACH is not in the vocabulary, so it should be
identified. However, since the instructions say to only return…" and then
declines. So the regex was ~69× too generous here, having been ~10× too strict
on SCR-2 circularity. Same lesson, both directions: **on explanation text,
regexes measure phrasing, not behaviour.**

## Verified (second batch)

**Emitted-despite-failed-rule: exactly 2, and I was wrong about one of them.**
The material+industries agent found a second case beyond the known
`gwjvjhxy`. Verified verbatim at `gpk94tty` chunk 0 (steelcraft material_caps),
focal form `lead`:

    IGR-E1 FAILED: "The focal form is 'lead', but the synthesis refers to
      'lead time' … No material is evidenced."
    IGR-M1 chosen: "No material is evidenced; 'lead' here refers to time,
      not the element."
    -> and the tag 'Lead' was emitted anyway.

The evidence rule failed, the match rule's own text says nothing is evidenced,
and a tag object was still minted. **This corrects my earlier statement that
"`Lead` was correctly declined this run."** At the OUTPUT level my statement
holds — screening killed it (SCR-1 failed, `screened_out`) in both chunks — but
the grounding stage did emit it. The distinction matters: of the 2
emitted-despite-failed-rule tags, **screening caught 1 (`Lead`) and missed 1
(`Stainless Steel Stiffener`, which passed SCR-1/SCR-2 and reached final
output).** Since descent runs post-screening and is 99.3% unvetted, this class
of defect has no backstop wherever the screen is not in the path.

**SCR-2 is inverted for `industries`, and one rule causes both halves of that
field's failure.** Verified by reading all 17 alecmfg industries rejections.
The rule tests whether the activity is attributable to the manufacturer itself,
which is the right question for capability fields and exactly the wrong one for
a *served-industries* field. It therefore:

- **kills genuine customer sectors** — `Unmanned Aerial Systems` ("the French
  UAV manufacturer contacted Alec Model, but does not specify any further
  action"), `Surgical Robotics`, `Rail Technology`, `Rail Equipment`,
  `Passenger Rail`, `Electronics`, `Semiconductor Packaging`, `Food Packaging`,
  `Personal Care Products`. Being approached by a client in sector X *is* the
  evidence of serving X; the rule reads it as evidence of absence.
- **while ratifying ~25 own-activity tags** (`Machine Tools`,
  `Manufacturing Equipment`) precisely because those ARE attributable to the
  subject itself.

So the field simultaneously loses its real customers and fills up with its own
processes, from a single misaimed rule. Fixing SCR-2 for this field removes the
27 own-activity X-codes and restores ~10 real sectors at once.

## Verified (third batch) — and two more corrections to MY numbers

**Emitted-despite-failed-rule, definitive global count: exactly 6**, and the
split is diagnostic (counted by me across all 14 dumps):

| failed rule | count | fate |
|---|---:|---|
| `IGR-E1` — in-vocab EVIDENCE rule | 3 | `Polishing`, `Urethane Casting`, `Lead` — **all 3 killed by screening** |
| `OGR-N1` — OOV NOVELTY rule | 3 | `Stainless Steel Stiffener`, `Factory assembly of jamb and head components`, `Finish painting of doors` — **all 3 reached final output** |

The mechanism is now exact: **a failed rule never gates its own tag.** Evidence
failures are caught only incidentally — a tag with no evidence also fails
SCR-1, so the screen kills it. Novelty failures have no backstop at all,
because screening tests evidence and actor, never novelty. So 3 of 3 OGR-N1
failures shipped. Fix: gate tag emission on the tag's own rule outcomes
(one guard, both stages); do not rely on the screen to clean up after grounding.

**Correction to my own "37% of steelcraft process tags are lab or installation
work."** That figure was mine, produced by keyword-matching distinct tag LABELS
(18 testing-flavoured + 15 installation-flavoured of 89 distinct). The
process_caps agent classified tag INSTANCES by reading and gets
**LAB 13 + INSTALL 12 = 25 of 153 instances = 16.3%** (17.4% of survivors);
all wrong-actor codes together are 24.8%. My number was inflated by roughly 2x
by two errors at once: counting distinct labels rather than instances, and
letting a keyword stand in for a judgment (`Assembly Testing` is Steelcraft's
own, not a lab's). **This is my third regex failure of the session** — under by
10x on SCR-2 circularity, over by ~69x on self-refuting declines, over by ~2x
here. The correct claim is: **wrong-actor attribution is steelcraft
process_caps' dominant failure at 24.8% of instances**, of which lab and
installation are 16.3%.

**Correction to the "~75-80% of alecmfg OOV singletons are discards" estimate**
(from the earlier eyeball pass): the exact count is **61 of 92 = 66%**. The
75-80% band is only reached if you additionally discard singletons whose own
OGR-N1 text names an already-identified parent — 33 of 101 (alec) and 22 of 87
(steel) were emitted despite that admission, which is the same
failed-rule-does-not-gate defect above, in its softer "satisfied but
self-contradicting" form.

**`dropped_options` row count reconciled:** I reported 14 rows for steelcraft
process_caps and 20 for alecmfg (group-deduplicated); the agent reports 14 + 22
at ROW level. Both are right — my analysis collapses chunk twins, the agent's
does not. Run-wide the figure is 56 dropped options over 53 rows (39 groups).

## Verified (fourth batch) — hallucinated vocabulary citations, and what they imply

The process_caps agent reported 2 hallucinated vocabulary citations. Checking
all 8 "the vocabulary includes X" claims in the process dumps against the real
521-name + 152-altLabel ontology:

**Correct citations (5):** `Packing`, `Labeling`, `Bending`, `Precision
Machining`, `Precision Fabrication` — all genuinely present.

**Hallucinated (3, one more than reported), and all three concern the same
missing concept:**

- `g7xf8jnf`: "The vocabulary includes 'inspection' as a word in some process
  names" — it appears in **none** (substring search over all 673 labels: zero).
- `g64t96sg`: "The vocabulary includes inspection and quality inspection" —
  neither exists.
- `gpc22jbj`: "The vocabulary includes **'Inspection, testing, and
  measurement'** as a general category" — no such category; `Testing`,
  `Inspection` and `Measurement` are all absent.

The pattern: asked to name what the vocabulary MISSES, the model invents a
description of what it CONTAINS in order to frame its coinage. **The verdicts
are right** (the vocabulary genuinely lacks inspection/testing, which is the
same gap that produced 25 of the 56 `dropped_options`) **while the stated
reasoning is fabricated.**

### The general lesson this run keeps teaching

Four independent observations say the same thing: **rule explanations are
post-hoc justifications, not traces of the decision.**

1. `IGR-M1` outcome `chosen` whose own text reads "No material is evidenced"
   (`gpk94tty`).
2. An OOV decline that argues "…so it should be identified" and declines
   (`gutmbbfq`).
3. Vocabulary contents invented to justify a correct verdict (above, 3 cases).
4. Regexes over explanation text failed 3 times this session in both
   directions (10x under, 69x over, 2x over).

**Consequence for how this project measures itself:** any metric computed from
explanation text — including the rule-outcome fields, which is what
`applied_rules` is — measures the model's phrasing, not its behaviour. The
outcomes are still worth logging, but a rule outcome must never be treated as a
gate or a measurement without checking the tag it produced. This is exactly why
the 6 emitted-despite-failed-rule tags could ship: the code trusted a field
that the model treats as prose.

## Verified (fifth batch) — SCR-1 literalism is inconsistent, not destructive

The process_caps agent's "6 WRONG rejections" holds. I read all 15 steelcraft
process rejections; **5 are clearly wrong and all 5 are one failure**: SCR-1
demands the vocabulary label appear verbatim in the text, which defeats the
purpose of an ontology generalization.

    g5b12ru0 'Thermal Welding' <- focal 'tack welds around openings'
      SCR-1 failed: "The synthesis does not mention or describe thermal
      welding; it only discusses tack welding and stitch welding."

Tack, stitch and face welding ARE thermal welding. Same for `ga8154tz`
('welded for installation as a complete unit'), `g5bp2k12`, `g2tkvw2x`,
`g88efzja`.

**But I checked the consequence before asserting one, and the obvious inference
is wrong.** Welding is NOT lost from steelcraft's capability list: the *same
tag* `Thermal Welding` PASSES on 11 verdicts and FAILS on 7, and `Tack
Welding`, `Stitch Welding`, `Fusion Welding`, `Face Welding`,
`Steel Stiffener Welding`, `Unit Frame Welding` and `3-Sided Frame Welding` all
survive. So the literalism defect costs **consistency and per-record accuracy,
not field-level recall.** A record-level ground truth would score it as 7
errors; a field-level one would not see it at all.

Two other things this rejection set shows, both to the screen's credit:

- **The screen catches negated evidence that descent does not.** `g52bewq4`:
  "Steelcraft's frames are designed to **avoid** continuous profile welding, not
  that Steelcraft performs it" — correctly rejected. This is exactly the
  anti-evidence pattern descent walked all the way to `PVC` on the vinyl
  record, and the difference is only that descent runs after the screen.
- **The distributor-ambiguity rejections are individually defensible** (6
  verdicts on "Steelcraft **or its distributor's** fabrication shop"), though
  the same sentence is known to pass elsewhere in the run — the inconsistency,
  not the judgment, is the defect.

Also visible here: **8 distinct welding labels for one capability** — the
near-duplicate explosion, in the field where it is least obvious.

## Verified (sixth batch) — steelcraft products, the last census

**`FGR-Q1` validates a different string than the one persisted — verified, 31 of
454 instances.** The pattern runs one direction only: the rule approves the
SPECIFIC form and the pipeline stores the GENERAL one.

    gbubf8pp   persisted tag: 'doors'
               FGR-Q1 judged: "'L Series doors' refers to a determinate product
                               category, not an individual item or too general."
    got7s42j   persisted tag: 'hurricane rated flush doors'
               FGR-Q1 judged: 'H Series Hurricane Rated Flush Doors'
    gos8cty5   persisted tag: 'steel door frames'
               FGR-Q1 judged: 'F/MU'

So the determinacy gate never actually gated the artifact. The saving grace is
the direction — the stored tag is *more* general, which is what ontology-grade
output wants — but it is silent and inconsistent: **58 series/trade names DID
leak into the persisted tags** in the same dump. One behaviour (normalization at
persist time), two opposite symptoms.

**Zero fabrications in steelcraft products, against 19 of 20 in steelcraft
equipments** — same stage, same subject, same run, sibling prompts. This is the
strongest available evidence that the equipments regression is caused by the
*equipment* catalog's E1 anchor plus its contaminated upstream, and not by the
mentions rewording as a general effect. It also localizes the fix: only
`equipment_phrase_freehand_grounding` needs the E1 repair, not all 10 catalogs.

**Precision on the largest field:** 70% keeping D+N, **56% at ontology grade**
(dropping the 58 series-name tags), working estimate ~60%. Recall is not the
problem — only 10 of 264 declines are false negatives, all recoverable elsewhere.

**Twins again:** 115 twin pairs, 52 disagree (45%), including 2 screening flips
on byte-identical text. Consistent with the run-wide 51%.
