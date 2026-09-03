# Census-in-progress notes — run 20260901T013332

Working scratch for findings raised by judge agents and by the assistant while
the census runs. Folded into FINDINGS.md at the end; not itself a deliverable.

## Eval-set defects raised by judges (standing rule: the corpus is under test)

- **acimachine_com / conformity_attestations — confirmed entry `CSA/CUS` is
  unreachable.** The judge grepped all 11 windows (8,583 lines) and found the
  string only in the packet's own must-find legend, never in window text. It
  cannot be found by any run and cannot be honestly recorded as a miss.
  Action: re-validate the entry against the wire text; `disputed` if the quote
  is absent. Raised by the primary judge, unprompted — the brief's "Doubt the
  list" section working as intended.

## Instrument defects found this run (assistant)

- **`repetition_loops` cannot see the loops that matter.** The detector reads
  the PARSED phrase list, so a loop severe enough to blow the token cap and
  break the JSON leaves nothing to inspect and reports clean. All 6 truncated
  windows this run were textbook loops (`"products"` x1,900; `"robotic
  welders"` x621) and all 6 were reported as zero loops. `unparseable_windows`
  caught them, so nothing shipped silently, but the diagnosis was lost.
  Fix: scan raw response text when a window is unparseable.

- **ROOT CAUSE CONFIRMED for the silent truncation.** The last wave-1 agent on
  the unsplit steelcraft products packet (856 forms) died with an explicit
  error rather than a quiet stop: `Claude's response exceeded the 64000 output
  token maximum`. So the earlier "agent reported success at 209/755" cases were
  the same ceiling hit at a point where the agent still managed a final
  summary. This is a hard output limit, not a judgment or diligence problem,
  and it is why bounding the task (not exhorting the agent) is the only fix
  that works. 150 forms/task keeps a worst-case file around 20k output tokens.

- **Census judging was not reproducible before today.** Agents silently
  truncate on large packets: measured 0%, 34% and 72% skipped among the first
  three returning agents, all self-reporting success. Now bounded by
  `split_judge_packets.py` (150 forms/task) and checked by
  `verify_census_coverage.py`. NOTE: the 2026-08-27 census (2,757 forms) was
  never coverage-checked, so its precision figures carry an unknown truncation
  risk and should be re-verified before being quoted again.

- **`merge_judgments.py` would have dropped every part file.** It derives the
  scorecard from the filename by splitting on `__`, so `X__products__part01`
  resolves to field `products__part01`, matches no scorecard, and is skipped
  with a warning. `assemble_census.py` now reconciles parts into unit files
  before any merge.

- **Packet form regex missed every possessive.** The exporter switches from
  single to double quotes when a form contains an apostrophe; a single-quote
  pattern dropped exactly 47 forms corpus-wide (`Steelcraft's DE-Series
  frames`, `CNC HMC's`, `BOP's`). Fixed; counts now reconcile at 21,176 across
  packets, split parts and scorecards.

## Judge protocol deviation (handled)

- One agent read `equipment_phrase_search.txt` and coded 27 agstech equipments
  forms by the PROMPT's exclusion rules rather than plain meaning — throwing
  out hardness testers, microscopes, conveyors, palletizing robots. That basis
  is circular: it makes the prompt the ground truth and so cannot detect a
  wrong prompt boundary, which is the F2/F3 defect class. Quarantined under
  `judgments_quarantine/`, unit re-queued, and the brief now forbids opening
  the extraction prompts and explains why.

## Stage findings emerging from the census (to be quantified at merge)

- **acimachine / industries — the field collapses into equipment taxonomy.**
  141 forms, almost all machine-category and brand-facet names ("CNC Lathes",
  "Vertical Machining Centers", "Hoists (All Types)") returned as *industries*.
  Judge coded them `W` (word-association). The site is a used-machine dealer, so
  its page furniture is category navigation; the stage read the catalogue facets
  as served sectors. One real miss cluster: a single AKIRA SEIKI product
  sentence naming mold/aerospace/medical/semiconductor/optical/automotive, none
  of which any returned form covered.

- **acimachine / material_caps — taper standards returned as materials.** 20
  forms (`CAT-40` plus an 18-item taper facet) are spindle/tool-holder
  interface standards, an equipment attribute. Coded adjacent-field.

- **ableengineering / industries — a person's CV read as company activity.**
  Forms drawn from a named employee's prior work at another company
  ("Defense & Space business", "U.S. Air Force programs") before he joined.
  Coded junk. Same shape as the pradeepmetals employment-history entries that
  the 2026-08-28 run found in the EVAL SET (F5) — the defect exists on both
  sides of the comparison.

- **ableengineering / equipments — process-vs-machine boundary is genuinely
  soft.** "horizontal and vertical honing", "CNC shot peen", "Grind (CNC and
  Manual)" sit under a page heading that calls them processes, while the nouns
  themselves read as machines. Judge split them P/V and flagged each. Worth
  reading beside the standing equipments precision finding: some of that
  field's error is real ambiguity in the source, not stage carelessness.

## HEADLINE CANDIDATE — catalogue listings return the category, not the models

**acimachine / equipments: 634 miss records across 4 parts** (166 / 427 / 22 /
19) against 387 returned forms. On every brand-catalogue window (Willis,
Wellsaw, Dainichi, Waterjet Corp, Akira Seiki, Jet) the stage returned the
generic category name — "CNC Lathes", "Vertical Mills" — and missed nearly
every specific brand+model designation printed in the same window.

The extreme case is the Jet brand window `66765:78696`: **0 of ~340 named Jet
models returned**, with 10 full product cards present in that window's text.

Why this was invisible until now:
- acimachine's expectations are documented as SAMPLED (HANDOFF/RUNBOOK both
  say so), so the eval set never carried these designations and confirmed
  recall cannot charge a miss for them. Mechanical recall for this subject
  reads fine.
- The 2026-08-28 difference-set pass judged only forms the run RETURNED. A
  systematic failure to return anything is invisible to that method by
  construction. **This is the first evidence of why the full census was worth
  its cost** — the defect lives entirely in what the stage did not say.

All misses carry `actor: reseller_inventory` (correct: acimachine is a dealer,
not the manufacturer), so this is a recall finding, not a wrong-actor one.

Caveat before quoting: acimachine reads only 1.3% of its site text under the
production 40k-token cap, so this is measured on a small and unrepresentative
slice of that subject. What generalises is the SHAPE — category returned,
models missed, on catalogue-structured pages — not the count.

**CORROBORATED on a second subject, and this one is a MANUFACTURER, not a
dealer.** `blackadvtech / equipments`: 14 misses, "mostly specific branded
equipment examples the pipeline dropped while capturing only the generic
category" — **Makino P-300, Trumpf MB 4200, KUKA KR 16, TRUMPF AutoLoader**.
That kills the obvious objection that acimachine's 634 misses are an artefact
of dealer-catalogue page furniture. The stage prefers the category noun over
the branded model designation wherever both are present, which is the opposite
of what downstream wants: the make and model is the identifying information,
the category is recoverable from it. Check tanfel and taylordunn to complete
the picture.

## A DEFINITIONAL FORK THE USER MAY NEED TO SETTLE — metrology in `equipments`

The census and the eval set now disagree about the same six entities, and both
are behaving as designed.

`alecmfg / equipments` census: 16 misses, mostly branded metrology and tooling
from the case-study "Equipment Used" tables — Zeiss CONTURA G2 CMM, Laser
Interferometer, Nikon optical comparator, Renishaw probing, carbide end mills,
a bare `CMM` recurring across five windows.

Those are the SAME entities that the 2026-08-27 correction moved to `disputed`
in the eval set, on the ground that the equipment search prompt explicitly
excludes things that "only measure, inspect, or test the work" and excludes
tooling. Mechanical confirmed-recall for this unit therefore reads **100%**,
while the census reads 16 misses.

Neither number is wrong. They rest on different standards, and that split is
deliberate (EXPECTATIONS_SCHEMA pins recall to the prompt's `What qualifies`
clause; the packet header and CENSUS_JUDGE_BRIEF pin codes to plain meaning,
so that a wrong prompt boundary stays detectable). But the two must never be
quoted side by side without saying which standard each uses.

**The question for the user:** should a CMM count as equipment? Downstream
consumers of `equipments` plausibly want the shop's metrology capability. If
yes, the prompt's exclusion is the defect and those 6 disputed entries should
be promoted. If no, the census misses here are correct restraint and should be
recorded as such rather than as recall loss. Do not let this resolve itself
silently in whichever direction the next agent happens to read it.

## The equipments field-axis defect reproduces on alecmfg

Window `42936:66294` returned 27 forms; **26 are process vocabulary**
(deburring, cleaning, electropolishing, passivation, bending, welding,
reaming, blasting, anodizing, sealing, spectrometry). One is defensible
equipment (a PCD cutting tool); 3 more are borderline fixtures/TCP control.
Same shape as the 2026-08-27 finding on steelcraft and alecmfg — the
operations the subject performs, returned in place of the machines it uses.

## MY OWN INSTRUCTION DEFECT — metrology handling is inconsistent across units

The "Doubt the list" section of CENSUS_JUDGE_BRIEF.md illustrated a list defect
with "an equipments list once demanded metrology instruments the field
explicitly excludes." That sentence was meant to show the SHAPE of a malformed
entry. Agents read it as a boundary ruling, and applied it:

- `alecmfg / equipments` (judged BEFORE the wording was noticed): counted
  Zeiss CMM, Laser Interferometer, Nikon comparator etc. as **misses** — plain
  meaning, as the brief's main instruction says.
- `anchor-mfg / equipments`: **deliberately excluded** Hexagon Global S,
  LK G90-C CMM, Brown & Sharp CMM, Micro-VU comparator, blue light scanner and
  tensile testers, citing "the brief's documented list-defect precedent that
  this field explicitly excludes metrology instruments."

Two units, opposite standards, because of one ambiguous sentence I wrote. This
is precisely the instruction-drift the RUNBOOK warns contaminates agreement
figures — self-inflicted this time.

**Fixed 2026-09-01:** the example was replaced with two malformed-entry cases
that carry no boundary claim (`balls` / comma-joined forms; `CSA/CUS` /
unreachable quote), plus an explicit line: nothing in the brief rules on where
a field's boundary sits, and a judge citing the brief as authority for
excluding a kind of thing has misread it.

**Owed before the equipments numbers can be quoted:** the equipments units
judged under the old wording must be reconciled to one standard. Affected so
far: anchor-mfg (excluded), **blackadvtech (excluded — the agent quoted the
old sentence back verbatim in its report)**, alecmfg (included as misses),
agstech (already quarantined for a related reason). Any equipments unit judged
before 2026-09-01 by an agent that started before the brief was corrected is
suspect; check each agent's report for the metrology sentence. Cheapest fix is re-running the equipments
units under the corrected brief; the miss lists are what changes, not the
form codes. Until then, **equipments miss counts are not comparable across
subjects** and no corpus-wide equipments recall figure should be reported.

## POSSIBLE MARKDOWN-SPECIFIC MISS CLASS — spec-table cells go unextracted

`alecmfg / process_caps` part03: two confirmed must-find entities were missed
while "sitting unextracted inside a markdown spec table" — `Blasted` /
`epoxy coated` and `dimensional inspection`. The same subject's material_caps
judge independently recorded `epoxy` as a miss from the same construct
("Blasted + epoxy coated (offshore marine coating)").

Why this is worth its own line rather than folding into general recall: the
markdown_v2 cutover was undertaken partly BECAUSE markdown exposes content that
innerText flattened — pipe tables, HTML islands, closed accordions (see
`expectations_markdown/README.md`, TOPUP_BRIEF). If the stage systematically
under-reads pipe-table cells, the cutover's main benefit is not reaching the
output, and the topped-up corpus entries drawn from those tables will read as
stage misses forever.

NOT yet established — this is two observations on one subject. To confirm:
count misses whose evidence quote sits inside a `|`-delimited line, corpus-wide,
once the census completes. If the rate is materially above the baseline miss
rate, it is a real class and belongs in FINDINGS as a cutover-follow-up item.

## A judge alarm that RESOLVED CLEAN — and a packet-design flaw it exposed

`blackadvtech / material_caps` judge reported 16 of 40 must-find entries absent
from its packet text (nickel steel, cobalt-chrome, argon, acetylene, Vitallium,
thermoplastics, ...) and correctly declined to fabricate miss records for them.

Checked mechanically. All 16 occur in the full site text — `nickel steel` 7
times, `filler metal` 7, `thermoplastic` 7 — and **zero times in the wire text
this run actually read.** The scorecard agrees: 16 confirmed entries scored, 24
out of coverage. blackadvtech reads 70.2% of its text under the 40k cap, and
these entries live in the other 30%.

So: eval set correct, stage not charged, harness behaved exactly right. The
judge's doubt was appropriate and the standing rule worked — doubt raised,
doubt resolved against the text, no silent miss and no fabricated one.

**But it exposes a flaw in MY packet splitting.** `split_judge_packets.py`
repeats the whole shared header — including the subject's FULL must-find list —
in every part. A part carrying 2 of 9 windows still advertises all the
subject's confirmed entities, most of which cannot possibly appear in it. That
wastes judge effort and manufactures false "list defect" reports that then need
mechanical refutation (three judges have now raised one).

Fix for the next census, NOT applied now because re-splitting would invalidate
in-flight work: filter the must-find list per part to entries whose evidence
quote occurs in that part's windows, and label it "entities expected in THESE
windows" rather than in the subject. Same for the unsplit case, where the list
should be scoped to the unit's own windows.

## Two files hold a MIXED provenance — read their miss counts with care

The pre-ledger double-assignment hit two more files before the fix landed:
`alecmfg_com__material_caps__part01.jsonl` and
`alecmfg_com__process_caps__part03.jsonl`. Two independent judges judged the
same packets and wrote to the same path.

The second agent handled it well — it detected the overwrite, verified the
replacement content was itself complete, and **appended only its own distinct
misses instead of re-clobbering**. So both files are valid and coverage-complete.
But their provenance is now split:

- **form codes**: from ONE pass (whichever wrote last) — internally consistent,
  safe for precision.
- **miss records**: the UNION of two independent readings — richer than any
  other unit in the census, and therefore NOT comparable with them.

Effect: those two units will show inflated miss counts relative to
single-judged peers. `material_caps__part01` carries 3 misses (1 + 2),
`process_caps__part03` carries 6 (1 + 5). Subtract the non-primary
contributions, or exclude these two units, before computing any per-field miss
rate. They are fine to keep for the miss INVENTORY, which wants completeness.

Lost opportunity worth noting: this was two independent judges on identical
packets — exactly the double-judge agreement data the RUNBOOK asks for — but
the first pass's form codes were overwritten, so no agreement can be computed
from what survived. The planned double-judge pass must use the
`.judge2.jsonl` suffix the merger already expects.

Also collided pre-ledger: `agstech_net__industries__part01/02/03`. Two agents
judged these independently; the second overwrote the first wholesale (no miss
merging, unlike the alecmfg pair). Miss counts differed between passes — 9/2/4
vs 7/2/2 — which is itself a rough signal that independent readers differ by
~2 misses per part on this material. The surviving file is one judge's
complete, internally consistent pass, so it is safe to use as-is.

## Windows that return NOTHING while holding in-field content

`howcogroup / equipments`, window `110839:126985`: **0 forms returned**, while
that window's text carries a full Quality Control inspection list — two named
CMM models, a Shadow Graph, Gage Maker equipment, Boroscopes. 38 equipment
misses for this subject overall, the highest non-acimachine count so far.

`howcogroup / conformity_attestations`, window `18000:35516`: 10 of the
subject's 14 misses concentrated in ONE ESG-policies page whose entire document
list was skipped — GDPR, Conflict Minerals, Modern Slavery, Section 172, SECR,
Supply Chain Code of Conduct — **though the same documents were returned
correctly in other windows.** Intra-run inconsistency, same class as the
steelcraft fire-door finding (2026-08-27) and decimal's `carbon steel` this run.

CORRECTED 2026-09-02: the claim I first wrote here — that `empty_windows`
counts only no-response windows — is WRONG; the code counts empty-list
answers and did list howcogroup's `110839:126985`. What was true: the metric
misses NEAR-empty answers (the ESG window returned 1 phrase) and reached no
verdict, so nobody looked. The `low_yield_windows` tripwire is now built
(>=8k chars, <=2 phrases, warns-never-gates; 84 hits on this run).

Second pattern from the same judge, worth separating: howco captures its OWN
shop-floor equipment well but misses client/OEM equipment nouns (valves, pumps,
compressors, heat exchangers, gas turbines) on the Energy sector pages. That is
the recall-first stance failing on the actor axis — the stage appears to filter
by "is this ours?" when the field's rule says extract and flag the actor.

## CONSOLIDATED HEADLINE — search skips whole blocks of listing-shaped content

Four subjects now, three of them manufacturers rather than dealers. The failure
is not a boundary or precision problem: it is **entire regions of a window
producing no output at all**, while the stage answers normally elsewhere in
the same window.

| subject | field | evidence |
|---|---|---|
| acimachine | equipments | 634 misses; Jet brand window returned **0 of ~340** named models with 10 product cards present |
| agstech | equipments | 94 misses; an entire "Networking and Communication Products" page uncovered — **30 of 35** misses in one part come from that single block; a Hikrobot/Hikvision machine-vision brochure block likewise fully missed |
| howcogroup | equipments | 38 misses; window `110839:126985` returned **0 forms** while holding a full QC instrument list (2 CMM models, Shadow Graph, Gage Maker, Boroscopes) |
| blackadvtech | equipments | 14 misses, all branded models dropped while the category noun was kept (Makino P-300, Trumpf MB 4200, KUKA KR 16, TRUMPF AutoLoader) |

Two distinct shapes, possibly one cause:
1. **Category kept, models dropped** — the stage returns the generic noun
   ("CNC Lathes", "Vertical Mills") and omits every branded designation beside
   it. Backwards from what downstream needs: make+model identifies, category
   is recoverable from it.
2. **Whole block silently skipped** — a product-listing page, a brochure-link
   block, a spec table contributes nothing, though prose in the same window is
   handled.

agstech also missed the literal must-find strings `CNC machines` and
`manual machines`, which are as plain as this field gets.

**Why no existing metric sees it.** `empty_windows` counts windows with no
RESPONSE, not windows whose response was an empty or partial list. Confirmed
recall cannot see it either where the corpus is sampled (acimachine) or where
the entries sit outside the 40k read cap. The difference-set method judges only
what was returned, so a systematic non-return is invisible by construction.
**The full census is the only instrument that has ever seen this**, which
retrospectively justifies its cost.

Owed at merge: count misses per window against forms returned per window, and
rank windows by miss:return ratio. That ranking is the deliverable — it names
the exact windows to show a prompt engineer.

Fifth subject, same shape: `anchor-mfg / process_caps` — an entire
equipment/technology cluster on the Assembly & Automation page (robots,
collaborative robots, assembly cells, vision systems, PLCs, DCS) returned by
none of that window's 39 forms, 7 misses from one paragraph; and the Quality
page's instrument list yielded only "Large & Small Tensile Testers" out of six
siblings. CHECK AT MERGE: several of these are equipment nouns logged as
process_caps misses, where the code table makes equipment adjacent-field (`E`)
rather than in-field — so they may be mis-filed misses rather than real
process_caps recall loss. The block-skip observation stands either way; the
per-field attribution needs re-reading.

## SHARPENED: it is not whole windows — it is PIPE-TABLE CELLS

Verified mechanically on `alecmfg` window `94729:113432` (18,761 chars), where
three judges independently reported must-find misses. Forms returned by that
same window, per field:

    products 103 · process_caps 49 · industries 18 · material_caps 13
    equipments 3 · conformity_attestations 5

**The window is being read and answered normally** — 103 products from it. So
this is not truncation, not a skipped window, not a capacity limit.

What is missed sits inside markdown pipe tables in that window's text:

    | Surface Treatment | Blasted + epoxy coated (offshore marine coating) |
    | Documentation Required | Weld map, WPS/PQR, dimensional inspection, material certs |

`Blasted`, `epoxy coated`, `WPS/PQR` and `dimensional inspection` are all
must-find entities for their fields. None was returned. Meanwhile
`ASME-certified welders`, which sits in ordinary prose in the same window, was
also missed by conformity — so prose is not immune, but the table rows are
where the concentration is.

**This supersedes the "whole block skipped" framing for this case.** Re-read
the four subjects in the consolidated finding above with this in mind: some of
those may also be table/list structure rather than page-level skipping. The
acimachine and agstech cases involve product-card and brochure blocks, which
are list-structured; howco's zero-form window held an instrument LIST. The
common factor may be **structured, delimited layout** rather than page identity.

This is the single most actionable finding of the run, because it points at
something concrete: the markdown cutover deliberately EXPOSED pipe tables
(`expectations_markdown/README.md`, TOPUP_BRIEF), and if the stage under-reads
exactly that structure, the cutover's main benefit is not reaching the output.

MERGE-TIME TEST (mechanical, cheap, decisive): for every miss across the
census, check whether its evidence quote sits on a line containing `|`.
Compare that share against the share of `|` lines in the wire text overall. If
misses are enriched on table lines, the class is real and quantified.

## ...AND THE TEST REFUTES IT. Run 2026-09-01 on 1,066 located misses.

    misses whose quote sits on a '|' line :   10 / 1066  =  0.9%
    base rate of '|' lines in wire text   : 5418 / 185316 =  2.9%
    enrichment                            :  0.32x

Misses are **under-represented** on pipe-table lines by a factor of three, not
enriched. Per field, every one is at or below base rate except industries
(5.7%, n=53, three misses — noise at that count).

**The pipe-table hypothesis is dead.** The alecmfg window `94729:113432`
observation is still true as a specific instance — those table cells really
were missed — but it does not generalise, and three judges independently
noticing the same vivid case is exactly the trap the RUNBOOK's "a metric you
have not spot-checked is not yet a number" rule exists to catch. Salient
anecdotes recur in reports because they are memorable, not because they are
frequent.

Recorded rather than deleted, per the standing practice of keeping refuted
hypotheses with their refutation: the next person to notice a missed table row
should find this note instead of re-deriving the theory.

What the number does NOT settle: 782 of the 1,066 misses are `equipments`,
and 634 of those come from acimachine alone. The corpus-wide rate is therefore
dominated by one subject's catalogue. The category-vs-model finding
(branded designations dropped while the category noun is kept) stands on its
own evidence and is untouched by this test.

## Agents collide on SCRATCH files too, not just outputs

An agstech judge reported that a concurrent agent clobbered its scratch script
`gen_part01.py` by reusing the same filename for unrelated `decimal_net` work.
It recovered under a collision-resistant name. Output-file collisions are now
prevented by the in-flight ledger; scratch-file collisions are not, because
agents choose those names themselves and nothing coordinates them.

For the next census, the brief should tell judges to write any scratch file
under a unique path (their packet name is already unique and to hand).

Consequence for THIS run: nothing is known to be lost — every judgment file
passes `verify_census_coverage.py` — but the agent's own advice is right and is
adopted: **re-run the coverage verifier over all 102 units immediately before
the merge**, not only as files land, because a late collision would otherwise
go unnoticed. Added to the merge checklist below.

## Merge checklist (do these in order, none optional)

STATUS 2026-09-01: 1 DONE (182 tasks, only the known mathewsco phantom-part
names flagged; unit file verifies 126/126). 2 DONE (102/102 complete after the
zero-form-packet fix). 3 STILL BLOCKED on the user's CMM ruling — FINDINGS F6
reports both readings and the mechanical filter is in census_report.py. 4 DONE
(noted in FINDINGS numbers section, not deducted from files). 5 DONE by
inspection (census_report.py section G: 12 of 17 are equipment nouns; reading
corrected in FINDINGS F6). 6 DONE — and it REVERSED the corpus-wide refutation:
alecmfg 6x, mathewsco 36x enrichment (FINDINGS F3). 7 DONE (4 spurious misses
removed mechanically). 8 DONE (JUDGED.md + census_report.py sections C/D).
FINDINGS.md written; the double-judge pass remains the one owed instrument step.

1. `verify_census_coverage.py --run 20260901T013332` over ALL units — final,
   not incremental.
2. `assemble_census.py --run ... --apply` to fold parts into unit files.
3. Reconcile the equipments metrology split (see the brief-defect note above)
   before any equipments number is quoted.
4. Deduct the mixed-provenance miss inflation on
   `alecmfg material_caps part01` and `alecmfg process_caps part03`.
5. Re-read the anchor-mfg process_caps misses that are equipment nouns; they
   may be mis-filed.
6. Recompute the pipe-line miss share per subject, not corpus-wide, since
   acimachine dominates the pooled figure.
7. **Re-check misses on the 17 windows that were SPLIT ACROSS PARTS.** A judge
   holding forms 1-150 of a 227-form window cannot tell whether forms 151-227
   already cover an entity, so it logs a miss that may be spurious. A
   blackadvtech judge caught this itself and caveated its own miss records.
   Affected (unit / window): acimachine products 66765:78696 (3 parts),
   108872:122561, 90176:108872; agstech process_caps 0:24767; agstech products
   47982:72485, 152380:178371; alecmfg products 66294:88178; blackadvtech
   products 108890:130827; decimal process_caps 0:20914, 20914:36776; decimal
   products 20914:36776; fze products 39736:63495, 114412:137717; steelcraft
   products 41525:64203, 64203:84954; superiortech products 0:19121; tanfel
   products 60628:83353.
   FIX: for each, take the UNION of forms from all parts of that window and
   drop any miss whose entity is covered by a form the judge could not see.
   This is mechanical — `form_covers` already exists.
   PREVENT NEXT TIME: never split inside a window. Either raise the per-task
   cap so the largest single window fits, or give every part of a split window
   the FULL form list as read-only context and have only one part judge it.
8. `merge_judgments.py --run ...`, then rank windows by miss:return ratio.

## COUNTER-EXAMPLE: the industries field works fine when the site labels sectors

I wrote earlier that the industries defect (own activity returned as sector,
served sectors missed) reproduced on alecmfg, acimachine and agstech and
called it a field-level defect. `fzemanufacturing / industries` is the control
case that constrains it: **489 forms across 6 windows, ONE miss.**

The judge's explanation is the whole point: FZE's site "repeats a large
'Industry Focus:' table and runs several dedicated 'Industries Served' pages",
so most returned forms genuinely name served sectors and were coded V.

So the pattern is not "the industries field is broken." It is:

- site states its sectors explicitly under a labelled heading → the stage
  reads them correctly (fze)
- site does not, and sectors must be inferred from context → the stage fills
  the field with the subject's own activity instead (alecmfg, acimachine,
  agstech)

That is a much more useful finding than a flat field defect, and it points at
a different remedy: the failure is inference, not extraction. It also predicts
where to look next — check whether the three failing subjects have any
"industries served" heading at all, and whether the one miss classes of fze
cluster on its non-labelled pages.

Method note to carry forward: I generalised from three subjects without
looking for a control. The control arrived by accident, in the ordinary course
of the census, and reversed the shape of the claim. Two of today's four
over-claims (this and the pipe-table one) came from the same habit — reading a
repeated observation as a rule before testing the negative case.

## "OK" on the mechanical pass, 0% in-field on the census

`med-tekinc / conformity_attestations`: the mechanical scorecard says
`OK / not gated (0 candidates)`. The census says **none of its 4 returned
forms is a standard or certification** — three are QC-practice sentence
fragments (`U`), one is generic capability language (`G`). In-field: 0 of 4.

Small n, but it makes the point cleanly: a unit with no confirmed entries to
gate on shows OK regardless of what it returned, because recall-only gating
has nothing to measure there. Precision is invisible to the gate by design.
Any subject/field with a thin expectation set is in this position.

Worth a look at merge: how many units are `OK` purely because they had nothing
to gate on? Those are the blind spots of the whole recall-first design, and the
census is the only thing that has ever looked at them.

Related, from the same judge, on cross-field sense: `neutral Salt Bath`,
`Austemper Bath` and `Polymer Quench` were coded as the MEDIUM (`C`) in
material_caps and as the OPERATION (`V`) in process_caps — the same string,
judged independently per field, correctly landing on different codes. That is
the census working as intended and is worth keeping as an example of why
per-field judging beats a single global vocabulary.

Split-window correction, done by a judge unprompted: `decimal_net products`
window `20914:36776` (split across part02/part03 by form-number range 1-150 /
151-185). The judge found that form 176 `'Value-Added Assembly'` in part03
covered a miss it had logged in part02, and removed the stale miss. That is
exactly the step-7 correction, applied at source. One of the 17 affected
windows is therefore already clean; the other 16 still need the mechanical
pass.

Second and third split-windows handled at source, this time deliberately:
`decimal_net process_caps` windows `0:20914` (171 forms over part01/02) and
`20914:36776` (165 over part03/04). The judge read BOTH halves before coding
and filed each window's misses once, in the part where the window first
appears — naming the exact risk: forms like `Centerless Grinding` and
`OD Grinding` appear only in the second half and "would otherwise have been
false misses in the first half." part02 and part04 therefore carry zero misses
by design, not oversight — do not read that as a judge skipping work.

Running tally of the 17 split windows: 3 handled at source (decimal products
20914:36776, decimal process_caps 0:20914 and 20914:36776), 14 still needing
the mechanical union check at step 7.

## INTRA-RUN INCONSISTENCY is now the best-evidenced finding of the run

The same designation, in the same subject's text, returned in one window and
missed in another — with no difference in how the text presents it. Four
subjects, four fields, all found independently by different judges:

| subject | field | term | shape |
|---|---|---|---|
| fzemanufacturing | process_caps | `repair` / `repairs` | missed in two windows, correctly returned in a third |
| decimal | material_caps | `carbon steel` | named twice on the Laser Cutting page; returned in one window, missed in the adjacent one |
| howcogroup | conformity_attestations | GDPR, Conflict Minerals, Modern Slavery, Section 172, SECR | whole document list skipped in one window, returned correctly in others |
| steelcraft (2026-08-27) | process_caps | fire-door pressure testing | missed in 6 of 9 windows despite appearing on nearly every product page |

This is a stronger result than the category-vs-model finding, because it is
**self-controlled**: the same string, the same site, the same prompt, the same
model, the same run — only the window differs. No appeal to what the field
"should" contain is needed, and no eval-set judgement is involved. Whatever
causes it is not a boundary definition problem and cannot be fixed by
clarifying what qualifies.

It also bounds the A/A noise floor's relevance: the 2026-08-26 probe measured
28.4% whole-window set identity on byte-identical repeated requests. Some of
this inconsistency is that same instability showing up across different
windows rather than across repeats. Quantify at merge: for each subject/field,
count designations that appear in >=2 windows' TEXT and are returned in some
but not all — that ratio is the metric, and it needs no judges at all.

Fifth intra-run-inconsistency instance: `fzemanufacturing /
conformity_attestations` — `Lean Manufacturing` appears as a spec-table bullet
and was returned in 6 of 9 windows, uncovered in 3. Found by the judge's own
cross-window verification pass, not by the must-find list.

Fifth listing-skip instance, same subject: `fze / equipments`, 36 misses over
two parts, concentrated on pages the stage returned almost nothing from — a
"Professional Hand Tools, Power Tools, Diagnostic Equipment" page (CT Scanner,
MRI Scanner, Ultrasound, heart rate monitor, jigsaw, circular saw, screwdrivers
all unreturned) and a turf-care "Machinery ... includes" list (mowers, edgers,
dethatchers, aerators, brush hogs, slit seeders).

Note the shape of that fze equipments case: these are enumerated lists in
running prose, NOT pipe tables — consistent with the refuted table hypothesis
and with the surviving "enumerated listing" reading.

## THE CATEGORY-OVER-SPECIFIC PATTERN IS NOT EQUIPMENTS-ONLY

`mathewsco / material_caps`: the text names three specific aluminium grades —
**319, 356, A357** — in a single sentence, and the stage returned only the bare
word `Aluminum`. Same file: `CRS`, `HRPO`, `Pressure Sensitive Adhesives` also
missed. All on the must-find list.

This is the identical shape already recorded for equipments (branded model
dropped, category noun kept: Makino P-300 → "CNC machines"; ~340 Jet models →
"CNC Lathes"), now appearing in a second field with a different vocabulary.

So the finding generalises: **where the text offers both a general term and
specific designations of the same thing, the stage returns the general one and
drops the specifics.** That is precisely backwards for every downstream
consumer — `Aluminum 356` is actionable, `Aluminum` is not; `Makino P-300`
identifies a machine, `CNC machines` does not.

Evidence now spans:
- equipments — acimachine, agstech, blackadvtech, howcogroup, fzemanufacturing
- material_caps — mathewsco
- products — steelcraft (document-title flood, 2026-08-27, same direction)

This, together with intra-run inconsistency, is what FINDINGS should lead on.
Both are recall defects, both are invisible to the difference-set method, and
neither can be fixed by clarifying a field boundary — which is what the three
2026-08-29 prompt edits attempted.

## Eval-set boundary dispute raised by a judge — pradeepmetals conformity

The pradeepmetals conformity judge coded the site's "Charter Documents"
(Certificate of Incorporation etc.) and its 23-item internal governance
"Policies" page as `D` (document, adjacent field) rather than `V`, and flagged
that **the must-find list and the stage's own returns both treat some of these
as in-field.** It also triaged a ~30-item Scheme-of-Amalgamation document index
down to 7 genuine certificate/order/compliance-report misses, excluding routine
M&A procedural paperwork.

This is a real question for the corpus, not a coding slip: is a company's own
incorporation certificate or anti-harassment policy a *conformity attestation*?
Plain reading says no — the field is about conformance to external standards
and certifications. If the eval set says yes, its conformity entries for this
subject are inflated and its recall figure is measuring the wrong thing.

Action at eval-set evolution: review pradeepmetals conformity entries sourced
from the Charter Documents and Policies pages; `disputed` any that are internal
governance documents rather than third-party attestations.

## The metrology split is now a LARGE number, not a footnote

`decimal_net / equipments`: **54 misses on 68 returned forms** — and the judge
says they are "mostly the 'Testing & Inspection Equipment' spec-table items
(CMM, Deltronic Pins, gauges, comparators, Virtek) that recur across four
windows but were never returned as forms."

So this judge counted metrology as in-field equipment (plain meaning), like
alecmfg's and unlike anchor-mfg's and blackadvtech's, which excluded it citing
my old brief wording.

Consequence: the equipments miss counts are not merely inconsistent in
principle, they differ by tens of records per subject. Running rough tally of
metrology-driven miss volume:

    decimal      54 misses, "mostly" metrology        -> counted IN
    alecmfg      16 misses, mostly metrology/tooling  -> counted IN
    anchor-mfg   17 misses, metrology deliberately excluded -> counted OUT
    blackadvtech 14 misses, metrology deliberately excluded -> counted OUT
    howcogroup   38 misses, incl. a full QC instrument list -> counted IN

**No corpus-wide equipments miss figure can be quoted until this is settled**,
and the settling is a definitional choice, not a measurement. The choice also
decides whether decimal's equipments looks like a 79%-miss disaster or a
mostly-clean field.

The mechanical fix once the definition is chosen is cheap: metrology misses are
identifiable by their entity names and by the judges' own notes, so they can be
filtered in or out of the miss set without re-judging anything.

## The cleanest intra-run-inconsistency case in the corpus — taylordunn products

Windows 2-5 of `taylordunn / products` are a ~250-entry dealer directory: the
same kind of text, repeated, page after page. Search caught `forklifts`,
`golf cars` and `low-speed-vehicles` out of dealer names and URLs **in window 3
and missed the identical pattern in windows 2, 4 and 5.**

Same entities. Same textual construct. Same subject, run, prompt, model. Only
the window differs. There is no boundary judgement, no eval-set opinion and no
field-definition ambiguity anywhere in this observation — which makes it the
best single piece of evidence for the finding.

Also from the same unit: two whole uncaptured product categories on the Manuals
page (`TOW TRACTORS`, `INDUSTRIAL CARTS`), and a cluster of vehicle safety
components (emergency off switches, shutoff timers, operator compartments,
adjustable seats, foot rests) covered by no returned form. 15 misses on 77
returned forms.

Sixth subject for this finding: fzemanufacturing, decimal, howcogroup,
steelcraft (Aug), alecmfg (spec-table across 3 windows), taylordunn.

## The pradeepmetals employment-history contamination is WIDER than August found

August's F5 recorded two pradeepmetals *products* entries sourced from a
person's employment history rather than the company's offering, and disputed
them. This census finds the same contamination still present in the must-find
list: a judge confirmed `furnaces` and `industrial gases` trace to **a
director's personal bio**, and correctly declined to manufacture misses from
them.

So the defect was disputed entry-by-entry in August rather than swept, and the
sweep was never done. Action for eval-set evolution: grep pradeepmetals'
expectation files for entries whose evidence quote sits in the directors'/board
biography section and dispute the class, not the instances.

Same judge also flagged, correctly per the recall-first rule, that Steelcraft's
stainless-steel line is now made by sibling company Next Door Company —
recorded via `actor: parent_sibling` rather than by dropping the entities.

## Inconsistency QUANTIFIED within a single unit, and a page whose subject was missed

`howcogroup / process_caps` (289 forms, 23 misses) gives the first per-unit
quantification of the inconsistency finding. The judge: heat-treat condition
rows — `Hardened and Tempered`, `Solution Annealed and Water Quenched`,
`Cold Worked`, `Annealed` — are "captured correctly in some windows but missed
in others for near-identical material spec tables," and **that one pattern
accounts for roughly a dozen of the unit's 23 misses.**

So on this unit, intra-run inconsistency explains about HALF the recall loss.
If that share holds elsewhere, it is the largest single recall defect in the
stage, and it is not a boundary problem.

Separately, the sharpest single miss found anywhere in the census:

> The Welding & Cladding page (window `73577:91419`) is titled and organised
> around **"Weld Cladding & Overlay Facility"** — and no returned form in that
> window contains the word `cladding` or `overlay` at all.

The page's own main subject, absent from the output. Not a boundary judgement,
not a fragment, not an actor question: the stage read a page about weld
cladding and did not say "cladding".

Also from this unit, worth keeping for the actor axis: forms attributing work
to third parties ("via our established supply chain partners", VIM/VAR/ESR
melt practices) were coded in-field with `actor: supplier`, exactly as
recall-first asks — the opposite of the howco *equipments* behaviour where
client/OEM equipment was dropped rather than flagged. Same subject, two fields,
opposite handling of third-party content. Worth checking whether the actor
filtering is field-specific.

## Packet-format hazard: numbered lines inside WINDOW TEXT look like form entries

The anchor-mfg products judge nearly miscounted part02 because the window text
contains a legal terms-and-conditions document whose own numbered clauses
(1-17) match the shape of the packet's numbered form list. It caught the false
positive by checking where the numbers sat rather than trusting a raw grep.

My own tooling is immune — `split_judge_packets.forms_in` and
`verify_census_coverage` both slice at `### The window text` before matching, so
body-text numbering is never scanned. But any agent or future script that
greps the packet whole will hit this. Worth a line in the brief next time:
the numbered form list ends at `### The window text`; numbers below that are
site content.

Eighth intra-run-inconsistency case, and among the cleanest: `tanfel /
process_caps` window `100894:121768` returned 55 forms and **none of them is
"Quality Control & Inspection Services"** — a phrase recurring under "Our
Services" on at least 7 pages inside that same window's text, and correctly
captured in every sibling window of the same packet. The judge called it "the
clearest recall gap found in either packet."

Same phrase, same site, same run, captured elsewhere, absent here.

## CORRECTION to my own claim: the stage CAN return nothing, and sometimes rightly does

I wrote earlier, on the strength of steelcraft, that "the stage never returns
nothing — asked about machines at a site with no machines it returns 78 doors
and frames rather than an empty list." That is false, and the control case is
in this same corpus.

`lucasmilhaupt / equipments`: **0 forms returned, 0 misses found.** A judge
swept both windows for machine/tool nouns (torch, furnace, CNC, mill, lathe,
press, induction, laser, robot, welder, microscope, gauge, pump, mold, die,
fixture, jig, chamber) and found none present. Verdict: correct restraint —
the silence matches the ground truth exactly.

So the two zero-in-field equipments subjects behave OPPOSITELY on the same
input condition:

| subject | equipment in text | stage returned | in-field |
|---|---|---|---|
| lucasmilhaupt | none | **nothing** | n/a — correct |
| steelcraft | none | **78 forms** (its own doors and frames) | 0% |

That reframes the F2 question from August. It is not "the stage cannot say
nothing." It is: **the stage says nothing when the page is thin, and
substitutes the subject's own goods when the page is rich but off-topic.**
Steelcraft's product pages are dense with door and frame nouns, so the
equipments request finds something to return; lucasmilhaupt's brazing-alloy
pages offer no near-miss vocabulary, so it correctly returns nothing.

The remedy implied is different too. An explicit "return nothing" instruction
would not help — the stage already does that when nothing is nearby. The
failure is substitution under pressure, which is a different prompt problem.

## Double-judge pass observations (2026-09-02, floor pending)

- **Consumables scope tension (blackadvtech material_caps judge2):** the
  must-find seeding counts process chemicals (pickle liquor, acids, shielding
  gases) as in-field material_caps, while the census code table routes
  `C consumable` to adjacent_field. Both instruments are internally
  consistent; they disagree about whether "materials the process consumes"
  are a material capability. USER QUESTION for eval-set evolution, same
  class as the settled metrology fork.
- **Packet-design flaw reconfirmed at source:** the judge2 on blackadvtech
  found the same 16 unreachable must-find entries the primary did (they live
  in the 30% of text outside the read windows) and declined to fabricate.
  The per-part must-find filtering fix recorded earlier stands.
- **Expected-empty packets print no must-find section at all** (acimachine
  industries judge2 wondered whether it was dropped): export should print an
  explicit "no confirmed entries for this unit" line instead of silence.
- API timeouts killed 4 of the first 12 judge2 agents mid-task with NO
  partial file (clean failures); retries carry an incremental-write
  instruction so a crash loses one window at most.
- **pradeepmetals industries needs the same bio-sweep as its products file
  (judge2, named entries):** must-find entries seeded from director
  biographies — `BFSI, FMCG, hospitality, retail pharma, media and
  advertising`, `electrical and electronics`, arguably `heavy engineering` —
  the exact F5/employment-history class, now in a third field. Also 61% of
  the unit's RETURNED forms are bio-sector prose (judge2 coded W/G where the
  primary leaned V: expect this unit to drag the exact-agreement floor).
- **Judge2 "unreachable" claims need the reserved test before any dispute:**
  `aerospace`, `construction equipment`, `dairy` are absent from every
  window of the pradeep industries packet — but per the user's 2026-09-01
  ruling that is only a defect if the quote is absent from the FULL site
  text; absent-from-windows-only means reserved (beyond read coverage).
  Check quotes against full text at eval-evolution time; only full-text
  absences join the CSA/CUS dispute class.
- **FLOOR MEASURED (2026-09-02): 74.1% exact / 87.6% bucket over 1,402 forms,
  12/12 units coverage-verified.** 85.4%/91.9% excluding the two industries
  units, whose spread is definitional (dealer-catalog codes W vs O; director-
  bio sectors V vs W/G at pradeepmetals 50.4% exact) — settle the bio ruling
  before quoting industries precision. agstech judge2's "16 of 60 must-finds
  unreachable" is the packet-design flaw (full list shown against 14%-read
  coverage): apply the reserved test, not the CSA/CUS dispute, to those.
