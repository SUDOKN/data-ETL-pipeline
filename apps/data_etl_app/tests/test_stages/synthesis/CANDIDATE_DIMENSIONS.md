# Candidate dimensions — awaiting promotion into the rubric

Defect classes found by judging that no existing dimension names directly.
They are recorded HERE rather than in the rubric files on purpose:
`taxonomy_version` is the hash of TAXONOMY.md, so editing it invalidates
every cached verdict for that field. Promote in a
**batch, between runs**, then accept that the affected fields re-judge.

Each entry: the mechanism, the verified example, and which existing dimension
currently absorbs it (so nothing is lost before promotion).

**Promoted 2026-09-10 (the taxonomy batch bundled with the synthesis focus
run, design doc `docs_local/SYNTHESIS_GROUNDING_REDESIGN_2026-09-10.md` D9,
D17):** C4, C6, C11 and C12 became **J7 — Agent laundering** in TAXONOMY.md;
the three pending edits at the foot were applied (products document-title
bullet, J4 parent-policy note, severity per record), plus the J1
grounded-import / bleed split and the J2 own-designations clause. Their
entries below are kept as the case history. C10 (retry-path register drift)
stays a candidate: the trigger-scope change of the same date is expected to
take the retried population from 1,593 records to ≈30, and the class is
re-measured on the next run before deciding whether it needs a name.

## C1 — Fragment completion
**Mechanism:** when a snippet is a truncated, subject-less sentence fragment,
the synthesizer invents a subject and asserts the completed sentence as fact.
**Verified example** (steelcraft/industries `gbp366rp`, run 194457): evidence
fragment "account for one-third of our total energy, two-thirds of our
electricity and one-eighth of our water." → paragraph: "energy accounts for
one-third of Steelcraft's total energy use" (also silently resolving "our" to
the subject). Confirmed: the record's own entries never support the completed
claim.
**Currently absorbed by:** J1 (minor). **Why it deserves its own name:** the
trigger is upstream snippet truncation, so the fix is a clipping-radius
question, not a prompt question — counting it inside J1 hides that.

## C2 — Cross-entity anaphor / contrast capture
**Mechanism:** during multi-snippet fusion, a pronoun or a contrast clause
whose antecedent is a *different* entity is absorbed into the focal entity's
description. Identity stays correct; a neighbor's attribute migrates in.
**Example** (steelcraft/equipments `gbubf8pp`): the GRAINTECH-finish snippet's
"It" is re-attached to the L Series, yielding an availability claim the
evidence gives to the finish. Contrast variant: `gjrirs9y`, where "similar to
K Series frames, **but** anchored also through..." is absorbed as K Series'
own installation method.
**Currently absorbed by:** J1 (minor). **Distinct from** the FE→DE swap (J3),
where the whole paragraph is about the wrong entity.

## C3 — Cross-record evidence bleed within a co-pack
**Mechanism:** a paragraph borrows a factual detail visible only in a
*co-packed sibling's* evidence. The claim may be true of the world, but it is
untraceable from the record's own entries — which is all any downstream
auditor can check.
**Verified example** (steelcraft/material_caps `gzejq7ow` "steel"): paragraph
claims "security ... and vandal resistance"; the steel record's entries
contain neither word; the co-packed `wood` record's entry contains exactly
"security, fire ratings, sound reduction, vandal resistance". Confirmed by
diffing the two records' evidence.
**Currently absorbed by:** J1 (minor). **Why it matters separately:** it is a
packing artifact — evidence of leakage *between* records in one request, the
same channel that produces identical-synthesis collapse and the FE→DE swap.
Worth measuring against `max_entries_per_request`.

## C4 — Educational-copy use-inference
**Mechanism:** generic definitional/tutorial copy on the subject's own service
page becomes a "the company uses X" claim.
**Verified example** (alecmfg/equipments `gvoll26g` "CNC machines"): two
textbook sentences about what CNC machines can do become "only using them in
their services"; no entry says the subject runs, owns or installed one
(checked: no first-person ownership language anywhere in its evidence).
**Currently absorbed by:** J6 (unclear). **Why it matters:** it is the
equipments fabrication inference one stage earlier than grounding, arising
from educational copy rather than product listings. Here it was hedged; on a
less careful subject it would assert.

## C5 — Disjunctive-hedge smuggling
**Mechanism:** an otherwise correctly attributed paragraph closes with a
two-branch hedge where one branch is unsupported ("specifying **or
providing**", "used **or recommended**", "provides **or arranges for**").
**Example** (alecmfg/products, 3 occurrences in one slice): the client
specified the finish; the paragraph offers "specifying or providing" as
alternatives, licensing the stronger reading.
**Currently absorbed by:** judged `pass` (attribution preserved, no wrong tag
follows). **Why record it:** a template tic that widens claims at zero
evidential cost; if its rate grows it becomes a real J4 leak.

---

*The classes below came out of the 2026-09-05/06 run `20260905T213127`
(18 subjects × 7 fields, the first run on the flattened snippets+chunk-text
wire). Each carries a regression probe in `expectations/<subject>/<field>.yaml`
added 2026-09-06.*

## C6 — Group-membership misattribution
**Mechanism:** a sentence of the shape "[subject] joins N other companies who
⟨verb1, verb2, verb3⟩" gets the *collective's* varied activities attributed to
the subject itself. Distinct from the parent-brand (Allegion) class: no
corporate relationship is involved, only a relative clause whose head is the
other members.
**Verified example** (fzemanufacturing/process_caps `glq7xi3z`, `gepiw7bm`,
`go6d8frw`): evidence "we join nearly 250 other companies who grow, brew,
build, engineer, and otherwise produce in this great state" (a Made-in-
Wisconsin badge) → "FZE Manufacturing Solutions is recognized as a company
that grows/brews/engineers". FZE's only stated act is "join"; a metal-
machining shop does not farm or brew.
**Currently absorbed by:** J1 + J4 + J6, all major. **Why it deserves a name:**
the trigger is a grammatical one (relative-clause head), so it is testable
mechanically and will recur on every membership badge, award list, and
association page.

## C7 — List-register conflation
**Mechanism:** two same-worded bullets that live in *different* list registers
on one page — a product catalog and a served-sector list — merge into a single
paragraph, which then asserts the wrong axis for both.
**Verified example** (agstech/industries `g8fmtbw1`): "- Construction Tools"
(from "Private Labeling & White Labeling Your Products") merged with
"- Construction" (from "...the following industrial sectors:") → a paragraph
calling Construction a private-label product. The real served-sector fact is
erased and inverted.
**Currently absorbed by:** J1 + J6 major. **Why it deserves a name:** the
evidence array flattens away the list each bullet came from, so the judge (and
the model) can only recover the register from the chunk text — an evidence-
shape problem, not a reasoning one.

## C8 — Contrast and negation inversion
**Mechanism:** evidence that names an entity in order to EXCLUDE or CONTRAST it
is read as evidence for it.
**Verified examples:** (fzemanufacturing/process_caps `gpa4z2mo`) "shaft
machining uses more rigid materials than those used for casting and forging" →
"FZE includes casting as one of the methods"; (tanfel/material_caps
`gr5tcz7r`) "Chrome-free passivation" → "Tanfel offers Chrome as a finish
option". Both mint a wrong tag.
**Currently absorbed by:** J1 major (and J6 when the tag follows). **Why it
deserves a name:** the same one-token flip (than / -free / instead of) drives
both, and its rate is a clean prompt-level measurement.

## C9 — Sibling paragraph copied under a derived form
**Mechanism:** when search carves a broader focal form out of a longer name,
the synthesizer reuses the narrower sibling's paragraph near-verbatim instead
of saying what it actually has.
**Verified example** (fzemanufacturing/industries `gbp366rp`): the "Energy"
record repeats the "Department of Energy (DOE)" record's paragraph, though no
standalone "Energy" evidence exists. Contrast the same run's "Defense" record
(`g2n7t6j4`), which generalizes openly from DOD + Military/Defense and is
correct.
**Currently absorbed by:** J3 (identity), severity depends on whether the
implied tag survives. **Why it deserves a name:** it is the identity failure of
the *search* stage's over-segmentation, and the correct behaviour (name the
narrower evidence) is already demonstrated by sibling records — a directly
promptable contrast.

## C10 — Retry-path register drift
**Mechanism:** the pipeline's own under-enumeration retry re-asks a record and
the second answer systematically differs in register from its non-retried
siblings — in three directions seen this run: hedges collapse (a claim the
sibling hedged is asserted), the document rule retreats (a listing the sibling
read as "names X as available" becomes "offers a document listing X"), and a
closing template launders feedstock into a supplied product.
**Verified examples:** (blackadvtech/products `ga39any9`, `ghv5wv2c`) retried
records attribute reader-directed how-to content to the subject while every
non-retried sibling hedges; (tanfel/products `g173ibh6` vs `gcbaf7zo`) the same
alloy listing reads "offering a document that lists X" in the retry batch and
"naming X as an available alloy" in the original; (tanfel/products `g8sq9ukm`,
`gkqhvubm`) "Tanfel's dealing with Tool Steels (H13, D2) is as a material
supplier and manufacturer" for table-column feedstock, 9 records.
**Currently absorbed by:** J1/J2/J6, mostly minor. **Why it deserves a name:**
run-wide the retried population fails at roughly three times the rate of the
rest (containment 8.8% vs 2.7%, any-fail 19.6% vs 5.8%), so retry is a
measurable quality axis of its own, and the retry is a PIPELINE behaviour the
eval can price.

## C11 — Application-list inversion
**Mechanism:** "our ⟨material/part⟩ is used in ⟨X, Y, Z⟩" — a list of the
customer's end products — becomes "the subject provides/manufactures X, Y, Z".
**Verified example** (howcogroup/products, 20 records in one slice): the
alloys' application list mints Pumps, Flow Control Equipment, Crackers... as
Howco's own products; inconsistently, within one source sentence. Same shape
in fzemanufacturing/products (`gfic0hx9` power tools) and superiortech/products
(`gxz5ck5o` Medical Equipment).
**Currently absorbed by:** J1 + J6 major. **Why it deserves a name:** it is the
single highest-volume wrong-tag mechanism measured this run, and it is one
preposition ("used in" vs "makes") away from correct.

## C12 — Educational-copy laundering at scale
**Mechanism:** C4's use-inference, but from third-person or second-person
*advice* content (a listicle, a glossary, a "why X matters" post) that names no
party at all, and across many records at once because one such page seeds a
whole slice.
**Verified examples:** (fzemanufacturing/equipments `gk8ic02c`, `gun1b6ge`,
`g0juzygs`) one unattributed "Common metal fabrication processes" bullet yields
three claims that FZE operates press brakes and CNC back gauges;
(blackadvtech/process_caps, parts 2/3/4) one shared "tips" listicle laundered
into own NDT/Lean/QMS practice in 20+ records, plus an anonymous example shop's
"they reduced turnaround by 25%" resolved to the subject.
**Currently absorbed by:** C4 (unpromoted) and J1/J6. **Why it matters now:**
C4 was recorded as a single hedged instance; this run shows it asserting, in
volume, on subjects whose sites carry a blog.

---

*The classes below came out of run `20260911T003500` (2026-09-11, the
focal-form statics; 10,467 records judged, 831 verified). Each carries a
regression probe in `expectations/<subject>/<field>.yaml` added 2026-09-11.*

## C13 — Document rule over-applied to the subject's own catalog
**Mechanism:** the statics' document rule ("a title in a list of literature,
downloads, brochures ... shows the manufacturer offering a document and
nothing further") is applied to evidence that is NOT a literature list: the
subject's own product-catalog bullets on a page that says they are products
for sale, case-study titles whose own words name the subject's work on the
part, materials-catalog grade headings, and certificate download links. The
paragraph concludes "no dealing beyond a document bearing its name" and a
real tag is lost.
**Verified examples:** agstech/products `gnl9lsyt` + 17 siblings ("ready
products ... let us know which ones you would like to purchase" → "offers
documentation for X"); alecmfg/products `gzkbdi1z`, `gn6we4ig`, `gsvxzo4k`,
`gtvcp7f5` (case-study titles "Custom CNC Machining of Aluminum Mounting
Brackets ..." → "no dealing beyond a document title"); howcogroup/
conformity `g3f931vc` (`## LOW ALLOY 4145: ASTM A29` read as a document in
one chunk and as supply-to-spec in another); anchor-mfg/conformity
`gir8j6tq`, `gg5gsbjm`, `ggozr8r9` ("Click Here to View X Certificate" →
"offers access to a downloadable document"). 65 rows stand after
verification; 5 rows were the rule correctly applied.
**Currently absorbed by:** J2 + J6, major when the tag is lost. **Why it
deserves a name:** it is a one-paragraph statics issue (the rule needs "and
the page frames the list as documents, not as things it sells or makes"),
and it is the largest single source of majors in the conformity field.

## C14 — Scrape fusion pseudo-entities (upstream)
**Mechanism:** adjacent table cells or bullets concatenated without a
separator by the Markdown extraction become one focal form, and the model
faithfully describes a thing that does not exist.
**Verified examples:** decimal/process_caps `g1iynzwu` "Hard Coat Barrel
Plating", `gwt0xb1b` "Cadmium Chromate", `gldui96i` "Copper Etching" (the
Metal Stamping page renders the same list as separate bullets);
pradeepmetals/conformity `gjtut4id` "PED 2014/68/EU,AD2000-MERKBLATT W0"
read as one certifying the other; decimal/industries `gxszk92j` "Defense
Electronics" (two alphabetized cells).
**Currently absorbed by:** J1 major (the model's paragraph is faithful, the
entity is fake). **Why it deserves a name:** it is a scraper/fold defect the
synthesis eval keeps paying for; the fix is in `html_to_markdown.py` /
the fold's cell handling, not in a prompt.

## C15 — Homonym bridging into a served sector
**Mechanism:** an industries focal form that is also a word in the subject's
own process or department name ("Energy" ← "Energy Beam Welding";
"Laboratory" ← "Quality Laboratory"; "Packaging" ← the shipping service)
gets its served-sector claim padded or invented from the subject's own
activity.
**Verified examples:** fzemanufacturing/industries `gbp366rp` (major, no
checklist support for an energy sector at all), `gme3cycj`, `gqc7n8x2`,
`ggasj17x` (minor, real tag padded); howcogroup/industries `gbp366rp`
(ISO 50001 energy-management certification folded into the Energy sector).
**Currently absorbed by:** J1/J6. **Why it deserves a name:** the sibling
records that disambiguated transparently (`g8in1rji` "Department of Energy
(DOE)") show the correct behaviour exists in the same run — a promptable
contrast, and a mechanical nominator (focal form ⊂ an own process/department
name) is cheap.

## Watches confirmed on this run (not defects)
- **Grounded import** (J1 pass + note): 210 of 10,467 judged records; flips
  between identical-evidence twins fell from 19/210 to 1/214.
- **Retry-path drift (C10):** the population the old trigger re-asked failed
  at 26.5% on the baseline and 5.9% here; 82 records were re-asked this run
  (was 1,593). C10 stays a candidate until a run with a non-trivial retry
  population exists again.

## TAXONOMY.md edits — APPLIED 2026-09-10 (kept for the record)

All three landed in the 2026-09-10 batch, together with the J1
grounded-import / bleed split (retry memo, Discussion 2026-09-07 §1) and the
J2 own-designations clause (design doc D17). Nothing below is pending.

1. **The products addendum contradicts itself on document titles.** Its bullet
   says a document written as a product fails J6 "even when correct scoping
   cannot save the record", which reads as: a correctly scoped document title
   still fails. Practice across run `20260905T213127` went the other way in
   about 470 rows judged by both models — a correctly scoped "offers a document
   titled X" is a J6 **pass** carrying `not_a_product: true`, matching the
   general rule that faithfully transmitted noise passes with a note. Three
   outlier rows were re-graded to pass at verification (tanfel/products part5
   ×2, mathewsco/products "Brochure from 1952"). Rewrite the bullet so J6 fails
   only when the paragraph presents the document AS a product.
2. **Name the parent-policy case in the J4 addendum.** A policy the PARENT
   issues for its brands' suppliers, hosted on the subject's site with the
   parent as speaking voice, is J4 minor / J6 pass (taylordunn/Waev). It is
   distinct from a credential the parent HOLDS being credited to the subject
   (the Allegion class), which stays major.
3. **Say that severity is per RECORD, not per dimension.** One defect commonly
   trips J1 and J6 together; reports should count records with a major, and the
   scorecards should say which they mean.

Editing TAXONOMY.md changes `taxonomy_version` and empties the verdict cache
for every field, so make these edits in the window right after a synthesis
statics republish (which empties the cache anyway), never mid-pass.

## Candidates from run 20260911T223222 (run A, the shared block) — added 2026-09-12

- **C16 — Own-listing hedge (the "capacity unstated" over-fire).** The statics'
  new sentence ("where the snippets show a dealing but leave its capacity open
  … say the capacity is unstated") fires on bare items under the subject's OWN
  introducing sentence or page frame ("Our Capabilities:", "we provide the
  following services:", "Our inventory … includes:", a grade heading on the
  own materials page, an own badge): 160 records (84 major), the whole of the
  J2 rise (107 → 178). Two shapes: REFUSES any dealing (major) vs ASSERTS the
  dealing and hedges the in-house/partner or make/source MODE (minor). Judged
  under J2+J6 with `frame_rule: violated`; probes TF-P6, SC-P9. Promotable as a
  J2 clause ("a list item takes its introducing sentence's dealing") rather
  than a new dimension.
- **C17 — Frame named, wrong page.** Requiring the paragraph to name the frame
  created a minor class: gallery hub named for its sub-gallery (agstech ×17),
  Facilities pages placed under "OUR PRODUCTS" from a flat nav string
  (pradeepmetals ×22), a bio under "### clientele" named as "Our Team"
  (blackadvtech). ≈62 J1 minor; attribution intact. Probe PM-P3. Mechanical
  nominator possible: paragraph names a heading absent from the record's
  `locations`.
- **C18 — Wire vocabulary in the paragraph.** "The focal_form …" / "The focal
  entity …" openers (67 rows noted; 4 in conformity, 2 in taylordunn
  process_caps). Not a fail; a watch on the statics' vocabulary. Probe TD-P4.
- **C19 — Closer restates a hedge as unconditional.** The formulaic closing
  sentence ("This shows … holds AS9100") collapses a disjunction or quantifier
  kept mid-paragraph. Probe AG-P9. The tryout judge predicted it (JUDGE_RUN_A.md
  §7: the opener/closer template fires 39/39 on the weak model).

- **C20 — Requirement rows read as delivered work, and the mirror.** A
  case study's client-requirement / specification table row ("Weld map",
  "WPS/PQR", "matte black anodized", "fully machined") is written as a
  standing service the subject provides, or, in the mirror, a row the chunk
  narrates as delivered is refused as "requirement only / capacity
  unstated". alecmfg process_caps, run 20260911T223222 (J17/J18 + packet 09;
  stage-2 `65feebc0…`, `ea18b271…`, `ce261d70…`). The two readings split
  across sibling records of one subject. Ruling recorded in the template
  (delivery narrated in the chunk → capability; otherwise a requirement).
  Candidate for a probe on alecmfg process_caps.

- **C21 — Sales-rep entries laundered under the list rule.** On a
  manufacturers'-representative site, product-line entries whose own words
  put the doing with an unnamed principal ("This American manufacturer…",
  "They offer…", "Their equipment list includes…") are re-narrated as "the
  subject offers X" once the list rule licenses the introducing frame:
  mathewsco process_caps, run 20260912T191548, 17 J1+J6+J7 majors (run A: 5).
  The doer clause held on the production-model tryout (39/39) but not on
  this packing. Candidate fix: the judge's R2 of JUDGE_C16.md §7.6 (pin the
  doer rule's precedence over the list rule) — needs a tryout on THESE
  requests. Probe MW-P3.
- **C22 — Bare-heading index and cross-link hedge (the C16 residue).** Items
  under a bare-noun capabilities heading (`## Metal`, `## Additional
  Capabilities`) or a "Related Products: X" cross-link on the subject's own
  site are still hedged "capacity unstated" / "no dealing beyond the
  listing" (tanfel process_caps 10, tanfel products 7, alecmfg material_caps
  18 minor under a sentence that names the processes, not the materials).
  Candidate fix: the judge's R1 (a carve-out that reads the entries and
  names the bare-noun index). Probe TF-P7.
- **C23 — Internal record id leaked into prose.** "This snippet repeats the
  content of record gms7el81" (taylordunn process_caps g38bfn9n; agstech
  process_caps gqic2e33): the wire-vocabulary ban does not cover ids. Watch.

## Candidates from run 20260913T023316 (the packing run, cap 10, mathewsco + tanfel) — added 2026-09-13

- **C24 — Subject-elided product-line fragment laundered (a C21 sub-shape).**
  A line-card entry whose sentence has no subject at all ("With manufacturing
  and being a distributor of elastomeric rubber, silicone materials, FDA
  approved Buna, Cellular foam material", mathewsco Kansas Product Line
  kpl05) gets the subject written in as the manufacturer/distributor:
  material_caps g1qicz86, g4ee3bu2, gh0myffg, gjw3b955, gvlgc4m0, g06pxo08
  (6 J1+J4+J6+J7 majors), while conformity_attestations gcjldsq7/geqo5h8i
  read the same sentence correctly as a third party's. Pronoun-bearing
  entries on the same page ("They offer…") were attributed correctly in the
  same run. Probe MW-P4. Same fix family as C21 (the doer rule), and the same
  per-request coin: the six sit in one 8-record request.
- **C25 — The snippets stop short of the sentence that carries the dealing
  (evidence stage, not synthesis).** A testimonial's first sentence is the
  record's snippet while its attribution and thank-you ("- John Marcin,
  President, ROJO Sport, Inc. … Thank you for delivering quality products
  ahead of schedule!") sit two lines later in the chunk: the paragraph
  faithfully declines any Tanfel dealing and a well-evidenced served sector
  is lost (tanfel industries g58vzo7i, gx5j2vzn and their chunk-2 twins).
  Same shape on heading-only occurrences whose page states "Tanfel Metrology
  offers … selling directly to our customers" (products gylk3k14, gzco65b9).
  The fix belongs upstream (snippet radius / search), and the judge must not
  fail the paragraph for it. Probe TF-P8.

## Candidates from run 20260913T170246 (run X1, the label wire; all 18 subjects) — added 2026-09-14

- **C26 — Customer-equipment feature list laundered as the subject's own
  process.** A bullet list of automation features the subject can build INTO
  equipment it makes for customers ("automation types we can incorporate
  into YOUR custom made equipment: Oiling, Surface Finishing, Painting,
  Coating") is written as "the subject provides X" — a shop process the
  customer's plant performs with purchased equipment: agstech process_caps
  gt8h1ms7, gxlo64tk, glnxw175, gv71ltyl (verified J1+J4+J6+J7). The
  machine-builder variant of the use/application laundering shape. Probe
  AG-P10.
- **C27 — Nested-list attribution blindness (a C25 sub-shape).** A
  compliance or capability claim that is a sub-bullet or adjacent sentence
  under an explicit first-person parent line ("Steelcraft offers a wide
  variety of hurricane certified doors and frames … tested with…"; "Approved
  Door series: H / HE / TH … Tests include the following protocols") is
  refused when the parent line is not in the record's own snippet, although
  the chunk text the model also read settles it: steelcraft conformity
  gprjb7bo, gwfr852u, gm763gfz, g1eet2qw (four judge-self-flagged passes,
  all verified J2+J6 major); the same shape on 11 of 12 siblings of the SDI
  Membership cluster, and in industries (gotfeskc, gwlu4iz5). Fix belongs
  upstream (snippet radius) or in the frame paragraph; probe SC-P10.
- **Not a class, recorded for the revert:** the decide-first default flowing
  into the prose (`doer: nobody` / `capacity: none` → a shape-(a) refusal on
  the subject's own capability, materials, approvals and gallery pages) is
  the LABEL wire's own defect (design doc §37) and disappears with the
  revert; it is why C22 became the run's dominant class.

## Watches confirmed on run 20260913T170246 (not defects)
- The rep-site laundering coin unchanged (mathewsco process_caps 25 this
  draw vs 18 / 30 / 0 on the three cap-50 draws); twins 8/214 flip; mixed
  clusters 34 → 65 — the request-level mode, now with the labels riding it.
- Judges' most-missed rules, again: the field-membership gate (a standard
  or spec judged in products; a fragment split from a compound; a process
  judged in products), the refuse-vs-hedge severity split, laundering
  scored on J6+J7 only (J1+J4 added by the verifiers on 33 rows).
- Custodian job-posting duty bullets (blackadvtech products, 24 rows) are
  NOT products defects: the paragraph names the posting and keeps the
  dealing internal; verified pass.
- Upstream notes from the judges: `group_id` is not unique across a
  subject's two chunks (the same hash names unrelated groups — probes must
  target by content, not id); evidence snippets stop one sentence short of
  the attributing line (C25) far more often than the taxonomy anticipated;
  process_caps and material_caps carry 15–25% non-member focal forms
  (machines, consumables, spec rows) that the paragraphs handle faithfully.

## Watches confirmed on run 20260913T023316 (not defects)
- The per-request mode at 4 records per request: 81% of failing records sit
  in majority-failing requests (`checks/request_mode_readout.py`), 18
  whole-request fails; mixed identical-evidence clusters 2 → 17. Tooling
  notes from the judges: the code-derived `locations` pointer names a pipe
  table's first row for its later rows (tanfel process_caps gq7jqu1f + 5);
  a compound phrase's substring nominated as its own record ("Metal
  Fabrication" from "Sheet Metal Fabrication", gyzids48); one group id
  nominated in two fields (gljaee1t in equipments and industries).

## Watches confirmed on run 20260911T223222 (not defects)
- Grounded import 269 of 10,055; twin flips 5/206 (was 1/206 — UP, the
  sibling inconsistency the stage-2 graders could not score); mixed clusters
  27 → 32; J3 0.
