# Candidate dimensions — awaiting promotion into the rubric

Defect classes found by judging that no existing dimension names directly.
They are recorded HERE rather than in the rubric files on purpose:
`taxonomy_version` is the hash of TAXONOMY.md, so editing it invalidates
every cached verdict for that field. Promote in a
**batch, between runs**, then accept that the affected fields re-judge.

Each entry: the mechanism, the verified example, and which existing dimension
currently absorbs it (so nothing is lost before promotion).

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

## Pending TAXONOMY.md edits (batch these with the promotions above)

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
