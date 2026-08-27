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
