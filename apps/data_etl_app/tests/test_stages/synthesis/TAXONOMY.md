# TAXONOMY — synthesis-stage judgment codes

The dimensions every synthesis record is judged on, and the per-field
extensions. This file IS the eval set's judgment contract: its content hash
is the `taxonomy_version` stamped into every verdict, so **editing it
invalidates that field's cached verdicts**. Batch edits between runs.

Candidate classes not yet promoted into a dimension: CANDIDATE_DIMENSIONS.md.

---

You are judging one synthesis **record**: a paragraph an LLM wrote about one
extracted entity (the `focal_form`), using only the `evidence` entries
(each a verbatim `snippet` from the manufacturer's website plus an optional
`location` sentence describing where the snippet sits). The paragraph is the
ONLY thing downstream stages see — they cannot see the site. Judge the
paragraph against the evidence, never against your own world knowledge of the
company.

## Dimensions

Score every record on each dimension below. Verdicts: `pass`, `fail`,
`unclear` (use `unclear` when the evidence genuinely underdetermines the
judgment — it is not a soft fail). Severity on fails: `minor` (blemish, tail
could still act correctly) or `major` (a downstream stage acting faithfully
on this paragraph would produce a wrong tag).

### J1 — Faithfulness (no overclaim)
Every claim in the paragraph must be traceable to the entries. A `fail` is a
statement of fact the entries do not support: an invented capability, an
unevidenced generalization ("offers a full range of..."), a listing upgraded
to a delivered service, or hedges dropped from hedged evidence. The dealing
rule permits "the manufacturer offers X" when the evidence is X on the
subject's own product/service page — that is a supported reading, not a fail.
A claim scoped weaker than the evidence is J2's business, not J1's.

### J2 — Under-claim (evidence left on the table)
`fail` when the entries support a materially stronger TRUE statement whose
omission would cost a real tag downstream. The type case: evidence lists
"ISO 9001:2015" under the company's own Certifications section and the
paragraph says only that the string "is listed", so grounding correctly
declines a real certification. Ordinary conservative phrasing that still
carries the fact is a `pass`.

### J3 — Entity identity
The paragraph must be about the focal entity and distinguishable from its
co-packed neighbors. `fail` (always `major`) when the paragraph names or
describes a DIFFERENT entity than the focal form (the FE-Series→DE-Series
swap class), or when it is a copy of another record's paragraph AND wrong for
this record. An accurate composite sentence shared by two records (one true
sentence naming both) is `pass` here — note it under `undifferentiated: true`
in the note instead.

### J4 — Party attribution
Every claim must be credited to the right party: the manufacturer itself, a
parent or sister brand, a client, a certification lab, a publication, a job
applicant. `fail` when the paragraph transfers a third party's property to
the manufacturer (crediting the parent company's LEED credits to the
subject), converts a client's requirement into the subject's certification,
or presents a sister brand's product as the subject's own. Naming the
manufacturer is CORRECT behavior (the own-name ban was retired 2026-08-24);
never flag the name itself.

### J5 — Claim scoping
`fail` when a special evidence shape is mis-scoped: a document title claimed
as a product/service instead of "offers a document titled X"; a client
requirement stated as the subject's practice; an explicit negation in the
evidence ("we do NOT machine automobile parts") dropped or inverted; content
from a job posting, legal page or news item stated as a shop-floor
capability.

## Discipline

- Quote verbatim: every `fail` carries a short quote from the paragraph AND
  the entry that convicts or fails to support it.
- Count the `location` text as evidence, not just the snippet (a heading like
  "More from Allegion" is provenance).
- Do not judge with regex or keyword matching; read the record. Enumerator
  hits handed to you are nominations, not verdicts.
- Rule explanations elsewhere in the pipeline are post-hoc; nothing outside
  the paragraph + entries is admissible for these dimensions.
- When in doubt between `fail`/`unclear`, ask: "would a faithful downstream
  reader of this paragraph produce a wrong tag?" Yes → fail. Can't tell →
  unclear, with the reason in `note`.

## Output schema (one JSON object per record, one per line)

```json
{"content_key": "<from the work order>", "subject": "...", "field": "...",
 "group_id": "...", "chunk_bounds": "...", "focal_form": "...",
 "taxonomy_version": "<from the work order>", "pv": "<from the work order>",
 "evidence_sha256": "<from the work order>",
 "first_judged_run": "<run id>", "judge": "agent:<one-phrase self-description>",
 "checks": {
   "J1": {"verdict": "pass", "severity": null, "note": "", "quote": ""},
   "J2": {"verdict": "pass", "severity": null, "note": "", "quote": ""},
   "J3": {"verdict": "pass", "severity": null, "note": "", "quote": ""},
   "J4": {"verdict": "pass", "severity": null, "note": "", "quote": ""},
   "J5": {"verdict": "pass", "severity": null, "note": "", "quote": ""},
   "J6": {"verdict": "pass", "severity": null, "note": "", "quote": ""}
 },
 "verified": false}
```

Add a `"PROBE:<probe id>"` entry to `checks` for every probe in the work
order whose target records include this one, with the same verdict shape.

---

# Per-field extensions (J6)


## products — field addendum (J6)

The field asks: what does this company MAKE and SELL as its own?

### J6 — Field serviceability
`fail` when a faithful downstream reader of this paragraph would tag as an
own product something that is not one, or miss one that is:

- A CLIENT's product (a contract-manufacturing case study describes the
  client's part) written as the subject's own product line.
- A sister/parent brand's product on the subject's site (a Falcon SZ Series
  page on Steelcraft's site) written as the subject's own.
- A document, brochure, or portfolio PDF written as a product (the document
  rule asks for "offers a document titled X" — J5 covers the scoping; J6
  fails when even correct scoping cannot save the record because the focal
  entity is not a product at all).
- A process, material, spec, or facility written up as if it were a product.
- Dealer/distributor stock (equipment carried for RESALE) written as the
  subject's manufactured product — decisive on dealer-shaped subjects.

Search precision for this field measured 37% — expect many records whose
focal entity is simply not a product; say so in the note (`not_a_product:
true`) even on a pass, so the noise share stays countable.


## contract_products — field addendum

Through the synthesis stage this field is a byte-copy of `products` (one
shared LLM call). It is never judged separately: its scorecard verifies the
byte-copy invariant and points at products. If you were handed a
contract_products work order, something upstream broke — stop and say so.

(The products/contract distinction is decided downstream, at screening. The
field-serviceability question for the CONTRACT side — "does the paragraph
preserve who ordered what from whom?" — is exactly J4 on the products rubric;
judge it there.)


## equipments — field addendum (J6)

The field asks: what machinery does this company OWN and OPERATE on its own
floor?

### J6 — Field serviceability
The one-field regression class (run 20260825T194457): grounding inferred
"machines that must exist to make what they sell" — 9 fabricated
`<door type> manufacturing machine` tags from product listings. The synthesis
must not license that inference:

- `fail` when the paragraph presents equipment-for-SALE, equipment mentioned
  in a client's spec, or equipment implied only by products sold, in the same
  register as owned equipment — a faithful reader must be able to tell
  "operates a Chevalier EM2040L" from "sells JET bandsaws" from "makes doors
  (no machine named)".
- `pass` requires that any operated/owned framing in the paragraph is
  evidenced (the site says they run/use/installed it — a shop-floor page, a
  news item about a purchase, a capabilities list of machines).
- A named machine model in a press/news blurb ("Brandon clearly loves his new
  toy" about a purchased mill) IS ownership evidence; note the informal
  register rather than failing it.

Upstream contamination is worst here (68/75 steelcraft "equipment" groups
were products). As with products, mark `not_equipment: true` in the note when
the focal entity is not equipment at all.


## industries — field addendum (J6)

The field asks: which SECTORS does this company SERVE — its customers'
industries, not its own activity.

### J6 — Field serviceability
The axis error runs in both directions (SCR-2 was measured killing real
client sectors while ratifying own-activity tags):

- `fail` when the paragraph frames the subject's OWN activity as a served
  sector ("CNC machining" or "machine shop" written as an industry served),
  or leaves the reader unable to tell which side of the relationship the
  sector name sits on.
- `fail` when a real customer sector in the evidence (a UAV client, a rail
  operator, "serving the nuclear power industry") is written so weakly that
  the served-sector reading is lost.
- The client-approaches-shop shape is EVIDENCE FOR this field, not against
  it: "a French UAV client came to us" supports serving aerospace/UAS. A
  paragraph that preserves that provenance passes; one that erases it (or
  flips it into the subject BEING a UAV maker) fails.
- Note (not fail): sector names appearing only in boilerplate slogans ("We
  Lead the Industry in Efficiency") — the entity is not a served sector.


## conformity_attestations — field addendum (J6)

The field asks: which standards/certifications does this company CLAIM
conformity to, as attested on its site?

### J6 — Field serviceability
The paragraph must preserve the exact strength and owner of the attestation:

- Distinguish, in the writing: certified/audited ("SDI Certified via audits",
  a certificate download of their own) vs. claims-compliance ("meets ANSI/SDI
  A250.8") vs. product-tested-to (an Intertek report) vs. mentioned/contextual
  prose about a rating system (LEED explainer text) vs. a CLIENT's or
  APPLICANT's requirement (a job posting demanding EPA 608; "aligned with
  their ISO9001 protocols"). `fail` when the paragraph's framing upgrades or
  downgrades the strength the evidence carries.
- Parent-company credentials (the Allegion class) are J4 fails; here they are
  also J6 fails when the paragraph would ground the parent's credential as
  the subject's.
- A certificate document title in a downloads list IS evidence of the
  attestation (unlike products, where a title is just a document) — a
  paragraph that withholds it entirely is a J2/J6 under-claim. This is the
  known field-specific tension with the document rule; report which way each
  record lands.
- "Standard, not a certification" hair-splitting cost real attestations (UL
  1784, ANSI A250.8) downstream. The synthesis passes when it states the
  compliance claim as made; it need not adjudicate standard-vs-certification.


## material_caps — field addendum (J6)

The field asks: which MATERIALS can this company work with?

### J6 — Field serviceability
- Polysemes are this field's signature hazard: `Lead` the metal vs "lead
  time"/"We Lead the Industry"; `Ground` steel vs electrical ground. The
  paragraph passes when its wording makes the sense unmistakable, fails when
  a faithful reader would mint the wrong sense as a material capability.
  Sense adjudication is screening's burden BY DESIGN (D14) — so the J6
  question is narrow: does the paragraph transmit the evidence's sense
  faithfully, or launder a non-material sense into material-shaped prose?
- Materials named only inside a CLIENT's spec or a product datasheet the
  subject merely stocks: J4/J5 territory; J6 fails when the capability
  reading ("works in Ti6Al4V") is unsupported by any doing-evidence.
- Series/grade families ("300 & 400 series stainless", "6000 Series
  aluminum") are materials, not products; a paragraph treating a grade
  enumeration as a product line fails.
- Typos and odd casings in the evidence ("Steel's", "Structual") must not be
  silently corrected into different entities; quoting or plain restating both
  pass.


## process_caps — field addendum (J6)

The field asks: which manufacturing PROCESSES can this company perform?

### J6 — Field serviceability
The measured pollution (37%→16.3% by instance, still the field's disease) is
processes performed by SOMEONE ELSE or SOMEWHERE ELSE:

- `fail` when the paragraph presents certification-lab testing (Intertek's
  missile impact / salt spray), installation-site work (anchor prep, field
  assembly), or a client's process as the subject's own shop capability
  without preserving who performs it where.
- `fail` when a document/heading fragment ("Testing and Rating of Severe
  Windstorm Resistant Components") is written up as a performable process.
- A demonstrated process step inside a delivered case study (micro-blasting
  in a project table) IS capability evidence — the audit settled this; do not
  fail it for living in a table.
- Sentence-shaped focal forms (≥6 words) are usually search noise; mark
  `not_a_process: true` in the note when the focal entity is not a process,
  even on a pass.
- Verb-fold merges (`milling`/`milled`) are legitimate; a paragraph covering
  the folded family together passes J3.

