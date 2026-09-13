# Answerability survey — brief (Phase 2 of the field-specific synthesis design)

You are grading ONE field of a manufacturing-website extraction pipeline. Read this whole brief before opening
your sample file.

## What you are measuring

A "record" = one focal entity (`focal_form`) + verbatim `evidence` snippets copied from the manufacturer's site +
the headings they sat under (`locations`) + an LLM-written paragraph (`synthesis`). Downstream, a SCREENING stage
reads ONLY `focal_form` + `synthesis` + the manufacturer's name (`subject_name`) — it never sees the snippets — and
decides whether the manufacturer itself has the field's dealing with the entity. A GROUNDING stage reads the same
two things and decides what the entity is.

The question for every record: does the paragraph CARRY the facts those readers need, as the snippets settle
them? You are NOT judging whether the entity is a product/material/etc., and NOT giving a screening verdict.
You are grading fact transmission, axis by axis.

## The eight axes (grade every one, for every record)

| axis | the fact the paragraph must carry |
|---|---|
| **A identity** | what the focal entity IS as the snippets present it (a part, a process, a machine, a grade, a sector, a standard, a document title, a heading); its OWN designations verbatim (grade, code, number, issuing body); whether the snippets name one specimen or a kind |
| **B sense** | where a word could be read in more than one sense (a homonym, a substring of a different term, a word used in an unrelated sense), the paragraph carries the sense the snippets support and does not bridge to the other |
| **C party** | WHOSE dealing the snippets show: the manufacturer itself (its brands, divisions, plants); a parent or sister company kept as the speaking voice; a customer/client; a supplier; a distributor/representative or its principal; a partner facility; a certification body; "companies in general"; an individual employee; a publication; NOBODY (agentless, definitional, how-to prose) |
| **D arrangement / capacity** | what the dealing IS in the snippets' own verbs, and in what capacity where the snippets fix it: makes/designs/sells as its own; works on to another party's order (whose spec, drawing, brand); resells/distributes/represents/stocks; services/repairs/installs/inspects/tests; uses as an input, tool, or machine; supplies into/serves; holds/is certified/claims to meet/tested to/references/requires of others; performs itself vs arranges for a partner. Where the snippets leave the capacity UNSTATED, a paragraph that says so is `carried`; a paragraph that supplies a capacity the snippets do not give is `contradicted` |
| **E currency** | present and actual vs aspirational, planned, in progress, on order, discontinued, a completed past project, a denial, a placeholder ("coming soon") — carried in the snippet's own placement |
| **F role** | whether the focal entity is the thing done/made/used/held, OR the use, application, destination, or market of something else, OR the customers' category a page is titled by, OR a word in a document title, OR an item in a list whose frame the page sets. ("used in X", "for the X industry", "components for X" land on either side; the paragraph must state the side) |
| **G frame** | where the text places the snippet and what that frame asserts: a menu/nav line; a heading; a list item on the manufacturer's own product/service/materials page; a catalog or price list; a case-study or project title; a gallery caption; a testimonial; a job posting; a blog explainer, listicle, glossary, how-to; a policy or supplier code; a document library; a certifications list, badge, or certificate link; a table cell. The frame decides whether a bare title means "offers a document" or "offers the thing" |
| **H attribution mode** | who is speaking (site copy, a quoted customer, a publication, a third-party badge) and the strength of the claim AS MADE (certified/audited by X, claims to meet, tested to by X, referenced, required of suppliers), neither upgraded nor downgraded |

## Marks

For each axis: `carried` (the fact is in the paragraph and matches the snippets), `absent` (the snippets settle
the fact but the paragraph does not carry it), `contradicted` (the paragraph asserts the opposite of, or
something beyond, what the snippets settle — a supplied default counts here), `na` (the snippets themselves do
not settle it, and the paragraph does not pretend they do).

Rules of grading:
- Read the PARAGRAPH FIRST with only `focal_form` and `subject_name`, and write down (privately) what it tells you
  on each axis. THEN read `evidence` and `locations` and grade. This order matters: you are simulating the
  downstream reader who never sees the snippets.
- Judge by reading. Never grade from a keyword. Quote the words that decide each non-`carried` mark.
- The paragraph naming the manufacturer is normal, never a flag. "No dealing shown" / "capacity unstated" is a
  legitimate finding when the snippets support it.
- Where a snippet is unrelated to the focal entity (a homonym, a different sense), the fact to carry is that it is
  unrelated; a paragraph that says so is `carried` on B.
- Do not use any file outside your sample file; do not open the run's verdicts or the taxonomy.

## Field item (one extra mark per record, `field_item`)

Grade the field's own demand, taken from the design doc, with the same four marks:

- **products**: is the CAPACITY named or declared unstated — made and sold as its own / worked on to another
  party's order (whose spec, drawing, brand) / resold, distributed, represented / serviced, installed, tested /
  used as an input — and where the focal entity is not a thing made or sold at all (a process, a material, a
  document), does the paragraph say so plainly?
- **equipments**: does the paragraph keep apart "used/operated/owned on the manufacturer's own floor" from
  "offered for sale on its product line" from "named in a customer's spec" from "described in an explainer of
  how such machines work" — and does it preserve what the thing DOES on the work and its configuration words
  (axes, control, drive, capacity) verbatim, with maker/model kept as designation?
- **industries**: does the paragraph state which SIDE the sector-shaped word sits on — the customers' or
  destination sector (served; includes "our product goes into X", "supplied to companies in X", a client
  story) vs the manufacturer's own activity or trade named by a sector-like word vs a sales channel vs a
  homonym inside another term?
- **conformity_attestations**: does the paragraph carry the MODE and HOLDER of the claim — certified/audited/
  registered by a named body vs claims to meet vs tested to vs referenced or explained vs required of suppliers
  or applicants vs a form collected from others — and whether the page presents it as the manufacturer's OWN
  (certifications list, badge, downloadable certificate) vs a document merely offered? Standard designation and
  issuer verbatim?
- **material_caps**: does the paragraph say whether the focal entity is the thing worked/processed/supplied/
  stocked/specified vs the material a PART is made of vs a material inside a client's spec or a stocked
  datasheet; is a grade/series kept as designation with the material it is a form of visible; who works it and
  whether at a partner plant to the manufacturer's direction?
- **process_caps**: does the paragraph say WHO performs and WHERE — the manufacturer's own shop vs a partner or
  subcontract facility (arranged by the manufacturer) vs a certification lab vs an installation site vs a
  client's own step — and whether the focal entity is an operation performed on the work vs a business or
  contractual undertaking, a credential, or a program? ("[process] is used for X" is own-capability evidence
  with X as the application.)

## Output

Write `results_{FIELD}.jsonl` in this directory, ONE line per record, built with shell appends in batches of
about 25 rows (`cat >> file <<'JSONL' … JSONL`) so a partial file is still valid. Do not create any other file in
the repository; scratch goes under your scratchpad only. Row schema:

```json
{"content_key": "<copied>", "field": "<field>", "judge": "agent:claude-opus <field>",
 "axes": {"A": {"mark": "carried|absent|contradicted|na", "quote": "", "note": ""},
          "B": {...}, "C": {...}, "D": {...}, "E": {...}, "F": {...}, "G": {...}, "H": {...}},
 "field_item": {"mark": "...", "quote": "", "note": ""},
 "worst": "<the single axis letter whose failure would most mislead the downstream reader, or 'none'>",
 "comment": "<one sentence, only if something about this record does not fit the axes>"}
```

`quote` = the deciding words (from the paragraph for `contradicted`, from the snippets for `absent`); empty for
`carried` and `na`. Keep notes to one sentence.

When finished, reply with at most 15 lines: rows written; per axis the counts of carried / absent / contradicted
/ na; field_item counts; the three most common ways facts went missing or got contradicted in this field, each
with one content_key; and anything the axes could not express.
