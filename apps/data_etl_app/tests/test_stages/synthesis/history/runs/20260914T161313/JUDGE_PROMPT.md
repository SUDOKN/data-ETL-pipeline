# Judge prompt template — synthesis-stage evaluation

The text every judge agent is given, with the `{...}` fields filled per
slice. RUNBOOK step 4: copy the filled text to
`history/runs/<run_id>/JUDGE_PROMPT.md` before the fan-out, so the run's
judgment stays reproducible; at the end of the run, add that run's verified
calibration rulings to the section at the bottom of THIS file (it is the
accumulated rulings — the run copy is a snapshot). Created 2026-09-10 from
run 20260905T213127's prompt plus the taxonomy batch of that date (J1
grounded-import split, J2 own-designations clause, J4 parent-policy note,
J7 agent laundering, products document-title bullet, severity per record).

Fields: `{TAXONOMY}` path, `{FIELD}`, `{ORDER}` work-order path, `{START}`,
`{END}`, `{N}`, `20260914T161313`, `{MODEL}` (Sonnet by the user's standing rule),
`{SUBJECT}`, `{PART}`, `{OUT}` verdict file path.

---

You are one judge in the synthesis-stage evaluation of an extraction pipeline.
You will read a slice of synthesis records and write one verdict per record.
Work from the files; do not ask questions; do not sample — every record in
your slice gets a row.

**Read first, in this order:**
1. `{TAXONOMY}` — the whole file. Sections J1–J5, J7 and "Discipline" apply
   to every record; the `## {FIELD}` addendum defines J6 for your field. Note
   the J1 *containment* clause (grounded import vs bleed), the J2
   *designation* clause (the focal entity's OWN designations only), and J7
   *agent laundering*.
2. Your work order: `{ORDER}` — a JSON object. Judge `records[{START}:{END}]`
   (0-based, half-open; {N} records). Every record has: `content_key`,
   `chunk_bounds`, `group_id`, `focal_form`, `forms`, `retried`, `synthesis`
   (the paragraph under judgment), `evidence` (the snippets, verbatim — ALL
   the fenced evidence the model was given for this record), `locations`
   (code-derived heading/table-header pointers; the model never saw these
   lines), `designations_dropped` (a mechanical nomination for J2, already
   scoped to the record's own designations — verify by reading),
   `request_custom_id` (records sharing it were co-packed in one request —
   J3's context), `evidence_sha256`, `labels` (only on runs of the
   label wire — run 20260913T170246; reverted 2026-09-14: the model's OWN answers, decided before it wrote the
   paragraph — `doer`, `doer_name`, `capacity`, `dealing_words`; judge the
   PARAGRAPH exactly as before, and add `label_contradicts_paragraph: true`
   to the note where a label and the paragraph disagree and `label_wrong:
   true` where a label contradicts the snippets), and the order-level `pv`,
   `taxonomy_version`, `subject_name`, `probes`, `chunk_texts`.
3. `chunk_texts[<chunk_bounds>]` is a local Markdown file holding the whole
   chunk of site text the model ALSO read. Open it only (a) to place a snippet
   whose heading/table matters, or (b) when the paragraph asserts something
   the snippets do not support — grep the file for the claim's key words to
   decide between a *grounded import* (the text supports it, it is about the
   focal entity, it contradicts nothing in the snippets: J1 `pass` with
   `grounded_import: true`), a *bleed* (`fail`, `containment_breach: true`
   when the text carries it as another entity's fact), and fabrication
   (`fail`, nothing supports it). Never judge a claim as supported merely
   because the chunk text supports it — only a same-entity, non-contradicting
   import passes, and it passes as a watch, not a merit.

**Verdict rows** — JSONL, one object per record, schema exactly as
TAXONOMY.md §Output. Echo `content_key`, `taxonomy_version`, `pv`,
`evidence_sha256`, `subject`, `field`, `group_id`, `chunk_bounds`,
`focal_form` unchanged from the work order. `first_judged_run` =
`20260914T161313`. `judge` = `agent:{MODEL} {FIELD} {SUBJECT} part{PART}` (the model
you are running as is given below). `checks` carries J1–J7, each
`{"verdict": "pass|fail|unclear", "severity": null|"minor"|"major", "note":
"...", "quote": "..."}`; add a `"PROBE:<id>"` entry for every probe in the
order whose `target` names this record. Every `fail` quotes the paragraph
AND the snippet that convicts or fails to support it. Put the field's
notation flags in the note when they apply (`not_a_product: true`,
`not_equipment: true`, `not_a_process: true`, `not_a_material: true`,
`not_a_sector: true`, `undifferentiated: true`, `grounded_import: true`,
`containment_breach: true`, `designations_dropped: [...]`).

**Model:** you are `{MODEL}`; write exactly that token into the `judge` string.

**Write** to `{OUT}` — build it with shell appends in batches of ~25 rows
(`cat >> file <<'JSONL' ... JSONL`), never one giant write, so a partial file
is still valid JSONL if you are cut off. Do not create any other file in the
repository. Scratch work (extracted slices, builder scripts) goes ONLY under a
uniquely named directory `<scratchpad>/judge_{SUBJECT}_{FIELD}_part{PART}/` —
the scratchpad is shared with concurrent judges and generic names collide. Do
not edit the work order, the taxonomy, or any other file in the repository.

**Calibration rulings (verified on earlier runs — apply them):**
- Severity is the taxonomy's wrong-tag test, and it is a property of the
  RECORD: one defect that trips J1, J6 and J7 together gets the same severity
  on each. Mark `major` only when a faithful downstream reader would mint a
  WRONG tag.
- A page-supported claim that is true of the same focal entity and
  contradicts nothing in the snippets (a machine's own construction facts, a
  part's own model list, the subject's own certification) is a grounded
  import: J1 `pass` + `grounded_import: true`. A neighbour's fact imported,
  or a contradiction of the snippets, is a bleed: J1 `fail`, `minor` unless a
  wrong tag follows, `containment_breach: true` when the page carries it.
- **This run's snippets are WIDER (snippet radius 1, run 20260914T161313):**
  each snippet is the sentence unit naming the entity PLUS one unit each side
  within its page — a list item's neighbours, the line above and below, a
  table row's header. So a snippet may open or close with a neighbouring
  sibling's line ("CASTING\n\nDie Casting," for the record `CASTING`). What
  the neighbouring unit says ABOUT THE FOCAL ENTITY (the introducing
  sentence, the attribution, the parent line) is now snippet evidence, not a
  grounded import. What it says about ITS OWN subject (the sibling item) is
  still not this record's: a paragraph that narrates the neighbour's item as
  the focal entity is a J3 swap; one that absorbs the neighbour's attribute
  into the focal entity is a J1 fail; naming the neighbour as a neighbour is
  neither. Do not fail a paragraph for placing the entity by what its wider
  snippet shows.
- J3 is entity identity and its fails are always `major`. A paragraph about
  the right entity that absorbs a sibling item, an anaphor, or a neighbour's
  attribute is a J1 fail, not a J3 fail.
- J2's designation clause covers the focal entity's OWN designations only.
  A sibling's grade, a neighbouring standard, a model code that has its own
  record: its absence is never a J2 drop, and its presence is not a merit.
- Search noise is not a synthesis defect. When the focal entity is not a
  member of the field at all (a process in equipments, the subject's own
  activity in industries, a part catalog entry in material_caps, a document
  title in products) and the paragraph transmits that sense faithfully, J6 is
  `pass` with a note flag (`not_a_product: true`, `not_equipment: true`,
  `not_a_process: true`, `not_a_material: true`, `not_a_sector: true`). J6
  fails when the paragraph itself launders the entity into the field's
  shape — frames own activity as a served sector, writes dealer stock as
  owned machinery, a client's part as an own product, a document AS a
  product — or leaves a faithful reader unable to tell which side it is on.
- J7 is the subject installed as the agent of a dealing the snippets give to
  another party, to nobody (generic advice, a listicle, a glossary), or to
  the subject only as a use or application of something else (application
  lists, membership clauses, served-industry pages titled by product). The
  dealing rule still licenses "the manufacturer offers X" from a bare list
  item on the subject's own product or service page — that is a pass.
- A policy the parent issues for its brands' suppliers, hosted on the
  subject's site with the parent as the speaking voice, is J4 `minor` when
  the paragraph keeps the parent as the voice; a credential the parent HOLDS
  credited to the subject stays `major`.
- A retried record (`retried: true`) is judged like any other; its co-pack
  is the retry request.
- **The document rule (verified on run 20260911T003500).** "Offers a
  document / no dealing beyond a title" is CORRECT for a bare list of
  catalog, brochure or PDF titles with no framing. It is a J2+J6
  under-claim (major when the tag is lost) when the record's snippets or the
  chunk text frame the list as things the subject sells, supplies, makes or
  holds: a product-catalog page that says "ready products ... which ones you
  would like to purchase"; a case-study title whose own words name the
  subject's work on the part ("Custom CNC Machining of Aluminum Brackets");
  a materials-catalog grade heading (`## LOW ALLOY 4145: ASTM A29`) on the
  subject's own materials page; and, in conformity_attestations, a
  certificate title or download link (the field addendum says a certificate
  title IS evidence). Write `document_rule: under-claim` or
  `document_rule: applied` in the note so the class stays countable.
- **Reseller and dealer stock.** "The manufacturer offers X" for a
  third-party-branded item on the subject's own product page is licensed by
  the dealing rule and is NOT a J4/J6 fail; it fails only when the paragraph
  says the subject MAKES it, or drops a maker's name the snippet gives.
- **The frame decides "offers" (ruling 2026-09-11, run 20260911T223222
  onward).** The dealing rule licenses "the manufacturer offers X" only where
  the page's own frame asserts the offering: the subject's own product,
  service, materials, or capabilities page. Where the frame does not assert
  it — a line card, a represented maker's or principal's page hosted on the
  site, a document library, a blog explainer, a job posting — the faithful
  paragraph says the item is listed there and that the capacity (made,
  resold, represented) is unstated; "offers X" written there is a J6 fail
  (major when a downstream reader would mint the subject's own tag from it),
  and "makes X" is J4+J6 major. A paragraph that says "capacity unstated" or
  "offers or represents" on such a page is a pass, not an under-claim.
  Write `frame_rule: applied` / `frame_rule: violated` in the note.
- **Severity inside the frame/document rule (verified on run 20260911T223222,
  8 packets).** Two sub-shapes: (a) the paragraph REFUSES any dealing on the
  subject's own listing ("lists X … but does not specify the capacity", "no
  dealing beyond a document", "presents a document about") — the tag is at
  risk: `major`; (b) the paragraph ASSERTS the dealing ("offers X as part of
  its product line", "works with X") and hedges only the make-vs-source or
  in-house-vs-partner MODE — the tag survives and the partner-plant ruling
  says the synthesis need not settle that mode: `minor`. An under-claim is
  J2 (+J6 when the tag is lost), never J1: a claim scoped weaker than the
  evidence is not a fabrication. Field membership gates severity: declining
  a dealing on a focal entity that is not a member of the field costs no
  tag (J6 `pass` + the `not_a_*` flag, J2 `minor` if the under-claim is
  real).
- **First-person copy is never the frame-rule shape.** "our capabilities",
  "our experts", "we offer/provide/carry" assert the offering wherever they
  sit, including on a blog or explainer page; the frame rule governs items
  LISTED where the page asserts no capacity. A bare bullet under the
  subject's own introducing sentence ("Our Capabilities:", "we provide the
  following services:", "Our inventory … includes:") takes that sentence's
  dealing even when the record's own snippet omits the sentence.
- **A patent on the subject's own patents page** (number, grant date) is the
  process_caps analogue of a certificate title: "no dealing beyond a
  document" there is a J2+J6 under-claim.
- **Parent policy voiced as the subject** (the Waev/Taylor-Dunn shape):
  `major` in a field whose tag IS the named standard or credential
  (conformity_attestations); `minor` where nothing is minted (a supplier
  requirement in process_caps whose suppliers stay the acting party).
- **Frame NAMED but the wrong page** (a gallery hub for its sub-gallery; a
  facilities section placed under "OUR PRODUCTS" because the site's nav
  string is flat): J1 `minor` when the attribution itself stays correct.
- **Wire vocabulary in the paragraph** ("The focal_form …", "The focal
  entity …"): note it (`wire_vocabulary: true`); not a fail on its own.
- **Client-requirement and specification tables in a case study (verified
  on run 20260911T223222, packet 09).** A row under "Client Requirements" /
  "Core Client Requirements" / a parameter table is capability evidence
  whenever the chunk narrates the job as delivered ("all steel plates were
  cut and pre-machined in-house", "fully machined and anodized by Day 6"): a
  paragraph that REFUSES any dealing on such a row is a J2+J6 `major`
  under-claim; one that asserts the dealing and hedges only the mode (in-house
  vs partner) is `minor`. Where nothing in the chunk narrates delivery, the
  row is a requirement and "required by the client" is the faithful reading.
- **Subcontracted steps** ("collaborated with a dedicated … facility to …")
  credited to the subject as its own process fall under the partner-plant
  ruling: real J4/J7 fails, but the tag survives, so `minor`.
- **Document-shaped focal forms in process_caps** ("Weld map", "WPS/PQR",
  "inspection template"): field-membership gate — J6 `pass` +
  `not_a_process: true`, never a J5/J6 fail for reading a delivered
  project's documentation as supplied.
- **The field-membership gate is a SEVERITY rule everywhere (verified on run
  20260912T191548, 7 packets — the judges' most-missed rule).** Before
  writing `major`, ask whether the focal entity is a member of the field:
  a case-study TITLE in products, the bare process "Welding" in products, a
  T&C "raw material", a resale tax form in conformity_attestations, a
  fragment like "institutions" in industries — no tag of the field can be
  won or lost, so a refusal or a supplied dealing there is `minor` at most
  (J6 `pass` + the `not_a_*` flag). The same entity judged in two fields
  fails at most in the field it belongs to.
- **A J2 `major` requires a matching J6 `fail`.** J6 `pass` means the tag
  survived, so an under-claim or a dropped designation with J6 passing is
  J2 `minor`.
- **Read the whole sentence before the severity split.** "displays X as an
  example of its metal stamping work, but the snippet does not specify any
  further dealing" and "offers X … but the capacity is not specified" ASSERT
  the dealing and hedge the mode: shape (b), `minor`. Only a paragraph that
  refuses any dealing is shape (a).
- **A generic process explainer does not assert.** "How does X work?" steps,
  "Manufacturers use this process to…", a numbered how-to with no party: a
  paragraph that declines the dealing there is correct even on the subject's
  own site, unless the subject's own words on the same page claim the
  process ("which we describe below" alone does not).
- **Dealer/resale register.** On a dealer-shaped subject ("We sell several
  types of industrial equipment from top brands"), "offers X" is licensed
  by the reseller ruling; "offers X for sale, capacity unstated" is the
  cleaner reading; neither is a `major` unless the paragraph writes owned
  or operated machinery from resale stock.
- **An internal record id in the prose** ("This snippet repeats the content
  of record g…") is `wire_vocabulary: true`, not a J3 fail; there is no
  "J3 minor".
- **Field-membership gate, three more shapes (verified on run
  20260913T170246, 11 packets).** A standard or specification judged in
  products (ASTM A182, BS EN 10088) mints no product tag; a fragment split
  from a compound phrase ("carbon" from "carbon & stainless steels",
  "institutions", "Ceiling height") mints nothing; a process judged in
  products (MJF, FFF) fails at most in process_caps. Each is J6 `pass` +
  the `not_a_*` flag and J2 `minor` at most — never `major` by inheritance
  from a real-entity sibling in the same co-pack.
- **Line-card listing is the licensed dealing on a representative site.**
  "Mathews & Company lists X as part of its Kansas product line" / "lists
  or represents X, capacity unstated" is a PASS (it is exactly what probe
  MW-P3 expects); the possessive variant — "as part of ITS secondary
  operations / ITS precision machining capabilities" — is the laundering,
  J1+J4+J6+J7 `major`. "Mathews represents Hudson and its products for the
  X sector" keeps the principal as maker: pass.
- **"For" is not "by".** A case-study title "Custom Machining for Oil & Gas
  Client" credited to the client in the paragraph is a REFUSAL of the
  subject's own work — J2 (+J6 where a tag of the field is lost), never
  J1/J4 laundering; the head noun decides the field tag (part-headed forms
  keep `major`, service- or title-headed forms drop to `minor`).
- **The introducing line may sit in the chunk, not the snippet.** A bare
  sub-bullet or adjacent sentence under a first-person parent line the
  record's own snippet omits ("Steelcraft offers … tested with…";
  "Approved Door series: H / HE / TH … Tests include:") takes that line's
  dealing; a refusal there is J2+J6 `major` (verified packet 11). Open the
  chunk text for every refusal on the subject's own page.
- **A buyer's-guide materials table is an explainer frame.** "### Materials:
  Commonly Used Alloys for Casting" under a party-less intro ("Different
  metal casting processes allow designers to choose…") does not assert the
  subject's offering; "capacity unstated" there is a pass, and the page's
  first-person claims (the casting SERVICES) are the only own dealing.
- **Job-posting duties stay internal.** A custodian's or operator's duty
  bullet ("mow lawns", "load and unload parts") that the paragraph names
  as a posting and keeps as facility upkeep or a role's tasks is a pass;
  it becomes a J5/J6 fail only when written as a service the subject
  offers to customers.
- **Do not copy a sibling's note onto a record you have not read**, and do
  not score a record by its siblings' verdicts: open the chunk text.

**Discipline:** judge by reading, never by keyword; quote what convicts;
`unclear` is honest when the evidence underdetermines it; probes apply only
to the records they name; the paragraph naming the manufacturer is correct
behaviour, never a flag; an identical paragraph shared by co-packed records
is a J3 pass with `undifferentiated: true` unless it is wrong for this
record. When in doubt between fail and unclear ask: would a faithful
downstream reader of this paragraph produce a wrong tag? Yes → fail.

When finished, reply with ONE line: `{SUBJECT}/{FIELD} part{PART}: <rows
written> rows, J1 <fails>, J2 <fails>, J3 <fails>, J4 <fails>, J5 <fails>,
J6 <fails>, J7 <fails>, grounded_import <n>, containment <n>, unclear <n>`
and up to three one-sentence observations of NEW defect classes (mechanism +
group_id), nothing else.
