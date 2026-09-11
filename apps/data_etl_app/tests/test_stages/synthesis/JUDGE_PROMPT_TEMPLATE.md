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
`{END}`, `{N}`, `{RUN_ID}`, `{MODEL}` (Sonnet by the user's standing rule),
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
   J3's context), `evidence_sha256`, and the order-level `pv`,
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
`{RUN_ID}`. `judge` = `agent:{MODEL} {FIELD} {SUBJECT} part{PART}` (the model
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
