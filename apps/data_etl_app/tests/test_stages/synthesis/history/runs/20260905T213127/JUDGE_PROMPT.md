# Judge prompt template — run 20260905T213127

Every judge agent of this run was given this text with the four `{...}`
fields filled. Kept beside the verdicts so the judgment is reproducible.

---

You are one judge in the synthesis-stage evaluation of an extraction pipeline.
You will read a slice of synthesis records and write one verdict per record.
Work from the files; do not ask questions; do not sample — every record in
your slice gets a row.

**Read first, in this order:**
1. `{TAXONOMY}` — the whole file. Sections J1–J5 and "Discipline" apply to
   every record; the `## {FIELD}` addendum defines J6 for your field. Note the
   J1 *containment* clause and the J2 *designation* clause.
2. Your work order: `{ORDER}` — a JSON object. Judge `records[{START}:{END}]`
   (0-based, half-open; {N} records). Every record has: `content_key`,
   `chunk_bounds`, `group_id`, `focal_form`, `forms`, `retried`, `synthesis`
   (the paragraph under judgment), `evidence` (the snippets, verbatim — ALL
   the fenced evidence the model was given for this record), `locations`
   (code-derived heading/table-header pointers; the model never saw these
   lines), `designations_dropped` (a mechanical nomination for J2 — verify by
   reading), `request_custom_id` (records sharing it were co-packed in one
   request — J3's context), `evidence_sha256`, and the order-level `pv`,
   `taxonomy_version`, `subject_name`, `probes`, `chunk_texts`.
3. `chunk_texts[<chunk_bounds>]` is a local Markdown file holding the whole
   chunk of site text the model ALSO read. Open it only (a) to place a snippet
   whose heading/table matters, or (b) when the paragraph asserts something
   the snippets do not support — grep the file for the claim's key words to
   classify the J1 fail as `containment_breach: true` (the text supports it,
   the snippets do not) or leave it as fabrication (nothing supports it).
   Never judge a claim as supported because the chunk text supports it.

**Verdict rows** — JSONL, one object per record, schema exactly as
TAXONOMY.md §Output. Echo `content_key`, `taxonomy_version`, `pv`,
`evidence_sha256`, `subject`, `field`, `group_id`, `chunk_bounds`,
`focal_form` unchanged from the work order. `first_judged_run` =
`20260905T213127`. `judge` = `agent:{MODEL} {FIELD} {SUBJECT} part{PART}` (the model you are running as is given below).
`checks` carries J1–J6, each `{"verdict": "pass|fail|unclear", "severity":
null|"minor"|"major", "note": "...", "quote": "..."}`; add a
`"PROBE:<id>"` entry for every probe in the order whose `target` names this
record. Every `fail` quotes the paragraph AND the snippet that convicts or
fails to support it. Put the field's notation flags in the J6 note when they
apply (`not_a_product: true`, `not_equipment: true`, `not_a_process: true`,
`undifferentiated: true`, `containment_breach: true`, `designations_dropped:
[...]`).

**Model:** you are `{MODEL}`; write exactly that token into the `judge` string.

**Write** to `{OUT}` — build it with shell appends in batches of ~25 rows
(`cat >> file <<'JSONL' ... JSONL`), never one giant write, so a partial file
is still valid JSONL if you are cut off. Do not create any other file in the
repository. Scratch work (extracted slices, builder scripts) goes ONLY under a
uniquely named directory `<scratchpad>/judge_{SUBJECT}_{FIELD}_part{PART}/` —
the scratchpad is shared with concurrent judges and generic names collide. Do not
edit the work order, the taxonomy, or any other file in the repository.

**Calibration notes (from the verified slices of this run — apply them):**
- Severity is the taxonomy's wrong-tag test. A containment breach whose imported
  facts are true of the same focal entity (a machine's own construction facts,
  a part's own model list, the subject's own certification) is `minor`. Mark it
  `major` only when a faithful downstream reader would mint a WRONG tag.
- J3 is entity identity and its fails are always `major`. A paragraph about the
  right entity that absorbs a sibling item, an anaphor, or a neighbour's
  attribute is a J1 fail (minor), not a J3 fail.
- Search noise is not a synthesis defect. When the focal entity is not a member
  of the field at all (a process in equipments, the subject's own activity in
  industries, a part catalog entry in material_caps) and the paragraph
  transmits that sense faithfully, J6 is `pass` with a note flag
  (`not_a_product: true`, `not_equipment: true`, `not_a_process: true`,
  `not_a_material: true`, `not_a_sector: true`). J6 fails when the paragraph
  itself launders the entity into the field's shape — frames own activity as a
  served sector, writes dealer stock as owned machinery, a client's part as an
  own product — or leaves a faithful reader unable to tell which side it is on.

**Discipline:** judge by reading, never by keyword; quote what convicts;
`unclear` is honest when the evidence underdetermines it; probes apply only
to the records they name; the paragraph naming the manufacturer is correct
behaviour, never a flag; an identical paragraph shared by co-packed records
is a J3 pass with `undifferentiated: true` unless it is wrong for this
record. When in doubt between fail and unclear ask: would a faithful
downstream reader of this paragraph produce a wrong tag? Yes → fail.

When finished, reply with ONE line: `{SUBJECT}/{FIELD} part{PART}: <rows
written> rows, J1 <fails>, J2 <fails>, J3 <fails>, J4 <fails>, J5 <fails>,
J6 <fails>, containment <n>, unclear <n>` and up to three one-sentence
observations of NEW defect classes (mechanism + group_id), nothing else.
