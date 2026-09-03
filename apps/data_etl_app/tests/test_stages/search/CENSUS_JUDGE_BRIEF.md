# CENSUS_JUDGE_BRIEF — the identical prompt every census judge receives

RUNBOOK step 3 (full census) and step 4 (double-judge) both point here. The
double-judge's instructions must be **identical** to the primary's, or the
agreement number measures the assistant's own instruction drift rather than how
much two careful readers differ (locked lesson, 2026-08-27: the alecmfg
equipments pair was run with different boundary glosses after the field's
definition changed mid-flight, and its agreement figure is contaminated).

Keeping the wording in one file rather than in two hand-written agent prompts is
what makes "identical" checkable. Spawn agents with a prompt that names the
packet, the output path, and this file — nothing else about how to judge.

---

## The task

You are judging the output of an information-extraction stage called **search**.
Search reads one window of a manufacturer's website text and returns every
surface phrase naming a thing of one particular field (products, equipments,
process_caps, material_caps, industries or conformity_attestations).

Your packet holds, per window: the phrases search returned, and the exact window
text search read. **Code every returned form**, and list the in-field entities
the text names that no returned form covers.

## How to judge

1. **Read the window text.** Judge by the PLAIN MEANING of the field, reading
   the text — not by any extraction prompt, and not by what you think the
   pipeline intended.
2. **Code every form in every window it appears in.** A form appearing in three
   windows gets three records. Windows are the unit search works in, and the
   same phrase can be right in one window and wrong in another.
3. **Use the field's code table**, printed at the top of your packet.
4. **The stage is RECALL-FIRST.** A thing the text names is in-field even when
   it belongs to a client, supplier, lab, parent company or a machine the
   subject is reselling. Record whose it is with the `actor` flag — never
   downgrade the code for it. A client's product is still `V`.
5. **A form must be judged as the text uses it, not as it reads alone.** The
   `Lead` in "lead time" is junk in a materials packet; `Lead` beside solder is
   a material. When a form genuinely carries two senses in-window and search
   supplied no context, code your best reading and set `polyseme: true`.
6. **Do not code by regex or by matching against a list.** Read.
7. **Judge only from your packet. Do not open any other file in the
   repository — above all not the extraction prompts the stage runs on**
   (`*_phrase_search.txt` and anything under `knowledge/prompts/`).

## Why the prompt is off limits

This matters enough to explain rather than just assert, because a conscientious
judge will be tempted to go looking for the "official" field boundary.

Coding a form against the stage's own prompt makes the prompt into the ground
truth, and the census then measures only whether the stage obeyed itself. It
becomes structurally incapable of finding the defect class this evaluation
exists to find: **a prompt whose boundary is wrong.** The stage can score
perfectly while returning the wrong things, because the yardstick and the thing
being measured are the same object.

The field's plain meaning is the independent standard. If the prompt excludes
something a reasonable reader would call a machine, that gap is a FINDING — and
the only way it can surface is if you code what you see and let the disagreement
show. Where you think the boundary is genuinely arguable, code your reading and
say why in `note`; that note is the signal. Do not resolve the argument by
deferring to the prompt.

(This is also why recall and precision rest on different standards here, which
is deliberate: the must-find list was seeded against the stage's stated scope,
while your codes are the outside check on that scope.)

## Codes

Your packet prints the table for YOUR field. Every table rolls up the same way:

| rollup | codes | meaning |
|---|---|---|
| in_field | V, B | a defensible candidate for this field |
| adjacent_field | P M C S D E N L I O T W Z | a real thing, but of ANOTHER field |
| generic | G | generic noun carrying no designation content |
| junk | U | UI fragment, boilerplate, not an entity |

## Flags

- `actor`: `own` | `client` | `supplier` | `lab` | `parent_sibling` |
  `reseller_inventory` | `unknown` — whose thing the text says it is.
- `evidence_kind`: `prose` | `heading` | `nav` | `doc_title` | `case_study` |
  `job_ad` | `legal_boilerplate` | `spec_table` — where the occurrence sits.
- `polyseme`: `true` only per rule 5 above.

## Misses

For each window, list every entity of your field that the window text NAMES and
that **no returned form covers**. Quote the text verbatim as evidence. Finding
these is the point of the census — the must-find list at the top of your packet
is a floor, never a ceiling, and it is not always right (see "Doubt the list").

A form "covers" an entity when the entity's designation occurs inside the
returned form as whole words. `Shearing` covers `shear`; `titanium` does NOT
cover `titanium fusion cages` — a returned fragment covers nothing.

## Doubt the list

The must-find list at the top of your packet is a previous reader's work and is
under test alongside the stage. If an entry there is wrong — the text does not
say it, or it is not a thing of this field, or the quote describes a person's
employment history rather than the company's offering — say so in a `note` on
the relevant record rather than bending your codes to agree with it.

Two live examples of list defects, so you know the shape. A products list
carried the entry `balls` whose only acceptable form was the comma-joined
scrape `Balls, bearings, pulleys`, so the right token could never match it. A
conformity list demanded `CSA/CUS` for a subject whose text never contains that
string anywhere — the entry was unreachable by any run.

**These are examples of MALFORMED entries, not rulings about where a field's
boundary sits.** Nothing in this brief tells you what belongs in a field. That
judgment is yours, from the field's plain meaning and the window text, every
time. If you find yourself citing this brief as authority for excluding a kind
of thing, stop — you have misread it.

## Verbatimness is not your job

Some returned forms do not occur in the window text word for word — the stage
sometimes distributes a shared prefix across a list (`ASTM A182, A276` →
`ASTM A276`) or changes an inflection (`tested` → `testing`). A separate
mechanical check measures that. **Judge the entity, not the string**: if the
text names the thing, code it on its merits and add `note: "not verbatim"`.

## Output

Write JSONL — one JSON object per line, no wrapping array, no markdown fence —
to the exact path your spawning prompt gives you.

One record per (form, window):

```json
{"window": "0:23771", "form": "CNC machining", "code": "P", "actor": "own", "evidence_kind": "prose", "note": ""}
```

One record per miss:

```json
{"type": "miss", "window": "0:23771", "entity": "Zeiss CONTURA G2 CMM", "quote": "Zeiss CONTURA G2 CMM", "suggested_forms": ["Zeiss CONTURA G2 CMM"]}
```

Rules for the file:
- `window` is the bare `start:end` bounds, exactly as the packet heading prints
  them.
- `form` is copied byte-for-byte from the packet's numbered list.
- Every returned form in every window gets a record. Do not skip, sample,
  summarize, or collapse duplicates across windows.
- Do not write anything to the file but these records.

## Count your records before you finish — this is not optional

The single most common failure in this job is a judge who codes the first
stretch of a long list, writes the file, and reports success. The file parses,
merges, and produces a precision number computed on whatever subset the judge
happened to reach. Nothing downstream can tell that from a complete file.
Measured on the first three agents of run 20260901T013332: one skipped 34% of
its forms and another skipped 72%, both reporting success.

So, before you report done:

1. Count the numbered forms in your packet, per window.
2. Count the form records you wrote, per window.
3. They must be equal. If they are not, write the missing records.

Your packet is bounded so that this is achievable — if it names a part number,
you have a slice of a larger unit and are responsible for exactly the forms
printed in YOUR file, no more. Report the two counts in your reply.
