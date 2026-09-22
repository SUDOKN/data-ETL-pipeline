# Judge brief — the proposal wave (first complete Step 2 run, 20260922T215605)

You are coding rows from `out/judge_proposals/packets/<packet>.jsonl`. Each row is ONE proposal on ONE
record: a label the model PROPOSED because the vocabulary of that field seemed to lack it. The row
carries the record as the model saw it (`record`, `focal_form` = the record's subject as the site names it,
`synthesis`), the proposal (`label`), where it came from (`sources`: `grounding` = the one grounding call,
`proposal_pass` = the second look at records left with no label, `descent:<parent>` = a sibling proposed
while narrowing under an accepted parent, `leaf:<parent>` = the leaf step under an accepted label with no
children), the words it was proposed on (`grounding_quote`), the proposal wave's verdict (`accepted`,
with `evidence` named/inferred and `screening_quote`, or `failed_rule`), and `vocabulary_hints`:
vocabulary labels (with their other names) that share words with the proposal.

Code by READING the record. Quotes are colour, never the decision. The binding rules are the grounding
harness TAXONOMY (`apps/data_etl_app/tests/test_stages/grounding/TAXONOMY.md`) and the addendum rulings
1–29 (`docs_local/grounding_gap_survey_2026-09-14/TAXONOMY_ADDENDUM.md`). Judge the general phenomenon;
when the wording does not settle a row, code it as best you can and mark it `"unsettled": true` with the
reason in `note` — never invent a sub-rule.

## The question, in two parts

**1. `code` — what IS this proposal, for this record?**

- `D` a genuine gap: a specific entity of the field's kind (a process performed on the work; a material the
  work is made of; a market = a group of organizations served; a conformance status asserted against a named
  standard) that the record names or is one plain step from, and that the vocabulary does not hold.
- `L` a vocabulary label in disguise: the vocabulary already holds this entity under a name or other name
  (use `vocabulary_hints` and, when in doubt, the field's vocabulary — `knowledge/ontology/<field>.json`);
  put that label in `vocab_label`. Also `L` when the proposal is a narrower or broader restatement that the
  vocabulary's label would have carried (ruling 26: compare the OPERATION named, not the words).
- `V` vague: a term that fixes no particular member of the kind ("Cleaning", "Finishing Operations",
  "Batch Production" may or may not be — decide on the record's words: does the record name a particular
  operation/material/market, or only a class of activity?).
- `X` wrong kind: not an entity of the field's kind (a product, a service offering, a company attribute,
  a machine, a tool; for industries the company's own product line read as a sector — ruling 22 → `X`).
- `P` another party's, or a different dealing (the record shows another company performs it, or the
  manufacturer buys/sells/uses it rather than performs/serves/is made of/holds it — rulings 3, 17, 21).
- `F` not in the record: the proposal is absent from the record's words and not one plain step from them.
- `U` unsettled (say why).

**2. `screen_ok` — was the proposal wave's verdict on this record right?** `yes` when an accepted row is
`D` (the record proves the manufacturer's own current dealing with a real, specific entity of the kind) or
when a rejected row's `failed_rule` names the right reason; `no` otherwise. A rejected `D` row is a wrong
reject (`screen_ok: no`). For `L` rows answer on the entity, not the spelling: the screening was right to
accept a real dealing even though grounding should have used the vocabulary's label.

## Output

One JSON object per line into `out/judge_proposals/verdicts/<packet name>.jsonl`, appended in batches of
10–20 rows so a cut-off leaves valid JSONL. Every row of the packet gets exactly one line.

```json
{"item_id": "p_0001", "code": "L", "vocab_label": "Inspection", "screen_ok": "yes", "unsettled": false, "note": "'CMM inspection of machined parts' — the vocabulary holds Inspection; a narrower spelling of it", "judge": "sonnet:<agent name>"}
```

End with a short summary (counts per code, the rows you marked unsettled and why, any pattern you saw
repeat) — the summary is returned, not written to the verdicts file.
