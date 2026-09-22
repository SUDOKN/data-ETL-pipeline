# Judge brief — Step 2 tryout sample (2026-09-21)

You are coding rows drawn from a prompt tryout. Each row carries ONE record as the model saw it
(`record_id`, `focal_form`, `synthesis`), a `label` one or both arms produced for it, and which
arm(s) produced it. Code by READING the record; never let a quote or an explanation decide the
code — they are colour. The binding rules are the grounding harness TAXONOMY
(`apps/data_etl_app/tests/test_stages/grounding/TAXONOMY.md`) and the addendum rulings 1–23 with
amendments 22a–d / 23a–d (`docs_local/grounding_gap_survey_2026-09-14/TAXONOMY_ADDENDUM.md`).
Judge the general phenomenon; when the wording does not settle a row, code it as best you can and
REPORT it in your summary — never invent a sub-rule.

## Row kinds

**`kind: grounding`** (concept fields: industries, material_caps, process_caps,
conformity_attestations). `arms` says who produced the label: `a` = today's two-call grounding
only, `b` = the Step 2 one-call grounding only, `ab` = both. A label beginning `OOV:` is a
proposal (a name outside the vocabulary). Code the label for THIS record:

- `D` clean and specific; `N` clean but broader than the record supports;
- `X` wrong kind (for industries: `X-own` when the company's own product or activity was read as a
  sector it serves — ruling 22 — else `X-sector`);
- `V` vague (a term that fixes nothing); `B` bridge (one unstated inference from something IN the
  record); `P` another party's fact; `F` fabricated (absent from the record).
- The focal-form scope (23d): the label must be what the FOCAL FORM is or names; a thing the
  synthesis mentions only beside it belongs to another record → `X`, note "beside the focal form".
- For a `b` row, also say whether the quote(s) in `quotes_b` are found in the record and evidence
  the label: `quote_ok: yes | no | partial`.

**`kind: outline`** (the outline tryout, 2026-09-21; concept fields). Six arms answered the same
records with the same instructions and differed only in how the vocabulary was pasted and where it
sat; `arms` lists the arms whose majority answer carries this label for this record, out of
`arms_total`. Which arms carry it plays NO part in the code: code the label for THIS record exactly
as for `kind: grounding` (D / N / X with `sub_kind` for industries / V / B / P / F; the focal-form
scope of 23d), and add `quote_ok: yes | no | partial` for the `quotes` shown (found in the record,
and evidencing the label). A label beginning `OOV:` is a proposal; two proposals that differ only
in spelling are two rows — code each on its own words.

Rulings 24–29 of the addendum (2026-09-21) bind every later packet: a record that uses one of a label's OTHER
NAMES in the outline names that label exactly (D/N, never B) — read the field's outline with its other names
before coding; a grounding row is coded on identification alone (whether the record names the thing and it is
of the field's kind), and the synthesis's own "no dealing shown" is screening's question, never a reason for B,
P or F; a vocabulary parent chosen for a specifically named member is N, not V; for a process label compare the
operation the label means with the operation the words name (same D/N, narrower B, different X, absent F); an
industries label names a group of organizations — a product-shaped label of a real placement is N; a standard
named without a status asserted against it is X in conformity.

**`kind: freehand`** (equipments, products, contract_products). `fa` = today's freehand prompt
only, `fb` = the reworded prompt only, `fafb` = both. Same codes; for equipments apply ruling 23
and 23a–c exactly: the record's words (including a heading the site files the subject under, and a
name in any grammatical form, abbreviation or recognisable variant) must NAME a class of
machines; a bare process name, a control method, a feature, or a tool/energy source names none →
`F`. Ruling 18: measuring/inspecting/testing equipment is out of the field → `X`.

**`kind: screening`.** Both screening prompts judged the same (record, label) pair;
`sa_accepts` is today's prompt, `sb_accepts` the Step 2 unit screening; `sb_detail` shows the
unit screening's per-repeat answer (evidence `named`/`inferred`, or the rule it failed on, with
its quote). Code two things: `truth: accept | reject` — does the record prove the manufacturer's
own, current dealing with the label (SCR-2 read from the manufacturer's side; rulings 3, 17, 21,
22d) — and, when `sb` accepted, `distance: named | inferred | beyond` — the record's own words
name the label (any grammatical/shortened form), or the label is one plain step from what they
name, or further than that. When `sb` rejected, say whether the rule it named is the right reason
(`reason_ok: yes | no`).

## Output

One JSON object per line into `out/judge/verdicts/<packet name>.jsonl`, appended in batches of
10–20 rows so a cut-off leaves valid JSONL:

```json
{"item_id": "j_0001", "kind": "grounding", "code": "X", "sub_kind": "X-own", "quote_ok": "yes", "note": "'offers CNC machining' names what it does, places it in no market", "judge": "sonnet:<agent name>"}
{"item_id": "o_0007", "kind": "outline", "code": "N", "quote_ok": "yes", "note": "'<decisive words>' — the record supports a narrower option than this", "judge": "sonnet:<agent name>"}
{"item_id": "j_0002", "kind": "freehand", "code": "F", "note": "'forging' is a process; no machine named", "judge": "sonnet:<agent name>"}
{"item_id": "j_0003", "kind": "screening", "truth": "reject", "distance": null, "reason_ok": "yes", "note": "'applied by a finishing partner' — the partner's business", "judge": "sonnet:<agent name>"}
```

`note`: at most 25 words, quoting the decisive words of the record. Every item in your packet gets
exactly one row. Finish with a short summary: rows the wording did not settle (say which), and
any pattern you saw across rows (never a new rule).
