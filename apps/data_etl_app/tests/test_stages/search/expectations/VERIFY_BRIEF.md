# Verification brief — the second, independent pass

Only `confirmed` entries gate recall (user decision 2026-08-26). An entry
becomes `confirmed` when a **second reader, working independently of the first,
re-derives it from the text and agrees**. This brief is that reader's
instructions.

Read §1–§6 of `SEEDING_BRIEF.md` first — the field boundaries, the recall-first
stance and the matching traps are identical for both passes, and are not
repeated here. Skip its §7 (seeding output format); your output format is §3
below.

---

## 1. What independence means here

You are given the subject's scraped text and a **stripped packet**: entry id,
name, acceptable forms, and evidence quote. You are deliberately **not** given
the seeding agent's notes, its actor labels, or its reasoning.

Re-derive each judgment **from the text**. The failure this guards against is
the second reader ratifying the first reader's argument instead of checking the
first reader's claim — which produces a number that looks like agreement and
measures nothing. If you find yourself thinking "that sounds right", go and read
the surrounding lines before you write `confirm`.

## 2. The four verdicts

- **`confirm`** — the text names this entity, it falls inside the field's
  quoted `What qualifies` clause, and at least one acceptable form appears in
  the evidence quote. Gates recall from now on.
- **`dispute`** — the entity is real in the text but its membership in this
  field is genuinely arguable (a membership offered as a certification, an
  operation only inferable from a product name, a metrology device under
  `equipments`). Never gates; the argument stays on the record. **Give the
  argument in `reason`, not a verdict word.**
- **`retire`** — the entry is wrong: the quote does not say what the entry
  claims, the "entity" is a substring artefact (`PET` inside *competitive*,
  `ITAR` inside *military*), or the text explicitly denies it ("we do NOT
  machine automobile parts"). Say which in `reason`.
- **`amend`** — the entity is right but the entry's mechanics are wrong: a
  missing spelling the text actually uses, an over-broad form that would credit
  half the field, a quote that does not contain any acceptable form. Supply
  `corrected_forms` and/or `corrected_quote`. An amended entry is confirmed
  with its correction applied.

`corrected_quote` is **added beside** the entry's existing quotes, not
substituted for them: recall is window-scoped, so a quote you drop is a window
the entry can no longer be found in. Add `"replace_evidence": true` only when an
existing quote is actually WRONG rather than merely unhelpful.

Prefer `amend` over `retire` when the entity survives and only the wording is
off. Prefer `dispute` over `retire` when the disagreement is about the field's
boundary rather than about the facts. Retire only what is actually wrong.

## 3. Output

One JSON object per line, to the path you were given:

```json
{"id": "tanfel_com-products-0007", "verdict": "confirm"}
{"id": "tanfel_com-equipments-0002", "verdict": "dispute",
 "reason": "a CMM: the field's clause excludes machines that only measure the work"}
{"id": "tanfel_com-material_caps-0011", "verdict": "retire",
 "reason": "'PET' occurs only inside 'competitive'; no polymer is named anywhere"}
{"id": "tanfel_com-process_caps-0004", "verdict": "amend",
 "corrected_forms": ["heat treating", "heat-treated", "heat treatment"],
 "reason": "the hyphenated past-participle spelling is what 6 of 9 occurrences use"}
```

Then, separately, the entities the seed **missed** — same bar as a seeded entry,
in the same shape the seeding brief specifies:

```json
{"type": "miss", "field": "conformity_attestations", "name": "PCN Level II",
 "acceptable_forms": ["PCN Level II"],
 "evidence": [{"quote": "technicians qualified to PCN Level II"}],
 "actor": "own", "reason": "on the NDT services page, absent from the packet"}
```

Misses enter as `candidate` — your pass is their *first* reading, so they are
not confirmed by finding them. They are promoted the next time someone reads
this subject.

## 4. Verify the mechanics too, not only the judgment

For every entry, check the two things a later run will actually depend on:

1. **The quote is verbatim.** Copy it out of the file and compare. Quotes that
   were retyped rather than copied are the single most common defect, and they
   turn into false "fabrication" verdicts months later.
2. **At least one acceptable form is inside that quote.** An entry whose forms
   appear nowhere in its own evidence can never credit — it is a silent hole in
   the recall denominator.

Both are also checked mechanically by `validate_expectations.py` afterwards, so
you will not be the last line of defence — but a machine can only tell that a
quote is absent, not which correct quote was meant.

## 5. Coverage

Judge **every** entry in the packet. A packet returned with three quarters of
its entries silently unjudged is worse than one returned with an explicit note
saying where you stopped, because the missing verdicts are indistinguishable
from `confirm` at merge time — they simply stay `candidate` and stop gating,
with nothing to say why.
