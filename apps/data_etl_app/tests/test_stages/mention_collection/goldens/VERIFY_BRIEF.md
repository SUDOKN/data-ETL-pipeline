# Verification brief — the second pass that promotes a golden label

Handed verbatim to every verification agent. A label seeded by one reader is
`candidate` and gates nothing; a label a **second, independent** reader has
re-derived from the text is `confirmed` and gates.

Read `../GOLDEN_LABELS_SCHEMA.md` for the contract and `SEEDING_BRIEF.md` for
what the first pass was asked to do.

---

## 1. Independence is the whole point

**Re-derive each judgment from the text, not from the seed's notes.** If you
read the label first and then go looking for confirmation, you are not a second
pass — you are a spell-checker, and the corpus gains nothing but false
confidence.

Work in this order for each label: read the form, find its occurrences yourself,
form your own expectation, and only then compare with what the seed wrote.

## 2. The failure this rule exists to prevent

On 2026-08-27 the search harness fired a RED verdict on **correct** output. Its
alecmfg `equipments` inventory had been seeded from the everyday meaning of
"equipment" and listed a coordinate measuring machine, a laser interferometer, a
Renishaw probe, a vacuum fixture and bending molds as must-find. The equipment
prompt explicitly excludes metrology and tooling. Search obeyed its own
definition; the eval called it six recall misses.

**An unverified expectation set does not just fail to help — it manufactures
false alarms.** That is why `confirmed` requires a provenance row from someone
who was not the seed, and why `checks/validate_goldens.py` refuses a
`confirmed` label that lacks one.

## 3. What to check, by label kind

**Occurrence labels** are computed by the production collector, so you are not
re-counting by hand — you are checking that the count means what it claims:

- Does the evidence quote really appear in the snapshot, verbatim? (The
  validator checks this whitespace-elastically; your job is the *judgment*, not
  the string match.)
- Is the count dominated by a **repeated line**? `doors and frames` occurs 120
  times on steelcraft and 54 of those are one footer blurb. A label whose
  evidence is a footer says almost nothing; annotate it and add a prose quote.
- For a **must-not-occur** label: is the form genuinely absent, or merely
  absent *under the matcher's rules*? `TIG` scores zero on howcogroup only
  because the site writes "TiG" and short forms are matched case-sensitively.
  That is a matcher artifact and **must not** be labelled must-not-occur —
  raise it instead.
- For a **substring trap**: are both counts right, and is the trap
  case-conditional?

**Snippet, location, grouping and inheritance labels** are judgments. Re-read
the passage and form your own answer before comparing.

## 4. Verdicts you can record

- **agree** → add `{action: verified, by: <you>, date: …}` to `provenance` and
  set `status: confirmed`.
- **disagree** → set `status: disputed`, add a provenance row saying what you
  read differently. **Disputed never gates**, so this is cheap and safe; use it
  rather than arguing a label into shape.
- **wrong for a reason worth keeping** → `status: retired` with the reason.
  Never delete a label: the reason it was wrong is usually the most useful
  thing about it.
- **not a fact about this stage** → raise it rather than labelling it. The
  `TIG` case above is the worked example.

## 5. Sampling

Occurrence labels are machine-generated from the production collector, so a
**sample** is the honest warrant: verify enough per (subject, field) to stand
behind the file, record how many you read in the file's provenance, and say so.
Judgment labels — snippet, location, grouping, inheritance — are read
individually; there is nothing mechanical behind them to lean on.

## 6. Finish

`python checks/validate_goldens.py --subject <domain> --quotes` must report
0 errors, and `VERIFICATION_LEDGER.md` gains a row saying what you verified,
how much of it, and what you disputed.
