# Expectation-set verification ledger

Promotion policy (user decision 2026-08-26): entries are seeded as `candidate`;
a SECOND independent reader promotes to `confirmed`; only `confirmed` entries
gate recall. `disputed` never gates. The user may veto anything.

## State: VERIFICATION COMPLETE for all 8 subjects (2026-08-26)

**522 entries / 751 evidence quotes. 514 confirmed · 8 disputed · 0 candidate.**
`eval_set_version: 2` in every field file.

Two independent passes stand behind every entry: the seeding read, and a
verification read by a different agent that re-derived each judgment from the
text rather than from the seed's notes. On top of that the assistant
mechanically re-checked **every** quote through the harness's own matcher —
verbatim presence, and at least one acceptable form covered by its own
evidence: **0 failures**.

| subject | entries | confirmed | disputed |
|---|---|---|---|
| 101machine_com | 41 | 40 | 1 |
| ableengineering_com | 85 | 83 | 2 |
| acimachine_com | 25 | 25 | 0 |
| alecmfg_com | 135 | 132 | 3 |
| anchor-mfg_com | 76 | 76 | 0 |
| austinelectricservices_com | 15 | 14 | 1 |
| steelcraft_com | 132 | 131 | 1 |
| taylordunn_com | 13 | 13 | 0 |

## The 8 disputes (none gate recall)

- `101machine industries` **Automobile parts** — the text says "We DO NOT
  machine automobile parts!"
- `ableengineering conformity` **DER** — a staff job title in a personnel
  roster, not a company attestation; and only 2 of 18 case-insensitive hits
  are the acronym (the rest sit inside *provider*, *leader*, *grinders*).
- `ableengineering equipments` **CNC shot peen** — "CNC shot peen and manual
  capabilities" describes how a process is run; no machine is named.
- `alecmfg conformity` **D-U-N-S Number** — a business registry identifier.
- `alecmfg equipments` **PowerMill** — CAM software, not a machine.
- `alecmfg industries` **marine** — an explainer blurb and a spec cell, no
  marine client.
- `austinelectric conformity` **Arizona ROC license** — a contractor licence,
  not a conformity attestation.
- `steelcraft process_caps` **Embossing** — every surface is product-side
  ("CE Series embossed doors"); the operation is inferred, never named.

## Eval-set quality flags raised by the verification pass (not defects — read before trusting a single field's recall)

- **Over-broad acceptable forms** inflate recall for their entry: steelcraft
  `commercial` (56 mostly-adjectival occurrences), `industrial`,
  `institutional`; ableengineering `Part 145` ⊂ `EASA PART 145`, `NADCAP` ⊂
  its scoped siblings, `testing` ⊂ `Non-Destructive Testing`; anchor-mfg
  `Steel` ⊂ every other steel entity, `press brake` ⊂ `CNC 7 Axis Press
  Brakes`, `shear` ⊂ `Shearing`.
- **Series-form containment collisions** (steelcraft products): `T Series` ⊂
  `FT Series`, `L Series` ⊂ `SL Series`, `K Series` ⊂ `CK Series`. A judged
  pass must attribute these per window.
- **Short forms are already safe**: `EMS` is NOT credited by "quality
  assurance systems" — forms ≤3 chars match on word boundaries,
  case-sensitively.
- **One forms gap**: steelcraft `material_caps-0005` does not list the
  `Galvanealled` (single-n) spelling that occurs in the text, so a faithful
  echo of that occurrence would not credit. Left for the user to accept or fix
  (changing forms is an eval-set change, not a verification act).

See EXPECTATIONS_SCHEMA.md "Two scoring properties that are DELIBERATE" for
why containment leniency is kept rather than tightened.

## Recommendations recorded, deliberately NOT applied (they add or change entries)

- steelcraft: narrow the `anchor prep` false friend to "anchoring"/"anchor
  locations" and seed "Special anchor preps" as its own entry — the
  verification pass found both occurrences sit in service-center capability
  menus (own factory work), not installation-site work.
- ableengineering: add `Rotor Wing PMA` / `Fixed Wing PMA` as products
  false friends (datasheet literature titles).
- anchor-mfg: laser cutting has no process_caps entity though both laser
  lines are seeded as equipment.
- steelcraft: USGBC and CDPH (conformity); polishing/etching (process_caps);
  "kwik-pak doors", "WS Strikes" (products) observed but unseeded.

## Corrections applied to the eval set itself (2026-08-26)

- **acimachine industries.yaml + subject.yaml carried a FALSE grep claim** —
  "Aerospace/automotive/medical/defense/agriculture: zero hits". The real
  counts are aerospace 11 lines, automotive 10, mining 10, medical 1,
  semiconductor 1 (all inside third-party OEM marketing on resold-machine
  listings); only defense and agriculture are genuinely absent. Both files
  corrected. `expected_empty: true` still holds — no sector is claimed BY ACI
  — but a run returning "aerospace" is echoing real text, not hallucinating.
- **acimachine's `Energy` false friend was mis-reasoned** as word-association;
  it is a POLYSEME (Uni-Hydro "transfer energy" prose AND Hurco's "aerospace
  and energy sectors"). Reason corrected; it stays a false friend because
  neither sense is ACI's own sector claim.
- **Cross-subject contradiction on field-site work (code `I`) RULED** — see
  TAXONOMY.md "Rulings on codes that two subjects read differently". Recall
  and precision answer different questions: an operation the subject itself
  performs is a recall target wherever performed, while `I` still rolls up to
  adjacent_field for precision. acimachine's entries stand; austinelectric
  stays deliberately under-seeded as the negative probe.

## Known eval-set defects left for the user (changing forms/entries is not a verification act)

- steelcraft `material_caps-0005` omits the `Galvanealled` (single-n) spelling
  that occurs in the text.
- acimachine `HP-5` is contained in `JET HP-5A`, a different machine.
- acimachine `UL/CSA motor` and `UL motor` are covered by neither entries nor
  false friends.
- austinelectric's `"driver's license"` false friend uses a straight
  apostrophe where the text has U+2019, so it can never match; `brazing` is
  listed where the text says `braze`.
- austinelectric products: Glass Break Detectors, Panic Buttons, Smoke & CO
  Detectors, Flood Sensors, Remote Key Fobs appear in neither list.

## Two matcher defects the mechanical sweep exposed (both fixed 2026-08-26)

1. **Non-breaking spaces.** steelcraft carries 843 NBSPs MID-PHRASE ("in
   accordance with\xa0ASTM D4585"). Unnormalized, a correct form scored
   `not_in_window` (fabrication) and its entity read as a recall miss — false
   REDs on correct output.
2. **Whitespace runs and short-form substrings.** Double spaces inside phrases
   ("Full  glass architectural entrance doors") scored faithful echoes as
   fabrication; and plain containment credited "tight" for `TIG`, the brute
   `Lead` trap reproduced inside the eval.

Both now live in `_shared/text_matching.py` so every stage instrument
inherits the protection, and are pinned by tests there and in
`../tests/test_search_eval_harness.py`.
