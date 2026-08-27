# Expectation-set verification ledger

Promotion policy (user decision 2026-08-26): entries are seeded as `candidate`;
a SECOND independent reader promotes to `confirmed`; only `confirmed` entries
gate recall. `disputed` never gates. The user may veto anything.

## State: the corpus is 20 subjects; 19 are two-pass verified (2026-08-27)

Run `.venv/bin/python checks/validate_expectations.py` for live numbers — it is
the mechanical re-check this ledger used to describe in prose, and it re-derives
every count from the files. Last full run: **20 subjects, 9,499 entries,
11,178 quotes, 0 errors — 8,462 confirmed (89.1%), 666 candidate, 322 disputed,
49 retired.** Every one of the 20 is now two-pass verified.

Two independent passes stand behind every `confirmed` entry: the seeding read,
and a verification read by a different agent that re-derived each judgment from
the text rather than from the seed's notes (`SEEDING_BRIEF.md` /
`VERIFY_BRIEF.md` are the two procedures). Only `confirmed` gates recall;
`candidate` and `disputed` never do.

### Round 1 — the original 8 (2026-08-26)

522 entries / 751 quotes. The alecmfg figures below are as re-counted on
2026-08-27: six metrology entries were moved to `disputed` when the equipments
boundary was corrected, which this table previously did not reflect.

| subject | entries | confirmed | disputed |
|---|---|---|---|
| 101machine_com | 41 | 40 | 1 |
| ableengineering_com | 85 | 83 | 2 |
| acimachine_com | 25 | 25 | 0 |
| alecmfg_com | 135 | 126 | 9 |
| anchor-mfg_com | 76 | 76 | 0 |
| austinelectricservices_com | 15 | 14 | 1 |
| steelcraft_com | 132 | 131 | 1 |
| taylordunn_com | 13 | 13 | 0 |

### Round 2 — the 12 corpus-expansion subjects (2026-08-27)

8,465 entries / 9,906 quotes. `candidate` here means either an entity the
verifying pass ADDED (its first reading, so not yet confirmable) or — for
agstech — a packet still in verification.

| subject | entries | confirmed | candidate | disputed | retired | non-`own` actor |
|---|---|---|---|---|---|---|
| med-tekinc_com | 53 | 49 | 3 | 1 | 0 | 3 |
| superiortech_org | 309 | 272 | 9 | 27 | 1 | 71 |
| mathewsco_com | 261 | 236 | 7 | 18 | 0 | **239** |
| decimal_net | 390 | 350 | 20 | 20 | 0 | 93 |
| tanfel_com | 349 | 323 | 12 | 14 | 0 | 80 |
| fzemanufacturing_com | 490 | 447 | 16 | 26 | 1 | 157 |
| howcogroup_com | 356 | 338 | 12 | 6 | 0 | 59 |
| lucasmilhaupt_com | 429 | 386 | 21 | 22 | 0 | 156 |
| pradeepmetals_com | 319 | 283 | 11 | 25 | 0 | 127 |
| sterlingmfg_net | 360 | 339 | 11 | 10 | 0 | 35 |
| blackadvtech_com | 477 | 413 | 45 | 18 | 1 | 138 |
| agstech_net | 5,184 | 4,518 | 499 | 121 | 46 | 2,120 |

**Only 3 entries were retired across the eleven single-packet subjects' 3,793
verified entries**, and those passes ran 3,286 confirms against 187 disputes.
agstech, verified by ten packet agents over 4,672 entries, ran **4,394 confirms
(94.0%), 124 amends, 121 disputes and 33 retires** — 31 of the 33 being duplicate
twins rather than errors of fact. That the sliced subject lands at the same
shape as the single-reader ones is the main evidence that slicing did not cost
quality. Disputes cluster
exactly where `SEEDING_BRIEF.md` §4 predicts they will: memberships and registry
identifiers under `conformity_attestations`, metrology and material-handling
under `equipments`, and design/management activities under `process_caps`.

### What the round-2 subjects are FOR

- **`attribution_negative`** (mathewsco, tanfel, howcogroup, agstech) — the text
  names real in-field entities belonging to someone else. mathewsco is the
  extreme: **239 of 261 entries carry a non-`own` actor**. Its recall should be
  as high as any subject's; the wrong-party rate is measured downstream from
  those actor labels, NOT by thinning the inventory. Seeding these fields empty
  — which the corpus-expansion note's "correct answer is the empty set" framing
  would have produced — would have made every recall number here meaningless.
- **`genre_probe`** (pradeepmetals, blackadvtech, lucasmilhaupt, fzemanufacturing,
  agstech) — investor-relations filings, SEO process encyclopedias, second-person
  technical manuals, textbook chapters with "we" injected.
- **`empty_field_probe`** (med-tekinc) — a healthy site whose correct answer for
  `products` and `conformity_attestations` really is the empty list. Both claims
  were independently re-checked and both survived.

### agstech: what ten parallel verifiers agreed on without conferring

- **The tooling-as-products reading, 10 for 10.** The seeding pass read the
  products clause's exclusion — "the machinery, tooling, and facilities *the work
  is done with*" — as scoped to the SUBJECT'S OWN production, so drill bits and
  saw blades AGS-TECH sells as merchandise are products. Every one of the ten
  verifiers reached that reading independently, and several tested it in both
  directions rather than merely asserting it: packet 2 disputed tombstone
  fixtures and bed-of-nails devices quoted from "fixtures **we deploy throughout
  our job shops**"; packet 5 disputed wax patterns and welding electrodes the
  text says are consumed in the subject's own process; packet 3 disputed blanking
  dies named as the tooling of a route being *avoided*. The reading is settled.
- **The cross-slice gap risk did NOT materialise.** Several verifiers dropped
  candidate misses because the entity also occurred outside their own line range,
  assuming a sibling packet held it — the §6b failure mode in a new costume. All
  36 entities named that way were audited against the merged inventory
  afterwards: **36/36 covered, 0 gaps.**
- **Where the seeding slices were weak: prose, not lists.** 512 misses were
  added, skewed to products (160+) and process_caps (116+). Verifiers repeatedly
  found materials and processes dropped from the very sentences the seed had
  already quoted for a sibling entity, and whole enumerated lists harvested
  part-way. Slice agents covered catalog structure well and running prose less
  well — worth telling the next slice fan-out explicitly.

### Two predictions from the corpus-expansion vetting that the TEXT refuted

Both would have fired a false RED on correct output had they been seeded as
written.

1. **tanfel `equipments` was predicted genuinely empty.** True only of machine
   BRANDS — a sweep of 60+ machine-tool makers returns zero. The site names
   roughly twenty production machines in a first-person owned-fleet claim
   ("We use the following types of CNC lathes..."), plus tonnage-rated presses.
   19 of 23 confirmed by the second pass; the 4 that failed are at the clause's
   stated boundaries (a press ram, a conveying puller), not at its centre.
2. **med-tekinc `industries` was predicted hard-empty.** Line 19 names the
   served market plainly — "for machine shops and fabricators to use".

A third was refuted in DETAIL rather than direction: blackadvtech's `Hemming`
trap is real, but the process word occurs ZERO times (not as a 51-hit brand
substring), while `hems` IS a first-party capability and must find.

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

## A THIRD matcher defect, found by the round-2 seeding (fixed 2026-08-27)

**Typographic hyphens.** med-tekinc prints its ONLY capability as
`heat‑treating` with a U+2011 NON-BREAKING hyphen on two lines and an ordinary
hyphen on three others; agstech carries U+00AD SOFT hyphens mid-word
(`X­ray`, `high­resolution`) which render as nothing at all. `normalize_spaces`
folded Unicode spaces but not hyphens, so a faithful ASCII echo scored as a miss
on exactly the lines that mattered — a whole subject's recall resting on one
character.

Measured across all 20 corpus texts before changing shared code: 5 non-breaking
hyphens in 3 subjects, 3 soft hyphens in 1. U+2010/U+2011/U+2012/U+00AD now fold
to ASCII hyphen, one character for one character, so the offset guarantee holds.

**EN and EM dashes are deliberately NOT folded.** 1,046 of 1,094 en dashes in
the corpus are whitespace-adjacent separators; folding them would credit
`steel-and` for `steel—and`. That is false credit, which is the dangerous
direction — leniency is only safe when it rescues a faithful match. The
sterlingmfg verification pass then produced the case that proves the rule was
set correctly: the site prints `21 CFR 820 – Compliant` with an EN DASH on two
lines and with an ASCII hyphen on a third, so the entry's own evidence could not
credit it. The right fix was the one the verifier applied — add the en-dash
spelling as an acceptable form — not to fold the character globally.

## Eval-set defects the round-2 verification passes found (all fixed in place)

These are the class the 2026-08-26 ledger flagged as "over-broad acceptable
forms" and left open. They are **silent holes in the recall denominator**: the
entry can never independently miss, so its recall number carries no information.
`forms_overlap` is bidirectional casefold containment for forms over 3
characters, which is what makes them possible.

- **mathewsco** `rings` ⊂ `springs` and `bearings`, both themselves seeded
  products for that subject; `assembly` credited by the product phrase
  `Precision components and assemblies`.
- **howcogroup** `Iron` ⊂ **environ**ment — 49 occurrences, the worst in the
  corpus; `NACE` ⊂ fur**nace**; `hangers` ⊂ heat ex**changers**; `carbon`
  credited by the `Energy & Carbon Report` in every footer.
- **superiortech** three compound product forms paid out for their neighbours'
  entities; one certification was seeded twice under two punctuations, both
  double-counting the denominator AND booking a guaranteed false miss.
- **lucasmilhaupt** bare `dies` is a homograph (7 of 9 hits are stamping tooling,
  which the products clause excludes); `gear` credited by the `Lucas Gear`
  merchandise link in every page footer.
- **blackadvtech** 11 equipment pairs shared an identical acceptable form, so one
  returned phrase credited two entries.
- **pradeepmetals** the bare form `500` on the Monel entry word-matched
  `12,500 MT` and a `500 x 500 x 400 mm` work envelope.

Each was narrowed to a longer span that still credits the plain return by
containment but no longer matches the impostor. **Lesson for future seeding:**
prefer distinctive forms, and check every short or common form against the whole
text before seeding it — the seeding brief now says so explicitly.

### And now it is measured, not anecdotal: 9.8% of the eval set

`validate_expectations.py` check 5 flags every entry whose forms are ALL
substrings of sibling entries' forms in the same field. Such an entry is
credited whenever any sibling is returned, so its recall number carries no
information of its own. First full measurement, 2026-08-27:

| subject | entries | shadowed | | subject | entries | shadowed |
|---|---|---|---|---|---|---|
| agstech_net | 4,672 | 697 (14.9%) | | lucasmilhaupt_com | 429 | 23 (5.4%) |
| anchor-mfg_com | 76 | 8 (10.5%) | | mathewsco_com | 261 | 14 (5.4%) |
| 101machine_com | 41 | 3 (7.3%) | | decimal_net | 390 | 20 (5.1%) |
| superiortech_org | 309 | 21 (6.8%) | | blackadvtech_com | 477 | 21 (4.4%) |
| tanfel_com | 349 | 15 (4.3%) | | fzemanufacturing_com | 490 | 21 (4.3%) |
| howcogroup_com | 356 | 14 (3.9%) | | med-tekinc_com | 53 | 2 (3.8%) |
| pradeepmetals_com | 319 | 10 (3.1%) | | steelcraft_com | 132 | 3 (2.3%) |
| sterlingmfg_net | 360 | 8 (2.2%) | | alecmfg_com | 135 | 2 (1.5%) |
| ableengineering / acimachine / austinelectric / taylordunn | — | 0 |

**First measurement (before agstech's verification landed): 882 of 8,987 = 9.8%.
Current, with agstech's 512 verification misses folded in: 1,229 of 9,499 =
12.9%,** of which agstech alone contributes 1,044 — its inventory is dense
enough that hypernyms and their specific siblings routinely coexist.

**This is a WARNING, not an error, and most of it must not be "fixed".** The
containment leniency is deliberate (see `EXPECTATIONS_SCHEMA.md`, "Two scoring
properties that are DELIBERATE"): it is right on the recall question — the
capability WAS found — and tightening it would manufacture false REDs, which
this harness has already fired once. What the number buys is an honest caveat on
how to read a recall figure: roughly a tenth of the denominator is carried by
its siblings. agstech is highest at 14.9% simply because its inventory is the
densest in the corpus; the four subjects at 0% are the small and near-empty ones.

Act on an individual warning only when the shadowing is ACCIDENTAL — a homograph
or an unrelated word, like the six defects listed above — not when it is a
genuine hypernym sitting under its own specific siblings.

## FOUR-character forms are the danger zone (isolated 2026-08-27)

Forms of **three characters or fewer** match case-sensitively on word boundaries
(`TIG`, `ABS`, `CMM` are safe). Everything longer matches by plain bidirectional
casefold containment, with NO word boundary. That leaves four-character forms
exposed: long enough to lose the boundary rule, short enough to sit inside
ordinary words. Every instance the round-2 passes found:

| form | silently credited by | where |
|---|---|---|
| `STEM` | *system*, *systems*, *EDM systems* | agstech packet 3 |
| `Iron` | *environ*ment — 49 occurrences | howcogroup |
| `Hone` / `Hones` | *Phone* — 172 footer lines | agstech packet 9 |
| `NACE` | fur*nace* | howcogroup |
| `Lead` | *lead* time, *lead*ership | the original brute-search bug |

**Do NOT fix this in the matcher.** Extending word boundaries to all lengths
would break the inflectional containment `EXPECTATIONS_SCHEMA.md` documents as
deliberate — `shear` credited by `Shearing`, `press brake` by `CNC 7 Axis Press
Brakes`. Raising the case-sensitivity threshold instead would manufacture false
misses on ordinary casing differences. The correct fix is the one every verifier
applied: narrow the FORM to a longer span that still credits the plain return by
containment but no longer matches the impostor.

**Rule for future seeding, now in `SEEDING_BRIEF.md` §5:** before seeding any
four-character form, grep the whole text for it as a substring, not just as a
word.

## Name-keyed dedup does not collapse singular/plural twins

`build_expectations.py` deduplicates entries by `name`, casefolded and
whitespace-collapsed. That misses twins whose names differ only by inflection
(`V-pulley` / `V-pulleys`), so on a subject seeded by parallel slice agents the
same entity can be entered twice with identical forms AND identical evidence.
The agstech verification found this at scale: packet 2 disputed 17 duplicate
product pairs, packet 8 retired 10, packet 9 disputed 2.

Deliberately NOT fixed by loosening the dedup key. Stemming would over-merge
genuinely distinct entities (`die` vs `dies`, `press` vs `presses` are different
entries on some subjects), and the failure mode of over-merging is a silently
missing must-find entity — worse than a duplicate, which merely double-weights
one string in the denominator and is caught by the verification pass. The
verifiers' handling is the right one: confirm one, dispute or retire the twin,
name the survivor in the reason.

## A known LIMIT of round-2 seeding: one quote per entry narrows window scope

Raised by the agstech packet-6 verifier and deliberately not "fixed". Recall is
window-scoped: an entry is in scope for a run only in windows that contain one
of its evidence quotes. Most round-2 entries carry a single quote, so an entity
the site names on five pages is scored in one window and the stage is never
charged for missing it in the other four.

The direction matters. This makes recall **under-fire**, not false-fire — the
safe direction under recall-only gating, and the same stance
`EXPECTATIONS_SCHEMA.md` takes on containment leniency. It is a reason a round-2
recall figure is a FLOOR on the stage's real miss rate, not a reason to distrust
it.

Widening coverage is a deliberate future pass (add every occurrence of an entity
as additional evidence), not something to apply ad hoc per entry — doing it for
some entries and not others would make subjects incomparable. `apply_verifications.py`
already appends rather than replaces evidence, so that pass can be run
incrementally without losing anything.
