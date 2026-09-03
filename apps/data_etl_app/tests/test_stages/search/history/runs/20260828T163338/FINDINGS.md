# Search-stage evaluation — run 20260828T163338

Method: **difference-set judging under the standing rule** (user, 2026-08-28) that
the eval set is itself under test. 20 subjects x 6 fields = 116 scored units;
27,993 returned phrase-occurrences. Every returned form creditable against no
eval entry was judged (14,527 rows); every apparent miss was re-validated against
the window text (179); every candidate entry the run matched was re-read (122);
and a seeded random sample of AWARDED credits was re-checked (410) — the last of
these is what caught the instrument defect below.

**Comparability.** Model (`gpt-4.1`) and search prompt version are unchanged from
baseline 20260825T194457, but window bounds are NOT: `search_divisor=4` sub-windows
are new, so per-window deltas against baseline are not readable. alecmfg and
steelcraft are cache-replayed; the other 18 subjects are live.

**Reliability floor.** 36 units were judged a second time by an independent reader
with no sight of the first pass: over 2,689 shared forms, **81.0% exact-code and
88.7% same-bucket agreement**. No judged difference below ~11 points is readable.

---

## Numbers

### Recall (confirmed entries covered)

Scored twice: under the credit rule this run shipped with, and under the F1 fix,
which was made and the whole battery re-run on 2026-08-28.

| field | scored | before fix | AFTER FIX | delta | newly missed |
|---|---|---|---|---|---|
| products | 973 | 97.3% | **94.3%** | -3.0 | 29 |
| conformity_attestations | 290 | 95.9% | **95.2%** | -0.7 | 2 |
| process_caps | 1173 | 95.3% | **93.9%** | -1.4 | 17 |
| material_caps | 688 | 94.8% | **92.7%** | -2.0 | 14 |
| industries | 427 | 94.6% | **94.1%** | -0.5 | 2 |
| equipments | 305 | 91.1% | **90.2%** | -1.0 | 3 |
| **ALL** | **3856** | **95.4%** | **93.6%** | **-1.7** | **67** |

**93.6% is a floor, not the answer.** Per the standing rule the 67 new misses
were themselves validated: 16 of them are eval-set defects, not stage misses
(F6). Treating all 16 as wrongly-charged puts true recall at **94.0%**. Read
the range 93.6–94.0%, and 88.7% agreement beside every judged number.

### Verbatim fidelity (fabrication tripwire)

27,993 judged: 97.8% exact, 1.4% casing-only, **126 (0.45%) not in the window text
at all**. Worst field process_caps at 0.61%.

### In-field share of everything returned

Credited forms counted as in-field (optimistic; F1 says ~8% of those credits are wrong):

| field | returned | credited | uncredited | in-field of uncredited | in-field share |
|---|---|---|---|---|---|
| conformity_attestations | 1184 | 1007 | 177 | 14.7% | 87.2% |
| material_caps | 3029 | 2374 | 655 | 22.9% | 83.3% |
| process_caps | 6575 | 4250 | 2325 | 42.1% | 79.5% |
| equipments | 2554 | 806 | 1748 | 67.1% | 77.5% |
| industries | 2877 | 1742 | 1135 | 18.2% | 67.7% |
| products | 11774 | 3287 | 8487 | 16.4% | 39.7% |
| **ALL** | **27993** | **13466** | **14527** | **27.0%** | **62.1%** |

Rollup of the 14,527 judged uncredited forms: in_field 27.0% · adjacent (a real
designation, but for a DIFFERENT field) 56.0% · generic 12.3% · junk 4.7%.

---

## Findings

### F1 — the harness awards false credit; every recall number here is ~2 points high

`_shared/text_matching.forms_overlap` does **bidirectional** casefold containment
for any form longer than `SHORT_FORM_MAX_LENGTH` (3). A bare generic word therefore
credits any specific eval entry containing it. Verified at code level — all True:

    parts       -> "metal structural parts"      titanium -> "Titanium fusion cages"
    steel       -> "Steel Front Panel"           machining -> "EDM machining"
    industries  -> "Aerospace industries"        Cutting  -> "laser cutting"

Blind spot-check of 410 awarded credits: **33 false, 8.0% overall**; by field
products 19.1% · industries 9.5% · process_caps 8.3% · equipments 6.1% ·
material_caps 4.4% · conformity_attestations 0%.

That 8% is per (form, entry) PAIR. Entry-level damage is smaller because most
entries are credited by several forms: re-scoring under the one-directional rule
moves overall recall **95.4% -> 93.7%**, products **97.3% -> 94.3%**.

This is the same family as the historical `Lead` / `lead time` bug, but at the
credit level, where it only ever FLATTERS the stage — which is why three prior
matcher fixes missed it. It was discoverable only because the standing rule forces
spot-checks of the MATCHED set, not just the two difference sets.

**FIXED 2026-08-28, battery re-run.** `forms_overlap` is replaced by
`form_covers(expected, returned)` in `_shared/text_matching.py`: one-directional,
plus a LEFT word boundary at every length (the right edge stays open, which is
what keeps the documented `shear` -> `Shearing` leniency alive — see
EXPECTATIONS_SCHEMA.md "Two scoring properties that are DELIBERATE"). The
exporter shares the same compiled pattern so the two cannot drift; the pattern
builder is memoized because scoring calls it millions of times and `re`'s own
cache holds 512.

Verification: **29 of the 33 judge-confirmed false credits are killed.** The 4
survivors are semantic, not mechanical, and two of them are eval-set problems:

  'Boeing 737 Max Leap-1B Engine Mounting System' still covers 'Engine mounts'
      (via acceptable form 'engine mount' + the deliberate open right edge)
  'automation lines and equipment' still covers 'industrial automation'
  'Miller Syncrowave 350 LX' still covers 'Miller syncrowave 350'
      (the site lists them as two separate machines)
  'high-voltage switchgear' still covers 'switch contact assemblies'
      (that entry lists 'switchgear' as an acceptable form — fix the entry)

Residue kept knowingly: `steel` still covers `stainless steel`, because
EXPECTATIONS_SCHEMA pins that as deliberate. Whether it should stay is a
separate, measurable decision, not a matcher bug.

### F2 — equipments asks a question most subjects cannot answer, and the stage never says "none"

Four subjects returned **0% in-field**: steelcraft (93 forms), alecmfg (30),
anchor-mfg (6), 101machine (3). Hand-verified on steelcraft: 82 of 93 forms are the
company's own doors and frames, 8/8 sampled forms are verbatim in the packet, and
`grep -ciE "press brake|shear|cnc|milling machine|lathe|welder|roll former"` over
its window texts returns **0**. The correct answer was the empty list.

**Do not quote the 67.1% equipments in-field figure.** acimachine.com is a used-machinery
dealer contributing 964 of the 1,748 judged forms at 99.2%; its expectations are
documented as SAMPLED. Excluding it, equipments in-field is **27.7%**.

Per-subject in-field: acimachine 99.2 · austinelectric 92.3 · blackadvtech 84.2 ·
pradeepmetals 75.0 · howco 57.1 · taylordunn 50.0 · fze 42.0 · lucasmilhaupt 35.7 ·
agstech 34.6 · tanfel 26.1 · decimal 20.0 · mathewsco 16.7 · ableengineering 14.3 ·
superiortech 12.9 · sterlingmfg 11.6 · **101machine / alecmfg / anchor-mfg / steelcraft 0.0**.

### F3 — field boundaries leak; this is the dominant defect, not recall

56.0% of everything the stage returns beyond the eval set is a real designation for
a DIFFERENT field. Top codes per field make the direction explicit:

- products: P process/service 29% · S spec/attribute 22% · M material 14% · G generic 13% · V valid 9%
- process_caps: V 35% · E equipment 21% · G 17% · Z business/legal activity 11%
- material_caps: P process 27% · N part name 21% · V 15% · G 14%
- industries: U junk 35% · O the subject's OWN activity as a sector 33% · B 15%
- conformity_attestations: G 40% · U 38% — only 15% in-field, the weakest signal of any field

### F4 — judges will bless a fabricated phrase

The merge could not locate 40 judge-declared in-field forms in their window text;
**32 of those 40 are independently flagged by the mechanical verbatim check** as
`not_in_window`. Two independent mechanisms agree the text does not contain them,
and a human reader still coded them as valid designations. Examples:
`ACER AGS-24100AHD`, `NOMURA BN-100SR` (acimachine products), `AMS 5663`, `AMS 5664`,
`ASTM B564`, `Material 625` (howco material_caps), `Pratt & Whitney 4000`,
`STC 43`, `MIG machine`. Model numbers and standard designations are exactly where a
fabricated phrase is most plausible to a reader and most damaging downstream.
These are recorded in DIFFSET.md and were **NOT seeded** into the eval set.

### F5 — the eval set had 8 defective confirmed entries

Re-validation of the 179 apparent misses returned 171 `valid_miss` and **8
`entry_wrong`** — entries that would have scored as stage failures. Causes: the
evidence quote is absent from the window (`Tin` at blackadvtech, `Die Hammers` at
pradeepmetals), or the quote is verbatim but is generic encyclopedic prose rather
than a claim about the subject (`wet paint` at blackadvtech, `biocompatible
materials` at lucasmilhaupt), or the quote describes a person's employment history
rather than the company's offering (2 pradeepmetals products entries).

Of 122 candidate entries re-read: **116 confirm**, 2 dispute, 4 not_found.

### F6 — the fix exposed 16 eval-set entries whose acceptable forms are comma lists

Applying the standing rule to the fix itself: of the 67 entries that flipped from
covered to missed, **16 have a comma-joined `acceptable_forms` value scraped
whole from the page** while the entry names a single item — `balls` with
`['Balls, bearings, pulleys']`, `evaporation` with `['evaporation, plating']`,
`worm` with `['worm, speed reducer']`, `Drills, mills, taps` with `['drills,
mills, and taps']`. Search returned exactly the right token (`Balls`,
`evaporation`) and is now charged a miss for it. **These are eval-set defects the
old leniency was hiding, not stage regressions**, and they are why the honest
post-fix figure is a range and not a point.

The other 51 are correct tightenings, and several are exactly what the fix was
for: three distinct welding processes at blackadvtech (`flux-cored`, `gas metal`,
`gas tungsten arc welding`) were all being credited by one generic returned
`Arc welding`; `11 gauge steel` and `18 ga steel` were both credited by `steel`.

Remedy is per-entry, not per-matcher: split the list entries, or add the
individual item to `acceptable_forms`. Until then those 16 read as misses.

---

## Proposed eval-set changes — AWAITING USER VETO, nothing applied

`evolution/<slug>.jsonl` (apply_verifications.py) and `evolution/<slug>__gaps.jsonl`
(build_expectations.py --merge). Written as proposals only.

- **116 promote** candidate -> confirmed (second reader verified)
- **4 dispute**, **4 retire** — the defective entries in F5
- **3,885 new-entry seeds** from in-field unmatched forms, each with a mechanically
  derived verbatim quote: products 1379 · equipments 1172 · process_caps 962 ·
  industries 205 · material_caps 142 · conformity_attestations 25

Read the equipments seeds with F2 in mind: 956 of the 1,172 come from acimachine alone.

---

## Recommended order of work

1. ~~Fix F1 and re-run the battery.~~ **DONE 2026-08-28** — numbers above are
   post-fix. Every recall figure from a run scored before this date is ~1.7
   points high (products ~3).
2. Repair the 16 comma-list entries in F6, then re-score; that closes the
   93.6–94.0% range into a single number.
3. Decide the F2 question: should equipments be asked at all for subjects with no
   machine vocabulary, and does the stage need an explicit "return nothing" path?
4. F3 is the real prize but needs a prompt change and a re-run to measure.

## Cross-field collision baseline (pre-prompt-edit)

Frozen here 2026-08-29 as the comparison point for the three `What qualifies`
edits made that day (products: operations/substances are not artifacts;
industries: a served sector is not your own offering; process_caps: commercial
and logistics services are not operations). SUMMARY.md now emits this table on
every run — mechanical, no judges, so no agreement floor applies.

| field | distinct forms | claimed by a sibling | share |
|---|---|---|---|
| equipments | 2096 | 1599 | 76.3% |
| material_caps | 1900 | 1159 | 61.0% |
| products | 8449 | 4372 | 51.7% |
| process_caps | 4642 | 2269 | 48.9% |
| industries | 1824 | 697 | 38.2% |
| conformity_attestations | 685 | 129 | 18.8% |
| **ALL** | **19596** | **10225** | **52.2%** |

| field pair | shared | Jaccard |
|---|---|---|
| process_caps ↔ products | 2059 | 18.7% |
| equipments ↔ products | 1532 | 17.0% |
| material_caps ↔ products | 1109 | 12.0% |
| industries ↔ products | 578 | 6.0% |
| equipments ↔ process_caps | 560 | 9.1% |

Counted another way: **4,604 distinct phrases (23.5% of the 19,597 distinct
(subject, phrase) pairs) are claimed by two or more fields.** Those same 4,604
phrases occupy 10,225 of the 19,596 field-slots, which is where 52.2% comes
from — the two numbers are the same fact under different denominators.

**A fall here is only good news if confirmed recall holds.** 43% of confirmed
process_caps entries and 67% of confirmed equipments entries legitimately appear
in a sibling's window, so some of this collision is the shape of the domain, not
a defect. Expected exposure of the three edits: 391 products + 98 industries
confirmed entries sit on a sibling axis; losing more than a couple means a
clause is over-firing.

## Provenance note on the diff-set block

The scorecards' `metrics.diffset` numbers, DIFFSET.md and the evolution proposals
were produced from packets partitioned under the PRE-fix credit rule — that is
what the judges actually read, so they are reported as judged. Under the new rule
a few forms that were credited (and so never judged) would now fall into the
unmatched pile; that residue is unjudged and is not counted anywhere as in-field.
