# HANDOFF — search-stage eval set, corpus expansion to 20 subjects

**Purpose.** Everything a person or agent needs to pick this up cold. Read this
first, then the file it points you at for the topic you care about. It is
written to be **appended to**, not rewritten: put new work in a dated entry
under §12 and correct the state tables in §1 in place.

**Scope.** This covers the SEARCH stage's expectation set (`expectations/`), the
tooling that builds and verifies it (`checks/`), and the cross-stage changes that
came out of that work. Sibling stage instruments (`grounding/`, `synthesis/`,
`mention_collection/`) are owned by other sessions — see §10 for what they owe
and what they are owed.

---

## 1. State, as of 2026-08-27

**The eval set is complete and two-pass verified for all 20 corpus subjects.**
Nothing is half-finished. The numbers below are re-derivable at any time:

```bash
cd apps/data_etl_app/tests/test_stages/search
../../../../../.venv/bin/python checks/validate_expectations.py     # ~100s, all 20
```

**9,499 entries · 11,178 evidence quotes · 0 validation errors**
8,462 confirmed (89.1%) · 666 candidate · 322 disputed · 49 retired

| subject | role | entries | conf | cand | disp | retd | quotes | non-`own` actor | shadowed |
|---|---|---|---|---|---|---|---|---|---|
| `101machine_com` | inventory_subject | 41 | 40 | 0 | 1 | 0 | 52 | 0 | 3 |
| `ableengineering_com` | inventory_subject | 85 | 83 | 0 | 2 | 0 | 143 | 3 | 0 |
| `acimachine_com` | scale_stress | 25 | 25 | 0 | 0 | 0 | 28 | 16 | 0 |
| `agstech_net` | attribution_negative | 5,184 | 4,518 | 499 | 121 | 46 | 5,405 | 2,250 | 1,044 |
| `alecmfg_com` | full_unit | 135 | 126 | 0 | 9 | 0 | 203 | 22 | 2 |
| `anchor-mfg_com` | inventory_subject | 76 | 76 | 0 | 0 | 0 | 101 | 0 | 8 |
| `austinelectricservices_com` | out_of_domain_negative | 15 | 14 | 0 | 1 | 0 | 28 | 12 | 0 |
| `blackadvtech_com` | genre_probe | 477 | 413 | 45 | 18 | 1 | 635 | 138 | 21 |
| `decimal_net` | full_unit | 390 | 350 | 20 | 20 | 0 | 514 | 93 | 20 |
| `fzemanufacturing_com` | genre_probe | 490 | 447 | 16 | 26 | 1 | 497 | 157 | 21 |
| `howcogroup_com` | attribution_negative | 356 | 338 | 12 | 6 | 0 | 511 | 59 | 14 |
| `lucasmilhaupt_com` | genre_probe | 429 | 386 | 21 | 22 | 0 | 576 | 156 | 23 |
| `mathewsco_com` | attribution_negative | 261 | 236 | 7 | 18 | 0 | 330 | **239** | 14 |
| `med-tekinc_com` | empty_field_probe | 53 | 49 | 3 | 1 | 0 | 78 | 3 | 2 |
| `pradeepmetals_com` | genre_probe | 319 | 283 | 11 | 25 | 0 | 504 | 127 | 10 |
| `steelcraft_com` | full_unit | 132 | 131 | 0 | 1 | 0 | 182 | 6 | 3 |
| `sterlingmfg_net` | full_unit | 360 | 339 | 11 | 10 | 0 | 583 | 35 | 8 |
| `superiortech_org` | full_unit | 309 | 272 | 9 | 27 | 1 | 408 | 71 | 21 |
| `tanfel_com` | attribution_negative | 349 | 323 | 12 | 14 | 0 | 386 | 80 | 15 |
| `taylordunn_com` | degenerate_input_probe | 13 | 13 | 0 | 0 | 0 | 14 | 0 | 0 |

Entries by field across all 20: products 3,516 · process_caps 2,485 ·
material_caps 1,423 · industries 775 · equipments 726 ·
conformity_attestations 574. (`products.yaml` doubles as the
`contract_products` inventory — they share one physical search request. There is
no seventh file.)

**Status semantics.** Only `confirmed` gates recall. `candidate` = seen by one
reader only (a seed not yet verified, or a miss the verifying pass ADDED, which
is its first reading). `disputed` = real in the text but its field membership is
arguable; never gates, argument preserved. `retired` = wrong, kept for the
record, exempt from the quote checks.

**Test suite:** `.venv/bin/python -m pytest apps/data_etl_app/tests/test_stages/ -q`
→ **75 passed**. All new code is pyright-clean
(`--pythonpath .venv/bin/python`).

**Everything is uncommitted** in the working tree (19 new paths, ~67 modified).
Two of those pre-date this work and are somebody else's:
`mention_collection/EVAL_PLAN.md` and
`test_stages/GOLDEN_CORPUS_EXPANSION_2026-08-27.md`.

---

## 2. The task, and the user's locked decisions

The corpus grew from 8 to 20 subjects (see
`../GOLDEN_CORPUS_EXPANSION_2026-08-27.md`). The 12 new ones had scraped text
but **no golden data**, so on any run they would have gated nothing —
and `confirmed_recall: null` reads as "not gated", never "no misses", so the
silence would have been invisible.

Asked and answered by the user on 2026-08-27:

| question | answer |
|---|---|
| Which stage is "the third stage"? | **The search stage.** (It is stage 1 in the prompt numbering, but its expectations are the only golden data producible without a pipeline run — which is what "haven't entered the pipeline" describes.) |
| How many of the 12, and how deep? | **All 12, all exhaustive** — including agstech's 1.8 MB read in full, not sampled. |
| Verification depth? | **Two passes → `confirmed`.** Only confirmed entries gate. |

Standing authorization for unlimited agent fan-out (from `RUNBOOK.md`) applied.
About 40 agents ran in total.

---

## 3. The procedure — exact commands

All paths relative to `apps/data_etl_app/tests/test_stages/search/`;
`PY=../../../../../.venv/bin/python`.

```bash
# 1. SEED  — one agent per subject, following expectations/SEEDING_BRIEF.md.
#    The agent emits JSONL only; it never writes YAML.
$PY checks/build_expectations.py --slug <slug> --jsonl <seed>.jsonl
#    Options: --merge folds into existing files instead of replacing entries.

# 2. VALIDATE — the mechanical gate. Must be 0 errors before going on.
$PY checks/validate_expectations.py --subject <slug>

# 3. PACKET — strip the seed's notes/actors/provenance so pass 2 is independent.
$PY checks/export_verify_packet.py --slug <slug> --out <dir>
#    For a huge subject: --max-entries 470  → numbered packets, clustered BY
#    LOCATION with a line-range hint, so each agent reads a contiguous region.

# 4. VERIFY — one agent per packet, following expectations/VERIFY_BRIEF.md.

# 5. APPLY — records the promotion with provenance and bumps eval_set_version.
$PY checks/apply_verifications.py --slug <slug> --jsonl <verdicts>.jsonl \
    --by verify-agent-<name>
#    For a sliced subject, `cat` the packet verdict files together first and
#    apply once, so miss-entry id numbering stays consistent.

# 6. VALIDATE again. Then update expectations/VERIFICATION_LEDGER.md.
```

**Why agents emit JSONL and not YAML:** id numbering, `approx_offset`, the
snapshot header, the sha256 and the 200-char quote cap are mechanical concerns.
Putting them inside a judgment task is how they get quietly wrong. The builder
owns all of them; the agent only does the part that needs reading.

---

## 4. Files: what exists and why

### Documents (the procedure of record)
| file | what it is |
|---|---|
| `expectations/SEEDING_BRIEF.md` | Handed verbatim to every seeding agent. **Quotes all six `What qualifies` clauses verbatim from the search prompts** — §4. §6b covers slicing. Its §2 is the anti-false-RED rule. |
| `expectations/VERIFY_BRIEF.md` | The independent second pass. Four verdicts + `miss` rows. |
| `expectations/VERIFICATION_LEDGER.md` | Per-subject counts, every measured trap, and the reasoning behind each deliberate NON-fix. The place to look before trusting any recall number. |
| `EXPECTATIONS_SCHEMA.md` | The entry contract. Extended with the `role` vocabulary (§ below) and the "seed to the clause, not the field name" rule. |
| `METRICS.md` | Now records how much of a recall figure is load-bearing. |
| `RUNBOOK.md`, `README.md` | Updated for a 20-subject corpus. |

### Code (`checks/`, all new unless noted)
| file | what it does |
|---|---|
| `build_expectations.py` | Agent JSONL → schema-conformant YAML. Assigns ids, computes offsets, writes the snapshot header, sets `status: candidate`, dedups by name, auto-trims over-long quotes to a window around a covering form, drops false friends that collide with a seeded form, honours `status_hint: disputed` (and a `notes:` starting `DISPUTED` as a fallback). A seed can never self-confirm. |
| `validate_expectations.py` | The mechanical gate, 6 checks (see its docstring). Formerly an ad-hoc hand check described in the ledger. |
| `export_verify_packet.py` | Stripped packets for pass 2; `--max-entries` splits by LOCATION with line-range hints. |
| `apply_verifications.py` | Applies verdicts with provenance, bumps `eval_set_version`, appends (never replaces) corrected quotes, rejects misses whose quote is not verbatim, and **reports unjudged entries by id** — silence is not consent. |
| `paths.py` *(modified)* | `SAMPLE_TEXTS_DIR` repointed; see §9. |
| `tests/test_search_eval_harness.py` *(modified)* | +8 tests covering the seeding pipeline. |

### Shared, cross-stage
| file | change |
|---|---|
| `../_shared/text_matching.py` | **Trap 3 added: typographic hyphens.** Affects every stage instrument. |
| `../_shared/tests/test_shared_text_matching.py` | +4 tests pinning it. |
| `../_shared/README.md` | Documents the new trap for sibling owners. |

---

## 5. THE decision that mattered most

`GOLDEN_CORPUS_EXPANSION_2026-08-27.md` says mathewsco's correct answer is
"THE EMPTY SET" for six of seven capability fields, and that blackadvtech's
blog-only processes should be seeded as "NEGATIVE ground truth".

**Both are wrong for this stage, and following them literally would have
destroyed the measurement.** Search is recall-first: it extracts in-field
designations the TEXT NAMES, and deciding whose capability it is happens
downstream. A process printed in an SEO blog article is a real process
designation, so search returning it is correct behaviour. Seeding those fields
empty would have made search look broken for doing its job.

Seeded correctly, **mathewsco has 261 entries of which 239 carry a non-`own`
actor** — and its recall should be as high as any subject's. The wrong-party
rate is then measured downstream from the `actor` labels, which is where it
belongs. Corpus-wide, 3,467 of 9,499 entries carry a non-`own` actor.

That doc is a *vetting* record written for a different question. Treat its
capability claims as leads about the company, never as golden data for search.

---

## 6. Everything measured, and the deliberate non-fixes

Each of these is a place where the obvious fix is wrong. The reasoning is in
`VERIFICATION_LEDGER.md`; the short version:

### 6.1 Typographic hyphens — FIXED in the shared matcher
`normalize_spaces` folded Unicode spaces but not hyphens. med-tekinc prints its
**only** capability as `heat‑treating` with U+2011 on two lines and an ASCII
hyphen on three others; agstech carries U+00AD SOFT hyphens mid-word (`X­ray`,
`high­resolution`) that render as nothing. A faithful echo scored as a miss.
Measured across all 20 texts **before** touching shared code: 5 non-breaking
hyphens in 3 subjects, 3 soft hyphens in 1. U+2010/2011/2012/00AD now fold to
ASCII hyphen, one character for one character, so the offset guarantee holds.

**EN and EM dashes are deliberately NOT folded.** 1,046 of 1,094 en dashes are
whitespace-adjacent separators; folding them would credit `steel-and` for
`steel—and`. That is false credit — the dangerous direction. sterlingmfg then
proved the rule right: it prints `21 CFR 820 – Compliant` with an en dash on two
lines and an ASCII hyphen on a third, so the entry's own evidence could not
credit it. The correct fix was local — add the en-dash spelling as a FORM.

### 6.2 Four-character forms are the danger zone — DO NOT fix in the matcher
Forms ≤3 chars match case-sensitively on word boundaries. Everything longer
matches by plain bidirectional casefold containment with **no** word boundary.
Four-character forms are one character over the guard and short enough to sit
inside ordinary words. Six live cases found independently:

| form | credited by | found in |
|---|---|---|
| `STEM` | *system*, *systems* | agstech packet 3 |
| `Iron` | *environ*ment — 49 hits | howcogroup |
| `Hone` / `Hones` | *Phone* — 172 footer lines | agstech packet 9 |
| `NACE` | fur*nace* | howcogroup |
| `NATO` | *Coordinator*, *terminators* — 12 of 13 hits | agstech packet 1 |
| `Lead` | *lead* time, *lead*ership | the original brute-search bug |

Extending word boundaries to all lengths would break the inflectional
containment the schema documents as deliberate (`shear` credited by `Shearing`,
`press brake` by `CNC 7 Axis Press Brakes`). Raising the case-sensitivity
threshold would manufacture false misses. **Fix the FORM, never the matcher.**

### 6.3 Shadowed entries — 12.9%, a caveat not a defect list
An entry whose forms are ALL substrings of sibling entries' forms is credited
whenever any sibling is returned, so it can never independently miss.
`validate_expectations.py` check 5 flags them. **1,229 of 9,499 = 12.9%**
(882/8,987 = 9.8% before agstech's misses landed); agstech alone contributes
1,044. The leniency is deliberate and must stay — tightening it manufactures
false REDs, which this harness has already fired once. Read a subject's recall
knowing part of its denominator is carried by its siblings. Act on an individual
warning only when the shadowing is ACCIDENTAL (a homograph), not when it is a
genuine hypernym under its own specific siblings.

### 6.4 Name-keyed dedup misses singular/plural twins — DO NOT stem
`build_expectations.py` dedups on casefolded, whitespace-collapsed `name`, so
`V-pulley` / `V-pulleys` both survive. agstech's verification found this at
scale (packet 2 disputed 17 duplicate product pairs, packet 8 retired 10).
Stemming would over-merge genuinely distinct entries (`die` vs `dies`), and a
silently missing must-find entity is worse than a duplicate — which merely
double-weights one string and is caught by the verification pass.

### 6.5 One quote per entry narrows window scope — a FLOOR, not a flaw
Recall is window-scoped: an entry is in scope only in windows containing one of
its evidence quotes. Most round-2 entries carry a single quote, so an entity
named on five pages is scored in one window. This makes recall **under-fire**,
not false-fire. Widening coverage is a deliberate future pass (§11), not
something to apply ad hoc — doing it for some entries makes subjects
incomparable. `apply_verifications.py` appends rather than replaces evidence, so
that pass can run incrementally.

---

## 7. Per-subject notes for the 12 new subjects

Condensed; each subject's `subject.yaml` carries its full `context`,
`scrape_hazards` and `sampling_notes`.

- **`med-tekinc_com`** (7 KB, `empty_field_probe`) — heat-treating toll
  processor. `products` and `conformity_attestations` are genuinely empty and
  both claims survived independent re-checking. **Prediction refuted:**
  `industries` was predicted hard-empty; line 19 names the served market
  ("for machine shops and fabricators to use"). Domain reads as medical;
  `grep -i medical` = 0.
- **`superiortech_org`** (60 KB) — build-to-print machining; the corpus's largest
  equipments census (98 seeded). Stratified depth: in-house processes with named
  machines vs an outsourced list-only tier. `PET` occurs only inside
  *competitive* (9×), `ITAR` only inside *military* (9×), and "plastics" occurs
  once with zero specific polymers.
- **`mathewsco_com`** (62 KB, `attribution_negative`) — sales agency. **239 of
  261 entries are non-`own`.** Eight anonymous principals described in the first
  person with no company name to key on; line 602 puts the agency's name in
  subject position of a foundry capability.
- **`decimal_net`** (112 KB) — contract manufacturer. Spec-table **cell fusion**
  is its signature hazard: adjacent cells concatenate with no separator
  (`MillingMill-Turn`, `HipernikKovar`), so a designation loses its word
  boundary. Supplier-held NADCAP; ~580 lines of country/state dropdown.
- **`tanfel_com`** (182 KB, `attribution_negative`) — broker writing partner
  factories' capabilities in the first person. **Prediction refuted:**
  `equipments` was predicted empty — true only of machine BRANDS (zero across
  60+ makers); the site names ~20 production machines in a first-person fleet
  claim. Seeding it empty would have fired a false RED.
- **`sterlingmfg_net`** (211 KB) — the domain resolves to a **different company**
  (EPTAM Precision Solutions). Certifications bound to four legal registrants;
  ISO 13485 is the only one all four hold. 51% of the file is one repeated
  nav+footer block.
- **`howcogroup_com`** (223 KB, `attribution_negative`) — Scottish
  stockholder/distributor. Carries the British + oil-and-gas standards regime the
  corpus otherwise lacks (UKAS, PCN, CSWIP, ICorr, ISO 3834-2, API 5B/7-2/Q1,
  AMS 2750, "Made in Sheffield"). Facts stated at different geographic scope in
  identical voice; a phantom entity "Drummond" holds 3 certificates and appears
  nowhere else.
- **`blackadvtech_com`** (246 KB, `genre_probe`) — sheet-metal shop + ~22
  first-person-plural SEO blog articles. **Prediction refuted in detail:** the
  `Hemming` trap is real but the process word occurs ZERO times (brand hits are 3,
  not 51) — while `hems` IS a first-party capability. Second `ITAR`-inside-
  *military* case (45 hits, 0 word-bounded).
- **`lucasmilhaupt_com`** (259 KB, `genre_probe`) — brazing alloys; **the product
  IS the material**, so 13 objects are deliberately seeded in both `products` and
  `material_caps`. ~1,000 lines of second-person manual; a page arguing AGAINST
  welding. `gear` was credited by the "Lucas Gear" link in every footer.
- **`fzemanufacturing_com`** (259 KB, `genre_probe`) — capability pages plus a
  third-person process encyclopedia. Confirmed negation echo: "electropolishing
  is the reverse of electroplating". Laser gain media (Nd:YAG, Erbium) disputed —
  substances belonging to the machine, not to the work.
- **`pradeepmetals_com`** (280 KB, `genre_probe`) — BSE-listed Indian forging
  house. Widest non-US standards regime, and several designations are printed
  **only as spelled-out expansions** (`INDIAN BOILER`, `ENGINEERS INDIA LTD`,
  `CANADIAN REGISTRATION` — IBR/EIL/CRN occur zero times), with `INDIAN DEFENSE`
  in American spelling and `ISO 140001-2015` carrying a digit typo. Directorship
  register of ~25 unrelated companies; regulators phrased like standards.
- **`agstech_net`** (1.8 MB, `attribution_negative`) — 5,184 entries, larger than
  the other eleven combined. Headline trap is **TEXTBOOK-AS-CAPABILITY**: ~45
  pages are engineering textbook chapters with "we" injected. Certifications
  split cleanly — the Quality page says the plants hold them (`supplier`) while
  product marks (UL, CE, FCC) are first-person (`own`). Seeded by 8 page-aligned
  slice agents, verified by 10 location-clustered packet agents.

---

## 8. agstech: what ten independent verifiers agreed on

- **The tooling-as-products reading, 10 for 10.** The products clause excludes
  "the machinery, tooling, and facilities *the work is done with*" — scoped to
  the SUBJECT'S OWN production, so drill bits and saw blades AGS-TECH sells as
  merchandise are products. Every verifier reached this independently, and
  several tested it in both directions rather than asserting it: packet 2
  disputed tombstone fixtures quoted from "fixtures **we deploy throughout our
  job shops**"; packet 5 disputed wax patterns the text says are consumed in the
  subject's own process; packet 3 disputed blanking dies named as the tooling of
  a route being *avoided*. **Settled — do not re-open without new evidence.**
- **The cross-slice gap risk did NOT materialise.** Verifiers dropped candidate
  misses when the entity also occurred outside their range, assuming a sibling
  packet held it. All 36 entities named that way were audited against the merged
  inventory: **36/36 covered, 0 gaps.**
- **Where the slices were weak: prose, not lists.** 512 misses added, skewed to
  products and process_caps. The recurring shape is a material dropped from the
  very sentence the seed had already quoted for a sibling entity, and enumerated
  lists harvested part-way. Slice agents handle catalog structure well and
  running prose badly. **Tell the next slice fan-out this explicitly.**
- Confirm rate **94.0%** with 33 retires (31 of them duplicate twins) — the same
  shape as the single-reader subjects, which is the main evidence that slicing
  did not cost quality.

---

## 9. Repairs made to pre-existing state

- **Broken sample-text path.** The corpus texts moved to
  `tests/test_stages/sample_scraped_texts/` and the old
  `knowledge/sample_scraped_texts/` was deleted, but `checks/paths.py` and 58
  files under `search/` still pointed at it. All sha256s matched at the new
  location, so this was a pointer fix, not a re-verification. **9 files under
  `grounding/` and 1 under `synthesis/` still carry the stale path** — not mine
  to edit (§10).
- **Stale ledger table.** `VERIFICATION_LEDGER.md` recorded alecmfg as 132
  confirmed / 3 disputed; the files hold 126 / 9 since the six metrology entries
  were walked back on 2026-08-27. Corrected.
- **`role` fields.** Seed agents guessed roles from the old five-value
  vocabulary. All twelve round-2 subjects were re-set to the role that names why
  they are in the corpus, with a free-text `business_type` beside it.
- **Two quadratic performance bugs.** `build_expectations.py` and
  `validate_expectations.py` re-normalized the whole text once per quote; on
  agstech neither ever finished. Normalization is now hoisted — the full
  20-subject validation runs in ~100s.
- **24 malformed miss entries** from verification passes whose quotes were not
  verbatim: 11 repaired by re-deriving a quote from the text, 13 retired with a
  reason. `apply_verifications.py` now **rejects** such misses instead of adding
  them.

---

## 10. Cross-stage items (for other sessions)

**Owed TO the sibling instruments:**
1. `_shared/text_matching.py` gained a fourth trap (typographic hyphens) on
   2026-08-27. It is length-preserving, so no offset contract changed, and all
   75 stage tests pass — but if your instrument predates that date, re-read
   `_shared/README.md`.
2. **9 files under `grounding/` and 1 under `synthesis/` still reference the
   deleted `apps/data_etl_app/src/data_etl_app/knowledge/sample_scraped_texts`
   path.** One-line fix each:
   `apps/data_etl_app/tests/test_stages/sample_scraped_texts`. Left alone
   deliberately — those instruments are not mine to edit.

**Standing boundary:** this session owns `search/` only. Shared traps go into
`_shared/` (code, where every stage inherits the protection) rather than into
messages, because there are ~40 peer sessions and no way to tell which is which.

---

## 11. Open items — what a successor could do next

Ordered by value, not urgency. **None of these blocks a run.**

1. **Widen window coverage** (§6.5). Add every occurrence of an entity as
   additional evidence, so an entity named on five pages is scored in five
   windows. Do it for ALL subjects or none — a partial pass makes them
   incomparable. `apply_verifications.py` appends, so it can be incremental.
2. **Promote the 666 `candidate` entries.** These are verification-pass misses,
   each seen by exactly one reader. A third pass over them would move most to
   `confirmed` and is the cheapest remaining gain in gating coverage. 499 of them
   are agstech's.
3. **Adjudicate the 322 disputes.** They cluster where the schema predicts:
   memberships and registry identifiers under `conformity_attestations`,
   metrology and material-handling under `equipments`, design and management
   activity under `process_caps`. Several are genuine boundary questions the
   search prompts could settle by wording.
4. **Sweep the accidental shadowing** (§6.3). 1,229 flagged; most are legitimate
   hypernyms. The ones worth fixing are homographs and footer words — the
   four-character list in §6.2 is the highest-yield filter.
5. **Re-run the A/A probe** if the search prompts, model or chunking change; the
   2026-08-26 floor (window identity 28.4%, mean Jaccard 0.769) is what makes
   any per-window delta readable.
6. **Round 3 of the corpus** — the expansion doc measured returns flattening
   (round 2's best pick adds +14 concepts vs round 1's +48). The binding cost is
   the census, and this session's tooling roughly halves it. Re-decide with that
   in mind, and add a blog-share pre-filter if the deep-concept slot is used.

---

## 12. Change log — append here

Each entry: date, who, what changed, and anything the next reader must know.
**Do not rewrite earlier entries.** Correct §1's tables in place instead, and
note the correction here.

### 2026-08-27 — corpus expansion round 2 (this session)
Seeded and two-pass verified all 12 new corpus subjects; 522 entries / 8
subjects → 9,499 / 20. Built the five-script seeding pipeline and the two
briefs. Fixed the shared matcher's fourth trap, two quadratic performance bugs,
the broken sample-text path, and the stale alecmfg ledger row. Measured
shadowing (12.9%) and the four-character-form hazard for the first time. ~40
agents; every subject validates with 0 errors; 75 tests pass.

Known-good state to return to if something later breaks: every number in §1 is
re-derivable from `checks/validate_expectations.py` alone.
