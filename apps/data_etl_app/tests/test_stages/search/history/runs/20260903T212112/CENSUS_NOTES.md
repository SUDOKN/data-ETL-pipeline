# Census notes — run 20260903T212112 (full census, user-selected depth)

Machinery reused from the 2026-09-01 census (export → split at 150 forms →
one agent per packet → coverage-verify → assemble parts → double-judge →
merge). Canonical judge prompt frozen at `raw/judge_prompt_template.txt`;
in-flight ledger at `raw/census_ledger.json`. 260 tasks / 108 units /
25,440 forms. Products serves contract_products (never judged twice).

## Judge-model tiering (user instruction: smaller models for smaller judgements)

- First rule tried: haiku for packets ≤40 forms, sonnet above.
- **Haiku failed verification on its first form-bearing packet**
  (101machine equipments, 6 forms): coded "CNC machining" B where the
  packet's table has an explicit `P process name` code and the 09-01
  sonnet census coded that exact string P every time (5+ precedents
  checked). 4/6 forms diverged. The failure is code-table discrimination,
  not packet size.
- **Standing rule: sonnet for every packet with forms; haiku only for
  zero-form packets** (pure miss-scans; exactly 1 of 260 tasks). All four
  form-bearing packets haiku touched (101machine equipments/industries/
  material_caps, ableengineering equipments) were re-judged by sonnet with
  fresh files; no haiku coding survives into the merge.

## Observations logged while judging (for FINDINGS triage)

- 101machine industries (sonnet re-judge): the 7 sector forms coded
  V/**client**/prose — "industries we serve" mentions; the haiku pass had
  read actor differently. Actor-flag reads on served-industries prose are
  a known judge-variance surface; watch it in the double-judge agreement.
- Same packet, new miss class: **negated sector mention** — "We DO NOT
  machine automobile parts!" names `automobile`; no returned form covers
  it. Recall-first doctrine says named ⇒ in-field (the flag, not a lower
  code, carries the nuance), but negation is a class the code tables never
  anticipated. Bucket these misses separately at merge; possible
  EXPECTATIONS_SCHEMA question for the user.
- 101machine equipments (both judges): photo-caption entities
  (`depth micrometer`, `face cutter` — "Photo of depth micrometer") missed
  by search. Alt-text/caption lines are a scrape-time artifact class; count
  their share before proposing anything.
- **Part-name flood into material_caps (ableengineering): 49/66 forms coded
  N (Bearing, Bolt, Bushing, Fuel Nozzle Shroud…).** The 09-01 census on the
  same subject/field had N=14/42 (33%) — now 74%. Corpus (v2→v3, heading
  propagation over the landing-gear part lists) and the union pass both
  changed, so attribution needs the per-pass dump split (`>pass>2>` refs)
  before blaming the second pass. Top precision finding so far.
- **Top recall finding — 114 SHARP model numbers missed in one window**
  (acimachine equipments, window 8520:19598): they surface only as
  filter-facet line items and search returned none of them; must-finds
  SHARP STA-38 and SVL-2416SE-F are among the missed. Same window family
  where the JET grid (window 30539:40405) was returned 378-strong.
  **REVISED by products part02: the PRODUCTS search on the very same window
  8520:19598 returned 150 SHARP facet forms (models included).** So the
  facet lines are returnable and propagation likely fine — the gap is
  per-FIELD behavior on identical text: equipments declines the dealer
  facet list that products embraces. FINDINGS task: diff the two fields'
  responses on this window (and check the union pass on each); this is a
  prompt-stance question (merchandise machines are ruled IN by nature),
  not a scrape question. Explains equipments' lowest mechanical recall (92.6).
- **Heading-line entity misses confirmed as a class** (same packet):
  CHEVALIER FSG-3A1224, WINEMA RV 10 FLEXMASTER, HURCO VMX42I are prominent
  headings in window 0:8520 and returned by nothing — the exact HURCO
  watch-class flagged during the smoke-run analysis, now census-confirmed.
  **Products part01 returned all three on the same window** — the
  heading-line miss is also field-stance, not text shape; same FINDINGS
  diff as the facet-list case.
- Footer-recurrence miss class (ableengineering conformity): footer links
  ("California Transparency in Supply Chains Act", ICA document title)
  recur in ~every window but were captured in only one each — the 09-01 F1
  intra-run inconsistency shape, now concentrated in footers. 21 misses on
  the unit are dominated by it; bucket separately at merge.
- **Largest phantom-miss injection to date — MUST cancel at merge**:
  acimachine products part03 held only 1 form for window 8520:19598 and
  declared 124 SHARP SKU + 13 facet misses there; part02 (same window,
  same field) holds the 150 forms that cover almost all of them. The
  09-01 checklist item 7 hazard realized at scale; the mechanical
  cross-part miss reconciliation cancels these before any miss count is
  quoted. (The equipments-vs-products SHARP contrast SURVIVES this:
  equipments' whole form set for that window lives in its part01 — its
  114 SHARP misses are real.)
- **Second big recall window — agstech 69099:95453, missed by BOTH fields**:
  products part06 reports ~126 misses (the whole ~54-item Private & White
  Label catalog bullet list + ~42 pneumatics/hydraulics component items);
  equipments part02 independently flagged the same lists (71 misses).
  Unlike the acimachine SHARP case (field-stance: products caught what
  equipments missed), here BOTH fields skip the lists — so text shape or
  sub-window cuts are back on the table FOR THIS WINDOW. FINDINGS: check
  propagation and the 5k cut positions on these two pages specifically.
- **Sibling-list partial capture** (ableengineering products, window
  53116:56070): a 5-item service card list where only "Individual Component
  Repair" was returned and its 4 siblings (Repair and Overhaul, Exchange,
  Outright Sale, Lease) were missed — within-one-list inconsistency, a
  sharper shape than cross-window F1. Footer ICA link also inconsistently
  captured (4 of 7 windows) on the same unit, reinforcing the footer class.
- Bio-evidence misses to filter at merge (same unit): 4 employee-training
  certifications (Dale Carnegie leadership etc.) claimed as misses from a
  bio page — the F5/employment-history class the 2026-09-03 boundary
  rulings put OUT of scope; do not count them as pipeline misses.
- **Unverbatim returns — a form with no textual basis in its window**:
  six census-confirmed cases so far (case 5: decimal material_caps part01
  'tool steel' — nearest window text is "Tool & die repair"; case 6:
  steelcraft products part07 "Tornado Series tornado doors" where the text
  reads "Hurricane Series tornado doors" — a SUBSTITUTION variant, the
  model swapping one series name for another). (1) agstech
  process_caps part07 form
  'assembling them' not locatable verbatim in its window; (2) alecmfg
  conformity: `ISO 14001` returned for window 159552:172989 whose text
  contains no "14001" anywhere (judge grep-verified); (3) alecmfg
  industries part02: "structural components for rolling stock" absent from
  its window's text; (4) alecmfg process_caps part02 form 'designing parts'
  ungrounded — and **'designing parts' is verbatim prompt vocabulary** from
  the 2026-09-03 engineering-inclusion sentence in
  process_cap_phrase_search.txt (grep-confirmed). Case 4 is a
  PROMPT-VOCABULARY ECHO, not window leakage: the model emitted the
  prompt's own example phrase as a found form. At merge, scan process_caps
  forms for the sentence's other multi-word examples; if the echo recurs,
  the new sentence needs its examples reworded (prompts-are-generic lesson,
  now measurable). Either
  window-attribution leakage (found elsewhere, credited here) or
  hallucination. The mechanical eval cannot see this class; count it at
  merge and, if it recurs, make it a FINDINGS defect with the dump's
  per-window responses as evidence.
- **Inferred-sector misses need their own bucket** (agstech industries
  part05 and part01): judges claim misses like Agriculture from "Customized
  Agricultural Robots" or Telecom from a client name "TNC Telecom" — the
  entity is inferred from a product example or company name, not named as a
  sector. Softer class than a verbatim sector word; count separately or the
  industries miss rate mixes two doctrines.
- Judges are self-calibrating conventions across sibling files (part05 read
  parts 01/02/04 to align G-vs-U) — good for intra-field consistency;
  double-judge still measures the residual variance.
- **Measured miss-hunting variance from an accidental double-judge**
  (blackadvtech products part01, judged twice by identical prompt/model
  due to the pause-and-respawn overlap): judge A found 14 misses (welding
  process types, materials, ISO pair, QC instruments), judge B found 0 on
  the same packet. Form coding agreed (75/75 both). Miss counts carry FAR
  more reader variance than form codes — treat per-unit miss totals as
  order-of-magnitude signals, and lean on the formal double-judge sample
  before quoting any miss rate. (File on disk holds judge B's version.)
- acimachine conformity: judge confirmed the standing CSA/CUS dispute from
  the eval-vote backlog — the must-find never occurs in any window text of
  the packet (grep-verified). Not a judge miss: per the 2026-09-01 reserved
  test this is beyond-read-coverage unless the quote is absent from the FULL
  site text too; check at eval-evolution time. Judge also read conformity
  narrowly (taper/interface designations R8, CAT/BT40, 5C excluded as
  out-of-field) — good double-judge candidate field for this subject.

## The taylordunn "material handling" question — census verdict

The user's original question (why "material handling" repeats in the
taylordunn material_caps dump) is now formally judged: the repeats are
**"___ Material Handling" dealer/company names** from four dealer-directory
windows (37787:53135 … 83290:94169), plus one bare "MATERIAL HANDLING"
sector fragment — all coded U (junk, not materials), actor=partner for the
named dealers. Search sweeps the dealer directory's company names into
material_caps because the string contains "material". Fix direction is
precision (the field prompt's judge-by-use doctrine vs. directory listings),
not scraping.

## Run interruptions

- 4 API-timeout agent deaths (agstech processes p06/p10, products p02,
  alecmfg-era processes p03) — all clean (no partial file), all retried
  successfully. Same class as the 09-01 judge2 timeouts.
- **2026-09-03: account monthly spend limit hit mid-census** (HTTP 429,
  "session limit resets 4:40pm America/Phoenix"). 4 blackadvtech agents
  killed cleanly (products p02/p08, material_caps, industries p01 — no
  partial files; ledger status `rate-limited-retry`); remaining in-flight
  agents expected to die on their next API call. Census pauses at 88/260
  tasks done; resume = respawn `rate-limited-retry` + remaining statuses
  after the reset. Same limit that halted the synthesis eval at 546/2,133.

- Exact-duplicate forms inside one window's packet list (steelcraft
  industries: `stainless steel door and frame manufacturer` ×2,
  `hollow metal` ×2, `manufacturing` ×2): check whether the union pass or
  the packet export fails to dedupe exact strings per window — small
  FINDINGS item, affects per-window form counts.

## Eval-set validity flags raised by judges (for the eval-doubt pass)

- blackadvtech equipments: must-finds "Cincinnati CL-707" and "powered
  shears" absent from the ENTIRE packet's window text (full-file grep; the
  only "shears" is gardening shears in a custodian job posting). Reserved
  test applies: check the full v3 site text at eval-evolution time; if
  absent there too, these are port errors, not recall misses.
- alecmfg products part06: 3 must-finds reachable only via Prev/Next nav
  links (nav-evidence seeding).
- howcogroup equipments: 6 of 19 must-finds absent from every window text,
  and the judge saw "Error establishing a database connection" pages and
  collapsed duplicates in the raw windows — the error-page scraper
  watch-class contaminating this subject's snapshot. Reserved-test these 6
  at eval-evolution time and count error-page tokens for the scraper-work
  backlog.

## Step-5 slice verification (personal, 2026-09-04)

305 sampled rows (2 random + 1 U/G row per unit, seed 20260904) plus the
code-disagreement rows of the 3 lowest-agreement double-judge pairs, each
read against its packet window text. Result: **1 correction**
(blackadvtech products `shrubs` G→U — custodian job-posting gardening duty).
Every other checked row was correct or sat on a measured variance axis with
judge1's read defensible. The 3 outlier pairs each decompose into ONE
systematic axis, and in all three judge1 (the merged data) has the better
read: acimachine material_caps 55.6% = 9 spindle-taper facet values
(R-8/NMTB#40/…) j2 called N/part vs j1's U/junk (a taper is an interface
spec, not a component); alecmfg industries 69.7% = O-vs-G on bare
manufacturing/prototyping words (both not-a-sector; O+G bucket absorbs);
anchor-mfg industries 37% actor = j2 read "Industries served include…" lists
as actor=own where j1's client matches doctrine and every precedent. Zero
unverbatim forms in the 305-sample (consistent with 6 known cases in ~28.7k).

## Double-judge agreement floor (36 pairs, 2,909 shared forms)

Raw code 85.3% · rollup 89.7% · actor 91.7% · evidence-kind 88.7%
(`raw/double_judge_agreement.json`). Per-field rollup floor: conformity
89.0, equipments 94.6, industries 92.0, material_caps 84.6, process_caps
90.6, products 85.0. Dominant axes: B<->V everywhere (family vs specific),
G<->O industries, G<->S/G<->V products, N<->U materials (the taper family),
client<->own actor on served-industries prose (95 cases). Judge2 rows with
note "MISS:*" are miss annotations in form schema (their prompt had no miss
shape) — excluded from agreement by the merger.

## A/A floor to print beside every judged number

73/100 fresh pairs byte-identical (old 28.4%); per-form Jaccard 0.897
non-empty (old 0.769); 19/100 windows deterministically empty. Fresh vs
stored 80/100. See `AA_PROBE.md`. Mechanical eval: RED=0 / OK=108.
