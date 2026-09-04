# Findings — full census of run 20260903T212112

First run with `search_union_pass=True` on the v3 100k-cutoff markdown
corpus. 260 judge tasks / 108 units / 25,440 forms / 18 subjects; every
form coded, double-judged on 36 packets (2,909 shared forms), slice-verified
by hand (305 rows + outlier disagreements; 1 correction).

**Read every number beside its floor.**
A/A reproducibility (100-window fresh-pair sample): identical sets 73%,
per-form Jaccard 0.897, 19% deterministically empty (`AA_PROBE.md`).
Double-judge floor: raw code 85.3%, rollup 89.7%, actor 91.7%
(`raw/double_judge_agreement.json`). Mechanical eval at census time:
**62 OK / 46 RED** on the recall-only gates (the REDs are the
confirmed-recall misses this census judged; packet coverage was the clean
number — 260/260 packets, 108/108 units, 0 problems. An earlier revision
misquoted this as "RED=0/OK=108"; corrected 2026-09-04).
A judged difference smaller than these disagreement rates is not a result.

## Headline (pooled rollup, judged forms)

| field | judged | in-field | adjacent | generic | junk | misses* | rollup floor |
|---|---|---|---|---|---|---|---|
| conformity | 1,000 | 77.6% | 6.4% | 11.6% | 4.4% | 137 | 89.0% |
| equipments | 2,084 | 65.5% | 33.0% | 1.0% | 0.4% | 571 | 94.6% |
| industries | 2,941 | 62.6% | 20.3% | 12.8% | 4.3% | 152 | 92.0% |
| material_caps | 2,796 | 67.6% | 24.7% | 5.2% | 2.5% | 112 | 84.6% |
| process_caps | 6,318 | 67.9% | 17.7% | 12.8% | 1.7% | 469 | 90.6% |
| products | 10,301 | 32.4% | 53.9% | 12.9% | 0.7% | 832 | 85.0% |
| **ALL** | **25,440** | **53.1%** | **34.2%** | **11.0%** | **1.7%** | **2,273** | **89.7%** |

*misses = surviving after cross-part reconciliation (+1,007 phantoms
cancelled; see F9). Miss totals are order-of-magnitude only (F10).

## F1. Intra-run inconsistency is still the top recall defect (mechanical)

Of the windows whose text contains an in-field form the run returned
somewhere, only **53.0%** actually returned it (09-01: 54.8% — same shape,
no improvement from the union pass). Fully-consistent in-field forms: 25.1%.
The worst tail is nav/footer recurrence (pradeepmetals nav-menu items missed
in 9 of 10 windows; ableengineering footer ICA link 4 of 7) — sitewide
boilerplate the model returns once and then drops, which both inflates this
metric's tail and genuinely loses heading-anchored entities.

## F2. Per-FIELD stance splits recall on identical text (the SHARP window)

acimachine window 8520:19598, identical text, both passes, finish=stop:
**equipments returned 22 forms (no model numbers) and missed 114 SHARP
facet-line machines; products returned ~150 incl. must-finds STA-38 and
SVL-2416SE-F.** Same shape on heading-line entities (CHEVALIER FSG-3A1224,
WINEMA RV 10, HURCO VMX42I: products returned all three, equipments none).
Not scrape, not propagation, not truncation — the equipments prompt declines
dealer facet/heading lists that the products prompt embraces, although
merchandise machines are ruled in-field by nature. This is the main driver of
equipments' lowest mechanical recall (92.6%).

**REVISED 2026-09-04 (user re-ruling of boundary ruling 4): purely-sold
machines are PRODUCTS, not equipments** — equipments keeps only machines in
described production use. Under the revised boundary this finding INVERTS:
equipments declining the dealer facet list is CORRECT behavior, products
capturing it is the intended division of labor, and the 114 SHARP "misses"
stop being a recall defect. 127 reseller_inventory equipments entries
(acimachine 95, agstech 32) were retired from the eval set; the merchandise
lives in products (actor reseller_inventory). Equipments' recall story after
the re-ruling is dominated by the metrology-inclusion publish instead (15
harvested metrology candidates now awaiting verification).

## F3. The union pass adds almost nothing (probe-level; formal diff owed)

Every probed window returned near-identical pass1/pass2 sets (SHARP window:
151 vs 150 phrases, same members; agstech dense window: 239 vs 243;
acimachine 0:8520: 48 vs 44, heavy overlap). Its one measurable side effect
so far is duplicate stacking (F5). A formal run-wide per-pass unique-yield
diff (`>pass>2>` custom_ids) is owed before declaring it dead.

## F4. Ultra-dense windows hit a yield ceiling (agstech 69099:95453)

A ~26k-char sub-window (of chunk 0:95453). Search returned 239/243 phrases
with finish=stop — no truncation, no repetition loop, text present in the
prompt (grep-verified) — yet BOTH fields independently skipped the ~54-item
Private & White Label catalog list and ~42 pneumatics items (products ~126
misses, equipments 71, cross-confirmed). The model saturates before
exhausting a window this dense. Direct evidence for the unrun
`search_divisor` A/B (smaller sub-windows on dense pages).

## F5. Duplicate returned forms — no exact-string dedupe per window

1,113 judged rows (4.4%) are exact (window, form) duplicates; products holds
671. Both sources confirmed: within-response repetition (`automation lines`
6x in ONE response) and cross-pass union stacking (union 5x/8x for probed
strings; something later partially dedupes to the 3x/6x the packets held).
Cheap fix: exact-string per-window dedupe at union/collect time.

## F6. Unverbatim returns exist but are rare (6 in ~28.7k)

Two mechanisms census-confirmed: prompt-vocabulary echo (alecmfg
process_caps returned `designing parts` — verbatim from the engineering
sentence in `process_cap_phrase_search.txt`; a census-wide scan found NO
other echo of that sentence) and substitution (steelcraft "Tornado Series"
for the text's "Hurricane Series"); plus 4 window-attribution leaks (e.g.
`ISO 14001` credited to a window with no "14001"). The mechanical eval
cannot see this class. Watch-class; no prompt rewording justified on 1 echo.

## F7. Wrong-actor share is mostly structure, not defect

20.2% of judged forms carry a non-own actor — but 50.8% on industries is the
field's nature (served industries ARE client sectors; recall-first says
return + flag), and equipments' 29.7% is dealer/reseller inventory
(acimachine JET/SHARP catalogs) plus client gear. The flags were highly
stable across judges (actor floor 91.7%; the one bad axis was judge2
misreading "industries served" lists as own — judge1/merged data has the
doctrinal read). Risk concentrates where downstream consumers ignore the
flag, not in the search stage itself.

## F8. Junk is nearly eliminated (1.7%)

Largest single junk pool: the taylordunn dealer directory ("___ Material
Handling" company names swept into material_caps because the string contains
"material") — the run's original user question, now formally judged: all
dealer-name forms U/partner across two independent judges. Fix direction is
the field prompt's judge-by-use doctrine vs directory listings, not scraping.

## F9. Split-window phantom misses: 31% of all claimed misses

1,007 of 3,280 miss claims named entities covered by a sibling slice of the
same window (acimachine products part03 alone declared 137 misses whose
covering forms lived in part02). `checks/reconcile_split_misses.py` now
cancels these mechanically (annotates, never deletes) and is a permanent
census step. Any split-packet miss count quoted without it overstates ~1.4x.

## F10. Miss counts are order-of-magnitude signals

An accidental identical-prompt double-judge (blackadvtech products part01)
produced 75/75 code agreement but 14 vs 0 misses. Miss-hunting depth
dominates judge variance (the formal double-judge shows the same: codes
stable, miss depth the loose axis). Never gate on per-unit miss totals.

## Merge-time filter classes applied/annotated

Bio/job-ad evidence (employee certifications, custodian job postings — one
form corrected G→U in slice verification), governance documents
(transparency-act/CSR footers), CAD-software out of equipments, negated
sector mentions ("We DO NOT machine automobile parts"), inferred sectors
(Agriculture from "Agricultural Robots"), nav/footer recurrence, O+G pooled
as the robust "not-a-sector" share (absorbs the measured O<->G judge axis).

## Eval-validity flags (for the eval-doubt pass, user-vetoable)

- blackadvtech equipments: must-finds "Cincinnati CL-707" and "powered
  shears" absent from the entire packet text (reserved test at evolution).
- howcogroup equipments: 6/19 must-finds absent from every window; wire-text
  error-page contamination measured tiny (3 occurrences, ~30 tokens) — the
  scraper-backlog item stands but does not explain these absences.
- alecmfg products: 3 must-finds reachable only via Prev/Next nav links.
- acimachine conformity CSA/CUS dispute: must-find absent from all packet
  windows (standing reserved test).
- 16 units are verdict-OK with zero confirmed entries to gate on (census
  in-field rates beside each in `CENSUS_REPORT.txt` §E) — seeding targets.
