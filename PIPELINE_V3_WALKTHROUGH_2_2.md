# Pipeline v3 — walkthrough of 2.2 (floor-scan engine) and the 2.3 fold design

Written 2026-08-21 as a resume aid: every illustration below is **actual output**
of the shipped modules (`core/utils/floor_scan.py`, `core/utils/form_normalizer.py`)
or of the fold prototype (`pipeline_v3_evidence/fold_prototype.py`), run on the
same toy window. The plan of record is [PIPELINE_V3_PLAN.md](PIPELINE_V3_PLAN.md)
(STATE → ledger D1–D20 → next substep → last journal row); this file explains the
2.2 mechanics and records the 2.3 design exactly as shown to the user.

Re-run: `cd packages/core && ../../.venv/bin/python ../../pipeline_v3_evidence/fold_prototype.py`

---

## 1. The word-boundary matcher for technical terms (D7, D8's engine)

`find_form_occurrences(text, form, case_sensitive)`: `re.escape(form)` with a guard
on an edge **only when that edge of the form is a word character** (`\w`,
Unicode-aware). `(?<!\w)` left = no word char immediately before; `(?!\w)` right =
no word char immediately after.

```
form '6061-T6'   pattern=(?<!\w)6061\-T6(?!\w)      hits=1
   6061-T6 aluminum, not 16061-T6 or 6061-T651.
   ^^^^^^^                                         ← not 16061-T6 ('1' precedes), not 6061-T651 ('5' follows)
form 'CNC'       pattern=(?<!\w)CNC(?!\w)            hits=2
   CNC/Manual Machining (CNC) and CNCs
   ^^^                   ^^^                         ← '/' and ')' are not word chars; CNCs is another token
form 'C++'       pattern=(?<!\w)C\+\+                hits=2
   C++ tooling; C+++; C+
   ^^^          ^^^                                  ← no right guard: the form ENDS in '+'
form 'Lead'      pattern=(?<!\w)Lead(?!\w)           exact hits=2   (case-insensitive: 4)
   Lead Time. Lead-free. Leader. leading. lead. LEAD.
   ^^^^       ^^^^                                   ← Leader/leading never; lead/LEAD are tier 2's
form 'façade'    hits=1 in "façade façades; facade"  ← 'ç' is a word char; façades guarded out
form 'SS'        hits=2 in "304SS stainless, SS 304, SS."  ← 304SS: '4' before it is a word char
```

Why not `\b`: it fails on `C++` (no boundary between `+` and space) and on forms
starting with `(`; guarding both sides unconditionally would wrongly reject them.

## 2. Scan domain: page headers blanked, length-preserving

The scraper renders a page as a 50-`#` separator line, the bare URL line, a blank,
then content. `mask_page_headers` replaces ONLY those header lines with spaces
(`·` below), keeping newlines — same length, so **offset 88 is the same character
in both**. A form occurring only in a URL line gets zero hits (`acme`); a
path-like string in BODY text still counts (`/materials/lead`, line 7).

```
len(window)= 266  len(masked)= 266
 0 |##################################################| -> |··················································|
 1 |https://acme.example/materials/lead               | -> |···································               |
 3 |Sample Lead Time: 2 weeks. Lead-free solder, ...  | -> |Sample·Lead·Time:·2·weeks.·Lead-free·solder,·... |
 4 |##################################################| -> |··················································|
 5 |https://acme.example/about                        | -> |··························                        |
 7 |Lead paint removed. See /materials/lead.          | -> |Lead·paint·removed.·See·/materials/lead.          |
offset of 'Sample Lead' in window=88, in masked=88
```

## 3. Page geometry (the code-derived page that replaced the LLM's `page` field)

`page_spans` tiles the window; a page starts at its URL line. Text before the first
URL line is `None` unless the caller passes the inherited `preceding_page` (a search
sub-window cut mid-page — bounds respect lines, not pages).

```
PageSpan(url=None,                                  start=0,   end=51)   ← leading separator
PageSpan(url='https://acme.example/materials/lead', start=51,  end=197)
PageSpan(url='https://acme.example/about',          start=197, end=266)
page_at( 88) 'Sample Lead' -> materials/lead ; page_at(225) 'Lead paint' -> about ; page_at(0) -> None
mid-page start, no preceding_page   -> [(None,0,67), ('…/next',67,100)]
mid-page start, preceding_page=prev -> [('…/prev',0,67), ('…/next',67,100)]
```

## 4. Two tiers + the short-form policy (settled: `SHORT_FORM_MAX_LENGTH = 3`)

```
floor_scan(window, ["Lead","lead","Al","SS","acme","/materials/lead"]):
'Lead'            tier1=['Lead','Lead','Lead'] pages=[lead,lead,about]  tier2=['Lead','Lead','lead','Lead','lead']
'lead'            tier1=['lead','lead']        pages=[lead,about]       tier2=(same 5)
'Al'  'SS'        tier1=[] tier2=[]     ← short: tier 2 stays exact; flagged in short_forms=['Al','SS']
'acme'            tier1=[] tier2=[]     ← URL-only: not text
'/materials/lead' tier1=['/materials/lead']                              ← body occurrence counts
```

Tier 1 (exact) = the HOLD: the collector's mentions must account for every hit or
the window flags a discrepancy (zero GT cost — the satisficing tripwire). Tier 2
(case-insensitive) = DISCOVERY: casings search never emitted → missed-form
surface. Tier 2 ⊇ tier 1 always.

## 5. Hypothesis properties (what a regression would violate)

Every hit is a whole-word exact substring with the boundary invariant; tier 2 ⊇
tier 1, casefold-equal, sorted, non-overlapping, == tier 1 for short forms; a form
surrounded by spaces is always found; masking is length-preserving and pages tile
the window. (In 2.1 the same machinery caught two real Unicode edges — dotless `ı`,
letters with no uppercase — properties of Unicode; the test alphabet was tightened.)

## 6. The 2.3 fold, as prototyped (APPROVED and BUILT 2026-08-21 — `core/utils/aggregation_fold.py`)

Toy window: two pages; sent forms `Aluminum, aluminum, Brass, Lead, Lead Time,
Sample Lead Time, die-casting`; a deliberately imperfect collector answer (sloppy
casing, one sentence filed under two forms, a sentence holding several forms,
`Lead` reported for "Sample Lead Time: 2 weeks.", three forms lazily empty).

**A+B — re-attribution + longest-match containment (D8).** Locate each snippet in
the window; find every sent form occurring exactly in it; the longest occurrence
owns each spot.
```
snippet@ 83 'We stock Aluminum and Brass. Sample Lead Time: 2 weeks.'
    kept: [(92,100,'Aluminum'), (105,110,'Brass'), (112,128,'Sample Lead Time')]   <- LLM said 'aluminum'; re-keyed
snippet@112 'Sample Lead Time: 2 weeks.'
    kept: [(112,128,'Sample Lead Time')]                                            <- LLM said 'Lead'; re-keyed
```
Visible: casing mis-attribution repaired with no reconciler; `Lead`/`Lead Time`
inside "Sample Lead Time" are NOT separate occurrences (D14's polyseme dissolves
structurally whenever search emitted the fuller span — the 1.1 fullest-span
decision paying off); a sentence holding Aluminum AND Brass becomes a mention of
BOTH groups even though the LLM filed it once (fold authoritative, LLM advisory).

**C — D19 residual rule (proposed): dedup key = (group, OCCURRENCE span), longer
snippet kept.**
```
dup ('sample lead time',112,128): kept 'We stock … Sample Lead Time: 2 weeks.' dropped 'Sample Lead Time: 2 weeks.'
14 raw (form, occurrence) rows -> 7 after dedup
```
Keying on the occurrence (not the snippet) collapses two different-extent snippets
of one spot while keeping two genuine occurrences in one sentence as two mentions.

**D — bundles: group by `normalize()` key (2.1 dict, global scope), LOCKED order
(window position == page order then position, so the synthesis input and its
`|ud=` digest are independent of LLM answer order/batching), code-derived page,
`group_id` as the synthesis `record_id`. Empty bundles KEPT, marked, skipped by
synthesis, dump-visible.**
```
record_id=gknmr82d key='aluminum'  forms=['Aluminum','aluminum']   @92 materials, @139 materials, @269 about
record_id=gshl3r6g key='brass'                                      @105 materials, @280 about
record_id=gpk94tty key='lead'      forms=['Lead']                   @246 'Lead-free solder only.'  ← lead-time sense never lands here
record_id=g72u3hid key='lead time'  <- EMPTY BUNDLE (swallowed by containment)
record_id=gt4q9qi7 key='sample lead time'                          @112
record_id=gqc7akxk key='die casting' <- EMPTY BUNDLE (never anchored)
```

**E — finding for the 2.3 build: the tier-1 HOLD must apply the SAME containment
rule to the scan's hits**, or fuller-span forms generate phantom discrepancies for
their sub-forms:
```
'Lead'       hits=2 obligations(after containment)=1 unaccounted=[]   ← the hit inside "Sample Lead Time" is not Lead's
'Lead Time'  hits=1 obligations(after containment)=0 unaccounted=[]
```

Grouping illustration (2.1): `Aluminum/aluminum/ALUMINUM` → one key; `aluminium`
separate; `6061-T6 aluminum` → `6061 t6 aluminum`; `metal stampings/Metal Stamping`
→ `metal stamping` (guarded fallback); with `verb_fold=True` (process field)
`CNC milled/CNC milling` → `cnc mill`, `Polished/Polishing` → `polish`.

**Built 2026-08-21.** The user approved A–E; `core/utils/aggregation_fold.py`
reproduces every block above (it is the module's golden test in
`tests/test_utils/test_aggregation_fold.py`). Three refinements over the
prototype, found while building: (i) attribution reads the tier-1 SCAN — the
owners (hits after containment) inside a located snippet's span — rather than
re-matching forms inside the snippet string, so the word-boundary guard stays
honest at snippet edges (a snippet cut at "un|lead alloys" does not pass) and B
and E are one computation; (ii) containment applies WINDOW-wide across all
snippets, not per snippet — a fragment snippet "Lead Time" cannot claim a spot
inside "Sample Lead Time" wherever that was reported; (iii) D19's tie among
equal-length snippets prefers the collector's own filing, then lexical order,
so `reported_form` on a kept mention is truthful and never depends on answer
order. Also surfaced: `unlocated` (snippet not verbatim) and `unanchored`
(verbatim, but no owner inside — anaphora or a fragment) reports per window,
and the raw `candidates` count; empty bundles carry status `no_mentions` and
`FoldResult.synthesis_records()` skips them.

## Evidence files
- `pipeline_v3_evidence/normalize_dry_run.py` — appendix E tool (runs the production normalizer over the dump corpus per layer).
- `pipeline_v3_evidence/2026-08-21_normalize_dry_run_output.txt` — the 2026-08-21 run (4,157 pairs; 162→189→191→198 groups).
- `pipeline_v3_evidence/fold_prototype.py` — the §6 prototype (seed of `core/utils/aggregation_fold.py`, which reproduces its output as the golden test; the module differs in the three refinements above).
