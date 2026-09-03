# Never-found miss analysis — run 20260901T013332 (2026-09-02)

Purpose: the per-field pattern dive the user requested for search-prompt
optimization, computed under the user's miss bar of 2026-09-02: **a
search-stage failure is an entity whose evidence sits inside the run's read
windows and that the LLM search returned in NO sub-window of the run.**
Entities found in some other window are separated out, because mention
collection pools forms chunk-wide (`get_chunk_forms` → `collect_sub_window`)
and mechanically collects a form's mentions in every sub-window of the chunk
once ANY sub-window returned it.

Machinery: `never_found_recompute.py`, `pile_structure.py`, `classify_prep.py`
(this session's scratchpad; outputs `never_found_pile.json`,
`healed_in_chunk.json`, `cross_chunk_gap.json`, `review_<field>.jsonl`).
Inputs: `judgments/*.jsonl` (primary judge, miss records only) +
`raw/search_requests.json` (window texts + parsed responses). Crediting used
the harness's `form_covers`; occurrence scanning used `occurs_in` (whole word,
case-insensitive except short forms — no header masking, so occurrence counts
are approximate).

## Denominator

1,530 miss records dedup to **1,419 distinct missed entities**; of these:

| field | distinct | never found in run | healed in chunk | cross-chunk gap | single-occ | multi-occ |
|---|---|---|---|---|---|---|
| conformity_attestations | 86 | 65 | 18 | 3 | 56 | 9 |
| equipments | 864 | 798 | 49 | 17 | 753 | 45 |
| industries | 80 | 49 | 19 | 12 | 42 | 7 |
| material_caps | 93 | 61 | 25 | 7 | 53 | 8 |
| process_caps | 206 | 129 | 59 | 18 | 109 | 20 |
| products | 90 | 60 | 26 | 4 | 54 | 6 |
| **total** | **1,419** | **1,162** | **196** | **61** | **1,067** | **95** |

- **196 entities (14%) are already healed** by chunk-wide mention pooling —
  not search failures under the bar.
- **61 entities sit in the cross-chunk gap**: found in one 20k chunk, present
  but unfound in the other, and pooling is per-chunk
  (`get_chunk_forms` unions only the chunk's own sub-windows) — the measured
  business case for subject-wide form pooling (a dispatch change).
- Single-occurrence entities dominate (1,067/1,162) — but this is NOT mostly
  stochastic flicker: **88% of the equipments pile and 22–34% of
  conformity/process sit in massed windows** (≥8 losses in one window), i.e.
  whole regions yielded nothing while individual coin flips would scatter.
  Massed windows: acimachine 66765:78696 (335 losses), 47521:60642 (112),
  decimal 56613:78696 (23), superiortech 0:19121 (22), pradeepmetals
  132048:151338 (17), anchor-mfg 0:24859 (11+10), mathewsco 0:21715 (11), etc.
- 6 windows died to degeneration/truncation (finish_reason=length +
  unparseable): agstech products x2, alecmfg industries, blackadvtech
  products, fzemanufacturing products, pradeepmetals products. Their misses
  are dispatch artifacts (salvage/re-ask), not prompt behavior.

## The occ0 discovery — 542 entities have no returnable surface form

**536 equipments entities (467 of them acimachine) have NO contiguous
occurrence of their recorded name anywhere in the window text.** The dealer
catalog prints bare model codes under a brand heading:

    entity: WELLSAW 58BD   → the text prints only "58BD"
    entity: UNI-HYDRO PRO 125 → the text prints only "PRO 125"
    entity: WELLSAW 600    → the text prints only "600"

The judges (correctly) recorded brand+model as the entity, but the
surface-form contract cannot return "WELLSAW 58BD" — only the bare code span
exists. No prompt wording can produce a span the text does not contain. This
is the single largest miss class in the census (~35% of the whole pile) and
it is a **contract/structure problem, not a wording problem**: fixing it means
either (a) instructing extraction of the bare designation span and accepting
the downstream cost (mention collection scans "600"/"V20" with word
boundaries → collisions with prices, quantities, addresses), (b) accepting
category-level recall on catalog pages (the stage returned "CNC Lathes",
actor=reseller_inventory), or (c) structural context (the parked
location/breadcrumb work would let mentions carry their heading). USER
DECISION.

## Patterns in the genuinely returnable never-found pile (~620 entities)

**P1 — Structure blindness: lists, tables, captions, catalogs (largest
returnable class; the F3 shape at full scale).** Present in every field:
alecmfg case-study pipe tables (WPS/PQR, Laser Interferometer, epoxy coated —
alecmfg misses are 6x enriched on `|` lines), mathewsco partner line-card
tables (Centrifugal Castings, Micro Stampings — 36x enriched),
howcogroup chemical-composition tables (Boron, Grade 630) and
applications link-lists (Heat Exchangers, Pressure Vessels, land-based gas
turbines), superiortech's "(1) WENZEL XO97 CMM…" inventory list (22 in one
window), pradeepmetals microwave-plant spec bullets (Cyclone separator,
Vibro-feeder, MAGNETIZERS — a confirmed must-find), decimal's
Testing & Inspection list, agstech link-directories, taylordunn serial-number
listings. The label-shaped variant: tanfel's carousel/caption labels —
**"Engineering & Design Support" occurs in 7 windows, never returned once**;
"Casting Finishes" occ5; photo-gallery captions (Powder Metallurgy).
The model reads prose; it under-reads label-shaped and tabular text.

**P2 — Specificity inversion (F2): the general term returned, the specific
designation dropped.** UNS codes on alloy pages whose alloy name was returned
(UNS N06625…, 10 at howcogroup), DP900 (anchor), Grade 630, aluminum grades,
machine models on manufacturer pages (Makino P-300, Trumpf MB 4200, KUKA KR
16 at blackadvtech — a manufacturer, not a dealer), WILLIS model 1340,
Keyence IM-8000/TESAHITE/Mitutoyo instruments named in a news article. Includes
**under-spanning**: the model returns a shorter fragment that does not cover
the fuller name — 'assemblies' for 'value added assemblies', 'Cross Pendants'
for 'Silver Cross & Titanium Pendant' (decimal, judge-noted).

**P3 — Third-person/educational-prose blindness.** Entities named in blogs,
how-it-works pages, standards encyclopedias, buyer advice — not in capability
claims: blackadvtech's welding blog (titanium, tin cans, racing cars, fillet
welds, KUKA/Trumpf/Makino), agstech's standards encyclopedia (AS9000,
Communications Act of 1934, JIS bodies), fzemanufacturing's turf/hand-tool
explainers (aerators, dethatchers, circular saw), lucasmilhaupt's Brazing
Academy (soldering, welding — occ2 each). Nothing in the prompts restricts
qualification to the subject's own claims — the restriction is the model's
bias, and attribution is downstream's job (actor flags, screening).

**P4 — Coordination-distributed names (the small non-equipment occ0 class).**
"Aluminum die castings in 319, 356, and A357 alloys" → no span reads
"Aluminum 356"; "Aluminum 1xxx-7xxx series"; butt/fillet welds from "butt,
fillet …" coordinations. Sibling of the catalog problem: the name the world
uses is distributed across the span. Partially reachable by wording (return
each designation's own span), same short-form downstream caveat.

**P5 — Boundary classes awaiting rulings (inflate the pile but are NOT
wording defects).** The metrology ruling's siblings, in miss volume order:
tooling (dies, punches, jigs, fixtures, end mills — prompt excludes, judges
include), design/ERP software (SolidWorks, Catia, AutoCAD, DEFORM, PLEX),
facilities/plant/material-handling (ovens, wash lines, Dust Collectors,
Pallet Jacks — acimachine occ 3–4, never returned; prompt excludes
plant-conditioning and conveyance), merchandise/supplied components (agstech
valves, actuators, barcode scanners — sold, not used), engineering/design
services in process_caps (DFM, drawing review, R&D, stress analysis —
"performed on the work" excludes them; judges count them), job-ad/bio/
testimonial-sourced capabilities (Forklift Driving; the pradeepmetals
bio-sweep, ruling already pending), governance/ESG document titles in
conformity (howcogroup ESG list, pradeepmetals charter documents — dispute
already proposed in FINDINGS), standards-ORGANIZATION names vs attestations
(ISO/ITU/JSA as bodies). Also anchor-mfg process_caps: 12 of 17 are equipment
nouns (census refile note stands).

**P6 — Metrology (equipments): ruling landed, edit drafted.** ~70–90 of the
226 non-acimachine equipments never-founds are metrology instruments sitting
in explicit inspection-equipment lists (decimal, superiortech, anchor, howco,
fze, alecmfg). The drafted equipment prompt edit is the fix; they double as
P1 witnesses (they sit in lists).

Caveat per the standing rule (eval-doubt): individual miss records carry the
census's 74.1%/87.6% double-judge floor; the CLASSES above are corroborated
across subjects and, for the headline cases, by the judge2 replication pass.

## Line-type ratio of the never-found pile (added 2026-09-02, after the A/B)

Where each entity's evidence quote sits, over all 1,162:

| line type | n | share | measured remedy status |
|---|---|---|---|
| short_label (catalog tiles, inventory lines) | 613 | 52.8% | heading propagation: 2→332/335 on gpt-4.1 (table_ab/AB_NOTE.md) |
| prose | 318 | 27.4% | prompt drafts E3/E4/E5 (untested) + boundary rulings + retry-union |
| list_item | 166 | 14.3% | same shape as short_label; heading propagation untested here |
| heading | 31 | 2.7% | flickery (A/B arm C); retry-union |
| not_located (coordination-distributed names) | 18 | 1.5% | wording permission for per-designation spans, or eval-forms repair |
| pipe_table | 16 | 1.4% | table→KV measured NO gain (A/B arm D) |

acimachine alone holds ~575 of the label/list classes; the other subjects'
label/list slice is 204 (howcogroup 41, agstech 28, pradeepmetals 28,
fzemanufacturing 23, tanfel 21, ...). Prose is spread thin across 17
subjects (agstech 53, decimal 30, anchor-mfg 29, alecmfg 28,
blackadvtech 28, mathewsco 23, ...).

## General-only extraction proposal (user, 2026-09-02) — the co-occurrence measurement

Proposal: search extracts only the GENERAL designation (Aluminum, not
Aluminum 356); the mechanical mention scan then sweeps every line containing
the general form — specifics riding those lines reach synthesis as snippet
text, and the synthesis prompt's preserve-specifics sentence carries them
into the record. Measured precondition: the specific's evidence line must
contain a form the run actually returned. Over the never-found pile
(approximation: any returned form of the unit, not chunk-scoped):

| field | evidence line already contains a returned form |
|---|---|
| material_caps | 75% |
| products | 69% |
| process_caps | 68% |
| industries | 62% |
| conformity_attestations | 35% |
| equipments | **8%** |
| overall | 24.8% (equipments-dominated) |

So the scheme is mechanically viable for the concept fields (~two-thirds to
three-quarters of their never-found evidence is already swept into some
record's mentions today) and fails alone on equipments' bare-code catalogs —
but composes with heading propagation, which puts the general token (brand/
heading) on every code line. Composition also makes the mini tier viable
again: in the A/B, gpt-4.1-mini read augmented text and returned the
generals — which is all this architecture asks of search.

Load-bearing untested link: synthesis specifics-preservation fidelity (the
sentence exists in the synthesis statics; never measured — synthesis eval
harness owns it). Strains: record snippet volume (a general record can
accumulate scores of snippets; 50-mention request cap; coverage-vs-length),
mixed-actor evidence in one record for screening, radius-0 clips cutting
specifics in long multi-sentence lines (radius is a knob), and specifics
existing ONLY as text — no per-grade record/tag in the graph (fine if
grounding stays category-level; user's call). Adopting this REVERSES the
drafted E3 general-vs-specific clause and the census F2 framing.

Prompt wording can plausibly attack P1, P2, P3 (and P4 partially) — together
roughly half of the never-found pile; P5 needs rulings, the occ0 catalog
class needs a decision, and the residual scattered singles plus everything
below the A/A floor need dispatch (second-pass union; the 61 cross-chunk
entities need subject-wide pooling; the 6 degenerate windows need re-ask).
Draft edits and the decision list are in the session chat of 2026-09-02;
edits are NOT applied — user reviews first.
