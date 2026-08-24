# Sixth v3 run analyzed: 20260823T200044 — the synthesis stage on the rewritten statics

The first run of the synthesis statics rewritten on 2026-08-23 (form-family-not-concept, keep-on-doubt,
all-set-aside floor, the "what the manufacturer does with the entity" section, the own-name masking rule).
Same two subjects, `stop_after(initial_grounding)`, location arm ON (`loc=1`), 50 entries per request,
`gpt-4.1`, temperature 0, seed 12345.

**This is a clean A/B, not just a run.** Ten minutes earlier, `20260823T195031` replayed the *same* mention
state through the *previous* statics. Every one of the 95 synthesis requests carries an identical `ud=`
digest across the two runs and an identical id set — only `pv=` differs. So every difference below is the
prompt and nothing else. Search and mention collection were not re-run at all; both runs replay the
04:45:00 mention answers (visible in `run.time_span.by_stage` — search and mention stamped 04:45, synthesis
stamped 19:50 / 20:00).

Scripts: `synthesis_ab.py` / `synthesis_ab_output.txt`, run from the repo root.

> **Counting caveat.** `products` and `contract_products` share every phrase stage through synthesis —
> `ContractProductSynthesisNode.get_request_custom_id` mints the id as if the field were `products`, so the
> LLM is asked once and both branches read the one answer. Their two dump files are therefore byte-identical
> apart from `field_type`, and any statistic summed over all dump files double-counts them by ~32%. Every
> number below skips `contract_products`: **2,133 records, 95 requests** — the true totals.

## Verdict in one line
The stage is mechanically flawless — 2,133/2,133 records synthesized, zero retries, zero unknown ids, zero
parse failures, 4,174 output tokens against a 20,000 cap — and the rewrite delivered its headline goals
(alecmfg's own-name leak **47 → 0**; the set-aside floor fires **24× more often**; third-party attribution
6.5% → 9.1%) for **+28% output**. Audited against the source text, the new "what the manufacturer does"
section is **sound in 174 of the 176 cases it fires on thin evidence** — the two failures are both the
"More from Allegion" attribution — so the live questions are cost, the one stochastic entity swap, and
whether synthesis should be making that judgement at all rather than leaving it to screening.

## RIGHT

1. **Perfect delivery, both runs.** 2,133 records sent, 2,133 synthesized. `not_synthesized=0`,
   `retried=0`, `retry_requests=0`, `unknown_answer_ids=0` across all 95 requests and both subjects. The
   under-answer retry path built in 3.2 has still never had to fire. No response parse errors; the only
   `synthetic_response` rows in the dumps are the two known zero-form mention windows
   (`alecmfg/conformity_attestations 0:23771`, `steelcraft/equipments 154347:158790`), created and never
   dispatched — the dummy-dispatch filter still holds.
2. **The empty-group invariant holds exactly.** 3,309 groups − 148 empty = 3,161 records; 3,161 records
   reached synthesis, zero mismatches over every chunk of every field.
3. **Massive token headroom.** Largest single response 4,174 tokens against `max_completion_tokens=20000`;
   p95 3,306. Nothing was near truncation, which is why nothing came back unsynthesized. Mean per-request
   latency 9.3 s (was 7.0 s), whole stage 209 s wall clock for 95 requests.
4. **alecmfg's own-name leak is gone: 47 → 0.** The old statics leaked "Alec Model" 47 times, all in
   `industries`. The new masking rule ("read back every string you wrote and check that no mention of the
   manufacturer's name survived") took it to zero — no hits anywhere, in any field, for that subject.
5. **The set-aside floor works, and now actually fires: 2 → 48 records (0.1% → 2.3%).** The measured
   exemplar: `process_caps` focal `rolling`, whose only two snippets are "get the ball rolling" and "orders
   started rolling in". Both prompts caught it, but the new one states the verdict in the shape the plan
   asked for ("used metaphorically and does not refer to a manufacturing process… the entries do not show
   any dealing by the manufacturer with rolling").
6. **Attribution to third parties improved: 6.5% → 9.1% of syntheses** name a customer / client / supplier
   / other party as the one doing the dealing rather than silently crediting the manufacturer. That is the
   "whose dealing is it" clause landing.
7. **The fold feeding synthesis is clean.** Only 10 of 2,133 records merge forms that differ by more than
   case or plural, and all 10 are legitimate verb-fold or punctuation merges (`CNC Milling`/`CNC milled`,
   `Paladin PW Series…`/`Paladin™ PW Series…`). No cross-entity collapse reached the model.
8. **Locations are effectively complete.** 1 of 6,085 mention entries lacked a usable location. The Location
   stage fix from run 044500 is holding, and the `(location not described)` branch of the new static is
   almost dead code in practice.
9. **Big groups are where the stage earns its keep.** The largest record (`products`, focal `door`, 60
   entries / 113 mentions, 1,994 chars) is a genuinely good aggregate: series, gauges, cores, fire and
   tornado ratings, standards, applications — coherent, attributed, and with zero own-name leaks.

## WRONG

1. **An entity was silently renamed.** `steelcraft/equipments`, focal `FE Series Double-Egress Frames`,
   one entry whose snippet is the literal string `FE Series Double-Egress Frames`. The new statics wrote:
   *"**DE Series** Double-Egress Frames are presented as a product line by the manufacturer… the
   manufacturer offers **DE Series** Double-Egress Frames as a product."* Both the focal form and the sole
   snippet say FE. DE Series is a real, different Steelcraft product that appears in other records of the
   same batch — this is cross-record contamination inside a 50-entry request. It is **new** (the same record
   is correct in 195031) and **stochastic**, not systematic: the identical record in the `products` field
   came out right, and a sweep of every `<X> Series` name in every steelcraft synthesis found this as the
   only genuine invention in 1,250 records (the 5 `Paladin Series` flags are the ™ being dropped from
   "Paladin™ Series", not inventions).
   **Why nothing caught it:** the hold validates `record_id` echoes, never the entity. A wrong entity is
   indistinguishable from a right one downstream, and it will ground as DE Series.
2. **The dealing rule changes the register of the output — but it is not inventing facts.**
   *(This entry replaces an earlier, wrong reading. The first pass flagged all 176 as overreach; auditing
   them against `sample_scraped_texts/` shows the great majority are well founded.)*

   On the 393 records whose entire evidence is one snippet of ≤60 characters, "the manufacturer
   offers/produces/provides…" went from **4 (1%) to 176 (45%)**. Where those 176 come from:

   | page the evidence sits on | records | assertion sound? |
   |---|---|---|
   | the company's own product pages | 71 | yes |
   | alecmfg case studies (delivered projects) | 43 | yes |
   | own services pages | 17 | yes |
   | own service-centre / location pages | 16 | yes |
   | own applications pages | 9 | yes |
   | homepage / support pages | 5 | yes |
   | resources & downloads index | 15 | mixed — see below |

   Verified against the source text: `Micro-blasting` sits in a table headed *Step / Parameters / Result*
   under "▶ Medical-Grade Surface Treatment" in a case study of work the company delivered
   (`alecmfg.com.txt` L1049) — "the manufacturer provides micro-blasting" is correct, and a demonstrated
   process step is stronger evidence of capability than a marketing list. Likewise `4-axis machining`,
   drawn from a *Client Requirements* table, produced the hedged "the manufacturer **is expected to**
   provide 4-axis machining" — the model registered that the snippet was a customer's spec, and the same
   page goes on to describe the work being done (`L2464 ff.`). The rule also produced a **negative**
   dealing correctly: snippet "Frames not available with factory finish paint" → *"The manufacturer **does
   not offer** factory finish paint for frames."*

   **Genuine defects in the 176: two.** Both are the same failure — the location said the item sat under
   the **"More from Allegion"** heading on the sustainability page, and the new synthesis dropped that
   qualifier while the old one kept it:
   - `LEED Credits` — old: *"…under the 'More from Allegion' section."* → new: *"the manufacturer provides
     information about LEED Credits."*
   - `CalGreen Building Standards` — identical shape.

   Source confirms these belong to the parent: *"More from Allegion / Find industry affiliations, resources
   and more in the Sustainability section of us.allegion.com. / General Sustainability Information / LEED
   Credits CalGreen Building Standards"* (`steelcraft.com.txt` L146–151). Sweeping every record whose
   location names a third party (87 of them), synthesis dropped ≥1 party name in 32 and still credited the
   manufacturer in 15 — but 13 of those 15 dropped only "Falcon", a Steelcraft sub-brand, which is not an
   attribution error. **Allegion is the only real one, and it is the one that matters**, since both records
   are in `conformity_attestations` and would ground as the company's own claims.

   **A separate, smaller effect worth knowing about: 33 records get "the manufacturer provides this as a
   resource" because their only occurrence in that chunk is a link label in a downloads list.** 23 of those
   focal forms are plainly document titles (`Stainless Steel Data Sheet`, `Steelcraft Catalog`) — correct,
   and arguably *helpful*, since a screener can now drop them on the strength of the word "resource",
   where the old text ("listed as an item in the literature list") left the inference to be made. The other
   10 are **real product lines used as download link labels** (`GRAINTECH Doors`, `FT Series (Thermal
   Break)`, `Falcon SZ Series`, `Integral Kerfed Frame`), which this reading demotes to documents. Checked
   for actual loss: GRAINTECH is covered by 9 other records in the same field and FT Series by 5, so
   nothing is lost for those; `Integral Kerfed Frame` and `Falcon SZ Series` are covered only by
   literature-list records, and there the second chunk's record correctly says *"the entries do not show
   any specific dealing by the manufacturer with it."* The floor caught it. No net loss found.

   **So the real question is not correctness but division of labour**: the new statics move a judgement
   ("this is something they do") from screening into synthesis. That may be exactly what you want — it is
   the claim grounding needs — but it means synthesis now commits earlier, on thinner evidence, than the
   deposition framing intended.

3. **The own-name ban is self-defeating on steelcraft, and the metric over-reads it.** 139 → 126 hits, a
   9% improvement against alecmfg's 100%. But the ban is structurally unsatisfiable here:
   - **58 of the 126 hits (46%) are in records whose `focal_form` itself contains "Steelcraft"** —
     `Steelcraft L Series`, `Steelcraft Hurricane products`, `Republic and Steelcraft Limited Warranty`.
     The static says *"refer to the entity by its focal_form"* and *"do not emit the manufacturer's name
     anywhere"*. Both cannot hold.
   - Most of the rest are proper nouns the site owns: `Steelcraft Technical Data Manual` (31),
     `Steelcraft Main Plant`, `Steelcraft West Coast Service Center`, the `'Working with Steelcraft'`
     support page. These arrive **in the location text the pipeline itself wrote** — 12.8% of steelcraft's
     3,579 mention locations contain the name, and 13.5% of snippets do. Mention collection is not under
     the ban; synthesis is. We feed the name in and then penalise the model for reading it back.
   - Genuine gratuitous leaks — bare "Steelcraft" as the sentence subject where "the manufacturer" would
     do — are a small residue: *"states that Steelcraft is SDI Certified"*, *"Steelcraft was founded in
     1933"*, *"in 1959, Steelcraft sold its metal building division"*.
   - The `[the manufacturer]` bracket substitution the static specifies was used **7 times in 1,250
     steelcraft syntheses**. It is essentially unused.
4. **+28% output for the same 2,133 records.** 645,553 → 829,318 characters; mean 303 → 389, median
   243 → 328. Output tokens 158,933 → 189,669; estimated stage cost $2.13 → $2.42 for two subjects.
   The growth is uniform across every evidence size (1 entry: 221 → 303 chars; 10+: 1,304 → 1,422), so it
   is the extra mandated sentence, not better coverage of rich records. Extrapolated to a real corpus this
   is the stage's dominant cost line.

## Expected, and confirmed
- Retry path idle, dummy path idle, digest pinning exact — the 3.2 machinery behaves as designed.
- `include_location=true` on every field; the location arm is the only one that has been exercised.
  **The A/B the plan asked for (`loc=1` vs `loc=0`) has still not been run** — both 195031 and 200044 are
  the same arm, so this run says nothing about whether locations are worth their tokens.
- Focal form absent from its own synthesis: 21 → 24 of 2,133 (~1%), and nearly all are benign
  re-phrasings of long sentence-shaped focal forms (`filled with 1 pound fiberglass batting between
  stiffeners` → "the manufacturer fills the space between stiffeners with…").

## Unexpected
- **70% of records carry exactly one entry; 86% carry two or fewer.** The stage writes a ~300-character
  paragraph about a single snippet, 1,486 times. That is where the money goes, and it is also exactly the
  population in which finding 2 (invented dealings) lives. Synthesis is faithfully amplifying whatever the
  search stage produced, including the noise: `process_caps` focal forms in steelcraft include
  `Prime Paint`, `Testing and Rating of Severe Windstorm Resistant Components`, and 47 focal forms of six
  words or more.
- **The `contract_products` dump is a copy of the `products` dump.** Correct by design and it saves 39 of
  134 requests, but nothing in the dump says so, and the file's own `custom_id`s read `>products>`. Anyone
  totalling the dump folder will overstate the stage by a third.
- The `run.time_span.seconds` for these fields reads 55,153 s (15 h) because the run resumed on top of the
  04:45 mention state. The per-stage span (209 s) is the real synthesis cost.

## The two open design questions

### Should the own-name ban stay in synthesis?
The evidence says no, and moving it downstream repairs something already broken.
- The ban is **unsatisfiable by construction on steelcraft**: 46% of its hits are records whose focal form
  contains the name, against a rule that says to call the entity by its focal form.
- The pipeline **supplies the name itself**: 12.8% of steelcraft's mention locations and 13.5% of its
  snippets carry it, written by a mention-collection stage that is not under the ban.
- The prescribed workaround is **dead**: `[the manufacturer]` appears 7 times in 1,250 syntheses. In
  practice the model either leaves the name in (a "violation") or paraphrases the quotation away (real
  fidelity loss).
- The lint **changes nothing downstream** by design (`subject_name_lint.py`: "renders a verdict about a
  string, never about the pipeline").
- Decisively: **the screening statics already assert the ban as fact and already carry the logic that
  would replace it.** All seven `4_phrase_relationship_screening/*.txt` say *"Records refer to the
  manufacturer in question only as 'the manufacturer' or '[the manufacturer]', never by name"* — false on
  live data, and visibly so to the model — immediately followed by *"A brand, trade name, former name,
  division, plant, or business unit of the manufacturer IS the manufacturer itself."* That second sentence
  is exactly the rule that resolves `Steelcraft L Series`. It only needs the name to apply it.
- **Wiring cost is one line, but it is not already there.** `create_record_screening_batch_request` builds
  its context as `render_record_blocks(screening_payloads)` alone — no subject name. Synthesis already
  prefixes `"the name of the manufacturer in question: {subject_name}"`; screening and grounding would
  need the same. (An older note claiming screening already carries `Manufacturer name:` is stale for the
  v3 path.)
- What is lost: the ban currently doubles as an anti-shortcut device, pushing the model to reason about
  *standing* rather than name-matching. If it goes, the screening statics should say what to do with a
  name rather than pretend none appears.

### Should the location arm be turned off?
Probably not — but there is a better lever.
- Locations are **63% of the evidence payload** (952,386 chars vs 567,678 of snippets; median 191 vs 112).
- **89% of syntheses name a page, section or heading**, so the arm is being used, not ignored.
- It matters most exactly where the corpus is thinnest: **70% of records carry one entry**, and for those
  the location is the only thing separating "listed in a Downloads section" from "stated in the FEATURES
  AND BENEFITS list" — which is the distinction screening needs. Several findings in this document
  (the Allegion attribution, the literature-vs-product split, the client-requirement hedge) exist *only*
  because the location was there to read.
- **The cheaper lever: 27% of location text is a boilerplate opener** ("This passage appears as…",
  "This sentence is found in…") — 333,450 of 1,229,700 characters. Cutting it saves ~17% of the whole
  synthesis input with no information lost, and is a mention-stage prompt change, not an arm flip.
- The `loc=0` arm is still worth running once as a measurement, but on this evidence the expected result
  is worse records for a modest saving.

## Final recommendations (2026-08-23, after the source-text audit)

1. **Flip the manufacturer name from a taboo into an identification aid.** Delete the masking paragraph
   and the read-back check from the six synthesis statics; keep passing the name and add *"where the focal
   entity is the manufacturer itself, or one of its brands, divisions, plants or former names, say so."*
   45 steelcraft records have a focal form containing the name and today read as ordinary products —
   that is a signal the screening rule *"a brand, trade name, former name, division, plant, or business
   unit of the manufacturer IS the manufacturer itself"* is already written to consume.
   **Sequencing:** screening is in `stages_disabled`, so change synthesis NOW at zero risk, and do the
   screening-side wiring (`create_record_screening_batch_request` needs `subject_name` in its context)
   **as part of 3.3**, which is re-keying screening onto `group_id` anyway — otherwise it is written twice.
   While in those statics, the false sentence *"Records refer to the manufacturer in question only as
   'the manufacturer'… never by name"* must go regardless.
   `own_name_hits_in_syntheses` then stops meaning "violations"; keep the counter, stop reading it as a
   defect count.
2. **Keep the location arm; replace the boilerplate opener with a named job.** "Avoid boilerplate" alone
   will produce different boilerplate. Name the three things synthesis visibly uses: (a) what kind of text
   it is (heading, bullet, body copy, table cell, menu item, quote); (b) what it belongs to — page and
   section, in words; (c) **whose words they are** — the site's own copy, a customer, a publication, the
   parent company. (c) is also the fix for the Allegion defect, which happened because "More from Allegion"
   was in the location and was compressed out on the way into the synthesis. Expected saving ≈17% of the
   whole synthesis input (opener = 27% of location text; locations = 63% of payload), no information lost.
3. **Add the party-preservation sentence to the synthesis static:** *"Where a location or snippet attributes
   the item to a party other than the manufacturer — a parent company, a publication, a customer — that
   party must be named in the synthesis."* Two records ride on this today, both in
   `conformity_attestations`. Cheapest fix on this list.
4. **Add an entity guard as a dump lint** (not a retry trigger): the focal form or one of its member forms
   must appear in its own synthesis, firing only on **entity-shaped** focal forms (2–6 words, with a
   capitalized token after the first). Measured on this corpus:

   | run | entity-shaped records | flags | real bugs |
   |---|---|---|---|
   | 20260823T195031 | 700 | 3 | 0 |
   | 20260823T200044 | 700 | **2** | 1 (FE→DE) |

   Two rows per run, one of which is the actual bug — and it is the one failure mode nothing else can see.
5. **Do nothing about the +28% yet.** The cost driver is upstream: 70% of records have a single entry
   because search produces records for document titles and sentence-shaped fragments. Falls under the
   user's 2026-08-22 rule — execute the whole pipeline first, then adjust knobs.
6. **WITHDRAWN — do not add a "listed, therefore probably offered" floor.** An earlier draft of this
   document suggested it. The audit says the dealing rule is working (it hedges on client-requirement
   tables, extracts negative dealings, and the existing all-set-aside floor already catches the
   literature-only records). Another floor would suppress good output to fix a problem that is not there.

### A/B hygiene for the next runs
- The **name change** is synthesis-prompt-only: `ud=` is unchanged, search and mention replay from Mongo,
  only synthesis re-runs — a clean A/B against 200044 for ≈$2.40.
- The **location change** is a mention-stage prompt change: new locations flow into `wire_dict()` → new
  `ud=` → new synthesis ids, so both stages re-run (≈$4 for both subjects). That is correct behaviour —
  synthesis is protected from the stale-replay trap by `ud=` — but it means effects cannot be attributed
  if the two changes are bundled. **Two runs, not one.**

## Earlier suggestions (superseded by the section above, kept for the record)
## Suggested next steps (not taken)
1. Decide the focal-form/own-name conflict explicitly: either exempt a focal form that contains the
   manufacturer's name from the ban (and say so in the static), or mask it in the focal form before it is
   sent. Either way the lint should stop counting it.
2. Consider putting the own-name rule in the **mention-collection** static too, so locations stop carrying
   the name into a stage that is forbidden to repeat it.
3. Fix the one attribution class that is actually broken: require the party a location names to survive
   into the synthesis. Two records ride on it today, both in `conformity_attestations`.
4. The FE→DE swap argues for a cheap mechanical guard: assert the focal form (or one of the group's member
   forms) appears in its own synthesis, and flag the ~1% that fail. It would have caught this record.
