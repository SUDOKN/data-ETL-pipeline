# JUDGE_PASS2 — synthesis prompt, published arm vs the five-edit arm

Judged 2026-09-12. Same three real synthesis requests, same weak model (gpt-4.1-mini, temperature 0,
seed 12345), same user block; only the system prompt differs.

## Headline

| chunk | better | same | worse |
|---|---:|---:|---:|
| 1 — mathewsco.com, process_caps (39) | 10 | 10 | **19** |
| 2 — anchor-mfg.com, process_caps (38) | 7 | 30 | 1 |
| 3 — superiortech.org, material_caps (34) | 3 | 8 | **23** |
| **total (111)** | **20** | **48** | **43** |

**Truth/frame regressions: 21 records. Verdict: REVISE FIRST.** Two of the five edits earn their place
(page naming, first-person crediting) and one is a clear shape win (request vocabulary gone from 111/111).
But edit (1) reintroduces the wrong-party defect that the published prompt was written to fix — 17 records
on the sales-rep subject now read a represented maker's capability as the manufacturer's own, 9 of them with
an added "as part of their manufacturing capabilities" — and edit (3) removes the only sentence that named
the focal entity, which costs focus on 33 records and lets the sibling-list dump back into 23.

## 1. Method and files

- **OLD arm** = `docs_local/field_requirements_survey_20260911/tryout/runA_system.txt` — the prompt that ran
  in production as run A.
- **NEW arm** = `apps/data_etl_app/src/data_etl_app/knowledge/prompts/final_texts/static/multi_stage/4_phrase_synthesis/product_phrase_synthesis.txt`
  — differs by exactly five edits: (1) the list-item dealing rule replacing the capacity sentence;
  (2) name the page only as the text shows it above the snippet; (3) where-it-sits once and the dealing once,
  never a closing sentence that repeats/sums/strengthens; (4) never write focal_form / focal entity / record;
  (5) a we/our or self-naming sentence is the manufacturer's own wherever it sits.
- **Requests**: `docs_local/synthesis_shape_tryout_20260909/{1,2,3}/A_user.txt`.
- **Outputs**: `pass2/out_{1,2,3}_{old,new}.json`.
- Every one of the 222 paragraphs was read against its record's snippets and against the page the snippet
  sits on in the request text. Counts of duplicate paragraphs, closers and vocabulary were computed
  mechanically and then confirmed by reading.
- Per JUDGE_RUN_A's standing rule, both arms were doubted: OLD's own defects (imports past the snippet,
  a closing source restatement, request vocabulary in 77/111 records) are recorded in §3 as NEW's wins.

**The fact that governs chunk 1.** Mathews & Company is a sales representative, not a manufacturer
("only represent companies…", "a sales representative's organization"). Its Kansas Product Line page lists
its principals' offerings and keeps the principals' pronouns — "**They** offer Die Cutting Water Jet
Cutting…", "**Their** equipment list includes 9-axis Swiss machines…" — while using "We" for Mathews itself
("We can supply your rapid prototype machined"). Whose dealing each snippet shows is the live question.

---

## 2. Per-grade counts, old vs new

Layout follows JUDGE_RUN_A §7a, with the pass-2 items appended.

### Chunk 1 — mathewsco.com, process_caps (39 records)

| grade | old | new |
|---|---|---|
| **(1) frame named** — yes | 39 | 39 |
| — frame accurate | 39 | **37** (`ghu2cklc`, `g7zeb1wf` put the Ellwood City Forge page's text on the "California Office Product Line page") |
| **(2) capacity** — stated as the snippets fix it | 2 (`glrjcsaw`, and `gulv78zg`/`g4qrt3w2` partly) | 3 |
| — declared open, correctly ("offers … but the snippet does not specify … role" / "offers or represents") | **34** | 19 |
| — supplied beyond the snippets | 0 | **17** (the hedge is kept but the closer supplies "their manufacturing capabilities" / "their casting processes" / "their equipment list") |
| — omitted though the snippets fix it | 3 | 0 |
| **(3) generic / agentless copy** (relevant in 2: `ga39any9` near-net, `g8nedk2z`) — handled | 1 | 1 |
| — attributed wrongly | 0 | 0 |
| **(4) document rule** — applied correctly / over-applied / n/a | 1 / 0 / 38 (`g8rnu8jo` Line Card read as a list of companies) | 1 / 0 / 38 |
| **(5) party correct** — yes | **39** | **22** |
| — no (a principal's doing reads as the manufacturer's) | 0 | **17** |
| **(6) focus kept** — yes | 39 | 38 (`g8c93upg` = `gef1saje` byte-identical) |
| **(7) unsupported claim** — yes | 0 | **10** ("their manufacturing capabilities" ×6, "their casting processes" ×2, "their manufacturing or finishing services", "states that they offer precision machining services") |
| *pass-2* **list-item dealing** — credited as the introducing sentence says | n/a (page is a rep's listing) | — |
| — hedged wrongly | 0 | 0 |
| — **supplied beyond the frame** | 0 | **17** |
| *pass-2* **closer** — absent | 36 | **2** |
| — present, harmless | 3 | 20 |
| — **present, restating or strengthening** | 0 | **17** |
| *pass-2* **wire vocabulary** (focal_form / focal entity / record) | 0 | 0 |
| *pass-2* — request vocabulary of the same kind ("the snippet …") | **39** | **0** |
| *pass-2* **page named that the text does not show above the snippet** | 0 | **2** |
| *pass-2* **first-person sentence** — credited / declined | 2 / 0 | 2 / 0 |

### Chunk 2 — anchor-mfg.com, process_caps (38 records)

| grade | old | new |
|---|---|---|
| **(1) frame named** — yes | 38 | 38 |
| — frame accurate | 37 (`gs353907` claims four snippets sit on two pages; one is on a third) | **36** (`gs353907` puts all four on one page; `gtilkt9z`'s wrong sub-section is fixed) |
| — names the sub-section as well | 8 | 0 (page only — permitted by edit 2, a loss of specificity on `goggv9k9`, `gmaz6n4m`, `gnra7i0n`, `gbjdjmc3`, `g09szctr`) |
| **(2) capacity** — stated as the snippets fix it | 38 | 38 |
| — supplied / declared-open | 0 / 0 | 0 / 0 |
| **(3) generic or agentless copy** (relevant in 10: "Manufacturing processes use…", "Assembly is performed…", "…are assembled", "Simulation-driven design … are used") — handled | 10 | 10 |
| — attributed wrongly | 0 | 0 |
| — *kept in the passive and marked as the manufacturer's own statement* | 0 | **10** |
| **(4) document rule** — n/a | 38 | 38 |
| **(5) party correct** — yes | 38 | 38 |
| **(6) focus kept** — yes | 38 | **14** |
| — no (paragraph byte-identical to 1–5 siblings) | 0 | **24** (7 groups: 6, 5, 3, 3, 3, 2, 2) |
| **(7) unsupported claim** — yes | **2** (`gc89hyw5` "reducing capital costs and benefiting customers" — the snippet's next two sentences) | 0 |
| *pass-2* **list-item dealing** — credited as the introducing sentence says | 38 | 38 |
| — hedged wrongly / supplied beyond the frame | 0 / 0 | 0 / 0 |
| *pass-2* **closer** — absent | 8 | **38** |
| — present, restating or strengthening | **30** | **0** |
| *pass-2* **wire vocabulary** | 0 | 0 |
| *pass-2* — "the snippet …" | **38** | **0** |
| *pass-2* **page named that the text does not show** | 1 (`gs353907`, partial) | **1** (`gs353907`, total) |
| *pass-2* **first-person sentence** — credited / declined | 12 / 0 | 12 / 0 |

### Chunk 3 — superiortech.org, material_caps (34 records)

| grade | old | new |
|---|---|---|
| **(1) frame named** — yes | 34 | 34 |
| — frame accurate | 34 | 34 |
| — frame specific (names the page's own heading, not just "the capabilities page") | 21 | **31** |
| — *frame stated twice in one paragraph* | **1** (`gx7l6w75`: opening list of pages and a closing "This synthesis is based on …") | 0 |
| **(2) capacity** (work managed with vendors: heat treatment, plating, NDT) — stated as the snippets fix it | 8 | 8 |
| — supplied beyond the snippets | 0 | 0 |
| — declared open / silent where snippets are silent | 26 | 26 |
| **(3) generic or agentless copy** (relevant in 3: "Multi-axis milling is used…", "Lapping and grinding are used…") — handled | 1 | **3** |
| **(4) document rule** — n/a | 34 | 34 |
| **(5) party correct** — yes | 34 | 34 |
| **(6) focus kept** — yes | **29** | **11** |
| — no (body is the sibling list, or paragraph byte-identical to siblings) | 5 | **23** (14 recite the whole materials list; 9 plating records share one paragraph) |
| **(7) unsupported claim** — yes | 0 | 0 |
| *pass-2* **list-item dealing** — credited as the introducing sentence says | 34 | 34 (the Value-Added page's "Superior Technology, Inc. offers … Value Added Secondary Processes" correctly licenses "offers" on every row; edit 1 working as intended) |
| — hedged wrongly / supplied beyond the frame | 0 / 0 | 0 / 0 |
| *pass-2* **closer** — absent | **33** | 5 |
| — present, harmless | 0 | 19 |
| — present, restating or strengthening | 1 | **10** |
| *pass-2* **wire vocabulary** (focal_form / focal entity / record) | 0 | 0 |
| *pass-2* — request vocabulary of the same kind ("the manufacturer's own …" instead of the company's name) | 0 | **32** |
| *pass-2* **page named that the text does not show** | 0 | 0 |
| *pass-2* **first-person sentence** — credited / declined | 34 / 0 | 34 / 0 |
| *pass-2* **homonym rule** ("lead" as lead time, `gpk94tty`) — held | yes | **yes** |

---

## 3. Better / same / worse, with examples

| chunk | better | same | worse |
|---|---:|---:|---:|
| 1 | 10 | 10 | **19** |
| 2 | 7 | 30 | 1 |
| 3 | 3 | 8 | **23** |
| **total (111)** | **20** | **48** | **43** |

### Chunk 1 — better (10): `gbqwv8mr`, `gpa4z2mo`, `gsawivwk`, `gulv78zg`, `g8c93upg`, `gef1saje`, `gbkrl7iv`, `g64ggddx`, `glrjcsaw`, `g8nedk2z`

- `gulv78zg` — NEW: *"Mathews & Company states, \"We can supply your rapid prototype machined,\""* — the first person is
  quoted and attributed; OLD buried it in *"The snippet naming rapid prototype machined appears…"*.
- `glrjcsaw` — NEW keeps to the entry's own words and does not import the next sentence's size ranges.
- `g8nedk2z` — NEW: *"lists \"Precision components and assemblies\" as a product line, describing companies that do
  precision machining"* — the party stays with the unnamed companies.
- `gbqwv8mr` — 502 → 386 chars with the frame and the hedge both intact.
- `gpa4z2mo` — both casting entries reported in the entries' own terms, no meta preamble.

### Chunk 1 — worse (19)

- `gjpkpgsp` (and `gvoll26g`, `gkxhdeph`, `ghhnmcs9`, `g63nk11h`, `gbyy3ii7`, `gb7lm6fw`) — NEW: *"Mathews & Company
  states that **their equipment list** includes 9-axis Swiss machines… This shows that Mathews & Company offers or
  represents **manufacturing capabilities** including 9-axis Swiss machines."* The snippet's "Their" is a principal's.
  OLD: *"…but the snippet does not specify Mathews & Company's manufacturing role."*
- `gwnanq35` (and `grrakqm9`, `gl1imbur`, `gnwwkljf`, `gcwci3hl`, `gdt982tg`) — NEW: *"Mathews & Company states that
  **they offer** die cutting … as part of **their manufacturing capabilities**."* Snippet: "**They** offer Die Cutting…"
- `gcsfu74l` — NEW: *"Mathews & Company states that **they offer precision machining services and products**"*; the entry
  actually reads *"If you are looking for a company that does precision machine…"*.
- `gmqxyi1d`, `gluqsr68` — NEW closes *"as part of **their casting processes**"*.
- `ghu2cklc`, `g7zeb1wf` — NEW: *"appears in the **California Office Product Line page** under the Ellwood City Forge
  section"*; the snippet sits on the page headed "# Ellwood City Forge", and the California Product Line page's visible
  list does not even contain Ellwood City Forge.

### Chunk 2 — better (7): `gc89hyw5`, `g0jckvyk`, `gdytq9zc`, `g9zoswf4`, `gl6dq871`, `ga39any9`, `g9nb3sn1`

- `gc89hyw5` — OLD imported the snippet's next two sentences (*"reducing capital costs and benefiting customers"*);
  NEW stops where the snippet stops.
- `gdytq9zc` — NEW gives each of the four snippets its own page instead of OLD's pooled "the Quality Management page
  and the Advance Engineering page".
- `g9zoswf4` — NEW: *"states that assembly **is performed** using both robotic and manual MIG, TIG and laser welding"* —
  the passive is kept and marked as the manufacturer's own statement; OLD rewrote it as *"Anchor performs assembly"*.
- `gl6dq871` — same, for *"PEM nuts … are assembled"*.
- `ga39any9` — 650 → 395 chars, all three sentences still reported.

### Chunk 2 — worse (1)

- `gs353907` — NEW: *"**On the Advance Engineering page**, … it specializes in mid to high volume stampings … ; **the page
  also** highlights collaborative product development."* The "We specialize…" sentence is on the About page and
  "Collaborative Product Development" is a heading on the design-and-tooling page. OLD named two pages of the three.

### Chunk 3 — better (3): `gx7l6w75`, `g6xgsv4h`, `gcn1529e`

- `gx7l6w75` — OLD stated its source pages twice, opening and closing (*"This synthesis is based on the main page
  capabilities table, …"*); NEW states them once.
- `g6xgsv4h`, `gcn1529e` — NEW opens with the page (*"According to the 'CNC Milling' capability page…"*) and keeps the
  agentless "is used" rather than OLD's *"Superior Technology, Inc. uses multi-axis milling"*.

### Chunk 3 — worse (23)

- `glnxw175`, `g4o19lac`, `ga30s0he`, `gr5tcz7r`, `g26bcj3m`, `goo79est`, `g3t9xlce`, `ghtzsxw9`, `gfgcjktg` — **one
  byte-identical 389-char paragraph for all nine**; the focal entity is never the subject. OLD: *"Superior Technology,
  Inc. offers **blackening** as part of their plating, paint, and specialized surface treatments."* (167 chars).
- `gov88shm`, `gm37d125`, `g8rkxb1m`, `gywkf0xo`, `gsgixn7o`, `gshl3r6g`, `gtgvvhcz`, `gnqgk3b5`, `ganencyn`, `grs9kb8i`
  — each now recites the whole materials list (*"…carbon and stainless steels, aluminum alloys, super alloys (such as
  Titanium and Inconel, Hastalloy), brass and copper alloys, plastics, castings, forgings, and all materials in
  between"*) before the closer names the focal one: 217 → 531 chars on `gov88shm`, 207 → 521 on `gshl3r6g`.
- `gknmr82d` 613 → 889, `gkqhvubm` 334 → 699, `gpa4z2mo` 510 → 852, `gr5ktl6l` 510 → 871 — same list, on top of the
  process rows they already carried.

---

## 4. Regressions, exhaustively by record id

**21 records carry a truth or frame regression.** All but one are in chunk 1.

### 4a. Wrong party — a represented maker's doing reads as the manufacturer's own (17, all chunk 1)

The snippet's own pronoun puts the doing with a principal; NEW makes the manufacturer the subject of the
reporting verb and then resolves the pronoun to it.

| record_id | focal form | NEW's convicting words |
|---|---|---|
| `gwnanq35` | Die Cutting | "Mathews & Company states that **they offer** die cutting … as part of **their manufacturing capabilities**" |
| `grrakqm9` | Water Jet Cutting | same sentence, closer names water jet cutting |
| `gl1imbur` | Slitting | same sentence, closer names slitting |
| `gnwwkljf` | Extrusions | same sentence, closer names extrusions |
| `gcwci3hl` | Transfer | same sentence, closer names transfer molding |
| `gdt982tg` | Compression Molding | same sentence, closer names compression molding |
| `gpoeco15` | Injection | "Mathews & Company states that **they offer** die cutting, water jet cutting…" (OLD said plainly the snippets do not indicate Mathews manufactures injection-moulded parts) |
| `gjpkpgsp` | 9-axis Swiss machines | "states that **their equipment list** includes …" + "offers or represents **manufacturing capabilities**" |
| `gvoll26g` | CNC Machining | same |
| `gkxhdeph` | 5-axis vertical milling | same |
| `ghhnmcs9` | vertical milling | same |
| `g63nk11h` | milling | same |
| `gbyy3ii7` | precision grinding | same |
| `gb7lm6fw` | grinding | same |
| `gcsfu74l` | precision machine | "Mathews & Company states that **they offer precision machining services and products**" |
| `gmqxyi1d` | floor molding | "offers or represents floor molding as part of **their casting processes**" |
| `gluqsr68` | cope & drag | same |

### 4b. Laundering — a capability asserted as the manufacturer's that the snippets never give it (10, subset of 4a plus one)

`gwnanq35`, `grrakqm9`, `gl1imbur`, `gnwwkljf`, `gcwci3hl`, `gdt982tg` ("their manufacturing capabilities");
`gmqxyi1d`, `gluqsr68` ("their casting processes"); `gcsfu74l` ("they offer precision machining services");
and `g4qrt3w2` ("as part of **their manufacturing or finishing services**" — here the party is right, the
manufacturer's own "We can apply…", but "manufacturing" is supplied on a sales representative).

### 4c. Lost hedge — an open capacity closed by the closing sentence (9)

The same nine of 4b that carry "their …": in each, OLD's explicit refusal ("*but the snippet does not
specify Mathews & Company's manufacturing role*") is replaced by a possessive that answers the question the
hedge left open. The hedge phrase "offers or represents" survives in the same sentence, so the paragraph
both hedges and un-hedges; a reader takes the possessive.

### 4d. Newly supplied dealing beyond the frame (17)

Identical to 4a: in every one, the list rule of edit (1) supplied the dealing from the page frame where the
item's own words had assigned it to someone else. No case of a dealing supplied where the snippets were
silent about any party.

### 4e. Frame fabricated — a page named that the text does not show above the snippet (3)

- `ghu2cklc`, `g7zeb1wf` (chunk 1) — "California Office Product Line page under the Ellwood City Forge section";
  the snippet is under the page heading "Ellwood City Forge", and the named page's own list omits that company.
  The name is taken from the navigation sidebar, not from above the snippet.
- `gs353907` (chunk 2) — all four snippets placed on the Advance Engineering page; two are on other pages.

### 4f. What did NOT regress

- **No wrong party anywhere in chunks 2 and 3** (76 records).
- **The represented-maker carve-out held**: on `ghu2cklc`/`g7zeb1wf` the other company's first person
  ("We specialize in a variety of custom forgings") is still credited to Ellwood City Forge in both arms.
- **No first-person sentence was declined as nobody's sentence in either arm** (48 first-person records across
  the three chunks). Edit (5) cost nothing and its target defect did not appear in the OLD arm either.
- **The document rule was not over-applied** (`g8rnu8jo`'s Line Card is still read as a list of companies).
- **The homonym rule held**: `gpk94tty` keeps "lead" as lead times in both arms; neither bridges to the metal.
- **Wire vocabulary**: "focal_form", "focal entity" and "record" appear in 0 of 222 paragraphs, both arms.
  Edit (4) is unfalsifiable on this data — the defect it targets did not occur in the OLD arm.

### 4g. Not a truth regression, but the largest shape loss: 33 records that no longer distinguish themselves

24 in chunk 2 (7 groups) and 9 in chunk 3 are **byte-identical to their siblings**; a reader of the paragraph
cannot tell which of 2–6 focal entities it is about. OLD had 0 such records in chunk 2 and 5 in chunk 3.

---

## 5. Length reading

Totals are of synthesis text, not file bytes.

| chunk | old total | new total | old mean | new mean | change |
|---|---:|---:|---:|---:|---:|
| 1 | 16,461 | 13,529 | 422 | 347 | −17.8% |
| 2 | 13,948 | 8,896 | 367 | 234 | −36.2% |
| 3 | 11,261 | 19,188 | 331 | 564 | **+70.4%** |

**Chunks 1 and 2 shrink for the same two reasons**, both good: the meta preamble ("The snippet naming X
appears on the … page") is gone in 111 of 111 records, and in chunk 2 the "This shows that …" closer is gone
in 38 of 38. Chunk 1's shrink also removes something that was not padding — the explicit capacity refusal
clause — and that removal is what §4c counts as a lost hedge.

**Chunk 3's growth is repetition, not information.** Reading the grown sentences by class:

| class | records | ≈ chars added | information or repetition |
|---|---:|---:|---|
| the whole materials list recited before the focal material is reached | 14 | ~4,200 (53%) | **repetition** — the siblings' material names, which OLD correctly stripped |
| the plating cluster's shared full-process paragraph | 9 | ~1,900 (24%) | **repetition** — one paragraph serving nine records |
| "certifiable and traceable to many industrial and Mil-Specs", brazing clause | 6 | ~900 (11%) | **information** — inside the record's own snippets, absent from OLD |
| closing "This shows the manufacturer's own …" sentences elsewhere | ~10 | ~700 (9%) | **restatement** |
| accurate page/heading naming replacing "the capabilities page" | ~10 | ~250 (3%) | **information** |

So roughly **14% of the growth is new, snippet-supported information and 86% is repetition or restatement.**
The mechanism is visible: edit (3) forbade the closing sentence, the model kept a closer anyway in chunks 1
and 3, and the space that edit (2)'s tighter framing freed was filled by the sibling snippet body. Chunk 2 is
the control — there the closer ban actually took, and the chunk shrank by a third with no loss of truth.

---

## 6. Verdict: REVISE FIRST

Two of the five edits are keepers as written. Edit (5) (the we/our rule) costs nothing and its carve-out
holds. Edit (4) leaves the banned words at zero. Edit (2) makes chunk 3's frames markedly more specific
(21 → 31 records naming the page's own heading) at the cost of two navigation-derived page names.

Edit (3) half-works: it removed the restating closer completely in chunk 2 (30 → 0) and not at all in
chunks 1 and 3 (37/39 and 29/34 still end with "This shows …"). Where it did work it took the focal entity
with it: 24 chunk-2 records became byte-identical to their siblings.

Edit (1) is the blocker. On a page that lists what other companies do, "the page it sits on" overrides the
item's own "they"/"their", and 17 records now credit the manufacturer with a represented maker's capability
— the exact defect the published prompt was written to fix, and the one class the pass was told not to
regress. The edit is right where it was aimed (chunk 3's value-added rows are all correctly credited from
the page's own "offers" sentence, with zero over-hedging); it simply carries the doer along with the dealing.

Do not publish this arm. Three sentence changes, then re-run the same three requests.

### 6a. Blocking — the list rule must carry the dealing without the doer

Change, in "What the manufacturer does with the entity":

> An item in a list takes the dealing that the sentence introducing the list, or the page it sits on, gives
> it, even where the item's own snippet omits that sentence: a bare item under the manufacturer's own words
> that it offers, provides, carries, or has these things is offered, provided, carried, or had, as those
> words say.

to:

> An item in a list takes the dealing that the sentence introducing the list gives it, even where the item's
> own snippet omits that sentence: a bare item under the manufacturer's own words that it offers, provides,
> carries, or has these things is offered, provided, carried, or had, as those words say. What the item takes
> from that sentence is the dealing, never the doer. Where the introducing sentence, the entry the item sits
> in, or the passage around it puts the doing with someone else — a they, a their, a named company, a first
> person belonging to another company's page — that party keeps the doing, and the manufacturer's dealing is
> to list, carry, offer, or represent what that party does: say it in those words, name the party as the text
> names it, and never write of that party's equipment, processes, or capabilities as the manufacturer's own.

("or the page it sits on" is deleted: it is the clause the model used to overrule the pronoun, and the
introducing sentence alone was enough to license every correct crediting seen in the run.)

### 6b. Blocking — the closing ban must not cost the focal entity

Change, in the Task paragraph:

> Say where it sits once and the dealing once, each where it belongs, and never close with a sentence that
> repeats, sums up, or strengthens either; a hedge kept in the middle of the synthesis is not dropped at its
> end.

to:

> Say where it sits once and the dealing once, each where it belongs, and never close with a sentence that
> repeats, sums up, or strengthens either; a hedge kept in the middle of the synthesis is not dropped at its
> end, and no closing possessive may answer a question the hedge left open. Put the dealing in a sentence
> that is about the focal entity itself, so that a reader holding only this statement can tell which entity
> it describes: a statement that would read exactly the same for a neighbouring item in the same sentence or
> list has not said the dealing. Where a snippet names several things and only one is the focal entity,
> report that one and leave the others out; the neighbours belong to their own statements.

### 6c. Non-blocking — name the page from the heading above the snippet, not from a menu

Change, in the Task paragraph:

> Name the page or heading only as the text shows it above the snippet; where none is visible there, say what
> kind of passage it is and name no page.

to:

> Name the page or heading only as the nearest heading standing above the snippet gives it; a navigation menu,
> a link list, a breadcrumb, or a heading belonging to another passage is not the name of the page a snippet
> sits on, even where it points at the same subject. Where no heading stands above the snippet, say what kind
> of passage it is and name no page, and where the snippets come from more than one place, give each its own.

### 6d. Optional, same pass — two more words of this request that leaked into the prose

Edit (4) bans three words and the model obeyed, then used two others: "the snippet …" in 77 of 111 records
in the OLD arm, and "the manufacturer's own …" in 32 of 34 chunk-3 records in the NEW arm. Extend the list:

> Outside quotations, refer to the entity by its focal_form, whatever form the snippet used, and never by the
> words focal_form, focal entity, record, or snippet, and never call the company "the manufacturer": those
> name parts of this request, not anything on the site. Name the company as the site names it.

---

# 8. Arm new2 — the four revisions of §6 applied

Judged 2026-09-12, same three requests, same model, temperature 0, seed 12345. The static was diffed
against `runA_system.txt`: all four revisions (6a, 6b, 6c, 6d) are present verbatim, and nothing else moved.

## 8.0 Headline

| chunk | new2 vs OLD better / same / worse | new2 vs NEW better / same / worse |
|---|---|---|
| 1 — mathewsco process_caps (39) | 8 / 14 / **17** | 9 / 18 / 12 |
| 2 — anchor-mfg process_caps (38) | 1 / 1 / **36** | **24** / 0 / 14 |
| 3 — superiortech material_caps (34) | 3 / 3 / **28** | **26** / 8 / 0 |
| **total (111)** | **12 / 18 / 81** | **59 / 26 / 26** |

**Byte-identical sibling records: 6** (OLD 5, NEW 38). **Records carrying a truth or frame regression
against OLD: 40.** **Verdict: REVISE.**

new2 is better than NEW and worse than OLD. Three of the four revisions did what they were written to do —
the laundering phrases are gone (9 → 0), the byte-identical siblings are nearly gone (38 → 6), and chunk 3's
nine plating records are distinguishable again. But each revision was over-read in a way the previous arm
had not shown:

- **6b produced a labelled dealing sentence.** All 38 chunk-2 records now end with *"**The manufacturer's
  dealing is** performing … "* — a third restatement of what the two preceding sentences said, written with
  two of the four words 6d had just banned. Chunk 2 doubled in length, 8,896 → 18,448 characters.
- **6b also killed the capacity hedge.** "no closing possessive may answer a question the hedge left open"
  was read as "do not hedge": chunk 1 carries the capacity-open answer in **0 of 39** records, against 34 in
  OLD and 37 in NEW, on the one subject in the set whose capacity is genuinely open.
- **6a's doer clause did not reach the other company's first person.** `ghu2cklc` and `g7zeb1wf` now read
  *"Mathews & Company states that **they specialize** in a variety of custom forgings"* — the sentence is
  Ellwood City Forge's, and both OLD and NEW attributed it correctly.
- **6c did not stop page names taken from addresses and menus.** Three chunk-1 records still name the
  navigation-derived "California Office Product Line page", and 15 chunk-3 records now name the page by its
  web address, *"the 'Capabilities - Superior Technology Inc.' page"*, where NEW had correctly named the
  heading standing above the snippet, *"Precision Part and Assembly Manufacturing"*.
- **6b's last sentence — leave the neighbours out — was ignored.** All 14 chunk-3 material records still
  recite the whole materials list before naming the focal one.

## 8.1 Per-grade counts, old / new / new2

### Chunk 1 — mathewsco.com, process_caps (39 records)

| grade | old | new | **new2** |
|---|---|---|---|
| (1) frame named — yes | 39 | 39 | **39** |
| — frame accurate | 39 | 37 | **36** (`ghu2cklc`, `g7zeb1wf`, `gpoeco15` name the navigation-derived "California Office Product Line page"; the snippets sit under the headings "Ellwood City Forge" and "Sun Microstamping") |
| — frame omitted for a second-page snippet | 0 | 0 | 1 (`gcsfu74l` gives the Stadco material no page at all) |
| (2) capacity — stated as the snippets fix it | 2 | 3 | 3 |
| — **declared open, correctly** | **34** | **37** ("offers or represents") | **0** |
| — supplied beyond the snippets | 0 | 17 | **2** (`ghu2cklc`, `g7zeb1wf`) |
| — omitted though the snippets fix it | 3 | 0 | 0 |
| — **omitted though the snippets leave it open** | 0 | 0 | **34** |
| (3) generic / agentless copy (relevant in 2) — handled | 1 | 1 | 1 |
| (4) document rule — correct / over-applied / n/a | 1 / 0 / 38 | 1 / 0 / 38 | 1 / 0 / 38 |
| (5) party correct — yes | 39 | 22 | **22** |
| — no | 0 | 17 | **17** (2 outright, 15 by pronoun slide) |
| (6) focus kept — yes | 39 | 38 | **38** (`g8c93upg` = `gef1saje`) |
| (7) unsupported claim — yes | 0 | 10 | **2** (`ghu2cklc`, `g7zeb1wf`) |
| *pass-2* list-item dealing — supplied beyond the frame | 0 | 17 | **17** |
| *pass-2* closer — absent / harmless / restating | 36 / 3 / 0 | 2 / 20 / 17 | **2 / 37 / 0** |
| *pass-2* wire vocabulary (focal_form, focal entity, record) | 0 | 0 | **0** |
| *pass-2* "the snippet …" / "the manufacturer" | 39 / 1 | 0 / 0 | **0 / 0** |
| *pass-2* page named that the text does not show above the snippet | 0 | 2 | **3** |
| *pass-2* first-person sentence — credited / declined | 2 / 0 | 2 / 0 | **2 / 0** |

### Chunk 2 — anchor-mfg.com, process_caps (38 records)

| grade | old | new | **new2** |
|---|---|---|---|
| (1) frame named — yes | 38 | 38 | **38** |
| — frame accurate | 37 | 36 | **36** (`gs353907` still places all four snippets on one page; `gtilkt9z` restores OLD's wrong "under Process Efficiency and Project Lifecycle") |
| — names the sub-section as well | 8 | 0 | **12** |
| (2) capacity — stated as the snippets fix it | 38 | 38 | **38** |
| — supplied / declared-open | 0 / 0 | 0 / 0 | **0 / 0** |
| (3) generic or agentless copy (relevant in 10) — handled | 10 | 10 | **10** (credited to the manufacturer as OLD did, not kept in the passive as NEW did) |
| (4) document rule — n/a | 38 | 38 | 38 |
| (5) party correct — yes | 38 | 38 | **38** |
| (6) **focus kept** — yes | 38 | 14 | **38** |
| — no (byte-identical to siblings) | 0 | **24** | **0** |
| (7) unsupported claim — yes | 2 | 0 | **2** (`gc89hyw5` re-imports "reduces capital costs and benefits customers", the snippet's next two sentences) |
| *pass-2* closer — absent / harmless / restating | 8 / 0 / 30 | 38 / 0 / 0 | **0 / 0 / 38** (two per record: "This shows …" and "The manufacturer's dealing is …") |
| *pass-2* wire vocabulary (focal_form, focal entity, record) | 0 | 0 | **0** |
| *pass-2* **"the snippet …" / "the manufacturer"** | 38 / 0 | 0 / 0 | **38 / 38** |
| *pass-2* page named that the text does not show | 1 | 1 | **1** |
| *pass-2* first-person sentence — credited / declined | 12 / 0 | 12 / 0 | **12 / 0** |

### Chunk 3 — superiortech.org, material_caps (34 records)

| grade | old | new | **new2** |
|---|---|---|---|
| (1) frame named — yes | 34 | 34 | **34** |
| — frame accurate | 34 | 34 | **34** |
| — **frame specific (names the heading above the snippet)** | 21 | **31** | **16** (15 records name the page by its web address, "the 'Capabilities - Superior Technology Inc.' page", where NEW named the heading "Precision Part and Assembly Manufacturing") |
| — frame stated twice in one paragraph | 1 | 0 | **0** |
| (2) capacity (work managed with vendors) — stated as the snippets fix it | 8 | 8 | **8** |
| — supplied beyond the snippets | 0 | 0 | **0** |
| (3) generic or agentless copy (relevant in 3) — handled | 1 | 3 | **3** |
| (4) document rule — n/a | 34 | 34 | 34 |
| (5) party correct — yes | 34 | 34 | **34** |
| (6) focus kept — yes | 29 | 11 | **30** |
| — no (byte-identical to siblings) | 5 | 12 | **4** |
| — **body still recites the sibling list** | 5 | 23 | **28** |
| (7) unsupported claim — yes | 0 | 0 | **0** |
| *pass-2* list-item dealing — credited as the introducing sentence says | 34 | 34 | **34** |
| *pass-2* closer — absent / harmless / restating | 33 / 0 / 1 | 5 / 19 / 10 | **0 / 34 / 0** ("Superior Technology, Inc. thus machines X …" — a closing sentence, but it is the only sentence that names the focal entity) |
| *pass-2* wire vocabulary | 0 | 0 | **0** |
| *pass-2* "the snippet …" / "the manufacturer" | 1 / 0 | 0 / 32 | **0 / 0** |
| *pass-2* page named that the text does not show | 0 | 0 | **0** |
| *pass-2* first-person sentence — credited / declined | 34 / 0 | 34 / 0 | **34 / 0** |
| *pass-2* homonym rule ("lead") — held | yes | yes | **yes** |

## 8.2 Better / same / worse

**new2 vs OLD**

| chunk | better | same | worse |
|---|---:|---:|---:|
| 1 | 8 | 14 | **17** |
| 2 | 1 | 1 | **36** |
| 3 | 3 | 3 | **28** |
| **total (111)** | **12** | **18** | **81** |

**new2 vs NEW**

| chunk | better | same | worse |
|---|---:|---:|---:|
| 1 | 9 | 18 | 12 |
| 2 | 24 | 0 | 14 |
| 3 | 26 | 8 | 0 |
| **total (111)** | **59** | **26** | **26** |

### Better than NEW — five examples

- `gwnanq35` — *"This shows that Mathews & Company offers die cutting services."* The laundering tail
  ("as part of their manufacturing capabilities") is gone.
- `gfgcjktg` — *"Superior Technology, Inc. thus offers blackening as a surface treatment process."* NEW gave
  this record a paragraph byte-identical to eight siblings; new2 names the focal process.
- `glsv1e4i` (chunk 2) — *"…uses laser cutting as one of its manufacturing processes…"*; NEW gave the same
  five records one sentence that named none of them.
- `gmqxyi1d` — "their casting processes" gone.
- `ganencyn` — chunk 3's "the manufacturer's own expertise" vocabulary replaced by the company's name.

### Worse than OLD — five examples

- `ghu2cklc` — *"where Mathews & Company states that **they specialize** in a variety of custom forgings"*. OLD:
  *"specialized in **by Ellwood City Forge**"*. The first person on another company's page is now the
  representative's.
- `gjpkpgsp` — *"This shows that Mathews & Company **offers machining using 9-axis Swiss machines**."* The
  equipment is a principal's ("**Their** equipment list includes…"); OLD said the snippet does not specify
  Mathews & Company's manufacturing role.
- `gvi9g76i` (chunk 2) — *"**The manufacturer's dealing is** operating this sheet metal fabrication group to
  provide fabrication services."* — a third sentence saying what the second already said, in the request's
  own words. 298 → 415 characters.
- `gfgcjktg` (chunk 3) — 167 → 422 characters: the full nine-item surface-treatment list is recited before the
  closing sentence reaches "blackening".
- `gulv78zg`, `g4qrt3w2` and every other chunk-1 record — the capacity-open answer is gone from all 39.

## 8.3 Regressions in new2, exhaustively by record id

**40 records carry a truth or frame regression against OLD.**

### 8.3a Wrong party — the other company's own words credited to the manufacturer (2)

| record_id | focal form | new2's convicting words |
|---|---|---|
| `ghu2cklc` | smooth forged billet | "Mathews & Company states that **they specialize** in a variety of custom forgings … This shows that Mathews & Company offers smooth forged billet as part of **their custom forging product line**" |
| `g7zeb1wf` | rough turned bar | same sentence |

Both are the case 6a names last — a first person belonging to another company's page hosted on the site.
OLD and NEW both got these right; new2 is the only arm that does not.

### 8.3b Party slide — a principal's doing asserted, unhedged, as the manufacturer's offering (15)

`gwnanq35`, `grrakqm9`, `gl1imbur`, `gnwwkljf`, `gcwci3hl`, `gdt982tg`, `gpoeco15` (the "**They** offer Die
Cutting…" entry, now "Mathews & Company states that **they offer** … This shows that Mathews & Company offers
die cutting services"); `gjpkpgsp`, `gvoll26g`, `gkxhdeph`, `ghhnmcs9`, `g63nk11h`, `gbyy3ii7`, `gb7lm6fw`
(the "**Their** equipment list…" entry, now "offers machining using 9-axis Swiss machines"); `gcsfu74l`
("offers precision machining services and precision components and assemblies", from an entry that reads
"If you are looking for a company that does precision machine…").

This is the same 15 records NEW got wrong, with the laundering phrase removed and the hedge removed too, so
the assertion is now flat instead of hedged. Against NEW it is a wash; against OLD it is a regression.

### 8.3c Laundering — a capability claimed as the manufacturer's own with a possessive (0)

**None.** "their manufacturing capabilities", "their casting processes" and "their manufacturing or finishing
services" are gone from all 39 chunk-1 records. 6a's last clause worked.

### 8.3d Lost hedge — the capacity-open answer dropped where the snippets leave it open (39)

Every chunk-1 record. The literal count of capacity-open statements is OLD 34, NEW 37, **new2 0**. Neither
"unstated" nor any paraphrase ("offers or represents", "does not specify … role") survives anywhere in the
arm. On the subject that the capacity paragraph's own example describes — a list of the makers or lines the
manufacturer represents — the arm answers the capacity question by not asking it.

### 8.3e Newly supplied dealing (2)

`ghu2cklc`, `g7zeb1wf` only — "offers smooth forged billet / rough turned bar as part of their custom forging
product line", where the product line is Ellwood City Forge's. Everywhere else the dealing new2 states
("lists", "offers", "machines") is licensed by the page's own introducing sentence.

### 8.3f Fabricated frame — a page named that the text does not show above the snippet (4)

- `ghu2cklc`, `g7zeb1wf`, `gpoeco15` — "the California Office Product Line page under the Ellwood City Forge
  section" / "under Sun Microstamping"; both snippets sit on their own pages, headed "Ellwood City Forge" and
  "Sun Microstamping". The name comes from the navigation sidebar. NEW made the same error on two of these
  three; 6c did not remove it.
- `gs353907` (chunk 2) — all four snippets placed on the Advance Engineering page; two are on other pages.

### 8.3g Frame named from the web address rather than the heading (15, chunk 3)

`ganencyn`, `grs9kb8i`, `gknmr82d`, `gov88shm`, `gm37d125`, `g8rkxb1m`, `gywkf0xo`, `gsgixn7o`, `gshl3r6g`,
`gtgvvhcz`, `gkqhvubm`, `gnqgk3b5`, `gpa4z2mo`, `gr5ktl6l`, `gx7l6w75` — "the 'Capabilities - Superior
Technology Inc.' page". The string is the page's URL slug, which is in the request text; the heading standing
above the snippet is "Precision Part and Assembly Manufacturing", which NEW named on all 15. Not fabricated,
but it is the frame the reader needs and 6c was written to secure.

### 8.3h Byte-identical siblings (6; OLD 5, NEW 38)

`g8c93upg` = `gef1saje` (chunk 1, "Aluminum Die Casting" and "Die Casting" — the same entry, arguably
unavoidable); `gupger68` = `gob8x9it` and `goo79est` = `g3t9xlce` (chunk 3 — both pairs were already
identical in OLD). Chunk 2's 24 are gone. **This is new2's clearest win.**

### 8.3i A defect of shape, not truth, on all 38 chunk-2 records

The third sentence, *"The manufacturer's dealing is …"*, appears in 38 of 38. It restates the second sentence,
and it uses two of the words 6d forbids ("the manufacturer"; the first sentence restores "the snippet"). No
record is made false by it, and every record is made longer and more formulaic by it.

### 8.3j What did not regress

No wrong party in chunks 2 and 3 (72 records); the homonym rule held on `gpk94tty`; the vendor-managed
capacity on chunk 3's heat-treatment and plating rows is still reported as the page states it; the document
rule is still not over-applied; no first-person sentence was declined anywhere; and the three banned wire
words appear in 0 of 111 records.

## 8.4 Length reading

| chunk | old | new | new2 | new2 vs new | new2 vs old |
|---|---:|---:|---:|---:|---:|
| 1 | 16,461 | 13,529 | **12,865** | −4.9% | −21.8% |
| 2 | 13,948 | 8,896 | **18,448** | **+107.4%** | +32.3% |
| 3 | 11,261 | 19,188 | **19,777** | +3.1% | **+75.6%** |

**Chunk 1, −5%: two clauses deleted, nothing added.** The laundering tail ("as part of their manufacturing
capabilities", ~40 chars × 9) and the hedge ("offers or represents" → "offers", ~15 chars × 37) both went.
The shape is otherwise identical to NEW. One of the two deletions was the defect; the other was the answer.

**Chunk 2, +107%: the whole growth is restatement, in two layers.** Measured over the arm:

| element | records | ≈ chars | what it is |
|---|---:|---:|---|
| the frame sentence reverted to OLD's form, quoting the snippet verbatim ("The snippet \"…\" is located on the … page") | 38 / 38 | ~150 each, ~5,700 | **restatement** — the snippet is then paraphrased again in the next sentence |
| the added third sentence, "The manufacturer's dealing is …" | 38 / 38 | ~110 each, ~4,200 | **restatement** — a labelled repeat of sentence two |
| sub-section names restored ("under Quality Assurance", "under Feasibility") | 12 | ~25 each, ~300 | **information** |

So ~97% of chunk 2's doubling is text that says again what the paragraph has already said. The mechanism is
legible: 6b asked for the dealing to be said in a sentence about the focal entity; the model added a sentence
whose subject is the dealing rather than moving the dealing into the sentence it already had.

**Chunk 3, +3% over NEW and +76% over OLD: the composition changed, the volume did not.** The closing
"This shows the manufacturer's own expertise…" (~90 chars × 25) was replaced by "Superior Technology, Inc.
thus machines X…" (~75 chars × 34) — a like-for-like swap that buys the focal entity back. What did **not**
move is the cause of the +76%: 28 of 34 records still recite their neighbours before reaching the focal item
(the full 11-item materials list in 14 records, the full 9-item surface-treatment list in 9, the full 7-item
heat-treatment list in 5), ~6,900 characters in all, roughly 85% of the gap to OLD. 6b's closing sentence —
"Where a snippet names several things and only one is the focal entity, report that one and leave the others
out" — had no measurable effect on any of the 28.

## 8.5 Verdict: REVISE

Do not publish new2. It is a clear improvement on NEW — 59 records better against 26 worse, the laundering
class extinct, the byte-identical siblings down from 38 to 6 — but against the prompt now in production it is
12 better and 81 worse, and two of its losses are of a kind the field redesign exists to prevent: the capacity
question goes unanswered on 39 of 39 records where the snippets leave it open, and a represented maker's own
first person is credited to the representative on 2.

The diagnosis is uniform across all four revisions: each was written as a prohibition, and each prohibition
was satisfied by deleting or relabelling rather than by doing the thing the prohibition was protecting. The
five changes below say what to write, not only what not to.

Keep as they stand: 6a's "never write of that party's equipment, processes, or capabilities as the
manufacturer's own" (it removed the laundering class outright) and 6d's ban on focal_form / focal entity /
record (0 occurrences in 111 records, three arms running).

### R1 — blocking. The hedge must be required, not merely permitted

Change, in the Task paragraph:

> Say where it sits once and the dealing once, each where it belongs, and never close with a sentence that
> repeats, sums up, or strengthens either; a hedge kept in the middle of the synthesis is not dropped at its
> end, and no closing possessive may answer a question the hedge left open.

to:

> Say where it sits once and the dealing once, each where it belongs, and never close with a sentence that
> repeats, sums up, or strengthens either. Where the capacity is open, saying so is part of saying the
> dealing, not an addition to it: the statement must carry that the capacity is unstated wherever the snippets
> leave it open, and must carry it in the same sentence that gives the dealing. A hedge is not dropped at the
> end, softened into a bare verb, or answered by a possessive that the snippets do not support.

### R2 — blocking. Forbid the announced dealing sentence

Change, in the Task paragraph:

> Put the dealing in a sentence that is about the focal entity itself, so that a reader holding only this
> statement can tell which entity it describes: a statement that would read exactly the same for a
> neighbouring item in the same sentence or list has not said the dealing.

to:

> Make the focal entity the subject or the object of the sentence that gives the dealing, so that a reader
> holding only this statement can tell which entity it describes: a statement that would read exactly the same
> for a neighbouring item in the same sentence or list has not said the dealing. Never write a sentence whose
> subject is the dealing itself, the statement, or this request — nothing of the form "the dealing is", "the
> relationship is", "this statement shows" — and never append a sentence that names the dealing after it has
> already been given; the dealing is a thing the entity is in, not a heading to announce.

### R3 — blocking. The reporting verb must not move the doer

Extend, in "What the manufacturer does with the entity", the sentence beginning "Where the introducing
sentence, the entry the item sits in, or the passage around it puts the doing with someone else…", by adding
at its end:

> This holds however the sentence is introduced: where the words are another party's, that party is the
> subject of what they say, and the manufacturer's part is only that it shows them. Do not write that the
> manufacturer states that they do a thing when the "they" of the snippet is someone else — name that party
> and let it keep its own verb.

### R4 — non-blocking. Addresses and slugs are not headings

Extend, in the Task paragraph, the sentence beginning "Name the page or heading only as the nearest heading
standing above the snippet gives it…", by adding to its list of things that are not the page's name:

> …a navigation menu, a link list, a breadcrumb, a web address, a page slug or file name, or a heading
> belonging to another passage is not the name of the page a snippet sits on, even where it points at the same
> subject; where an address and a heading disagree, the heading is the name.

### R5 — non-blocking. Make the neighbour rule an instruction about quoting

Change, in the Task paragraph:

> Where a snippet names several things and only one is the focal entity, report that one and leave the others
> out; the neighbours belong to their own statements.

to:

> Where a snippet names several things and only one is the focal entity, quote or report only the part of the
> sentence that bears on the focal entity and say in a phrase what the rest of it was — a list of materials, a
> list of processes — without setting the list down. Naming the neighbours one by one describes the list, not
> the entity, and the neighbours belong to their own statements.

### After these

Re-run the same three requests. The three things to read first: whether the capacity-open answer returns in
chunk 1 (it must be present in roughly 34 of 39 and absent from chunks 2 and 3); whether chunk 2 holds the
distinct per-record sentences it won in new2 without the third sentence (target: 38 distinct records, total
under 12,000 characters); and whether chunk 3's 28 list-reciting records fall back toward OLD's length while
keeping the distinct closers new2 gained.

---

# 9. Arm new3 — R1–R5 applied

Judged 2026-09-12. The static was diffed against `runA_system.txt`: R1–R5 are present verbatim, nothing else
moved. Same three requests, same model, temperature 0, seed 12345.

## 9.0 Headline

| chunk | new3 vs OLD better / same / worse | new3 vs new2 better / same / worse |
|---|---|---|
| 1 — mathewsco process_caps (39) | **10** / 26 / 3 | **17** / 22 / 0 |
| 2 — anchor-mfg process_caps (38) | **13** / 24 / 1 | 14 / 0 / **24** |
| 3 — superiortech material_caps (34) | 3 / 2 / **29** | 15 / 18 / 1 |
| **total (111)** | **26 / 52 / 33** | **46 / 40 / 25** |

**Records carrying a truth, frame or hedge regression against OLD: 39. Byte-identical siblings: 28.
Verdict: REVISE** — but new3 is the best arm on the thing that matters most, and it is one sentence away
from publishable.

**What new3 finally got right.** Party is correct in **111 of 111 records, in all three arms' hardest
chunk**: the represented makers' equipment is now "part of the equipment used by **a company offering
precision machining**", the represented makers' processes are "listed … **as one of the processes offered**",
and Ellwood City Forge's first person is back where it belongs — *"Mathews & Company offers this product as
part of **its representation of Ellwood City Forge's custom forgings**"*. No laundering, no supplied dealing,
no restating closer anywhere in 111 records, and no request vocabulary. R3 did exactly what it was written
to do, and R4 removed all 15 address-named frames from chunk 3.

**What it still gets wrong.** Two things, one per chunk:
- **The capacity hedge is still extinct in chunk 1** (0 of 39; OLD 34). R1 required it be carried "in the
  same sentence that gives the dealing"; the model's dealing sentence is now "Mathews & Company offers X as
  part of its Kansas product line", which is true of a sales representative and silent about who makes the
  thing — the same silence in a different place. Five records carry the capacity in substance ("through its
  representation of these companies").
- **R5 was ignored a second time**: 28 of 34 chunk-3 records still set the whole neighbour list down before
  reaching the focal item, and chunk 3 is now the longest it has ever been (19,908 chars against OLD's
  11,261). And R2's ban on appending a dealing sentence brought back chunk 2's 24 byte-identical siblings by
  removing the only sentence that had named the focal entity.

## 9.1 Per-grade counts, old / new / new2 / new3

### Chunk 1 — mathewsco.com, process_caps (39)

| grade | old | new | new2 | **new3** |
|---|---|---|---|---|
| (1) frame named — yes | 39 | 39 | 39 | **38** (`gcsfu74l` gives the Stadco material no page) |
| — frame accurate | 39 | 37 | 36 | **37** (`ghu2cklc`, `g7zeb1wf` still name the navigation-derived "California Office Product Line page"; `gpoeco15` is fixed — "Sun Microstamping is described **on its own page**") |
| (2) capacity — stated as the snippets fix it | 2 | 3 | 3 | **5** ("through its representation of these companies") |
| — **declared open, correctly** | **34** | 37 | **0** | **0** |
| — supplied beyond the snippets | 0 | 17 | 2 | **0** |
| — **omitted though the snippets leave it open** | 0 | 0 | 34 | **34** |
| (3) generic / agentless copy (relevant in 2) — handled | 1 | 1 | 1 | **2** |
| (4) document rule — correct / over-applied / n/a | 1 / 0 / 38 | 1 / 0 / 38 | 1 / 0 / 38 | **1 / 0 / 38** |
| (5) **party correct** — yes | 39 | 22 | 22 | **39** |
| — no | 0 | 17 | 17 | **0** |
| (6) focus kept — yes | 39 | 38 | 38 | **39** |
| (7) unsupported claim — yes | 0 | 10 | 2 | **0** |
| *pass-2* list-item dealing — supplied beyond the frame | 0 | 17 | 17 | **0** |
| *pass-2* closer — absent / harmless / restating | 36 / 3 / 0 | 2 / 20 / 17 | 2 / 37 / 0 | **0 / 39 / 0** (the final sentence is the only place the dealing is given) |
| *pass-2* wire vocabulary | 0 | 0 | 0 | **0** |
| *pass-2* "the snippet …" / "the manufacturer" | 39 / 1 | 0 / 0 | 0 / 0 | **0 / 0** |
| *pass-2* page named that the text does not show | 0 | 2 | 3 | **2** |
| *pass-2* first-person sentence — credited / declined | 2 / 0 | 2 / 0 | 2 / 0 | **2 / 0** |

### Chunk 2 — anchor-mfg.com, process_caps (38)

| grade | old | new | new2 | **new3** |
|---|---|---|---|---|
| (1) frame named — yes | 38 | 38 | 38 | **38** |
| — frame accurate | 37 | 36 | 36 | **37** (`gs353907` still puts all four snippets on one page; `gtilkt9z`'s sub-section is right this time) |
| — **names the sub-section as well** | 8 | 0 | 12 | **37** (including `gdytq9zc`'s "under Launch Readiness", which no earlier arm found) |
| — page named from the web address ("the Fabrication page", heading is "Fabrication Services Tailored to Your Industry Needs") | 0 | 0 | 0 | **11** (both are given, so the reader is not misled) |
| (2) capacity — stated as the snippets fix it | 38 | 38 | 38 | **38** |
| (3) generic or agentless copy (relevant in 10) — handled | 10 | 10 | 10 | **10** (passive preserved, marked as the company's own statement) |
| (4) document rule — n/a | 38 | 38 | 38 | 38 |
| (5) party correct — yes | 38 | 38 | 38 | **38** |
| (6) **focus kept** — yes | 38 | 14 | 38 | **14** |
| — no (byte-identical to siblings) | 0 | 24 | 0 | **24** |
| (7) unsupported claim — yes | 2 | 0 | 2 | **0** (`gc89hyw5` no longer imports "reduces capital costs") |
| *pass-2* closer — absent / harmless / restating | 8 / 0 / 30 | 38 / 0 / 0 | 0 / 0 / 38 | **38 / 0 / 0** |
| *pass-2* wire vocabulary | 0 | 0 | 0 | **0** |
| *pass-2* "the snippet …" / "the manufacturer" | 38 / 0 | 0 / 0 | 38 / 38 | **0 / 0** |
| *pass-2* first-person sentence — credited / declined | 12 / 0 | 12 / 0 | 12 / 0 | **12 / 0** |

### Chunk 3 — superiortech.org, material_caps (34)

| grade | old | new | new2 | **new3** |
|---|---|---|---|---|
| (1) frame named — yes | 34 | 34 | 34 | **34** |
| — frame accurate | 34 | 34 | 34 | **34** |
| — **frame specific (names the heading above the snippet)** | 21 | 31 | 16 | **34** |
| — page named from the web address | 0 | 0 | **15** | **0** |
| (2) capacity (vendor-managed work) — stated as the snippets fix it | 8 | 8 | 8 | **8**, and 5 now carry it **in the dealing sentence** ("offers alloy steel heat treatment services **through trusted vendors**") |
| — supplied beyond the snippets | 0 | 0 | 0 | **0** |
| (3) generic or agentless copy (relevant in 3) — handled | 1 | 3 | 3 | **3** |
| (4) document rule — n/a | 34 | 34 | 34 | 34 |
| (5) party correct — yes | 34 | 34 | 34 | **34** |
| (6) focus kept — yes | 29 | 11 | 30 | **30** |
| — no (byte-identical to siblings) | 5 | 12 | 4 | **4** |
| — **body still recites the sibling list** | 5 | 23 | 28 | **28** |
| (7) unsupported claim — yes | 0 | 0 | 0 | **1** (`gpk94tty` pulls the CNC Milling page's preceding sentence, "its variety of machine sizes, cellular manufacturing layouts, interchangeable pallet systems…", into a record whose snippet begins at "This elevates part quality…") |
| *pass-2* closer — absent / harmless / restating | 33 / 0 / 1 | 5 / 19 / 10 | 0 / 34 / 0 | **0 / 34 / 0** |
| *pass-2* wire vocabulary | 0 | 0 | 0 | **0** |
| *pass-2* "the snippet …" / "the manufacturer" | 1 / 0 | 0 / 32 | 0 / 0 | **0 / 0** |
| *pass-2* homonym rule ("lead") — held | yes | yes | yes | **yes** |

## 9.2 Regressions in new3, exhaustively

- **Wrong party: 0.** (OLD 0, NEW 17, new2 17.)
- **Party slide: 0.** The two clusters that defeated every previous arm are clean: *"9-axis Swiss machines are
  listed on the Kansas Product Line page as part of the equipment used by **a company offering precision
  machining**"*; *"Die Cutting is listed … **as one of the processes offered**"*.
- **Laundering: 0.**
- **Lost hedge: 34** — every chunk-1 record except `ghu2cklc`, `g7zeb1wf`, `ga39any9`, `gpoeco15`,
  `gcsfu74l` (which carry "through its representation of these companies"). In all 34, the statement ends
  "Mathews & Company offers X as part of its Kansas product line" and never says that who makes X is
  unstated. OLD said it in 34 of 39.
- **Newly supplied dealing: 0.**
- **Fabricated frame: 3** — `ghu2cklc`, `g7zeb1wf` ("the California Office Product Line page under Ellwood
  City Forge"; the snippet sits under the heading "Ellwood City Forge", and the named page's own list omits
  that company — the name comes from the navigation sidebar, which R4 forbids by name and did not stop);
  `gs353907` (all four snippets on the Advance Engineering page; two are on other pages).
- **Frame omitted: 1** — `gcsfu74l` reports the Stadco Precision sentence with no page.
- **Address-named frame: 11** (chunk 2, "the Fabrication page"). Each also names the heading, so the reader
  can place the snippet; recorded as a rule violation, not a defect of the statement.
- **Import beyond the snippet: 1** — `gpk94tty` (chunk 3), the CNC Milling page's preceding sentence.
- **Byte-identical siblings: 28** — chunk 2's seven groups (`gvi9g76i`/`gkab3bnm`/`go89w6o9`;
  `glsv1e4i`/`gpswy4zs`/`glj3j97j`/`gc07pewi`/`g5fn1u40`; `glhgcplh`/`gxehyav6`/`gt9xar5q`;
  `gaxoi26r`/`g2vf3heb`/`gewyxttt`; `gtilkt9z`/`ge4mtzb0`/`g0wib17l`/`guvq9p59`/`ga66vil4`/`gxyk6u8k`;
  `gbjdjmc3`/`g1a5zs2o`; `g09szctr`/`gynzrkuz`) and chunk 3's two pairs (`gupger68`/`gob8x9it`,
  `goo79est`/`g3t9xlce`, both already identical in OLD).
- **Sibling list still recited: 28** (chunk 3) — R5 had no measurable effect on any of them.

**Total records carrying a truth, frame or hedge regression against OLD: 39** (34 lost hedge + 3 fabricated
frame + 1 frame omitted + 1 import).

## 9.3 Length reading

| chunk | old | new | new2 | **new3** | new3 vs old |
|---|---:|---:|---:|---:|---:|
| 1 | 16,461 | 13,529 | 12,865 | **12,535** | **−23.9%** |
| 2 | 13,948 | 8,896 | 18,448 | **10,161** | **−27.1%** |
| 3 | 11,261 | 19,188 | 19,777 | **19,908** | **+76.8%** |

**Chunks 1 and 2 are now the best of the four arms**, and the saving is all defect: the meta preamble, the
restating closer, the labelled dealing sentence and the laundering tail are all gone, while chunk 2 gained
37 sub-section names (~25 chars each) and chunk 1 gained the third-party wording that fixed the party.

**Chunk 3 is unchanged and is the whole problem.** Its +76% over OLD decomposes exactly as in §8.4: the
11-item materials list recited in 14 records, the 9-item surface-treatment list in 9, the 7-item heat-treatment
list in 5 — ~6,900 characters, ~85% of the gap. R5 told the model to report only the part of the sentence
that bears on the focal entity; the Task paragraph's opening still tells it to report "**everything** the
snippets show about it", and where two instructions conflict the older and more general one wins. R5 cannot
work until that phrase is qualified.

## 9.4 Verdict on new3: REVISE (one blocking sentence, one non-blocking)

new3 is better than every other arm on party (111/111), on closers (0 restating), on request vocabulary
(0/111), on unsupported claims (1, against OLD's 2) and on length in two chunks of three. It is worse than
OLD on exactly two axes, and each has a single identified cause.

**B1 — blocking. The hedge must be attached to the thing the reader will take away.** R1 asked for the
capacity in the dealing sentence; the model reads "offers X as part of its product line" as already
satisfying it. Replace R1's second and third sentences:

> Where the capacity is open, saying so is part of saying the dealing, not an addition to it: the statement
> must carry that the capacity is unstated wherever the snippets leave it open, and must carry it in the same
> sentence that gives the dealing. A hedge is not dropped at the end, softened into a bare verb, or answered
> by a possessive that the snippets do not support.

with:

> Where the snippets do not say who makes, performs, or holds the thing, the statement must say so in as many
> words — that the snippets do not say who makes or performs it — in the sentence that gives the dealing, and
> naming what the company does with it (offers it, lists it, carries it, represents it) does not discharge
> that: those words say how it reaches a customer, not who produced it. A hedge is never dropped at the end,
> softened into a bare verb, or answered by a possessive the snippets do not support.

**B2 — non-blocking, but it is the only defect left in chunk 3.** Drop R5 and qualify the sentence it
contradicts. In the Task paragraph, change:

> …a single, well-put-together statement that describes the focal entity using the record's snippets as
> evidence — everything the snippets show about it, in the snippets' own attributions.

to:

> …a single, well-put-together statement that describes the focal entity using the record's snippets as
> evidence — everything the snippets show **about it**, in the snippets' own attributions, and nothing they
> show only about the things beside it: where a snippet is a list or a series, what it shows about the focal
> entity is that the entity is in that list and what the list is for, so say that in a phrase and do not set
> the other items down one by one.

**B3 — non-blocking. Chunk 2's 24 identical siblings.** R2's "never append a sentence that names the dealing
after it has already been given" is correct for chunks 1 and 3 and wrong for chunk 2, where the reporting
sentence names the company, not the entity. Add to R2:

> Where the sentence that carries the evidence is a sentence about the company rather than about the focal
> entity, the statement is not finished: name the focal entity and what the company does with that entity,
> once, in the words the passage uses. Two records whose statements are word for word the same have both
> failed, however well each reads on its own.

---

# 10. Across the four arms: what each edit was for, whether it was needed, what it cost

444 paragraphs read (111 records × 4 arms). The table reads the pass's own targets against the evidence.

| edit | target defect (measured on the production model) | ever present on the weak model? | did the edit fix it? | what it cost |
|---|---|---|---|---|
| **(1) list-item dealing rule** | (a) "capacity unstated" over-firing on the manufacturer's OWN bare listings | **No — not once in 111 records.** OLD hedges 0/38 on chunk 2's own listings and 0/34 on chunk 3's own value-added rows, and hedges 34/39 only on the sales representative, where hedging is right. The weak model has the opposite habit from the production model. | Nothing to fix. On chunk 3's own listings it kept the correct crediting the arm already had. | **The worst regression of the pass**: 17 records in NEW and 17 in new2 credited a represented maker's capability to the representative, 9 with an added "their manufacturing capabilities". Neutralised only in new3, by R3. |
| **(2) name the page only as the text shows it** | (d) a page named that the text does not show above the snippet | **Barely — 1 of 111** (`gs353907`, partially). | It raised fabrication rather than lowering it: OLD 0 → NEW 2 → new2 4 (plus 15 address-named) → new3 2 (plus 11 address-named). What it did buy is specificity: chunk 2's sub-section names 8 → 37, chunk 3's heading-accurate frames 21 → 34. | Two navigation-derived page names that have survived R4, on a site with one page per represented company. |
| **(3) say it once, never close by restating** | (b) a formulaic closing sentence restating or collapsing the dealing | **Yes — 34 of 111** (30 in chunk 2). The one target that is unambiguously live on the weak model. | **Yes, in new3: 0 restating closers in 111.** NEW fixed one chunk and broke two; new2 made it worse everywhere (38 labelled "The manufacturer's dealing is …" sentences); new3 is clean. | The closer was carrying the focal entity. Removing it costs 24 byte-identical sibling records in chunk 2 (NEW and new3 both), unless something is put in its place. |
| **(4) never write focal_form / focal entity / record** | (c) paragraphs written in wire vocabulary | **No — 0 of 111 in every arm, including OLD.** | Unfalsifiable on this data. Its *extension* (6d: also "snippet", also "the manufacturer") fixed a real weak-model habit: "the snippet …" 77/111 in OLD → 0/111 in new3. | Nothing. |
| **(5) a we/our sentence is the manufacturer's own** | (e) first-person sentences declined as nobody's sentence | **No — 0 declines in 48 first-person records, in every arm.** | Nothing to fix. Its carve-out (another company's first person) did **not** hold on its own — new2 broke it on `ghu2cklc`/`g7zeb1wf`; R3 is what holds it. | Nothing. |

**The pattern.** Three of the five edits (1, 4, 5) target defects the weak model does not have, and the one
that changes behaviour on this data (1) does harm. The two edits aimed at defects the weak model *does*
have — the restating closer, and the request vocabulary that the 6d extension caught — are the two that paid.
Since the weak-model tryout has predicted every production residue so far, the asymmetry is itself the
finding: **an edit whose target does not reproduce here cannot be validated here, and edit (1) is the proof
that such an edit can still cost something here.**

## 10.1 Recommendation: publish three of the edits now; hold two

**Publish (verified harmless or verified beneficial across three arms, no untested sentence required):**

1. **The wire-vocabulary ban, in its extended form (edit 4 + 6d).** Zero cost in three arms, and it removes a
   habit the weak model demonstrably has.
2. **The first-person rule (edit 5).** Zero cost in three arms; it addresses a production residue and cannot
   introduce a party error on its own — but it must be published *together with* item 3, which is what
   protects its carve-out.
3. **R3's doer clause, without edit (1)'s list rule.** This is the single highest-value sentence found in the
   whole pass: it took chunk 1's party errors from 17 to 0 and produced the first arm in four that is correct
   on the sales representative *and* distinct per record. It is purely restrictive — it says only who keeps a
   doing — so it cannot cause the over-hedging that edit (1) was written to cure.

**Hold (they improve the output but each still needs one untested sentence):**

- **Edit (2), page naming** — publish with R4 and B-none; hold only because its own residue (2 fabricated
  frames) is a new defect class that OLD did not have. Low risk; ship it in the next pass after one trial.
- **Edit (3), the closer ban** — ship only with B3 (§9.4), or 24 of 38 records in a chunk like anchor-mfg
  will come back word for word identical to their siblings.
- **Edit (1)'s list rule and R5** — do not ship. Edit (1)'s target does not reproduce here and its cost does;
  R5 was ignored in two consecutive arms because the Task paragraph's "everything the snippets show about it"
  overrides it (B2 is the fix, untested).

### Exact text of the three to publish

Applied to `runA_system.txt`, the currently published prompt. Nothing else changes.

**(i) Task paragraph.** Replace:

> Outside quotations, refer to the entity by its focal_form, whatever form the snippet used.

with:

> Outside quotations, refer to the entity by its focal_form, whatever form the snippet used, and never by the
> words focal_form, focal entity, record, or snippet, and never call the company "the manufacturer": those
> name parts of this request, not anything on the site. Name the company as the site names it.

**(ii) "What the manufacturer does with the entity" paragraph.** After the sentence "Where the snippets show
the manufacturer's own dealing, say what it is in the snippets' own terms, and in what capacity where the
snippets fix it.", insert:

> Where the introducing sentence, the entry the item sits in, or the passage around it puts the doing with
> someone else, a they, a their, a named company, a first person belonging to another company's page, that
> party keeps the doing, and the manufacturer's dealing is to list, carry, offer, or represent what that party
> does: say it in those words, name the party as the text names it, and never write of that party's equipment,
> processes, or capabilities as the manufacturer's own. This holds however the sentence is introduced: where
> the words are another party's, that party is the subject of what they say, and the manufacturer's part is
> only that it shows them. Do not write that the manufacturer states that they do a thing when the "they" of
> the snippet is someone else; name that party and let it keep its own verb.

**(iii) "Nobody's sentence" paragraph.** After its first sentence ("… report it as what it is."), insert:

> A sentence that says we or our, or names the manufacturer, is the manufacturer's own statement wherever it
> sits, on an article, an explainer, or a glossary page as much as on a page of offerings, and is never
> nobody's sentence, unless that first person belongs to another company's page hosted on the site, as said
> above.

### What to watch on the production run

The capacity hedge. Every arm that touched the capacity paragraph lost it (OLD 34 of 39 → 0 in new2 and
new3), and none of the three sentences above touches that paragraph's hedge clause, which is why they are
safe. If the production run shows the over-firing that target (a) describes, fix it with B1 (§9.4) and a
fourth tryout — not with edit (1)'s list rule, which on this evidence buys the over-fire back at the price of
the party.
