# JUDGE_RUN_A — field-requirements synthesis prompt, old vs new

Judged 2026-09-11. Three real synthesis requests (`docs_local/synthesis_shape_tryout_20260909/{1,2,3}/A_user.txt`),
one weak model (gpt-4.1-mini, temperature 0, seed 12345), same user block, only the system prompt differs.

- **old** = the published static (`.../synthesis_shape_tryout_20260909/1/D_system.txt`)
- **new** = `apps/.../4_phrase_synthesis/product_phrase_synthesis.txt` (frame-naming, capacity-unstated,
  frame-dependent document rule, the five shape rules, the homonym rule)

| chunk | subject | field | records |
|---|---|---|---|
| 1 | mathewsco.com — Mathews & Company | process_caps | 39 |
| 2 | anchor-mfg.com — Anchor Manufacturing Group, Inc. | process_caps | 38 |
| 3 | superiortech.org — Superior Technology, Inc. | material_caps | 34 |

**The one fact that governs chunk 1.** Mathews & Company is not a manufacturer. Its home page says
"Mathews & Company only represent companies who are interested in a long term relationship"; the Kansas
and California offices are described as "a sales representative's organization"; the "Kansas Product Line"
and "California Office Product Line" pages list the offerings of its principals (Ellwood City Forge,
Hudson Technologies, Sun Microstamping, Rowley Spring & Stamping, STADCO Precision, unnamed casters and
stampers). The copy on those pages keeps the principals' pronouns — "**They** offer Die Cutting Water Jet
Cutting…", "**Their** equipment list includes 9-axis Swiss machines…" — and the per-principal pages carry
that company's own first person ("**We** specialize in a variety of custom forgings", on the page headed
*Ellwood City Forge*). Whose dealing each snippet shows is therefore the live question in chunk 1, and it
is where the new prompt loses.

---

## 1. Per-grade counts, old vs new

Grades are my per-record reading of each paragraph against the record's snippets and the surrounding page.
Where a count is a judgement call at the margin I have said so.

### Chunk 1 — mathewsco.com, process_caps (39 records)

| grade | old | new |
|---|---|---|
| **(1) frame named** — yes | 36 | 39 |
| — no | 3 (`ghu2cklc`, `g7zeb1wf`, `ga39any9`) | 0 |
| — *of those, the reader can tell it is a **place in the text*** | ~6 (rest say only "as part of the Kansas Product Line", a category) | 39 (names a page, and for `g8rnu8jo` the Line Card) |
| **(2) capacity** — stated as the snippets fix it | 21 | 23 |
| — declared unstated correctly | 0 | 0 |
| — supplied beyond the snippets | 0 | **16** |
| — omitted though the snippets fix it | 18 | 0 |
| **(3) generic / agentless copy** (relevant in 2: `ga39any9` S5, `g8nedk2z`) — handled | 1 | 2 |
| — attributed to the manufacturer wrongly | 1 | 0 |
| — n/a | 37 | 37 |
| **(4) document rule** — applied correctly | 1 (`g8rnu8jo`, Line Card read as a list of companies, not a library) | 1 |
| — over-applied | 0 | 0 |
| — n/a | 38 | 38 |
| **(5) party correct** — yes | 21 | 23 |
| — no | 0 | **16** |
| — left unnamed | 18 | 0 |
| **(6) focus kept** — yes | 16 | 27 |
| — no (paragraph's body is the neighbour list) | 23 | 12 |
| **(7) claim not supported by the snippets** — yes | 1 | 2 outright + 5 context-derived ("represented by Mathews & Company") |

### Chunk 2 — anchor-mfg.com, process_caps (38 records)

| grade | old | new |
|---|---|---|
| **(1) frame named** — yes | **0** | **38** |
| — no | 38 | 0 |
| — *frame accurate where named* | — | 37 of 38 (`gc89hyw5` names Assembly & Automation only; the sentence also stands on the Welding page, and neither arm notes the recurrence) |
| **(2) capacity** — stated as the snippets fix it | 38 | 38 |
| — supplied / omitted / declared-unstated | 0 / 0 / 0 | 0 / 0 / 0 |
| **(3) generic or agentless copy** (relevant in 7: the agentless "Manufacturing processes use…", the passive "Assembly is performed…", "…are assembled") — handled | 7 | 7 |
| — attributed wrongly | 0 | 0 |
| **(4) document rule** — n/a | 38 | 38 |
| **(5) party correct** — yes | 38 | 38 |
| **(6) focus kept** — yes | 20 | 35 |
| — no (sibling records share one restated sentence) | 18 | 3 |
| **(7) unsupported claim** — yes | 0 | 1 borderline (`gdytq9zc` "during product launches", taken from the *Launch Readiness* heading over the snippet — placement, not a new claim) |

### Chunk 3 — superiortech.org, material_caps (34 records)

| grade | old | new |
|---|---|---|
| **(1) frame named** — yes | 30 | 34 |
| — no | 4 | 0 |
| — *frame accurate* | **16** | **34** |
| — *frame fabricated* | **14** — cites an "about us" page (14×) and a "facilities list" page (3×); the material sentence "We have vast experience in the precision machining close tolerance parts…" occurs exactly once in the text, on `/capabilities-superior-technology-inc/` ("# Precision Part and Assembly Manufacturing"), and the Heat Treatment row occurs once, on `/…/value-added-secondary-processes` | 0 |
| **(2) capacity** (work sent out: heat treatment, plating, NDT) — stated as the snippets fix it | 7 | 7 |
| — declared unstated correctly | 0 | 0 |
| — supplied beyond the snippets | 0 | **9** (the plating cluster: the snippet is the *Plating, Paint & Specialized Surface Treatments* row, which says nothing about vendors; new appends "These services are managed internally or through trusted vendors" from the page's intro paragraph) |
| — silent, snippets silent too (defensible) | 9 | 0 |
| **(3) generic or agentless copy** — n/a | 34 | 34 |
| **(4) document rule** — n/a | 34 | 34 |
| **(5) party correct** — yes | 34 | 34 |
| **(6) focus kept** — yes | 21 | 10 |
| — no (body is the sibling list) | 13 | **24** |
| **(7) unsupported claim** — yes | 1 | **9** (the vendor clause above; page-supported, snippet-silent) |

---

## 2. Better / same / worse

| chunk | better | same | worse |
|---|---:|---:|---:|
| 1 — mathewsco process_caps | 8 | 15 | **16** |
| 2 — anchor-mfg process_caps | **37** | 1 | 0 |
| 3 — superiortech material_caps | 7 | 19 | **8** |
| **total (111)** | **52** | **35** | **24** |

---

## 3. Every record where new is worse

### 3a. Chunk 1 — the party regression (15 records)

The snippet's own pronoun puts the doing with a principal; new moves it to Mathews & Company.

| record_id | focal form | new's offending words | old, for contrast |
|---|---|---|---|
| `ghu2cklc` | smooth forged billet | "a type of custom forging that **Mathews & Company specializes in** … as stated on the Ellwood City Forge product page" | "specialized in **by Ellwood City Forge**" |
| `g7zeb1wf` | rough turned bar | "a type of custom forging that **Mathews & Company specializes in**" | "specialized in **by Ellwood City Forge**" |
| `gwnanq35` | Die Cutting | "**Mathews & Company offers Die Cutting** as part of its Kansas Product Line" (snippet: "**They offer** Die Cutting Water Jet Cutting…") | "is offered as part of the Kansas Product Line services" (no party asserted) |
| `grrakqm9` | Water Jet Cutting | "**Water Jet Cutting is offered by Mathews & Company**" | "one of the services offered in the Kansas Product Line" |
| `gl1imbur` | Slitting | "**Slitting is offered by Mathews & Company**" | "provided as part of the Kansas Product Line services" |
| `gnwwkljf` | Extrusions | "**Extrusions are offered by Mathews & Company**" | "among the services offered in the Kansas Product Line" |
| `gcwci3hl` | Transfer | "**Transfer molding is offered by Mathews & Company**" | "listed as one of the services in the Kansas Product Line" |
| `gdt982tg` | Compression Molding | "**Compression Molding is offered by Mathews & Company**" | "one of the manufacturing processes offered in the Kansas Product Line" |
| `gjpkpgsp` | 9-axis Swiss machines | "**Mathews & Company offers machining equipment including 9-axis Swiss machines**" (snippet: "**Their** equipment list includes…") | "part of the equipment list used by **companies in the Kansas Product Line**" |
| `gvoll26g` | CNC Machining | "**Mathews & Company offers CNC Machining equipment**" | "included in the equipment list of **companies in the Kansas Product Line**" |
| `gkxhdeph` | 5-axis vertical milling | "**Mathews & Company offers 5-axis vertical milling equipment**" | "among the equipment used by **companies in the Kansas Product Line**" |
| `ghhnmcs9` | vertical milling | "**Mathews & Company offers vertical milling equipment**" | "part of the equipment list for **companies in the Kansas Product Line**" |
| `g63nk11h` | milling | "**Mathews & Company offers milling equipment**" | "included in the equipment list of **companies in the Kansas Product Line**" |
| `gbyy3ii7` | precision grinding | "**Mathews & Company offers precision grinding equipment**" | "one of the equipment capabilities of **companies in the Kansas Product Line**" |
| `gb7lm6fw` | grinding | "**Mathews & Company offers grinding equipment**" | "included in the equipment list of **companies in the Kansas Product Line**" |

**Rule that caused it.** The frame-dependent document paragraph: *"Where the page frames the entries as
things the manufacturer offers, makes, works in, performs, or holds, the entry names the thing itself: a
catalog or product list offered for purchase names what is offered…"* — read by a weak model as licence to
make the page's owner the doer of everything the page lists, overriding the snippet's own "They"/"Their"
and overriding the first-person voice on a page that belongs to another company. It is reinforced by the
Task sentence *"a heading or a list on a page of the manufacturer's own offerings"*, which presumes the
list is the manufacturer's own. Note that the prompt's counterweight — *"Where they show a dealing but
leave its capacity open, say that the capacity is unstated; do not supply one"* — **fired zero times in
111 records**, in either arm.

### 3b. Chunk 1 — one import beyond the snippets (1 record)

| record_id | focal form | new's offending words | why |
|---|---|---|---|
| `glrjcsaw` | Sand Castings | "…with **size ranges from 1 lb to 10,000 lbs in a variety of processes including floor molding, cope & drag, semi-automatic, and permanent mold**, as described on the Kansas Product Line page" | the record's single snippet stops at "…Gray Irons (Full range of ASTM A4876) Castings."; the sizes/processes are the *next* sentence on the page. Old stayed inside the snippet. Caused by the same "the entry names the thing itself" licence read as "describe the whole catalog entry". |

### 3c. Chunk 3 — the sibling-list dump (8 records)

On every single-material record whose snippet is the one long materials sentence, new opens with an
accurate frame, then re-lists the entire sibling list, then closes with a restatement. Old was a tight
200–230 characters and named only the focal material.

| record_id | focal form | new's offending words | old length → new |
|---|---|---|---|
| `gov88shm` | aluminum alloys | "The company machines **carbon & stainless steels, aluminum alloys, super alloys (such as Titanium and Inconel, Hastalloy), brass & copper alloys, plastics, castings, forgings, and all materials in between.** Superior Technology, Inc. manufactures parts in aluminum alloys as part of its precision machining capabilities." | 215 → 527 |
| `gm37d125` | super alloys | same sibling dump + "…manufactures parts in super alloys as part of its precision machining capabilities." | 246 → 531 |
| `g8rkxb1m` | Titanium | same sibling dump + closing restatement | 200 → 525 |
| `gywkf0xo` | Inconel | same sibling dump + closing restatement | 199 → 523 |
| `gsgixn7o` | Hastalloy | same sibling dump + closing restatement | 201 → 527 |
| `gshl3r6g` | brass | same sibling dump + closing restatement | 227 → 517 |
| `gtgvvhcz` | brass & copper alloys | same sibling dump + closing restatement | 210 → 539 |
| `gnqgk3b5` | plastics | same sibling dump + closing restatement | 208 → 523 |

**Rule that caused it.** Not one of the new blocks directly — it is the interaction of the new opening
requirement (*"say where it sits… A reader who sees only your synthesis must be able to tell which of
these the snippet is"*) with the unchanged demand that every synthesis settle the dealing. Putting the
frame first displaces the dealing to a closing sentence, and the middle fills with the snippet's whole
sentence. The prompt's own guard — *"Designations, names, and details that a snippet attaches to other
entities — neighbours in the same list, table cell, or sentence — are not part of this record"* — is not
being read as covering a plain list of siblings.

### 3d. Systematic, sub-threshold (not counted as worse, but watch it)

Nine chunk-3 plating records (`glnxw175`, `g4o19lac`, `ga30s0he`, `gr5tcz7r`, `g26bcj3m`, `goo79est`,
`g3t9xlce`, `ghtzsxw9`, `gfgcjktg`) each append "**These services are managed internally or through
trusted vendors, ensuring certified and traceable parts**". That is true of the page (its intro says
"limitless internal and external managed Value Added Secondary Processes" and "Working with our trusted
vendors…"), but the *record's snippet* — the Plating/Paint table row — says nothing about vendors. The
**Work sent out** rule is reaching past the snippet into the page intro. Old said nothing here. It is
hedged both ways, so it misleads nobody; it is still a claim the snippets do not carry.

---

## 4. Eight records where new is clearly better

1. **`go8yx79u` (chunk 2, "Bending")** — snippet is a bare heading. new: "The Fabrication Services page
   **includes a heading titled \"Innovative Metal Bending Solutions,\"** indicating that Anchor …offers
   metal bending solutions". old: "Anchor Manufacturing Group, Inc. offers innovative metal bending
   solutions." — a heading silently promoted to a claim. Exactly the frame the new prompt asks for.
2. **`gknmr82d` (chunk 3, "aluminum")** — new names the two pages correctly ("Capabilities - Precision
   Part and Assembly Manufacturing page", "Value Added Secondary Processes page"). old cited "the
   manufacturer's about us, **value added services, and facilities list pages**" — two of the three are
   fabricated; neither snippet appears on a facilities list.
3. **`gx7l6w75` (chunk 3, "materials")** — new attributes each strand to the page it came from ("as
   described on the CNC Milling page", "…on the Grinding, Lapping & Honing page", "…on the CNC Turning
   page"); old closed with the empty "This description is from the manufacturer's own website content."
4. **`gzejq7ow` (chunk 3, "steel")** — new is 483 chars to old's 681 and drops the neighbouring heat-treat
   processes (induction hardening, nitriding, annealing…) that belong to other records, while keeping
   "certifiable to ASME AWS standards in Steel, Stainless Steel & Aluminum" verbatim.
5. **`g8c93upg` / `gef1saje` (chunk 1, "Aluminum Die Casting" / "Die Casting")** — new keeps the snippet's
   own party and adds the site's relation: "offered by **an American manufacturer represented by Mathews &
   Company**". This is the one place in chunk 1 where the rep relation is named, and it is right.
6. **`gv3i8pzu` / `gmqxyi1d` (chunk 1, "permanent mold" / "floor molding")** — new: "one of the casting
   processes offered by Mathews & Company **for Sand Castings**, with size ranges from 1 lb to 10,000 lbs"
   — the heading the snippet sits under supplies what the snippet alone does not say (that these are sand
   casting processes), and the sibling process list is dropped. old repeated all four processes in each of
   the four records.
7. **`gxyk6u8k` / `guvq9p59` (chunk 2, "final launch" / "initial concept development")** — new re-centres
   on the focal ("…proprietary Gating Process **covers through to final launch**"); old shipped one
   identical 229-char paragraph across all six APQP siblings.
8. **`glj3j97j` (chunk 2, "punching")** — new: "Anchor …**uses punching** as part of its manufacturing
   processes, **specifically turret punching** alongside laser cutting and CNC press brakes" — resolves
   the bare focal form against the snippet; old shipped the undifferentiated sentence.

---

## 5. Paragraph length: is the growth frame, or padding?

| chunk | old mean | new mean | Δ | what the added characters are |
|---|---:|---:|---:|---|
| 1 | 219 | 236 | +17 | **net frame.** The frame clause costs ~45 chars ("as stated on the Kansas Product Line page"); it is nearly cancelled by neighbour lists that new correctly dropped (the equipment cluster falls 190→130, the sand-casting processes 190→180). Growth here is information. |
| 2 | 182 | 217 | +36 | **all frame.** The delta is almost exactly the prefix "The Fabrication Services page states that " (41 chars). Nothing else moved; where new restructured, it got shorter (`gt9xar5q` 151→150, `guvq9p59` 229→167, `gxyk6u8k` 229→186). This is the cleanest arm: 38/38 frames, all accurate, no other change. |
| 3 | 418 | 585 | **+167** | **mostly padding.** The frame itself is ~70 chars ("mentioned on the Capabilities - Precision Part and Assembly Manufacturing page"), and it *replaces* old's vaguer 60-char frame, so frame is roughly +10. The remaining ~155 chars are (a) the sibling material or process list restated as the body (~200 chars on the 8 worse records), and (b) a closing sentence that restates the paragraph's own first clause — "Superior Technology, Inc. manufactures parts in X as part of its precision machining capabilities" (~95 chars), present on 20 of 34 records. |

So: chunks 1 and 2 buy frame with their extra words. Chunk 3's growth is neighbour restatement plus a
formulaic closing restatement, and it is the same defect that produced the 8 "worse" records there.

---

## 6. Verdict

**Revise first. Do not publish for a production run yet.**

The frame requirement works and should ship: 38/38 accurate frames in chunk 2 where old named nothing, and
34/34 accurate in chunk 3 where old fabricated a page location 14 times. The homonym rule holds (`gpk94tty`
"lead" is kept on lead times in both arms and never bridges to the metal). The nobody's-sentence escape
hatch behaves — not one agentless sentence was wrongly declined. No over-application of the document rule
anywhere.

What blocks publication is that on a subject that represents other companies — a sales rep, a distributor,
any site that hosts partner pages — the frame rule captures the party as well as the thing, and 15 of 39
records in chunk 1 assert a capability for a company that does not have it. That is a wrong-fact
regression, not a style one, and reps/distributors are common in this corpus.

### The exact sentence to revise

In **"What the manufacturer does with the entity"**, second paragraph:

> "Where the page frames the entries as things the manufacturer offers, makes, works in, performs, or
> holds, the entry names the thing itself: a catalog or product list offered for purchase names what is
> offered; …"

Insert the whose-clause immediately after "the entry names the thing itself", before the colon's
examples — the frame must be made to settle *what* is named and never *whose* it is:

> "…the entry names the thing itself — the frame settles what an entry names, never whose doing it is:
> where the snippet's own words put the doing with someone else (*they*, *their*, a named company, or a
> first person that belongs to another company's page), the dealing is that party's and the manufacturer's
> is listing or representing it, and where the snippets leave that open the capacity is unstated —: a
> catalog or product list offered for purchase names what is offered; …"

Two secondary edits to make in the same pass (neither blocking on its own):

- **The capacity sentence never fired once in 111 records.** *"Where they show a dealing but leave its
  capacity open, say that the capacity is unstated; do not supply one."* Give it the case it is for:
  add "…open — a list of what the manufacturer sells, represents, or carries does not say who made it —
  say that the capacity is unstated."
- **The closing restatement.** Add to the Task paragraph, after "A reader who sees only your synthesis must
  be able to tell which of these the snippet is": "Say where it sits once, and do not close by restating
  what the synthesis has already said." This is what inflates chunk 3 by ~95 chars a record and, with the
  sibling dump, produced its 8 regressions.

A re-run of these same three chunks after the edit should show chunk 1's 16 worse collapsing toward zero
with chunk 2's 37 better intact; chunk 3's sibling dump may need the neighbour guard strengthened
separately if the closing-restatement edit alone does not shrink it.

---

## 7. Arm new2 (revised prompt)

The three edits of §6 were applied and the same three requests re-run as a third arm (same weak model,
temperature 0, seed 12345). The revised prompt now carries, verbatim:

- Task: "…Say where it sits once, and do not close by restating what the synthesis has already said."
- Capacity: "Where they show a dealing but leave its capacity open, **as a list of what the manufacturer
  sells, represents, or carries does when it says nothing about who made the things listed**, say the
  dealing as shown and that the capacity is unstated; do not supply one."
- Document/frame: "…the entry names the thing itself. **The frame settles what an entry names, never whose
  doing it is: where the snippet's own words put the doing with someone else, a "they" or "their", a named
  company, a first person that belongs to another company's page hosted on the site, the dealing is that
  party's and the manufacturer's is listing or representing it; where the snippets leave that open, the
  capacity is unstated.** With that settled: a catalog or product list offered for purchase names what is
  offered; …"

### 7a. Per-grade counts, old / new / new2

**Chunk 1 — mathewsco.com, process_caps (39 records)**

| grade | old | new | **new2** |
|---|---|---|---|
| (1) frame named — yes | 36 | 39 | **39** |
| — frame accurate where named | 36 | 39 | **39** |
| (2) capacity — stated as the snippets fix it | 21 | 23 | **17** |
| — capacity declared open (hedged "offers or represents") | 0 | 0 | **22 correctly + 5 over-hedged** |
| — supplied beyond the snippets | 0 | **16** | **0** |
| — omitted though the snippets fix it | 18 | 0 | 0 |
| (3) generic/agentless (relevant in 2) — handled | 1 | 2 | 1 (`ga39any9` re-admits the agentless "'near net' bar stock is often specified…" without marking it) |
| (4) document rule — applied correctly / over-applied / n/a | 1 / 0 / 38 | 1 / 0 / 38 | 1 / 0 / 38 |
| (5) party correct — yes | 21 | 23 | **37** |
| — no | 0 | **16** | **0** |
| — left unnamed / ambiguous | 18 | 0 | 2 (`ghu2cklc`, `g7zeb1wf`) |
| (6) focus kept — yes | 16 | 27 | **18** |
| — no (body is the sibling list) | 23 | 12 | **21** |
| (7) unsupported claim — yes | 1 | 2 + 5 context-derived | **1** (`glrjcsaw`) |

**Chunk 2 — anchor-mfg.com, process_caps (38 records)**

| grade | old | new | **new2** |
|---|---|---|---|
| (1) frame named — yes | 0 | 38 | **38** |
| — frame accurate | — | 37 | **37** |
| — frame names the *section* within the page as well | 0 | 0 | **12** (Flexible Welder Design, Quality Assurance, Equipment Design, Launch Readiness, Precision Machining Capabilities) |
| (2) capacity — stated as the snippets fix it | 38 | 38 | 38 |
| — supplied / declared-open | 0 / 0 | 0 / 0 | 0 / 0 |
| (3) generic or agentless copy (relevant in 7) — handled | 7 | 7 | 7 |
| (4) document rule — n/a | 38 | 38 | 38 |
| (5) party correct — yes | 38 | 38 | 38 |
| (6) focus kept — yes | 20 | 35 | **34** |
| — no | 18 | 3 | 4 (incl. `ga66vil4`, which loses its focal entity entirely) |
| (7) unsupported claim — yes | 0 | 1 borderline | **3** (`gc89hyw5`, `gs353907`, `g0jckvyk`) |

**Chunk 3 — superiortech.org, material_caps (34 records)**

| grade | old | new | **new2** |
|---|---|---|---|
| (1) frame named — yes | 30 | 34 | **34** |
| — frame accurate | 16 | 34 | **32** (`gpa4z2mo`, `gr5ktl6l` now put the machine-capacity snippets on "the detailed capabilities page"; they are on the Large Parts CNC Turning & Milling page, which **new** named correctly) |
| — frame specific (names the page's own heading) | 16 | 34 | **21** (13 fall back to the generic "the detailed capabilities page") |
| (2) capacity — stated as the snippets fix it | 7 | 7 | 7 |
| — supplied beyond the snippets | 0 | **9** | **0** (the invented vendor clause is gone) |
| (3) generic or agentless — n/a | 34 | 34 | 34 |
| (4) document rule — n/a | 34 | 34 | 34 |
| (5) party correct — yes | 34 | 34 | 34 |
| (6) focus kept — yes | 21 | 10 | **18** |
| — no (body is the sibling list) | 13 | 24 | **16** |
| (7) unsupported claim — yes | 1 | 9 | **0 outright** (2 frame misattributions above) |

### 7b. Better / same / worse

**new2 vs old**

| chunk | better | same | worse |
|---|---:|---:|---:|
| 1 | 34 | 4 | 1 |
| 2 | 35 | 0 | 3 |
| 3 | 24 | 9 | 1 |
| **total (111)** | **93** | **13** | **5** |

**new2 vs new**

| chunk | better | same | worse |
|---|---:|---:|---:|
| 1 | 24 | 13 | 2 |
| 2 | 12 | 23 | 3 |
| 3 | 22 | 7 | 5 |
| **total (111)** | **58** | **43** | **10** |

### 7c. Every record where new2 is worse than old

| record_id | focal form | new2's offending words | likely cause |
|---|---|---|---|
| `glrjcsaw` | Sand Castings | "**Size ranges are from 1 lb to 10,000 lbs in various processes including floor molding, cope & drag, semi-automatic, and permanent mold.**" | Unchanged from **new**: the record's single snippet ends at "…Gray Irons (Full range of ASTM A4876) Castings."; the sizes and processes are the *next* sentence of the catalog entry. The frame paragraph's "a catalog or product list offered for purchase names what is offered" is still read as licence to report the whole catalog entry. |
| `ga66vil4` | concept development | "Anchor Manufacturing Group, Inc. **develops designs and processes that are efficient and cost-effective and meet all required quality standards, ensuring feasibility of manufacturability.** This design development is part of Anchor's own engineering services." — the focal form never appears; this is `g1a5zs2o`'s / `gbjdjmc3`'s snippet, not this record's APQP sentence | Cross-record bleed. Six sibling records share one snippet; under the new "re-centre on the focal, say where it sits once" pressure the model produced six differently-centred paragraphs and one of them drifted onto a neighbouring record's material. Old and new both stayed on the right sentence. |
| `gc89hyw5` | developed flexible welding cells | "…on the same weld cell, **which benefits start-up phases or low volume production by reducing capital costs**. This development is Anchor's own **proprietary** system…" | Two imports past the snippet. The benefit clause is the snippet's next two sentences; "proprietary" belongs to a *different* system on the same page ("Anchor has developed a proprietary weld system…"), so a designation attached to another entity has been moved onto the focal. Caused by the sub-section frame ("under the Flexible Welder Design section") pulling the whole section into view. |
| `gs353907` | Product Development | "Collaborative Product Development is prioritized **to tailor solutions to client requirements**." | The record's snippet is the bare heading "##### Collaborative Product Development"; the tailoring language is the paragraph beneath it. Same cause: naming the section invites reading the section. |
| `gknmr82d` | aluminum | 1,191 chars (old 551): the paragraph reports the full heat-treatment process list, the full plating list, and the welding & brazing sentence before returning to aluminum | All of it is inside the record's own four snippets, so nothing is fabricated — but the focal material is buried under three other entities' process lists. The "Say where it sits once" edit removed the closing restatement and the freed space filled with snippet body. |

Nothing else in the 111 falls below old. In particular there are **no wrong-party assertions anywhere in
new2**, in any chunk.

### 7d. Are the 15 chunk-1 party regressions gone?

**Yes — all 15, and the fix is visible in the text, not merely in the absence of the old wording.**

- **The "They offer…" cluster (6: `gwnanq35`, `grrakqm9`, `gl1imbur`, `gnwwkljf`, `gcwci3hl`, `gdt982tg`,
  and `gpoeco15` alongside them).** new2 resolves the pronoun against the catalog entry it sits in:
  "…where Mathews & Company states that **their rubber fabrication and gasket principal** offers die
  cutting, water jet cutting, slitting, extrusions, transfer, injection, and compression molding." The word
  *principal* is the site's own ("our principals can be brought in at the very beginning of projects"), so
  the resolution is grounded, not invented. This is better than **old**, which named no party at all.
- **The "Their equipment list…" cluster (7: `gjpkpgsp`, `gvoll26g`, `gkxhdeph`, `ghhnmcs9`, `g63nk11h`,
  `gbyy3ii7`, `gb7lm6fw`).** new2: "…where Mathews & Company **lists** equipment including 9-axis Swiss
  machines… This shows Mathews & Company offers or represents advanced machining capabilities including
  9-axis Swiss machines **through their principals**." The verb moved from *offers equipment* to *lists*,
  and the capability is placed with the principals.
- **The Ellwood City Forge pair (`ghu2cklc`, `g7zeb1wf`) is only half fixed.** new2: "appears in the
  California Office Product Line page **under the Ellwood City Forge section**, where Mathews & Company
  presents it as one of the custom forgings **they specialize in**". The frame is right and the closing
  hedge ("Mathews & Company represents or offers smooth forged billet") is true, but the nearest referent
  of "they" is still Mathews, and old said it plainly: "specialized in **by Ellwood City Forge**". Graded
  *same as old* (ambiguous, not false), *better than new* (no longer false). This is the one residue of the
  defect, and it is the case the whose-clause names last — "a first person that belongs to another
  company's page hosted on the site."

### 7e. Does "capacity is unstated" fire, and correctly?

**The instruction fires in substance and never in its own words.** The literal string "unstated" appears in
**0 of 111** records in all three arms. What new2 produces instead is a hedge of its own coinage,
**"offers or represents"**, in **39 of 39** chunk-1 records (and in neither of the other two chunks, where
capacity is never open — correct restraint).

- **Correct on ~22 of 39**: every product-line entry whose copy says "They", "Their", "This American
  manufacturer", or "a company that does precision machine". For a sales representative this hedge is
  exactly right and is more informative than old's silence.
- **Over-fires on ~5**: `gulv78zg` ("We can supply your rapid prototype machined"), `g4qrt3w2` ("We can
  apply Pressure Sensitive Adhesives on most materials"), `glrjcsaw` (the entry literally reads "kpl07 Sand
  Castings - **Mathews & Company** a large range of Aluminum…"), and partly `gsawivwk` and `ga39any9`. Here
  the snippets *do* fix the party as Mathews and the hedge loses information the record had.
- It arrives as a closing sentence on all 39 — see §7g.

### 7f. Did the chunk-3 sibling dump and closing restatement shrink?

**The single-snippet sibling dump: fixed.** All 8 records listed as worse in §3c collapse to roughly old's
length with the frame kept and no sibling list:

| record | old | new | new2 |
|---|---:|---:|---:|
| `gov88shm` aluminum alloys | 215 | 527 | **237** |
| `gm37d125` super alloys | 246 | 531 | **275** |
| `g8rkxb1m` Titanium | 200 | 525 | **251** |
| `gywkf0xo` Inconel | 199 | 523 | **250** |
| `gsgixn7o` Hastalloy | 201 | 527 | **252** |
| `gshl3r6g` brass | 227 | 517 | **227** |
| `gtgvvhcz` brass & copper alloys | 210 | 539 | **245** |
| `gnqgk3b5` plastics | 208 | 523 | **230** |
| `ganencyn` carbon | 409 | 532 | **235** |
| `grs9kb8i` carbon & stainless steels | 401 | 536 | **249** |

**The closing restatement: fixed in chunk 3 only.** "Superior Technology, Inc. manufactures parts in X as
part of its precision machining capabilities" is gone from all 34; the chunk's mean fell 584 → 495. The
invented vendor clause on the 9 plating records is gone too, and the plating paragraphs now sit at ~428
chars against old's ~400, with the page named correctly.

**But it returned elsewhere.** Multi-snippet chunk-3 records now dump their own snippets whole
(`gknmr82d` 668→1,191, `gkqhvubm` 483→694, `gzejq7ow` 483→768), and `gx7l6w75` states its list of source
pages **twice** — once in the opening sentence and again as the closing sentence — which is precisely the
restatement the edit forbids. And in chunks 1 and 2 the ban did not take at all (§7g).

### 7g. Chunk 1's length growth, 235 → 398: frame, capacity, or padding?

It is **two new formulas, applied without exception**, and it is roughly half information and half padding.
Measured over the arm:

| element | records | ~chars | what it is |
|---|---:|---:|---|
| opening `The term "X" appears on the <page> where Mathews & Company lists/states …` | **39 / 39** | 75–90 | **frame + party.** Real information, and the party half of it is what fixed the 15 regressions. |
| closing `This shows Mathews & Company offers or represents X … in their Kansas product line.` | **39 / 39** | 95–110 | **the capacity answer, delivered as a restatement.** It carries one genuinely new bit — that the capacity is open — and otherwise repeats the opening sentence. This is the sentence "Say where it sits once, and do not close by restating what the synthesis has already said" was written to prevent; the model reads that ban as covering the *frame* only, not the *dealing*. |
| sibling lists returned to the body | ~21 / 39 | 40–60 | **padding.** The equipment cluster and the sand-casting process cluster, which **new** had correctly stripped, are enumerated again in full in every sibling record. |

So: about 45% frame-and-party information, about 25% capacity information stated once too often, about 30%
neighbour lists coming back. Chunk 2's smaller growth (217 → 287) splits the same way: ~25 chars of real
sub-section frame ("under Quality Assurance", "under Equipment Design", "under the Flexible Welder Design
section") on 12 records, and a "This is part of Anchor's own manufacturing capabilities" closing on
essentially all 38 — pure restatement there, since chunk 2's party was never in doubt and is already named
in the paragraph's first clause.

### 7h. Verdict

**Publish new2 for a production run.**

The blocking defect is gone: zero wrong-party assertions across 111 records, the 15 chunk-1 regressions all
repaired and repaired on the evidence (the site's own word *principal*), the 9 invented vendor clauses gone,
the chunk-3 sibling dump gone, and 93 of 111 records better than the currently published prompt against 5
worse. Frame naming — the point of the whole redesign — holds at 111/111 named and 108/111 accurate, where
old named nothing in chunk 2 and fabricated a page location 14 times in chunk 3. The five worse-than-old
records are sporadic (one catalog over-read, one cross-record bleed, two section over-reads, one
over-inclusion), not a class, and they sit below old's own defect rate.

What remains is length, not truth. Do **not** hold the run for it; fold one edit into the next pass, after
the run gives a larger sample. The sentence to change is the Task paragraph's:

> "Say where it sits once, and do not close by restating what the synthesis has already said."

It is being read as governing the *frame* only. Extend it to the dealing, and forbid the closing position
outright:

> "Say where it sits once and say the dealing once, each where it belongs in the statement, and never close
> with a sentence that repeats either: a synthesis that ends 'this shows the manufacturer offers or
> represents…' has said its last sentence twice."

Two smaller ones for the same pass, both non-blocking: (a) in the capacity sentence, say that a first
person or a named party inside the snippet *does* fix the capacity, so the hedge stops firing on "We can
apply Pressure Sensitive Adhesives on most materials"; (b) in the Task paragraph, say that naming the
section a snippet sits under does not admit the rest of that section as evidence — which is what produced
`gc89hyw5`'s borrowed "proprietary" and `gs353907`'s imported tailoring clause.
