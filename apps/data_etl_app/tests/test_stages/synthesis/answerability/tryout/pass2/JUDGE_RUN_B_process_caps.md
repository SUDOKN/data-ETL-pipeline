# Run-B tryout — process_caps judgment

Arms: **c16** (`c16_system.txt`, production) vs **b** (`b_process_caps_system.txt` = c16 + the
"## What to settle about the dealing" section). Five repeats per arm per request, gpt-4.1, temp 0,
strict schema. Requests: `proc_mw2` (mathewsco, 39 records), `proc_tf15` (tanfel, 33 records),
`proc_ag19` (agstech, 32 records) = **104 records, 1,040 paragraphs, all read**.

Repeat labels below: `c16·0…c16·4` = `b_out_<tag>_c16_0…4.json`; `b·0…b·4` = `b_out_<tag>_b_0…4.json`.

---

## 1. Headline

**The section does nothing on process_caps.** Records whose MODE (≥3 of 5 repeats) is the right
reading: **c16 84/104, b 84/104**. **Zero records flip** between the arms — not one record reads
differently at mode level. The 20 records that are wrong are wrong in *both* arms and in the same
way: tanfel's bare-noun capability index (`Metal: Lathe / Turning`, `Metal: Forging`,
`Additional Capabilities: Plastic Products`, …) sits under the company's own **CAPABILITIES** page,
so the calibration says it is offered, and both arms hedge it as "listed … the capacity is not
specified" — c16 in 5/5 repeats, b in 4/5 (b·4 alone leads with "Tanfel offers"). The section's
own-shop and represented-maker clauses therefore did not move the one error class this field has.

Worse for the section's case, **its two distinctive sentences never fired.** The
"work at a customer's site or by the customer, including work a customer does with what the
manufacturer supplies it" clause was written for `proc_ag19` — and on this c16 static **all five c16
repeats already get agstech right** ("listed among the types of automation that AGS-TECH Inc. can
incorporate into custom-made equipment"), so b had nothing to fix and no b paragraph mentions the
customer's plant. The "a sentence that says what the work is used for or suited to is evidence of
the manufacturer's own work" clause matched no record in any of the three requests. Both are
**unvalidated, not validated**.

What does change is repeat-level and roughly offsetting. **b fixes two c16 repeat-level defects**:
c16·0's forbidden `"Mathews & Company states that they offer Die Cutting"` collapse of the
principal's *they* into Mathews (7 records), and c16·4's `"equipment used in its precision machining
offerings"` over-claim on another company's equipment list (7 records). **b introduces one tic and
two small losses**: a formulaic `"but the capacity (such as manufacturer or distributor) is not
specified"` parenthetical in 2 of 5 repeats on nearly every mathewsco record; a wrong page name on
2 tanfel records in 2/5 repeats; and one dropped evidence item on `gdytq9zc` in 1/5. **b's real
gains are also repeat-level**: the sharpest anti-laundering sentence in the whole tryout is b·2's
`"the capacity is as a sales representative, not as the manufacturer"` (39/39 mathewsco records, 1/5
repeats, never said by c16), and b·3/b·4 correctly call agstech's captioned-image page a **gallery**
page (~11 records, 2/5 repeats), which is the more precise placement c16's own text asks for.

**Laundering mode fraction on mathewsco — the reading the brief asked for most carefully: 0/39
records in either arm.** Every record in both arms carries the hedge ("the capacity is unstated" /
"is not specified") or the representing frame in ≥4 of 5 repeats. Laundering survives only inside
c16·0 (see §4.1); b has no laundering repeat.

---

## 2. Per request

### 2.1 `proc_mw2` — mathewsco (39 records)

Ground truth: Mathews & Company is a sales representative's organization. Every record sits on the
**Kansas Product Line** page (a line card of the principals the Kansas office represents), the
**California Line Card**, or a represented maker's own hosted page (Ellwood City Forge, Sun
Microstamping, Stadco Precision). Correct reading = *listed / represented; the capacity is unstated*,
and where the entry says "They offer …", "Their equipment list …", "This American manufacturer …",
that party keeps the doing.

Records group into five classes; the mode is uniform inside each class.

| Class (records) | c16 mode | b mode | Truth | b quote where b differs |
|---|---|---|---|---|
| Represented maker's own page — `ghu2cklc` smooth forged billet, `g7zeb1wf` rough turned bar | "lists / represents … as a product offered by Ellwood City Forge" 5/5 | same 5/5 | both **right** | b·2: *"Mathews & Company represents Ellwood City Forge and lists smooth forged billet as part of their offering; the capacity is as a sales representative, not as the manufacturer."* |
| Bare kpl entry — `gbqwv8mr` Investment Castings, `g8c93upg` Aluminum Die Casting, `gef1saje` Die Casting, `gpa4z2mo` casting, `g8rnu8jo` Stampings, `glsv1e4i` laser cutting, `gbkrl7iv` Rubber Fabrication, `g64ggddx` Gaskets, `glrjcsaw` Sand Castings, `g8nedk2z` Precision components and assemblies (10) | "lists X as part of its Kansas product line … the capacity is unstated" 5/5 | same 5/5 | both **right** (line-card ruling) | b·0: *"Mathews & Company lists Investment Castings as part of its Kansas product line, indicating it offers these castings, but the capacity (such as manufacturer or distributor) is not specified."* |
| Passive / "We" inside a kpl entry — `g8xwhmip` tapping, `g1pc2e6i` reaming, `gkuvt5np` welding, `gxensd0m` assembly, `gulv78zg` rapid prototype machined, `g4qrt3w2` Pressure Sensitive Adhesives, `gmqxyi1d` floor molding, `gluqsr68` cope & drag, `gptrnivb` semi-automatic, `gv3i8pzu` permanent mold (10) | "offers X as a secondary operation / casting process; the capacity is unstated" 5/5 | same 5/5 | both **right** (the page is Mathews' own, so "We" is Mathews' per c16's first-person rule) | b·4: *"This shows that Mathews & Company represents or supplies tapping as a secondary operation in its Kansas Product Line, though the capacity (manufacturer or distributor) is not specified."* |
| **"They offer …" (kpl05)** — `gwnanq35` Die Cutting, `grrakqm9` Water Jet Cutting, `gl1imbur` Slitting, `gnwwkljf` Extrusions, `gcwci3hl` Transfer, `gpoeco15` Injection, `gdt982tg` Compression Molding (7) | "listed among the services offered … the capacity is unstated" 4/5; **c16·0 launders**: *"Mathews & Company states that they offer Die Cutting"* | "listed among the services offered … not specified" 5/5; **no laundering repeat** | both **right at mode**; c16 has a 1/5 party collapse b does not | b·2: *"The dealing shown is that Mathews & Company offers die cutting **through the companies it represents**; the capacity is as a sales representative, not as the manufacturer."* — the only paragraph in the 70 that names the party correctly |
| **"Their equipment list …" (kpl12)** — `gjpkpgsp` 9-axis Swiss machines, `gvoll26g` CNC Machining, `gkxhdeph` 5-axis vertical milling, `ghhnmcs9` vertical milling, `g63nk11h` milling, `gbyy3ii7` precision grinding, `gb7lm6fw` grinding (7) | "equipment available for precision components and assemblies … the capacity is unstated" 4/5; **c16·4 over-claims**: *"lists grinding as equipment **used in its** precision machining offerings"* | "equipment available … not specified" 5/5; b·4 keeps the third party explicitly | both **right at mode**; c16 has a 1/5 over-claim b does not | b·4: *"'grinding' is listed as part of the equipment available **from a company that does precision machining**"* |
| Cross-page composites — `ga39any9` machining, `gsawivwk` rapid prototype, `gcsfu74l` precision machine (3) | "lists machining … and represents companies that perform machining; the capacity is unstated" 5/5 | same 5/5 | both **right** | b·2 (`gcsfu74l`): *"Mathews & Company offers precision machining **through the companies it represents**; the capacity is as a sales representative, not as the manufacturer."* |

**mw2 result: 39/39 right in both arms, 0 flips, 0 laundering at mode level in either arm.**

### 2.2 `proc_tf15` — tanfel (33 records)

| Class (records) | c16 mode | b mode | Truth | b quote where b differs |
|---|---|---|---|---|
| **Bare-noun capability index — `gffrcucn`, `gud2hu64`, `g8x90n25`, `gq36rc0k`, `gxu47gb7`, `gdbukhyn`, `ghllw04a`, `g89d1iqh`, `gj9j0gnd`, `gnabckk1`, `gifzt4q3`, `gi1e71a7`, `g7ae933f`, `ggh91ucl`, `g3bi3j4r`, `gvv3q5ze`, `gg6psrhg` (17 "Metal: X") + `gm0e12ot`, `gp6smhba`, `gfmtr7f0` (3 "Additional Capabilities: X") = 20** | "lists X as one of its metal capabilities, but does not specify the capacity beyond listing it" **5/5** | same **4/5**; b·4 alone leads "Tanfel offers X" and then still qualifies | **both WRONG — under-claim** (`https://www.tanfel.com/metal/` is Home › CAPABILITIES › Metal, the company's own capabilities index) | b·4 (`gj9j0gnd`): *"This placement shows that **Tanfel offers Lathe / Turning** as part of its metal-related capabilities, but the snippet does not provide further detail … beyond listing it as a capability."* vs c16·2: *"showing that Lathe / Turning is listed as a capability, but the snippet does not specify the capacity beyond listing it."* |
| Own bullet capabilities — `gm9hoa1m` Precision CNC-Machining, `gku1flax` CNC Milling - Turning, `gk3csh03` UV hard coating, `gfb0aiqo` EMI, RFI and ESD shielding, `gj3yttvy` Heat Sinks (5) | "Tanfel offers / lists X as a capability" 4/5, one hedged repeat | "Tanfel offers X as part of its capabilities" 5/5 | both **right**; b slightly firmer | b·1 (`gm9hoa1m`): *"The snippet shows that Tanfel offers precision CNC-machining as part of its product and service offerings."* |
| `gulapbsd` Metal Stamping | "Tanfel offers / provides metal stamping" 5/5 | "**Tanfel performs metal stamping**" 3/5, "offers" 2/5 | both **right**; b commits harder | b·0: *"These snippets, all from Tanfel's own site, show that **Tanfel performs metal stamping** and offers it as a capability."* |
| Explainer-blog techniques — `g3n0t5p9` baking, `go8yx79u` bending, `glj3j97j` punching | "a technique in the context of metal stamping; does not show Tanfel performing it as a standalone service" 5/5 | same 5/5 | both **right** (nobody's-sentence ruling) | b·3: *"…does not attribute the activity specifically to Tanfel beyond this general description."* |
| `gqwbdj9a` Metal Stamping Processes, `gmmpaa2h` Custom metal stamping | split: "informational article" 3/5 vs "positions itself as provider" 2/5 | split the same way 3/5 vs 2/5 | wash; both defensible | b·4 (`gqwbdj9a`): *"The context of the blog post shows that Tanfel is describing and promoting its own expertise and services in metal stamping processes."* |
| `gnexao28` Membrane Switches, `gsj3z25c` Silicone Rubber Products | "Tanfel manufactures and offers membrane switches" / "offers Silicone Rubber Products, capacity unspecified" 5/5 | same 5/5 | both **right** | b·0: *"These snippets, **all from Tanfel's own product and capabilities listings**, show that Tanfel manufactures and offers membrane switches as part of its product line."* |

**tf15 result: 13/33 right in both arms, 20/33 under-claimed in both arms, 0 flips.**

### 2.3 `proc_ag19` — agstech (32 records)

| Class (records) | c16 mode | b mode | Truth | b quote where b differs |
|---|---|---|---|---|
| **"Some of the type of automation we can incorporate in your custom made equipment" — `g17qy36w` Motion Control, `gjkzqbdb` Power & Control, `g0us2plz` Dipping and Dispensing, `gz7l9czz` Pick and Place, `gw0grjlq` Controlled Shaking, `g8g7n1t0` Controlled Rotation, `gl1imbur` Slitting, `gt8h1ms7` Oiling, Surface Finishing, Painting, Coating (8)** | "listed among the types of automation AGS-TECH Inc. **can incorporate into custom-made equipment**" **5/5** | identical **5/5** | **both right** — the premise that production reads this as agstech's own shop process does **not** reproduce on this c16 static | b·2 (`gt8h1ms7`) is the only wording change and it is not a reading change: *"'Oiling, Surface Finishing, Painting, Coating' appears as a bullet point under the types of automation AGS-TECH Inc. can incorporate in custom made equipment…"* |
| Captioned gallery items — `gzfcdgvs`, `g2uj8tjv`, `gindgrfr`, `gi3a60ri`, `gvvcxg93`, `gknosxb9`, `g9ldz2ol`, `gudn02m9`, `gjflrp5i`, `gbo4lk8o`, `gsb4lris`, `g0ft2w2t` (12) | "On the 'Machined Components & Milling & Turning' **page** … AGS-TECH Inc. performs / manufactures X" 5/5 | same verdict; b·3 and b·4 say **"gallery page"** | both **right**; b's placement is more precise (the page is an image gallery of captions) | b·3 (`gi3a60ri`): *"On the 'Machined Components & Milling & Turning' **gallery page**, 'Hardness testing of Skive Blades' is listed as an item, showing that AGS-TECH Inc. performs hardness testing on skive blades…"* |
| Private-label / catalog machine lists — `gwwzatti`, `gpv1fx6l`, `gzu7odp4`, `g1oklm2q`, `go8yx79u`, `gmorsuzp`, `gvbo2w1n`, `g6d6kync`, `gjzoqrb1`, `gpi4mymk`, `g7yj1r4t` (11) | "AGS-TECH Inc. supplies / can supply X with private labeling" 5/5 | same 5/5 | both **right**; equipment listed for sale is not equipment used | b·2 (`gpv1fx6l`): *"…indicating that AGS-TECH Inc. can supply leak testing machines with private labeling, **but the capacity (manufacturer or reseller) is not specified**."* — the correct catalog hedge, which c16 omits |
| `gdytq9zc` testing (10 snippets) | all 5 repeats cover Hardness testing + private-label equipment + CIM + engineering integration | b·0 **drops the Hardness testing evidence**; b·1–b·4 cover it | c16 **right** 5/5, b **right** 4/5 | b·0: *"The snippets for 'testing' come from various contexts: on the 'Private Labeling…' page…"* — Hardness testing of Skive Blades is absent |

**ag19 result: 32/32 right in both arms, 0 flips.**

---

## 3. What the section's sentences actually did

| Sentence of "## What to settle about the dealing" | Fired? |
|---|---|
| "say who does it and where the snippets place it: the manufacturer in its own shop; …" | Fired only as re-wording. No record changed party or capacity. |
| "a partner or outside facility the manufacturer sends work to" | **Never fired** — no record raises sent-out work. Unvalidated. |
| "another company whose work the manufacturer lists or represents" | Fired on mathewsco in b·2 only (1/5) — the best paragraphs in the tryout — but c16·1/c16·3 already say "lists or represents". No mode change. |
| "a testing or certifying body" | **Never fired.** Unvalidated. |
| "work at a customer's site or by the customer, including work a customer does with what the manufacturer supplies it" | **Never fired.** The agstech case it was written for is already right in c16 5/5. Unvalidated. |
| "a step in one delivered project" | **Never fired** (mathewsco's "Secondary operations included …" is the nearest, and neither arm reads it as one project). Unvalidated. |
| "A sentence that says what the work is used for or suited to is evidence of the manufacturer's own work when the manufacturer's words claim the work, and the uses are its applications." | **Never fired** on any of the 104 records. Unvalidated. |
| "Where the focal entity names a business arrangement, a credential, or a program rather than work done to a thing, say so." | Nearest case is `gpi4mymk` White Label manufacturer; both arms already call it a manufacturer-and-supplier role. No change. |

---

## 4. Defects introduced by b

### 4.1 Option-list / capacity parenthetical (mode-level within 2 of 5 repeats, ~35 mathewsco records)
b·0 and b·4 append a formulaic parenthetical to nearly every mathewsco paragraph:

> `gbqwv8mr`, b·0: *"…indicating it offers these castings, **but the capacity (such as manufacturer or distributor) is not specified**."*
> `g8rnu8jo`, b·4: *"…as part of its product lines, **but the capacity (manufacturer or distributor) is not specified**."*

It is not false and not a truth defect, but it is a closer-tic: the same clause, same shape, on 35+
consecutive records. c16 varies its hedge ("the capacity is unstated", "with the capacity in which it
does so unstated", "in an unstated capacity"). Repeat-level (2/5), not mode.

### 4.2 Wrong page named (2 records, 2 of 5 repeats)
> `gku1flax` CNC Milling - Turning, b·1: *"CNC Milling - Turning is listed as a capability on Tanfel's industry page **for Audio - Music**."*
> `gku1flax`, b·3: *"…listed as a capability on an industry-specific page **for audio/music**."*

`- CNC Milling - Turning` occurs **only** on `https://www.tanfel.com/industries/medical/`; the Audio -
Music page carries the differently-spelled `- CNC Milling & Turning`. b·3 drops Medical entirely.
c16 commits a milder version in 1/5 (c16·1 on `gm9hoa1m`: *"…for the Medical - Life Science **and
Audio - Music** industries"*), so this is shared, not purely b's — but b is 2/5 against c16's 1/5 and
b·3 loses the right page. Repeat-level.

### 4.3 Dropped evidence (1 record, 1 of 5 repeats)
> `gdytq9zc` testing, b·0 omits the snippet *"Hardness testing of Skive Blades"* altogether, so the one
> place where agstech itself **performs** a test is missing from that paragraph. All five c16 repeats
> carry it. Repeat-level.

### 4.4 Out-of-snippet import (1 record, 1 of 5 repeats)
> `glrjcsaw` Sand Castings, b·1: *"…Ductile … and Gray Irons … castings, **with size ranges from 1 lb
> to 10,000 lbs and a variety of processes**."* That clause is the neighbouring sentence of the kpl07
> entry, not this record's snippet. A grounded import from the same entry — allowed by the
> calibration, but c16 never reaches for it. Repeat-level.

### 4.5 Not introduced by b — shared with c16, recorded so it is not misattributed
Wire vocabulary ("the snippet", "the snippets do not…") is endemic on tanfel in **both** arms
(c16·1 `g3n0t5p9`: *"The only snippet for baking comes from Tanfel's blog article…"*; b·1 the same).
c16's own text forbids the word. It is a C16 residue, not a b regression.

### 4.6 Defects b *fixes* (counted as gains, per the brief)
- `gwnanq35`/`grrakqm9`/`gl1imbur`/`gnwwkljf`/`gcwci3hl`/`gpoeco15`/`gdt982tg`, **c16·0**:
  *"On the Kansas Product Line page, Mathews & Company **states that they offer** Die Cutting…"* —
  exactly the construction c16 forbids ("Do not write that the manufacturer states that they do a
  thing when the 'they' of the snippet is someone else"). No b repeat does this.
- `gb7lm6fw`/`gbyy3ii7`/`gvoll26g`/…, **c16·4**: *"lists grinding as equipment **used in its**
  precision machining offerings"* — another company's equipment list made Mathews' own tooling. No b
  repeat does this; b·4 instead writes *"equipment available from a company that does precision
  machining"*.
- **b·2's anti-laundering sentence**, on all 39 mathewsco records: *"the capacity is as a sales
  representative, not as the manufacturer."* Page-supported (the site says Mathews & Company is "a
  sales representative's organization") and the single strongest party statement in the tryout.
  c16 never produces it in any repeat.

---

## 5. Wording

1. **The clause that should exist and does not.** Nothing in the section addresses the field's only
   mode-level error (20 of 104 records): a bare entry in the company's **own capabilities index**
   read as merely listed. Smallest addition that would move those 20 records, in the section's own
   vocabulary:

   > *A bare entry on a page the company heads Capabilities, or a heading of the form `<area>:
   > <process>` under such a page, names work the company itself offers: say that it offers it, and
   > do not report it as only listed.*

   Every one of the 20 failing records (`Metal: Lathe / Turning`, `Metal: Forging`,
   `Additional Capabilities: Plastic Products`, …) sits exactly in that shape.

2. **The sentence that causes the tic (§4.1).** "say who does it and where the snippets place it:
   the manufacturer in its own shop; a partner …; another company whose work the manufacturer lists
   or represents; …" — the colon-plus-menu invites the model to answer with a menu
   ("(such as manufacturer or distributor)"). Smallest rewrite: drop the enumeration to a lead and a
   tail and let the existing c16 text carry the cases —

   > *Where the focal entity is a kind of work done to a thing, say who does it and where the
   > snippets place it, naming the party in the snippets' own words; where the snippets leave the
   > doer open, say so once and supply nothing.*

3. **Unvalidated sentences (say so explicitly).** The partner/outside-facility clause, the
   testing-or-certifying-body clause, the customer's-site clause, the one-delivered-project clause,
   and the "used for or suited to" sentence matched **no record** in `proc_mw2`, `proc_tf15` or
   `proc_ag19`. They are unvalidated by this run — including the customer's-site clause the section
   was written for, because c16 already handles agstech correctly 5/5.

---

## 6. Verdict

**DO NOT SHIP** (for process_caps): zero of 104 records change reading, the field's one mode-level
error class — tanfel's own-capabilities index hedged as merely listed, 20 records — is untouched by
both arms, and five of the section's sentences (including the customer's-site clause it was written
for) never fired on any record, so the section buys ~120 prompt words and a new
"(such as manufacturer or distributor)" closer-tic for no measured gain.

*If the user wants to keep something:* ship **only** the new own-capabilities-index sentence from
§5.1 — that is the one edit with 20 records of evidence behind it — and re-run before adopting any
of the unvalidated clauses.
