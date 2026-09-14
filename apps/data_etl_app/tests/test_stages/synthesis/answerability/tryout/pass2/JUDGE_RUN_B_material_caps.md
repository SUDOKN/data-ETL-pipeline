# Run-B judge — material_caps (`mat_al6`, `mat_dm14`)

Arms: **c16** (`c16_system.txt`, production) vs **b** (`b_material_caps_system.txt` = c16 + the
"## What to settle about the dealing" section). 5 repeats each, gpt-4.1, temp 0, strict schema.
65 records × 10 paragraphs = **650 paragraphs read**, all present (no missing record ids in any of
the 20 output files).

The added section, verbatim (the only diff):

> ## What to settle about the dealing
> Where the focal entity is a substance or a grade of one, say what the snippets show the manufacturer doing with it, working, forming, machining, finishing, supplying, stocking, offering it as an option, or whatever else they show, and where: its own plant or a partner's at its direction. **Working a substance and making it are different dealings: keep the one the snippets give.** Where the substance appears only as what a part is made of, as an entry in a customer's requirement, or in a supplier's sheet the manufacturer carries, say that and no more. **A grade or series named in the focal form is a form of a substance: keep the designation and make the substance visible.**

---

## 1. Headline

**The section does nothing on this field, and costs a little.** Across both requests the MODE of
every single record is identical between the arms: **c16 mode-right 61/65, b mode-right 61/65,
and zero records flipped in either direction** — not one record changed party, capacity, or truth
at the mode. The four non-right records are the same four in both arms (`gng2x9pc` "Beryllium",
mode-wrong in 5/5 of both arms; `gdz7byap`, `gmjbfb9w`, `gw13ruf5`, which both arms decline in 5/5
although the page answers the question one line below the snippet). Both of the cases this section
was written for were misses: on **alecmfg the c16 arm never reproduced the production hedge at all**
(42/42 records, 5/5 repeats, c16 already says "Alec Model offers X as a material option for its
manufacturing services"), so the "working a substance and making it are different dealings"
sentence had no defect left to fix; and on **decimal the Beryllium record is wrong in both arms in
all ten paragraphs**, so the section did not fix the one record the tag named. Against that zero
gain, b introduces measurable harm, all of it below the mode except one item: a **capacity hedge
fired on 42/42 alecmfg records in repeat b[3]** ("the specific capacity … is not further specified
in the snippet") — exactly the J2 capacity-unstated over-fire on the manufacturer's own listing that
C16 was shipped to kill, now back in one request-mode of five; a **1/5 laundering of the customer's
part** on `gvj9vkui` ("Decimal Engineering, Inc. manufactures stamped parts"); **three 1/5 placement
misstatements** on alecmfg in repeat b[2]; the wire word **"snippet" leaks into the prose in 93/325
b paragraphs vs 15/325 c16 paragraphs**, and becomes the majority reading (≥3/5 repeats) on **9
decimal records under b vs 3 under c16**; and paragraphs inflate **38.2→41.0 words (alecmfg) and
55.4→59.6 words (decimal)**, about +7–8% for no added content. No option-list echo, no
"does not attribute…" refusal, no `focal_form`/`record` leak, no first-person leak, no dropped
focal entity in either arm.

---

## 2. Per request

### 2a. `mat_al6` — alecmfg / "Alec Model", 42 records

All 42 records are bare bullets under **`## Material Options` → "A variety of materials are
available for the manufacturing processes we offer."** on the company's own
`/services/` page, subdivided by `#### Copper`, `#### Additional Metals` (Brass / Bronze / Zinc /
Titanium), `#### Materials Used in Urethane Casting`, `#### Thermoplastics`,
`#### Thermosetting Plastics`, `#### Elastomers`, `#### Specialty and Advanced Materials`,
`#### Aluminum Alloys`, `#### Zinc Alloys`. Per the frame rule this is the manufacturer's own
offering; "offers X as a material option for its manufacturing processes" is the right reading and
any "whether it manufactures or sources it is not specified" would be an under-claim.

Because every record behaves identically, one class table plus the exceptions:

| class (records) | c16 mode | b mode | truth |
|---|---|---|---|
| all 42 (`guoaeegg`, `gg5dxmb4`, `gv392z5r`, `gwze2mjx`, `g1x78mgf`, `gv8iualz`, `grsdkris`, `gdmojpb1`, `gfyuzwoz`, `gpdvey9i`, `gi23xamd`, `gom9w5fv`, `gqr0m6lj`, `g0sgjxt4`, `gn3ifts8`, `g3naywz9`, `gr3pkip7`, `gvhg9sma`, `g1swvaqw`, `g75wla72`, `gfmgbsf2`, `g4isy6is`, `ghq18d7g`, `gdhj7ie3`, `ghlba2y6`, `gtyny6o6`, `gzo7s0jz`, `gv8l15jj`, `g6wl8l8x`, `gqlzbcab`, `ggvk4xnu`, `g6axss80`, `g87k2ub6`, `gsqmps1s`, `g1cn85pq`, `g5vjyfzm`, `g50ywm4y`, `gnfu8e4c`, `gfxrun1s`, `ganbbybo`, `gjnzvqvz`, `gyxmppz0`) | party = Alec Model's own; capacity = **offers as a material option for its manufacturing processes**, 5/5 | identical, party = Alec Model's own; capacity = **offers as a material option**, 5/5 (4/5 clean + 1/5 with an added capacity hedge, see below) | **right** in both arms |

**Where b differs from c16** — it is not the mode, it is repeat b[3], which appends a capacity
hedge to every one of the 42 paragraphs. `gv8iualz` (C510 (Phosphor Bronze)), b[3] verbatim:

> "On the 'Services' page of Alec Model's website, C510 (Phosphor Bronze) is listed as a specific type of Bronze available as a material option for their manufacturing processes. This shows that Alec Model offers C510 (Phosphor Bronze) as a material option for their manufacturing services, **with the specific capacity (such as supplying, machining, or fabricating) not further specified in the snippet.**"

c16[3] on the same record ends at "…as a material option for their manufacturing services." with no
hedge. Same clause, same repeat, all 42 records; the verb triple varies with the section of the list
("supplying, machining, or fabricating" for metals, "supplying, casting, or fabricating" for the
die-cast alloys, "supplying, molding, or fabricating" for the plastics) — which is the section's own
verb list coming back out of the model.

Grade/designation handling (the section's second sentence): no difference worth a flip. Both arms
keep the designation verbatim and both make the substance visible in the placement clause —
c16[1] `gpdvey9i` "Grade 5 titanium", b[0] `gpdvey9i` "titanium Grade 5"; c16[0] `g50ywm4y` "A380
aluminum alloy", b[1] `g50ywm4y` "A380 aluminum alloy". Nothing here was broken for the sentence to
fix.

### 2b. `mat_dm14` — decimal.net / "Decimal Engineering, Inc.", 23 records

| record | focal form | c16 mode (n/5) | b mode (n/5) | truth |
|---|---|---|---|---|
| `gm5pj38k` | metal parts | makes them; full-service metal parts manufacturer (5/5) | same (5/5) | right / right |
| `gmjbfb9w` | powder coat finish | FAQ question only; **no dealing shown** (5/5) | same (5/5) | thin in both — the answer directly under the snippet says "DECIMAL ENGINEERING outsources our powder coating requirements"; a grounded import was available and neither arm took it |
| `gw13ruf5` | stock raw material | FAQ question only; **no dealing shown** (5/5) | same (5/5) | thin in both (answer: "maintains a minimum level of stock material") |
| `ghp7vycc` | raw material | orders raw material cut to size for its own fabrication (5/5) | same, worded "procures and utilizes" (5/5) | right / right |
| `gdz7byap` | raw aluminum extrusions | FAQ question only; **no dealing shown** (5/5) | same (5/5) | **under-claim in both** — the answer below the snippet is "Yes, Decimal Engineering sources the raw extrusion. Once the extrusion is received, Decimal Engineering can manufacture and finish your project." The section's verb list ("supplying, stocking…") did not reach it |
| `gnwwkljf` | extrusions | works extrusions as a material form, machining/fabrication (5/5) | same (5/5) | right / right |
| `gfby60d9` | Pipe | works pipe; offers pipe and frame fabrication (5/5) | same (5/5) | right / right |
| `gi967lrn` | Aluminum extrusion | performs fabrication using aluminum extrusions (5/5) | same (5/5) | right / right |
| `geyzl1kd` | tooling | designs, fabricates, maintains, evaluates tooling in-house (5/5) | same (5/5) | right / right |
| `g8p804qz` | electroplating | **sources** it — arranges through outside providers (5/5) | same (5/5) | right / right |
| `gl4qxf1c` | wet paint finishing | **sources** it (5/5) | same (5/5) | right / right |
| `gnqgk3b5` | plastics | stamps some, machines various (5/5) | same (5/5) | right / right |
| `gvj9vkui` | stamped part | the **customer's** part, taken into Decimal's assemblies (5/5) | same (4/5); 1/5 launders it to Decimal's own making | right / right at the mode |
| `gzc5dyy7` `gklepssy` `gwy9dtlk` | pems / rivets / hardware | adds them to stamped parts in assembly (5/5) | same (5/5) | right / right |
| `gf0p9ufx` | spotwelding/welding | performs it in assembly (5/5) | same (5/5) | right / right |
| `gs5j98ci` | Fixturing | designs and fabricates fixturing in-house (5/5) | same (5/5) | right / right |
| `gqg2oxli` | 430 Bright annealed | a material it stamps (5/5) | same (5/5) | right / right (neither arm names the substance — see Wording) |
| `gt19yoc7` | Aluminum (All Alloys…) | a material it stamps, tempers kept verbatim (5/5) | same (5/5) | right / right |
| `gng2x9pc` | **Beryllium** | "**'Beryllium' and 'Beryllium Copper' are listed among the materials**… works with Beryllium and Beryllium Copper" (5/5) | same (5/5) | **wrong / wrong** |
| `gojes7ws` | Beryllium Copper | stamps and wire-EDMs it (5/5) | same (5/5) | right / right |
| `gmzht1c7` | Copper Nickel | stamps it; machines Copper Nickel Alloys by EDM (5/5) | same (5/5) | right / right |

**The named target case, `gng2x9pc`, is unfixed.** The word "Beryllium" occurs in the text only
inside the alloy name **Beryllium Copper** (`**430 Bright annealed Aluminum (…) Beryllium Copper
Brass Bronze Copper Copper Nickel …**` and `**Aluminum (All Alloys) Beryllium Copper Brass Bronze
Copper Nickel Copper Nickel Alloys:** Nickel Silver`). c16's own "Identifying the focal entity"
rule — "a word that is part of a longer name for something else … those are not evidence" — should
close it. Both arms instead split the alloy into two list entries. b[4] verbatim, which is the most
naked form of the error and appears only under b:

> "In the Metal Stamping Capabilities table on the Metal Stamping Services page, **'Beryllium' is listed among the materials Decimal Engineering, Inc. can stamp.** In the Wire EDM Capabilities table on the Wire EDM Services page, 'Beryllium Copper' is listed among the materials Decimal Engineering can machine using EDM. This shows that **Decimal Engineering stamps beryllium** and machines beryllium copper using EDM as part of its manufacturing services."

c16[3] on the same record: "'Beryllium' and 'Beryllium Copper' are listed among the materials
Decimal Engineering, Inc. can stamp… This shows that Decimal Engineering, Inc. can stamp and
machine Beryllium and Beryllium Copper." Both wrong; b[4] drops even the "and Beryllium Copper"
that made the c16 sentence look like a mis-parse rather than a claim. Not a mode change — noted as
a wrong record in both arms.

---

## 3. Defects introduced by b

**D1 — capacity-unstated over-fire on the manufacturer's own list. Single repeat (b[3]), but it
hits 42/42 alecmfg records in that repeat.** `gjnzvqvz` (Zamak 3), b[3]:

> "…This shows that Alec Model offers Zamak 3 as a material option for their manufacturing services, **but the specific capacity (such as supplying, casting, or fabricating) is not further specified in the snippet.**"

Nothing like it in any of the 210 c16 paragraphs. This is the C16 regression class (J2,
capacity-unstated over-fire on an own listing) coming back through the new section's own verb list.
Because gpt-4.1 is bimodal per request, one arm-repeat carrying it on 42/42 records is a live
production risk, not an isolated sentence.

**D2 — laundering a customer's part into the manufacturer's own making. Single repeat (b[2]),
`gvj9vkui`.** The snippet reads "taking **your** stamped part and then adding pems, rivets,
hardware…". b[2]:

> "This shows that **Decimal Engineering, Inc. manufactures stamped parts** and incorporates them into assemblies with additional components and processes."

All five c16 repeats keep the part with the customer ("manufactures assemblies that incorporate
stamped parts", "using stamped parts as a basis"). Major class, single repeat.

**D3 — placement misstatement. Single repeat (b[2]), 3 alecmfg records.** The page puts
`Epoxy Resins` and `Silicone Rubbers` under `#### Materials Used in Urethane Casting`;
`Epoxy` and `Silicone` are the separate `#### Thermosetting Plastics` / `#### Elastomers` entries.
b[2] on `g1swvaqw`:

> "Epoxy Resins are listed on Alec Model's 'Services' page as **one of the thermosetting plastics** available for their manufacturing processes."

b[2] on `gr3pkip7`: "Silicone Rubbers are listed … as **one of the elastomer materials** available
for their manufacturing processes" (and drops the urethane-casting placement c16 keeps in 5/5).
b[2] on `gvhg9sma`: "…listed … as materials used in their manufacturing processes, **specifically
under thermosetting plastics**." c16 places all three correctly in 5/5. The synthesis has to let a
reader tell what the snippet is; this repeat mis-files it.

**D4 — wire vocabulary "snippet" in the prose. Mode-level on 6 additional records.** c16 forbids the
word ("never by the words focal_form, focal entity, record, or snippet"). Leak rate: **c16 15/325
paragraphs, b 93/325**. Per-record majority (≥3 of 5 repeats): c16 3 records (`gmjbfb9w`,
`gw13ruf5`, `gdz7byap` — the question-only records, where the paragraph is genuinely talking about
the quoted line), b 9 records (those three plus `geyzl1kd`, `gnqgk3b5`, `gs5j98ci`, `gng2x9pc`,
`gojes7ws`, `gmzht1c7`). The vehicle is a formulaic closer: b[0]/b[1] on `gmzht1c7` —
"**These snippets show that** Decimal Engineering, Inc. works with Copper Nickel in its metal
stamping and wire EDM services" — where c16[0] writes "**This shows that** Decimal Engineering,
Inc. works with Copper Nickel as a material in its stamping and wire EDM services." Same content,
one wire word added. On alecmfg the leak is confined to b[3] (42 paragraphs, 0 in c16).

**D5 — length inflation with no added content.** Mean words per paragraph: alecmfg 38.2 (c16) →
41.0 (b), decimal 55.4 → 59.6. The added words are the D1 hedge clause, the "These snippets show
that…" closer, and re-quoting the table row inline (b[0] `gnwwkljf`: "…material forms handled
(\"Angle Bar Stock Castings Extrusions Pipe: Plate\")").

**Not observed** (checked in all 650 paragraphs): no verbatim echo of an option list, no
"does not attribute … to any party" refusal, no `focal_form` / `focal entity` / `record` /
"the manufacturer" leak, no first-person leak, no page named that the text does not show, no
dropped focal entity.

---

## 4. Wording

- **"Where the focal entity is a substance or a grade of one, say what the snippets show the
  manufacturer doing with it, working, forming, machining, finishing, supplying, stocking, offering
  it as an option, or whatever else they show…"** — this sentence is the cause of **D1**. The model
  read the open verb list as a menu it is expected to pick from, and when the snippet is a bare
  bullet that fixes only "offered", it reports the rest of the menu as unspecified. If the section
  is kept at all, the smallest fix is to cut the enumeration and pin the frame instead:
  *"Where the focal entity is a substance or a grade of one, say what the snippets show the
  manufacturer doing with it and where. A bare entry under the manufacturer's own heading of
  materials it offers or works is offered or worked as that heading says; do not add that the
  further capacity is unspecified."* The "do not add" half is the part that earns its place — it is
  the only wording here that would have prevented anything measured.
- **"Working a substance and making it are different dealings: keep the one the snippets give."** —
  **never fired on any of my 65 records.** The c16 arm did not hedge a single alecmfg listing in any
  of its 210 paragraphs, so the defect this sentence targets was not present in the control. It is
  **unvalidated, not validated**; nothing in this tryout shows it works, and nothing shows it is
  harmless either (D1 rides in on the same section).
- **"Where the substance appears only as what a part is made of, as an entry in a customer's
  requirement, or in a supplier's sheet the manufacturer carries, say that and no more."** — no
  record of mine sits in any of those three shapes. **Unvalidated.** The one record that comes
  closest, `gvj9vkui` (the customer's stamped part), is where b[2] laundered the party (D2), so if
  this sentence fired at all it fired backwards.
- **"A grade or series named in the focal form is a form of a substance: keep the designation and
  make the substance visible."** — fired **with no measurable effect**. Both arms already keep the
  designation verbatim (`C122 (Phosphorus-Deoxidized)`, `Zamak 3`, `Aluminum (All Alloys in soft,
  ¼ hard, ½ hard & annealed)`) and both already name the substance through the list heading
  ("listed under 'Zinc Alloys'", "Grade 5 titanium"). On its clearest target case — `gqg2oxli`
  "430 Bright annealed", where the substance (stainless) is *not* on the page — b behaves exactly
  like c16 and declines to name it, which is correct but is c16's behaviour, not the section's.

---

## 5. Verdict

**DO NOT SHIP** the material_caps section: it flipped zero of 65 records in either direction
(c16 61/65 mode-right, b 61/65), left both cases it was written for untouched (alecmfg's c16 arm
never hedged; decimal's Beryllium record is wrong 5/5 in both arms), and in exchange re-introduced
the capacity-unstated over-fire on the manufacturer's own listing across 42/42 records of one
repeat, laundered a customer's part in another, tripled the `snippet` wire-word leak into a
majority reading on six more records, and added ~8% length for no content.
