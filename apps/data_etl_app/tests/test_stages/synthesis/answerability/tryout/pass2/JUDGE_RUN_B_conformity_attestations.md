# Run-B tryout — judge report, field: conformity_attestations

Arms: **c16** (production static) vs **b** (c16 + the added section "## What to settle about the dealing").
Requests: `conf_td0` (taylordunn, 25 records), `conf_hw9` (howcogroup, 5 records), `conf_sc4` (steelcraft, 41 records).
710 paragraphs read (71 records × 5 repeats × 2 arms). Every arm read as a **mode over its five repeats**.

The added section, verbatim (`diff c16_system.txt b_conformity_attestations_system.txt`), sentences labelled for reference below:

> **[S1]** Where the focal entity is a standard, a rule, a mark, or a credential, say in what mode the snippets place the manufacturer against it and who says so, in the snippets' own words: granted, audited, registered, or listed by a named body; claimed to be met, by the manufacturer or by what it makes; tested to, and by whom; described or explained; required of others, suppliers, applicants, customers; a form the manufacturer asks others to complete; or whatever else the snippets show. **[S2]** Holding it and meeting it are different modes: keep the one the snippets give. **[S3]** Where the page shows it as the manufacturer's own, in a list of what it holds, as a badge, as a certificate offered for viewing, that showing is the dealing. **[S4]** Keep its number, issuer, and scope verbatim.

---

## 1. Headline

**The section does not help this field: it buys five party fixes on one request and pays for them with four truth reversals on another and a mode-level focal-entity collapse on fourteen records.** Records whose mode is *right*: **c16 35/71 (49%), b 37/71 (52%)** — inside noise for this model, which is bimodal per request at temperature 0. The two-point gain is not what it looks like. Direction of the flips: **5 wrong→right, all on `conf_td0`** (the parent-voice class the request was chosen for: b's closer keeps the commitment with **Waev**, the parent, where c16 hands it to Taylor-Dunn), and **4 right→wrong, all on `conf_hw9`** — the entire under-claim class the request was chosen for. On howcogroup, c16 reads a specification in the heading of Howco's own grade page as material it supplies to that specification (right, mode 4/5 on each of four records); b refuses it (mode 4/5 on each: *"does not show Howco Group claiming to be certified to, compliant with, or supplying material specifically to ASTM B805"*). The section was supposed to cure that under-claim; it deepened it. Defects introduced by b, at mode: **(a) focal-entity collapse — in 3 of 5 b repeats, all 11 records of the steelcraft SDI cluster receive one byte-identical paragraph** (c16: 11 distinct paragraphs in 5 of 5 repeats), and the same collapse hits the taylordunn Dodd-Frank trio in 3/5 b repeats vs 1/5 c16 — 14 records in all; **(b) option-list echo of the section's own vocabulary** as the closer, which is *how* the collapse happens: b ends on the mode ("This is a statement about Waev's own compliance commitments"), a sentence true of three different focal entities, instead of ending on the entity; **(c) page mis-location** on 4 records in one repeat (b[3] puts the Windstorm-page sentence on the 'H SERIES FLUSH DOORS' page). The mechanical pre-read is explained, not confirmed: the 10–30% length is **not** uniform inflation. `conf_td0` is **−1%** (54.4 → 53.8 words). `conf_hw9` is **+15.6%** (83.8 → 96.9) and every extra word is a refusal clause carrying a false claim. `conf_sc4` is **+19.6%** (47.0 → 56.2), but excluding the SDI cluster only **+5.7%** (47.7 → 50.4) — two thirds of the inflation is b pasting the ten-standard list verbatim into each of eleven records.

---

## 2. Per request

### 2.1 `conf_hw9` — howcogroup (5 records). The under-claim class. **b loses 4 of 5.**

Ruling applied: *"a heading that names a grade, alloy, or specification on the manufacturer's own materials page names what it supplies to that specification"* (c16's own text, and the brief's calibration). The pages are `/materials/nickel-super-alloys/alloy-625-plus/` and `/materials/stainless-duplex-super-duplex/grade-s31803-f51/` — Howco's own materials pages, heading `## UNS S31803 - F51 : DUPLEX STAINLESS : ASTM A182, A276, A479, BS EN 10088`.

| record | focal form | c16 mode (n/5) | truth | b mode (n/5) | truth |
|---|---|---|---|---|---|
| ghar3glk | ASTM B805 | supplies Alloy 625 Plus to the specification (4/5) | **right** | merely listed, no further dealing (4/5) | **hedged-when-fixed** |
| gduf5bt9 | AP16A | supplies to the specification (4/5) | **right** | merely listed (4/5) | **hedged-when-fixed** |
| ghv1iba0 | ASTM A182 | supplies Grade S31803-F51 to the specification (4/5) | **right** | merely listed (4/5) | **hedged-when-fixed** |
| gd0awayd | BS EN 10088 | supplies to the specification (4/5) | **right** | merely listed (4/5) | **hedged-when-fixed** |
| g2bo9dtf | UKAS approved testing laboratory | Howco operates its own in-house UKAS-approved lab (5/5) | right | same (5/5) | right |

Where b differs (ghar3glk, b[4], mode paragraph):

> "The page presents ASTM B805 as a standard associated with the alloy, but the snippets **do not show Howco Group claiming to be certified to, compliant with, or supplying material specifically to ASTM B805**; rather, it is listed as a relevant standard for the product. The only dealing shown is that Howco Group presents information about ASTM B805 as a standard relevant to Alloy 625 Plus."

against c16 on the same record (c16[2]):

> "This context is a technical specification for the alloy, indicating that **Howco Group supplies Alloy 625 Plus to the ASTM B805 standard**. … The evidence shows that Howco Group supplies Alloy 625 Plus to the ASTM B805 specification."

And on gd0awayd, b[4]: *"The snippet **does not show Howco Group claiming certification to, compliance with, or supply specifically to BS EN 10088**; it is simply listed as a relevant standard for the material."* — c16[2] on the same record: *"specifically as one of the standards for which this duplex stainless steel is supplied."*

c16 is not clean here: c16[0] under-claims on all four records (*"no further dealing is shown"*). It is the minority reading in c16 (1/5) and the majority reading in b (4/5). That is the whole flip.

### 2.2 `conf_td0` — taylordunn (25 records). The parent-voice class. **b gains 5, three of which it then collapses.**

The Supplier Code page is hosted on taylor-dunn.com but written in Waev's voice (*"At Waev, we conduct business honestly…"*, *"© 2026 WAEV INC."*). c16's rule: where the text places the item with a parent company, name that party.

| class (n) | records | c16 mode (n/5) | truth | b mode (n/5) | truth |
|---|---|---|---|---|---|
| Requirements placed on suppliers (15) | guxh8sjp, g9ruvir4, guydnnwu, gxys55oj, grlmkudl, gpp0d11p, gec9hj8v, g0ibea53, gywg9u0l, gf0ii5mo, gtkpu9p5, gq69g4yw, gjcuo8cx, gwbsle74, gv8gf2p2 | "Taylor-Dunn requires its suppliers to…" (5/5) | right mode (*required of others*), **shared party error: the requiring party is Waev** | same (5/5) | same, same shared error |
| Waev's own status / commitment (5) | gztq7f89, gki3fel2, gfni87ms, gz0rzpw9, gzdlvu5b | commitment transferred to Taylor-Dunn (4/5) | **wrong party** | kept with Waev (3/5) | **right** — 5 flips |
| Mixed own+supplier (2) | gxjlqgzl, gfayznjm | transferred to Taylor-Dunn (4/5, 5/5) | wrong party | transferred (4/5) | wrong party — no flip |
| Product meets a standard (3) | gvvo7f94, gumkdo27, gj5x8851 | "supplies vehicles that conform to…" (5/5) | right | "claims its C-426 AC … conform to" / "represents … as meeting" (5/5) | right |

The gain, verbatim. gfni87ms, b[4] (mode, 3/5):

> "Taylor-Dunn states that Waev is committed to complying with the conflict mineral requirements under the Dodd Frank Act … **This is a statement about Waev's own compliance commitments.**"

c16 on the same record, c16[0] (mode, 4/5):

> "**This shows Taylor-Dunn commits to** compliance with the conflict mineral requirements under the Dodd Frank Act and related SEC rules."

and c16[2] on the same record adds wire vocabulary: *"**The manufacturer's dealing is** stating its own commitment to compliance with these requirements."* (c16 does this in repeat 2 on ~15 of the 25 records; b never does. A c16 defect b fixes — counted as a gain for b.)

Same gain on gztq7f89 (EEO) and gki3fel2 (affirmative action), where c16's mode is the conflation *"This shows Taylor-Dunn (as Waev) identifies itself as an Equal Employment Opportunity employer"* and b's is *"This is a statement about Waev's own employment practices."*

**But** the closer that buys the fix is the closer that erases the entity: gfni87ms ("conflict mineral requirements under the Dodd Frank Act"), gz0rzpw9 ("Dodd Frank Act") and gzdlvu5b ("rules and regulations issued by the Securities and Exchange Commission") share one snippet, and in b[1], b[2], b[4] all three receive the **identical** paragraph ending *"This is a statement about Waev's own compliance commitments."* c16 collapses the same trio in 1 of 5 repeats. Net honest count for td0: **2 clean flips** (gztq7f89, gki3fel2), **3 flips bought at the cost of the focal entity**.

Neither arm ever names Waev as the requiring party on the 15 supplier records (*"Taylor-Dunn requires its suppliers to comply with applicable child labor laws"*, both arms, 5/5). The section does not touch that half of the parent-voice problem.

### 2.3 `conf_sc4` — steelcraft (41 records). The nested-list class. **No truth flips either way; 11 records take a new b-only defect.**

| class (n) | records | c16 mode | truth | b mode | truth |
|---|---|---|---|---|---|
| Steelcraft's own compliance claim (5) | g0dr9je7, g6vzhrd5, govn7228, gpifkwta, g2n191sl | "its products are in accordance with…" (5/5) | right | same (5/5) | right |
| General/explainer (1) | gy3fqrk6 | general claim about steel as a material, not about Steelcraft's products (5/5) | right | same (5/5) | right |
| Regulatory context + third party (4) | gn14wj11, g11igwfq, g83xblts, g2zr371k | reports the rule / names Allegion-Schlage (5/5) | right | same (5/5) | right |
| TDI's own doing / a denial (3) | gymnfrg1, gbpy6pxp, g3070u02 | TDI keeps the verb (5/5) | right | same (5/5) | right |
| Test protocols under Steelcraft's own frame (14) | gfvi0z4t, gprjb7bo, g344gmcr, gvstgk8x, gzg7tibu, gwfr852u, gm763gfz, gi488162, g1eet2qw, gtpy7xmu, gpjnbvpe, gr12qts7, gs8nce08, gbafvxu5 | "not a claim about Steelcraft's own products' certification" (4/5) | **hedged-when-fixed** | "does not specifically attribute the testing to Steelcraft's own products" (4/5) | **hedged-when-fixed** — no change |
| Approval agencies (3) | gscasx9t, gaxpqgv3, ggfrohep | listed as agencies, capacity hedged (4/5) | hedged-when-fixed | same (4/5) | hedged-when-fixed |
| SDI cluster (11) | g334dpzz + gbcj312l, gnd44bee, gtzqkj15, gzzyuc5m, g147yxim, g709bo4w, g2bkx9x6, g0799zbd, gzfn0vuf, ge7w9mzv | "listed among the industry standards with which SDI Membership communicates compliance … not a specific claim about Steelcraft's own products" (3/5), **focal entity named and distinct in 5/5 repeats** | hedged-when-fixed | **whole-list paste, focal entity indistinguishable (3/5)** | hedged-when-fixed **+ focal entity dropped** |

The nested-list class is the reason this request was chosen and **neither arm fixes it**. The parent lines are on the page: *"These specialty door assemblies are tested with the positive and negative pressures that occur in hurricanes"* → `### THE TEST STANDARDS`; and on the H Series page *"The following series are approved: … Tests include the following protocols"* → TAS 201/202/203. b's mode on gprjb7bo (TAS 201), b[3]:

> "This is presented as part of a general explanation of test standards for windstorm-resistant assemblies, **not as a claim about Steelcraft's own products being tested to TAS 201**."

c16's mode says the same thing in its own words (c16[3]: *"not as a statement about Steelcraft's own products' certification"*). One repeat in each arm attaches it correctly (c16[2]: *"part of the standards and tests associated with Steelcraft's windstorm-resistant products"*; b[4]: *"Steelcraft references TAS 201 as a relevant test standard for its windstorm-resistant products"*). Dead heat.

The SDI collapse, verbatim — this is the **same paragraph** returned in b[0] for all eleven records (g334dpzz, gbcj312l, gnd44bee, gtzqkj15, gzzyuc5m, g147yxim, g709bo4w, g2bkx9x6, g0799zbd, gzfn0vuf, ge7w9mzv):

> "On its Hurricane doors and frames page, Steelcraft states that 'SDI Membership communicates compliance with industry standards including ANSI/SDI A250.8, ANSI/SDI A250.10 (Prime Paint), ANSI UL 10C (Fire), UL 1784 (Air Leakage), ANSI/SDI A250.4 (Physical endurance), ANSI/SDI A250.6 (Hardware reinforcing), ANSI/SDI A250.3 (Finish coatings), ASTM A653 (Galv; All Tornado products are Galvanealled), ANSI A115 (Locations) and SDI 117 (Tolerances).' This is presented as a general statement about the standards with which SDI membership communicates compliance."

Nothing in it says which of the ten standards the record is about. c16 on the same record (gzzyuc5m, c16[0]) keeps the entity:

> "**UL 1784 (Air Leakage)** is listed among the industry standards with which SDI Membership communicates compliance."

Counts, mechanical: distinct paragraphs across the 11 SDI records, per repeat — c16: 11, 11, 10, 11, 11. b: **1, 1, 1**, 11, 11.

---

## 3. Defects introduced by b

1. **Focal-entity collapse — MODE-LEVEL, 11 records** (`conf_sc4` SDI cluster, b[0], b[1], b[2] = 3/5). Quote and counts above. In b[1] the closer is *"does not specifically attribute certification to Steelcraft's own products"*; in b[2], *"This is presented as part of Steelcraft's own compliance with these standards through SDI Membership."* — both identical across all eleven records. c16 never does this on this cluster.
2. **Focal-entity collapse — MODE-LEVEL, 3 records** (`conf_td0`: gfni87ms / gz0rzpw9 / gzdlvu5b, b[1], b[2], b[4] = 3/5, vs c16 1/5). Same closer, three different focal entities.
3. **Refusal of the manufacturer's own materials page — MODE-LEVEL, 4 records** (`conf_hw9`, 4/5 each). *"does not state that Howco Group supplies, manufactures, or certifies to ASTM B805 itself"* (ghar3glk, b[2]); *"The snippet does not show Howco Group supplying, manufacturing, or certifying to BS EN 10088 itself"* (gd0awayd, b[2]).
4. **Naming a page the text does not put the snippet on — single repeat, 8 records** (`conf_sc4` b[3]): *"On the **'H SERIES FLUSH DOORS' product page**, Steelcraft states that its products are in accordance with ANSI/SDI A250.13 …"* (g6vzhrd5, govn7228, gpifkwta, g2n191sl — the sentence is on the Windstorm applications page); and b[2] mirrors it the other way (*"On the 'WINDSTORM' applications page … Direct TDI labels are no longer available"*, gymnfrg1 — that line is on the H Series page). c16 makes the same class of error at a similar rate (c16[1]–[4] put the H Series protocol bullets on the Windstorm page; c16[1] invents a *"'Hurricane' applications page"*). **Not scored against b** — shared.
5. **Supplied extension — single repeat**: gymnfrg1 b[4], *"Steelcraft informs customers that direct TDI labels are no longer available **for its products**"* — the page says TDI labels are no longer available, full stop.

**Defects b does NOT introduce**, checked and negative: no verbatim echo of an option list as a phrase ("makes, performs, or provides it as its own" never appears); no refusal of the manufacturer's own listings on the Steelcraft own-claim records; no first-person leaks; no wire vocabulary (`focal_form`, `record`, `snippet`) — and b **removes** c16's recurring *"The manufacturer's dealing is …"* leak, which c16 emits in repeat 2 on roughly 15 of the 25 `conf_td0` records and on `conf_hw9`-style closers. That is a real, if small, b gain.

---

## 4. Wording — which sentence causes what

- **[S1] is the cause of defect 3 (the four hw9 flips) and of the td0 gain.** It demands a named mode *"and who says so"*, and offers *"described or explained"* in the menu. On a bare heading `## UNS S31803 - F51 : DUPLEX STAINLESS : ASTM A182, …` there is no verb and no named body, so the model resolves to "described" and writes a refusal — overriding c16's own materials-page rule, which S1 never mentions. Smallest rewrite: keep S1 and append one clause tying it back to that rule — *"A specification named in the heading of the manufacturer's own grade, alloy, or product page is one it supplies or makes to; that is its mode, not 'merely named'."* The *"and who says so"* half is what fixes the parent voice and should survive any edit.
- **[S4] "Keep its number, issuer, and scope verbatim" is the cause of defects 1 and 2.** Told to keep the number verbatim, the model quotes the whole sentence that carries all ten numbers, and then closes on the mode rather than the entity. This directly contradicts c16's standing neighbour rule ("Designations, names, and details that a snippet attaches to other entities … are not part of this record"). Smallest rewrite: *"Keep the focal entity's own number, issuer, and scope verbatim; the numbers of the other entries in the same list or sentence are not this record's, and the synthesis must end able to be told apart from theirs."*
- **[S2] "Holding it and meeting it are different modes" fired weakly and in the right direction** on 3 records (`conf_td0` gvvo7f94, gumkdo27, gj5x8851: b *"claims its C-426 AC and TT-316 tow tractors conform to or meet"*, c16 *"supplies vehicles that conform"*). It never fired wrongly. Weakly validated.
- **[S3] "Where the page shows it as the manufacturer's own, in a list of what it holds, as a badge, as a certificate offered for viewing, that showing is the dealing" never fired on any of my 71 records.** The one place it should have — `g334dpzz` SDI Membership, under *"Steelcraft is SDI Certified through regular audits"* — b still hedged at mode. **Unvalidated, not validated.**

---

## 5. Verdict

**DO NOT SHIP.** As drafted the section reverses the truth on four of the five records of the exact under-claim class it was written to fix (`conf_hw9`, mode 4/5 each) and introduces a mode-level focal-entity collapse on fourteen records — three of them the very records where it delivers its one real gain — while leaving the nested-list class on steelcraft exactly where c16 had it; the parent-voice gain it does deliver is worth a second draft, but only with the S1 materials-page exception and the S4 neighbour clause above, re-measured on these same three requests.
