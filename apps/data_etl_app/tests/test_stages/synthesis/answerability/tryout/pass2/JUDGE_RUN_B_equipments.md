# Run-B tryout — judge report, field: **equipments**

Judge: Opus 5. Requests: `eq_ag8` (AGS-TECH Inc., 32 records) and `eq_fz8` (FZE Manufacturing
Solutions, 23 records). 55 records × 10 paragraphs = **550 paragraphs read** (5 × c16, 5 × b per
record). All 20 output files delivered the full record set (32/32 and 23/23 in every repeat) — no
one-and-done losses in either arm.

---

## 1. Headline

**The b section does nothing at the mode and is mildly positive below it; it fixes neither of the two
defects this field's requests were chosen for.** Records whose MODE (3-of-5 or better) is the right
reading: **c16 51/55, b 51/55** — identical. **Zero records flipped their mode in either direction.**
Both arms are wrong at the mode on the same four records, all in `eq_fz8`: the Doosan lathe
(`g7lun3bw`, under-claimed 5/5 in *both* arms), the two general-cutting-list rows (`gjdci6dm`,
`gcwz02fo`, laundered 4/5 in c16 and 3/5 in b), and the GTAW/TIG capability row (`gfwgjubq`,
under-claimed 4/5 in c16 and 3/5 in b). Below the mode b is the cleaner arm: it removes **25
over-claiming paragraphs** that c16 emits — 21 of them the phrase "as part of its **own product
offerings**" on 21 of the 21 AGS-TECH dealer-catalog rows (repeat 1 of c16, on a page whose body says
the equipment comes from Fluke, Olympus, Oxford Instruments and other brands), and 4 more where c16
turns AGS-TECH's *own* assembly hardware into something it sells ("offers flexible assembly systems",
`gfn1ph3y`/`gu9e2jms`) — and it adds **5 correct open readings** on the three laundered/under-claimed
fz8 rows (2 on `gjdci6dm`, 2 on `gcwz02fo`, 1 on `gfwgjubq`). Against that, b introduces two defects,
both sub-mode or stylistic: **3 paragraphs escalate the laundering from "uses" to "offers"** (worst:
"FZE Manufacturing Solutions **offers computer-controlled cutters**", which turns a cutting method
into merchandise), and the wire word "snippet" spreads from 9 c16 paragraphs on 3 records to **20 b
paragraphs on 10 records**, becoming mode-level (5/5) on `g2ufgx71` and `g7lun3bw`. Length is flat
(ag8 320.6 → 327.6 chars/paragraph, +2.2%; fz8 375.1 → 373.0, −0.6%). **None of the feared regressions
appeared**: zero option-list echoes, zero "does not attribute … to any party" refusals, zero refusals
of the manufacturer's own service listings (all 13 fz8 own-listing records stay "offers/provides/
operates" 5/5 in b), zero fabricated pages, zero first-person leaks, zero dropped focal entities, zero
formulaic closers.

---

## 2. Per request

### 2.1 `eq_ag8` — AGS-TECH Inc. (32 records)

**Frame.** Records 1–11 sit in AGS-TECH's own automation prose ("Vital components in **our** automation
lines are INDUSTRIAL ROBOTS", "**we deploy**", "**Our** manual assembly operations") — things the
company runs. Records 12–32 sit on `agstech.net/industrial-test-equipment`, AGS-TECH's own catalog
page, whose body reads "**We sell** several types of industrial equipment from top brands such as
Hartip-SADT, Wiggenhauser, Fluke, Mitech, Hewlet-Packard, Oxford Instruments, Olympus…". By the frame
ruling these bare `####` rows ARE offered; they are **not** made by AGS-TECH.

| record | focal form | c16 mode | b mode | truth | note / b quote |
|---|---|---|---|---|---|
| ge1ceapr | robots | uses/deploys 5/5 | uses/deploys 5/5 | both right | no material difference |
| gvaq7rk7 | articulated variable-sequence robots | uses 5/5 | uses 5/5 | both right | — |
| gfo1akvm | intelligent sensory robots | uses 5/5 | uses 5/5 | both right | — |
| gbc63olt | robotic assembly | offers/performs 5/5 | offers/performs 5/5 | both right | "We offer … robotic assembly" fixes it |
| gy4swm1s | manual assembly operations | performs 5/5 | performs 5/5 | both right | — |
| gyjt4iad | high-speed automated assembly lines | operates 5/5 | operates 5/5 | both right | — |
| ghw986xz | transfer mechanisms | uses 5/5 | uses 5/5 | both right | — |
| gaq7cz2b | assembly system | uses 5/5 | uses 5/5 | both right | — |
| **gfn1ph3y** | flexible assembly systems | uses/operates 3/5, **offers 2/5** | uses/operates **5/5** | c16 has a 2/5 over-claim; b right | b: "This shows that AGS-TECH Inc. **operates** flexible assembly systems as part of its own automation capabilities." vs c16₀ "AGS-TECH Inc. **offers** flexible assembly systems as part of its automation capabilities." |
| **gu9e2jms** | assembly systems in automation | operates/uses 3/5, **provides 2/5** | operates/uses **5/5** | c16 has a 2/5 over-claim; b right | b: "…**uses** flexible assembly systems with these features **in its own automation operations**." vs c16₄ "…**provides** flexible, computer-controlled assembly systems … as part of its automation **offerings**." |
| ghjoqsls | automated guiding devices | uses 5/5 | uses 5/5 | both right | — |
| **g5tmr9if … g8m2r8h3** (21 catalog rows: Nondestructive Test Equipment, Fiber Optic Test Instruments, Microscope/Fiberscope/Borescope, Thickness and Flaw Gauges, Coating Surface Test Instruments, Vibration Meters, Vibration Meters+Tachometers, Tachometers, the 5-item vision heading, Vision Measuring Machines, Profile Projectors, Mechanical Test Instruments, Thermal & IR, Chemical/Physical/Environmental Analyzers, Electrical & Electronic Test Equipment, Specialized Test Equipment for Product Testing, and the five bullet rows) | offers 5/5, **"own product offerings" 1/5 on every one of the 21** | offers 5/5, **"own product offerings" 0/5** | both right at the mode; c16's repeat-1 wording is an over-claim | b₂ form: "This shows that AGS-TECH Inc. **lists** Nondestructive Test Equipment **as a category of industrial test equipment it offers**." vs c16₁ "…offers Nondestructive Test Equipment **as part of its own product offerings**." |

Shared miss (both arms, all 21 catalog rows): neither arm ever says the equipment is **another maker's**
— the page's own "from top brands such as … Fluke … Olympus" never reaches a paragraph, because it is
not in the snippets. Nobody is worse than c16 here; it is simply not fixed.

### 2.2 `eq_fz8` — FZE Manufacturing Solutions (23 records)

**Frame.** Thirteen records are FZE's own service listings and shop hardware ("**Our** advanced CNC HMC
& VMC Machining centers", "**Our** Swiss Turning Centers", "- Part Assembly Services"). Five are
equipment named inside the generic MIG/spot-welding explainer on FZE's own guide page. `gfwgjubq` sits
in FZE's **capability spec sheet** ("**Arc/Resistance Welding Process:** … GTAW (Gas Tungsten Arc
Welding) / Also known as Tungsten Inert Gas (TIG) welding"). `gjdci6dm`/`gcwz02fo` sit in a generic
industry explainer ("**The following are some of the most common metal fabrication processes**: -
Cutting: There are several types of cutting methods, including sawing, laser cutting…"). `g7lun3bw` is
a caption in FZE's own shop photo gallery.

| record | focal form | c16 mode | b mode | truth | b quote where it differs |
|---|---|---|---|---|---|
| gkc4qccv, gmbc0fms, g9y031hr, g3hzs9j5, gkdznwik, gtjogj39, gpoqgy7d, giapz7bf, gjm3zbf8, gavaj9h9, gi2ijc0z (11 own-service rows) | electropolishing, passivation, part assembly ×3, machining inspection, boring, welding & fabrication ×2, electropolishing treatments, part assembly proficiency | offers/performs 5/5 | offers/performs 5/5 | both right | no hedging, no refusal; b keeps the own-listing claim, e.g. "FZE Manufacturing Solutions **lists** Stainless Steel Electropolishing Services **as one of the manufacturing services it offers**." |
| gf5ps0qa | CNC HMC & VMC Machining centers | operates 5/5 | operates 5/5 | both right | "…indicating that **it operates these centers** as part of its manufacturing capabilities." |
| gi75fyxy | Swiss Turning Centers | uses/operates 5/5 | uses/operates 5/5 | both right | — |
| g2ufgx71 | welding gun with a wire feed unit | general explainer, no dealing 5/5 | same 5/5 | both right (open frame kept open) | b adds the wire word: "This **snippet** is part of a general explanation…" 5/5 |
| gmin8zgf | dedicated MIG welding machine | explainer 5/5 | explainer 5/5 | both right | — |
| gv5spta3 | MIG welding machine | explainer 5/5 | explainer 5/5 | both right | — |
| gcckv0bu | copper electrodes | explainer 5/5 | explainer 5/5 | both right | — |
| gviwevie | Flux-Cored Arc Welding (FCAW) | heading only, no dealing 5/5 | same 5/5 | both right | — |
| gbbhra6c | TIG welding | explainer 5/5 | explainer 5/5 | both right on the snippets (shared residue: the capability sheet elsewhere does list TIG) | — |
| **gfwgjubq** | Tungsten Inert Gas (TIG) welding | **under-claim 4/5** (right 1/5) | **under-claim 3/5** (right 2/5) | both WRONG at the mode; b one step better | b₃: "On the custom metal fabrication capabilities page, Tungsten Inert Gas (TIG) welding is listed as a welding process, also referred to as Gas Tungsten Arc Welding (GTAW). **This shows that FZE Manufacturing Solutions offers TIG welding as part of its welding capabilities.**" — right; but b₂/b₄ still say "does not show any specific dealing" |
| **g7lun3bw** | Doosan TT1800SY | **under-claim 5/5** | **under-claim 5/5** | both WRONG, unchanged | b₃: "Doosan TT1800SY appears as a photo gallery title on the Photo Gallery page… **The snippets show no dealing beyond a document or gallery entry bearing this name.**" — the machine in FZE's own shop gallery is still given to nobody |
| **gjdci6dm** | laser cutting | **laundered 4/5** ("among the methods it uses") | **laundered 3/5**, correct-open **2/5** | both WRONG at the mode; b closer | b₁ (right): "The snippet lists laser cutting as one of several types of cutting methods, but **does not specify whether FZE Manufacturing Solutions offers laser cutting as a service or merely describes it as a general method**." b₀ (worse than c16): "…indicating that **it offers laser cutting** as part of its fabrication services." |
| **gcwz02fo** | computer-controlled cutters | **laundered 4/5** ("uses … as part of its metal fabrication processes") | **laundered 3/5**, correct-open **2/5** | both WRONG at the mode; b closer but with a new error shape | b₂ (right): "This is part of a general description of fabrication processes and **does not show any specific dealing by FZE Manufacturing Solutions with computer-controlled cutters themselves**." b₃ (new error): "This shows that **FZE Manufacturing Solutions offers computer-controlled cutters** as part of its metal fabrication services." |

---

## 3. Defects introduced by b

**D1 — "offers" escalation on the general-methods list (sub-mode, 3 paragraphs, 2 records).**
c16 laundered these rows as *use* ("among the methods the company employs"); b, in the repeats that
still give the item to FZE, reaches instead for *offer*, which is a larger claim, and in one case makes
equipment sound like merchandise:
- `gcwz02fo`, b₃: "This shows that **FZE Manufacturing Solutions offers computer-controlled cutters** as part of its metal fabrication services." (c16 never wrote "offers" on this record.)
- `gjdci6dm`, b₀: "…indicating that **it offers laser cutting** as part of its fabrication services."
- `gjdci6dm`, b₃: "This shows that **FZE Manufacturing Solutions offers laser cutting** as part of its metal fabrication services."

**D2 — wire vocabulary "snippet" widened (mode-level on 2 records, sub-mode on 7 more).**
c16: 9 paragraphs across 3 records (`g2ufgx71` 3/5, `gviwevie` 2/5, `g7lun3bw` 4/5). b: 20 paragraphs
across 10 records — `g2ufgx71` 5/5 and `g7lun3bw` 5/5 (now mode-level), `gviwevie` 3/5, plus one repeat
each on `gkc4qccv`, `gmin8zgf`, `gv5spta3`, `gcckv0bu`, `gjdci6dm`, `gcwz02fo`, `g9y031hr`. Example,
`g9y031hr` b₄: "**The snippets for** "Part Assembly" appear in multiple contexts on FZE Manufacturing
Solutions' website…". The defect is inherited from c16, but b more than doubles it.

**D3 — nothing else.** Explicitly checked and **not** found in b: option-list echo (0 paragraphs
containing "runs, keeps, or has at hand", "offers to others", "an article explains"), "does not
attribute … to any party" (0), refusal of an own listing (0 of 13 fz8 own-service records, 0 of 21 ag8
catalog rows), invented page names (b's page names — About Us, Firearm Components, Fire Fighting &
Safety Equipment, Recreational Marine, Hydraulics, Pneumatics, Custom Metal Fabrication, MIG Welding
Guide, Photo Gallery, Industrial Test Equipment — all exist in the text), dropped focal entity (0),
first person (0), formulaic closer ("In summary"/"Overall" — 0), length inflation (flat). b's extra
detail on `g9y031hr` b₂ ("Skilled technicians ensure that each component fits correctly", "assembled
harmoniously … to ensure seamless function") is a grounded import — both sentences are on the fire-
fighting and recreational-marine pages verbatim.

---

## 4. Wording

The section is three sentences. Their fate on this field:

**S1** — "*Where the focal entity is a thing that does work, say what the snippets show it doing and for
whom: a thing the manufacturer runs, keeps, or has at hand for its own work; a thing it offers to
others, made by it or by another party as the snippets say; a thing a customer asks for or specifies; a
thing an article explains; or whatever else the snippets show.*"
**Partly validated.** Its "runs, keeps, or has at hand" option is what cleans up `gfn1ph3y`/`gu9e2jms`
(c16's "offers" → b's "operates/uses", 4 paragraphs) and what kills "its **own product** offerings" on
all 21 catalog rows. Its "made by it or by another party" clause **never fired** on any of my 55
records — on the dealer catalog the makers (Fluke, Olympus, Oxford Instruments) are in the page body,
never in a snippet, so the clause has nothing to bite on. It is **unvalidated**, not validated.

**S2** — "*A thing the manufacturer works with is not thereby a thing it makes, and a thing it lists is
not thereby a thing it uses: keep the two apart as the snippets keep them.*"
**The cause of D1, and half a fix.** It is the only sentence aimed at `gjdci6dm`/`gcwz02fo`, and it
does buy 4 correct open readings that c16 did not have. But it forbids exactly one wrong verb —
*uses* — and a repeat that still wants to hand the item to FZE simply picks a different verb, *offers*,
which is worse. Smallest rewrite (adds the second door, keeps the sentence's shape):

> A thing the manufacturer works with is not thereby a thing it makes, and a thing it lists is not
> thereby a thing it uses **or offers**: keep them apart as the snippets keep them. **Where a passage
> only names the methods or devices a kind of work uses in general, say that, and give the thing to
> nobody.**

**S3** — "*Keep the words that say what it does to the work and how it is built, driven, or controlled,
and keep its maker and designation as given, without adding any the snippets do not state.*"
**Fired, but did no work here.** "Doosan" and "TT1800SY" survive 5/5 in *both* arms, so the clause is
carrying nothing c16 was not already carrying, and it did not touch the under-claim it was presumably
meant to help (`g7lun3bw` is wrong 5/5 in both). Nothing in the section addresses a bare caption in the
manufacturer's **own gallery of its own plant**. If that defect is to be fixed, it needs its own clause
in S1's option list, e.g. appending to the first option: "*— including a thing shown only as a picture
or caption in the manufacturer's own gallery of its work or plant*". This is an **addition, unmeasured**;
I am not claiming the drafted section fails for want of it, only that the drafted section leaves the
target untouched.

---

## 5. Verdict

**SHIP WITH EDIT** — the section is safe (no refusals of own listings, no option-list echo, no
fabrication, no inflation) and nets +25 removed over-claim paragraphs against +3 escalations and +11
wire-word paragraphs, but it moves **no record's mode** and fixes **neither** named target, so ship it
only with the S2 rewrite above (close the "offers" door that S2's one-verb ban opened) plus a standing
ban on the word "snippet" in the paragraph; if the bar for this pass is "fix the Doosan under-claim or
the laundered cutting list", the section **as drafted does not clear it**.
