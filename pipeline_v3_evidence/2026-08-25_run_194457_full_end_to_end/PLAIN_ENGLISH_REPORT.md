# Run 20260825T194457 explained in plain English

I previously reported this analysis using labels I invented while doing it
("axis", "bridge", "party", "vague") without ever defining them. That was my
mistake. This document defines every word before using it, gives a real example
of each, and then walks through what happened.

---

# PART 1 — The words, defined

## 1a. Words this project already uses

**Subject** — one manufacturer's website that we extract data from. This run had
two: `steelcraft.com` (makes steel doors and door frames) and `alecmfg.com` (a
machine shop that makes custom parts to order for other companies).

**Field** — one kind of information we try to extract. There are seven:
*products*, *contract_products* (things made to a customer's order),
*equipments* (machines the manufacturer owns), *industries* (the industries it
sells to), *process_caps* (manufacturing processes it can perform),
*material_caps* (materials it works with), and *conformity_attestations*
(certifications it holds, like ISO 9001).

**Chunk** — the website text is too big to send to the model at once, so it is
cut into two large pieces. Each piece is a chunk. This matters later, because
the same phrase can appear in both chunks and then get processed twice.

**Phrase / form** — a piece of text found on the website, exactly as written, for
example `"A60 galvannealed steel"`.

**Group** — all the different ways the site writes the same thing, collected
together. `"Steel"`, `"steel"` and `"steels"` become one group. Grouping is done
by ordinary code, not by the model.

**Focal form** — the one phrase chosen to represent a group. If the group
contains `"steel"`, `"Steel"` and `"steel doors"`, the focal form might be
`"steel"`.

**Synthesis** — a short paragraph the model writes about one group, summarising
everything the website said about it. Example:

> "Steelcraft's own copy, in a bullet point under 'FEATURES AND BENEFITS', states
> that 14 Gauge A60 galvannealed steel is used for superior corrosion resistance
> on exterior openings."

**Record** — what gets passed to the later stages: just the focal form plus the
synthesis. Importantly, **the later stages never see the original website text** —
they only see this paragraph.

**Ontology / vocabulary** — our fixed, official list of concepts. For processes it
has 521 names. If a concept is on the list it is "in vocabulary"; if not, it is
"out of vocabulary" (often shortened to OOV).

**Grounding** — the step where the model takes a record and picks the official
concept it matches. Two flavours: pick from our list (in-vocabulary grounding),
or, if nothing on the list fits, invent a name for what is missing
(out-of-vocabulary grounding).

**Tag** — the final answer for one record: the concept name we end up storing,
e.g. `Galvanized Steel`.

**Screening** — a checking step that runs after grounding. It applies numbered
rules to each proposed tag and either passes or fails it. The rules are called
SCR-1, SCR-2, SCR-3:
- **SCR-1** asks: *is this really evidenced, and is the manufacturer substantially
  involved with it?*
- **SCR-2** asks: *is this work customer-directed / attributable to this
  manufacturer itself?*
- **SCR-3** asks: *is it the manufacturer doing this, rather than someone else?*

**Descent** (the code calls it *iterative grounding*, or *recursive grounding*) —
after a broad concept is chosen, the model walks *down* our concept tree to
something more specific: `Machining` → `Conventional Machining` → `CNC Machining`
→ `CNC Milling`. Each single step down is what I will call a **step**.

**Rule outcome and explanation** — every rule the model applies is logged with a
verdict (`satisfied` or `failed`) and a sentence explaining why. Rules that
belong to grounding have names like FGR-E1 (freehand grounding evidence rule),
IGR-E1 (in-vocabulary grounding evidence rule), OGR-N1 (out-of-vocabulary
novelty rule), RGR-E1 (descent evidence rule).

**Dump** — the big JSON file written for each subject and field, recording every
request, every record, every rule outcome and every tag. All the analysis below
comes from reading these files.

## 1b. The words I invented, and should have defined

To count *how* a tag was wrong, I sorted every single tag into one of seven
buckets. Here they are in plain English, each with a real example from this run.
**These are my labels, not the project's.**

| My label | What it actually means | Real example from this run |
|---|---|---|
| **Correct — stated outright** (I called it "D", for direct) | The website plainly says this, and the tag matches it. | The site says "14 Gauge A60 galvannealed steel is used"; the tag is `Galvanized Steel`. |
| **Correct — reworded** (I called it "N", for normalized) | Same meaning, different wording, casing or plural. Still correct. | The site says `"lathes"`; the tag is `lathe`. Or `"EN AW-6082 T6"` → `Wrought Aluminum Alloy`. |
| **Guessed** (I called it "B", for bridge) | The model added a logical leap the paragraph never made. The conclusion might even be true, but the evidence given doesn't support it. | The paragraph says a clear coat is "baked on". The model tagged `Wet Painting`, whose definition requires *liquid* paint, and simply inserted the word "liquid" into its own explanation. |
| **Too vague to use** (I called it "V") | Real, but so general it tells a user nothing. | The tag `Manufacturing` on a manufacturer. Or `Chemicals`. Or `Alloy`. |
| **Wrong category** (I called it "X", for axis — a bad name, ignore it) | The thing is real and correctly described, but it has been filed under the wrong field. | Alec's *own* service "CNC Machining" was filed under **industries it sells to**, as `Machine Tools`. CNC machining is something Alec *does*, not an industry it *serves*. |
| **Wrong company** (I called it "P", for party) | The thing is real, but somebody else does it — a testing lab, a supplier, a distributor, the customer, or the installer — not this manufacturer. | Steelcraft's frames are "tested and Intertek labeled". Intertek is an outside testing laboratory. We recorded `Product Testing` as a Steelcraft manufacturing capability. |
| **Invented** (I called it "F", for fabrication) | The thing named does not appear anywhere in the record. | The tag `flush door manufacturing machine`. No such machine is mentioned anywhere on Steelcraft's website. |

Two more of my phrases that need defining:

**"Twin"** — when one group's phrases appear in *both* chunks of the website
text, that group gets sent through the whole pipeline **twice**, independently.
The two runs can disagree. I call the two copies twins, and when they disagree I
call it twin divergence.

**"Noise floor"** — how much the model's answers change when you give it
*exactly the same input twice*. If the answer changes 15% of the time on its own,
then any experiment that moves a number by less than 15% has told you nothing.

**"Circular evidence"** — when the reason given for a conclusion is just the
conclusion restated. Example, verbatim from this run: *"The doors are
manufactured and offered to customers, indicating customer-directed work."* That
sentence is true of every product ever made; it does not show customer direction.

---

# PART 2 — How the pipeline works, in one paragraph

The website text is cut into two chunks. A search step finds candidate phrases.
A collection step gathers the exact sentences where each phrase appears.
Plain code (no model) groups the phrases that mean the same thing. A synthesis
step writes one paragraph per group. From here on, **the model never sees the
website again — only that paragraph.** Grounding picks an official concept for
each paragraph. Screening checks it with the SCR rules. Descent walks down to
more specific concepts. Whatever survives is stored.

One structural fact matters enormously and I will come back to it: **descent runs
AFTER screening**, so nothing ever checks what descent produces.

---

# PART 3 — What worked

**1. The run finished.** Both subjects completed all seven fields for the first
time ever — steelcraft in 12 minutes, alecmfg in 11. Previous attempts crashed.

**2. Delivery was perfect.** The pipeline made 381 calls to the model. I compared
the run log against the dump files: every single call was dispatched once and
answered once. No crashes, no unreadable answers, nothing left hanging.

**3. The crash fix worked, and I can prove it was needed.** Previously the run
died when the model answered with a concept that is not on our official list. The
fix records the bad answer and continues instead of crashing. **This run, the
model made the exact same mistake on the exact same record** (`g21dwglm`, where it
answered `Testing` — a word that does not appear anywhere in our 521-name process
list) **and the run simply carried on.** It happened 56 times in total.

*Walkthrough:* Steelcraft's site says its doors passed "Uniform static air
pressure tests". The model wanted to tag this `Testing`. Our list has no generic
`Testing` concept — only specific ones like `Spot Welding`. Old behaviour: crash,
losing all seven fields for that manufacturer. New behaviour: record `Testing` as
a rejected answer, move on, and let the out-of-vocabulary step name the gap. It
did, calling it `Uniform Static Air Pressure Testing`.

**4. The retry mechanism fired for real, for the first time.** In one request the
model was asked about 46 records and answered only 45. The pipeline noticed the
missing one, asked again just for that record, and got an answer in 1.5 seconds.
This code path had never actually run before.

**5. The prompt fix landed.** Our prompts used to promise the model a field called
"mentions" that we had stopped sending. 62.5% of the model's explanations
referred to this non-existent field. After the fix: 4.4%.

**6. The checking step is very good at saying NO.** Counted by hand: of all the
tags screening rejected, 25 out of 25 rejections were correct in one pair of
fields, 241 of 246 in another, and 312 correct rejections in another. It even
catches tricky cases — Steelcraft's site says its frames are designed to *avoid*
continuous profile welding, and screening correctly refused to record welding as
a capability there.

**7. Some fields are genuinely good.** alecmfg's materials list is 84% correct
(`Ti-6Al-4V`, `EN AW-6082 T6`, `C1100 copper` — all real and correctly
identified). alecmfg's equipment list is 33 of 34 correct, converting a named
service into the machine that performs it: "Die Casting" → `die casting machine`,
"CNC Turning" → `CNC lathe`, and even a model number, "HS-V55LS models" →
`vertical machining center`.

**8. Abbreviation handling is excellent.** Throughout descent, the model resolves
industry shorthand correctly: `SLS` → Selective Laser Sintering, `SLA` →
Stereolithography, `4140` → Low Alloy Steel, `A60 galvannealed` → Galvanized Steel.

---

# PART 4 — What didn't work, with walkthroughs

## 4a. The headline number

I sorted **2,005 individual tags** into the seven buckets from Part 1b, by reading
each one. **65% were correct** (either stated outright or correctly reworded).
**35% were wrong**, and here is how they were wrong:

| How it was wrong | Count |
|---|---:|
| Wrong category — real, but filed under the wrong field | 189 |
| Too vague to be useful | 188 |
| Guessed — a leap the paragraph didn't support | 166 |
| Wrong company — someone else does it | 125 |
| Invented — not in the record at all | 33 |

## 4b. Walkthrough: the invented machines (the worst single failure)

Steelcraft's website does not describe a single machine. Yet we recorded nine
kinds of machine it supposedly owns.

Here is exactly how one happened. The website says:

> "Steelcraft provides a full line of hollow metal steel doors."

The synthesis paragraph faithfully repeated that. Then grounding, asked what
*equipment* this record evidences, reasoned:

> "The focal form 'hollow metal steel doors' and synthesis state that Steelcraft
> manufactures and offers a full line of hollow metal steel doors. **This implies
> the use of machines that manufacture hollow metal doors.**"

and produced the tag **`hollow metal door manufacturing machine`** — a machine
that is named nowhere, and whose category we invented. Screening then approved it,
because the reasoning is internally consistent.

One single marketing sentence listing door types produced **six** such machines at
once. In total, **19 of the 20 equipment tags for Steelcraft are invented**; the
only real one is `engraining and staining machine`, which came from an actual
described process.

**Why it happened:** the prompt fix in point 5 above changed where the model is
told to look for evidence — from the verbatim quoted sentences to the summary
paragraph. Anchored to verbatim quotes, "no machine is named here" is obvious.
Anchored to a summary that says "Steelcraft manufactures doors", the leap to
"therefore machines exist" becomes available.

**How I know it's that and not something else:** Steelcraft's *products* field
uses the same stage, the same subject, the same run and a sibling prompt — and has
**zero** invented tags. So the fault is in the equipment prompt specifically, not
in the change generally. That single control saved us from rewriting ten prompts
when only one is broken.

## 4c. Walkthrough: the checking step is excellent at NO and terrible at YES

The rule SCR-2 asks whether work is customer-directed. On Steelcraft — a company
that sells a standard catalogue of doors — it approved 219 tags. I read all 219.
**202 of them (92%) do not actually test the question.** Verbatim examples:

> "The doors are manufactured and offered to customers, indicating customer-directed work."

That is circular: it argues that because doors were made and sold, they were made
to order. It would approve literally any product.

> "…meeting broad fire rating requirements … indicates these are made to customer
> or project requirements."

That substitutes a fire safety standard for evidence of a customer's instruction.

The mirror image happens on alecmfg, a shop where nearly *everything* is made to
order. Its *products* screen (which should be almost empty) approved 89 tags.
**Not one of the 89 has a sound justification.** The clearest case, verbatim:

> Paragraph: "a North American medical device client **provided Alec Model with
> detailed 3D models and specification sheets** for a series of titanium
> components."
> SCR-2 conclusion: "…showing Alec Model **manufactures and sells them as its
> own**."

The evidence says "the customer sent us their drawings". The rule concluded "these
are our own catalogue products". It read the evidence backwards.

**And SCR-3 — the rule meant to check whether it's this manufacturer doing the
work — has never rejected anything. Zero rejections in 465 checks.** SCR-2 runs
first and short-circuits it, so it never gets a chance.

## 4d. Walkthrough: one rule pointed the wrong way ruins a whole field

The *industries* field should list the industries a manufacturer **sells to**.
SCR-2 asks whether something is "attributable to the manufacturer itself". For a
field about *customers*, that question is backwards, and it fails in both
directions at once:

**It throws away real customers.** Verbatim rejections:
> `Unmanned Aerial Systems` — "the French UAV manufacturer contacted Alec Model,
> but does not specify any further action by Alec Model. There is no evidence Alec
> Model itself serves … unmanned aerial systems."

A drone company hired them. That *is* serving the drone industry. Also thrown out
this way: `Surgical Robotics`, `Rail Equipment`, `Passenger Rail`,
`Semiconductor Packaging`, `Food Packaging`, `Electronics`.

**It keeps things that aren't industries at all.** About 25 tags survived because
they *are* attributable to Alec itself — its own services. Alec's own sentence
"We're more than a machine shop" became the industry `Machine Tools`. Its own
service "CNC Machining" became the industry `Machine Tools`. One came from the
*author biography* at the bottom of a blog post.

So the field simultaneously loses its real customers and fills up with the
company's own activities, from one misaimed rule.

## 4e. Walkthrough: the last step has nobody checking it

Descent is the step that walks from a general concept to a specific one. It runs
**after** screening. I checked: **134 of the 135 tags descent produced never
appear in any screening check.** The stage that makes the most specific — and
therefore most falsifiable — claims is the only one nothing verifies.

I read all 128 descent steps by hand. **54 of them (42%) are wrong.**

The clearest proof that the ordering matters: Steelcraft's site says its thermal
separator is *better than* "traditional vinyl separators" — i.e. vinyl is what
they **replaced**. Screening correctly **failed** the tag `Plastic` on that
record. But descent runs afterwards, so it walked down the tree anyway —
`Polymer` → `Plastic` → `Thermoplastic` → `PVC` — reasoning "Vinyl is a
well-known type of plastic (polyvinyl chloride, PVC)". We recorded PVC as a
material Steelcraft works with, on the strength of a sentence saying they don't.

Two of the descent failures are not the model's fault at all — they are forced by
our concept list:
- Our list offers only `Wet Painting` and `Powder Painting` beneath `Painting`.
  The website says "clear coat **baked on**" and never says which. All 7 times,
  the model chose `Wet Painting` and inserted the word "liquid" into its own
  justification. It cannot answer honestly, because we gave it no honest option.
- Same for `Anodizing`, where `Decorative Anodizing` appears to be the only
  reachable option. The site describes anodizing for *corrosion resistance*,
  *thermal stability*, and even *masked electrical grounding pads* — all
  functional, not decorative — and 5 times the model inserted "aesthetic" to make
  the fit work.

## 4f. Walkthrough: a rule can fail and the tag ships anyway

Each tag carries its own rule verdicts. I found **6 tags that were stored even
though one of their own rules is marked `failed`.** Verbatim, the clearest:

> Focal form: `lead`
> Paragraph: "Steelcraft's standard **lead time** is extended by two weeks for
> custom paint colours…"
> IGR-E1 (the evidence rule): **failed** — "the synthesis refers to 'lead time' …
> **No material is evidenced.**"
> IGR-M1 (the matching rule): `chosen` — "**No material is evidenced**; 'lead'
> here refers to time, not the element."
> **And the tag `Lead` was created anyway.**

The evidence rule failed. The matching rule's own text says nothing is evidenced.
A tag was minted regardless. Screening caught this one. But of the six, **three
were caught only by luck and three reached the final output**, because the three
that escaped failed a *novelty* rule, and screening checks evidence and
attribution — never novelty.

## 4g. Walkthrough: the model describes our own vocabulary incorrectly

Three times, the model justified an answer by describing what our concept list
contains — and was wrong about it:

> "The vocabulary includes **'Inspection, testing, and measurement'** as a general
> category, but does not list 'Paint surface testing'…"

I checked all 673 names and alternate labels in the process list. There is no such
category. There is no `Inspection`, no `Testing`, no `Measurement` anywhere.
The model's *verdict* was right — our list really is missing these — but the
reason it gave was invented.

Combine that with the `Lead` case above (a rule saying "chosen" whose text says
"no material is evidenced") and this, verbatim, from a rejection:

> "…REACH is not in the vocabulary, **so it should be identified**. However, since
> the instructions say to only return…" — and then it declined.

**The conclusion: the explanations are after-the-fact justifications, not a record
of how the model decided.** This matters practically — it is exactly why those six
failed-rule tags could ship. Our code trusted a field that the model writes as
prose.

## 4h. The same input gives different answers

Two fields (*products* and *contract_products*) send **byte-for-byte identical**
paragraphs to the same grounding step with the same prompt. I verified they are
identical: 381 of 381 and 680 of 680. So comparing them measures pure
inconsistency.

**The answers differ on 12.9% of records for alecmfg and 18.7% for steelcraft.**

Separately, when one group appears in both chunks it is processed twice.
**412 groups were processed twice, and 209 of them (51%) got different answers**
the two times — including 10 cases where the very same tag was approved by
screening in one chunk and rejected in the other.

Practical consequence: **any experiment whose result is smaller than about 15%
is measuring randomness, not improvement.**

---

# PART 5 — Every distinct problem found, enumerated

Grouped by where it happens. Counts are exact where given.

### In grounding (picking the concept)
1. **Invented equipment** — 19 of 20 Steelcraft equipment tags name machines that appear nowhere. (§4b)
2. **Work credited to a testing laboratory** — "tested and Intertek labeled" became the Steelcraft capability `Product Testing`. 13 cases.
3. **Work credited to the manufacturer that happens on a building site** — `Field Assembly`, `Compression Anchor Installation`, 7 kinds of anchor preparation. 12 cases.
4. **Work credited to a distributor** — 11 cases, all from one sentence: "Steelcraft **or its distributor's** fabrication shop…".
5. **Work credited from the customer's side** — 9 cases on alecmfg, where the client performed the step.
6. **A supplier's material recorded as ours** — e.g. `#2B smooth rolled mill finish`, which is how the steel arrives from the steel mill.
7. **The customer's building materials recorded as ours** — `Wood` from "wood or steel stud anchors" (the wall the door goes into), `CMU block` (concrete masonry).
8. **The company's own activity filed as an industry it serves** — 27 cases. (§4d)
9. **Vague, contentless tags** — 188 cases: `Manufacturing`, `Chemicals`, `Alloy`, `Precision Manufacturing`.
10. **Tags stored despite one of their own rules failing** — 6 cases, 3 reached output. (§4f)
11. **The quality check judges a different string than the one stored** — 31 cases: the rule approves `'L Series doors'` and we store `'doors'`.
12. **Product/series names leaking into tags** — 58 cases in the same field, the opposite symptom of #11. Both come from renaming at save time, done inconsistently.
13. **Our own vocabulary described incorrectly to justify an answer** — 3 cases. (§4g)
14. **A rejection that argues for the opposite conclusion and rejects anyway** — 1 case (REACH).
15. **A matching rule marked "chosen" whose text says nothing was evidenced** — 1 case (`Lead`).
16. **Ordinary words captured as technical ones** — "lead time" → `Lead`; engineering "Drawing" → the deep-drawing process; the marketing phrase "**cutting-edge** machines" cited as proof of `Precision Metal Cutting`.
17. **Correct answers rejected because we showed the model a decorated label** — 3 cases. The menu displayed `Machining (also: Material Removal Process, …)`; the model echoed that whole string; our exact-match test didn't recognise it. One real capability (`Extruding`) was lost entirely this way.
18. **Documentation treated as capability** — the site *listing* a document about hardware preparation counted as performing it. Admitted 5 times, rejected 40 times: inconsistent.
19. **Business activities recorded as manufacturing processes** — `Shipment`, `Production Ramp-Up`, `Production Capacity Expansion`, `Photographic Documentation`, `CMM Reporting`.

### In screening (checking the concept)
20. **Circular reasoning** — "made and sold, therefore made to order". 88 cases in one field, 51 in another. (§4c)
21. **A safety standard substituted for evidence of customer instruction** — 100 cases.
22. **Evidence read backwards** — "the client sent us their drawings" → "we sell them as our own". 21 cases in one field. (§4c)
23. **Screening merely repeating grounding's reasoning instead of testing it** — 55 cases in one field.
24. **SCR-3 never rejects anything** — 0 of 465. (§4c)
25. **SCR-2 pointed the wrong way for the industries field** — throws out real customers, keeps the company's own services. (§4d)
26. **Correct general answers rejected for not using the exact word** — 5 cases: the site says "tack welding", we reject `Thermal Welding` because the words "thermal welding" don't appear, even though tack welding *is* thermal welding.
27. **Screening approving invented things** — all 20 invented equipment tags passed. Screening checks the reasoning, and invented things have consistent reasoning.
28. **The same sentence judged both ways** — the "Steelcraft or its distributor" sentence passes in some rows, fails in others.
29. **Rules applied in reverse** — 69 approvals cite exactly the evidence the rule's own exclusion clause forbids; 35 rejections use ownership grounds the rule explicitly forbids.

### In descent (going from general to specific)
30. **Nothing checks descent output** — 134 of 135 tags unverified, because descent runs after screening. (§4e)
31. **Descent continuing on evidence screening already rejected** — the vinyl/PVC case. (§4e)
32. **Our concept list forcing a false choice** — `Wet Painting` (7 of 7 cases insert "liquid"), `Decorative Anodizing` (5 cases insert "aesthetic"). (§4e)
33. **"Difficult material, therefore specialised process"** — 5 cases.
34. **One mention producing a whole chain of tags** — one word, "anodized", produced five tags: Surface Finishing → Coating → Chemical Coating → Anodizing → Decorative Anodizing.
35. **"Our doors are used in hospitals, therefore we are in the healthcare equipment sector"** — 5 cases.
36. **A finish specification treated as a process we perform** — `#4 Brushed Satin` → `Mechanical Polishing`.
37. **A compliance claim treated as an observed process** — "ANSI/SDI A250.10 (Prime Paint)" → `Wet Painting`.
38. **A confidently stated but technically wrong equivalence** — "snap-fit assembly … exactly matches Press Fitting". They are different joining methods.
39. **The model noting its own evidence gap and proceeding** — "While the specific material is not named, the context of medical device manufacturing…".

### Consistency and duplication
40. **Same input, different answer** — 12.9% and 18.7% of records. (§4h)
41. **Groups processed twice disagreeing** — 209 of 412 (51%). (§4h)
42. **Many names for one thing** — 8 different welding labels; 15 variants of "flush door"; 10 tags for 2 FEMA standards; 19 of 40 industry tags are near-duplicates.
43. **A rejection template misfiring** — "X is a standard, not a certification" fired against sentences that explicitly say Steelcraft *claims compliance*. 14 misfires of 21 uses, costing 6 real certifications (UL 1784, ANSI A250.8-2003, ASTM A653, STC 46/43, TDI Impact, TAS 201+202).
44. **A real certification lost honestly** — alecmfg's ISO 9001 was dropped because the summary paragraph under-claimed it ("lists ISO 9001:2015 in its certification statistics"). Grounding followed the paragraph correctly; the loss happened earlier.

### In our measuring instruments
45. **The dump reports 216 descent requests when only 104 calls were made** — anyone sizing that stage from the file is off by more than double.
46. **The synthesis-collapse detector built earlier is invisible on full runs** — it only writes its output when you stop the pipeline early, so it never reports on the runs we actually ship.
47. **A quality lint that can no longer detect anything** — the "does the summary mention the manufacturer's own name" check now scores 100.0% (3,155 of 3,155).
48. **Timing fields that don't mean what they look like** — every request records the same start time, so the "turnaround" figure is meaningless; only the latency field is real.
49. **My own text-matching probes were wrong three times** — they undercounted circular reasoning by 10×, overcounted self-contradicting rejections by about 69×, and overcounted lab/installation misattribution by 2×. This is why the whole analysis was redone by reading.

---

# PART 6 — What to fix, in order

**Fix now — code only, no prompt changes, nothing needs re-running:**
1. **Refuse to store a tag when one of its own rules failed.** One guard covering both grounding stages. Removes 6 bad tags, 3 of which currently ship. (Problem #10)
2. **Ignore the decorations when matching a concept name.** Strip the `(also: …)` part before checking whether the answer is on our list. Recovers 3 wrongly-rejected answers including a lost real capability. (#17)
3. **Fix the two broken instruments** — stop counting non-calls as requests, and write the synthesis-collapse counters into full runs. (#45, #46)

**Decide next — this is an architecture question for you:**
4. **Check what descent produces.** Either move descent before screening, or add a second checking pass over its output. Right now the most specific claims we make are the only ones nothing verifies. A cheap version: only check steps whose justification doesn't quote the record. (#30, #31)

**Then one bundled re-run** (any prompt change forces the whole tail to re-run, so group them):
5. **Rewrite SCR-2 per field.** For industries, turn it around: a client hiring you *is* evidence you serve their industry, and your own services are not industries. For products/contract, forbid circular reasoning explicitly and stop accepting safety standards as proof of customer instruction. This single change should move more numbers than anything else here. (#20–#25)
6. **Fix the equipment prompt only** — require the machine to be actually named, and forbid inferring machines from the products sold. The products field proves the other nine prompts are fine. (#1)
7. **Require descent to quote its evidence**, and give it permission to stop at the general concept when the text doesn't say which specific one. (#32–#39)
8. **Fix the "standard, not a certification" rejection template** and relax the exact-word requirement that kills correct general answers. (#43, #26)

**Longer design work:**
9. **Decide how tags get named consistently** across records, chunks and runs — this is the single biggest quality lever left. (#11, #12, #42)
10. **Decide what to add to the concept list**: generic `Inspection`, `Testing`, `Welding`, `Cutting`; the certification families; `Polystyrene`, `Zinc`, `Magnesium`, `Fiberglass`; `Industrial Automation`. (#17, #32)
11. **Retire the manufacturer-name lint** (#47) and revisit why the equipment search returns products in the first place (68 of 75 groups) (#1's root cause).
