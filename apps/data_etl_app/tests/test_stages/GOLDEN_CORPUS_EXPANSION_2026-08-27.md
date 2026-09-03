# Vetting verdicts — round 1 (12 candidates).  5 of 12 returned, all ACCEPT.

| domain | slot | verdict | what it uniquely adds |
|---|---|---|---|
| decimal.net | coverage-mid | STRONG ACCEPT | structured capability spec-tables; tabular case studies naming customers (Tiffany & Co., Loram); equipment-acquisition blog; supplier-held NADCAP trap |
| superiortech.org | deep-concept | STRONG ACCEPT | STRATIFIED depth: 5 in-house deep processes w/ named machines vs 4 outsourced list-only; ~100-machine equipment census |
| lucasmilhaupt.com | coverage-mid | STRONG ACCEPT | ONLY consumable-materials maker — product IS the material (two axes, same object); ~1,000-line 2nd-person technical manual |
| fzemanufacturing.com | coverage-mid | STRONG ACCEPT | Thomas-directory spec tables; owned-equipment roster; 3rd-person SEO process encyclopedia blog |
| sterlingmfg.net | coverage-mid | STRONG ACCEPT | SEED DOMAIN RESOLVES TO A DIFFERENT COMPANY (EPTAM); 5-acquisition roll-up; certs bound to 4 legal registrants; medical-device regime |

## Cross-subject defect patterns (5/5 subjects)
1. **Owned machine -> own product leak.** decimal ("Salvagnini P2-L-2520 Panel Bender"),
   superiortech ("Large Capacity 4-Axis Horizontal CNC Machining Centers"). 
2. **Contract parts emitted as own catalog products.** ALL FIVE have a genuinely EMPTY own-products
   field, and the old pipeline emitted fabricated products for every one of them.
3. **Instructional / encyclopedia prose harvested as own capability.** lucasmilhaupt (10+ claims from a
   how-to manual), fzemanufacturing (waterjet/plasma/oxy-fuel from a blog explainer),
   superiortech (hedged "include but are not limited to" vendor list).
4. **Substring false positives (the known `Lead` bug class), confirmed live:**
   superiortech `PET` <- "com*pet*itive" (9x); `ITAR` <- "M*ilitar*y" (9x).
5. **Material-list fabrication from a generic word:** superiortech "plastics" -> 13 specific polymers,
   all zero-occurrence. sterlingmfg: Plexiglas/LDPE/PVC/Epoxy/Bronze/Carbon Steel all zero-occurrence.
6. **Negation / contrast echo:** fzemanufacturing tagged "Electroplating" from
   "electropolishing is the reverse of electroplating"; lucasmilhaupt tagged resistance welding
   from a page arguing AGAINST welding.
7. **Country-dropdown geo poison:** decimal, lucasmilhaupt, sterlingmfg all dump the full ~240-country
   ISO list from a web form. 3/5 subjects.

## Metadata corrections owed (Mongo record is wrong)
- sterlingmfg.net -> company is EPTAM Precision Solutions, HQ New Hampshire (Mongo says Colorado);
  is_product_manufacturer should be False (Mongo says True).
- decimal.net -> is_contract_manufacturer should be True (Mongo null); is_product_manufacturer False (null).

## 6th: tanfel.com | coverage-mid | STRONG ACCEPT
First BROKER THAT TALKS LIKE A MANUFACTURER. US sourcing intermediary; writes offshore partner
factories' capabilities + certs in the first person. Old pipeline credited it with 6 certifications
when the site holds ONE in its own name (invented "ISO 16949" AND double-counted it as IATF 16949).
Aerospace/Defense/Energy imported from textbook prose, form dropdowns, and a DOG-DOOR testimonial.
Genuinely empty: equipment brands (zero, anywhere). Conditions: flip is_product_manufacturer to True
(it runs a branded thread-plug-gage catalog division); decide+record the is_manufacturer policy for brokers.
Agent checked and DISMISSED my tanfel/sterlingmfg template-collision hypothesis: different companies,
different CMS, zero shared boilerplate. The shared line was just the English word "CAPABILITIES" in both navs.

## RUNNING FINDING: Mongo role flags are unreliable (3 of 6 vetted subjects wrong or missing)
- sterlingmfg.net: geo wrong (CO -> NH), is_product_manufacturer True -> should be False
- decimal.net:     is_contract_manufacturer null -> True, is_product_manufacturer null -> False
- tanfel.com:      is_product_manufacturer False -> should be True; is_manufacturer True is contestable (broker)
CONSEQUENCE: I used is_manufacturer=True to filter the candidate pool. That filter is noisy in BOTH
directions, so the pool both admitted a broker and may have excluded real manufacturers flagged False.

## 7th: blackadvtech.com | deep-concept | STRONG ACCEPT (but prediction INVERTED)
Sheet-metal job shop, Jamestown NY. Only ADVERSARIAL deep-concept subject.
DEPTH PREDICTION FAILED IN THE INTENDED SENSE: of 9 predicted deep processes, only 2 are real
(Sheet Metal Rolling, Laser Beam Welding). The other 7 -- Deep Drawing, Hot Working, Cold Rolling,
Pickling, Hemming, Spinning, Roll Forming -- live ONLY in ~22 first-person-plural SEO blog articles
about the industry at large. One post literally closes: "Are you looking for a defect-free cold metal
rolling? If yes, Blackstone Advanced Technologies helps you attain flawless end materials." They own
no rolling mill. "Hemming" was a substring hit on the HEM Saw brand (51 hits).
=> Seed these as NEGATIVE ground truth. That is its value.
Real depth is at EQUIPMENT-PARAMETER level instead (130-320 ton press brakes, 44-station turret).
is_product_manufacturer False -> should be TRUE (7 named Navy/marine catalog items).
Scrape partly degraded: 3 live DB-error pages, 2 404s, Divi shortcode dump, lorem ipsum.

## METHODOLOGICAL FINDING: the deep-concept slot is confounded by SEO content farms
Mongo's old labels cannot distinguish "deep process this company performs" from "deep process named
in its SEO blog." My 2 deep-concept picks split 1-1:
  superiortech.org  = REAL depth (in-house, named machines) + stratified outsourced tier
  blackadvtech.com  = FAKE depth (blog encyclopedia), valuable only as a negative case
CONSEQUENCE for round 2: do NOT select the deep-concept slot on label depth alone. Either drop the
slot or add a blog-share pre-filter before spending an agent.

## Mongo role-flag error rate now 4 of 7 vetted
sterlingmfg (geo + is_product_mfr), decimal (2 nulls), tanfel (is_product_mfr), blackadvtech (is_product_mfr).
ALL FOUR errors are is_product_manufacturer. The old flag systematically misses small own-brand
catalog lines sitting inside an otherwise build-to-print business.

## 8th: med-tekinc.com | small-scale | STRONG ACCEPT  <-- best empty-field test in the batch
Commercial heat-treating TOLL PROCESSOR, Minneapolis. New business type: sells process time on other
companies' parts; designs/makes/sells nothing. 3 unique pages (4th is a byte-identical dup), ~1,000 words.
THREE HARD-EMPTY FIELDS ON A HEALTHY SITE: products, contract_products, industries (all zero).
Fourth (certifications) empty BUT BAITED: "laboratory and specification certifications if requested",
"Rockwell Hardness Machines are certified every six months" -- yet zero certs held.
PROVEN FABRICATION: "Flame Hardening" has ZERO occurrences in the file. Haystack small enough to
adjudicate in minutes.
Keyword-list question answered: HYBRID. 20-item bare list, but a concrete equipment block underneath
(furnace families, atmospheres, temp ranges, work envelopes, 50-200 KW induction). 6 verified-with-
description / 4 list-only / 1 fabricated. Old pipeline recall was poor too: caught ~10, missed ~15.
BEST TRAP: the domain "MED-TEK" reads as medical technology; `grep -i medical` = 0 hits.
Not redundant w/ 101machine (a MAKING shop, has materials page + named verticals) or taylordunn
(empty by SCRAPE FAILURE on 228KB, not by genuine absence). Different empty-field test.
is_manufacturer: genuinely ambiguous -- makes no artifact, but heat treating is NAICS 332811. Forces
a real ontology decision. is_product_manufacturer null -> clearly False.

## 9th: mathewsco.com | non-mfr-neg | STRONG ACCEPT  <-- HIGHEST-VALUE FIND OF THE BATCH
Manufacturers' REPRESENTATIVE / commissioned sales agency (est. 1962). Owns no plant, no equipment,
no process. Its only "equipment" is Act Contact Manager + Harris Industrial Guides. Sells the output
of factories that pay it commission.
MEASURED WRONG-PARTY RATE: old pipeline made 59 concept attributions.
  52 wrong-party (4 certs, 14 materials, 18 processes, 1 industry, 15 products)
   5 not-found/fabricated (PET, Zinc Alloy, Bronze, Blanking, Cold Isostatics Pressing)
   2 arguably correct (Aerospace, Agriculture -- its own "Markets Covered")
  => 88% error. Correct answer for 6 of 7 capability fields is THE EMPTY SET.
ROLE FLAGS ALL THREE CORRECT (False/False/False) WHILE CAPABILITY FIELDS WERE FILLED ANYWAY.
  => proves role-flag decision and capability-attribution decision are wired INDEPENDENTLY. That is
     the bug worth pinning to a regression case.
THE SMOKING GUN (line 602): "Sand Castings - Mathews & Company a large range of Aluminum (319, 356
and A357), Ductile ... and Gray Irons ... ISO 9001" -- ungrammatical agency shorthand that puts the
AGENCY NAME IN SUBJECT POSITION of a foundry capability. Hardest single trap in the batch.
TWO CONSTRUCTIONS THE CORPUS LACKS:
  (1) EIGHT ANONYMOUS PRINCIPALS described in first person with NO company name to key on (lines 586-614)
  (2) 12 pages of principals' first-person marketing copy pasted onto Mathews' domain ("We manufacture
      concentric and eccentric contoured forgings" = Ellwood City Forge, not Mathews)
INFERENCE-REQUIRED negative: site NEVER says "we are not a manufacturer"; ~55% of pages argue the
opposite. Must be derived from "sales representative's organization" / "our principals".
Membership-as-certification trap: MANA / Society of Manufacturing Engineers / American Foundrymen's Society.
NOT redundant w/ acimachine: wrong-party-by-CAPABILITY vs wrong-party-by-CATALOG; 6x smaller; and
acimachine always NAMES the brand, Mathews sometimes names nobody.
Mixed emptiness: 6 empty + 1 populated (industries) -- tests SELECTIVE vs uniform emptiness.

## 10th: agstech.net | large-scale | ACCEPT (leaning strong) -- BUT MY SLOT RATIONALE WAS WRONG
Sourcing/integration intermediary w/ partial equity in offshore plants + distributor arm. Self-describes
as "Global Custom Manufacturer, Integrator, Consolidator, Outsourcing Partner" -- 3 of 4 are intermediary
roles. Only US addresses are a PO Box and a marketing suite; NO US plant address in 1.79 MB.
"We own some of these manufacturing plants and have partial ownership in some others."
MY "LARGE-SCALE STRESS CASE" SLOT RATIONALE FAILS: agstech is ~450K tokens; acimachine (already in the
corpus) is ~1.18M. The corpus ALREADY has a bigger chunking case. Do not spend census budget on that claim.
ACCEPT INSTEAD FOR: (1) process-ontology BREADTH -- ~110 distinct processes, ~44 materials in ONE
document, incl. tail concepts (explosive welding, atomic hydrogen welding, percussion welding,
electrochemical grinding, skiving, thread rolling) that appear nowhere else in the corpus;
(2) a NEW trap type: TEXTBOOK-AS-CAPABILITY. ~45 pages are manufacturing-engineering textbook chapters
with "we" injected. Fluent, technically correct, human-written encyclopedia prose where every generic
process description reads as a first-person capability claim. "We use currents between 300 and 2000
Amperes." Not keyword stuffing; not a catalog. PEDAGOGY MISTAKEN FOR CAPABILITY.
(3) ALL 8 CERTIFICATIONS BELONG TO SUPPLIER PLANTS, stated in plain English on its own Quality page:
"All plants manufacturing parts and products for AGS-TECH Inc are certified to..." -- rare UNAMBIGUOUS
ground truth for the attribution failure mode. But the site CONTRADICTS ITSELF in first person on the
Fasteners page ("We manufacture FASTENERS under TS16949, ISO9001"), so it is not trivially graded.
Human-written not AI: consistent non-native typos incl. Turkish dotless i ("artificial", 4x).
Both role-flag NULLS are wrong: is_contract_manufacturer and is_product_manufacturer should be TRUE.
Both are advertised in the PERMANENT NAV BAR on all 165 pages -> old flags look homepage-weighted.
Partly redundant w/ acimachine on its ~90 resold-instrument catalog pages (JANZ TEC, ATOP, SADT, Buehler).
COST CONTROL: stratified census, not full -- all 28 corporate pages + all 45 capability pages +
sample 15-20 of 90 catalog pages. ~30% of bytes are byte-identical boilerplate.
BLOCKING QUESTION BEFORE CENSUS: does a process performed at a PARTIALLY-OWNED partner plant count as
this subject's capability? ANSWERED 2026-08-29 by the user: YES, and screening owns it. SCR-2b was added to the
process_cap and material_cap screening catalogs and reworded in equipment's -- a plant the subject owns outright,
one it part-owns, and a partner plant producing to its direction all count as the subject itself. Deliberately NOT
extended to conformity_attestations (a partner plant's certificate stays that plant's), nor to products or
industries. Judging may proceed on that policy.

## 11th: pradeepmetals.com | non-US (India) | STRONG ACCEPT
Closed-die steel forging house, Navi Mumbai, BSE-listed since 1993. Investor-relations genre CONFIRMED
and justifies inclusion on its own: 24 pages = 30.1% of content bytes (blog 32.3%, capability 29.8%).
NEW TRAP CLASSES THE CORPUS CANNOT CURRENTLY TEST AT ALL:
 - director/officer person-name density (own-name leak of a DIFFERENT kind than steelcraft/Allegion)
 - a directorship register naming ~25 UNRELATED companies, several with manufacturing-sounding names
   (Forgexel Enterprises, Dhanlabh Engineering Works, NRB Bearings, Orient Cement)
 - regulators phrased like standards (SEBI, BSE, Companies Act, IEPF, DSIR)
 - governance policies sitting ADJACENT TO REAL CERTIFICATES on the Quality page ("IMS POLICY")
 - service providers posing as partners (MUFG Intime = registrar; Khaitan & Co = law firm)
 - rupee financial figures interleaved with tonnage capacity figures (Rs 4,602.62 lakhs next to 9200 MTPA)
 - directors' employment histories as capability bait (Lockheed Martin, Boeing, CISCO, Tata Motors)
WIDEST NON-US STANDARDS REGIME IN THE POOL: IBR/Indian Boiler, EIL vendor approval, Indian Railways
approved, Indian Defence approved, ISI, DSIR R&D recognition + European set absent from corpus:
PED 2014/68/EU, AD 2000-Merkblatt W0 (German), NORSOK (Norwegian), CRN (Canadian). Plus AS 9100D.
FORGING-DEPTH PREDICTION FAILED THE SAME WAY AS BLACKADVTECH: of 6 predicted forging processes only
Press Forging is verified (equipment-grounded) and Drop Forging is equipment-inferable. Open Die, Roll,
Cold, Upset are LIST-ONLY -- and Open Die is the explicitly REJECTED alternative (blog titled "HOW
CLOSED DIE FORGING TOPS OPEN DIE FORGING"). Sintering is concluded IMRC LAB RESEARCH, not production.
Casting appears only as the losing comparator.
4 GREP-VERIFIABLE FABRICATIONS: HDPE, LDPE, Teflon, Bronze = ZERO occurrences in 10,412 lines.
"Wind Energy" is INVERTED: their own captive wind mill covering 95% of THEIR OWN energy need, read as
a served market.
Entity trap: DMW (Houston subsidiary) US marketing copy is MIS-TEMPLATED under an "INDIA FACILITIES"
heading -> wrong legal entity AND wrong continent.
Caveats: 33% duplicate page fetches, 44%-of-lines repeated footer, 15 "Coming Soon" stubs,
2 CMS defects ("hdsjkfhsjkd", "test Machine Works"). Does NOT extend the bilingual axis (no Devanagari).

## 12th: howcogroup.com | non-US (Scotland) | STRONG ACCEPT
Metal stockholder/DISTRIBUTOR of high-performance alloy vertically integrated forward into processing.
Sumitomo Corporation subsidiary since 2006. 12 sites / 6 countries.
NON-US STANDARDS REGIME CONFIRMED AND IS THE HEADLINE: old pipeline found 6 certs -- every one a
globally-generic ISO the US subjects also have -- and MISSED 100% of the British + oil&gas regime:
ISO 17025, UKAS, ISO 3834-2, PCN Level II/III, CSWIP 3.2, ICorr, ASNT NDE II, API 5B, API 7-2,
API Spec Q1, AMS 2750 Class 3, ASME IX, EN ISO 9606-1, "Made in Sheffield". A straight, reproducible
recall hole, not a judgment call.
NOTE: my brief's guesses were WRONG -- CE, UKCA, PED, ATEX, Achilles, FPAL all have ZERO occurrences.
The gap this fills is the UKAS/PCN/CSWIP/NORSOK/ISO-3834 axis, not CE marking.
SCHEMA-LEVEL FINDING: the role_flags have NO SLOT FOR "DISTRIBUTOR". ~half the site (33 materials-grade
pages, consignment/JIT, mill relationships) is selling metal it did not make. acimachine resells
MACHINES; Howco resells the MATERIAL the ontology is built around. Will be systematically mis-scored.
MULTI-ENTITY SCOPING HARDER THAN STEELCRAFT: facts at different geographic scope in identical voice --
AS9100D is Houston-only, ISO 17025 Sheffield-only, API 5B Dubai-only, ISO 50001 UK-only. Plus a PHANTOM
ENTITY "Drummond" holding 3 certificates and appearing NOWHERE ELSE on the site. Site contradicts
itself on 9 vs 10 vs 12 locations.
SUPPLY-CHAIN-PARTNER AMBIGUITY (better than steelcraft's, because genuinely undecidable):
"providing fabrication services via our established supply chain partners" -- welding, cladding,
fabrication and HIP cannot be assigned from the text alone.
7 CRISP DEFECT EXEMPLARS: SLM280 (machine MODEL -> process "Selective Laser Melting" AND -> product);
Hastelloy (partner's cladding consumable -> own material); Plastic/Polyethylene/Rubber (customer's END
PRODUCT -> own material); Automotive (FURNACE VENDOR'S BROCHURE COPY -> served industry);
Tungsten/Copper/Chromium (COMPOSITION-TABLE HEADER -> material); PCB Assembly (pure fabrication from
"electromechanical assembly"); Nuclear (bare suitability-list token -> served industry).
Teflon/LDPE/Acrylic/Polypropylene/PCB = ZERO occurrences each.

# ============ FINAL: 12 of 12 ACCEPTED. ZERO REJECTIONS. ============
# 11 STRONG ACCEPT + 1 ACCEPT-leaning-strong (agstech, on revised rationale)
