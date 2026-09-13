
## conformity_attestations: 150 rows (malformed 0, duplicate 0, missing 0, extra 0)
| axis | stratum | n | carried | absent | contradicted | na | absent+contradicted % |
|---|---|---:|---:|---:|---:|---:|---:|
| A | fail | 50 | 40 | 8 | 2 | 0 | 20.0 |
| A | pass | 100 | 97 | 1 | 2 | 0 | 3.0 |
| A | all | 150 | 137 | 9 | 4 | 0 | 8.7 |
| B | fail | 50 | 3 | 0 | 0 | 47 | 0.0 |
| B | pass | 100 | 17 | 0 | 0 | 83 | 0.0 |
| B | all | 150 | 20 | 0 | 0 | 130 | 0.0 |
| C | fail | 50 | 46 | 2 | 2 | 0 | 8.0 |
| C | pass | 100 | 98 | 0 | 2 | 0 | 2.0 |
| C | all | 150 | 144 | 2 | 4 | 0 | 4.0 |
| D | fail | 50 | 21 | 3 | 26 | 0 | 58.0 |
| D | pass | 100 | 89 | 5 | 6 | 0 | 11.0 |
| D | all | 150 | 110 | 8 | 32 | 0 | 26.7 |
| E | fail | 50 | 26 | 0 | 1 | 23 | 2.0 |
| E | pass | 100 | 91 | 0 | 3 | 6 | 3.0 |
| E | all | 150 | 117 | 0 | 4 | 29 | 2.7 |
| F | fail | 50 | 30 | 0 | 20 | 0 | 40.0 |
| F | pass | 100 | 97 | 0 | 3 | 0 | 3.0 |
| F | all | 150 | 127 | 0 | 23 | 0 | 15.3 |
| G | fail | 50 | 23 | 6 | 21 | 0 | 54.0 |
| G | pass | 100 | 92 | 4 | 4 | 0 | 8.0 |
| G | all | 150 | 115 | 10 | 25 | 0 | 23.3 |
| H | fail | 50 | 21 | 5 | 24 | 0 | 58.0 |
| H | pass | 100 | 86 | 4 | 10 | 0 | 14.0 |
| H | all | 150 | 107 | 9 | 34 | 0 | 28.7 |
| field_item | fail | 50 | 19 | 7 | 24 | 0 | 62.0 |
| field_item | pass | 100 | 82 | 10 | 8 | 0 | 18.0 |
| field_item | all | 150 | 101 | 17 | 32 | 0 | 32.7 |
worst axis distribution: {'none': 95, 'D': 40, 'H': 5, 'A': 4, 'C': 4, 'E': 2}
A examples (13):
  - absent `e294697e9e7a2a61d382`: Never says this is a material specification the ALLOY 625 PLUS product is supplied to; calls it only 'this designation'. | «## API 6A CRA (UNS N07716) : AMS 5854 : ASTM B805 : AP16A)»
  - contradicted `e3ab06c27ef3663d3400`: The snippet is a comma-joined list of two separate certificates; the paragraph folds AD2000-Merkblatt W0 inside the PED designation as if it were part of it. | «including AD2000-MERKBLATT W0»
  - absent `1600471b95bc080974f2`: Never says this is the material specification the GRADE 410 stainless on the page is supplied to. | «## UNS S41000 : 13 CR. STAINLESS : ASTM A182, A276, A479»
  - contradicted `88f1db521382a4841dc2`: DOE is a federal agency listed as a market/industry focus in these snippets; the paragraph turns it into a standards-setter with standards the subject meets. | «is able to meet Department of Energy (DOE) standards for laser marking»
C examples (6):
  - contradicted `ac4df39ba0cb9da96226`: The snippet bullet is agentless definitional prose ('Compliance with recognized industry standards ensures that processes and products meet ... benchmarks') under a how-to heading; the subject is supplied as the agent. | «Blackstone Advanced Technologies adheres to industry standards, including ASTM»
  - contradicted `ab20008eff3172a32493`: Agentless definitional bullet under an explainer heading is attributed to the subject. | «Blackstone Advanced Technologies adheres to industry standards (ASTM, ASME) and custom specifications»
  - absent `f4d35fc507304280c2f4`: Says the standard is 'held by all United Kingdom manufacturing sites' without ever attaching those sites to Howco, then denies a holding altogether. | «#### LRQA ISO 50001 ENERGY STANDARD - HELD BY ALL UNITED KINGDOM MANUFACTURING SITES»
  - absent `4c19dcf47daa68c81c16`: The holding sites are never attached to Howco. | «HELD BY ALL UNITED KINGDOM MANUFACTURING SITES»
D examples (40):
  - contradicted `81edc3fe95e912c054c7`: The heading 'UT to AMS-STD-2154 Class AA' on an alloy product page asserts material supplied ultrasonically tested to that class; the paragraph reduces it to offering a document. | «the only direct dealing shown in the snippets is the offering of a document or information bearing this name»
  - contradicted `e294697e9e7a2a61d382`: The snippet is a spec heading on an alloy product page, not a document library. | «The only dealing shown between Howco Group and API 6A CRA (UNS N07716) in these snippets is the offering of documentation and policy statements related to this »
  - contradicted `b2aa3e1c14f488bba639`: The snippet says only that full certification per EN 10204 3.1 'was required' in a client spec; fulfilment as a standing service is supplied. | «Alec Model fulfills this requirement as part of its manufacturing services»
  - contradicted `2f9e757fe3747f9e2616`: The heading sits in an accreditations page whose sibling heading is 'LRQA AS9100D: QUALITY ASSURANCE'; it asserts Howco is registered by LRQA, not that it offers a document. | «The only dealing shown is Howco Group offering a document or certificate related to LRQA ISO 14001»
E examples (4):
  - contradicted `1ab2f5acb06587f29f92`: Snippet is past-tense project narrative ('Welded frames were fabricated'); paragraph renders it as standing present practice. | «employs ASME-certified welders to fabricate welded frames»
  - contradicted `cd42591b0263cfc55d27`: Completed past project turned into present standing practice. | «uses ASME-certified welders to fabricate welded frames»
  - contradicted `b2aa3e1c14f488bba639`: A past project requirement turned into an ongoing offering. | «fulfills this requirement as part of its manufacturing services»
  - contradicted `8e5294fb5558f6bcf532`: A single past project's requirement rendered as a standing present activity. | «prepares REACH/RoHS declarations, along with other documentation»
F examples (23):
  - contradicted `e294697e9e7a2a61d382`: Puts the entity on the 'word in a document title' side when the page frame makes it the spec the supplied alloy meets. | «the offering of documentation and policy statements related to this designation»
  - contradicted `2f9e757fe3747f9e2616`: Places the entity on the document side when the frame makes it a credential held. | «offering a document or certificate related to»
  - contradicted `53a0e23d40041e825d17`: Places the spec on the document-title side against the product-page frame. | «the offering of documents and statements bearing the name»
  - contradicted `145f9f682a95e1bef806`: Spec put on the document side against the product-page frame. | «the offering of documentation and policy statements»
G examples (35):
  - absent `1ab2f5acb06587f29f92`: Paragraph never says the line sits in a project case study rather than a capabilities or certifications page. | «## Challenges & Solutions»
  - absent `cd42591b0263cfc55d27`: Case-study frame not stated. | «## Challenges & Solutions»
  - contradicted `81edc3fe95e912c054c7`: The repeated 'X: Data Privacy' lines are heading-propagated footer nav, not documents; the paragraph reads that artifact as the frame and concludes 'document offered'. | «referenced as a heading and in a series of footer and policy links»
  - contradicted `e294697e9e7a2a61d382`: Heading-propagated footer nav read as the frame. | «referenced in the context of a heading and a series of footer and policy statements»
H examples (43):
  - contradicted `81edc3fe95e912c054c7`: Downgrades a product-page 'tested to' assertion into a bare reference. | «references AMS-STD-2154 Class AA in relation to ultrasonic testing standards»
  - contradicted `e294697e9e7a2a61d382`: Downgrades a supplied-to-spec claim to nothing. | «No further dealing is shown.»
  - contradicted `b2aa3e1c14f488bba639`: Upgrades a customer's stated requirement into the subject's own delivered attestation. | «Alec Model fulfills this requirement»
  - contradicted `2f9e757fe3747f9e2616`: Certified-by-LRQA strength downgraded to a document offer; LRQA never identified as the registrar. | «The only dealing shown is Howco Group offering a document»
field_item examples (49):
  - contradicted `81edc3fe95e912c054c7`: Mode should be 'material tested to' with Howco's product as holder; the paragraph supplies 'document offered'. | «the offering of a document or information bearing this name»
  - contradicted `e294697e9e7a2a61d382`: Mode and holder both replaced by a document-offer reading. | «the offering of documentation and policy statements»
  - contradicted `b2aa3e1c14f488bba639`: Mode should be 'required by the customer'; the paragraph converts it into the subject supplying the certification. | «Alec Model fulfills this requirement as part of its manufacturing services»
  - contradicted `2f9e757fe3747f9e2616`: Mode and holder both lost; LRQA not named as issuing body. | «offering a document or certificate related to LRQA ISO 14001»

## equipments: 150 rows (malformed 0, duplicate 0, missing 0, extra 0)
| axis | stratum | n | carried | absent | contradicted | na | absent+contradicted % |
|---|---|---:|---:|---:|---:|---:|---:|
| A | fail | 24 | 24 | 0 | 0 | 0 | 0.0 |
| A | pass | 126 | 120 | 5 | 1 | 0 | 4.8 |
| A | all | 150 | 144 | 5 | 1 | 0 | 4.0 |
| B | fail | 24 | 6 | 0 | 0 | 18 | 0.0 |
| B | pass | 126 | 41 | 0 | 0 | 85 | 0.0 |
| B | all | 150 | 47 | 0 | 0 | 103 | 0.0 |
| C | fail | 24 | 18 | 0 | 6 | 0 | 25.0 |
| C | pass | 126 | 125 | 0 | 1 | 0 | 0.8 |
| C | all | 150 | 143 | 0 | 7 | 0 | 4.7 |
| D | fail | 24 | 16 | 0 | 8 | 0 | 33.3 |
| D | pass | 126 | 115 | 2 | 9 | 0 | 8.7 |
| D | all | 150 | 131 | 2 | 17 | 0 | 12.7 |
| E | fail | 24 | 0 | 1 | 0 | 23 | 4.2 |
| E | pass | 126 | 13 | 0 | 0 | 113 | 0.0 |
| E | all | 150 | 13 | 1 | 0 | 136 | 0.7 |
| F | fail | 24 | 23 | 0 | 1 | 0 | 4.2 |
| F | pass | 126 | 126 | 0 | 0 | 0 | 0.0 |
| F | all | 150 | 149 | 0 | 1 | 0 | 0.7 |
| G | fail | 24 | 16 | 1 | 7 | 0 | 33.3 |
| G | pass | 126 | 104 | 10 | 5 | 7 | 11.9 |
| G | all | 150 | 120 | 11 | 12 | 7 | 15.3 |
| H | fail | 24 | 19 | 0 | 5 | 0 | 20.8 |
| H | pass | 126 | 126 | 0 | 0 | 0 | 0.0 |
| H | all | 150 | 145 | 0 | 5 | 0 | 3.3 |
| field_item | fail | 24 | 15 | 1 | 8 | 0 | 37.5 |
| field_item | pass | 126 | 109 | 9 | 8 | 0 | 13.5 |
| field_item | all | 150 | 124 | 10 | 16 | 0 | 17.3 |
worst axis distribution: {'none': 111, 'D': 13, 'G': 11, 'C': 6, 'H': 4, 'A': 4, 'F': 1}
A examples (6):
  - absent `0f11905812422a8632b7`: The heading settles that C Series frames are a casing-ready door-frame series of Steelcraft's own; the paragraph never says what they are. | «## C & CK SERIES CASING-READY FRAMES»
  - absent `a6b252025e3d314cd07a`: The maker Mazak and the 4th-axis / vertical-and-horizontal configuration of the system are dropped from its designation. | «latest Mazak Palletech CNC Milling with 4th Axis, in both Vertical & Horizontal Configurations»
  - absent `ac63f51ce35aa3e3cc81`: The equipment-used table names a specific machine, Zeiss CONTURA G2; the paragraph keeps only the generic kind. | «| Hole Position Tolerance | ±0.02 mm | 0.015 mm | Zeiss CONTURA G2 CMM |»
  - absent `5d63a6bee5f9eac2e681`: The heading settles what the machine is - a bar feed lathe or Swiss turning centre - but the paragraph says only 'a piece of equipment'. | «### Bar Feed Lathes & Swiss T/C»
C examples (7):
  - contradicted `add92e0bb775bdeddec2`: The snippet is agentless how-to prose ('methods ... are employed') under a blog heading; no party is shown. | «indicating the manufacturer uses magnetic particle inspection as an inspection method»
  - contradicted `5e704a1cbf5dabbfcfb8`: The snippet is an agentless bullet in a how-to list ('provide precise measurements during production'); no owner is shown. | «The manufacturer, Blackstone Advanced Technologies, employs 3D laser scanners»
  - contradicted `071fba61bbefa7d3308b`: The snippet is agentless how-to prose ('methods ... are employed') under a blog heading; no party is shown. | «indicating the manufacturer uses eddy current testing as an inspection method»
  - contradicted `d91181e3719cf5d81f94`: The single snippet is an agentless bullet in a strategies list; no owner or provider is shown. | «indicating the manufacturer uses or provides a variety of advanced machines and equipment»
D examples (19):
  - contradicted `add92e0bb775bdeddec2`: Own-use capacity supplied where the snippet states a generic role of inspection processes. | «This is presented as part of Blackstone Advanced Technologies' quality control practices»
  - contradicted `770ac7198c75b7efe5d5`: The '## Related Products' frame settles a product entry; the paragraph substitutes a document offering. | «shows only that Tanfel offers a document or webpage bearing the name»
  - contradicted `4c902e9ef6b17639bd27`: 'mro' appears as a tag, but no snippet mentions PMA or parts-manufacturing approval. | «available as part of Able's MRO and PMA (Parts Manufacturer Approval) offerings»
  - contradicted `399e835f7977270f84c3`: '## Related Products' settles a product entry, not a document offering. | «shows only that Tanfel offers a document or webpage bearing the name»
E examples (1):
  - absent `9e68275fcdc6250e89f9`: The row's dates - one entry 2006-10, this one 2006 ongoing - are not carried. | «Smelting-reduction of iron ore (Microwave Composite Heating Furnace) Read More 2006 ongoing»
F examples (1):
  - contradicted `01de9995f12c98a3d083`: The snippet has machine tools as one of the applications properties lead to ('wide use in precision control systems, heavy-duty machine tools, transportation'), not as the thing used in those applications. | «heavy-duty machine tools are noted for their wide use in precision control systems, transportation, marine, and aerospace applications»
G examples (23):
  - contradicted `add92e0bb775bdeddec2`: Frame is the explainer heading '## Implementing Quality Controls for Consistent Precision', not a statement of the company's practices. | «presented as part of Blackstone Advanced Technologies' quality control practices»
  - absent `2b526a8f7eaa5166d35d`: The explainer character of the sentence is not carried; the paragraph reads as an own-shop claim. | «The database developed by CAD is further processed by CAM into the necessary data and instructions»
  - contradicted `4514b729e1815493c067`: Locations are empty and the snippet is a bare comma list; the quality-department placement is supplied. | «part of the equipment featured in Decimal Engineering, Inc.'s quality department»
  - absent `0f11905812422a8632b7`: The product-series section frame is not named, so the reader cannot tell this is Steelcraft's own line. | «## C & CK SERIES CASING-READY FRAMES»
H examples (5):
  - contradicted `77983bdfaeff79004684`: No snippet or heading in this record carries an FAA approval claim; the credential is supplied. | «FAA-approved replacement parts»
  - contradicted `67f66b19173b3780f498`: Nothing in this record's snippets or headings claims FAA approval. | «its catalog of FAA-approved replacement parts»
  - contradicted `07db74a356b4fdf467ef`: No FAA claim appears in this record's snippets or headings. | «provides FAA-approved replacement parts»
  - contradicted `8480acefeedca111dbb2`: No snippet or heading mentions a weld inspector; the only AWS reference is 'Robotic welding cells built to AWS standards'. | «a full-time AWS Certified Weld Inspector on staff»
field_item examples (26):
  - contradicted `add92e0bb775bdeddec2`: An explainer of how inspection is done is converted into own-floor practice. | «indicating the manufacturer uses magnetic particle inspection»
  - contradicted `770ac7198c75b7efe5d5`: Neither offered-for-sale nor own-floor use is stated; a related-products entry is downgraded to a document. | «offers a document or webpage bearing the name»
  - contradicted `399e835f7977270f84c3`: Fails to place the gauges as an offered product line item. | «offers a document or webpage bearing the name»
  - absent `55283cd74c0d12843276`: Does not say whether Mathews represents/sells the line or merely documents it. | «| Centrifugal Castings johnsoncentrifugal.com |»

## industries: 150 rows (malformed 0, duplicate 0, missing 0, extra 0)
| axis | stratum | n | carried | absent | contradicted | na | absent+contradicted % |
|---|---|---:|---:|---:|---:|---:|---:|
| A | fail | 37 | 36 | 0 | 1 | 0 | 2.7 |
| A | pass | 113 | 111 | 2 | 0 | 0 | 1.8 |
| A | all | 150 | 147 | 2 | 1 | 0 | 2.0 |
| B | fail | 37 | 3 | 2 | 7 | 25 | 24.3 |
| B | pass | 113 | 10 | 2 | 2 | 99 | 3.5 |
| B | all | 150 | 13 | 4 | 9 | 124 | 8.7 |
| C | fail | 37 | 28 | 6 | 3 | 0 | 24.3 |
| C | pass | 113 | 104 | 2 | 7 | 0 | 8.0 |
| C | all | 150 | 132 | 8 | 10 | 0 | 12.0 |
| D | fail | 37 | 20 | 6 | 11 | 0 | 45.9 |
| D | pass | 113 | 93 | 6 | 14 | 0 | 17.7 |
| D | all | 150 | 113 | 12 | 25 | 0 | 24.7 |
| E | fail | 37 | 21 | 1 | 0 | 15 | 2.7 |
| E | pass | 113 | 87 | 0 | 0 | 26 | 0.0 |
| E | all | 150 | 108 | 1 | 0 | 41 | 0.7 |
| F | fail | 37 | 29 | 8 | 0 | 0 | 21.6 |
| F | pass | 113 | 111 | 1 | 1 | 0 | 1.8 |
| F | all | 150 | 140 | 9 | 1 | 0 | 6.7 |
| G | fail | 37 | 31 | 4 | 2 | 0 | 16.2 |
| G | pass | 113 | 103 | 8 | 2 | 0 | 8.8 |
| G | all | 150 | 134 | 12 | 4 | 0 | 10.7 |
| H | fail | 37 | 36 | 1 | 0 | 0 | 2.7 |
| H | pass | 113 | 112 | 1 | 0 | 0 | 0.9 |
| H | all | 150 | 148 | 2 | 0 | 0 | 1.3 |
| field_item | fail | 37 | 24 | 8 | 5 | 0 | 35.1 |
| field_item | pass | 113 | 106 | 6 | 1 | 0 | 6.2 |
| field_item | all | 150 | 130 | 14 | 6 | 0 | 13.3 |
worst axis distribution: {'none': 97, 'D': 24, 'C': 12, 'B': 8, 'G': 6, 'H': 2, 'A': 1}
A examples (3):
  - contradicted `4092d87c605026ecbdf0`: the snippet names no material; 'mild steel' is supplied | «using highly weldable and durable mild steel»
  - absent `2c99f09b63df8a650221`: the bare word sits on the patents page; the paragraph says only 'a standalone mention' and never says where or what it is | «# PATENTS»
  - absent `98f43ecc57809440e98d`: the focal token is only a word inside 'Test Equipment' and 'Automotive Test Equipment'; the paragraph never says the bare word names nothing | «- Test Equipment»
B examples (13):
  - contradicted `eb85a09d2cd493bc469d`: in the second snippet the term sits inside the organiser's name (Powder Metallurgy Association of India) for a ceramics/carbides workshop. | «These references indicate that Pradeep Metals Limited has presented or participated in events related to Powder Metallurgy»
  - contradicted `bef8fd3ccb88e8b82f16`: every occurrence is the physics word inside 'electromagnetic energy', 'microwave energy', 'energy conservation' or an 'energy monitor meter', not the energy sector | «is involved in activities and projects related to energy»
  - absent `de2c354eeda03eda6440`: occurrences inside the company's own name and in boilerplate are not separated from the activity sense | «Anchor Manufacturing Group is committed to protecting your privacy.»
  - contradicted `3a98d7d92e91b9430dad`: those properties come from generic material-table lines ('used in aerospace, automotive, and medical industries'), not from aerospace work | «Alec Model's activities in the aerospace field include manufacturing components that require high heat resistance, mechanical strength, and tight tolerances»
C examples (18):
  - absent `2b5312cb348ccacd4591`: the focal entity IS the subject's client; the paragraph reduces it to a document name | «### 🇺🇸 Ohio-Based Industrial Automation Client | Custom CNC Machining of Aluminum Mounting Brackets for Robotic Gripper Assembly»
  - contradicted `eb85a09d2cd493bc469d`: the snippets are paper/venue lines under research-paper headings and never name who attended. | «Pradeep Metals Limited has presented or participated in events»
  - absent `2bd8e6dcf949c06bc1b5`: the sentence is an agentless benefit claim; the paragraph never says no dealing with marine or food equipment is shown. | «Electropolishing is not just for aesthetics - it's a matter of functionality and longevity in Marine and food processing equipment.»
  - contradicted `bef8fd3ccb88e8b82f16`: the first snippet is an executive's personal bio and the venue lines sit under research-paper headings | «These activities and references are attributed to Pradeep Metals Limited's own dealings.»
D examples (37):
  - contradicted `9f3b89eab45658ba94b7`: the snippet gives only usability ('can be used in'), not a supply relation to restaurants. | «manufacturer and supplier of these doors for restaurant applications»
  - contradicted `c4b1cddeb86e158f9407`: the snippet lists products across all the named industries collectively; the pairing with this one sector is supplied | «assembling and testing products such as fuel injectors, valves, and regulators»
  - absent `2b5312cb348ccacd4591`: the title states custom CNC machining of aluminium mounting brackets done for that client | «### 🇺🇸 Ohio-Based Industrial Automation Client | Custom CNC Machining of Aluminum Mounting Brackets for Robotic Gripper Assembly»
  - contradicted `c1b3950c3609ce9324cd`: private labelling does not settle manufacture versus resale. | «AGS-TECH Inc. offers to manufacture and supply mobile computers for enterprises»
E examples (1):
  - absent `1c798f25e38f4d0ededc`: a 1992 magazine quote is reported as a present company value. | «As Bob told us, "Finding niches and filling them means being able to open your eyes as you visit plants and manufacturing facilities»
F examples (10):
  - absent `2b5312cb348ccacd4591`: the customer side fixed by the word 'Client' is left unstated | «### 🇺🇸 Ohio-Based Industrial Automation Client | Custom CNC Machining of Aluminum Mounting Brackets for Robotic Gripper Assembly»
  - absent `bef8fd3ccb88e8b82f16`: the paragraph never says the word sits inside other terms rather than naming a sector or an activity | «Continuous process for baking of cured friction material using electromagnetic energy»
  - absent `5d129d006148c57800cb`: the word 'Client' fixes the customer side; the paragraph leaves it as a bare title word | «### 🇺🇸 Ohio-Based Industrial Automation Client | Custom CNC Machining of Aluminum Mounting Brackets for Robotic Gripper Assembly»
  - absent `65b031007ca1024fa98f`: the client-sector side of the word is not stated at all | «an automation integrator specializing in high-speed packaging lines for the food and personal care sectors»
G examples (16):
  - contradicted `0398a81f22bcb7707325`: locations are empty and the snippets are bare captions; the gallery frame is supplied. | «in a gallery of Decimal Engineering, Inc.'s work»
  - absent `eb85a09d2cd493bc469d`: the paper-list frame that the venue lines sit under is not carried. | «##### 01 Microwave Sintering: A New Promising Option in PM»
  - absent `bef8fd3ccb88e8b82f16`: bio, paper-list and amenities frames are merged into company activity | «#### Dr. Kewal K. Nohria»
  - absent `de2c354eeda03eda6440`: gallery caption, privacy policy and careers copy are folded into capability prose with no frame named | «#### Our gallery is dedicated to manufacturing artistry, from welded steel, vibrant panels showcasing the beauty of powder-coated finishes, or intricate metalwo»
H examples (2):
  - absent `1c798f25e38f4d0ededc`: a magazine interview of an individual is passed off as unattributed company copy. | «As Bob told us, "Finding niches and filling them means being able to open your eyes as you visit plants and manufacturing facilities»
  - absent `39a0bdc46be293738344`: the snippet scopes AS9100 to one project's requirements; the paragraph states it as standing adherence | «At Decimal Engineering, we are ISO 9000:2008 compliant and had to adhere to several standards related to this project, including AS9100 quality management for A»
field_item examples (20):
  - absent `2b5312cb348ccacd4591`: no side is stated for a phrase that literally says 'Client' | «### 🇺🇸 Ohio-Based Industrial Automation Client | Custom CNC Machining of Aluminum Mounting Brackets for Robotic Gripper Assembly»
  - absent `2bd8e6dcf949c06bc1b5`: no side is named: the reader cannot tell whether these are served markets or only an explainer. | «Electropolishing is not just for aesthetics - it's a matter of functionality and longevity in Marine and food processing equipment.»
  - contradicted `bef8fd3ccb88e8b82f16`: no side is named and the reader is left with energy-sector involvement | «is involved in activities and projects related to energy»
  - absent `5d129d006148c57800cb`: the customers'-sector side is available in the title and not stated | «### 🇺🇸 Ohio-Based Industrial Automation Client | Custom CNC Machining of Aluminum Mounting Brackets for Robotic Gripper Assembly»

## material_caps: 150 rows (malformed 0, duplicate 0, missing 0, extra 0)
| axis | stratum | n | carried | absent | contradicted | na | absent+contradicted % |
|---|---|---:|---:|---:|---:|---:|---:|
| A | fail | 47 | 43 | 1 | 3 | 0 | 8.5 |
| A | pass | 103 | 94 | 5 | 4 | 0 | 8.7 |
| A | all | 150 | 137 | 6 | 7 | 0 | 8.7 |
| B | fail | 47 | 4 | 2 | 3 | 38 | 10.6 |
| B | pass | 103 | 32 | 3 | 0 | 68 | 2.9 |
| B | all | 150 | 36 | 5 | 3 | 106 | 5.3 |
| C | fail | 47 | 39 | 0 | 6 | 2 | 12.8 |
| C | pass | 103 | 97 | 1 | 5 | 0 | 5.8 |
| C | all | 150 | 136 | 1 | 11 | 2 | 8.0 |
| D | fail | 47 | 20 | 1 | 26 | 0 | 57.4 |
| D | pass | 103 | 79 | 2 | 22 | 0 | 23.3 |
| D | all | 150 | 99 | 3 | 48 | 0 | 34.0 |
| E | fail | 47 | 1 | 0 | 0 | 46 | 0.0 |
| E | pass | 103 | 29 | 0 | 2 | 72 | 1.9 |
| E | all | 150 | 30 | 0 | 2 | 118 | 1.3 |
| F | fail | 47 | 36 | 2 | 9 | 0 | 23.4 |
| F | pass | 103 | 102 | 0 | 1 | 0 | 1.0 |
| F | all | 150 | 138 | 2 | 10 | 0 | 8.0 |
| G | fail | 47 | 17 | 19 | 9 | 2 | 59.6 |
| G | pass | 103 | 76 | 25 | 2 | 0 | 26.2 |
| G | all | 150 | 93 | 44 | 11 | 2 | 36.7 |
| H | fail | 47 | 45 | 0 | 0 | 2 | 0.0 |
| H | pass | 103 | 100 | 0 | 2 | 1 | 1.9 |
| H | all | 150 | 145 | 0 | 2 | 3 | 1.3 |
| field_item | fail | 47 | 16 | 14 | 17 | 0 | 66.0 |
| field_item | pass | 103 | 86 | 13 | 4 | 0 | 16.5 |
| field_item | all | 150 | 102 | 27 | 21 | 0 | 32.0 |
worst axis distribution: {'none': 81, 'D': 28, 'G': 14, 'C': 12, 'A': 6, 'B': 5, 'F': 3, 'H': 1}
A examples (13):
  - absent `2b8e9981b17a0f364fdc`: The column the grade sits in settles it as a stainless steel grade; the paragraph says only 'material grades'. | «| STAINLESS STEEL | Alloy Steel | Duplex | Carbon Steel | Die Steel |»
  - contradicted `7e25d3984dacf692af8c`: The focal entity is a five-way steel list (cold rolled, hot rolled, carbon, structural, tool); the conclusion names only the first. | «the company works with Cold Rolled steel as part of its machining services»
  - contradicted `48ad8d5a0db5a0ccc823`: The snippet reads only 'The lightest structural metal, used in automotive and aerospace applications'; magnesium is supplied from outside. | «the use of metals such as magnesium, noted as the lightest structural metal»
  - absent `562607953be65afc69fc`: The co-listed plastics settle PP as a polymer; the paragraph never says what PP is. | «Materials include: PC, ABS, PMMA, HDPE, PVC, PP, PU, Nylon, PBT, LCP»
B examples (8):
  - absent `76206c23a80ee186ec0b`: The focal word occurs only inside a different alloy's name; the paragraph never says silicon itself is not the listed material. | «- Silicon Tombac»
  - absent `0d7f0ab58bc105945e7f`: The focal words occur only inside the item name; the paragraph never says the material itself is not what is listed. | «Standard Rigging Hardware - Synthetic Plastic Ropes»
  - absent `562607953be65afc69fc`: A bare two-letter abbreviation is left unresolved though the list fixes the sense. | «Materials include: PC, ABS, PMMA, HDPE, PVC, PP, PU, Nylon, PBT, LCP»
  - absent `96d8340104e6bde9e3e7`: The focal word occurs only inside 'powder metallurgy'; the closing clause generalises to 'their dealing with metallurgy'. | «powder metallurgy»
C examples (12):
  - contradicted `c20ee73c515bc72943f4`: The snippet's actor is 'companies' in general, not FZE. | «the electropolishing process provided by FZE Manufacturing Solutions»
  - contradicted `75312d031faf46c84ccb`: The snippet is agentless definitional prose ('Materials used for machine elements could range from ...'). | «AGS-TECH Inc. uses molded plastics»
  - contradicted `bcbe57675606615d6137`: The snippet is a general prescription: 'Fabrication shops must ensure that suppliers consistently provide materials ...'. | «The company ensures that suppliers provide stainless materials meeting required specifications»
  - contradicted `1c78fa357cc8bb7f16ac`: The snippet's actor is 'companies' in general. | «the electropolishing process provided by FZE Manufacturing Solutions»
D examples (51):
  - contradicted `2b8e9981b17a0f364fdc`: The table carries no verb; 'used in its manufacturing processes' is a supplied capacity. | «deals with 1.4541 as a material used in its manufacturing processes»
  - contradicted `66c8d9ef0c9df527c3f2`: The '### Capabilities' heading settles this as Tanfel's own material capability; the paragraph denies a dealing is shown. | «the snippets do not show any specific dealing by Tanfel with Super Alloy»
  - contradicted `c20ee73c515bc72943f4`: The explainer sentence gives no FZE dealing with titanium at all. | «The manufacturer's dealing is offering electropolishing services for titanium»
  - contradicted `2bf74f34c70bcdda34ba`: The line sits under a rigging-hardware manufacturing heading; nothing in the snippet makes it a document. | «the only dealing shown is the offering of a document bearing the name Synthetic Plastic Ropes»
E examples (2):
  - contradicted `75312d031faf46c84ccb`: 'could range from' is hypothetical; the paragraph makes it actual present use. | «uses molded plastics as one of the materials»
  - contradicted `3402f2ab1450af6404db`: A specification still to be met is rendered as a completed action. | «applied»
F examples (12):
  - absent `2e41ca584165d0a401ee`: The title settles the steel as the material a stamped ring is made of; the paragraph never states that side. | «Stamping Yellow Zinc Plated Steel Ring»
  - contradicted `2bf74f34c70bcdda34ba`: Puts the entity on the document-title side where the snippet has it as a hardware item. | «offers information about synthetic plastic ropes»
  - contradicted `0d7f0ab58bc105945e7f`: Puts the entity on the document-title side where the snippet has a hardware item. | «offers information about synthetic plastic ropes»
  - contradicted `0254a7ca289354146c87`: Puts the entity on the document-title side where the snippet has a hardware item. | «offers information about Polyhemp ropes»
G examples (55):
  - absent `66c8d9ef0c9df527c3f2`: Paragraph says only 'is listed', never that the list sits under the manufacturer's capabilities heading. | «### Capabilities»
  - absent `c20ee73c515bc72943f4`: Generic explainer prose under an industries-served heading; the paragraph presents it as an own-service statement. | «In addition to stainless steel, companies use the process to finish»
  - absent `249974002a5abc765a3f`: The sentence sits on the manufacturer's own grade datasheet page; the paragraph gives no frame. | «## UNS S17400 : ASTM A564 GRADE 630»
  - absent `0248ee14bc1d240fc7a3`: Frame is the manufacturer's own alloy datasheet page; the paragraph does not carry it. | «## UNS N06625: ASTM B446 & B564»
H examples (2):
  - contradicted `8dbbecad7c7cc202787d`: A company-level certification is tied to a material grade. | «The registered office address and ISO 9001:2015 certification are also listed in connection with LOW ALLOY 4145: ASTM A29»
  - contradicted `5611b3b8b9ea03c2adc0`: No issuing body appears anywhere in the snippet; the attribution is supplied. | «according to the American Boat and Yacht Council (ABYC)»
field_item examples (48):
  - absent `2b8e9981b17a0f364fdc`: Grade designation kept but the material it is a form of is not made visible. | «| STAINLESS STEEL | Alloy Steel | Duplex |»
  - absent `66c8d9ef0c9df527c3f2`: Never says whether the family is a thing Tanfel works or supplies. | «### Capabilities»
  - contradicted `c20ee73c515bc72943f4`: Asserts FZE works titanium where the snippet only says companies in general do. | «offering electropolishing services for titanium»
  - absent `2e41ca584165d0a401ee`: Does not say the focal material is what a part in the title is made of. | «Stamping Yellow Zinc Plated Steel Ring»

## process_caps: 150 rows (malformed 0, duplicate 0, missing 0, extra 0)
| axis | stratum | n | carried | absent | contradicted | na | absent+contradicted % |
|---|---|---:|---:|---:|---:|---:|---:|
| A | fail | 47 | 41 | 4 | 2 | 0 | 12.8 |
| A | pass | 103 | 99 | 3 | 1 | 0 | 3.9 |
| A | all | 150 | 140 | 7 | 3 | 0 | 6.7 |
| B | fail | 47 | 8 | 4 | 4 | 31 | 17.0 |
| B | pass | 103 | 10 | 1 | 0 | 92 | 1.0 |
| B | all | 150 | 18 | 5 | 4 | 123 | 6.0 |
| C | fail | 47 | 30 | 3 | 14 | 0 | 36.2 |
| C | pass | 103 | 96 | 3 | 4 | 0 | 6.8 |
| C | all | 150 | 126 | 6 | 18 | 0 | 16.0 |
| D | fail | 47 | 33 | 0 | 14 | 0 | 29.8 |
| D | pass | 103 | 91 | 3 | 9 | 0 | 11.7 |
| D | all | 150 | 124 | 3 | 23 | 0 | 17.3 |
| E | fail | 47 | 36 | 6 | 0 | 5 | 12.8 |
| E | pass | 103 | 79 | 7 | 1 | 16 | 7.8 |
| E | all | 150 | 115 | 13 | 1 | 21 | 9.3 |
| F | fail | 47 | 45 | 1 | 1 | 0 | 4.3 |
| F | pass | 103 | 99 | 3 | 0 | 1 | 2.9 |
| F | all | 150 | 144 | 4 | 1 | 1 | 3.3 |
| G | fail | 47 | 29 | 18 | 0 | 0 | 38.3 |
| G | pass | 103 | 80 | 21 | 2 | 0 | 22.3 |
| G | all | 150 | 109 | 39 | 2 | 0 | 27.3 |
| H | fail | 47 | 40 | 1 | 6 | 0 | 14.9 |
| H | pass | 103 | 98 | 3 | 2 | 0 | 4.9 |
| H | all | 150 | 138 | 4 | 8 | 0 | 8.0 |
| field_item | fail | 47 | 29 | 7 | 11 | 0 | 38.3 |
| field_item | pass | 103 | 84 | 10 | 9 | 0 | 18.4 |
| field_item | all | 150 | 113 | 17 | 20 | 0 | 24.7 |
worst axis distribution: {'none': 77, 'G': 24, 'C': 22, 'D': 8, 'A': 6, 'B': 4, 'F': 4, 'H': 3, 'E': 2}
A examples (10):
  - contradicted `aa2369e06397b4b7ea71`: The focal form is a marketing outcome clause inside a sentence about cutting/bending/finishing, not a service; the paragraph reifies it into one. | «The manufacturer's dealing is the direct provision of services to ensure parts meet exact specifications»
  - absent `aaf538370d1332ee6854`: The evidence is a bare two-word item with no content; the paragraph does not say that nothing identifies what the solutions are. | «Proprietary Solutions»
  - absent `30bc56a723b551ba52eb`: The snippets also settle monitoring as something AGS-TECH does on its own lines (sensors, machine vision); the paragraph reports only the software feature and the job-posting skill. | «Examples where we use machine vision are real-time inspection in sheet metal inspection lines, verification of part placement and fixturing, monitoring of surfa»
  - absent `4ed3a8a23c3a1d5015f8`: The focal form is a mid-sentence fragment, not an entity; the paragraph never says the phrase names nothing on its own. | «CNC is the preferred method when the repeatability of parts is crucial»
B examples (9):
  - absent `2bb1ef096e6d21e3a48e`: The form sits in an unseparated run-on of finishing names so 'Chrome | Copper | Etching' is a live reading; the paragraph does not flag the segmentation risk. | «Chromate (Clear or Yellow)(Also known as Iridite or Chem Film) Chrome Copper Etching:** Electroless Nickel»
  - absent `cec3b0674a6b638b1f8a`: The exact focal string occurs only inside a project-page title; the nav's service entry is the shorter 'Wire EDM', and the paragraph flags neither. | «Wire EDM Machining of Cross Pendants»
  - contradicted `9c9c5cc8ef6392266e60`: The run-on list separates into Brush Nickel / Cadmium / Chromate; the paragraph asserts the joined form is a real plating service. | «Brush Nickel Cadmium Chromate (Clear or Yellow)(Also known as Iridite or Chem Film) Chrome Copper Etching»
  - contradicted `d846eaeda4ce65979efc`: 'Hard Coat Barrel Plating' exists only as a run-on join of two list items; the paragraph treats it as one named service. | «Anodizing - Bright Dip - Hard Coat - Standard (Multiple Colors) Barrel Plating Black Chrome»
C examples (24):
  - contradicted `ad3fda5e41d1414ffdac`: A case study on the manufacturer's own site about work for an Ohio client is exactly its own dealing; the paragraph denies it. | «There is no evidence in the snippets of Alec Model's own dealing with Custom CNC Machining»
  - absent `dcb54d1e56eb28dd249b`: The manufacture supported is the customer's; the paragraph's echo of 'supports the design and manufacture' leaves it readable as Lucas Milhaupt's own. | «Our engineering-focused approach supports the design and manufacture of high-tech medical devices»
  - contradicted `83b1be6d16bbc3f210e7`: The snippet speaks of fabricators in general; 'including FZE Manufacturing Solutions' appears nowhere in it. | «many metal fabrication manufacturers, including FZE Manufacturing Solutions, offer powder coating»
  - contradicted `d4fc9c51167141609803`: The snippet says 'Our cold header' in the first person and names no principal; the paragraph reassigns the party to an unnamed represented company. | «The dealing shown is Mathews & Company representing a company that offers cold heading services»
D examples (26):
  - contradicted `ad3fda5e41d1414ffdac`: The snippets fix work done to a customer's order; the paragraph reduces it to a bare mention. | «beyond its mention in the title and heading of a case study document»
  - contradicted `83b1be6d16bbc3f210e7`: Supplies FZE an offering capacity the agentless sentence never gives. | «including FZE Manufacturing Solutions, offer powder coating»
  - contradicted `d4fc9c51167141609803`: Converts a first-person capability statement into a representation capacity the snippet does not state. | «representing a company that offers cold heading services»
  - absent `0f481866099c02b87e72`: The resale/distribution capacity that a dealer catalog listing settles is not carried. | «- Power Squaring Shears (Inch)»
E examples (14):
  - absent `db708531205b4754de2a`: The snippet is past-tense project narration; the paragraph converts it into a standing capability without saying it came from one job. | «The components featured complex geometries ... which required advanced 5-axis programming and optimized toolpath strategies»
  - absent `34bc0facade3cfca74a0`: A one-off fix on one job is presented as a standing part of 'Alec Model's workflow'. | «A virtual emergency meeting was convened with the client's mechanical team in Germany, resulting in a revised machining sequence»
  - absent `be868256d4b0a1c816a8`: Past-tense, single-project passive is rendered as a present standing practice ('Alec Model designs'). | «Custom fixtures and machining paths were designed to allow light, multiple-pass cutting»
  - absent `03be012ac7e0578cc9e6`: One job's process is presented as 'its surface treatment process', a standing routine. | «The process included: ultrasonic cleaning → acid passivation → secondary DI rinse»
F examples (5):
  - absent `7249e3430ba98dd828dd`: The phrase is the purpose/destination of a design review, not an operation offered; the paragraph never states which side it sits on. | «have the experience to help point out any problem areas for future part production»
  - absent `dcb54d1e56eb28dd249b`: Device manufacture is the destination/application of the brazing, not the manufacturer's own operation; the paragraph never says which side. | «supports the design and manufacture of high-tech medical devices»
  - contradicted `d23261c9f98894bd99d4`: The focal is the destination/use of the prototypes; the paragraph puts it on the operation-performed side. | «provides form-fit testing as part of its prototyping and documentation process»
  - absent `4de67115e547b01dda4d`: The turf-care cutting is what the customers' machines do; the paragraph folds it into 'its expertise in precision cutting'. | «With such precise machining capabilities, turf care businesses can ensure their grass is cut evenly and accurately.»
G examples (41):
  - absent `1c3bf29523863b72b5bb`: The snippet is a caption-style entry under a category heading; the paragraph states no frame at all. | «Machined Components & Milling & Turning: Rapid Prototyping of Metal Components»
  - absent `83b1be6d16bbc3f210e7`: The snippet is inside a generic explainer about working with fabricators; the paragraph gives no frame. | «## The advantages of working with a metal fabrication manufacturer»
  - absent `31c938c8e5e362ca5173`: It is a subsection heading in a list of casting methods on Tanfel's own site; that frame is what decides explainer vs offering and the paragraph omits it. | «- ### Shell Mold»
  - contradicted `253de544cfb95b9f1308`: The snippet is website copy under the heading 'Advance Product Development'; no document or documentation exists in the evidence. | «as stated in the manufacturer's advance engineering documentation»
H examples (12):
  - contradicted `83b1be6d16bbc3f210e7`: An industry-general statement is upgraded to a first-party claim. | «including FZE Manufacturing Solutions»
  - contradicted `0626e6727ba17f4645c8`: The snippet is a bare list of spring types; the 'leading supplier' claim is not in it and is attributed to it. | «stating that Tanfel is a leading supplier for custom springs and wire formed parts»
  - contradicted `4ed3a8a23c3a1d5015f8`: An educational generalization is converted into a first-party offering claim. | «This indicates that Alec Model offers CNC machining services»
  - contradicted `89a887f7af6227a27e3c`: 'must be applied' becomes a first-party assertion. | «Lucas Milhaupt, Inc. applies precision to fabrication»
field_item examples (37):
  - absent `1c3bf29523863b72b5bb`: AGS-TECH presents itself as an integrator/outsourcing partner and the caption fixes no performer or place; the paragraph says only 'offers'. | «Machined Components & Milling & Turning: Rapid Prototyping of Metal Components»
  - contradicted `ad3fda5e41d1414ffdac`: Who performs is settled (Alec, for a named client) and the paragraph erases it. | «There is no evidence in the snippets of Alec Model's own dealing»
  - absent `7249e3430ba98dd828dd`: The paragraph does not say this is not an operation the shop performs but the customer's downstream production. | «for future part production»
  - absent `dcb54d1e56eb28dd249b`: Does not say the device manufacture is a client's own step while brazing is the shop's. | «supports the design and manufacture of high-tech medical devices»

## products: 150 rows (malformed 0, duplicate 0, missing 0, extra 0)
| axis | stratum | n | carried | absent | contradicted | na | absent+contradicted % |
|---|---|---:|---:|---:|---:|---:|---:|
| A | fail | 120 | 97 | 12 | 6 | 5 | 15.0 |
| A | pass | 30 | 28 | 1 | 1 | 0 | 6.7 |
| A | all | 150 | 125 | 13 | 7 | 5 | 13.3 |
| B | fail | 120 | 13 | 3 | 2 | 102 | 4.2 |
| B | pass | 30 | 2 | 0 | 0 | 28 | 0.0 |
| B | all | 150 | 15 | 3 | 2 | 130 | 3.3 |
| C | fail | 120 | 72 | 4 | 44 | 0 | 40.0 |
| C | pass | 30 | 28 | 1 | 1 | 0 | 6.7 |
| C | all | 150 | 100 | 5 | 45 | 0 | 33.3 |
| D | fail | 120 | 44 | 29 | 47 | 0 | 63.3 |
| D | pass | 30 | 25 | 2 | 2 | 1 | 13.3 |
| D | all | 150 | 69 | 31 | 49 | 1 | 53.3 |
| E | fail | 120 | 108 | 0 | 0 | 12 | 0.0 |
| E | pass | 30 | 27 | 0 | 1 | 2 | 3.3 |
| E | all | 150 | 135 | 0 | 1 | 14 | 0.7 |
| F | fail | 120 | 103 | 2 | 10 | 5 | 10.0 |
| F | pass | 30 | 29 | 0 | 1 | 0 | 3.3 |
| F | all | 150 | 132 | 2 | 11 | 5 | 8.7 |
| G | fail | 120 | 30 | 72 | 18 | 0 | 75.0 |
| G | pass | 30 | 27 | 3 | 0 | 0 | 10.0 |
| G | all | 150 | 57 | 75 | 18 | 0 | 62.0 |
| H | fail | 120 | 114 | 3 | 2 | 1 | 4.2 |
| H | pass | 30 | 30 | 0 | 0 | 0 | 0.0 |
| H | all | 150 | 144 | 3 | 2 | 1 | 3.3 |
| field_item | fail | 120 | 33 | 26 | 61 | 0 | 72.5 |
| field_item | pass | 30 | 25 | 2 | 3 | 0 | 16.7 |
| field_item | all | 150 | 58 | 28 | 64 | 0 | 61.3 |
worst axis distribution: {'D': 42, 'C': 37, 'G': 29, 'none': 28, 'A': 7, 'F': 3, 'H': 2, 'B': 1, 'E': 1}
A examples (20):
  - absent `a03e1879ce3b8b870035`: The snippet presents it as an item on an equipment list; the paragraph presents it as part of product offerings. | «Their equipment list includes»
  - absent `cec960ba799e57a9134b`: The paragraph never says what 'They' are, so the reader cannot tell what kind of chain is at issue. | «They are used in bicycle locks, as locking chains, sometimes as pulling & hoisting chains»
  - contradicted `a425176d529d3dd4206b`: The run-together bullet attaches non-handed inventory control to 'Square hinge and lock edges', not to the cores. | «for enhanced thermal performance and non-handed inventory control for local distribution»
  - contradicted `178ad9740c7e056bd214`: The snippet's 'It' is the core/finish available in those series, not the H Series itself; series designations are otherwise carried verbatim. | «The H Series is also available in CE Series panel doors, L Series flush doors, and T Series temperature rise doors»
B examples (5):
  - absent `d185183e77d2ddb89e44`: The list fixes 'Transfer' as transfer molding; the paragraph leaves the bare word open to other senses. | «Transfer, Injection and Compression Molding»
  - absent `40333c506555e2fef41e`: The word appears as a degree field in a job ad, as 're-engineering' a workflow, and as a shop capability; the paragraph folds all of them into one engineering capability. | «By re-engineering that workflow, they reduced turnaround times by nearly 25%.»
  - contradicted `5d1ce1e04d3f6f346418`: Customers' subsea/surface equipment, Howco's own measuring equipment and its plant equipment are three different senses, collapsed into one. | «These snippets show that Howco supplies, manufactures, inspects, and assembles equipment and components»
  - absent `fdc542b4dfd7b4a5188e`: Software automation in a job ad, automation systems as shop equipment, and automation as an industry trend are folded into one. | «- · Experience developing business applications, automation, and analytics solutions.»
C examples (50):
  - contradicted `cf133e8d5793ec058d66`: The entry 'kpl05 Rubber Fabrication & Gaskets' names no agent; the paragraph supplies Mathews as the manufacturer/distributor. | «Mathews & Company is involved in manufacturing and distributing cellular foam material»
  - contradicted `d185183e77d2ddb89e44`: The snippet's subject is 'They', a third party described in the line card, not Mathews. | «Mathews & Company offers Transfer as one of the services»
  - contradicted `a03e1879ce3b8b870035`: 'Their' equipment list belongs to the third party described in the line-card entry. | «Mathews & Company offers access to 5-axis vertical milling»
  - contradicted `d0dd383d747d67dff55c`: The snippet is agentless explainer prose under 'Understanding Metal Injection Molding'. | «showing that Tanfel produces machined metal parts using MIM»
D examples (80):
  - contradicted `a03e1879ce3b8b870035`: 'Offers access to' is supplied; the snippet only lists machines somebody owns. | «offers access to 5-axis vertical milling as part of their precision components and assemblies offerings»
  - contradicted `d0dd383d747d67dff55c`: A production capacity is invented, and MIM parts are by definition not machined parts. | «produces machined metal parts using MIM»
  - absent `66be68eac4984f7fb762`: A bare list fixes no verb; the paragraph neither says the capacity is unstated nor names one, offering only 'the provision of'. | «Mechanical Tube, Coupling Stock, Hollow Bar, OCTG»
  - contradicted `cec7afe810deea2a0dd3`: Fabrication of automobile parts is attached to Blackstone though the sentence attaches it to 'you'. | «The manufacturer mentions fabricating automobile parts as an example»
E examples (1):
  - contradicted `29c4fa8aec77b00402f3`: The snippet is one project's past account ('Shipping box included ...'), turned into standing practice. | «includes a FedEx ID photo in their shipping box as part of their export packaging process»
F examples (13):
  - contradicted `d0dd383d747d67dff55c`: In the snippet the machined metal part is the comparison standard for MIM, not the thing made. | «showing that Tanfel produces machined metal parts»
  - contradicted `cec960ba799e57a9134b`: The snippet puts the focal phrase on the use side ('sometimes as pulling & hoisting chains'); the paragraph inverts it into the thing used in bicycle locks. | «pulling & hoisting chains are used in applications such as bicycle locks, locking chains, and similar uses»
  - contradicted `5d1ce1e04d3f6f346418`: 'Used extensively on completion equipment' and 'subsea equipment manufacturers' put equipment on the destination/customer side. | «supplies, manufactures, inspects, and assembles equipment»
  - contradicted `9f4ebb625d9a3f0edfa7`: The snippet puts the focal phrase on the use side ('used ... as locking chains'); the paragraph makes it the thing used. | «locking chains are used in bicycle locks, as locking chains»
G examples (93):
  - absent `cf133e8d5793ec058d66`: The line-card catalog frame (coded entry kpl05) is not carried, so the reader cannot see this may be a represented line. | «# Kansas Product Line»
  - absent `d185183e77d2ddb89e44`: Third-person line-card entry frame not carried. | «# Kansas Product Line»
  - absent `a03e1879ce3b8b870035`: Equipment-list-inside-a-line-card frame not carried. | «Their equipment list includes»
  - absent `d0dd383d747d67dff55c`: The explainer frame is not carried. | «## Understanding Metal Injection Molding»
H examples (5):
  - contradicted `912533bf0d1e10570368`: Generic explainer prose about forging in general is declared to be Pradeep's own claims. | «The snippets attribute all these descriptions and manufacturing activities to Pradeep Metals Limited»
  - absent `fe169f1dfe8a235650d8`: The first-party 'our expertise' claim strength is not carried. | «Project Background This case study highlights our expertise in copper heat sink machining for industrial power electronics.»
  - contradicted `289541bf1d9eab445ab8`: No snippet makes the tested-to-exceed-OEM or deep-inventory claim; it is added marketing language. | «as part of its deep inventory of quality, FAA-approved replacement parts, tested to meet or exceed the performance of original equipment»
  - absent `b634839f0c2d6fe3b8b3`: The holder is each factory holding one of those systems; the paragraph reads as though the company manufactures to all of them. | «All of the factories are either one of ISO9001:2000, QS9000, TS16949, ISO13485 or AS9100 quality management systems certified»
field_item examples (92):
  - contradicted `cf133e8d5793ec058d66`: Capacity named but attached to a party the entry does not name. | «Mathews & Company is involved in manufacturing and distributing cellular foam material»
  - contradicted `d185183e77d2ddb89e44`: Says plainly it is a service, but assigns the capacity to the wrong party. | «Mathews & Company offers Transfer as one of the services»
  - contradicted `a03e1879ce3b8b870035`: A machining capability is folded into product offerings instead of being called a process on someone else's equipment list. | «as part of their precision components and assemblies offerings»
  - contradicted `d0dd383d747d67dff55c`: A comparison referent in an explainer is turned into a product Tanfel makes. | «showing that Tanfel produces machined metal parts using MIM»

## all fields, all strata
| axis | carried | absent | contradicted | na | absent+contradicted % |
|---|---:|---:|---:|---:|---:|
| A | 830 | 42 | 23 | 5 | 7.2 |
| B | 149 | 17 | 18 | 716 | 3.9 |
| C | 781 | 22 | 95 | 2 | 13.0 |
| D | 646 | 59 | 194 | 1 | 28.1 |
| E | 518 | 15 | 8 | 359 | 2.6 |
| F | 830 | 17 | 47 | 6 | 7.1 |
| G | 628 | 191 | 72 | 9 | 29.2 |
| H | 827 | 18 | 51 | 4 | 7.7 |
| field_item | 628 | 113 | 159 | 0 | 30.2 |
