# Rule Catalog + Structured Feedback Design (SUDOKN extraction)

Goal: make every concept-extraction stage emit the RULES it applied (with an explanation)
so humans (manufacturing-background annotators) can give structured feedback on the FULL trail.
Feedback feeds two downstream goals: (1) fine-tuning, (2) automated prompt optimization.
Annotators judge at the CONCLUSION level; rule-level credit assignment is derived by machine.

## Pipeline recap (verified against code)
Concept fields go through: phrase_search -> recursive_search -> relationship -> screening ->
initial_grounding -> recursive_grounding -> reconcile. Trail dumped to
logs/phrase_trails/{ts}/{etld1}__{field}.json via
core/utils/phrase_trail_dump_util.py::write_phrase_trails_dump.
- Prompts: apps/data_etl_app/src/data_etl_app/new_prompts/multi_stage/{1_phrase_search,
  2_phrase_recursive_search,3_phrase_relationship,4_phrase_relationship_screening,
  5_initial_grounding,6_recursive_grounding}/<concept>_*.txt
- Recursion descends LEVEL BY LEVEL from level 1; seeds at concept.level==1, while loop to
  max_concept_level+1 (core/.../base/llm_phrase_iterative_grounding_node.py). Only the DEEPEST
  tag is retained (get_deepest_concepts_and_oov discards immediate parent). Industries ontology
  is only 2 levels, so at most one descent.
- Stopping condition is STRUCTURAL: None-of-the-above (RGR-M4) / leaf / max depth. NOT quality-based.

## CONCEPTS in scope (tree grounding = initial+recursive)
Only these 4 use screening+initial+recursive grounding. Equipment & product use keyword/freehand
grounding (different pipeline) — NOT covered by catalogs yet.
| field_type | entity_noun | rel base / third_person / gerund | recursive parent token |
|---|---|---|---|
| industries | industry, market, or sector | serve,supply,or operate in / serves,supplies,or operates in / serving,supplying,or operating in | {{parent_industry}} |
| certificates | certification, accreditation, registration, standard, or compliance | hold or conform to / holds or conforms to / holding or conforming to | {{parent_certificate}} |
| materials | material | process / processes / processing | {{parent_material}} |
| processes | manufacturing process, operation, or capability | perform,offer,or specialize in / performs,offers,or specializes in / performing,offering,or specializing in | {{parent_process_cap}} |
Note: process uses {{types_of_process_cap}} (not types_of_parent_process_cap) in template.

## CATALOG FILES (DONE — all 12 created & JSON-validated)
Dir: apps/data_etl_app/src/data_etl_app/knowledge/rule_catalog/
{screening,initial_grounding,recursive_grounding}.{industries,certificates,materials,processes}.json
Per-stage rule counts (uniform across concepts): screening=5 leaves, initial=7, recursive=10.

## CATALOG SCHEMA (the stable, generic envelope)
Top-level fields: catalog_version, stage, field_type, entity_noun, entity_relationships
{base,third_person,gerund}, outcome_vocab (keyed by kind), sections[].
Section: {section_id, heading, combinator, rules[]}.
Rule node (recursive via children[]):
  id (STABLE, position-independent), kind, reportable(bool), report_when, text (uses {{placeholders}}),
  combinator (optional), children[] (optional).
Placeholders in rule text: {{entity_noun}}, {{entity_relationships.base|third_person|gerund}},
and recursive parent token ({{parent_industry}} etc.). Template also fills {{types_of_parent_*}}.

### kind -> outcome_vocab -> report_when
A vocabulary may only list outcomes the parser accepts, because the rendered prompt presents the
whole vocabulary to the model as allowed. A rule reported on exactly one outcome therefore has a
one-value vocabulary; RuleCatalog rejects any catalog that offers more (2026-08-10).
- condition: [satisfied, failed, not_triggered]; report_when=always.
  Only `satisfied` counts as holding — an `all` section is a conjunction, and `not_triggered`
  ("nothing to evaluate against") blocks a pass exactly as `failed` does.
- guard: [violated]; report_when=on_violation (screening only)
- preference: [chosen]; report_when=when_chosen (grounding matching)
- quality: [appropriate, over_descended, under_descended]; report_when=always (recursive QC only)
- note: non-reportable (reportable=false, report_when=never); renders as guidance sub-bullet
### combinator (drives render + eval): all | any | ordered | note
CONDITION CHAIN (2026-08-10): within an `all` section, document order IS a dependency chain —
each condition's subject is what the previous one established (SCR-2 speaks of "the identified
<entity>", which exists only if SCR-1 held). `_check_condition_chain` rejects a condition reported
`satisfied` behind one that did not hold, pointing at the FIRST break so an intervening
`not_triggered` cannot silently repair it. The signature of a valid report is
`satisfied* failed? not_triggered*`. Nothing declares the chain besides order; on the grounding
sections, whose qualification conditions are closer to a flat conjunction, the check costs nothing
because a tag whose qualification fails is dropped rather than reported. Revisit if grounding ever
retains rejected tags.

NO `exception` kind or combinator (removed 2026-08-10). A carve-out from a condition is just a
named region of that condition's FAILURE — logically the same thing a guard is — and nesting it
under the condition with inverted polarity put a member into an all-must-hold section whose
`satisfied` meant reject. The test for whether a failure mode deserves its own rule: does it
SETTLE something the condition leaves open (keep it), or does it merely NAME a way the condition
already fails (drop it)? SCR-G2 passes that test — SCR-2 says "shown to serve", not "currently
serves", so the tense question is genuinely open. The old SCR-G1 ("the text states it does NOT
serve") did not: it was a strict subset of SCR-2 failing and could never flip a verdict, so it
was dropped from all 7 screening catalogs.

## RULES PER STAGE
Screening (SCR): section pass_conditions(all): THREE flat conditions, each positively phrased,
  split along the error modes actually seen in the data (restructured 2026-08-10):
    SCR-1 the entity is identifiable at all;
    SCR-2 the relationship is REAL — some party is shown to <base> it, not merely name or list it
      (catches nav menus, keyword stuffing, "industries we're familiar with");
    SCR-3 the relationship is the MANUFACTURER'S — not a customer, supplier, or distributor it
      merely relates to (catches the dominant error mode; was the nested exception).
  section guards(any): SCR-G2 (aspirational/planned/discontinued). Guards reportable ONLY
  on_violation, which is why SCR-3 is a condition and not a guard: as a guard it would be silent
  on every passing phrase, and exhaustive deliberation on wrong-party attribution is the whole
  point of reporting it.
  Equipment keeps SCR-G3 (a machine it sells, not one on its floor) and SCR-G4 (aspirational /
  on-order / discontinued); its ownership rule is a NOTE under SCR-3 — lease, partnership, and
  parent/sister access all count as the manufacturer itself, which is why SCR-3's third-party
  list names customers/suppliers/distributors and not parents.
  SCOPE (2026-08-10): conditions AND guards are evaluated per CANDIDATE entity, not per phrase.
  The phrase passes when at least one candidate clears everything; a candidate that trips a guard
  rules only itself out. The skeleton's "How to decide" block states the loop, because the old
  guard heading ("Reject the phrase if ANY of these guards is violated") contradicted the
  existential the conditions were written against — "ISO 9001 certified; AS9100 in progress"
  rejected a real certification on the strength of a different candidate.
  EQUIPMENT SCOPE (2026-08-12): the field is PRODUCTION MACHINES only. entity_noun became
  "production machine category" in both equipment catalogs, so the prompts never hand the model
  the bare word "equipment" as the thing it is naming — the observed failure was freehand
  returning "production equipment" as a tag for a phrase that fixed no kind. SCR-1 is now a
  determinacy test alone (a particular kind of machine, not an umbrella) and SCR-G2 carries ALL
  of the scope, so a rejection on scope has exactly one locus and the record cannot be ambiguous
  between "no kind identifiable" and "identified, but out of scope". SCR-G2 states the positive
  definition (acts on the work itself: process, shape, assemble, convert raw materials into
  finished goods) and rules out support equipment, with notes for the two classes that read as
  machines but are not: measurement/inspection/testing (establishes what the work already is and
  leaves it as it was) and tooling (mounted in or driven by a machine, no operation of its own).
  Inspection equipment leaving the field is a DELIBERATE narrowing — the search prompts listed
  CMMs and spectrometers as in-scope until this change.
  Equipment: the ownership condition (briefly a second SCR-4) is a NOTE under SCR-2, not an
  AND-term — websites state operation, not title, so as a condition it failed nearly every true
  positive. Its negative case is already SCR-G1. Equipment also gained SCR-G4 (aspirational /
  on-order / discontinued), which every other screening catalog had and it did not.
Initial grounding (IGR): attribution(all): IGR-Q1 (identified entity is one mfg itself does,
  gerund), IGR-Q2 (match by meaning not lexical) with non-reportable note child IGR-Q2a
  (definition-conflict — folded-in, was a guard, demoted). matching(ordered): IGR-M1 exact,
  IGR-M2 generalize, IGR-M3 self-name with non-reportable note child IGR-M3a (anti-restatement).
Freehand grounding (FGR) — uniform across all 3 catalogs as of 2026-08-10, CATEGORY-ONLY as of
  2026-08-10: entity_noun is "equipment category" / "product category" and the wire field is
  `category`, so the output IS the family and never the individual thing.
  attribution(all): FGR-Q1 (mfg itself does this to at least one instance of the category).
  category_qualification(all): FGR-Q2 (DETERMINACY BAND — neither an individual unit, now
  explicitly including brand/model/part number, nor a term so general it fixes nothing, e.g.
  "equipment"/"machinery" or "products"/"parts"; + note child FGR-Q2a giving a worked example),
  FGR-Q3 (distinct from every other category on the phrase; two names for one thing are one).
  naming(ordered): FGR-M1 (a category can be determined), FGR-M2 ("Cannot categorize").
  NO formatting section. FGR-F1 (strip brand/model) was DELETED: once the output is defined as a
  category, "no brand or model" is the lower bound of FGR-Q2's band, not a separate formatting
  step. Equipment's FGR-F1 had said the OPPOSITE of products' — it prescribed a
  "<Brand/Model> <category>" tag — so the two stages disagreed on the same id. Deleting it also
  drops one reported rule (with its explanation) per category per phrase.
  EQUIPMENT GRANULARITY (2026-08-12): equipment gained FGR-Q4, a two-sided band on WHERE in the
  hierarchy the name lands. Naming descends in steps — the operation the machine performs on the
  work, then the configuration telling machines performing that same operation apart — and the
  rule is "every step the words supply, and stop at the first one they do not". That single rule
  carries both halves of the ask: as specific as possible (take every step) and never more
  advanced than stated (typicality is not evidence). They are one rule because as two they can be
  traded off against each other. FGR-Q2 gained note children for the two ways a name fixes
  nothing: an umbrella term naming no operation, and a trade/service/line-of-business with a word
  for machines attached to it ("tool and die business" -> "tool and die equipment"). Equipment
  also gained FGR-QC1, a `quality` rule self-reporting member_level vs family_level, so
  family-capped output is measurable as a property of the TEXT rather than invisible. Its vocab
  must include not_triggered: the output-example builder emits that outcome for the
  Cannot-categorize entry, and a vocab without it would ship an example the parser rejects.
  NO CONCRETE ANCHORS (2026-08-12): equipment's rule text names no machine, brand, or worked
  example — the same policy the output-example builder already followed. The abstractions doing
  the work are "operation performed on the work" (the family test and the umbrella floor) and
  "the configuration that tells machines performing the same operation apart" (the descent step).
  The trade, accepted: the model instantiates those itself instead of pattern-matching a list, so
  expect more variance in where FGR-Q4 stops descending. FGR-QC1 is what makes that visible.
  Equipment's matching heading previously said "assign exactly one tag per phrase", contradicting
  both the job statement and the array schema and making FGR-Q3 vacuous; many-per-phrase is now
  uniform. Previously FGR-Q2 meant granularity in equipment and dedup in products — the same id for
  unrelated things, in one stage, with only field_type to tell them apart. Dedup matters
  everywhere: all three parse services collapse to `{tag: applied_rules}`, so an exact duplicate is
  swallowed last-write-wins rather than caught, and a near-duplicate survives as two tags. Products
  gained the ladder and escape hatch equipment already had; before that the "exactly one chosen
  preference" check did not run for them at all.
Recursive grounding (RGR): attribution(all): RGR-Q1 (type mfg itself does, not merely relates),
  RGR-Q2 (each type qualifies on own merits; parent evidence != child), RGR-Q3 (match by meaning)
  + note child RGR-Q3a (definition-conflict). matching(ordered): RGR-M1/M2/M3(+note RGR-M3a)/M4
  ("None of the above" terminal). quality(all): RGR-QC1 granularity check.

## SECTIONS SPLIT BY WHAT THEY JUDGE (2026-08-10)
Every grounding stage's qualification section split in two, on the line between a judgment about
the WORLD and a judgment about the OUTPUT:
- `attribution` — is the entity identified from the phrase the MANUFACTURER'S? IGR-Q1 / RGR-Q1+Q2 /
  FGR-Q1. (RGR-Q2 belongs here as attribution scoped to the child: do not attribute the parent's
  evidence to it.)
- `match_qualification` (`category_qualification` in freehand) — is what is being recorded for it
  well-formed? IGR-Q2 / RGR-Q3 / FGR-Q2+Q3. The two names differ because the OBJECT differs: a
  category the stage invented vs. a mapping onto a supplied option. The shared suffix carries the
  shared role.
- selection — `matching` where the stage picks from a supplied list (IGR/RGR), `naming` in freehand
  where it does not. Freehand's skeleton says "there is no list of options to choose from", so
  calling its section `matching` was the last place it spoke option-list vocabulary it has no list
  for. Rule IDS stay FGR-M1/M2: `RuleNode.id` is stable by contract and stored applied_rule records
  and ground-truth annotations key on it, whereas nothing persists a section_id — which is exactly
  why section ids are free to rename and rule ids are not.
Interleaved under one heading, a rule id did not say which kind of failure it stood for, and the
chain check treated them as one dependency run even though the match criterion does not depend on
the entity qualifying. Split, the chain resets between sections, which is correct.
Screening keeps ONE `pass_conditions` section on purpose: SCR-1/2/3 are a genuine chain (each one's
subject is what the previous established) and `_check_condition_chain` reads document order WITHIN
a section as that chain. Splitting SCR-3 out as `attribution` would silently stop the chain from
being checked across the break.

SCREENING AS GATE, GROUNDING AS ENUMERATOR (settled 2026-08-10). Screening answers "is there
anything here?" and carries ONE identified_entity; grounding answers "what exactly, and how does it
map?" and discovers the rest. So IGR-Q1/FGR-Q1 are NOT redundant with SCR-2/SCR-3 — they are the
same rule applied to entities screening never saw, exactly as RGR-Q1 is for types invented during
descent. identified_entity being write-only is fine under this reading: it is the recorded proof
that the gate opened, not a pipeline input. The consequence, accepted: grounding CAN reject an
entity, and those rejections are not recorded anywhere.
If screening is ever changed to carry ALL its qualifying entities forward, the `qualification`
section of initial and freehand becomes deletable as a unit and those stages become pure matching.
That is why the split puts it alone in its own section.

Screening may report an EMPTY applied_rules when identified_entity is null: nothing was identified,
so the conditions have no candidate to be about, and "SCR-1 failed, SCR-2 not_triggered, SCR-3
not_triggered" says exactly what `[]` says. Any other combination is validated in full.

## TAG vs OPTION (2026-08-10)
Two grounding wire schemas, deliberately not shared, because the stages do different jobs:
- Freehand invents its own label -> `{"tags": [{"tag", "applied_rules"}]}`, and the rules say "tag".
- Initial + recursive choose from a supplied list -> `{"options": [{"option", "applied_rules"}]}`,
  and the rules NEVER say "tag". They take the entity as their subject ("For each distinct
  <entity> you identify, ALL of these must hold"), because "tag" was a third concept the prompts
  never defined: nothing stated how the thing identified from the phrase became a tag.
- IGR-M3/RGR-M3 therefore PROPOSE an option of their own rather than naming a "self-named tag".
  That keeps the field name honest on every branch — chosen from the list (M1/M2), proposed (M3),
  or the reserved "None of the above" (M4), which reads as an option by construction. A proposed
  option is still absent from match_label_to_concept_map, so it lands in out-of-vocab exactly as
  a self-named tag used to.
The split stops at the end of parsing: both shapes collapse into PhraseToTagAndRulesMap and every
downstream name stays tag-based. `is_sentinel_grounding_label` is named for neither, since it is
called with a tag in one stage and an option in the other.

## SENTINEL LABELS
NONE_OF_THE_ABOVE_TAG ("None of the above") for initial/recursive; CANNOT_CATEGORIZE_TAG
("Cannot categorize") for freehand — one reads as an option, the other as the outcome of a
categorization job. `RuleCatalog.sentinel_tag` names the one a catalog uses and is validated to be
a reserved label spelled VERBATIM in exactly one preference rule (the branch that instructs the
model). Other kinds may mention it freely — a formatting rule exempting the sentinel from its own
layout requirement is what proved "exactly one rule" too strict. (Freehand's FGR-F1 was that rule
and is now gone: with the output defined as a category, stripping brand/model is the lower bound of
FGR-Q2's granularity band rather than a formatting step.) The check exists
because the literal lives in both the catalog text and a code constant: a prompt saying "None of
these" while the parser matches "None of the above" fails SILENTLY, with the sentinel flowing into
results as a discovered label.
A sentinel arriving ALONGSIDE a real label is not an error — the sentinel is dropped and the real
one proceeds. That currently falls out of the in-vocab descend-worthy filter rather than from
anything that says so; test_sentinel_grounding_tags pins it.

## RESPONSE ENVELOPE
Uniform primitive = applied_rule record: {rule_id, outcome, explanation}. One pydantic type,
`AppliedRule`, serves both the wire and the stored side — validation checks a report against its
catalog but no longer transforms it.
- explanation = per-rule prose, and the WHOLE of the justification. The prompt asks it to cite the
  phrase and its relationship summary, quoting the words relied on. Nothing checks that; only that
  it is non-empty. `not_triggered` rules explain why there was nothing to evaluate and quote
  nothing.
- Report policy: report all rules whose report_when=always; guards only when violated; preference
  only the chosen branch (outcome=chosen). not_triggered rules ARE kept (user wants exhaustive).

EVIDENCE REMOVED (2026-08-11). Each rule previously carried `evidence`: an array of
{source, quote} spans, quote being a LIST of slices, each located in the declared source at parse
time by a normalising matcher (`applied_rule_validation` + `core/utils/text_normalize.py`), with
offsets and match kind persisted through a parallel `Resolved*` type family. All of it is gone:
the wire/stored split, the matcher, the `SliceMatchStats` telemetry, the `Resolved*` types, and
`extract_phrase_summaries_block` (the parse-time readback that supplied the summary to check
against — `render_phrase_summaries_block` stays, since the block is still how summaries are shown
to the model).
Why it is worth recording rather than just deleting: the design kept losing to the same shape of
failure. Every prohibition added against fabrication ("never reworded", "never assembled from
separate places", "never added to") was satisfied by the next one, because a model asked for a
span the source does not contain has no compliant move — and one bad quote failed the whole group
request, so a fifteen-phrase batch died over a sentence-initial "The" retyped as "the". Slices and
the normalising ladder were both attempts to widen the set of legal moves. What replaced the field
is the positive half of the same instruction, in prose, where a paraphrase is a weakness an
annotator can see rather than a parse failure that costs the request.
What was given up, deliberately: nothing now distinguishes a quoting explanation from an inventing
one, and the exact-reproduction rate the slice stats measured is no longer observable. If that
needs to come back, it comes back as a structured field with the reader restored — not as a check
on this prose.
Screening entry: {phrase, identified_industry(nullable singular; generic: identified_entity),
  applied_rules[]}. NO top-level reason (dropped). NO `passed`: the model does not vote.
  `passed` is DERIVED at parse time by `passed_implied_by` — every condition satisfied, no guard
  reported — and stored on ScreeningVerdict for downstream readers. It was briefly a reported field
  cross-checked against the rules; deriving it removes the second channel entirely, so a report can
  no longer argue one way and vote the other, and a disagreement can no longer fail a whole group
  request. An empty applied_rules[] rejects rather than vacuously accepting: the only validated
  report with no rules is the null-entity shortcut, which is a phrase that offered no candidate.
  A pass still requires a non-null identified_entity; a REJECT may still name the candidate it
  weighed, which is what an annotator needs to correct the call.
Freehand grounding entry: {phrase, categories[]}, each = {category, applied_rules[]}. The word is
  CATEGORY, not tag: the stage is asked for the family a named thing belongs to, never the thing
  itself, so brand/model/serial never appear in the value. Initial/recursive keep {options[]}.
  NO match_rule field (the chosen preference rule appears in applied_rules with outcome=chosen).
  NO top-level reason.

## KEY DECISIONS (locked)
- Drop top-level reason everywhere; per-rule explanation carries justification.
- identified_industry SINGULAR + nullable (screening = one-tag).
- NO structured evidence field; the explanation carries the citation in prose (see RESPONSE
  ENVELOPE for why the machine-checked version was removed on 2026-08-11). Superseded the earlier
  "evidence: array of {source,quote}" decision.
- Keep not_triggered rules (force exhaustive deliberation).
- Guards are failure-mode sharpeners; reportable ONLY in stages that retain rejected units
  (screening yes; grounding no — a veto drops the tag so there's no slot -> definition-conflict
  guard demoted to non-reportable note under Q2/Q3).
- Quality checks on SURVIVING tags ARE reportable (RGR-QC1).
- Granularity vocab kept SYMMETRIC (appropriate/over_descended/under_descended). Reality: model
  self-reports ~always "appropriate", sometimes under; over_descended is essentially a
  HUMAN-applied correction label. Over-descend = false-positive RGR-Q2 (bounded: only deepest tag
  retained, so a chain collapses to one over-specific tag). Fix over-descend upstream via stricter
  Q1/Q2, NOT via stop condition.
- Stable rule IDs decoupled from display outline numbers; every stored record must carry
  catalog_version for joinability across catalog edits.

## RENDER CONTRACT (assembler must produce)
Group rules by section; render each section under its heading; combinator -> connective
(all="ALL of…", any="ANY…", ordered="in order:", exception="— UNLESS:" nested,
note=sub-bullet guidance). Lead each node with "[ID] ". NO outline numbers (dropped
2026-08-11): they gave every node a second, positional name that nothing refers to, and an
ordered section already carries its ordinal in the ids (M1 before M2).
"Rules you must report" block: report every condition+quality; report guard only if violated;
report the single chosen preference. Output JSON example must be GENERIC (show the record shape
once, reference "one of the rule IDs above") — never enumerate specific IDs.

## BINARY CLASSIFICATION (2026-08-11): whole-text screening
is_manufacturer / is_product_manufacturer / is_contract_manufacturer converted from static
single-stage prompts to catalogs. Framing: each IS screening applied to the site instead of a
phrase — one existence proof ("at least one <entity> the business itself <rel>") through a chained
pass_conditions(all) + guards(any), same kinds, same chain check, verdict derived by
passed_implied_by. Stage = `binary_classification`; field_type = the classification name itself
(keeps (stage, field_type) 1:1 with a prompt); ids MFG-*/PRD-*/CON-* so a stored applied_rule names
its classifier without a join. Ordinal semantics shared across all three: identify → real →
(discriminate) → attribute.
- MFG: entity "production activity" (carry out). MFG-1 identify (transformation of tangible goods;
  moves/stores/sells/inspects/designs/advises excluded; note: single step suffices), MFG-2 real,
  MFG-3 this business's own operation (note: lease/JV/division count, bought-in work does not).
  Guards: MFG-G1 tense, MFG-G2 repair/refurb/maintenance of items already in service (USER CALL
  2026-08-11: not manufacturers, for now).
- PRD: entity "product of the business's own design" (produce and offer). PRD-1 identify (tangible
  good, not service/capability), PRD-2 real, PRD-3 design originates with and belongs to the
  business (note: sold under another party's name still counts — ODM decision), PRD-4 produces it
  itself (note: division/site counts; bought-finished-and-rebranded does not). Guard PRD-G1 tense.
  NO own-brand/general-buyers condition: USER CALL 2026-08-11 — an ODM (owns design IP,
  manufactures for a client's brand) is BOTH a product and a contract manufacturer.
- CON: entity "instance of production for another party" (perform). CON-1 identify (note: standing
  offer to produce to others' requirements counts, no named customer needed), CON-2 real, CON-3
  commissioned — to the other party's order: their spec fixes the good OR output supplied for them
  to offer as their own (note: the business's own design still qualifies — the other half of the
  ODM decision), CON-4 performs it itself, not brokered (note: having your own goods made elsewhere
  is buying, not performing). Guard CON-G1 tense/prospective.
Envelope: wire = {identified_entity nullable, confidence int, applied_rules[]} (
BinaryClassificationReport). `answer` DERIVED, `reason` SYNTHESIZED at parse time as the full
[id outcome] explanation trail — humans read it (GT survey, keyword API message), so no editorial
picking of a "decisive" rule. `confidence` KEPT deliberately: directionless, so not a second
verdict channel; recorded, never gated on; calibration measurable against
BinaryGroundTruth.final_decision, delete later only if measured flat. NO empty-rules shortcut
(unlike screening): one unit per request, so a no-candidate text reports first condition failed +
rest not_triggered.
Assembler: `evidence_source` catalog header ("the given text" here, default "the phrase and its
relationship summary") threads into report block + example explanation slots. STAGE_DIR_BY_STAGE
values became full prefixes so `single_stage/` fits; S3 keys unchanged (the keys the hand-written
prompts occupied). Binary example is ONE object; condition outcomes are alternatives slots (a
worked satisfied-chain would anchor accept bias), chain shape carried by skeleton prose.
Old hand-written prompts archived at knowledge/archive/prompts/pre_catalog_single_stage/, their
static pins deleted. find_business_desc + extract_any_address remain static (no yes/no to derive).

## OPEN / NEXT STEPS (assembler + pydantic — NOT started)
1. Assembler: load catalog by (stage, field_type); resolve {{entity_noun}}/{{entity_relationships.*}}
   from catalog header, then {{parent_*}}/{{types_of_*}} runtime vars; render sections+report block;
   emit generic output example. Lives likely near existing prompt-building/extraction_pipeline_factory.
2. Pydantic response schemas (core/models/extraction_schemas/): add AppliedRule {rule_id:str,
   outcome:str, explanation:str}. Screening: add identified_entity(nullable), applied_rules; drop
   reason. Grounding (PhraseGroundingResponse in grounding.py): tag += applied_rules; drop
   reason/match_rule. Build via build_gpt_response_format.
3. VALIDATION at parse time: enforce report_when policy (all conditions present; chosen preference
   present; guards only if violated), the condition chain, and a non-empty explanation. Quote
   checking was built here and removed on 2026-08-11 — see RESPONSE ENVELOPE.
4. Thread new fields through phrase_trail_dump_util + parsing (llm_recursive_grounding_service,
   llm_initial_grounding_service, screening service).
5. Equipment & product (keyword/freehand grounding) NOT yet catalogued — separate pattern.
6. Prompt .txt files currently REVERTED by user to the earlier hand-written applied_rules version;
   they must be regenerated FROM catalogs by the assembler (don't hand-edit).
