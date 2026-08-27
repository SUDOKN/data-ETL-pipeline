# Judgment taxonomy for search-stage output

Every judged metric classifies each returned surface form with a one-letter code
from the FIELD's table, plus optional orthogonal flags. Codes are per-form; a
form appearing in several windows is judged per window (windows are the search
unit). Judgments are made by READING the window text — never by regex over
prose (locked lesson: rule/explanation regexes failed in both directions).

## Shared rollup (cross-field comparability)

Every field table maps into this rollup, which is what trend lines use:

| rollup | meaning |
|---|---|
| in_field | V + B — a defensible candidate for this field |
| adjacent_field | names a real thing of ANOTHER field (code says which) |
| generic | generic noun with no designation content ("parts", "equipment") |
| junk | UI fragments, upload formats, boilerplate, not-an-entity |

`precision = in_field / judged`, always reported beside the judge-agreement
floor (see RUNBOOK step 6). Wrong-actor is a FLAG, not a code — a client's
product is still `V` for search (recall-first); the flag feeds the wrong-actor
share metric.

## Field tables

**products** (identical to the 2026-08-22 prototype, for comparability; D is a
new sub-code carved out of S — report S+D merged when comparing to the 37%
baseline):
V valid product designation · B borderline (component, option, family,
third-party part offered) · P process/service/finish · M material · G generic
noun · C client's product/application · S spec/attribute/equipment/facility/
feature · D document/literature title · U UI or fragment junk

**equipments**: V machine/tool designation (make/model or specific category) ·
B borderline (generic-but-machine category: "CNC lathes") · D product/good made
or sold (THE known confusion — steelcraft 68/75) · P process name · T
software/tooling/fixture ambiguous · S spec/facility · G generic ("equipment",
"machinery") · U junk

**process_caps**: V performed operation on the work (inspection/testing/
measurement ARE operations — locked carve-out) · B borderline operation · L
certification-lab or test-house procedure (wrong-actor; flag lab) · I
installation/field-site work · Z business/legal/HR activity · M material · E
equipment · G generic ("manufacturing", "production") · U junk

**material_caps**: V material/alloy/grade designation · B borderline (material
family) · P process named after a substance (still qualifies per locked
keep-rule — code P only when NO substance is implied) · N part/component name ·
C consumable (paint, packaging foam) · G generic ("metal", "materials") · U
junk (incl. polysemes with zero in-window material sense: `Lead` in lead time)

**industries**: V industry/sector served or targeted · B borderline (customer
segment, application domain) · O the subject's OWN activity presented as a
sector ("CNC machining" as industry) · W word-association ("Defense" from
weather-defense prose) · G generic ("industry", "manufacturing") · U junk

**conformity_attestations**: V standard/certification/regulatory designation
(ISO 9001, ASTM E152, NFPA 80, NADCAP...) · B borderline (named program,
audit scheme, code body) · D document title containing no standard id · Z
non-conformity certificate sense (HR "certificate programs") · G generic
("certified", "compliance") · U junk

## Orthogonal flags (any code may carry them)

`actor:` own | client | supplier | lab | parent_sibling | reseller_inventory |
unknown — whose thing the text says it is.
`evidence_kind:` prose | heading | nav | doc_title | case_study | job_ad |
legal_boilerplate | spec_table — where the form's occurrence(s) sit.
`polyseme:` form has multiple senses in-window and search gave no context.

## Audit trail

Every judged run writes one audit file per (subject, field):
`history/runs/<run_id>/judgments_<subject>_<field>.txt`, one line per form:
`<code>[flags]  '<form>'  (window <bounds>)` — the exact pattern of
`search_self_eval_judgments.txt`, so any number can be re-derived and any
judgment challenged.

## Rulings on codes that two subjects read differently

A code that one subject treats as a recall target and another counts against
precision makes those subjects' numbers incomparable. Rulings live here.

### Field-site / installation work (code `I`) — ruled 2026-08-26

Raised by verification: acimachine seeds ACI's field services (spindle repair,
retrofit, precision leveling, ball bar testing) as recall ENTRIES, while
austinelectric's seed treats field-site work as out-of-field and seeds none.

**The ruling, in two parts, because recall and precision answer different
questions:**

1. **Recall** — an operation the SUBJECT ITSELF performs, named in the text,
   is a legitimate recall target regardless of *where* it is performed. ACI's
   machine servicing is ACI's own named activity, so those entries stand.
2. **Precision** — the judged code still separates `I` (work performed at a
   customer's site, on something that is not the subject's own work-in-process)
   from `V` (an operation on the work), and `I` rolls up to `adjacent_field`,
   not `in_field`. Steelcraft's installation flood (anchor prep, field
   assembly) is the case this protects: it is real text, correctly extracted,
   and still not a manufacturing process capability.

So a field-site operation can be BOTH a legitimate recall target and an
adjacent_field judgment. That is not a contradiction: recall asks "did search
find what the text names?", precision asks "is this the field's kind of
thing?". Only the second is a boundary question.

**Consequence accepted:** austinelectric is under-seeded relative to this rule
(its construction field work is unseeded). Left as is on purpose — it is the
out-of-domain NEGATIVE probe, where under-seeding can only produce a false OK,
never a false RED, and its value is measuring junk-minting rather than recall.
Recorded in expectations/VERIFICATION_LEDGER.md.
