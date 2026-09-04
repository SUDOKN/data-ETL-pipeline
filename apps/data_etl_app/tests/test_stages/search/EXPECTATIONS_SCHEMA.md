# Expectations schema — the evolving eval set for the search stage

One YAML file per (subject, field) under `expectations/<subject_slug>/<field>.yaml`,
plus one `expectations/<subject_slug>/subject.yaml`. Subject slugs use the dump
convention: dots become underscores (`alecmfg_com`).

## Semantics (read this before adding an entry)

The search stage extracts short verbatim **surface forms** ("designations, not
claims") that name an in-field entity anywhere in the text. The pipeline is
**recall-first at search**: attribution (whose machine, whose certificate) is
downstream's job. Therefore:

- An entity belongs in the `entries` (must-find) list if the TEXT names an
  in-field thing — even when the actor is a client, supplier, lab, or a resold
  third-party machine. Actor concerns are recorded as `actor` metadata, and are
  measured on the precision/judged side, never by excluding an entity from recall.
- Entries are **designations** (the name of the thing), not sentences.
- Recall at eval time is window-scoped: an entry only counts against a run if at
  least one of its `evidence` quotes occurs inside a window that run actually
  searched. Quotes are located by string match against the run's wire text —
  never by raw offset (the sample file bytes differ from the normalized wire
  text; offsets in this file are advisory).

## File shape

```yaml
subject: alecmfg.com
field: products            # industries | process_caps | material_caps |
                           # equipments | conformity_attestations | products
snapshot:
  file: apps/data_etl_app/tests/test_stages/sample_scraped_texts/alecmfg.com.txt
  sha256: "<sha256 of that file at annotation time>"
eval_set_version: 1        # bump on any change to entries/false_friends
expected_empty: false      # true = the text offers (almost) nothing in-field;
                           # any sizable yield is junk-minting signal
entries:
  - id: alecmfg_com-products-0001    # stable; never reuse after retirement
    name: "High-Precision Stainless Steel Valve Body"
    status: candidate      # candidate | confirmed | disputed | retired
    acceptable_forms:      # any ONE of these (case-insensitive, containment
                           # either way) covers the entity
      - "High-Precision Stainless Steel Valve Body for Semiconductor Equipment"
      - "stainless steel valve body"
    evidence:              # >=1 verbatim quote CONTAINING an acceptable form
      - quote: "High-Precision Stainless Steel Valve Body for Semiconductor Equipment"
        approx_offset: 101234   # advisory only
    actor: client          # own | client | supplier | lab | parent_sibling |
                           # reseller_inventory | unknown  (omit when own)
    notes: "case study part; still a product designation in-text"
    provenance:
      - {action: added, by: "seed-agent", date: "2026-08-26", source: corpus-seed}
false_friends:             # strings that look in-field but are not; the judged
                           # pass counts any of these appearing among returned forms
  - form: "Lead"
    reason: "occurs only inside lead time / leadership / heavy-lift"
  - form: "Defense"
    reason: "word-association: 'defense against harsh weather'"
```

`subject.yaml` records: `role`, an optional free-text `business_type`, scrape
hazards (short quotes), sampling notes (acimachine is sampled, not exhaustively
read), and the file sha256.

### `role` — what the subject is FOR in the corpus

`role` is the subject's job in the eval set, not its industry. It answers "what
would we stop being able to measure if this subject were removed?"

- `full_unit` — an ordinary manufacturer, read exhaustively; the baseline case.
- `inventory_subject` — a catalog-shaped site where the interesting question is
  breadth of listed items rather than depth of claims.
- `scale_stress` — text large enough that chunking and windowing are themselves
  under test.
- `out_of_domain_negative` — not a manufacturer at all; measures what the stage
  mints from irrelevant text.
- `degenerate_input_probe` — the SCRAPE is the test (cookie modals, error pages,
  duplicate stubs), not the company.
- `attribution_negative` — **added 2026-08-27.** The text names real in-field
  entities that belong to SOMEONE ELSE: brokers, sales agencies, distributors and
  sourcing intermediaries writing partners' capabilities in the first person.
  Search is recall-first, so these are must-find entries with a non-`own` `actor`
  — the subject exists to measure whether the ATTRIBUTION survives downstream,
  and its recall here should be as high as any other subject's.
- `genre_probe` — **added 2026-08-27.** A prose genre the corpus otherwise lacks
  (investor-relations filings, SEO process encyclopedias, second-person technical
  manuals, engineering-textbook chapters with "we" injected). The entities are
  real designations in real text; what is being probed is whether the stage can
  read that genre at all.
- `empty_field_probe` — **added 2026-08-27.** A healthy, well-formed site where
  the correct answer for one or more fields is genuinely the empty list. Distinct
  from `degenerate_input_probe`, where emptiness comes from a broken scrape.

Pick the role that names the reason the subject was added. Where two apply, put
the primary one in `role` and say so in `notes`; the business description belongs
in `business_type`, which is free text and never gates anything.

**A subject's role never changes what goes in its inventories.** An
`attribution_negative` subject is seeded exactly like a `full_unit` one — the
role changes how its `actor` labels are read downstream, not which entities the
text names.

## Entries beyond a run's read coverage are RESERVED, not defective (user ruling 2026-09-01)

An entry whose evidence quote sits outside every window a given run actually
read is **reserved** for that run: it does not count in the recall denominator
(the mechanical pass already excludes it as `confirmed_out_of_coverage`), and
its existence is not a defect. The moment a run's coverage reaches it — larger
caps, different chunking, a deeper read — it activates automatically: it gates
recall AND becomes open to critique under the doubt-the-corpus rule, like any
other entry.

Reserved is a per-run, computed condition, NOT a status value written into the
YAML: which entries lie beyond coverage depends on that run's chunking and
token caps, so a stored label would go stale the day either changes. Do not
add a `reserved:` field; the scorecard's out-of-coverage count is the record.
Seeding beyond the current production read budget remains allowed — such
entries are an investment that activates when coverage grows.

## Status lifecycle (locked by user decision 2026-08-26)

Agents/assistant add entries as `candidate` with provenance. Promotion to
`confirmed` — the only status that gates recall — requires a SECOND independent
pass (a different agent or the assistant) re-reading the text and agreeing; the
promotion is a provenance row. The user may veto or mark `disputed` at any time;
`disputed` entries never gate. Retire (never delete) entries that turn out wrong,
with a provenance row saying why. Recall numbers always state the
`eval_set_version` they were computed against.

## Two scoring properties that are DELIBERATE — do not "fix" them into strictness

Both were raised by an independent verification pass (2026-08-26) and kept on
purpose. Under recall-only gating, a lenient recall metric merely under-fires;
a strict one manufactures FALSE REDs on correct output, which this harness has
already done once (the NBSP defect). Leniency is the safe direction, and the
judged census — not the recall gate — is what catches over-generation.

**1. Coverage is entry-level, not window-level.** An entry counts as covered
when ANY window holding its evidence finds a covering form. So an entity whose
evidence quote also appears in a page footer (e.g. `Tool & Die`, present in 15
copyright footers) is in scope for many windows but is NOT charged a miss for
each window that correctly stayed silent. Only entries covered NOWHERE are
misses.

**2. Containment credits a broader entry when a narrower form is returned.**
`Steel` is covered when search returns `Stainless Steel`; `shear` by
`Shearing`; `press brake` by `CNC 7 Axis Press Brakes`. That is right on the
recall question — the capability was found — but it means an entry that is a
substring of its siblings can never independently miss, so its recall number
carries no information of its own. Prefer distinctive `acceptable_forms`, and
do not read a broad entry's coverage as evidence about that entity alone.

Short forms are the exception and are strict: `EMS` is NOT credited by
"quality assurance systems", because forms of three characters or fewer must
match on word boundaries case-sensitively (see `_shared/text_matching.py`).

**The leniency runs ONE WAY, and did not until 2026-08-28.** The matcher asked
whether either form contained the other, so a returned FRAGMENT credited the
entry it was a fragment of: the bare word `titanium` credited `titanium fusion
cages`, `metal` credited `sheet metal`, `Cutting` credited `laser cutting`.
That was never the policy this section describes — the examples above are all
"returned form is at least the designation" — it was an accident of a symmetric
implementation, and because false credit only ever flatters the stage nothing
downstream ever looked broken. 410 blind spot-checks of AWARDED credits found
33 false (8.0% overall, 19.1% for products), worth 1.7 points of overall
confirmed recall. `forms_overlap` was replaced by `form_covers(expected,
returned)`, which also anchors the LEFT word boundary at every length (`Stem`
was being credited by `MICROWAVE SYSTEM`). The right edge stays open, which is
what keeps `shear` → `Shearing` working.

## An expectation must encode the FIELD'S DEFINED BOUNDARY, not the field's name

Learned the hard way 2026-08-27. The alecmfg equipments set was seeded from
the plain meaning of "equipment" and included a CMM, a laser interferometer, a
Renishaw probe, a vacuum fixture and bending molds. The equipment search
prompt excludes all of those on purpose — a production machine "acts on the
work itself, which is what separates it from equipment that only measures,
inspects, or tests the work ... and from tooling". So search was obeying its
definition, the eval called it a 6-miss recall failure, and a **false RED**
fired on correct behaviour.

Before seeding a field, read that field's `What qualifies as a phrase` clause
in `knowledge/prompts/final_texts/static/multi_stage/1_phrase_search/` and
seed to THAT boundary.

Where the two genuinely disagree — you believe the field SHOULD include
something its prompt excludes — keep the entry, set it `disputed`, and say so
in the notes. It then never gates, but it is on the record as a boundary
question for the user, and it can be re-promoted in one edit if the boundary
moves. Deleting it would lose the argument.

## The eight boundary rulings of 2026-09-03 (user; adopted per assistant recommendation)

Applied to the markdown set the same day (16 promotions, 19 disputes, 10
dispute-upholds, each with a `user-boundary-ruling-2026-09-03:*` provenance
row). Seed to these boundaries:

1. **Tooling is NOT equipment** (dies, punches, jigs, fixtures, molds, end
   mills — what a machine holds, guides, or acts through). SUDOKN has no
   tooling-as-equipment concept; making or designing tooling is a PROCESS
   capability (DieMaking/MoldMaking/ToolDesign...), tooling sold is a
   product. Machines NAMED after tooling processes (punch press, die-casting
   machine, die hammer) are machines and stay in.
2. **Software is NOT equipment** (CAD/CAM/ERP names).
3. **Plant and material handling stay OUT of equipments** (the prompt's
   powers/conditions/moves/stores/carries clause stands). A furnace or oven
   that treats the WORK is production equipment and stays in.
4. **RE-RULED 2026-09-04 (user): purely-sold machines are PRODUCTS, not
   equipments.** A machine appearing only as sale inventory (dealer
   catalogs, resale listings, build-to-sell lines) is a made artifact —
   seed it in `products` with `actor: reseller_inventory`. `equipments`
   keeps only machines in DESCRIBED PRODUCTION USE by someone in the text
   (the subject's shop, a client's line, a principal manufacturer's plant —
   actor flags carry whose). Non-machine components a subject sells
   (valves, sensors) were always products. The original 2026-09-03 form of
   this ruling ("merchandise machines are IN by nature" for equipments) is
   superseded; 127 reseller_inventory equipments entries (acimachine 95,
   agstech 32) were retired under this re-ruling, their merchandise living
   in `products`.
5. **Engineering and design work IS a process capability** (SUDOKN
   EngineeringCapability / EngineeringDesignCapability / ToolDesign /
   RapidPrototyping / ReverseEngineering...). The search static now carries
   the inclusion sentence. DFM, process design, simulation, analysis,
   programming, prototyping: in-field.
6. **Job-ad and biography evidence does not attest the subject's
   capabilities**: an entry whose ONLY evidence is a hiring ad, a
   director's/founder's biography, or a personal credential is `disputed`
   (the entity class may be fine — re-promote on shop-capability evidence).
7. **Self-published governance/ESG/legal-compliance documents are NOT
   conformity attestations** (charters, policies, codes of conduct, modern
   slavery / transparency / Section 172 statements, GDPR-page items).
   Third-party certifications, registrations and product-standard
   compliances stay.
8. **A standards BODY's name is not an attestation** (ISO/ANSI/JEDEC/GEIA as
   organizations, association memberships). A certification, approval, or
   standard designation ISSUED by a body stays in (`SDI Certification`,
   `CAAC Maintenance Organization` approval, `ITU grid`, `AISI 304`).

## `out_of_snapshot` — reserved by the snapshot itself (v3 port, 2026-09-03)

The 100k crawl cutoff (and ordinary site drift) can remove the pages an
entry's evidence lives on. Such an entry is verified judgment, not a defect —
the reserved ruling applies — so the port marks it `out_of_snapshot: true`
with a provenance row instead of retiring it. Consequences: the validator
exempts it from the quote contract (and errors on a STALE marker whose quote
does occur — re-run the port); the eval needs no special handling, because an
entity absent from the snapshot is absent from every run window and lands in
`out_of_coverage` — the reserved state — mechanically. A future port against
a larger snapshot clears the marker and the entry reactivates, gating again.
Kept entries whose evidence list mixed surviving and dead quotes have the
dead ones pruned (`pruned-dead-quotes` provenance); the surviving evidence
carries the entry. v3-port outcome: 5,421 kept (83 quotes pruned across 78),
4,770 held (agstech 4,495 — its 165-page site read at 38 pages), 637 retired
carried, all snapshot pins updated.
