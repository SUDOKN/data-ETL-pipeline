# Seeding brief — how a new corpus subject gets its golden data

This is the instruction set handed verbatim to every seeding agent, and (minus
§7) to every verification agent. It exists so that a subject seeded a year from
now is seeded the same way as the first eight. Read `EXPECTATIONS_SCHEMA.md`
alongside it — this brief is the *procedure*, that file is the *contract*.

---

## 1. What you are producing, in one sentence

A **must-find inventory**: every in-field entity the subject's scraped text
actually names, each with the verbatim words it is named by and a verbatim
quote proving it, so that a later run of the search stage can be scored for
recall against something a human derived from the text rather than from the
model's own output.

You are **not** judging the pipeline. You are reading a website and writing
down what is in it.

## 2. The one rule that has already caused a false alarm

**Seed to the field's `What qualifies as a phrase` clause, quoted in §4 below —
never to the plain meaning of the field's name.**

On 2026-08-27 the alecmfg `equipments` inventory was seeded from the everyday
sense of "equipment" and included a coordinate measuring machine, a laser
interferometer, a Renishaw probe, a vacuum fixture and bending molds. The
equipment search prompt excludes every one of those on purpose. Search obeyed
its own definition; the eval called it a six-miss recall failure; a RED verdict
fired on correct behaviour. Six entries had to be walked back.

If you believe a field *should* cover something its clause excludes, still write
the entry, mark it `"status_hint": "disputed"` and say why in `notes`. It then
never gates, but the argument survives on the record and can be re-promoted in
one edit. Deleting it loses the argument.

## 3. Recall-first: the actor never excludes an entity

The search stage is deliberately recall-first. Attribution — whose machine,
whose certificate, whose customer — is a **downstream** stage's job. So:

- A certification held by a **supplier**, a machine owned by a **client**, a
  process performed by a **partner plant**: all are entries. The text names an
  in-field thing, so search is right to return it.
- Record the attribution in `actor` instead: `own`, `client`, `supplier`,
  `lab`, `parent_sibling`, `reseller_inventory`, `unknown`. Omit for `own`.
- This means the open attribution questions about the intermediary subjects
  (partially-owned partner plants, brokers, distributors, sales agencies) do
  **not** block you. They change the `actor` label, never the entry's
  existence.

The one thing that *is* excluded: an entity that the text does not name at all.
An inventory is a record of what is written, not what is true about the company.

## 4. The six field boundaries, quoted from the search prompts

These are copied verbatim from
`knowledge/prompts/final_texts/static/multi_stage/1_phrase_search/`. If a prompt
changes, re-copy them here and re-check the affected inventories.

### `industries`
> A qualifying phrase names, states or implies an industry, market, or sector —
> a recognizable domain of economic activity that a manufacturer's goods or
> services can serve.

### `process_caps`
> A qualifying phrase names, states or implies a manufacturing process,
> operation, or capability — an operation performed on materials, parts, or
> products at any stage of production, from raw material through to finished
> goods ready to ship. **Inspection, testing, and measurement performed on the
> work are operations, and qualify.**

### `material_caps`
> A qualifying phrase names, states or implies a material — a substance that the
> work is made from or performed in, whether named generically, by trade name,
> or by an alloy or grade designation. **A process or treatment named after a
> substance it deposits on or diffuses into the work does imply that substance,
> and qualifies.**

### `equipments`
> A qualifying phrase names, states or implies a production machine — a device
> or system used directly to process, shape, assemble, or convert materials into
> finished goods. A production machine **acts on the work itself**, which is what
> separates it from equipment that only **measures, inspects, or tests** the
> work, from equipment that **powers or conditions the plant** or **moves,
> stores, or carries** what it makes, and from **tooling**, which is what a
> machine holds, guides, or acts through rather than the machine driving it.

Read that exclusion list twice. A CMM, a hardness tester, an air compressor, a
forklift, a storage rack, a die, a mold, a jig, a fixture and a cutting insert
are all **not** equipment here. Most of them are `process_caps` (inspection and
testing qualify there) or `material_caps`, so they are rarely lost — put them
where they belong instead of dropping them.

> **2026-09-04 drift note.** The IN-REPO search statics carry drafted changes
> NOT yet published to the pins production runs use: equipments gains
> metrology INCLUSION ("...or to measure, inspect, or test the work") and an
> explicit software EXCLUSION; products gains a tooling carve-back (dies,
> molds, jigs, fixtures the shop MAKES are products); process_caps gains a
> software exclusion beside its engineering-inclusion sentence (user ruling
> 2026-09-04: software is excluded from EVERY field until a SoftwareCapability
> class exists in the ontology). Seed to the clauses quoted ABOVE (the
> published boundary) until the user publishes; at publish, re-copy every
> clause here and re-check the affected inventories (`equipments`: CMMs,
> testers and gauges become in-field; `products`: made tooling entries
> promote; software false-friends stay valid everywhere).

### `conformity_attestations`
> A qualifying phrase names, states or implies a certification, accreditation,
> registration, standard, or compliance — one with an identity of its own, such
> as a name, number, mark, or issuing body, that a company, its people, its
> sites, or its products could specifically hold, meet, or follow.

Trade-association **memberships**, business-registry identifiers (D-U-N-S),
contractor **licences** and staff **job titles** have repeatedly been argued
either way. Seed them `disputed` with the argument in `notes`.

### `products`
> A qualifying phrase names, states or implies a made artifact — a product,
> part, component, system, or item that comes out of manufacturing work, whether
> an own-brand good or something made or worked on for a customer. **Coming out
> of the work is what qualifies an artifact**, which separates it from the
> machinery, tooling, and facilities the work is done with, and from awards,
> memberships, training, documents, and the names of companies, brands, or
> customer groups.

`products.yaml` doubles as the `contract_products` inventory — they share one
physical search request. Do not write a separate file.

## 5. What an entry looks like, and what makes a good one

```json
{"field": "process_caps",
 "name": "vacuum brazing",
 "acceptable_forms": ["vacuum brazing", "vacuum-brazed", "vacuum braze"],
 "evidence": [{"quote": "our vacuum brazing furnaces run continuously"}],
 "actor": "own",
 "notes": ""}
```

- **`name`** is your label for the entity — it is not matched against anything.
- **`acceptable_forms`** are the surface forms that should COUNT as finding it.
  Any one of them credits the entry. Include the inflections and spellings the
  text actually uses.
  - **Prefer distinctive forms.** A form that is a substring of its siblings
    (`Steel` when the text is full of `Stainless Steel`) can never independently
    miss, so its recall number carries no information. Broad entries are allowed;
    just do not rely on them.
  - Forms of **three characters or fewer** (`TIG`, `ABS`, `CMM`) are matched
    case-sensitively on word boundaries. Get their casing exactly right.
  - **FOUR-character forms are the danger zone.** They are one character over
    that rule, so they match by plain containment with no word boundary — and
    they are short enough to sit inside ordinary words. Real cases found in this
    corpus: `STEM` inside *system*, `Iron` inside *environment* (49 hits),
    `Hone` inside *Phone* (172 footer lines), `NACE` inside *furnace*, and the
    original `Lead` inside *lead time*. Before seeding any four-character form,
    grep the whole text for it as a SUBSTRING, not as a word.
- **`evidence`** — at least one quote, copied **character for character** from
  the file, that CONTAINS at least one acceptable form. This is checked
  mechanically and a mismatch is a hard error.
  - Keep quotes to roughly one line, and **at most 200 characters** — the
    harness enforces that cap. Never let a quote cross a line break: the scraped
    text is line-oriented and the matcher will not find it. When the only
    occurrence sits inside a long comma-separated service list, quote a window
    around your form rather than the whole list.
  - The text carries **non-breaking spaces and double spaces inside phrases**.
    Copy the span as-is; the matcher treats whitespace runs as elastic.
  Do not retype a quote from memory. Copy it out of the file.

### `false_friend` rows
Strings that look in-field and are not — the traps you want a later judged pass
to count. Two useful kinds, both worth writing:
- **witnessed**: the string occurs in the text but not as an in-field thing
  (`Hemming` occurring only inside the `HEM Saw` brand name).
- **anticipatory**: the string does not occur at all, but a model is likely to
  mint it from word association. Only fabrication can hit it, which is itself
  worth catching.

## 6. Exhaustiveness

Read the **entire** file. Not a sample, not the pages that look promising.

- Work through it in slices (`sed -n '1,400p' <file>` and so on) and keep
  running notes as you go; do not try to hold the whole site in your head.
- Then run targeted `grep` sweeps for the things prose hides: standards bodies
  (`ISO`, `ASTM`, `ANSI`, `API`, `AMS`, `AS`, `NADCAP`, `UL`, `CE`, `EN`), brand
  names near machine words, alloy and grade patterns (`\b[0-9]{3,4}\b` near
  metal words), and every `-ing` process word.
- The two passes catch different things. Reading finds entities named in
  sentences; grepping finds them in tables, nav bars and specification cells.
- **A page you skipped is a miss you will not know you have.** If the text is
  large enough that you genuinely cannot finish, say so explicitly in
  `sampling_notes` and name the byte ranges you covered — an honest partial
  inventory is usable, a silent one poisons every recall number computed from it.

## 6b. Very large subjects: slicing, and the one hazard it introduces

A subject too big to read in one pass (agstech.net, 1.8 MB) is split into
PAGE-ALIGNED slices, one agent each, every slice covering all six fields. Cut on
page boundaries — the scraped text puts each page's URL on its own line — so no
agent inherits half a page. Entries are deduplicated by name at build time, so
overlap between slices is harmless and slice agents emit only entry and
false_friend rows; the `subject` row and `field_meta` rows are composed centrally
once every slice has landed.

**Slice agents cover catalog structure well and PROSE badly.** Measured on
agstech: the ten verification packets added 512 misses, and the recurring shape
was a material or process dropped from the very sentence the seed had already
quoted for a sibling entity, or an enumerated list harvested part-way. When you
finish a slice, re-read your own quotes and ask what else each sentence names.

**Two further failure modes, both observed live on 2026-08-27:**

- **The scratchpad is SHARED between parallel agents.** Two slice agents wrote a
  helper called `emit.py` and silently overwrote each other; one then executed
  its sibling's script and wrote to its sibling's output path. Both caught it,
  but only because their OUTPUT filenames were slice-numbered. Put helper scripts
  in a per-agent subdirectory and give every generated file a name unique to your
  slice.
- **Boilerplate falls between slices.** Site-wide nav and footer text is
  byte-identical on every page, so a slice agent reasonably skips it as already
  covered — and if every agent reasons that way it is covered by nobody. Whoever
  composes centrally must check that the nav/footer designations survived the
  merge, and seed them if not.

## 7. Output (seeding agents only)

Write **one JSON object per line** to the path you were given. No prose, no
markdown fences, no wrapping array — the file is read line by line.

Row types: `subject` (once), `field_meta` (once per field, carrying
`expected_empty`), the entry rows shown in §5, and `false_friend` rows.
`build_expectations.py` turns them into the YAML: it assigns ids, computes
offsets, writes the snapshot header and sets every entry to `candidate`.

**`expected_empty: true` is a finding, not a gap.** Several corpus subjects
have fields whose correct answer is the empty list — a sales agency with no
processes, a toll processor with no products. Say so explicitly rather than
straining to fill the field; anything the stage mints there is junk-minting
signal, which is exactly what those subjects are in the corpus for.

## 8. The verification pass

A second agent, which never sees the seed's notes or rationale, re-derives every
entry from the text and returns `confirm` / `dispute` / `retire` plus any
entities the seed missed. Only `confirmed` entries gate recall. That agent's
instructions are in `VERIFY_BRIEF.md`.
