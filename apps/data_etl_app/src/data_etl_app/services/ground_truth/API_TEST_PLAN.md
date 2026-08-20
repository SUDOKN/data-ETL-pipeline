# LLM Phrase GT API — live test plan (the P3 gate)

**Purpose:** the end-to-end run the P3 REVIEW gate requires — template → phrase →
submissions → truth, driven from Postman against a live server, real Mongo, real S3,
real deployed catalogs. Everything here has offline test coverage (suite 1069); this
run is about what the unit tests *can't* see: index seeding, S3 fetch, catalog
deployment state, URL normalization against real data, and the ergonomics of actually
annotating through this API.

**How we work it:** run cases in order (groups are topologically sorted — later groups
consume documents and state the earlier ones create). Each case has a stable ID; when
something surprises you, paste the response back tagged with the ID and we discuss.
Tick the box and jot PASS / FAIL / SKIP / **FINDING** (works, but revealed something
worth a decision) next to it.

**Legend:** ⚠ *conditional* = only testable if your data has the shape (skip with a
note otherwise). ✋ *needs a second author* = use the admin user as annotator B.

---

## Postman environment variables

| Variable | Value |
|---|---|
| `base` | `http://127.0.0.1:8000` (default `uvicorn` port) |
| `annotator` | `annotator@test.local` (role `annotator`) |
| `admin` | `admin@test.local` (role `admin` — doubles as annotator B) |
| `default_user` | `default@test.local` (role `default`) |
| `ghost` | `ghost@test.local` (NOT in the users collection) |
| `mfg_url` | a manufacturer with fresh extraction results |
| `kw_field` | one of `products`, `contract_products`, `equipments` |
| `con_field` | one of `industries`, `conformity_attestations`, `material_caps`, `process_caps` |
| `doc_kw`, `doc_con` | filled from B6/B10 responses |

`author_email` is always a **query param** (Params tab), even on the POST.

---

## S. Setup checklist

- [ ] **S1** Server up: `PYTHONPATH=src uvicorn data_etl_app.main:app --reload` from
  `apps/data_etl_app`. Mongo + AWS env loaded (the detail plane and missed-phrase
  submissions fetch the pinned text from S3 live).
- [ ] **S2** Indices seeded (`scripts/db_seed_indices.py`, however you normally run
  it) — get-or-create's duplicate-key race handling and the one-doc-per-identity
  guarantee depend on the unique index actually existing.
- [ ] **S3** Seed the three users (mongosh; dummy salt/hash — this plane has no
  password auth):

  ```js
  db.users.insertMany([
    {firstName:"Ann", lastName:"Notator", email:"annotator@test.local", role:"annotator",
     companyURL:null, salt:"x", hashedPassword:"x", createdAt:new Date(), updatedAt:new Date()},
    {firstName:"Ad", lastName:"Min", email:"admin@test.local", role:"admin",
     companyURL:null, salt:"x", hashedPassword:"x", createdAt:new Date(), updatedAt:new Date()},
    {firstName:"Reg", lastName:"Ular", email:"default@test.local", role:"default",
     companyURL:null, salt:"x", hashedPassword:"x", createdAt:new Date(), updatedAt:new Date()},
  ])
  ```
- [ ] **S4** Pick the subject: a manufacturer whose `Manufacturer` doc has non-null
  results for `{{kw_field}}` AND `{{con_field}}` (ideally the subject of your latest
  notebook run). Verify the slots in Mongo before starting.
- [ ] **S5** Catalog-pin sanity: the run's stored `catalog_version`s must match the
  deployed catalogs in `knowledge/prompts/rule_catalog/`. If you've edited catalogs
  since that run, the first template GET will 409 (that's case B14 firing early) —
  the remedy is a fresh extraction run from the notebook, then restart here.
- [ ] **S6** Import `LLM_Phrase_GT_API.postman_collection.json` (this folder) — one
  request per case ID below, expected statuses as Postman tests, doc ids / digests /
  first phrases auto-captured into collection variables by B6/B10. Groups F–J carry
  `<<PASTE:…>>` placeholders for the section trees (recipes R2/R3) and H/I need
  integer/null coordinates edited by hand — it is a stepwise companion, not a blind
  Collection-Runner sweep. `{{base}}/docs` stays the live reference.
- [ ] **S7** Subject for this run: **alecmfg.com**, dump run `20260819T012442` — all 7
  phrase fields completed; pinned catalog versions verified equal to the deployed
  catalogs (checked 2026-08-18, all 17 node pins) → no 409 drift expected; B14 = SKIP
  unless catalogs change mid-test. Collection defaults: `kw_field=equipments`,
  `con_field=conformity_attestations` (smallest docs → fastest sittings).

---

## Error taxonomy (what each status means on this plane)

| Status | Meaning | Detail shape |
|---|---|---|
| 422 | Request shape rejected by pydantic — query params missing, body malformed, or a **model validator** fired (audit requiredness, missed-phrase happy path, divergence path shape) | FastAPI's error list |
| 400 | Route-level addressing (bad ObjectId, bad URL, unknown field_type, cursor misuse) — plain string; **or a rejected batch item** — structured `{"item_index": N, "surface": "...", "reason": "..."}` | string or structured |
| 403 | Registered user, wrong role | names the role and the allowed set |
| 404 | Missing thing: user, manufacturer, field results, document, chunk, phrase, exhausted cursor, no truth doc | string, says which |
| 409 | State conflict: catalog pin drift, or S3 text ≠ witness | string, names both versions/digests |
| 500 | Route bug (missing caller dependency) — should never appear | — |

---

## Payload recipes (used from group E on)

**R1 — audit entries: TWO kinds** *(split 2026-08-19)*. `at` is required, no
default; `source` is a free string today (see WATCH-1) — use `"postman"`.

**R1a — `TextFieldAudit`** — PROSE surfaces only: the relationship text audit
(`audit:` on a relationship item) and rule-level audits inside sections.
`corrected_text` = the addendum (`agree_but`) or the replacement prose
(`disagree`); optional `note` for extra rationale (forbidden on `agree`):

```json
{"type": "agree",     "author_email": "{{annotator}}", "at": "2026-08-18T12:00:00Z", "source": "postman"}
{"type": "agree_but", "corrected_text": "…the addendum…",   "note": "…optional context…", "author_email": "{{annotator}}", "at": "…", "source": "postman"}
{"type": "disagree",  "corrected_text": "…the replacement…","note": "…optional rationale…", "author_email": "{{annotator}}", "at": "…", "source": "postman"}
```

**R1b — `EntityFieldAudit`** — IDENTIFIER surfaces: the `audits` list inside a
screening derivation, a grounding derivation (oov/initial tags), an in-vocab
node keep, and divergence hops. **No `corrected_text`** — the asserted value
IS the derivation's `identified_entity`/`tag`. Verdicts `agree | disagree`
only; `note` REQUIRED on `disagree` (the only prose slot), forbidden on
`agree`:

```json
{"type": "agree",    "author_email": "{{annotator}}", "at": "…", "source": "postman"}
{"type": "disagree", "note": "…why the LLM's identifier is wrong and yours right…", "author_email": "{{annotator}}", "at": "…", "source": "postman"}
```

**R2 — keep-path mirror.** To audit while keeping the LLM's entity/tag/link: GET the
phrase detail, copy the relevant `sections` tree **verbatim** (same section ids,
combinators, rule ids, nesting, order — the shape check compares exact signatures),
then add R1a entries to the `audits` of the rules you judged. Any change to a rule's
`outcome`/`explanation` without an audit entry on that rule is a 400 ("no silent
edits").

**R3 — fresh tree** (entity/tag replacement, human-asserted tag, missed-phrase
sections, divergence hops). Shape must equal the full catalog skeleton — easiest
source is again a copied LLM tree of the same stage (inflation always materializes
the whole catalog). Then:
- every non-`note` rule needs an `outcome` from its kind's vocabulary (read the
  catalog JSON in `knowledge/prompts/rule_catalog/`, or crib from the LLM's own
  reported outcomes) — **`quality` rules included**;
- `ordered` sections: exactly one rule with outcome `"chosen"`;
- guards: their vocab, or the silence marker `not_violated`;
- `note` rules: `reported=false`, no outcome;
- keep each rule's `reported` flag as copied; `reported=true` rules must carry
  outcome AND explanation;
- at least one audit entry somewhere (entity/tag audit or a rule audit), all authored
  by the submitting user.

**R4 — submission wrapper** (author in the URL, not the body):

```
POST {{base}}/ground_truth/llm-phrase/submissions?author_email={{annotator}}
{"document_id": "{{doc_kw}}", "items": [ …surface items… ], "return_full": false}
```

---

## A. Auth & role gates

On the template GET unless noted.

- [ ] **A1** No `author_email` param → **422** (required query param).
- [ ] **A2** `author_email={{ghost}}` → **404** "no registered user… register on sudokn.com".
- [ ] **A3** `{{default_user}}` → **403** naming role `'default'` and allowed `['admin', 'annotator']`.
- [ ] **A4** `{{annotator}}` → passes the gate (result is whatever group B says).
- [ ] **A5** `{{admin}}` → passes the gate.
- [ ] **A6** `{{default_user}}` on **phrase GET** → **403**; on **submissions POST** → **403**.
- [ ] **A7** `{{default_user}}` on **truth GET** and **list GET** → **200/404-for-data** but never 403 (X6: truth is readable by any registered user).

## B. Template GET — the browse plane (creates the documents)

`GET {{base}}/ground_truth/llm-phrase/template?author_email={{annotator}}&mfg_url={{mfg_url}}&field_type=…`

- [ ] **B1** `field_type=garbage` → **400** listing all seven values.
- [ ] **B2** `field_type=Products` (wrong case) → **400** (enum values are exact).
- [ ] **B3** `mfg_url=http://` (no hostname) → **400** "no valid hostname".
- [ ] **B4** Valid URL, manufacturer not in DB (e.g. `https://definitely-not-a-subject.example.com`) → **404** "run extraction first; nothing has been queued for scraping" (and confirm nothing WAS queued).
- [ ] **B5** Real manufacturer, a field whose slot is null → **404** "no stored … results — nothing to audit".
- [ ] **B6** Happy path `{{kw_field}}` → **200**. Browse shape: `document_id` (→ save as `{{doc_kw}}`), `created=true`, `mfg_etld1`, `field_type`, witness (`scraped_text_file_version_id`, `scraped_text_sha256`, `scraped_text_char_len`), `identity_digest`, chunks keyed `start:end` in ascending start order; each phrase row has `search_round`, `llm_relationship_text` == `effective_relationship_text`, `relationship_addendum=null`, both reviewed flags false; `missed_phrases` empty.
- [ ] **B7** Same GET again → `created=false`, **same** `document_id` and `identity_digest` (get-or-create idempotence).
- [ ] **B8** URL-variant equivalence: repeat with `www.` prefix, bare domain, trailing slash, an inner path — all → same `document_id`, `created=false` (normalization + etld1 keying).
- [ ] **B9** `&full=true` → the whole saved document: `id`, `created`, embedded `metadata`, `run_identity`, full `chunks` trees. Spot-check: screening sections contain `reported=false` synthesized rules (auditable silence), stored `passed` present per phrase.
- [ ] **B10** Happy path `{{con_field}}` → **200**, save `{{doc_con}}`. With `full=true`: phrases carry `oov_grounding.tags` (the initial-map entries) and `in_vocab_grounding.levels` (the descent, `parent_group_id=null` at the roots, `stop_reason` on terminals). *(Regression case: the first live run 2026-08-18 hit the sentinel-inflation gap — "tag 'None of the above' has no rules" as a generic 400 — fixed via `OovGroundingGT.llm_declined`; see the results journal.)*
- [ ] **B11** Mongo: `llm_phrase_ground_truths` holds exactly the 2 docs (B7/B8 made no duplicates).
- [ ] **B12** Truth GET baseline for both docs → **200**: everything unreviewed, rule-agreement rollup empty (unreviewed nodes are excluded, not counted as agreement).
- [ ] **B13** List GET baseline → both rows, newest first, per-doc counts: `unreviewed` == total phrases, missed 0.
- [ ] **B14** ⚠ *conditional* — catalog drift → **409** naming the stored and deployed versions. Only observable if you're in the drifted state (see S5); otherwise SKIP.

## C. Phrase GET — the detail plane

`GET {{base}}/ground_truth/llm-phrase/phrase?author_email={{annotator}}&document_id={{doc_kw}}&…`

- [ ] **C1** `chunk_key`+`phrase` AND `next_unreviewed=true` → **400** "not both".
- [ ] **C2** Neither; only `chunk_key`; only `phrase` → **400** each.
- [ ] **C3** `document_id=not-an-id` → **400**.
- [ ] **C4** Well-formed unknown ObjectId (e.g. `0123456789abcdef01234567`) → **404** "fetch the template first".
- [ ] **C5** Unknown `chunk_key=9999:10999` → **404** listing the available keys.
- [ ] **C6** Real chunk, phrase not extracted there → **404** pointing at missed-phrase submission.
- [ ] **C7** Explicit happy path → **200**: `chunk_text` is the pinned text sliced by the chunk bounds (sanity: length ≈ `end−start`, content matches the phrases in it), full `trail` (relationship, screening with LLM copy, groundings per family), `effective` view, `reviewed=false`, `unreviewed_remaining` == total phrase count.
- [ ] **C8** `next_unreviewed=true` → **200**, serves the first phrase of the lowest chunk (same order as the browse view).
- [ ] **C9** A phrase containing `&`, `+`, `™`, or non-ASCII → **200** once percent-encoded (Postman gotcha: raw `+` in a query decodes as a space).
- [ ] **C10** Concept doc detail (`{{doc_con}}`): `in_vocab_grounding.levels` nodes carry `(parent_group_id, group_id, stop_reason, sections)`; initial-map tags in `oov_grounding.tags` with IGR sections.
- [ ] **C11** Keyword doc detail: freehand tags in `oov_grounding`, **no** `in_vocab_grounding`.
- [ ] **C10b** A sentinel-declined phrase (e.g. `D-U-N-S® Number` in conformity_attestations chunk `0:98612`): detail trail shows `oov_grounding.llm_declined=true` with `tags` empty (or the sentinel absent from `tags` when real tags exist beside it), AND the folded `effective.llm_declined=true` — so declined-vs-never-answered stays distinguishable in the truth plane too (truth GET carries the same flag per phrase). The sentinel itself never appears as a tag entry. Mid-descent declinations appear as stored terminal nodes (`stop_reason: "sentinel"`, empty sections) — unchanged.

## D. Submissions POST — request-shape rejections (no writes yet)

All against `{{doc_kw}}` with `{{annotator}}`.

- [ ] **D1** `items: []` → **422** (min length 1).
- [ ] **D2** `surface: "no_such_surface"` → **422** (discriminator).
- [ ] **D3** Extra unknown field on an item → **422** (`extra="forbid"`).
- [ ] **D4** `agree` WITH `corrected_text` → **422** ("forbidden on an 'agree'").
- [ ] **D5** `agree_but` without `corrected_text`; `disagree` with `corrected_text: "  "` → **422** each.
- [ ] **D6** Audit missing `at` → **422** (deliberately no default).
- [ ] **D7** `document_id: "junk"` → **400**.
- [ ] **D8** Well-formed unknown id → **404** "fetch the template first".
- [ ] **D9** Valid item, `chunk_key` not in doc → **400** structured `{"item_index": 0, "surface": "relationship_audit", "reason": …available keys…}`.
- [ ] **D10** Valid item, phrase not extracted → **400** structured, reason points at missed-phrase.
- [ ] **D11** *(added 2026-08-19)* `agree` WITH a `note` → **422** ("note is forbidden on an 'agree'").
- [ ] **D12** *(added 2026-08-19)* `disagree` with `note: "  "` (blank) → **422**.

## E. Surface 1 — `relationship_audit` (simplest write)

Item: `{"surface": "relationship_audit", "chunk_key": "…", "phrase": "…", "audit": R1a}`

- [ ] **E1** `agree` on phrase₁ → **200**: `applied_items[0]` echoes address+index, `unreviewed_remaining` dropped by 1, `updated_at`, `truth` block present.
- [ ] **E2** Browse verify: phrase₁ `relationship_reviewed=true`, `reviewed=true`, effective text unchanged. NOTE the cursor consequence: `next_unreviewed` now **skips phrase₁ entirely** even though its screening/grounding are untouched — reviewed = any-audit-anywhere-in-slice, by design (verdict 5b). Observe, don't fail.
- [ ] **E3** `agree_but` (same phrase, same author) → effective text unchanged + `relationship_addendum` in browse.
- [ ] **E4** `disagree` with a replacement → `effective_relationship_text` == the correction in browse and detail.
- [ ] **E5** After E1→E3→E4 (same author, consecutive), detail trail shows **one** audit entry (each pop-replaced the last), type `disagree`.
- [ ] **E6** ✋ Pop is stack-top-only: `{{annotator}}` audits phrase₂, then `{{admin}}`, then `{{annotator}}` again → trail = `[A1, B, A2]`, **three** entries (A1 survives because B sat on top).
- [ ] **E7** Audit's `author_email={{admin}}` in a submission by `{{annotator}}` → **400** "audit authored by … in a submission by …".
- [ ] **E8** Atomicity: `[valid agree on phrase₃, item with bad chunk_key]` → **400** naming index 1; re-GET phrase₃ → the valid item did NOT persist.
- [ ] **E9** Two valid items on different phrases in one batch → both applied, `unreviewed_remaining` −2.
- [ ] **E10** Same node twice in ONE batch (same author) → **200**, trail holds only the second (in-batch pop). Semantics observation.
- [ ] **E11** `return_full: true` → full document embedded in the response.
- [ ] **E12** *(added 2026-08-19)* `disagree` WITH a `note` → **200**; browse row carries `relationship_note`, and the detail trail's audit entry shows the note verbatim. On entity/tag disagrees (groups F/G) the note lives on the audit inside the derivation — visible in the detail trail.

## F. Surface 2 — `screening_derivation`

Item: `{"surface": "screening_derivation", "chunk_key", "phrase", "derivation": {"identified_entity", "audits": [R1…], "sections": […]}}`

- [ ] **F1** Keep-entity: entity = LLM's, sections mirrored (R2), one `agree` entity audit → **200**.
- [ ] **F2** Derivation with **no** audit anywhere → **400** "asserts nothing".
- [ ] **F3** Mirrored sections with one outcome flipped, **no** audit on that rule → **400** "no silent edits".
- [ ] **F4** Flip one condition to a failing outcome WITH a `disagree` rule audit → **200**. Then truth GET: effective `passed` flipped to false for that phrase; rollup gains a `disagree` and an `overridden` for that rule id.
- [ ] **F5** Replace-entity: `identified_entity` = your replacement, latest entity audit `disagree` with its `note` (R1b — no corrected_text exists on entity audits), sections **fresh** (R3) → **200**.
- [ ] **F6** Replace-entity rejections, one at a time → **400** each: (a) a rule deleted from the tree (shape mismatch); (b) a non-note rule left without outcome; (c) an outcome not in that kind's vocabulary; (d) a made-up `rule_id`; (e) no `disagree` entity audit; ~~(f) `corrected_text` ≠ `identified_entity`~~ *(2026-08-19: impossible by construction — the field no longer exists on entity audits)*; (g) ⚠ if the screening catalog has an `ordered` section: zero or two `chosen`.
- [ ] **F10** *(added 2026-08-19)* Entity-audit shape 422s: (a) entity audit WITH `corrected_text` → **422** (unknown field); (b) `agree_but` entity audit → **422** ("kept or replaced"); (c) `disagree` entity audit without `note` → **422** ("requires a non-blank note").
- [ ] **F7** `identified_entity: null` against an entity-bearing LLM copy → **400** "express rejection through rule outcomes; the fold decides passed".
- [ ] **F8** `disagree` entity audit while KEEPING the entity → **400** "incoherent".
- [ ] **F9** ⚠ Screening item on a phrase whose run never screened it (`llm_screening` null — exists only in stage-cut-off runs) → **400** "no LLM verdict to audit".

## G. Surface 3 — `oov_grounding_derivation` (keyword freehand / concept initial map)

Item: `{"surface": "oov_grounding_derivation", "chunk_key", "phrase", "tag": <addresses the stored entry>, "derivation": {"tag", "audits", "sections"}}`

On `{{doc_kw}}`:
- [ ] **G1** Keep-tag: `tag` == stored, derivation.tag same, mirror + agree → **200**.
- [ ] **G2** Replace-tag: `tag` == stored entry, derivation.tag = new label, `disagree` audit with `note` (R1b), fresh sections → **200**. `applied_items` has **no** `tag_classification` (keyword docs never classify).
- [ ] **G3** Human-asserted NEW tag: `tag` == derivation.tag, not in the stored map, ≥1 `agree` audit, fresh sections → **200**; full doc shows the entry with `llm_result: null`.
- [ ] **G4** `tag` ≠ derivation.tag and `tag` not stored → **400** "addressed by the derivation's own tag".
- [ ] **G5** derivation.tag = `"None of the above"` (also try `"Cannot categorize"`) → **400** "reserved non-labels never reach ground truth".
- [ ] **G6** ⚠ Human-asserted tag on a phrase with NO grounding block (a screened-out phrase) → **200**, creates the block (the human-flip path).

On `{{doc_con}}`:
- [ ] **G7** Keep an initial-map tag → **200**.
- [ ] **G8** **RESET** to a different ontology concept (replacement derivation per G2 mechanics, IGR sections fresh) → **200** + `applied_items[i].tag_classification: "in_vocab"`. Truth/detail verify: the replaced tag's stored descent is **excluded** from effective truth; the phrase records a direct link to the new concept.
- [ ] **G9** RESET to a non-ontology string → **200** + `tag_classification: "out_of_vocab"` — and it **persists** (the echo is the only typo guard, verdict 6d). Observe deliberately.
- [ ] **G10** `disagree` audit on an unchanged tag → **400** "incoherent — disagreeing means replacing it".
- [ ] **G11** One fresh-tree violation vs the IGR catalog (e.g. the ordered matching ladder with two `chosen`) → **400**.

## H. Surface 4 — `in_vocab_node_audit` (concept only, keep-path only)

Item: `{"surface": "in_vocab_node_audit", "chunk_key", "phrase", "level", "parent_group_id", "group_id", "derivation"}`

- [ ] **H1** Coordinates matching no stored node → **400** "no stored descent node at level …".
- [ ] **H2** Keep-path happy: derivation.tag == the node's `group_id`, sections mirrored, an `agree_but`/rule audit → **200**.
- [ ] **H3** derivation.tag ≠ `group_id` → **400** instructive redirect (divergence path / reset).
- [ ] **H4** `disagree` audit on the kept tag → **400** "disagreeing with the link itself is a divergence path".
- [ ] **H5** Silent outcome edit on node sections → **400**.
- [ ] **H6** ⚠ A node whose sections mix IGR + RGR rows (roots often do) → **200**, each section routed to its own catalog transparently.
- [ ] **H7** Any in-vocab item on `{{doc_kw}}` → **400** "no recursive-descent record in this run".

## I. Surface 5 — `descent_divergence` (concept only)

Item: `{"surface": "descent_divergence", "chunk_key", "phrase", "path": {author_email, at, source, anchor_level, anchor_parent_group_id, anchor_group_id, replaces_group_id, hops: [GroundingDerivation…], stopped}}`

Model-level (pydantic) rejections — expect **422**:
- [ ] **I1** `hops: []` → 422 "asserts nothing".
- [ ] **I2** `stopped: false` → 422 "explicit stop".
- [ ] **I3** A hop whose sections leave a condition unsatisfied → 422 "must ground to satisfaction".

Contract-level — expect **400** structured:
- [ ] **I4** Anchor coordinates not stored → 400 "the anchor is the last stored node you still agree with".
- [ ] **I5** `replaces_group_id` not a stored child of the anchor → 400 listing the stored children.
- [ ] **I6** `replaces_group_id: null` while the anchor HAS stored children → 400 "name the one this path replaces" (sibling addition is out of v1).
- [ ] **I7** A hop that is not a real child edge in the pinned ontology → 400 listing the true children.
- [ ] **I8** A sentinel label as a hop → 400 "the sentinel is the stop signal … end with stopped=true".
- [ ] **I9** `path.author_email` ≠ submitting user → 400.

Happy paths:
- [ ] **I10** Divergence: anchor at a mid-descent node, `replaces_group_id` = one stored child, one hop that IS a real ontology child of the anchor, fresh RGR sections (R3), `stopped: true` → **200**. Truth verify: the replaced child's subtree excluded, path row appended with `replaced_from` on the first hop, sibling branches still standing.
- [ ] **I11** Continuation-from-stop: anchor = a stored **leaf** (no children), `replaces_group_id: null`, hops descend further → **200**.
- [ ] **I12** Pop keying: same author resubmits same (anchor, replaces) → one path (replaced); same author, different `replaces` → both paths stand.
- [ ] **I13** Any divergence item on `{{doc_kw}}` → **400** "no recursive grounding — divergence paths do not apply".

## J. Surface 6 — `missed_phrase`

Item: `{"surface": "missed_phrase", "chunk_key", "entry": {phrase, author_email, at, source, relationship_text, screening: {identified_entity, audits, sections}, groundings: […]}}`
The entry is a full positive claim (PH-10): screening fresh + happy (R3), every grounding fresh + happy.

- [ ] **J1** Happy path on `{{doc_kw}}`: a real word from the chunk text the run missed, entity identified, screening sections satisfied, one freehand grounding → **200**; `applied_items` carries the phrase; browse shows the missed row (phrase, author, entity, at); truth shows it as a reviewed human positive.
- [ ] **J2** Phrase not present in that chunk's text → **400**.
- [ ] **J3** Substring-only presence (assert `weld` when the text has only `welding`) → **400** (whole-word).
- [ ] **J4** Case-insensitive whole word (`CNC MACHINING` vs `cnc machining` in text) → **200**.
- [ ] **J5** A screening section with an unsatisfied condition → **422** (model-level happy path).
- [ ] **J6** `identified_entity: null` → **422**.
- [ ] **J7** `entry.author_email` ≠ submitting user → **400**.
- [ ] **J8** ✋ Same author re-asserts the same phrase → replaced (still one entry, keyed by author+phrase); `{{admin}}` asserts the same phrase → two entries side by side.
- [ ] **J9** Concept doc missed phrase with two groundings — one in-vocab concept, one OOV label → **200** (both validated fresh vs the initial catalog).
- [ ] **J10** Unknown `chunk_key` → **400**.
- [ ] **J11** A grounding fresh-tree violation → **400**.
- [ ] **J12** Sentinel label as a missed-phrase grounding tag → expected **400**; if it goes through, that's a **FINDING** (record it — the sentinel gate may only sit on the derivation path).

## K. Truth + list — reading back what E–J wrote

- [ ] **K1** Truth `{{doc_kw}}`: effective relationship texts (E4's correction), F4's flipped `passed`, J1's human positive; rollup keyed by rule id with the agrees/disagrees/overridden accumulated above; identity summary + witness present.
- [ ] **K2** Truth with `identity_digest` pinned (from B6) → same doc; with a wrong digest → **404** naming the digest.
- [ ] **K3** Truth for a (mfg, field) with no doc → **404** "fetch the template first".
- [ ] **K4** Truth with bad field_type / bad URL → **400** each.
- [ ] **K5** Truth as `{{default_user}}` → **200** (X6 again, now with content).
- [ ] **K6** List unfiltered → 2 docs newest first; `unreviewed` counts dropped by exactly the phrases E–J touched; missed count = J1 (+J8/J9).
- [ ] **K7** List filters: `mfg_url` only; `field_type` only; both; a no-match filter → `{"total": 0, "documents": []}`.
- [ ] **K8** Concept truth: G8's reset precedence (old descent gone, direct link), I10's path rows with `replaced_from`; audits on superseded nodes still counted in the rollup (verdict 6a — deliberate).

## L. Lifecycle

- [ ] **L1** Sitting loop: alternate `next_unreviewed` → submit one item on the served phrase, until `unreviewed_remaining` hits 0; then `next_unreviewed=true` → **404** "nothing unreviewed remains"; list shows unreviewed 0.
- [ ] **L2** Template GET after all writes → `created=false`, audits intact, effective texts in the rows (get-or-create never clobbers).
- [ ] **L3** **The identity-keying proof.** Re-run extraction for (subject, field) from the notebook with ANY config/prompt/catalog change, then template GET → **new** document (`created=true`, different `identity_digest`); list shows both docs for the pair; truth default = the new one; digest pin still fetches the old annotated one.
- [ ] **L4** ⚠ Re-run with nothing changed: same digest → same doc (`created=false`) — only if the text version and every identity field truly held (a re-scrape mints a new text version, which keys apart by design; observe which happened).
- [ ] **L5** *(optional)* Concurrency demo: two tabs submit to the same doc in quick succession → last write wins on overlap (recorded v1 watch-item; Beanie `revision` is the hardening if this ever bites).
- [ ] **L6** `created_at` stable, `updated_at` monotonic across the session.

## M. Documented skips (exhaustiveness accounting)

Covered by unit tests, not exercised live, with reasons:

- **M1** Witness mismatch → 409 on detail/missed-phrase (needs tampering with the pinned S3 object; the deletability guard now protects it — don't).
- **M2** Missing-dep paths (`full_text`/`ontology_children` ValueErrors) — unreachable through the route wiring by construction. **Corrected 2026-08-18:** if ever reached they would NOT be 500s — the app-wide `ValueError` handler in `main.py` flattens any uncaught ValueError into a generic 400 `{"error": "Validation Error"}` (that's how the B10 InflationError surfaced). See WATCH-5.
- **M3** 16 MB document cap under accreting audits — recorded watch-item.
- **M4** Divergence hop from a non-concept `previous` — unreachable live (anchors are stored concepts).
- **M5** B14 catalog-drift 409 if S5 found no drift (then it stayed untested this run).

## WATCH — observations to carry into the review, not failures

- **WATCH-1** `source` on audit entries is an unconstrained string end-to-end; the app's `GroundTruthSource` enum exists but nothing binds it on this plane. Decide whether to bind or bless free-form.
- **WATCH-2** `at` is client-supplied and stored as sent; the model docstring's intent ("the submission time the service witnessed") is not enforced at the route. Decide whether the route should stamp it.
- **WATCH-3** G9: a typo'd concept reset persists as a fake OOV assertion; `tag_classification` in the response is the only tripwire (verdict 6d, echo-surfaces lesson). Confirm the echo is prominent enough in practice.
- **WATCH-4** E2: one audit on any surface marks the whole phrase reviewed for the cursor — fine for the sitting-per-phrase workflow, but means "reviewed" ≠ "fully reviewed". Confirm this reads right while actually annotating.
- **WATCH-5** The app-wide `@app.exception_handler(ValueError)` (main.py) converts every uncaught ValueError into 400 `{"error": "Validation Error"}` — it masked the B10 InflationError and would mask genuine route bugs as client errors. The GT plane now wraps its own errors into the 409 taxonomy, but the global handler's blast radius (all routes) is a standing decision: narrow it, or keep documenting around it.

---

## Results journal

| Date | Cases run | Verdicts / findings |
|---|---|---|
| 2026-08-18 | S, A, B1–B9 (implied), **B10 FAIL→FIXED** | **B10 FINDING:** template GET for `conformity_attestations` → 400 `{"error": "Validation Error", "detail": "…tag 'None of the above' has no rules…"}`. Root cause: initial grounding's escape hatch is stored faithfully as a sentinel key with zero rules in `llm_phrase_initial_grounding` stats (13–119 per concept field in run 20260819T012442); `inflate_tag_groundings` predates any sentinel fixture and read it as corruption. Two-part fix: (1) `OovGroundingGT.llm_declined` (option 3 — the declination is part of the surviving record) + `split_sentinel_tag_rules` at inflation; (2) `InflationError` wrapped into `TemplateAssemblyError` → route 409 (was escaping to the app-wide ValueError handler → generic 400, see WATCH-5/M2). B10 re-run passed. Follow-up same day: `PhraseTruth.llm_declined` passthrough — the folded `effective`/truth plane previously collapsed "LLM declined" and "stage never answered" into identical empty `tags`; a human tag asserted against a declination now keeps the flag (the contradiction is part of the record). Suite 1119 green, pyright 0. |
