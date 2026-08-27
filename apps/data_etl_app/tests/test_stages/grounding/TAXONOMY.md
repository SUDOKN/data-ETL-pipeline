# Grounding evaluation taxonomy — version 1

Every judged item in this instrument is coded with the labels below. Counts from two
runs are comparable **only when their taxonomy versions match**; the ledger records the
version next to every number. Version 1 is the taxonomy of the 2026-08-25 full hand
census of run `20260825T194457` (`pipeline_v3_evidence/2026-08-25_run_194457_full_end_to_end/`),
carried over unchanged so that census remains the baseline row of the ledger.

Changing anything in this file — adding a code, moving a boundary, renaming — requires
bumping the version at the top and recording the change in the log at the bottom.

## What gets judged

One judgment per **item**, where an item is one of:

1. **A tag instance** — one tag emitted by one grounding stage (freehand, initial, or
   OOV) for one record in one chunk. In-vocab and OOV tags on the same record are
   separate items. A group that appears in both chunks is judged in both (twins
   diverge on 51% of runs measured; collapsing them hides defects).
2. **A decline** — one refusal by one grounding stage for one record in one chunk.
3. **A descent hop** — one parent→child step in `lvl_by_lvl_itps` (the recursive
   descent tree), or one terminal stop.

## How to judge (binding rules, each one a scar)

- **Judge by reading the record**: the focal form, the member forms, the synthesis,
  and — when attribution or existence is in doubt — the scraped source text itself.
- **Never let the rule explanation decide the code.** Explanations are post-hoc
  justifications, not decision traces: they hallucinate vocabulary contents, argue
  against their own verdicts, and re-phrase their boilerplate between runs. Three
  regexes over explanation text failed in both directions during the census (10×
  under, 69× over). Quote explanations as color; code from the evidence.
- **Regex and code enumerate; they never judge.** Mechanical passes build the item
  list and structural counts. Every code below is assigned by reading.
- **Ask for the control before believing a cause**, and **check the consequence
  before asserting one** (does the same tag pass elsewhere? did the defect actually
  ship, or did screening catch it?).
- **When the location text is part of the evidence, count it** — a party name that
  appears only in the location line is still evidence of attribution.

## Tag-instance codes

| code | name | definition |
|---|---|---|
| **D** | direct | The tag names the entity as the synthesis states it (or an exact vocabulary match of it). Clean. |
| **N** | normalized | The tag is the vocabulary's normalized or parent form of something the synthesis names specifically (`A60 galvannealled steel` → `Steel`; `hotels` → `Commercial Construction`). Clean. |
| **X** | wrong axis | Real, but the wrong field: the subject's own activity tagged as an industry served, a process as a product, equipment in a product field. For `industries`, report the two sub-kinds separately: **X-own** (own activity as sector served) and **X-sector** (a real sector, but not the one evidenced). |
| **V** | vague | Real but so generic or non-categorical it carries no information (`Metal`, `Polymer`, `Chemicals`, `Honeycomb`, `Thermal Systems`). |
| **B** | bridge | The tag needs an inference step the synthesis never states — including grounding on **negated or comparative** evidence ("designed to avoid X" → tagged X). |
| **P** | wrong party | Real, but another actor's: certification lab, supplier, customer, installer, distributor, parent company, or the client's own customers. |
| **F** | fabricated | The entity is absent from the record entirely — not in the focal form, member forms, or synthesis. The headline safety count. |

**Clean share** = (D + N) / all tag instances. D and N are the only clean codes.

Boundary calls, as settled in the census:

- A demonstrated process step in a delivered project is **D**, not V — worked
  evidence beats a marketing list.
- A correct hedge ("is expected to provide", from a client-requirements table) is
  clean; the register of the sentence is not the defect, the claim is.
- A correct negative dealing ("does not offer factory finish paint") is clean.
- `X` vs `P`: X is a category error within the subject's own facts; P is a fact that
  belongs to a different actor.
- `B` vs `F`: B's entity is reachable from the record by one unstated inference;
  F's entity is not in the record at all. `<door type> manufacturing machine`
  inferred from doors sold is **F** (the machine is never named), not B.

## Decline codes

| code | name | definition |
|---|---|---|
| **DS** | sound | The refusal is correct: the entity is not evidenced, is out of field, is an idiom, a file format, a document title, or is already covered. |
| **DF** | false decline | The refusal kills a real, evidenced entity (the "standard, not a certification" template killing `ANSI UL 10C` compliance; a homonym guard killing a true use). |
| **DR** | self-refuting | The text of the decline asserts the grounds for tagging and then declines anyway. Code by reading the whole decline, never by a reversal-marker regex (measured 69× over-count). |

A decline that is faithful to an under-claiming synthesis (ISO 9001 "listed in
certification statistics") is **DS** — the defect belongs to the upstream stage and
should be recorded in `notes`, not blamed on grounding.

## Descent hop codes

| code | name | definition |
|---|---|---|
| **H-OK** | sound hop | The child concept's discriminating feature is evidenced in the record (`SLS` → Selective Laser Sintering; `A60 galvannealed` → Galvanized Steel). |
| **H-BRIDGE** | world-knowledge hop | The hop's justification lives in world knowledge, not the record ("Vinyl is a well-known type of plastic"). The M2 leak. |
| **H-FORCED** | forced distinction | The vocabulary forces a distinction the text never makes and the model picks a branch anyway (`Painting` → `Wet Painting` inserting "liquid"; `Anodizing` → `Decorative Anodizing` inserting "aesthetic"). |
| **H-STOP** | wrong stop | Stopped at the parent though the child was evidenced, or descended past the last evidenced level. |

**Descent defect share** = (H-BRIDGE + H-FORCED + H-STOP) / hops.

## Context annotations (recorded, never judged here)

Each tag-instance judgment carries the screening outcome for that tag (`passed`,
`failed`, or `absent`) and the row's final `status`. They grade **severity** — a
defective tag that shipped is worse than one screening caught — but screening's own
soundness is out of this instrument's scope.

## Judgment record format

One JSON object per line (JSONL), one file per (subject, field) dump, written to
`history/runs/<run_id>/judgments/<subject>__<field>.jsonl`:

```json
{"run_id": "20260825T194457", "subject": "steelcraft_com", "field": "equipments",
 "chunk": "0:91562", "group_id": "gab12cde", "focal_form": "...",
 "item": "tag", "stage": "freehand", "tag": "flush door manufacturing machine",
 "code": "F", "sub_kind": null,
 "justification": "one sentence: what the record shows and why the code follows",
 "evidence": "short verbatim quote from synthesis or source text",
 "screening": "passed", "row_status": "grounded",
 "watch_items": ["GW-EQ-1"],
 "agent": "judge-3", "taxonomy_version": 1}
```

`item` ∈ `tag` | `decline` | `descent_hop`. For declines: `stage` and `code` (DS/DF/DR),
no `tag`. For hops: `parent`, `child` (or `stop`), `level`, code H-*. `watch_items`
lists ids from the eval set the item bears on, when any. `sub_kind` carries X-own /
X-sector for industries.

## Change log

- **v1 (2026-08-26).** Codes lifted unchanged from the 2026-08-25 census conventions
  (`census_material_industries.md` §"Coding conventions", generalized to all fields);
  decline and descent codes formalized from the same census's practice.
