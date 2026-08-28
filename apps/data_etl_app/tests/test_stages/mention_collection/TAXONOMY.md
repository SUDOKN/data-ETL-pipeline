# Mention-stage evaluation taxonomy — version 1

The judgment contract. **This file's sha256 is `taxonomy_version`**, stamped into
every verdict; editing any byte invalidates the cached verdicts of every field,
which is the intent. Batch changes between runs, never mid-pass, and say so in
the report.

## What gets judged

Three item kinds. Each has exactly one owner and one counting rule.

| item | one row per | population on a 2-subject run (pre-D8 measure) |
|---|---|---|
| **snippet** | distinct snippet in a window (the thing the model was asked to locate) | 3,345 |
| **group** | fold group (its forms, its status) | 2,248 |
| **inheritance** | occurrence whose matched span is longer than the form itself | new; unmeasured |

Occurrences are **not** judged individually — they are mechanically verified
against golden labels. `contract_products` is never judged: it is a byte-copy of
`products` through this stage (12 units per 2-subject run, never 14).

## How to judge (binding rules, each one a scar)

1. **Judge by reading.** Regex over this pipeline's model prose has mismeasured
   in both directions repeatedly — 10× under, 69× over, and once a passive-to-
   active voice change faked a 45→64% jump. The harness's string scans only
   NOMINATE items; they are never verdicts.
2. **Quote what convicts.** A verdict without a quote from the snippet or the
   location text is not reviewable and will be dropped at verification.
3. **The location is evidence, not decoration.** Grounding and synthesis both
   instruct their judges to read it ("a heading like 'More from Allegion' is
   provenance"). Judge it as the reader downstream will use it.
4. **Never judge whether the form should have been searched for.** A document
   title, a client's product, a polyseme — all are in scope for *how well the
   stage handled them* and out of scope for *whether they belong*. Code the
   handling; the search eval owns the admission.
5. **`unclear` is honest.** Use it rather than guessing; it is counted and
   reported, never silently folded into pass.
6. **Compare against the text, not your expectation.** The pipeline reads a
   page-trimmed copy; check the snippet against the pinned snapshot before
   calling a clip wrong.

## Snippet dimensions (S-codes)

Each snippet gets a verdict on each dimension: `pass` | `fail` | `unclear`,
with `severity: minor | major` on a fail.

| id | dimension | fails when |
|---|---|---|
| **S1** | **extent** | the clip cuts the sense — a subject-less fragment, a truncated list, a heading severed from the row it labels. (Synthesis candidate C1 blames this stage's clipping radius; this is that measurement.) |
| **S2** | **location: kind of text** | the described kind is wrong — body prose called a heading, a table cell called a bullet, a nav label called a sentence |
| **S3** | **location: what it belongs to** | the named page or section is wrong, or invented, or so generic it identifies nothing |
| **S4** | **location: whose words** | the attribution is wrong — a customer testimonial, a parent-company block, a standards body's text or a vendor's brochure copy described as the site's own. **The boilerplate `in the site's own copy` (99.5% of the slot) is a FAIL here whenever the passage is not the site's own words** |
| **S5** | **self-containment** | the location leans on another mention, restates the snippet instead of placing it, or carries a URL (all three are banned by the prompt) |

**Counting rules.** `S<n> pass rate = passes / (passes + fails)`; `unclear` is
reported separately and excluded from both. **Location correctness =
S2 ∧ S3 ∧ S4 all pass.** This is the headline number and has never been measured.

## Group dimensions (G-codes)

| id | dimension | fails when |
|---|---|---|
| **G1** | **merge soundness** | two forms in one group are not the same thing (a wrong merge — 0 found to date; the `forms` list exists to make this visible) |
| **G2** | **split soundness** | two groups are plainly the same thing and should have merged (under-merge is the accepted steady state, so record it, do not treat it as a defect) |
| **G3** | **focal form** | the chosen focal form does not name what the group is about |
| **G4** | **status** | `no_mentions` / `collapsed` is wrong for this group's evidence |

## Inheritance dimension (I-code)

| id | dimension | verdicts |
|---|---|---|
| **I1** | **inherited evidence** | `sound` — the longer span really is evidence about the shorter form ("our steel **doors and frames** set the standard" is evidence about `door`); `diluting` — the span is about a specific different thing and reads as generic evidence ("**Paladin™ PW Series flush doors and frames**" credited to bare `door`); `unclear` |

**Counting rule.** `dilution rate = diluting / (sound + diluting)`, reported per
field and per group, plus the count of groups whose entries exceed the
synthesis packing limit of 50 (`groups_over_50_entries`) — that is where
dilution turns into a downstream defect.

## Context annotations (recorded, never judged here)

`repeated_line` (the same snippet occurring many times — menus, footers,
country dropdowns), `legal_page`, `polyseme`, `document_title`,
`third_party_named`. These describe the item so rates can be sliced; they are
not verdicts and never enter a pass rate.

## Judgment record format

One JSON object per line, written to
`history/runs/<run_id>/verdicts/<subject_slug>__<field>[__partN].jsonl`:

```json
{"content_key": "…", "subject": "steelcraft.com", "field": "process_caps",
 "item": "snippet", "mention_id": "m4erhr2t", "group_id": null,
 "taxonomy_version": "…", "pv": "…", "evidence_sha256": "…",
 "first_judged_run": "20260828T…", "judge": "agent:mention-judge",
 "annotations": ["repeated_line"],
 "checks": {"S1": {"verdict": "pass"},
            "S4": {"verdict": "fail", "severity": "major",
                   "quote": "in the site's own copy",
                   "note": "the passage is a customer testimonial"}},
 "verified": false}
```

Echo `content_key`, `taxonomy_version`, `pv` and `evidence_sha256` unchanged —
they are the cache key. Every item in the assigned slice gets a row; a skipped
item stays pending forever.

## Change log

- **v1, 2026-08-27** — established. S-codes from the location contract the
  mention statics actually ask for (kind / belongs-to / whose-words, plus the
  standing bans); G-codes from the fold's own visible surfaces; I1 new, created
  by the D8 reversal the same day.
