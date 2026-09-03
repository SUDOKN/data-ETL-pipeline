# Judge brief — reading one slice of a mention-stage work order

Handed verbatim to every judging agent, with a slice assignment appended. Its
sibling `VERIFY_BRIEF.md` governs the golden corpus; this one governs verdicts
about a run.

Read `../TAXONOMY.md` first. Its dimension tables are the contract; this brief
is how to apply them without producing confident nonsense.

---

## 1. What you are judging, and what you are NOT

You judge **how well this stage handled a passage it was given**. You never
judge whether the passage should have been given to it.

A document title, a client's product, a polyseme, an obvious junk form — all in
scope for *how well the stage handled them*, all out of scope for *whether they
belong*. Whether search should have sent a form is the search eval's question
and coding it here imports another eval's failures into this one.

## 2. The one rule everything else serves: judge by reading

Open the pinned snapshot at
`apps/data_etl_app/tests/test_stages/sample_scraped_texts/<domain>.txt`, find
the passage, and read what is actually around it.

Pattern-matching over this pipeline's model prose has mismeasured in both
directions repeatedly — 10x under, 69x over, and once a passive-to-active voice
change faked a jump from 45% to 64%. Your own scan of a location string is a
hypothesis, not a verdict.

**Quote what convicts.** A `fail` without a quote from the snippet or the
location text is dropped at verification and your slice is re-run.

**`unclear` is an honest answer.** It is counted and reported separately, never
folded into a pass. Use it instead of guessing.

## 3. A location is a CLAIM, not a sentence to be graded

There is no correct wording. Two descriptions sharing no words can both be
right. You are checking whether the assertions are TRUE of this passage:

- **S2 kind of text** — go to the span and look at what it structurally is.
- **S3 what it belongs to** — the prompt requires the section heading *quoted
  exactly as the text writes it*, so this is close to mechanical: the quoted
  string either sits above that passage in the document or it does not.
  Invented, or so generic it identifies nothing, is a fail.
- **S4 whose words** — check against the subject's `third_party_roster`.
  **`in the site's own copy` fills ~99.5% of the attribution slot, so the
  question is never whether the slot is filled but whether that phrase is TRUE
  here.** On a customer testimonial, a parent-company block, a standards body's
  text or a vendor's brochure copy, it is a fail.
- **S1 extent** — did the clip cut the sense? A subject-less fragment, a
  truncated list, a heading severed from the row it labels.
- **S5 self-containment** — leaning on another mention, restating the passage
  instead of placing it, or carrying a URL.

### `spans_multiple_pages` — read this before failing an S3

An item annotated `spans_multiple_pages` covers several occurrences, and the
prompt explicitly asks for one description covering all of them:

> "A passage may occur at more than one place in the text ... Describe it once,
> covering where it recurs."

So this is CORRECT, not a fail:

> Sentence of prose under the main content area on **both the Boeing and Airbus
> fixed wing replacement parts pages**, in the site's own copy.

Judge such a claim against the whole `pages` list on the item, never against
one page. Measured on ableengineering: one snippet, 13 occurrences, 8 pages, 6
descriptions — every one of them correct for its own window. Failing these was
the single largest false-failure risk in this instrument's design.

## 4. Groups

Each group item carries its own `evidence` — the distinct passages behind it.
Read them before coding G1-G4. If `evidence_truncated` is non-zero, more
passages exist than were inlined; say so in your note rather than assuming the
inlined set is complete.

- **G1 merge soundness** — are two forms in this group actually the same thing?
  Zero wrong merges have been found to date; the `forms` list exists to make
  one visible.
- **G2 split soundness** — under-merge is the accepted steady state. **Record
  it, do not treat it as a defect.**
- **G3 focal form** — does `group_key` name what the group is about?
- **G4 status** — is `no_mentions` / `collapsed` right for this evidence?

## 5. Inheritance (I1)

An occurrence whose matched span sits inside a longer collected one.

- `sound` — the longer span really is evidence about the shorter form:
  "our steel **doors and frames** set the standard" IS evidence about `door`.
- `diluting` — the span is about a specific different thing and reads as
  generic evidence: "**Paladin(TM) PW Series flush doors and frames**" credited
  to bare `door`.
- `unclear`.

This population did not exist before 2026-08-27 and nothing is known about its
rate. Do not anchor on an expectation.

## 6. Output contract

Append JSONL — **one row per item in your slice, no exceptions**. A skipped
item stays pending forever and silently becomes a sampling decision nobody
made.

Write to
`history/runs/<run_id>/verdicts/<subject_slug>__<field>__part<N>.jsonl`.

```json
{"content_key": "<echoed EXACTLY from the item>",
 "subject": "alecmfg.com", "field": "conformity_attestations",
 "item": "snippet", "mention_id": "mwbp0rxz", "group_id": null,
 "taxonomy_version": "<echoed>", "pv": "<echoed>",
 "judge": "agent:mention-judge-<your slice>",
 "annotations": ["repeated_line"],
 "checks": {"S1": {"verdict": "pass"},
            "S4": {"verdict": "fail", "severity": "major",
                   "quote": "in the site's own copy",
                   "note": "the passage is a customer testimonial by Jean Dupont"}},
 "verified": false}
```

`content_key`, `taxonomy_version` and `pv` are the cache key — echo them
unchanged or the verdict is unreachable. Every dimension listed in the item's
`dimensions` needs an entry in `checks`. `severity` (`minor` | `major`) only on
a fail.

**Build the file with shell appends in batches of ~25**, so an agent that dies
mid-slice still leaves valid JSONL rather than one truncated write.

## 7. What gets checked afterwards

The lead session re-reads every S4 fail, every `major`, every I1 `diluting`,
and five random passes per agent. Four numbers were corrected by exactly this
step in the last census. Write notes you would be willing to defend.
