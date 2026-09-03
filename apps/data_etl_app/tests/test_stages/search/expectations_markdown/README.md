# expectations_markdown — the search eval set for the markdown_v2 corpus

This is the live expectation set. It is pinned to
`apps/data_etl_app/tests/test_stages/sample_scraped_markdowns/` — the 18
corpus subjects as the scraper renders them since the innerText→Markdown
cutover of 2026-08-28/29, crawled 2026-08-29.

The sibling `../expectations/` holds the LEGACY set, pinned to
`sample_scraped_texts/`. It is kept, not archived: every number already in
`../history/metrics.jsonl` was scored against it, and deleting it would make
those runs uninterpretable rather than merely superseded. Nothing should be
compared across the two — they have different ground truth because they
describe different text.

Select a corpus with the `SEARCH_EVAL_CORPUS` environment variable, read in
`../checks/paths.py`. `markdown` is the default; `legacy` reaches the old set:

```bash
.venv/bin/python checks/validate_expectations.py                  # markdown
SEARCH_EVAL_CORPUS=legacy .venv/bin/python checks/validate_expectations.py
```

## Why this set was PORTED rather than re-seeded

The obvious move after a scraper change is to re-seed the corpus from scratch.
Measured before deciding, that would have been the worse option.

Of the 9,075 live entries in the legacy set, **8,281 (91.3%) still had an
evidence quote occurring verbatim in the new text.** Re-deriving them would
have spent a large agent fan-out reproducing work already done — and
re-derivation is lossy: `../expectations/VERIFICATION_LEDGER.md` records that
the verification pass on agstech alone recovered **512 entities** its seeding
agents had dropped, and `SEEDING_BRIEF.md` §6b records that slice agents cover
catalogue structure well and running prose badly. A from-scratch reseed would
most likely have produced a corpus *worse* than the one already two-pass
verified.

So the cutover ran in three passes, and agents were spent only where the text
actually changed:

1. **Mechanical port** (`../checks/port_corpus.py`, no agents) — carry every
   entry that still satisfies the two contracts `validate_expectations.py`
   errors on: a quote occurring verbatim in the new text, and an
   `acceptable_form` covered by that quote. Re-pin `snapshot`, recompute
   offsets, append a `ported` provenance row.
2. **Repair** (`REPAIR_BRIEF.md`, one Sonnet agent per affected subject) — the
   entries the port could not carry were NOT retired by the script. A string
   that stopped matching is not proof that the site stopped saying the thing,
   so each one went to an agent to re-anchor, retire with a reason, or dispute.
3. **Top-up** (`TOPUP_BRIEF.md`, one Sonnet agent per subject) — everything the
   new text names that the old corpus never recorded: the pages the new crawl
   gained, and the content markdown newly exposes on pages that already existed
   (closed accordions, inactive tab panels, pipe-table and HTML-island cells,
   image alt text).

## The crawl is a different SAMPLE, not a re-rendering

This is the fact that shaped everything above, and it is worth not
rediscovering. Three subjects changed because the SITES changed, not because
the scraper did:

- **lucasmilhaupt.com** — restructured. Its whole `/EN/Industries/*` and
  `/EN/Products/*` tree (the HANDY-FLO / AL-xxx braze alloy catalogue) is absent
  from this crawl; 69 pages became 42, and only 7% of its entries ported.
- **blackadvtech.com** — dropped `/capabilities/*` and `/about-us/*` in favour
  of SEO blog articles; 80 pages became 57.
- **anchor-mfg.com** — moved to a new CMS with `?lang=en` URLs and rewrote its
  prose. Page count went UP (15→17) while only 28% of entries ported.

And two of the original 20 subjects are gone entirely:
**austinelectricservices.com** and **sterlingmfg.net** are dead domains, not in
the new crawl. Their legacy expectations remain under `../expectations/`.

## Files here

| file | what it is |
|---|---|
| `MARKDOWN_ADDENDUM.md` | what changed about the TEXT; read alongside the seeding brief |
| `REPAIR_BRIEF.md` | the re-anchor task handed to every repair agent |
| `TOPUP_BRIEF.md` | the additive-seeding task handed to every top-up agent |
| `PORT_REPORT.md` | what the cutover actually did, per subject, with counts |
| `<subject_slug>/` | `subject.yaml` plus one YAML per field, schema in `../EXPECTATIONS_SCHEMA.md` |

The procedure itself still lives next door and is unchanged:
`../expectations/SEEDING_BRIEF.md` (§4 quotes the six field boundaries verbatim
from the search prompts and remains the only authority on what qualifies),
`../expectations/VERIFY_BRIEF.md`, `../expectations/VERIFICATION_LEDGER.md`.

## Two matcher rules that cost real entries during the cutover

Both were found by watching agents lose rows, and both are now in the briefs
and pinned by `../tests/test_corpus_cutover.py`.

**A form must match its quote as a WHOLE WORD.** `flexible_pattern` bounds both
edges (`(?<!\w)Casting(?!\w)`), so `Casting` is not covered by the text's
`Castings`, and `braze` is not covered by `brazing`. An agent re-anchored an
agstech entry onto `# Metal and Metal Alloy Castings` while keeping the forms
`["Casting", "die castings", "cast"]`; none matched and the entity was lost.

**A quote must START on a word boundary.** An agent copied
`ated & shown keen interest in Turmeric rhizome drying.` out of the middle of
the word *appreciated*. It is a true substring of the file and it still failed.
