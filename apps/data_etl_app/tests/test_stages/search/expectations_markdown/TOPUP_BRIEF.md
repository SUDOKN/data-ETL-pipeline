# Top-up brief — finding what the new text says and the old corpus never saw

Read `../expectations/SEEDING_BRIEF.md` first and in full: it is still the
procedure, and its §4 (the six field boundaries, quoted verbatim from the search
prompts) is still the only authority on what qualifies. Then read
`MARKDOWN_ADDENDUM.md`. This file covers only how a top-up differs from a seed.

## The difference from seeding

A seeding agent starts from nothing. **You start from an inventory that already
exists** — 8,219 entries were carried onto the new text mechanically, and the
entries that broke are being repaired by a different agent in parallel. Your job
is the third piece: **the entities the new text names that the old corpus never
recorded**, because the old innerText scrape could not see them or because the
site added them.

You will NOT be told what is already in the inventory, and you should not go
looking. Write down every in-field entity you find, exactly as a seeding agent
would. Duplicates are removed mechanically at merge time by entity name, so
re-finding something already known costs nothing. **Missing something is the only
real failure.**

## Where the new material is

Two places, and you must cover both.

**1. Pages the new crawl has and the old one did not.** Your top-up worklist
JSON lists them by line number and URL under `new_only_pages`. Go to those line
numbers and read those pages properly. This is the highest-yield part of the
task on subjects that gained pages.

**2. Newly-visible content on pages that already existed.** This applies to
EVERY subject, including those with no new pages at all, and it is the reason
the markdown cutover happened. The old rendering used the browser's `innerText`,
which returns only what was VISIBLE. The new rendering walks the DOM. So the
following are in the text now and were not before:

- **Closed accordions and inactive tab panels** — capability lists, FAQ answers,
  spec tables that sat behind a click. Often the densest in-field content on the
  page.
- **Pipe tables and HTML islands** — whole specification, grade and alloy tables.
  Every cell can be its own entity. `grep -n '^|' <file>` and
  `grep -n '<table' <file>` find them.
- **Image alt text**, now ordinary prose — frequently names a product or machine
  in a photo caption.
- **Heading structure** (`#`…`######`) — tells you whether a block is the
  company's own capability or sits under "Industries We Serve" / "Brands We
  Carry", which is how you get `actor` right.

## Method

Do both passes from SEEDING_BRIEF §6 — read, then grep — and remember the
measured lesson recorded there: **reading finds entities named in sentences,
grepping finds them in tables, nav bars and specification cells, and the two
passes catch different things.** A top-up that only greps the new pages will
miss the prose.

Targeted sweeps worth running on every subject: standards bodies (`ISO`, `ASTM`,
`ANSI`, `API`, `AMS`, `AS`, `NADCAP`, `UL`, `CE`, `EN`), brand names next to
machine words, alloy and grade patterns, and every `-ing` process word.

## WHOLE-WORD RULE — the single most common way to waste a row

The matcher (`_shared/text_matching.flexible_pattern`) wraps every form in word
boundaries on BOTH sides: `(?<!\w)Casting(?!\w)`. So a form must appear in your
quote **as a whole word**:

- `Casting` is NOT covered by the text's `Castings`. Add `Castings`.
- `cast` is NOT covered by `castings`.
- `braze` is NOT covered by `brazing`. Add the inflection the text actually uses.

Measured live on 2026-08-29: an agent re-anchored an agstech entry onto the
heading `# Metal and Metal Alloy Castings` while keeping the forms
`["Casting", "die castings", "cast"]`. None of the three matches, so the row was
rejected and the entity was lost. **Whenever you write or keep a quote, check
that one of your forms occurs in it as a whole word, and if it does not, add the
exact inflection the text uses to `acceptable_forms`.**

### A quote must also START on a word boundary

The same `(?<!\w)` applies to the QUOTE itself. A quote that begins mid-word
never matches, however faithfully it was copied. Measured live on 2026-08-29: an
agent copied `ated & shown keen interest in Turmeric rhizome drying.` out of the
word *appreciated*. It is a true substring of the file and it still failed, so
the row was rejected. **Start every quote at the beginning of a word — and END it at the end of one.**

`flexible_pattern` bounds BOTH edges, so a quote cut mid-word at either end
never matches. The commonest way this happens is truncating to the 200-character
cap with a fixed-width slice: measured 2026-08-29, one top-up agent lost 15
entries that way, every quote exactly 200 characters and every one ending in
half a word. Trim back to the last whole word instead.

## Output

One JSON object per line, no prose, no fences, to the path you were given.
The row shapes are exactly SEEDING_BRIEF §5 and §7:

```json
{"field": "process_caps", "name": "vacuum brazing",
 "acceptable_forms": ["vacuum brazing", "vacuum-brazed"],
 "evidence": [{"quote": "our vacuum brazing furnaces run continuously"}],
 "actor": "own", "notes": ""}

{"type": "false_friend", "field": "products", "form": "Aerospace",
 "reason": "an industry, not an artifact"}
```

**Do not emit a `subject` row and do not emit `field_meta` rows.** The subject
record, `expected_empty` and the field notes were all carried over from the
verified legacy corpus, and the merge preserves them. Emitting those rows would
overwrite prior judgment with a fresh guess.

Every quote must be copied CHARACTER FOR CHARACTER from the file, at most 200
characters, never crossing a line break, and must contain at least one of the
entry's `acceptable_forms`. Before seeding any FOUR-character form, grep the
whole text for it as a substring — `Iron` hides inside "environment", `Hone`
inside "Phone", `NACE` inside "furnace".

Recall-first still holds: a certification held by a supplier, a machine owned by
a client, a process performed by a partner plant are all entries. Record whose
it is in `actor` (`own`, `client`, `supplier`, `lab`, `parent_sibling`,
`reseller_inventory`, `unknown`); never exclude an entity over attribution.
