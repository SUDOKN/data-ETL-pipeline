# Repair brief — re-anchoring an entry the port could not carry over

Read `../expectations/SEEDING_BRIEF.md` for the field boundaries (§4) and the
entry contract (§5), and `MARKDOWN_ADDENDUM.md` for what changed about the text.
This file covers only the repair task.

## What happened, and what you are being asked

The eval set was built against the LEGACY innerText scrape. The scraper switched
to Markdown and the sites were re-crawled on 2026-08-29. `port_corpus.py`
carried over every entry whose evidence still occurs verbatim in the new text —
8,219 of them. Your worklist holds the ones it could not carry, for one of two
mechanical reasons, recorded per row in `reason`:

- `no evidence quote survives` — none of the entry's quotes occur in the new
  text.
- `surviving quotes cover no acceptable_form` — a quote survived, but no longer
  contains any of the forms the entry is credited by, so the entry could never
  score.

**These entries were deliberately NOT retired.** A string that stopped matching
is not proof that the site stopped saying the thing. The prose may have been
rewritten around the same entity, the page may have moved, or the entity may
genuinely be gone. Deciding which is a reading task, which is why it is yours
and not the script's.

## Your job, per row

Search the new text for the entity — **the thing, not the old quote**. Use the
`name` and every `acceptable_forms` string as search terms, and try the obvious
inflections and spellings the new prose might use instead.

Then return exactly one verdict:

**`reanchor`** — the entity is still named in the new text. Supply new
`evidence` (≥1 verbatim quote, ≤200 chars, containing an acceptable form) and,
if the new text uses different surface forms, an updated `acceptable_forms`.
Keep the entry's `id` and `name`. This is the common case and the one to try
hardest for.

**`retire`** — the entity is genuinely absent from the new snapshot. Say in
`reason` what you searched for and what you found instead (e.g. "the whole
/EN/Products tree is gone; no HANDY-FLO string anywhere in the file"). A retired
entry stays in the file as a record and never gates recall.

**`dispute`** — the entity is present but you believe it does not qualify under
its field's `What qualifies as a phrase` clause. Supply evidence anyway plus the
argument. Never delete the argument by retiring instead.

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

## The one way to get this badly wrong

**Do not re-anchor onto a `Navigation:` line, a footer, or a cookie banner just
to make the entry match again.** Those lines repeat site-wide and contain
in-field words lifted from other pages. An entry anchored there is technically
valid and practically worthless — it will match forever regardless of what the
stage does. If the only occurrence you can find is boilerplate, prefer `retire`
and say so, or re-anchor and flag it in `notes`.

Equally: do not widen `acceptable_forms` to something generic (`steel`,
`machining`) to force a match. That makes the entry un-missable and drains its
recall number of meaning — the defect `validate_expectations.py` check 5 exists
to catch.

## Output

One JSON object per line, to the path you were given. No prose, no fences.

```json
{"id": "lucasmilhaupt_com-products-0031", "field": "products", "verdict": "reanchor",
 "acceptable_forms": ["HANDY FLO 100", "Handy Flo 100"],
 "evidence": [{"quote": "HANDY FLO 100 is a low-temperature silver brazing alloy"}],
 "notes": ""}

{"id": "lucasmilhaupt_com-products-0044", "field": "products", "verdict": "retire",
 "reason": "searched AL-822, AL 822, 822 alloy; the /EN/Products tree is absent from the 2026-08-29 crawl"}
```

Every `id` in your worklist must appear exactly once in your output.
