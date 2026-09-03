# Markdown addendum — what changed about the TEXT, and what it does to seeding

Read this **after** `../expectations/SEEDING_BRIEF.md` and (for a verification
pass) `../expectations/VERIFY_BRIEF.md`. Those two are still the procedure and
still authoritative on *what qualifies* for each field. Nothing here changes a
field boundary. This file covers only the ways the **scraped text itself** now
looks different, because every one of them can make a careful agent write a
quote that fails the mechanical check, or miss entities the old rendering never
showed.

The corpus under this directory is pinned to
`apps/data_etl_app/tests/test_stages/sample_scraped_markdowns/`, produced by the
scraper's `markdown_v2` rendering (crawl of 2026-08-29). The sibling
`../expectations/` set is pinned to the legacy innerText rendering and is kept
for the historical runs that were scored against it. Do not mix them.

---

## 1. The envelope is unchanged; the body is Markdown

Page blocks still look like this, so `grep -n '^https'` still finds page
boundaries and slicing still works:

```
##################################################
<url>

<body, now Markdown>
```

Inside the body: headings are `#` … `######`, list items are `-` or `1.` with
indentation for nesting, tables are pipe tables, image alt text is now ordinary
text, and content that is in the DOM but was never rendered — closed
accordions, inactive tab panels — is now **present**.

## 2. Quote hazards, all of them measured on this corpus

**Copy quotes character for character, including Markdown punctuation.** A
heading's `### ` prefix, a list item's `- `, a table row's `|` and a bold span's
`**` are all part of the line. The matcher treats whitespace runs as elastic and
folds typographic hyphens, but it does not invent or remove punctuation. If your
quote starts mid-line you may start after the marker — just do not retype the
line without it and call it verbatim.

**Never let a quote cross a line break.** Unchanged from the base brief, and
more dangerous here: Markdown puts a blank line between blocks, so a sentence
that *looks* continuous on screen is often two lines.

**The 200-character cap now collides with real lines.** Measured longest line
per subject: agstech 17,501 chars, pradeepmetals 2,709, acimachine 2,411,
howcogroup 2,124, steelcraft 1,831, anchor-mfg 1,431. Quote a **window around
your form**, not the line.

**Pipe-table rows are single lines, and they pack entities.** From
pradeepmetals:

```
| STAINLESS STEEL | Alloy Steel | Duplex | Carbon Steel | Die Steel |
| F303 F304/F304L F316 Ti F316/F316L F321 F347H1.4301 1.4307 1.4435 ... | SAE – 4130 8620H 414020MnCr5 ...
```

Every cell is a separate entity, and the second row shows the trap: grade
designations run together **with no separator you can rely on** (`F347H1.4301`
is `F347H` followed by `1.4301`). Seed each grade as its own entry, quote a
window that contains it, and do not assume whitespace marks the boundary.

**HTML islands survive as raw HTML.** Where a cell spans rows or columns the
converter emits sanitized HTML instead of a pipe table. Present in alecmfg,
acimachine, tanfel, superiortech, lucasmilhaupt, pradeepmetals, steelcraft. A
quote taken from one contains angle brackets and attributes — still fine as a
verbatim quote, but prefer a shorter window inside the cell's text.

## 3. `Navigation:` lines — a new boilerplate surface that reads as content

`markdown_v2` collapses each page's nav into one line beginning `Navigation:`.
Those lines carry **in-field words lifted from other pages' titles**:

```
Navigation: Prev Previous 🇺🇸 U.S. Oil & Gas Client | Integrated Welding and CNC Machining of Structural Frames for Offshore Skid Systems · Next ...
```

That names an industry, two processes and a product — all real, all belonging to
a *different page*. The rule: **seed an entity from the page that is about it,
not from a `Navigation:` line.** A nav line is a link label, and it repeats on
every page in its section, so seeding from it inflates nothing in recall but
does put your evidence on a boilerplate line that says nothing about where the
entity actually lives. If a `Navigation:` line is the **only** place an entity
occurs anywhere in the file, it is still an entry — the text does name it — but
say so in `notes`.

## 4. What is genuinely NEW to read

These are the surfaces the legacy innerText rendering never contained. They are
the main reason a top-up pass exists at all:

- **Closed accordions and inactive tab panels.** Capability lists, FAQ answers
  and spec tables that were hidden behind a click are now in the text.
- **Pipe-table and HTML-island cells.** Whole specification and grade tables
  that innerText flattened into an unreadable label/value run, or dropped.
- **Image alt text**, now ordinary prose. Often names a product or machine in a
  photo caption. Also a **fabrication risk to watch as a verifier**: alt text is
  frequently marketing filler, so judge it by the same field clause as any prose.
- **Heading structure.** `##`/`###` levels tell you which entities are the
  page's own capabilities and which sit under a "Industries We Serve" or
  "Brands We Carry" heading — useful for getting `actor` right.

## 5. What DISAPPEARED, and why that is not your problem to fix

The 2026-08-29 crawl is a different sample, not the same pages re-rendered:
lucasmilhaupt lost its whole `/EN/Industries/*` and `/EN/Products/*` tree,
blackadvtech dropped `/capabilities/*` and `/about-us/*` for blog articles,
anchor-mfg moved to a new CMS with `?lang=en` URLs and rewritten prose.
austinelectricservices.com and sterlingmfg.net are dead domains and are not in
this corpus at all.

**Seed only what the file in front of you contains.** If you know from the
legacy corpus that a site used to name something, that is not evidence. An
inventory is a record of what is written in the snapshot it is pinned to.
