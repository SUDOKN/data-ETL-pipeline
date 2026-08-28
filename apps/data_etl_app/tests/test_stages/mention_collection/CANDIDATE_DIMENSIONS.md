# Candidate dimensions — the staging area

Defect classes found but **not yet promoted into `TAXONOMY.md`**. Promotion
changes `taxonomy_version` and invalidates every cached verdict, so candidates
accumulate here and are promoted in batches between runs.

Each carries a probe: a named case, on a named subject, with a verbatim quote,
so the class can be tested before it is trusted.

Everything below was found on **2026-08-27 while seeding the golden corpus** —
by reading the pinned texts, before any run of the current pipeline existed.
That is the point: these are predictions the first post-`bbfc42b` run will
confirm or refute, not post-hoc rationalizations of numbers already seen.

---

## C1 — Atomized table cells (extent; feeds S1)

**Probe AL-P1 · alecmfg.com.** Every case-study table is flattened to **one cell
per line**, separated by blank lines *and bare-tab lines*. The cell
`Zeiss CONTURA G2 CMM` sits 17 lines below its column header `Equipment Used`
and 12 below its row header `Hole Position Tolerance`.

**No line-level or sentence-level clip can recover either header.** The snippet
that reaches synthesis is a bare noun phrase with no indication that it names
equipment used on a delivered job. This is the strongest concrete case yet for
`snippet_radius > 0`, a dial that has never been exercised above 0.

Counter-probe **AUS-P1 · austinelectricservices.com**: the opposite failure —
24 job-table rows *fuse* four cells into one line
(`Rough Electrician\tPhoenix Area\tField\tApply Now`). One dial cannot fix both;
that is why this is a dimension and not a bug.

## C2 — Bare-token capability lists (extent + location; S1, S3)

**Probe AN-P1 · anchor-mfg.com.** The whole of `/capabilities-services/` is bare
lines with no bullets, indentation, or punctuation, and section headings are
**typographically identical to their items**. 56% of non-blank lines are three
words or fewer with no terminal period.

**Probe 101-P1 · 101machine.com.** ~25 of ~40 forms occur *only* as a one-word
line in a Services block (`Counter Boring`); the only thing carrying a claim is
a heading up to fifteen lines above.

The clip is correct and the evidence is still empty: `belongs_to` is
unrecoverable from the snippet alone, so S3 rests entirely on whether the model
looks upward. Predicted failure mode: a confident, generic `belongs_to`.

## C3 — Vendor and third-party first person (attribution; S4)

**Probe ACI-P1 · acimachine.com.** `Our signature line of VMX machining centers
takes machining to the next level.` — the "Our" is **Hurco**, on a dealer's
listing page. ~1,292 listing pages carry the builder's own brochure copy, so
first-person capability language on that site is overwhelmingly *not* the
subject's.

**Probe AL-P2 · alecmfg.com.** The `https://alecmfg.com/` block is a
vendor-generated cookie policy in first-person "we":
`This Cookie Policy was synchronized with cookiedatabase.org on July 31, 2025.`
→ `whose_words: vendor`, not `site`.

This is where the measured 99.5% `in the site's own copy` boilerplate is most
likely to be simply wrong, and it is the class both grounding and synthesis read
as provenance.

## C4 — Detached attribution (attribution; S4)

**Probe AGS-P1 · agstech.net.** `All plants manufacturing parts and products for
AGS-TECH Inc are certified to one or several of the following QUALITY MANAGEMENT
SYSTEM (QMS) standards:` — and the certificates then appear as bare items
(`- ISO 9001`) six **blank-line-separated** lines below.

No snippet can carry the ownership sentence, so **the location text is the only
surviving channel for the fact that these certificates belong to suppliers**.
The same site then contradicts itself in the first person on its Fasteners page.
This is the cleanest attribution case in the corpus: unambiguous ground truth,
and unreachable by clipping.

## C5 — Speaker named on an adjacent line (attribution; S4)

**Probe AL-P3 · alecmfg.com.** Eight testimonials whose speaker name and title
sit *before* the quote, so a clip carries the words and not the speaker.

**Probe ABLE-P1 · ableengineering.com.** Two structurally identical bylines
differ only by job title: `said Travis Tyler, vice president and general
manager, Able Aerospace Services Inc.` (→ `site`) versus `said Brad White,
senior vice president of Global Parts and Programs` (→ **parent**, a Textron
Aviation role). One word of the title is the whole difference.

**Probe BAT-P1 · blackadvtech.com.** Press excerpts are a journalist's words
wrapping the owner's words, with the outlet parenthetical at line end where a
short clip loses it.

## C6 — Heading ambiguity (location; S2, S3)

**Probe BAT-P2 · blackadvtech.com.** 620 all-caps lines carry three different
things at once. `WELDING` (14×), `MILITARY` (12×), `SHEET METAL` (10×) are
**blog category tags** under `Blog Categories`, not capability headings — a
model reading a lone `WELDING` as a heading is wrong about ten times in
fourteen.

Cross-subject warning: alecmfg has **eight** all-caps lines in the entire file,
anchor-mfg's all-caps is a clean nav/legal discriminator, and blackadvtech's is
ambiguous. **Any heading heuristic tuned on one subject will be wrong on
another.**

## C7 — The group heading carries the process (extent; S1)

**Probe BAT-P3 · blackadvtech.com.** In the equipment list the process exists
only in the ALL-CAPS group heading; the item line
`• 13 – Lincoln STT II` carries none. And the structure is inconsistent —
`SEAM WELDERS • 1 – Pandjiris 8′ bed` fuses heading and item on one line, so a
clip is sometimes complete and sometimes not, from the same list.

## C8 — Repeated-line dominance (context; annotation)

Measured shares of non-blank lines occurring five or more times: **acimachine
95.6%** (86.6% of bytes; only 9,762 distinct lines in 4 MB), **ableengineering
59.6%** (86.5% occur at least twice). Country dropdowns (~240 ISO country names
from a web form) appear on decimal, lucasmilhaupt and sterlingmfg.

Not a defect by itself — the fold already sends a repeated line once — but it
means a large share of judged snippets are menu and footer fragments, and rates
must be sliced by the `repeated_line` annotation or they will be dominated by
boilerplate.

## C9 — Invisible non-empty lines (extent; S1)

**Probe AGS-P2 · agstech.net.** 3,728 of 30,040 lines (12.4%) render blank but
contain only U+200B or U+00A0 (2,374 zero-width spaces, 5,124 non-breaking
spaces). **blackadvtech** uses a lone NBSP line as its paragraph separator
across 339 lines (5.4% of the file), and anchor-mfg does the same on
`/industries-served/`.

Consequence: "blank line = paragraph break" **never fires** in those documents.
The clipper's notion of a unit boundary is wrong there in a way that is
invisible to the eye and to a naive check.

## C10 — Wall-of-text lines (extent; S1)

**Probe AGS-P3 · agstech.net.** 301 lines exceed 1,000 characters and carry
**31.0% of all non-blank characters**; the longest is **17,397 characters**,
against a median non-blank line of 38. Where the clipper falls back to the whole
line, the snippet is a 17 KB block. 155 `….etc` ellipses sit inside those same
lines, defeating sentence splitting.

## C11 — Truncated nav labels (grouping; G1/G2, and a false-fabrication risk)

**Probe ACI-P2 · acimachine.com.** The footer menu ellipsis-truncates its own
labels, so one category exists in two non-matching spellings:
`Vertical Machining Centers` (2,839) and `Vertical Machinin...` (2,860).

**A zero-hit form here may not be a fabrication** — it may be a truncation. Any
instrument reading zero-hit forms as evidence of invention must know this.

## C12 — Punctuation that breaks exact matching (occurrence)

**Probe 101-P1b · 101machine.com.** `Steel's: Cold Rolled/Hot Rolled/Carbon/
Structual/Tool` uses U+2019, so an ASCII-apostrophe form misses entirely; five
material forms share one identical non-sentence snippet; and `Steel` matches
*inside* it, which is a C13 inheritance case as well.

Related, already handled by the shared matcher but worth a standing probe: two
quotes in this corpus that looked like they ended in a trailing space actually
end in U+00A0 — 1,430 acimachine lines that no exact match on the visible text
will ever hit.

## C13 — Inheritance dilution (already TAXONOMY I1; probes recorded here)

**Probe SC-P1 · steelcraft.com.** `Our steel doors and frames set the industry
standard.` — sound evidence for `door`. Contrast with a sentence naming only the
`Paladin™ PW Series` credited to the bare form `door`.

**Probe 101-P2 · 101machine.com.** `Steel` inside `Steel's: Cold Rolled/Hot
Rolled/...` — a slash-delimited list, not a sentence, so the inherited evidence
is a fragment with no predicate.

---

## Promotion checklist

Before any of the above moves into `TAXONOMY.md`:

1. It must have fired on a real run, not only on the texts.
2. Its counting rule must be stated and reproducible.
3. Its false-positive rate must be measured on the run that motivated it **and**
   on one other, because a relaxation-style check tuned on one run has twice
   been wrong on the next.
4. The promotion is batched with any others and announced in the run report,
   because it invalidates every cached verdict for that field.
