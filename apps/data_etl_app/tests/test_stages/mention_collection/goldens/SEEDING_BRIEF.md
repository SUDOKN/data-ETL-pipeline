# Seeding brief — how a corpus subject gets its mention-stage golden data

The instruction set handed verbatim to every seeding agent, so a subject seeded
a year from now is seeded the way the first twenty were. Read
`../GOLDEN_LABELS_SCHEMA.md` alongside it: this brief is the **procedure**, that
file is the **contract**.

Sibling of `search/expectations/SEEDING_BRIEF.md`. Where they overlap, follow
this one — the two stages ask different questions of the same text.

---

## 1. What you are producing, in one sentence

A record of **what the mention stage should do with the forms it is handed** on
one subject: where each form really occurs, what passage should carry each
occurrence, what that passage's context truly is, which forms are the same
thing, and — where a short form sits inside a longer one — whether crediting it
is sound.

You are **not** judging the pipeline, and you are **not** deciding whether a
form deserved to be searched for. That second question belongs to the search
eval. A document title, a client's product and a polyseme are all in scope here
for *how well they are handled*.

## 2. The two rules that have already caused false alarms

**(a) Measure with the collector, not with your own regex.** One form has three
defensible counts: `doors and frames` on steelcraft is 123 by a whole-word
elastic regex, **120** through `collect_window`, and something else again by
hand. The gap is real — the collector blanks URL lines and page-separator lines
before scanning. The collector's number is the authoritative one, because it is
what the stage will actually do. `checks/seed_goldens.py` calls the production
`collect_window` for exactly this reason.

**(b) Never retype a quote — lift it out of the file by offset.** This corpus
carries 843 non-breaking spaces on steelcraft alone, zero-width spaces on
agstech, and lines whose apparent trailing space is U+00A0. Two seeding passes
in 2026-08-27 had quotes fail verification for precisely this, and both were
caught only because the agent re-read the quote out of the file rather than
trusting what it had typed. Verify every quote programmatically before you
finish.

## 3. Procedure

1. **Harvest before deriving.** The same roster is already profiled by three
   sibling instruments. Read, if present:
   `search/expectations/<slug>/subject.yaml` (role and scrape hazards — most
   authoritative), `grounding/expectations/<slug>/subject.yaml`,
   `synthesis/expectations/<subject>/subject.yaml` (third-party roster), and
   `test_stages/GOLDEN_CORPUS_EXPANSION_2026-08-27.md` for the twelve newer
   subjects. **Re-verify anything you carry forward** — measured corrections to
   those documents are listed in §6 and there will be more.
2. **Reuse the search harness's `role` verbatim.** It is the same roster; a
   second vocabulary forks the corpus silently. If its word does not describe
   what this stage sees, keep the word and add a `role_note` saying so.
3. **Read the text for this stage's hazards** (§4) and write `subject.yaml`.
4. **Generate the occurrence labels mechanically** —
   `python checks/seed_goldens.py --subject <domain>`. Do not hand-write them.
5. **Validate** — `python checks/validate_goldens.py --subject <domain> --quotes`
   must report 0 errors before you finish.

## 4. What to survey, because this stage is the one that asks it

The stage clips each occurrence to the sentence holding it — or, where the line
has no sentence punctuation, to the whole line — and then a model describes
where that passage sits: **what kind of text it is, what it belongs to, and
whose words they are.** So record, each with a verbatim quote:

- **Repeated lines.** Navigation, footers, cookie banners, country dropdowns.
  Measured extremes worth knowing: acimachine has only 9,762 distinct non-blank
  lines in 4 MB (95.6% of lines occur 5+ times); pradeepmetals' footer is 46% of
  all lines; taylordunn is 99.3% exact repeats with *zero* page prose.
- **Table-like structures.** Two opposite failures, both real: alecmfg
  **atomizes** cells one per line so a header sits 17 lines from its value,
  while austinelectricservices **fuses** four cells onto one tab-separated line.
  One clipping dial cannot fix both.
- **Heading style.** All-caps that is a heading on one subject is a blog
  category tag on another (blackadvtech: `WELDING` is a tag ten times in
  fourteen). Any heading heuristic tuned on one subject will be wrong on
  another — say which this subject is.
- **Third-party voice — the highest-value thing you can record.** Who else
  speaks on this site, and where. Customer testimonials, parent-company blocks,
  suppliers' or principals' first-person copy, standards-body text, vendor
  brochure copy. Name the speaker and quote an example. This is what the
  location label's `whose_words` element must get right, and it is what both
  the grounding and synthesis evals read as provenance.
- **Detached attribution.** Where the sentence that says who owns something is
  separated from the thing itself — agstech's supplier certificates sit six
  blank-line-separated lines below the sentence that disowns them. No snippet
  can carry it, so the location text is the only surviving channel.
- **Invisible characters.** Non-breaking spaces, zero-width spaces, and lines
  that render blank but are not (agstech: 12.4% of lines). Where NBSP sits
  *between two word characters* it splits a form: steelcraft writes
  `ANSI A250.10-2011` fourteen times with an ASCII space and six times with an
  NBSP, so a byte-exact matcher on the ASCII spelling undercounts by 30%.
  Count them and say where they sit.
- **Degeneracy.** Duplicate pages, database-error pages, lorem ipsum, CMS junk,
  "Coming Soon" stubs, truncated navigation labels (acimachine's footer
  ellipsis-truncates its own labels, so a zero-hit form there may be a
  truncation rather than a fabrication).

## 5. Negative labels are first-class

A form that must produce **zero** mentions is as valuable as one that must
produce fourteen. Two kinds:

- **Substring traps** — the form is "found" inside a longer word. Record both
  counts: whole-word (should be 0) and naive-substring (the false positives a
  careless matcher produces). Note whether the trap is **case-conditional**:
  uppercase `PET` and `ITAR` do not occur on superiortech at all, so only a
  case-folding matcher turns 0 into 9, whereas `Iron` inside "environment"
  fires either way and only the word boundary saves it.
- **Zero-occurrence forms** — absent from the text entirely, yet tagged
  downstream. Every such claim inherited from another document must be
  re-measured; they have all survived so far, but two were misfiled (see §6).

## 6. Corrections already made — do not reintroduce them

- `Hemming` on blackadvtech is **not** a "HEM Saw" substring hit: it has zero
  hits of any kind. The 51 false positives belong to `HEM`, which *also*
  genuinely occurs three times as the brand. Two forms, opposite labels.
- The historical "`Lead`, 52 hits, 0 real" does not reproduce anywhere here.
  `Lead` is a real whole word on 16 of 20 subjects. Its defect is **sense**, so
  it belongs to snippet and location labels, not to occurrence labels.
- taylordunn is 31 of 31 pages identical, not 30 of 31, and the boilerplate
  share is 99.3% of lines, not ~97.5%.
- superiortech's equipment census is 152 machine units, not ~100. Beware the
  naive `^\(\d+\)` pattern — it also matches the phone number in every nav.
- sterlingmfg's country dropdown is 249 entries; lucasmilhaupt's is 194. Both
  were recorded elsewhere as "~240".

## 7. Output

`goldens/<slug>/subject.yaml` plus all seven field files. An empty
`labels: []` is **required**, never an absent file: "this field is genuinely
empty" is a real and valuable answer on several subjects, and it must be
distinguishable from "nobody labelled this field".

Labels are written as `candidate`. Promotion to `confirmed` — the only status
that gates — needs the second pass in `VERIFY_BRIEF.md`.
