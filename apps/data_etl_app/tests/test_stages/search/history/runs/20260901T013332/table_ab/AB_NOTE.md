# Text-augmentation A/B — no prompt changes (2026-09-02)

User's hypothesis: fix table/structure misses at scrape time (HTML→markdown
conversion) instead of adding table instructions to the search prompts, and
test whether the UNCHANGED prompts then recover the misses. Prompts: the
on-disk statics (equipment's includes the 2026-09-02 drafted metrology +
specifics clauses — same in both arms, so arm parity holds). Params mirror
production: temp 0, seed 12345, json_object, max_completion_tokens 4000,
via the litellm proxy (`apps/litellm_proxy_app/src/.env`). 24 calls
(~$0.50): 3 windows x 2 arms x {gpt-4.1, gpt-4.1-mini} x 2 repeats.
Machinery in this directory: `transforms.py`, `run_ab.py`, `results.json`.

## Context that reframed the question

Line-typing every never-found entity's evidence quote (see
`../NEVER_FOUND_ANALYSIS.md`): **pipe tables hold only 16 of the 1,162
never-found entities**. The dominant contexts are bare label lines (573
equipments alone — catalog tiles, inventory lines) and list items (166).
So "how should tables be rendered" was the wrong headline question for THIS
corpus; "how do label lines get their governing heading" is the right one.
The transforms prototype both:

- `label_augment`: runs of >=5 bare label lines get the page h1 prefixed —
  `J-2550 (1)` under `# JET` becomes `JET: J-2550 (1)`.
- `table_to_kv`: each pipe-table data row becomes one `Header: value; ...`
  line (row-level + repeated-header rendering). Known hazard seen live:
  layout tables (contact blocks) produce garbage KV (`Heat Treatment:
  additivesales@howcogroup.com`) — the real scraper must gate on data-table
  detection from the DOM.

Windows: A = acimachine equipments 66765:78696 (JET catalog, 335 gates);
C = howcogroup material_caps 35516:53359 (6 gates: UNS codes in h2 headings —
untransformed controls — plus chem-table Boron/Grade 630); D = alecmfg
process_caps 94729:113432 (4 gates embedded in spec-table values).

## Results (gate hits out of window's never-found entities)

| window | arm | gpt-4.1 rep0/rep1 (union) | gpt-4.1-mini rep0/rep1 (union) |
|---|---|---|---|
| A (335 gates) | original | 2 / 2 (2) | 7 / 1 (7) |
| A (335 gates) | **augmented** | **332 / 292 (332)** | 7 / 7 (7) |
| C (6 gates) | original | 0 / 3 (3) | 0 / 0 (0) |
| C (6 gates) | augmented | 3 / 2 (3) | 0 / 0 (0) |
| D (4 gates) | original | 0 / 0 (0) | 0 / 0 (0) |
| D (4 gates) | augmented | 0 / 0 (0) | 0 / 0 (0) |

## Verdicts

1. **Label augmentation is the largest single recall lever measured in this
   whole effort — on the production model, with zero prompt changes.**
   gpt-4.1 went from 2/335 to 332/335 on the exemplar catalog window,
   returning verbatim rendered forms (`JET: 1015VS`). Phrase volume 39 →
   337–377, essentially all real codes + the categories; finish=stop.
2. **The 4,000-token output cap is now the binding constraint on such
   windows**: completions hit 3,654/3,538 of 4,000. A modestly larger
   catalog run will truncate. Remedies: smaller search sub-windows (the
   2.5k divisor idea) or a raised cap for search.
3. **gpt-4.1-mini is unmoved by augmentation on the catalog class** (7/335
   both arms) even though (a) its prompt already carried the drafted
   general-vs-specific clause and (b) it demonstrably read the augmented
   text (it returned `JET: Engine Lathes`-style category forms). Bare-code
   enumeration appears to be a capability wall for the mini tier, not a
   text-shape or instruction problem. Consequence for the
   smaller-models-everywhere plan: search on catalog-heavy subjects is not
   currently mini-safe.
4. **Table→KV showed no measurable gain on its gates** (D: 0→0 everywhere;
   C: within the run-to-run churn, and chem-table Boron stayed missed).
   Consistent with tables being a 16-of-1,162 problem here. Value-embedded
   operations (`Blasted + epoxy coated` inside a spec-value string) stay
   missed even when the row carries its headers — that class is
   prompt/boundary territory, not rendering.
5. C's mini volume tripled (19–26 → 60–63 forms) with sane material forms —
   augmentation did not inject junk there; but its 6 gates stayed at 0.

## Implications (decisions are the user's)

- Scraper: implement heading-propagation for label/tile runs in
  `html_to_markdown.py` (DOM-aware, not this text heuristic), gated on a
  data-vs-layout distinction; table→KV is optional/deprioritized on this
  evidence. READ FIRST the scraper memories before touching it
  (scraper-markdown-format-decision, search-corpus-markdown-cutover).
  Any text-shape change re-fingerprints everything and owes an eval-corpus
  port/re-seed (the port machinery exists).
- Prompts: on this evidence the user's no-new-instructions stance holds for
  the label class on the production model. The untouched classes
  (educational prose P3, value-embedded table entities, specifics-in-prose
  P2) remain prompt/boundary territory per NEVER_FOUND_ANALYSIS.md.
- Dispatch: cap/window-size relief for enumerated regions; the catalog
  class also re-raises the acimachine coverage caveat (1.3% read).
