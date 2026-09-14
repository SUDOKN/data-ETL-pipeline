# Run-B tryout — judge brief (one judge per field; Opus)

You are judging a prompt tryout. The synthesis stage of this pipeline reads a manufacturer's
website text plus a set of RECORDS (each: a `record_id`, a `focal_form` = the name of one entity, and
its `snippets` = passages copied verbatim from the text) and writes ONE paragraph per record saying
what the snippets show about that entity and what the manufacturer does with it (whose dealing it is,
in what capacity, where on the site it sits). Two prompt arms were run on the same real production
requests, five times each, with the production call shape (gpt-4.1, temperature 0, seed fixed, strict
JSON schema). The model is BIMODAL per request even at temperature 0 — the same request can come back
in a different "reading" on another repeat — so **every arm is read as a MODE FRACTION over its five
repeats**, never as one sample.

- Arm **c16** = the static now in production (`c16_system.txt`).
- Arm **b** = c16 + one new section, "## What to settle about the dealing", specific to your field
  (`b_<field>_system.txt`). `diff c16_system.txt b_<field>_system.txt` shows exactly the added text.

## Files (all under this directory, `pass2/`)

- Requests: `reqb_<tag>_user.txt` — the full user message (site text, manufacturer name, the record
  blocks at the bottom). The records you judge are the ones inside `<<<RECORDS … RECORDS>>>`.
- Outputs: `b_out_<tag>_<arm>_<k>.json`, k = 0..4 — `{"syntheses":[{"record_id","synthesis"}…]}`.
- Your field's tags are given in your instruction message.

## What to read, per request

For every record, read its ten paragraphs (5 × c16, 5 × b) against the record's snippets and the
passage they sit in inside the site text. Judge by READING; never grep for hedge words and count them
(that failed three times on this prose). Decide, per record and per arm, the MODE: what the majority of
the five repeats say about
1. **party** — whose dealing the paragraph gives the entity to (the manufacturer's own; another named
   party the manufacturer lists/represents/hosts; a customer; nobody);
2. **capacity** — what the manufacturer does with it (makes as its own / works to order / resells or
   represents / services / uses as input or tool / supplies into / holds or meets / unstated / none);
3. **truth** — is that reading supported by the snippets and their placement? (`right`, `wrong`,
   `hedged-when-fixed` = the text fixes the capacity but the paragraph calls it unstated,
   `supplied-when-open` = the paragraph supplies a capacity the text does not fix);
4. **defects introduced** — anything the b arm does that c16 does not: echoing the section's option
   list verbatim ("makes, performs, or provides it as its own"), refusing the manufacturer's own
   listings ("does not attribute … to any party"), naming a page the text does not show, dropping
   the focal entity, the formulaic closer, wire vocabulary (focal_form, focal entity, record,
   snippet, "the manufacturer"), first-person leaks, length inflation.

## Calibration rulings (from the production eval's template; apply them)

- The FRAME decides "offers": a bare item under the manufacturer's own "we offer / our capabilities /
  our products" IS offered; a line card, a represented maker's page, a document library, or an
  explainer yields "listed under …; capacity unstated". Hedging an own listing is a defect
  (under-claim); supplying a capacity on a line card is a defect (over-claim / laundering).
- Another party's "they / their / we (on that party's hosted page)" keeps the doing; the manufacturer's
  part is to list, carry, or represent it. Writing "the manufacturer offers X" for a represented
  maker's line is LAUNDERING (major).
- Serving / supplying into a sector or a customer's product is a positive finding, not a hedge.
- Holding a standard vs meeting it vs being required of others are different modes; the paragraph
  must keep the snippets' one.
- A material a shop works (machines, casts, forms) is a dealing in its own right; asking whether the
  shop MAKES the metal is not required and "whether it manufactures or sources it is not specified"
  on the shop's own grade table is an under-claim.
- Equipment the shop uses is not equipment it makes; equipment it lists for sale is not equipment it
  uses.
- A page-supported fact about the focal entity that is not in the snippets is a grounded import
  (allowed); a claim the text does not support anywhere is a fabrication (major).

## Output

Write `JUDGE_RUN_B_<field>.md` in this directory with:
1. **Headline**: does the b arm's section help, hurt, or do nothing on this field, in one paragraph,
   with the numbers that carry it (records whose MODE is right: c16 vs b; records whose mode flipped
   between arms and in which direction; defects introduced by b, with counts).
2. **Per request**: a table per record or per class of records — record id, focal form, c16 mode
   (n/5 in that reading), b mode (n/5), truth of each, and a verbatim quote from one b paragraph
   where b differs from c16.
3. **Defects introduced by b**: each with a verbatim quote and the record id; say if it is mode-level
   (majority of repeats) or a single repeat.
4. **Wording**: if a sentence of the section is the cause of a defect, name the sentence and propose
   the smallest rewrite; if a sentence never fired on any of your records, say so (it is then
   unvalidated, not validated).
5. **Verdict** for this field's section: SHIP / SHIP WITH EDIT (give the edit) / DO NOT SHIP, with
   the reason in one sentence.

Discipline: quote what convicts; never fail a paragraph for not disclaiming something the snippets
do not raise; the mode is what matters, a 1/5 outlier is noted, not counted; be as hard on c16 as on
b (a c16 defect that b fixes is a gain and must be counted as one).
