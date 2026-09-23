# Judge brief — the shipped tags of a Step 2 run (census of run 20260922T215605)

You are coding rows from `out/judge_shipped/packets/<packet>.jsonl`. Each row is ONE (record, label) pair the
pipeline SHIPPED as a result: for the concept fields (industries, material_caps, process_caps,
conformity_attestations) an in-vocabulary label the reconcile step kept for that record, with the record's
descent `path` (wave by wave: the label screened, its verdict, what the descent under it reached, proposed or
declined); for the keyword fields (products, contract_products, equipments) a minted candidate that screening
accepted. Every row carries the record as the model saw it (`record`, `focal_form` = the record's subject as
the site names it, `synthesis`), the label, the screening's evidence label (`named` / `inferred`) and quote,
and for concept rows the labels the grounding call matched directly (`grounding_labels`).

Code by READING the record. Quotes and explanations are colour, never the decision. The binding rules are the
harness TAXONOMY (`apps/data_etl_app/tests/test_stages/grounding/TAXONOMY.md` — the tag-instance codes and the
descent hop codes, verbatim) and the addendum rulings 1–29
(`docs_local/grounding_gap_survey_2026-09-14/TAXONOMY_ADDENDUM.md`). Judge the general phenomenon; when the
wording does not settle a row, code it as best you can and mark `"unsettled": true` with the reason in `note`.

## What to code, per row

1. **`code`** — the taxonomy's tag-instance code for THIS label on THIS record: `D` direct, `N` normalized
   (the vocabulary's parent or normalized form of something the record names specifically — clean), `X` wrong
   axis (for industries add `sub_kind`: `X-own` = the company's own activity or product read as a sector served,
   ruling 22; `X-sector` = a real sector but not the one evidenced), `V` vague, `B` bridge (one unstated
   inference), `P` another party's or a different dealing (rulings 3, 17, 21), `F` fabricated (absent from the
   record). Rulings that bind most: 24–29 (a record using a label's OTHER NAME names it exactly, D/N never B;
   identification alone decides the code, the dealing is screening's question; a vocabulary parent chosen for a
   specifically named member is N; process labels compare the OPERATION; an industries label names a group of
   organizations; a designation decodes to its family); for equipments rulings 18 and 23 exactly (the record
   must NAME a class of machines; a bare process, control method, feature or tool/energy source names none →
   `F`; measuring/inspection equipment → `X`).
2. **`hop`** — concept rows only. When the shipped label sits BELOW the first label the record was screened
   under (the path shows waves > 1 or a descent that reached it), code the last hop that reached it: `H-OK`
   (the child's discriminating feature is in the record), `H-BRIDGE` (justified only by world knowledge),
   `H-FORCED` (the vocabulary forces a distinction the text never makes), `H-STOP` (stopped at a parent though
   the child was evidenced — look at the path's declined/rejected children — or descended past the last
   evidenced level). `none` when the label was matched directly and no descent applies. Keyword rows: `none`.
3. **`screen_ok`** — `yes` when accepting this pair was right (a clean code and the record shows the
   manufacturer's own current dealing), `no` otherwise.

## Output

One JSON object per line into `out/judge_shipped/verdicts/<packet name>.jsonl`, appended in batches of 10–20
rows so a cut-off leaves valid JSONL. Every row of the packet gets exactly one line.

```json
{"item_id": "s_0001", "code": "N", "sub_kind": null, "hop": "H-OK", "screen_ok": "yes", "unsettled": false, "note": "'6061-T6 aluminum' → Aluminum Alloy: designation decodes to its family (ruling 29); the hop to Wrought Aluminum Alloy is carried by '6061'", "judge": "sonnet:<agent name>"}
```

End with a short summary (counts per code and per hop, the rows you marked unsettled and why, any pattern that
repeated) — returned, not written to the verdicts file.
