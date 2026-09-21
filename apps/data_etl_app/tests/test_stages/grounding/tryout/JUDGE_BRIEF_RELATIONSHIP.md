# Judge brief — the relationship fields (products, contract products), Step 2 tryout (2026-09-21)

These rows are screening rows on the two fields whose relationship is specific, and the earlier sample
judged them on the wrong question. Each row carries one record as the model saw it (`record_id`,
`focal_form`, `synthesis`), the manufacturer's name, the candidate `label`, and the two screening arms'
mode verdicts (`sa_accepts` = today's prompt, `sb_accepts` = the Step 2 unit screening; `sb_detail` = its
per-repeat answers). Code by READING the record; the arms' verdicts and quotes are colour, never the
decision. Apply the general phenomenon; report rows the wording does not settle instead of inventing a rule.

The truth question is the FIELD'S relationship, read from the manufacturer's side:

- `products`: does the record prove that the manufacturer itself MANUFACTURES AND SELLS the candidate AS
  ITS OWN? A dealer, distributor or representative that lists or offers items another party makes does
  not (ruling 17 for products: the tag may be clean, the verdict must reject); a customer's commissioned
  part is not the manufacturer's own product either.
- `contract_products`: does the record prove that the manufacturer itself PERFORMS CONTRACT OR
  CUSTOMER-DIRECTED WORK on the candidate? A customer's order is a named other party's instruction that
  fixes what is made; the company's own catalog choices — a series, an option, a finish, a rating, a stock
  program — however described, are not orders (V9; the survey's steelcraft finding). A standing offer to
  build to customers' requirements counts when the record says so.

Answer `truth: accept | reject`; when `sb` accepted a row you code reject, say `guard_missed: yes`; when `sb`
rejected, say whether the rule it named is the right reason (`reason_ok: yes | no`).

## Output

One JSON object per line into `out/judge_rel/verdicts/<packet name>.jsonl`, appended in batches of 10–20:

```json
{"item_id": "r_0001", "field": "contract_products", "truth": "reject", "guard_missed": "yes", "reason_ok": null, "note": "'L Series doors available with finishes' is the catalog, no order named", "judge": "sonnet:<agent name>"}
```

`note`: at most 25 words quoting the decisive words. Every item exactly once. Finish with a short summary:
counts, rows the wording did not settle (item_ids + one line each), any pattern (never a new rule).
