# Diff-judge — the 2,958 forms the published prompts ADDED (run 20260904T230543)

Judged pass over every form the current run returned that the baseline
(20260903T212112) did not, on the same windows — the precision half of the
prompt-change A/B (the recall half lives in SUMMARY.md: +0.7 overall, the
three changed fields up, the three unchanged fields byte-identical).
14 sonnet judges, 56 census-style packets, coverage 2,958/2,958, zero count
mismatches. Read beside the census floors: double-judge rollup agreement
89.7%, A/A per-form Jaccard 0.897.

## Added-form quality vs the baseline's full stock

| field | added | in-field | (stock) | adjacent | (stock) | generic | (stock) | junk | (stock) |
|---|---|---|---|---|---|---|---|---|---|
| equipments | 329 | 34.7% | 65.5% | 63.8% | 33.0% | 1.2% | 1.0% | 0.3% | 0.4% |
| process_caps | 1,191 | 49.7% | 67.9% | 28.8% | 17.7% | 19.9% | 12.8% | 1.6% | 1.7% |
| products | 1,438 | 24.3% | 32.4% | 58.1% | 53.9% | 15.5% | 12.9% | 2.1% | 0.7% |
| **ALL** | **2,958** | **35.7%** | | **46.9%** | | **15.7%** | | **1.7%** | |

**Verdict: the prompt changes bought their recall (metrology captured,
software suppressed, engineering phrasings in, tooling carve-back live) at a
mild and expected precision dilution — the additions run ~36% in-field
against the stock's ~53%, junk stays ≤2%, and the dilution sits exactly
where predicted:**

- **process_caps additions are 19.9% generic** (vs 12.8% stock) — the
  engineering-inclusion sentence sweeps capability/outcome prose ("develop
  the ideal solutions…") alongside the real wins (reverse engineering,
  product development, DFM-class phrases). Also a product-name bleed class:
  gallery captions and product designations returned as process forms, coded
  U/adjacent by judges (alecmfg "Precision Sensor Ring", decimal album
  captions).
- **equipments additions skew adjacent (63.8%)** — process/product names
  swept alongside the genuinely in-field metrology third (CMMs, hardness
  testers, gauges: the census's exact flagged misses, now returned).
- **products additions are spec/adjacent-heavy** (58.1%), consistent with
  the field's structural adjacency; junk upticks to 2.1% absolute-small.
- One word-sense mint: pradeepmetals process returned `mould` from "yeast
  and moulds" (fungal-contamination R&D prose) — coded U.
- Actor flags on additions look healthy: dealer/rep content coded
  supplier/partner (acimachine, mathewsco), blog/educational content coded
  unclear (blackadvtech, fze welding guides), client items coded client.

Downstream relationship-screening exists to absorb the adjacent share; the
junk floor held. No prompt rollback indicated. Watch item for the NEXT
census: the generic share of process_caps (12.8% → expect ~14% pooled once
additions are folded in) and the claim-length engineering spans (an Extent
rule question if they grow).

Raw judgments: session scratchpad `diffjudge/out/*.jsonl` (56 files; local
only — the scratchpad is transient, the numbers above are the record).
