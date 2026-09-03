# Search-stage eval — mechanical pass, run 20260901T013332

Generated 2026-09-01T01:55:25.808753+00:00. Judged metrics
(precision census, recall census, wrong-actor, agreement) are produced
by the RUNBOOK's agent protocol and merged into these scorecards by the
assistant — this file alone is NOT the full evaluation.

| subject | field | verdict | live/replayed | recall (confirmed) | not-in-window | consistency | Δ vs baseline |
|---|---|---|---|---|---|---|---|
| ableengineering.com | conformity_attestations | RED — confirmed_recall_misses:1 | 8/0 | 0.9231 | 0.0 | 0.8714 | none comparable |
| ableengineering.com | equipments | OK | 8/0 | 1.0 | 0.0 | 1.0 | none comparable |
| ableengineering.com | industries | OK | 8/0 | 1.0 | 0.012 | 0.7147 | none comparable |
| ableengineering.com | material_caps | OK | 8/0 | 1.0 | 0.0 | 0.495 | none comparable |
| ableengineering.com | process_caps | RED — confirmed_recall_misses:3 | 8/0 | 0.8571 | 0.0159 | 0.8161 | none comparable |
| ableengineering.com | products | OK | 8/0 | 1.0 | 0.0029 | 0.6441 | none comparable |
| acimachine.com | conformity_attestations | OK | 11/0 | not gated (2 candidates) | 0.0 | 1.0 | none comparable |
| acimachine.com | equipments | OK | 11/0 | 1.0 | 0.0 | 0.9244 | none comparable |
| acimachine.com | industries | OK | 11/0 | not gated (0 candidates) | 0.0 | 0.6928 | none comparable |
| acimachine.com | material_caps | OK | 11/0 | not gated (14 candidates) | 0.0 | 0.6807 | none comparable |
| acimachine.com | process_caps | RED — confirmed_recall_misses:1 | 11/0 | 0.0 | 0.0036 | 0.7592 | none comparable |
| acimachine.com | products | OK | 11/0 | not gated (0 candidates) | 0.0024 | 0.8725 | none comparable |
| agstech.net | conformity_attestations | RED — confirmed_recall_misses:5 | 9/0 | 0.8958 | 0.0 | 0.937 | none comparable |
| agstech.net | equipments | RED — confirmed_recall_misses:7 | 9/0 | 0.8056 | 0.0094 | 0.6714 | none comparable |
| agstech.net | industries | RED — confirmed_recall_misses:7 | 9/0 | 0.8372 | 0.0 | 0.6898 | none comparable |
| agstech.net | material_caps | RED — confirmed_recall_misses:13 | 9/0 | 0.6977 | 0.0 | 0.6116 | none comparable |
| agstech.net | process_caps | RED — confirmed_recall_misses:23 | 9/0 | 0.8414 | 0.0169 | 0.763 | none comparable |
| agstech.net | products | RED — confirmed_recall_misses:14, unparseable_windows:2 | 9/0 | 0.9239 | 0.0 | 0.8389 | none comparable |
| alecmfg.com | conformity_attestations | RED — confirmed_recall_misses:3 | 9/0 | 0.7857 | 0.0 | 0.9639 | none comparable |
| alecmfg.com | equipments | OK | 9/0 | 1.0 | 0.0185 | 0.3234 | none comparable |
| alecmfg.com | industries | RED — unparseable_windows:1 | 9/0 | 1.0 | 0.0 | 0.6146 | none comparable |
| alecmfg.com | material_caps | OK | 9/0 | 1.0 | 0.0 | 0.8736 | none comparable |
| alecmfg.com | process_caps | RED — confirmed_recall_misses:3 | 9/0 | 0.9211 | 0.009 | 0.7978 | none comparable |
| alecmfg.com | products | OK | 9/0 | 1.0 | 0.0 | 0.6616 | none comparable |
| anchor-mfg.com | conformity_attestations | OK | 2/0 | not gated (8 candidates) | 0.0 | 1.0 | none comparable |
| anchor-mfg.com | equipments | RED — not_in_window_rate:0.0476>0.02 | 2/0 | 1.0 | 0.0476 | 1.0 | none comparable |
| anchor-mfg.com | industries | OK | 2/0 | 1.0 | 0.0 | 0.9556 | none comparable |
| anchor-mfg.com | material_caps | RED — not_in_window_rate:0.0263>0.02 | 2/0 | 1.0 | 0.0263 | 0.92 | none comparable |
| anchor-mfg.com | process_caps | OK | 2/0 | 1.0 | 0.0 | 0.9675 | none comparable |
| anchor-mfg.com | products | OK | 2/0 | 1.0 | 0.0 | 0.9056 | none comparable |
| blackadvtech.com | conformity_attestations | RED — not_in_window_rate:0.0263>0.02 | 9/0 | 1.0 | 0.0263 | 0.9568 | none comparable |
| blackadvtech.com | equipments | OK | 9/0 | 1.0 | 0.0 | 0.6677 | none comparable |
| blackadvtech.com | industries | RED — confirmed_recall_misses:3 | 9/0 | 0.7 | 0.0 | 0.4823 | none comparable |
| blackadvtech.com | material_caps | RED — confirmed_recall_misses:1 | 9/0 | 0.9375 | 0.0068 | 0.9435 | none comparable |
| blackadvtech.com | process_caps | RED — confirmed_recall_misses:3 | 9/0 | 0.9 | 0.0031 | 0.8117 | none comparable |
| blackadvtech.com | products | RED — confirmed_recall_misses:1, unparseable_windows:1 | 9/0 | 0.9375 | 0.0 | 0.7115 | none comparable |
| decimal.net | conformity_attestations | OK | 6/0 | 1.0 | 0.0 | 0.9731 | none comparable |
| decimal.net | equipments | RED — confirmed_recall_misses:1 | 6/0 | 0.9375 | 0.0 | 0.6828 | none comparable |
| decimal.net | industries | OK | 6/0 | 1.0 | 0.0 | 0.8069 | none comparable |
| decimal.net | material_caps | RED — confirmed_recall_misses:2 | 6/0 | 0.9623 | 0.0 | 0.813 | none comparable |
| decimal.net | process_caps | RED — confirmed_recall_misses:3 | 6/0 | 0.9741 | 0.0033 | 0.8318 | none comparable |
| decimal.net | products | RED — confirmed_recall_misses:2 | 6/0 | 0.9718 | 0.0 | 0.7492 | none comparable |
| fzemanufacturing.com | conformity_attestations | RED — confirmed_recall_misses:1 | 9/0 | 0.9091 | 0.0 | 0.8056 | none comparable |
| fzemanufacturing.com | equipments | OK | 9/0 | 1.0 | 0.0105 | 0.5544 | none comparable |
| fzemanufacturing.com | industries | OK | 9/0 | 1.0 | 0.0015 | 0.793 | none comparable |
| fzemanufacturing.com | material_caps | RED — confirmed_recall_misses:2 | 9/0 | 0.9718 | 0.0 | 0.6836 | none comparable |
| fzemanufacturing.com | process_caps | RED — confirmed_recall_misses:3 | 9/0 | 0.9717 | 0.0025 | 0.8541 | none comparable |
| fzemanufacturing.com | products | RED — confirmed_recall_misses:1, unparseable_windows:1 | 9/0 | 0.9783 | 0.0039 | 0.6387 | none comparable |
| howcogroup.com | conformity_attestations | RED — confirmed_recall_misses:6, not_in_window_rate:0.0574>0.02 | 8/0 | 0.8378 | 0.0574 | 0.9163 | none comparable |
| howcogroup.com | equipments | RED — confirmed_recall_misses:1 | 8/0 | 0.9286 | 0.0 | 0.6364 | none comparable |
| howcogroup.com | industries | RED — confirmed_recall_misses:3 | 8/0 | 0.9167 | 0.0075 | 0.6486 | none comparable |
| howcogroup.com | material_caps | RED — confirmed_recall_misses:8 | 8/0 | 0.8689 | 0.004 | 0.7391 | none comparable |
| howcogroup.com | process_caps | RED — confirmed_recall_misses:5 | 8/0 | 0.9231 | 0.0138 | 0.7795 | none comparable |
| howcogroup.com | products | RED — confirmed_recall_misses:1 | 8/0 | 0.988 | 0.0 | 0.7391 | none comparable |
| lucasmilhaupt.com | conformity_attestations | OK | 2/0 | not gated (0 candidates) | 0.0 | 1.0 | none comparable |
| lucasmilhaupt.com | equipments | OK | 2/0 | not gated (0 candidates) | — | — | none comparable |
| lucasmilhaupt.com | industries | OK | 2/0 | 1.0 | 0.0 | 0.9899 | none comparable |
| lucasmilhaupt.com | material_caps | RED — confirmed_recall_misses:1 | 2/0 | 0.75 | 0.0 | 0.9676 | none comparable |
| lucasmilhaupt.com | process_caps | OK | 2/0 | 1.0 | 0.0 | 0.9792 | none comparable |
| lucasmilhaupt.com | products | RED — confirmed_recall_misses:3 | 2/0 | 0.6667 | 0.0 | 0.8373 | none comparable |
| mathewsco.com | conformity_attestations | RED — confirmed_recall_misses:5 | 2/0 | 0.7222 | 0.0 | 1.0 | none comparable |
| mathewsco.com | equipments | OK | 2/0 | 1.0 | 0.0 | 0.9688 | none comparable |
| mathewsco.com | industries | RED — confirmed_recall_misses:1 | 2/0 | 0.9756 | 0.0 | 0.9913 | none comparable |
| mathewsco.com | material_caps | RED — confirmed_recall_misses:7 | 2/0 | 0.8333 | 0.0 | 0.9787 | none comparable |
| mathewsco.com | process_caps | RED — confirmed_recall_misses:10 | 2/0 | 0.8438 | 0.0 | 0.9626 | none comparable |
| mathewsco.com | products | RED — confirmed_recall_misses:8, not_in_window_rate:0.0635>0.02 | 2/0 | 0.875 | 0.0635 | 0.9455 | none comparable |
| med-tekinc.com | conformity_attestations | OK | 1/0 | not gated (0 candidates) | 0.0 | 1.0 | none comparable |
| med-tekinc.com | equipments | OK | 1/0 | 1.0 | 0.0 | 1.0 | none comparable |
| med-tekinc.com | industries | OK | 1/0 | 1.0 | 0.0 | 1.0 | none comparable |
| med-tekinc.com | material_caps | RED — confirmed_recall_misses:2 | 1/0 | 0.7778 | 0.0 | 1.0 | none comparable |
| med-tekinc.com | process_caps | OK | 1/0 | 1.0 | 0.0 | 1.0 | none comparable |
| med-tekinc.com | products | OK | 1/0 | not gated (0 candidates) | 0.0 | 1.0 | none comparable |
| pradeepmetals.com | conformity_attestations | RED — confirmed_recall_misses:1 | 9/0 | 0.95 | 0.0 | 0.6838 | none comparable |
| pradeepmetals.com | equipments | RED — confirmed_recall_misses:5 | 9/0 | 0.8611 | 0.0 | 0.4252 | none comparable |
| pradeepmetals.com | industries | RED — confirmed_recall_misses:1 | 9/0 | 0.963 | 0.0 | 0.6075 | none comparable |
| pradeepmetals.com | material_caps | RED — confirmed_recall_misses:2 | 9/0 | 0.9636 | 0.0 | 0.8327 | none comparable |
| pradeepmetals.com | process_caps | RED — confirmed_recall_misses:6 | 9/0 | 0.8909 | 0.0 | 0.9092 | none comparable |
| pradeepmetals.com | products | RED — confirmed_recall_misses:1, unparseable_windows:1 | 9/0 | 0.95 | 0.0 | 0.699 | none comparable |
| steelcraft.com | conformity_attestations | RED — confirmed_recall_misses:1 | 6/0 | 0.9792 | 0.0122 | 0.775 | none comparable |
| steelcraft.com | equipments | OK | 6/0 | not gated (0 candidates) | 0.0 | 0.8317 | none comparable |
| steelcraft.com | industries | RED — confirmed_recall_misses:2 | 6/0 | 0.8462 | 0.0 | 0.8043 | none comparable |
| steelcraft.com | material_caps | RED — confirmed_recall_misses:1 | 6/0 | 0.9167 | 0.0 | 0.9015 | none comparable |
| steelcraft.com | process_caps | RED — confirmed_recall_misses:2 | 6/0 | 0.8182 | 0.0 | 0.8107 | none comparable |
| steelcraft.com | products | OK | 6/0 | 1.0 | 0.0082 | 0.7742 | none comparable |
| superiortech.org | conformity_attestations | OK | 2/0 | 1.0 | 0.0 | 1.0 | none comparable |
| superiortech.org | equipments | RED — confirmed_recall_misses:5 | 2/0 | 0.8214 | 0.0 | 0.9574 | none comparable |
| superiortech.org | industries | RED — confirmed_recall_misses:1 | 2/0 | 0.9643 | 0.0 | 1.0 | none comparable |
| superiortech.org | material_caps | RED — confirmed_recall_misses:1 | 2/0 | 0.9444 | 0.0 | 0.9655 | none comparable |
| superiortech.org | process_caps | RED — confirmed_recall_misses:4 | 2/0 | 0.9551 | 0.0 | 0.8958 | none comparable |
| superiortech.org | products | RED — confirmed_recall_misses:2 | 2/0 | 0.9444 | 0.0 | 0.9523 | none comparable |
| tanfel.com | conformity_attestations | RED — not_in_window_rate:0.0278>0.02 | 7/0 | 1.0 | 0.0278 | 0.9665 | none comparable |
| tanfel.com | equipments | RED — confirmed_recall_misses:4 | 7/0 | 0.7895 | 0.0 | 0.459 | none comparable |
| tanfel.com | industries | RED — confirmed_recall_misses:3 | 7/0 | 0.85 | 0.0 | 0.4054 | none comparable |
| tanfel.com | material_caps | RED — confirmed_recall_misses:7 | 7/0 | 0.9091 | 0.0 | 0.7805 | none comparable |
| tanfel.com | process_caps | RED — confirmed_recall_misses:3 | 7/0 | 0.963 | 0.0 | 0.8969 | none comparable |
| tanfel.com | products | RED — confirmed_recall_misses:4 | 7/0 | 0.9375 | 0.007 | 0.855 | none comparable |
| taylordunn.com | conformity_attestations | OK | 5/0 | not gated (4 candidates) | 0.0 | 1.0 | none comparable |
| taylordunn.com | equipments | OK | 5/0 | not gated (1 candidates) | 0.0 | 0.6667 | none comparable |
| taylordunn.com | industries | OK | 5/0 | 1.0 | 0.0175 | 0.7274 | none comparable |
| taylordunn.com | material_caps | RED — not_in_window_rate:0.0714>0.02 | 5/0 | not gated (12 candidates) | 0.0714 | 0.9396 | none comparable |
| taylordunn.com | process_caps | RED — not_in_window_rate:0.0526>0.02 | 5/0 | not gated (4 candidates) | 0.0526 | 0.8333 | none comparable |
| taylordunn.com | products | OK | 5/0 | 1.0 | 0.0 | 0.8182 | none comparable |

## Cross-field collisions

Distinct forms this field returned that some OTHER field also returned
for the same subject. Mechanical, so no agreement floor applies. A
collision is not automatically a defect — see `_collision_rollup`.

| field | distinct forms | claimed by a sibling | share |
|---|---|---|---|
| equipments | 1270 | 1101 | 86.7% |
| process_caps | 3484 | 1703 | 48.9% |
| material_caps | 1698 | 812 | 47.8% |
| products | 6659 | 2719 | 40.8% |
| industries | 1521 | 429 | 28.2% |
| conformity_attestations | 759 | 125 | 16.5% |
| **ALL** | **15391** | **6889** | **44.8%** |

| field pair | shared | Jaccard |
|---|---|---|
| process_caps ↔ products | 1426 | 16.4% |
| equipments ↔ products | 994 | 14.4% |
| material_caps ↔ products | 703 | 9.2% |
| equipments ↔ process_caps | 528 | 12.6% |
| industries ↔ products | 338 | 4.3% |
| industries ↔ process_caps | 249 | 5.2% |
| material_caps ↔ process_caps | 214 | 4.3% |
| equipments ↔ industries | 122 | 4.6% |
