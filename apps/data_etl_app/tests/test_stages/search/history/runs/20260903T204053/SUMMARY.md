# Search-stage eval — mechanical pass, run 20260903T204053

Generated 2026-09-03T21:15:11.634904+00:00. Judged metrics
(precision census, recall census, wrong-actor, agreement) are produced
by the RUNBOOK's agent protocol and merged into these scorecards by the
assistant — this file alone is NOT the full evaluation.

| subject | field | verdict | live/replayed | recall (confirmed) | not-in-window | consistency | Δ vs baseline |
|---|---|---|---|---|---|---|---|
| 101machine.com | conformity_attestations | OK | 1/0 | not gated (0 candidates) | — | — | none comparable |
| 101machine.com | equipments | OK | 1/0 | 1.0 | 0.0 | 1.0 | none comparable |
| 101machine.com | industries | OK | 1/0 | 1.0 | 0.0 | 1.0 | none comparable |
| 101machine.com | material_caps | OK | 1/0 | 1.0 | 0.0 | 1.0 | none comparable |
| 101machine.com | process_caps | OK | 1/0 | 1.0 | 0.0 | 1.0 | none comparable |
| 101machine.com | products | OK | 1/0 | not gated (3 candidates) | 0.0 | 1.0 | none comparable |
| ableengineering.com | conformity_attestations | OK | 11/0 | 1.0 | 0.0 | 0.8773 | none comparable |
| ableengineering.com | equipments | OK | 11/0 | 1.0 | 0.0 | 0.9507 | none comparable |
| ableengineering.com | industries | OK | 11/0 | 1.0 | 0.0 | 0.547 | none comparable |
| ableengineering.com | material_caps | OK | 11/0 | 1.0 | 0.0 | 0.8884 | none comparable |
| ableengineering.com | process_caps | RED — confirmed_recall_misses:2 | 11/0 | 0.8824 | 0.0081 | 0.7488 | none comparable |
| ableengineering.com | products | OK | 11/0 | 1.0 | 0.0043 | 0.9159 | none comparable |
| acimachine.com | conformity_attestations | OK | 8/0 | not gated (2 candidates) | 0.0 | 1.0 | none comparable |
| acimachine.com | equipments | RED — confirmed_recall_misses:1 | 8/0 | 0.0 | 0.0 | 0.9817 | none comparable |
| acimachine.com | industries | OK | 8/0 | not gated (0 candidates) | 0.0 | 0.4618 | none comparable |
| acimachine.com | material_caps | OK | 8/0 | not gated (14 candidates) | 0.0 | 1.0 | none comparable |
| acimachine.com | process_caps | RED — confirmed_recall_misses:1 | 8/0 | 0.8889 | 0.0 | 0.8276 | none comparable |
| acimachine.com | products | OK | 8/0 | not gated (0 candidates) | 0.0 | 0.9458 | none comparable |
| agstech.net | conformity_attestations | RED — confirmed_recall_misses:2 | 9/0 | 0.913 | 0.0 | 0.9753 | none comparable |
| agstech.net | equipments | RED — confirmed_recall_misses:4 | 9/0 | 0.907 | 0.006 | 0.8002 | none comparable |
| agstech.net | industries | RED — confirmed_recall_misses:11 | 9/0 | 0.7708 | 0.0 | 0.7389 | none comparable |
| agstech.net | material_caps | RED — confirmed_recall_misses:15 | 9/0 | 0.7 | 0.0 | 0.888 | none comparable |
| agstech.net | process_caps | RED — confirmed_recall_misses:21 | 9/0 | 0.8571 | 0.0134 | 0.8215 | none comparable |
| agstech.net | products | RED — confirmed_recall_misses:27, unparseable_windows:1 | 9/0 | 0.9121 | 0.0 | 0.8203 | none comparable |
| alecmfg.com | conformity_attestations | OK | 8/0 | 1.0 | 0.0 | 0.9144 | none comparable |
| alecmfg.com | equipments | RED — confirmed_recall_misses:1 | 8/0 | 0.9091 | 0.0 | 0.7544 | none comparable |
| alecmfg.com | industries | OK | 8/0 | 1.0 | 0.0 | 0.5445 | none comparable |
| alecmfg.com | material_caps | OK | 8/0 | 1.0 | 0.0 | 0.8606 | none comparable |
| alecmfg.com | process_caps | RED — confirmed_recall_misses:3 | 8/0 | 0.9211 | 0.0116 | 0.7243 | none comparable |
| alecmfg.com | products | RED — confirmed_recall_misses:1 | 8/0 | 0.9583 | 0.0 | 0.7069 | none comparable |
| anchor-mfg.com | conformity_attestations | RED — not_in_window_rate:0.05>0.02 | 2/0 | not gated (8 candidates) | 0.05 | 1.0 | none comparable |
| anchor-mfg.com | equipments | RED — not_in_window_rate:0.0222>0.02 | 2/0 | 1.0 | 0.0222 | 1.0 | none comparable |
| anchor-mfg.com | industries | OK | 2/0 | 1.0 | 0.0 | 0.8268 | none comparable |
| anchor-mfg.com | material_caps | OK | 2/0 | 1.0 | 0.0 | 1.0 | none comparable |
| anchor-mfg.com | process_caps | OK | 2/0 | 1.0 | 0.0156 | 0.8971 | none comparable |
| anchor-mfg.com | products | RED — unparseable_windows:1 | 2/0 | not gated (23 candidates) | 0.0167 | 1.0 | none comparable |
| blackadvtech.com | conformity_attestations | OK | 10/0 | 1.0 | 0.0 | 0.9371 | none comparable |
| blackadvtech.com | equipments | RED — confirmed_recall_misses:1 | 10/0 | 0.8 | 0.0 | 0.9054 | none comparable |
| blackadvtech.com | industries | RED — confirmed_recall_misses:2 | 10/0 | 0.8182 | 0.0 | 0.4941 | none comparable |
| blackadvtech.com | material_caps | RED — confirmed_recall_misses:1, unparseable_windows:1 | 10/0 | 0.9231 | 0.0 | 0.9312 | none comparable |
| blackadvtech.com | process_caps | RED — confirmed_recall_misses:1 | 10/0 | 0.9697 | 0.0072 | 0.7476 | none comparable |
| blackadvtech.com | products | RED — confirmed_recall_misses:4 | 10/0 | 0.8333 | 0.0 | 0.7198 | none comparable |
| decimal.net | conformity_attestations | OK | 7/0 | 1.0 | 0.0 | 0.9921 | none comparable |
| decimal.net | equipments | RED — confirmed_recall_misses:1 | 7/0 | 0.9412 | 0.0 | 0.5786 | none comparable |
| decimal.net | industries | OK | 7/0 | 1.0 | 0.0 | 0.9402 | none comparable |
| decimal.net | material_caps | RED — confirmed_recall_misses:2 | 7/0 | 0.9623 | 0.0 | 0.9365 | none comparable |
| decimal.net | process_caps | RED — confirmed_recall_misses:7 | 7/0 | 0.9417 | 0.0049 | 0.8778 | none comparable |
| decimal.net | products | RED — confirmed_recall_misses:1 | 7/0 | 0.9859 | 0.0013 | 0.7995 | none comparable |
| fzemanufacturing.com | conformity_attestations | OK | 10/0 | 1.0 | 0.0 | 0.81 | none comparable |
| fzemanufacturing.com | equipments | OK | 10/0 | 1.0 | 0.0052 | 0.4506 | none comparable |
| fzemanufacturing.com | industries | OK | 10/0 | 1.0 | 0.0 | 0.6404 | none comparable |
| fzemanufacturing.com | material_caps | RED — confirmed_recall_misses:1 | 10/0 | 0.9859 | 0.003 | 0.7543 | none comparable |
| fzemanufacturing.com | process_caps | RED — confirmed_recall_misses:5 | 10/0 | 0.9479 | 0.0012 | 0.8211 | none comparable |
| fzemanufacturing.com | products | RED — confirmed_recall_misses:2 | 10/0 | 0.9615 | 0.0 | 0.5696 | none comparable |
| howcogroup.com | conformity_attestations | RED — not_in_window_rate:0.0933>0.02 | 8/0 | 1.0 | 0.0933 | 0.5807 | none comparable |
| howcogroup.com | equipments | OK | 8/0 | 1.0 | 0.0 | 1.0 | none comparable |
| howcogroup.com | industries | RED — confirmed_recall_misses:9 | 8/0 | 0.6786 | 0.0 | 0.6103 | none comparable |
| howcogroup.com | material_caps | RED — confirmed_recall_misses:5 | 8/0 | 0.8958 | 0.0118 | 0.6667 | none comparable |
| howcogroup.com | process_caps | RED — confirmed_recall_misses:2 | 8/0 | 0.9459 | 0.0 | 0.8347 | none comparable |
| howcogroup.com | products | RED — confirmed_recall_misses:1 | 8/0 | 0.9867 | 0.0025 | 0.785 | none comparable |
| lucasmilhaupt.com | conformity_attestations | OK | 2/0 | not gated (0 candidates) | 0.0 | 1.0 | none comparable |
| lucasmilhaupt.com | equipments | OK | 2/0 | not gated (0 candidates) | 0.0 | 1.0 | none comparable |
| lucasmilhaupt.com | industries | RED — confirmed_recall_misses:1 | 2/0 | 0.5 | 0.0 | 0.8972 | none comparable |
| lucasmilhaupt.com | material_caps | OK | 2/0 | not gated (15 candidates) | 0.0 | 0.9163 | none comparable |
| lucasmilhaupt.com | process_caps | OK | 2/0 | 1.0 | 0.0154 | 0.7949 | none comparable |
| lucasmilhaupt.com | products | OK | 2/0 | 1.0 | 0.0 | 0.8848 | none comparable |
| mathewsco.com | conformity_attestations | OK | 2/0 | 1.0 | 0.0 | 1.0 | none comparable |
| mathewsco.com | equipments | OK | 2/0 | 1.0 | 0.0 | 1.0 | none comparable |
| mathewsco.com | industries | RED — confirmed_recall_misses:1 | 2/0 | 0.9756 | 0.0 | 0.8727 | none comparable |
| mathewsco.com | material_caps | RED — confirmed_recall_misses:1 | 2/0 | 0.9762 | 0.0 | 1.0 | none comparable |
| mathewsco.com | process_caps | RED — confirmed_recall_misses:9 | 2/0 | 0.8594 | 0.0096 | 0.9017 | none comparable |
| mathewsco.com | products | RED — confirmed_recall_misses:5, not_in_window_rate:0.037>0.02 | 2/0 | 0.9219 | 0.037 | 0.9046 | none comparable |
| med-tekinc.com | conformity_attestations | OK | 1/0 | not gated (0 candidates) | 0.0 | 1.0 | none comparable |
| med-tekinc.com | equipments | OK | 1/0 | 1.0 | 0.0 | 1.0 | none comparable |
| med-tekinc.com | industries | OK | 1/0 | 1.0 | 0.0 | 1.0 | none comparable |
| med-tekinc.com | material_caps | RED — confirmed_recall_misses:2 | 1/0 | 0.7778 | 0.0 | 1.0 | none comparable |
| med-tekinc.com | process_caps | OK | 1/0 | 1.0 | 0.0 | 1.0 | none comparable |
| med-tekinc.com | products | OK | 1/0 | not gated (0 candidates) | 0.0 | 1.0 | none comparable |
| pradeepmetals.com | conformity_attestations | OK | 10/0 | 1.0 | 0.0 | 0.726 | none comparable |
| pradeepmetals.com | equipments | RED — confirmed_recall_misses:3 | 10/0 | 0.9143 | 0.0 | 0.9571 | none comparable |
| pradeepmetals.com | industries | OK | 10/0 | 1.0 | 0.0 | 0.5478 | none comparable |
| pradeepmetals.com | material_caps | RED — confirmed_recall_misses:2 | 10/0 | 0.963 | 0.0 | 0.7048 | none comparable |
| pradeepmetals.com | process_caps | RED — confirmed_recall_misses:5 | 10/0 | 0.9107 | 0.0 | 0.7792 | none comparable |
| pradeepmetals.com | products | RED — confirmed_recall_misses:5, unparseable_windows:1 | 10/0 | 0.8 | 0.0 | 0.8492 | none comparable |
| steelcraft.com | conformity_attestations | RED — confirmed_recall_misses:2 | 6/0 | 0.9583 | 0.0 | 0.7824 | none comparable |
| steelcraft.com | equipments | OK | 6/0 | not gated (0 candidates) | 0.013 | 0.8105 | none comparable |
| steelcraft.com | industries | RED — confirmed_recall_misses:2 | 6/0 | 0.8462 | 0.0119 | 0.6314 | none comparable |
| steelcraft.com | material_caps | RED — confirmed_recall_misses:1 | 6/0 | 0.9167 | 0.0097 | 0.8686 | none comparable |
| steelcraft.com | process_caps | OK | 6/0 | 1.0 | 0.0122 | 0.6709 | none comparable |
| steelcraft.com | products | RED — confirmed_recall_misses:1 | 6/0 | 0.9677 | 0.0042 | 0.7549 | none comparable |
| superiortech.org | conformity_attestations | OK | 2/0 | 1.0 | 0.0 | 1.0 | none comparable |
| superiortech.org | equipments | RED — confirmed_recall_misses:9 | 2/0 | 0.7 | 0.0 | 0.9586 | none comparable |
| superiortech.org | industries | OK | 2/0 | 1.0 | 0.0 | 0.956 | none comparable |
| superiortech.org | material_caps | RED — confirmed_recall_misses:1 | 2/0 | 0.9444 | 0.0 | 0.9643 | none comparable |
| superiortech.org | process_caps | RED — confirmed_recall_misses:2 | 2/0 | 0.978 | 0.0137 | 0.9272 | none comparable |
| superiortech.org | products | RED — confirmed_recall_misses:2 | 2/0 | 0.9444 | 0.0031 | 0.8959 | none comparable |
| tanfel.com | conformity_attestations | OK | 9/0 | 1.0 | 0.0 | 0.9912 | none comparable |
| tanfel.com | equipments | RED — confirmed_recall_misses:4 | 9/0 | 0.7778 | 0.0 | 0.4876 | none comparable |
| tanfel.com | industries | RED — confirmed_recall_misses:2 | 9/0 | 0.9 | 0.0041 | 0.5673 | none comparable |
| tanfel.com | material_caps | RED — confirmed_recall_misses:2 | 9/0 | 0.974 | 0.0 | 0.8274 | none comparable |
| tanfel.com | process_caps | RED — confirmed_recall_misses:1 | 9/0 | 0.987 | 0.0 | 0.8341 | none comparable |
| tanfel.com | products | RED — confirmed_recall_misses:2 | 9/0 | 0.9655 | 0.002 | 0.8942 | none comparable |
| taylordunn.com | conformity_attestations | OK | 6/0 | not gated (4 candidates) | 0.0 | 1.0 | none comparable |
| taylordunn.com | equipments | OK | 6/0 | not gated (1 candidates) | 0.0 | 0.8889 | none comparable |
| taylordunn.com | industries | RED — unparseable_windows:1 | 6/0 | 1.0 | 0.0 | 0.8308 | none comparable |
| taylordunn.com | material_caps | OK | 6/0 | not gated (12 candidates) | 0.0 | 0.8489 | none comparable |
| taylordunn.com | process_caps | OK | 6/0 | not gated (4 candidates) | 0.0 | 0.9062 | none comparable |
| taylordunn.com | products | OK | 6/0 | 1.0 | 0.0 | 0.8417 | none comparable |

## Cross-field collisions

Distinct forms this field returned that some OTHER field also returned
for the same subject. Mechanical, so no agreement floor applies. A
collision is not automatically a defect — see `_collision_rollup`.

| field | distinct forms | claimed by a sibling | share |
|---|---|---|---|
| equipments | 1607 | 1426 | 88.7% |
| products | 6486 | 3212 | 49.5% |
| material_caps | 1655 | 780 | 47.1% |
| process_caps | 4112 | 1813 | 44.1% |
| industries | 1505 | 523 | 34.8% |
| conformity_attestations | 594 | 103 | 17.3% |
| **ALL** | **15959** | **7857** | **49.2%** |

| field pair | shared | Jaccard |
|---|---|---|
| process_caps ↔ products | 1505 | 16.6% |
| equipments ↔ products | 1311 | 19.3% |
| material_caps ↔ products | 700 | 9.4% |
| equipments ↔ process_caps | 491 | 9.4% |
| industries ↔ products | 387 | 5.1% |
| industries ↔ process_caps | 272 | 5.1% |
| material_caps ↔ process_caps | 200 | 3.6% |
| equipments ↔ industries | 111 | 3.7% |
