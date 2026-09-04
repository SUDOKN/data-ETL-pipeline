# Search-stage eval — mechanical pass, run 20260904T230543

Generated 2026-09-04T23:21:45.467766+00:00. Judged metrics
(precision census, recall census, wrong-actor, agreement) are produced
by the RUNBOOK's agent protocol and merged into these scorecards by the
assistant — this file alone is NOT the full evaluation.

| subject | field | verdict | live/replayed | recall (confirmed) | not-in-window | consistency | Δ vs baseline |
|---|---|---|---|---|---|---|---|
| 101machine.com | conformity_attestations | OK | 0/1 | not gated (0 candidates) | — | — | no change |
| 101machine.com | equipments | OK | 1/0 | 1.0 | 0.0 | 1.0 | none comparable |
| 101machine.com | industries | OK | 0/1 | 1.0 | 0.0 | 1.0 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| 101machine.com | material_caps | OK | 0/1 | 1.0 | 0.0 | 1.0 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| 101machine.com | process_caps | OK | 1/0 | 1.0 | 0.0 | 1.0 | none comparable |
| 101machine.com | products | OK | 1/0 | 1.0 | 0.0 | 1.0 | none comparable |
| ableengineering.com | conformity_attestations | RED — not_in_window_rate:0.0247>0.02 | 0/11 | 1.0 | 0.0247 | 0.8638 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| ableengineering.com | equipments | OK | 11/0 | 1.0 | 0.0 | 0.6336 | none comparable |
| ableengineering.com | industries | OK | 0/11 | 1.0 | 0.0 | 0.4494 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| ableengineering.com | material_caps | OK | 0/11 | 1.0 | 0.0 | 0.88 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| ableengineering.com | process_caps | OK | 11/0 | 1.0 | 0.0 | 0.6973 | none comparable |
| ableengineering.com | products | OK | 11/0 | 1.0 | 0.0172 | 0.9105 | none comparable |
| acimachine.com | conformity_attestations | OK | 0/8 | not gated (2 candidates) | 0.0 | 1.0 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0 |
| acimachine.com | equipments | OK | 8/0 | not gated (3 candidates) | 0.0 | 0.9728 | none comparable |
| acimachine.com | industries | OK | 0/8 | not gated (0 candidates) | 0.0 | 0.4618 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0 |
| acimachine.com | material_caps | OK | 0/8 | not gated (7 candidates) | 0.0 | 1.0 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; sweep.floor_recall +0.0 |
| acimachine.com | process_caps | RED — confirmed_recall_misses:1 | 8/0 | 0.8889 | 0.0 | 0.8364 | none comparable |
| acimachine.com | products | OK | 8/0 | 1.0 | 0.0 | 0.9261 | none comparable |
| agstech.net | conformity_attestations | RED — confirmed_recall_misses:2 | 0/9 | 0.913 | 0.0 | 0.9759 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| agstech.net | equipments | RED — confirmed_recall_misses:1 | 9/0 | 0.9667 | 0.0 | 0.811 | none comparable |
| agstech.net | industries | RED — confirmed_recall_misses:10 | 0/9 | 0.7917 | 0.0 | 0.744 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| agstech.net | material_caps | RED — confirmed_recall_misses:8 | 0/9 | 0.84 | 0.0 | 0.883 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.1; sweep.floor_recall +0.0 |
| agstech.net | process_caps | RED — confirmed_recall_misses:12, not_in_window_rate:0.0277>0.02 | 9/0 | 0.9184 | 0.0277 | 0.8201 | none comparable |
| agstech.net | products | RED — confirmed_recall_misses:17 | 9/0 | 0.9496 | 0.008 | 0.8492 | none comparable |
| alecmfg.com | conformity_attestations | OK | 0/8 | 1.0 | 0.0 | 0.9604 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| alecmfg.com | equipments | OK | 8/0 | 1.0 | 0.0 | 0.8358 | none comparable |
| alecmfg.com | industries | OK | 0/8 | 1.0 | 0.0042 | 0.6556 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| alecmfg.com | material_caps | OK | 0/8 | 1.0 | 0.0 | 0.8359 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| alecmfg.com | process_caps | RED — confirmed_recall_misses:2 | 8/0 | 0.9474 | 0.0064 | 0.7801 | none comparable |
| alecmfg.com | products | OK | 8/0 | 1.0 | 0.0 | 0.7122 | none comparable |
| anchor-mfg.com | conformity_attestations | RED — not_in_window_rate:0.0476>0.02 | 0/2 | not gated (8 candidates) | 0.0476 | 1.0 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; sweep.floor_recall +0.0 |
| anchor-mfg.com | equipments | RED — not_in_window_rate:0.0345>0.02 | 2/0 | 1.0 | 0.0345 | 1.0 | none comparable |
| anchor-mfg.com | industries | OK | 0/2 | 1.0 | 0.0 | 0.8478 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| anchor-mfg.com | material_caps | OK | 0/2 | 1.0 | 0.0 | 1.0 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| anchor-mfg.com | process_caps | OK | 2/0 | 1.0 | 0.0 | 0.9542 | none comparable |
| anchor-mfg.com | products | OK | 2/0 | 1.0 | 0.0122 | 0.9058 | none comparable |
| blackadvtech.com | conformity_attestations | OK | 0/10 | 1.0 | 0.0 | 0.9337 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| blackadvtech.com | equipments | RED — confirmed_recall_misses:1 | 10/0 | 0.8 | 0.0 | 0.6816 | none comparable |
| blackadvtech.com | industries | RED — confirmed_recall_misses:1 | 0/10 | 0.9091 | 0.0064 | 0.4992 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| blackadvtech.com | material_caps | OK | 0/10 | 1.0 | 0.0067 | 0.9335 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| blackadvtech.com | process_caps | RED — confirmed_recall_misses:2 | 10/0 | 0.9394 | 0.0092 | 0.7695 | none comparable |
| blackadvtech.com | products | RED — confirmed_recall_misses:1 | 10/0 | 0.9583 | 0.0015 | 0.7795 | none comparable |
| decimal.net | conformity_attestations | OK | 0/7 | 1.0 | 0.0 | 0.989 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| decimal.net | equipments | OK | 7/0 | 1.0 | 0.0 | 0.5767 | none comparable |
| decimal.net | industries | OK | 0/7 | 1.0 | 0.0 | 0.9155 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| decimal.net | material_caps | OK | 0/7 | 1.0 | 0.0027 | 0.9131 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| decimal.net | process_caps | RED — confirmed_recall_misses:1 | 7/0 | 0.9917 | 0.0026 | 0.844 | none comparable |
| decimal.net | products | OK | 7/0 | 1.0 | 0.0012 | 0.8382 | none comparable |
| fzemanufacturing.com | conformity_attestations | OK | 0/10 | 1.0 | 0.0 | 0.8696 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| fzemanufacturing.com | equipments | OK | 10/0 | 1.0 | 0.0039 | 0.4644 | none comparable |
| fzemanufacturing.com | industries | OK | 0/10 | 1.0 | 0.0 | 0.6197 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| fzemanufacturing.com | material_caps | RED — confirmed_recall_misses:1 | 0/10 | 0.9859 | 0.0029 | 0.6466 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| fzemanufacturing.com | process_caps | RED — confirmed_recall_misses:2 | 10/0 | 0.9792 | 0.0055 | 0.8444 | none comparable |
| fzemanufacturing.com | products | RED — confirmed_recall_misses:2 | 10/0 | 0.9615 | 0.0007 | 0.5691 | none comparable |
| howcogroup.com | conformity_attestations | RED — not_in_window_rate:0.0886>0.02 | 0/8 | 1.0 | 0.0886 | 0.5907 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| howcogroup.com | equipments | OK | 8/0 | 1.0 | 0.0 | 1.0 | none comparable |
| howcogroup.com | industries | RED — confirmed_recall_misses:3 | 0/8 | 0.8929 | 0.0 | 0.7333 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| howcogroup.com | material_caps | RED — confirmed_recall_misses:3 | 0/8 | 0.9375 | 0.0178 | 0.7245 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0208; sweep.floor_recall +0.0 |
| howcogroup.com | process_caps | RED — confirmed_recall_misses:1 | 8/0 | 0.973 | 0.0 | 0.9182 | none comparable |
| howcogroup.com | products | RED — confirmed_recall_misses:1 | 8/0 | 0.9867 | 0.0127 | 0.7638 | none comparable |
| lucasmilhaupt.com | conformity_attestations | OK | 0/2 | 1.0 | 0.0 | 1.0 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; sweep.floor_recall +0.0 |
| lucasmilhaupt.com | equipments | OK | 2/0 | not gated (0 candidates) | 0.0 | 1.0 | none comparable |
| lucasmilhaupt.com | industries | RED — confirmed_recall_misses:1 | 0/2 | 0.5 | 0.0 | 0.9174 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| lucasmilhaupt.com | material_caps | OK | 0/2 | 1.0 | 0.0 | 0.9183 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; sweep.floor_recall +0.0 |
| lucasmilhaupt.com | process_caps | OK | 2/0 | 1.0 | 0.02 | 0.8087 | none comparable |
| lucasmilhaupt.com | products | RED — not_in_window_rate:0.0263>0.02 | 2/0 | 1.0 | 0.0263 | 0.8557 | none comparable |
| mathewsco.com | conformity_attestations | OK | 0/2 | 1.0 | 0.0 | 1.0 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| mathewsco.com | equipments | OK | 2/0 | 1.0 | 0.0 | 1.0 | none comparable |
| mathewsco.com | industries | RED — confirmed_recall_misses:1 | 0/2 | 0.9756 | 0.0 | 0.8779 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| mathewsco.com | material_caps | OK | 0/2 | 1.0 | 0.0 | 1.0 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| mathewsco.com | process_caps | RED — confirmed_recall_misses:7 | 2/0 | 0.8906 | 0.0 | 0.9646 | none comparable |
| mathewsco.com | products | RED — confirmed_recall_misses:5, not_in_window_rate:0.0319>0.02 | 2/0 | 0.9219 | 0.0319 | 0.8078 | none comparable |
| med-tekinc.com | conformity_attestations | OK | 0/1 | not gated (0 candidates) | 0.0 | 1.0 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; sweep.floor_recall +0.0 |
| med-tekinc.com | equipments | OK | 1/0 | 1.0 | 0.0 | 1.0 | none comparable |
| med-tekinc.com | industries | OK | 0/1 | 1.0 | 0.0 | 1.0 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0 |
| med-tekinc.com | material_caps | RED — confirmed_recall_misses:2 | 0/1 | 0.7778 | 0.0 | 1.0 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| med-tekinc.com | process_caps | OK | 1/0 | 1.0 | 0.0 | 1.0 | none comparable |
| med-tekinc.com | products | OK | 1/0 | not gated (0 candidates) | 0.0 | 1.0 | none comparable |
| pradeepmetals.com | conformity_attestations | OK | 0/10 | 1.0 | 0.0 | 0.7647 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| pradeepmetals.com | equipments | RED — confirmed_recall_misses:3 | 10/0 | 0.9143 | 0.0 | 0.9444 | none comparable |
| pradeepmetals.com | industries | OK | 0/10 | 1.0 | 0.0 | 0.4903 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| pradeepmetals.com | material_caps | RED — confirmed_recall_misses:2 | 0/10 | 0.963 | 0.0 | 0.7724 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| pradeepmetals.com | process_caps | RED — confirmed_recall_misses:3 | 10/0 | 0.9464 | 0.0083 | 0.8333 | none comparable |
| pradeepmetals.com | products | RED — confirmed_recall_misses:3, unparseable_windows:1 | 10/0 | 0.88 | 0.0075 | 0.6383 | none comparable |
| steelcraft.com | conformity_attestations | RED — confirmed_recall_misses:1 | 0/6 | 0.9792 | 0.0049 | 0.7476 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| steelcraft.com | equipments | RED — not_in_window_rate:0.0339>0.02 | 6/0 | not gated (0 candidates) | 0.0339 | 0.878 | none comparable |
| steelcraft.com | industries | OK | 0/6 | 1.0 | 0.0085 | 0.6744 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| steelcraft.com | material_caps | OK | 0/6 | 1.0 | 0.0079 | 0.89 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| steelcraft.com | process_caps | RED — not_in_window_rate:0.0208>0.02 | 6/0 | 1.0 | 0.0208 | 0.6426 | none comparable |
| steelcraft.com | products | OK | 6/0 | 1.0 | 0.0022 | 0.8007 | none comparable |
| superiortech.org | conformity_attestations | OK | 0/2 | 1.0 | 0.0 | 1.0 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| superiortech.org | equipments | RED — confirmed_recall_misses:6 | 2/0 | 0.8 | 0.0074 | 0.9548 | none comparable |
| superiortech.org | industries | OK | 0/2 | 1.0 | 0.0 | 0.956 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| superiortech.org | material_caps | RED — confirmed_recall_misses:1 | 0/2 | 0.9444 | 0.0 | 0.9677 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| superiortech.org | process_caps | RED — confirmed_recall_misses:1 | 2/0 | 0.989 | 0.011 | 0.9836 | none comparable |
| superiortech.org | products | RED — confirmed_recall_misses:2 | 2/0 | 0.9444 | 0.0 | 0.9399 | none comparable |
| tanfel.com | conformity_attestations | OK | 0/9 | 1.0 | 0.0 | 0.9912 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| tanfel.com | equipments | RED — confirmed_recall_misses:1 | 9/0 | 0.9444 | 0.0 | 0.4396 | none comparable |
| tanfel.com | industries | RED — confirmed_recall_misses:1 | 0/9 | 0.95 | 0.0034 | 0.6485 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| tanfel.com | material_caps | RED — confirmed_recall_misses:1 | 0/9 | 0.987 | 0.0 | 0.7554 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| tanfel.com | process_caps | OK | 9/0 | 1.0 | 0.0 | 0.8948 | none comparable |
| tanfel.com | products | RED — confirmed_recall_misses:2 | 9/0 | 0.9661 | 0.0044 | 0.8503 | none comparable |
| taylordunn.com | conformity_attestations | OK | 0/6 | 1.0 | 0.0 | 1.0 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; sweep.floor_recall +0.0 |
| taylordunn.com | equipments | OK | 6/0 | not gated (0 candidates) | 0.0 | 0.8211 | none comparable |
| taylordunn.com | industries | OK | 0/6 | 1.0 | 0.0 | 0.8025 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| taylordunn.com | material_caps | OK | 0/6 | 1.0 | 0.0 | 0.8452 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; sweep.floor_recall +0.0 |
| taylordunn.com | process_caps | RED — not_in_window_rate:0.0345>0.02 | 6/0 | not gated (0 candidates) | 0.0345 | 0.9062 | none comparable |
| taylordunn.com | products | OK | 6/0 | 1.0 | 0.0 | 0.869 | none comparable |

## Cross-field collisions

Distinct forms this field returned that some OTHER field also returned
for the same subject. Mechanical, so no agreement floor applies. A
collision is not automatically a defect — see `_collision_rollup`.

| field | distinct forms | claimed by a sibling | share |
|---|---|---|---|
| equipments | 1853 | 1688 | 91.1% |
| material_caps | 1883 | 1072 | 56.9% |
| products | 7675 | 4051 | 52.8% |
| process_caps | 4960 | 2304 | 46.5% |
| industries | 1800 | 743 | 41.3% |
| conformity_attestations | 672 | 148 | 22.0% |
| **ALL** | **18843** | **10006** | **53.1%** |

| field pair | shared | Jaccard |
|---|---|---|
| process_caps ↔ products | 1962 | 18.4% |
| equipments ↔ products | 1546 | 19.4% |
| material_caps ↔ products | 992 | 11.6% |
| equipments ↔ process_caps | 675 | 11.0% |
| industries ↔ products | 569 | 6.4% |
| industries ↔ process_caps | 393 | 6.2% |
| material_caps ↔ process_caps | 281 | 4.3% |
| equipments ↔ industries | 175 | 5.0% |
