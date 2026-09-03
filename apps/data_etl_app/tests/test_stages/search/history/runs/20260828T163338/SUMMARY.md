# Search-stage eval — mechanical pass, run 20260828T163338

Generated 2026-08-29T02:16:10.813694+00:00. Judged metrics
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
| 101machine.com | products | OK | 1/0 | not gated (0 candidates) | 0.0 | 1.0 | none comparable |
| ableengineering.com | conformity_attestations | RED — confirmed_recall_misses:3 | 9/0 | 0.85 | 0.0 | 0.9778 | none comparable |
| ableengineering.com | equipments | OK | 9/0 | 1.0 | 0.0 | 1.0 | none comparable |
| ableengineering.com | industries | RED — confirmed_recall_misses:1 | 9/0 | 0.8571 | 0.0 | 0.6392 | none comparable |
| ableengineering.com | material_caps | OK | 9/0 | 1.0 | 0.0 | 0.843 | none comparable |
| ableengineering.com | process_caps | OK | 9/0 | 1.0 | 0.0145 | 0.787 | none comparable |
| ableengineering.com | products | RED — confirmed_recall_misses:1 | 9/0 | 0.9524 | 0.0035 | 0.6039 | none comparable |
| acimachine.com | conformity_attestations | OK | 8/0 | not gated (0 candidates) | — | — | none comparable |
| acimachine.com | equipments | RED — confirmed_recall_misses:3, unparseable_windows:1 | 8/0 | 0.0 | 0.0 | 0.8807 | none comparable |
| acimachine.com | industries | OK | 8/0 | not gated (0 candidates) | 0.0 | 0.7513 | none comparable |
| acimachine.com | material_caps | OK | 8/0 | not gated (0 candidates) | 0.0 | 0.7907 | none comparable |
| acimachine.com | process_caps | RED — confirmed_recall_misses:1 | 8/0 | 0.8889 | 0.0 | 0.8534 | none comparable |
| acimachine.com | products | RED — unparseable_windows:2 | 8/0 | not gated (0 candidates) | 0.0052 | 0.9453 | none comparable |
| agstech.net | conformity_attestations | RED — confirmed_recall_misses:1 | 10/0 | 0.9412 | 0.0 | 0.9239 | none comparable |
| agstech.net | equipments | RED — confirmed_recall_misses:3 | 10/0 | 0.7857 | 0.0 | 0.5825 | none comparable |
| agstech.net | industries | RED — confirmed_recall_misses:7 | 10/0 | 0.7083 | 0.0067 | 0.7302 | none comparable |
| agstech.net | material_caps | RED — confirmed_recall_misses:9 | 10/0 | 0.8125 | 0.0 | 0.8518 | none comparable |
| agstech.net | process_caps | RED — confirmed_recall_misses:7, repetition_loops:1 | 10/0 | 0.965 | 0.0105 | 0.8346 | none comparable |
| agstech.net | products | RED — confirmed_recall_misses:23, not_in_window_rate:0.0209>0.02 | 10/0 | 0.9094 | 0.0209 | 0.6815 | none comparable |
| alecmfg.com | conformity_attestations | RED — confirmed_recall_misses:1 | 0/7 | 0.9286 | 0.0 | 0.968 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| alecmfg.com | equipments | OK | 0/7 | 1.0 | 0.0 | 0.8706 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| alecmfg.com | industries | OK | 0/7 | 1.0 | 0.0071 | 0.5622 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| alecmfg.com | material_caps | OK | 0/7 | 1.0 | 0.0 | 0.8563 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| alecmfg.com | process_caps | RED — confirmed_recall_misses:4 | 0/7 | 0.8947 | 0.0035 | 0.7836 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall -0.0264; sweep.floor_recall +0.0 |
| alecmfg.com | products | OK | 0/7 | 1.0 | 0.0023 | 0.7698 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0 |
| anchor-mfg.com | conformity_attestations | OK | 2/0 | 1.0 | 0.0 | 0.9524 | none comparable |
| anchor-mfg.com | equipments | RED — confirmed_recall_misses:1 | 2/0 | 0.875 | 0.0 | 0.9444 | none comparable |
| anchor-mfg.com | industries | OK | 2/0 | 1.0 | 0.0 | 0.8725 | none comparable |
| anchor-mfg.com | material_caps | OK | 2/0 | 1.0 | 0.0 | 0.9697 | none comparable |
| anchor-mfg.com | process_caps | OK | 2/0 | 1.0 | 0.0 | 0.9124 | none comparable |
| anchor-mfg.com | products | OK | 2/0 | 1.0 | 0.0 | 0.9035 | none comparable |
| austinelectricservices.com | conformity_attestations | OK | 6/0 | 1.0 | 0.0 | 0.878 | none comparable |
| austinelectricservices.com | equipments | OK | 6/0 | not gated (0 candidates) | 0.0 | 0.6591 | none comparable |
| austinelectricservices.com | industries | OK | 6/0 | 1.0 | 0.0081 | 0.6 | none comparable |
| austinelectricservices.com | material_caps | OK | 6/0 | not gated (0 candidates) | 0.0 | 0.6111 | none comparable |
| austinelectricservices.com | process_caps | RED — not_in_window_rate:0.0238>0.02 | 6/0 | not gated (0 candidates) | 0.0238 | 0.8507 | none comparable |
| austinelectricservices.com | products | OK | 6/0 | 1.0 | 0.0 | 0.6586 | none comparable |
| blackadvtech.com | conformity_attestations | RED — confirmed_recall_misses:1 | 10/0 | 0.9 | 0.0 | 0.9643 | none comparable |
| blackadvtech.com | equipments | RED — confirmed_recall_misses:6 | 10/0 | 0.8889 | 0.0043 | 0.7832 | none comparable |
| blackadvtech.com | industries | RED — confirmed_recall_misses:2 | 10/0 | 0.913 | 0.0 | 0.5696 | none comparable |
| blackadvtech.com | material_caps | RED — confirmed_recall_misses:6 | 10/0 | 0.8235 | 0.0066 | 0.8574 | none comparable |
| blackadvtech.com | process_caps | RED — confirmed_recall_misses:14 | 10/0 | 0.791 | 0.009 | 0.8127 | none comparable |
| blackadvtech.com | products | RED — confirmed_recall_misses:3 | 10/0 | 0.9286 | 0.0044 | 0.7226 | none comparable |
| decimal.net | conformity_attestations | RED — confirmed_recall_misses:1 | 6/0 | 0.96 | 0.0 | 0.9958 | none comparable |
| decimal.net | equipments | OK | 6/0 | 1.0 | 0.0 | 0.6527 | none comparable |
| decimal.net | industries | RED — confirmed_recall_misses:2 | 6/0 | 0.9394 | 0.0 | 0.7683 | none comparable |
| decimal.net | material_caps | RED — confirmed_recall_misses:7 | 6/0 | 0.8814 | 0.0 | 0.8527 | none comparable |
| decimal.net | process_caps | RED — confirmed_recall_misses:1 | 6/0 | 0.9919 | 0.0 | 0.8307 | none comparable |
| decimal.net | products | OK | 6/0 | 1.0 | 0.0 | 0.8855 | none comparable |
| fzemanufacturing.com | conformity_attestations | RED — confirmed_recall_misses:2 | 10/0 | 0.8182 | 0.0 | 0.9162 | none comparable |
| fzemanufacturing.com | equipments | RED — confirmed_recall_misses:2 | 10/0 | 0.9556 | 0.0059 | 0.5262 | none comparable |
| fzemanufacturing.com | industries | OK | 10/0 | 1.0 | 0.0 | 0.6755 | none comparable |
| fzemanufacturing.com | material_caps | RED — confirmed_recall_misses:5 | 10/0 | 0.9296 | 0.0 | 0.7766 | none comparable |
| fzemanufacturing.com | process_caps | RED — confirmed_recall_misses:9, unparseable_windows:2 | 10/0 | 0.9109 | 0.0072 | 0.8035 | none comparable |
| fzemanufacturing.com | products | RED — confirmed_recall_misses:8, unparseable_windows:1 | 10/0 | 0.8788 | 0.0036 | 0.7658 | none comparable |
| howcogroup.com | conformity_attestations | RED — confirmed_recall_misses:1, not_in_window_rate:0.0625>0.02 | 10/0 | 0.9737 | 0.0625 | 0.4491 | none comparable |
| howcogroup.com | equipments | RED — not_in_window_rate:0.025>0.02 | 10/0 | 1.0 | 0.025 | 0.9815 | none comparable |
| howcogroup.com | industries | RED — confirmed_recall_misses:5 | 10/0 | 0.8571 | 0.0 | 0.6608 | none comparable |
| howcogroup.com | material_caps | RED — confirmed_recall_misses:8 | 10/0 | 0.873 | 0.0164 | 0.51 | none comparable |
| howcogroup.com | process_caps | RED — confirmed_recall_misses:2 | 10/0 | 0.9701 | 0.0032 | 0.7815 | none comparable |
| howcogroup.com | products | RED — confirmed_recall_misses:3 | 10/0 | 0.9655 | 0.0 | 0.7483 | none comparable |
| lucasmilhaupt.com | conformity_attestations | RED — confirmed_recall_misses:1 | 7/0 | 0.9167 | 0.0 | 0.9565 | none comparable |
| lucasmilhaupt.com | equipments | RED — confirmed_recall_misses:4 | 7/0 | 0.5 | 0.0 | 0.8219 | none comparable |
| lucasmilhaupt.com | industries | RED — confirmed_recall_misses:3 | 7/0 | 0.875 | 0.0 | 0.8046 | none comparable |
| lucasmilhaupt.com | material_caps | RED — confirmed_recall_misses:9 | 7/0 | 0.8696 | 0.0 | 0.8116 | none comparable |
| lucasmilhaupt.com | process_caps | RED — confirmed_recall_misses:6 | 7/0 | 0.9211 | 0.0151 | 0.7935 | none comparable |
| lucasmilhaupt.com | products | RED — confirmed_recall_misses:1, unparseable_windows:1 | 7/0 | 0.9855 | 0.0077 | 0.7962 | none comparable |
| mathewsco.com | conformity_attestations | RED — confirmed_recall_misses:1 | 4/0 | 0.9444 | 0.0 | 0.625 | none comparable |
| mathewsco.com | equipments | OK | 4/0 | 1.0 | 0.0 | 1.0 | none comparable |
| mathewsco.com | industries | OK | 4/0 | 1.0 | 0.0 | 0.9376 | none comparable |
| mathewsco.com | material_caps | OK | 4/0 | 1.0 | 0.0 | 0.9932 | none comparable |
| mathewsco.com | process_caps | RED — confirmed_recall_misses:7 | 4/0 | 0.8906 | 0.0 | 0.952 | none comparable |
| mathewsco.com | products | RED — confirmed_recall_misses:6 | 4/0 | 0.9062 | 0.0067 | 0.7577 | none comparable |
| med-tekinc.com | conformity_attestations | OK | 1/0 | not gated (0 candidates) | 0.0 | 1.0 | none comparable |
| med-tekinc.com | equipments | OK | 1/0 | 1.0 | 0.0 | 1.0 | none comparable |
| med-tekinc.com | industries | RED — not_in_window_rate:0.125>0.02 | 1/0 | 1.0 | 0.125 | 1.0 | none comparable |
| med-tekinc.com | material_caps | RED — confirmed_recall_misses:2 | 1/0 | 0.7778 | 0.0 | 1.0 | none comparable |
| med-tekinc.com | process_caps | RED — confirmed_recall_misses:3 | 1/0 | 0.9062 | 0.0 | 1.0 | none comparable |
| med-tekinc.com | products | OK | 1/0 | not gated (0 candidates) | 0.0 | 1.0 | none comparable |
| pradeepmetals.com | conformity_attestations | RED — confirmed_recall_misses:2 | 8/0 | 0.9048 | 0.0 | 0.4491 | none comparable |
| pradeepmetals.com | equipments | RED — confirmed_recall_misses:4 | 8/0 | 0.8621 | 0.0 | 0.96 | none comparable |
| pradeepmetals.com | industries | RED — confirmed_recall_misses:3 | 8/0 | 0.8889 | 0.0118 | 0.362 | none comparable |
| pradeepmetals.com | material_caps | RED — confirmed_recall_misses:1 | 8/0 | 0.9818 | 0.0 | 0.6834 | none comparable |
| pradeepmetals.com | process_caps | RED — confirmed_recall_misses:6 | 8/0 | 0.8605 | 0.0 | 0.6578 | none comparable |
| pradeepmetals.com | products | RED — confirmed_recall_misses:4 | 8/0 | 0.8667 | 0.0 | 0.9351 | none comparable |
| steelcraft.com | conformity_attestations | OK | 0/9 | 1.0 | 0.0043 | 0.7365 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| steelcraft.com | equipments | OK | 0/9 | not gated (0 candidates) | 0.0 | 0.5701 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0 |
| steelcraft.com | industries | RED — confirmed_recall_misses:1 | 0/9 | 0.9375 | 0.0102 | 0.6172 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| steelcraft.com | material_caps | OK | 0/9 | 1.0 | 0.0 | 0.8688 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| steelcraft.com | process_caps | OK | 0/9 | 1.0 | 0.0041 | 0.5926 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| steelcraft.com | products | OK | 0/9 | 1.0 | 0.0011 | 0.7571 | verbatim.not_in_window_rate +0.0; lengths.six_plus_rate +0.0; consistency.consistency +0.0; expectations.confirmed_recall +0.0; sweep.floor_recall +0.0 |
| sterlingmfg.net | conformity_attestations | OK | 9/0 | 1.0 | 0.0 | 0.9742 | none comparable |
| sterlingmfg.net | equipments | OK | 9/0 | 1.0 | 0.0 | 0.4902 | none comparable |
| sterlingmfg.net | industries | OK | 9/0 | 1.0 | 0.0036 | 0.7273 | none comparable |
| sterlingmfg.net | material_caps | RED — confirmed_recall_misses:3 | 9/0 | 0.9583 | 0.0 | 0.9194 | none comparable |
| sterlingmfg.net | process_caps | RED — confirmed_recall_misses:4 | 9/0 | 0.9524 | 0.0018 | 0.8058 | none comparable |
| sterlingmfg.net | products | RED — confirmed_recall_misses:2 | 9/0 | 0.9733 | 0.0 | 0.7097 | none comparable |
| superiortech.org | conformity_attestations | OK | 3/0 | 1.0 | 0.0 | 1.0 | none comparable |
| superiortech.org | equipments | RED — confirmed_recall_misses:6 | 3/0 | 0.7857 | 0.0 | 0.8375 | none comparable |
| superiortech.org | industries | OK | 3/0 | 1.0 | 0.0 | 0.8182 | none comparable |
| superiortech.org | material_caps | OK | 3/0 | 1.0 | 0.0 | 0.9259 | none comparable |
| superiortech.org | process_caps | RED — confirmed_recall_misses:1 | 3/0 | 0.9888 | 0.0 | 0.8936 | none comparable |
| superiortech.org | products | RED — confirmed_recall_misses:1 | 3/0 | 0.9722 | 0.0019 | 0.8908 | none comparable |
| tanfel.com | conformity_attestations | OK | 10/0 | 1.0 | 0.0 | 0.6136 | none comparable |
| tanfel.com | equipments | RED — confirmed_recall_misses:1 | 10/0 | 0.9474 | 0.0 | 0.7145 | none comparable |
| tanfel.com | industries | RED — confirmed_recall_misses:1 | 10/0 | 0.96 | 0.0 | 0.5893 | none comparable |
| tanfel.com | material_caps | OK | 10/0 | 1.0 | 0.0 | 0.7895 | none comparable |
| tanfel.com | process_caps | RED — confirmed_recall_misses:7 | 10/0 | 0.9263 | 0.0043 | 0.8847 | none comparable |
| tanfel.com | products | RED — confirmed_recall_misses:3 | 10/0 | 0.9552 | 0.006 | 0.8466 | none comparable |
| taylordunn.com | conformity_attestations | OK | 8/0 | not gated (0 candidates) | — | — | none comparable |
| taylordunn.com | equipments | OK | 8/0 | not gated (0 candidates) | 0.0 | 0.25 | none comparable |
| taylordunn.com | industries | OK | 8/0 | 1.0 | 0.0 | 1.0 | none comparable |
| taylordunn.com | material_caps | OK | 8/0 | not gated (0 candidates) | 0.0 | 0.2695 | none comparable |
| taylordunn.com | process_caps | OK | 8/0 | not gated (0 candidates) | — | — | none comparable |
| taylordunn.com | products | OK | 8/0 | 1.0 | 0.0 | 1.0 | none comparable |

See **FINDINGS.md** in this directory for the written analysis of this run (verified findings, judged census status, caveats).
