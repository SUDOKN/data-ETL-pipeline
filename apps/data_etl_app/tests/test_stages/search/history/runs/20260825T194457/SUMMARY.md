# Search-stage eval — mechanical pass, run 20260825T194457

Generated 2026-08-27T18:26:15.947027+00:00. Judged metrics
(precision census, recall census, wrong-actor, agreement) are produced
by the RUNBOOK's agent protocol and merged into these scorecards by the
assistant — this file alone is NOT the full evaluation.

| subject | field | verdict | live/replayed | recall (confirmed) | not-in-window | consistency | Δ vs baseline |
|---|---|---|---|---|---|---|---|
| alecmfg.com | conformity_attestations | RED — confirmed_recall_misses:1 | 0/7 | 0.9286 | 0.0 | 0.968 | none comparable |
| alecmfg.com | equipments | OK | 0/7 | 1.0 | 0.0 | 0.8706 | none comparable |
| alecmfg.com | industries | OK | 0/7 | 1.0 | 0.0071 | 0.5622 | none comparable |
| alecmfg.com | material_caps | OK | 0/7 | 1.0 | 0.0 | 0.8563 | none comparable |
| alecmfg.com | process_caps | RED — confirmed_recall_misses:3 | 0/7 | 0.9211 | 0.0035 | 0.7836 | none comparable |
| alecmfg.com | products | OK | 0/7 | 1.0 | 0.0023 | 0.7698 | none comparable |
| steelcraft.com | conformity_attestations | OK | 0/9 | 1.0 | 0.0043 | 0.7365 | none comparable |
| steelcraft.com | equipments | OK | 0/9 | not gated (0 candidates) | 0.0 | 0.5701 | none comparable |
| steelcraft.com | industries | RED — confirmed_recall_misses:1 | 0/9 | 0.9375 | 0.0102 | 0.6172 | none comparable |
| steelcraft.com | material_caps | OK | 0/9 | 1.0 | 0.0 | 0.8688 | none comparable |
| steelcraft.com | process_caps | OK | 0/9 | 1.0 | 0.0041 | 0.5926 | none comparable |
| steelcraft.com | products | OK | 0/9 | 1.0 | 0.0011 | 0.7571 | none comparable |

See **FINDINGS.md** in this directory for the written analysis of this run (verified findings, judged census status, caveats).
