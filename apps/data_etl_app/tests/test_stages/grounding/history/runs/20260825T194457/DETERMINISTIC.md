# Deterministic report — run 20260825T194457

Generated 2026-08-27T02:30:43+00:00 · taxonomy v1 · normalizer core:1

Dumps: 14 · subjects: alecmfg.com, steelcraft.com
Row-divergence estimate (noise floor input): 0.182

| dump | tags | declines | hops | gate viol (shipped) | membership viol | sentinel | false drops | twins div/tot | churn clusters | descent screened |
|---|---|---|---|---|---|---|---|---|---|---|
| alecmfg_com__conformity_attestations | 28 | 54 | 0 | 0 (0) | 1 | 0 | 0/7 | 0/0 | 0 | 0/0 |
| alecmfg_com__contract_products | 135 | 233 | 0 | 0 (0) | 0 | 0 | 0/0 | 3/14 | 5 | 0/0 |
| alecmfg_com__equipments | 34 | 3 | 0 | 0 (0) | 0 | 0 | 0/0 | 0/2 | 0 | 0/0 |
| alecmfg_com__industries | 110 | 120 | 28 | 0 (0) | 0 | 0 | 0/1 | 9/17 | 0 | 0/28 |
| alecmfg_com__material_caps | 64 | 90 | 10 | 0 (0) | 0 | 0 | 0/2 | 3/11 | 0 | 0/10 |
| alecmfg_com__process_caps | 243 | 280 | 65 | 2 (0) | 0 | 0 | 0/22 | 9/21 | 0 | 0/65 |
| alecmfg_com__products | 142 | 226 | 0 | 0 (0) | 0 | 0 | 0/0 | 3/14 | 1 | 0/0 |
| steelcraft_com__conformity_attestations | 88 | 212 | 0 | 0 (0) | 0 | 0 | 0/8 | 17/24 | 8 | 0/0 |
| steelcraft_com__contract_products | 465 | 258 | 0 | 0 (0) | 0 | 0 | 0/0 | 71/121 | 3 | 0/0 |
| steelcraft_com__equipments | 20 | 69 | 0 | 0 (0) | 0 | 0 | 0/0 | 4/9 | 0 | 0/0 |
| steelcraft_com__industries | 120 | 50 | 5 | 0 (0) | 0 | 0 | 0/0 | 7/11 | 0 | 0/5 |
| steelcraft_com__material_caps | 99 | 103 | 9 | 2 (1) | 0 | 0 | 0/5 | 8/23 | 2 | 1/9 |
| steelcraft_com__process_caps | 153 | 213 | 18 | 2 (2) | 0 | 0 | 3/14 | 18/24 | 3 | 0/18 |
| steelcraft_com__products | 454 | 264 | 0 | 0 (0) | 0 | 0 | 0/0 | 59/121 | 6 | 0/0 |

## A/A reproducibility (products vs contract_products)

- alecmfg.com: comparable=True, identical 334/381 (87.7%), decline-vs-tag flips 31
- steelcraft.com: comparable=True, identical 556/680 (81.8%), decline-vs-tag flips 50

## Findings (10)

- **vocab_membership** alecmfg.com/conformity_attestations chunk 0:96974 group g03qqd2d: in-vocab tag 'Regulatory Approval' not an ontology label (name or altLabel)
- **failed_rule_gate** alecmfg.com/process_caps chunk 0:96974 group g0v71p5s: tag 'Polishing' emitted with outcomes ['chosen', 'failed'] (caught downstream)
- **failed_rule_gate** alecmfg.com/process_caps chunk 0:96974 group g08pdmj1: tag 'Urethane Casting' emitted with outcomes ['chosen', 'failed'] (caught downstream)
- **failed_rule_gate** steelcraft.com/material_caps chunk 0:91562 group gpk94tty: tag 'Lead' emitted with outcomes ['chosen', 'failed'] (caught downstream)
- **failed_rule_gate** steelcraft.com/material_caps chunk 91562:158790 group gwjvjhxy: tag 'Stainless Steel Stiffener' emitted with outcomes ['failed', 'satisfied'] (SHIPPED)
- **failed_rule_gate** steelcraft.com/process_caps chunk 0:91562 group gu8yq7hw: tag 'Factory assembly of jamb and head components' emitted with outcomes ['failed', 'satisfied'] (SHIPPED)
- **decoration_echo** steelcraft.com/process_caps chunk 0:91562 group gnwwkljf: dropped option 'Extruding (also: Extrusion)' is the decorated form of label 'Extruding' (false drop)
- **failed_rule_gate** steelcraft.com/process_caps chunk 0:91562 group gsjmyp84: tag 'Finish painting of doors' emitted with outcomes ['failed', 'satisfied'] (SHIPPED)
- **decoration_echo** steelcraft.com/process_caps chunk 0:91562 group gll66lbk: dropped option 'Machining (also: Material Removal Process, Subtractive Process, subtractive manufacturing)' is the decorated form of label 'Machining' (false drop)
- **decoration_echo** steelcraft.com/process_caps chunk 0:91562 group gmh447et: dropped option 'Machining (also: Material Removal Process, Subtractive Process, subtractive manufacturing)' is the decorated form of label 'Machining' (false drop)
