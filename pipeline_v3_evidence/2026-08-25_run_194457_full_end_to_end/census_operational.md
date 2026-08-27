# Census — operational behaviour, run 20260825T194457

Counted directly from the 14 dumps and the 15,811-line run log. Mechanical
facts only; judgment counts live in the other census files.

## Request accounting — every live call dispatched once, answered once

| stage | dump entries | with token usage | log dispatches |
|---|---:|---:|---:|
| freehand_grounding | 95 | 95 | **95** |
| initial_grounding | 47 | 47 | **47** |
| oov_grounding (incl. 1 retry) | 48 | 48 | **48** |
| relationship_screening | 87 | 87 | **87** |
| recursive_grounding (descent) | 216 | 104 | **104** |
| search | 112 | 112 | 0 (replayed) |
| mention_collection | 160 | 158 | 0 (replayed) |
| synthesis | 134 | 134 | 0 (replayed) |
| **total** | 899 | 785 | **381** |

**381 live LLM calls, and the log's dispatch count matches the dumps exactly on
every live stage.** Zero unanswered at end of run, zero unknown-id responses,
zero parse errors, one under-answer (repaired by the retry). This is the
cleanest delivery record any v3 run has posted.

**Observability defect found here: the descent stage's dump block lists 216
"requests" but only 104 calls were ever made.** The 112 extras carry a full
custom_id (pv + ud) but no `created_at`, no usage, and `note:
"usage_unavailable"`, and they appear nowhere in the log. They are node records,
not requests. Anyone sizing descent volume or cost from the dump — as this
analysis first did — overstates it by 2.08x. They cost nothing (verified: the
log's 104 dispatches are exactly the 104 usage-bearing entries), so the run's
$10.36 live cost is unaffected.

The other two non-usage entries are `note: "synthetic_response"` — the
pre-answered dummies for zero-mention windows. That is the fix from run
034518 (which crashed dispatching one) still holding, twice, silently.

Latency (client_latency_ms, the only honest timing field in the dump —
`created_at` is the orchestrator run-start and `turnaround_seconds` is
therefore meaningless on eager runs): n=785, p50 10.5 s, p90 28.1 s, max 73.7 s.

## Row accounting — 3,309 rows

| status | rows | share |
|---|---:|---:|
| grounded | 1,493 | 45% |
| no_candidates (grounding declined) | 1,276 | 39% |
| screened_out | 392 | 12% |
| no_mentions | 148 | 4% |

Provenance: 3,216 `llm_round_1`, 85 `unmatched`, 8 `brute`. **Search round 2+
produced nothing this run** — recursive search is off, so every form comes from
a single search round; the 8 brute rows are the containment sweep.

The 85 `unmatched` rows (forms the fold could not attribute to a search hit)
are not inert: 23 ground and survive to output, 7 are screened out, 10 decline,
45 have no mentions. **I read all 23 grounded ones: they are legitimate, and
several are among the run's better tags** — `Rail Equipment`, `EV` → Electric
Vehicles, `semiconductor` → Semiconductor, `offshore marine` → Offshore Marine,
`steel` → Steel, with real evidence (mention counts up to 10). This is mention
collection recovering entities the search stage did not emit, which is the
split working as designed, not a leak. Two of the 23 are twin-divergent
(`packaging` → `Packaging` in one chunk, `Medical Packaging` in the other;
`Electronics` → `Consumer Electronics` here, `Power Electronics` in its twin).

## Lints and tripwires

- **`record_own_name_hits` is fully saturated: 3,155 of 3,155 synthesized rows
  score 1+ (the other 154 are `no_mentions`, null by construction).** Not
  "84–100%" as the sampled read suggested — it is 100.0%. The lint cannot
  discriminate anything and should be retired or redefined.
- **Focal-form lint (recomputed by hand here, crude normalisation): 108 of
  3,161 synthesized rows (3.4%) do not contain their focal form.** Several are
  benign list-form focal forms (`'blanking, bending, welding, anodizing'`), so
  treat 3.4% as an upper bound.
- **The identical-synthesis tripwire built in `29d2167` DOES NOT APPEAR IN A
  FULL-RUN DUMP.** `identical_synthesis_in_request` is written by
  `synthesis_dump_util.py`, which only runs on the partial (stop-after-
  synthesis) dump; a full run writes the group spine and the field is absent —
  0 occurrences across all 14 dumps. The instrument built to watch synthesis
  collapse is blind on exactly the runs that get shipped. **Fix: carry the
  synthesis block (or at least its summary counters) into the full-run dump.**

## Collapse and the FE→DE swap, counted independently

Recomputed the collapse from the full dumps since the tripwire was unavailable:
**34 group-instances in 17 clusters share a synthesis string with another group
— 1.2% of the 2,793 synthesized groups** (steelcraft products/contract 14 each
in 6 clusters, equipments 2, conformity 2, alecmfg industries 2). In range of
the 12/23/1/14 census of earlier runs; the split did not make it worse.

**The FE→DE entity swap RECURRED.** Group `gl3j2k95` is synthesized six times
across three fields x two chunks; five describe FE Series correctly, and one —
`steelcraft_com__equipments`, chunk 0:91562 — describes **DE Series** under the
focal form `FE Series Double-Egress Frames`. That is 1 of 6 this run, and it
keeps the standing verdict intact: stochastic, unfixed, ~2-in-5 historically.
It is also the collapse cluster in that dump (FE and DE share the string).
