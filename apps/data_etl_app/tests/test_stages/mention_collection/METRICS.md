# The mention-stage metric battery

Every number this instrument reports, what flaw it tracks, and where its
baseline came from. Mechanical metrics (**M**) are computed by `checks/`; golden
metrics (**G**) come from the corpus in `goldens/`; judged metrics (**J**) come
from the RUNBOOK's agent census.

**Only tripwires gate.** Everything a model wrote is banded or trended, because
model wording has a noise floor and mechanical facts do not.

⚠ **Read every baseline below with its date.** The stage was rewritten on
2026-08-27 (`bbfc42b`): longest-span containment was reversed and a `collapsed`
status added. Every baseline marked *pre-D8* is the "before" side of that
change and is not comparable to anything measured after it.

## Mechanical (computed from the dump alone)

| id | metric | what it tracks | baseline / today |
|---|---|---|---|
| M1 | **delivery** — `not_described`, `unknown_answer_ids`, nonce echoes, `retried` | the model failing to answer what it was asked; once 0 of 47 passages described | 0 / 0 / 0 / 0 (pre-D8). All three **gate** |
| M2 | **span integrity** — form present whole-word in its own snippet; one `mention_id` = one snippet; no duplicate occurrence at one span | the `Steel`-inside-`Steelcraft` boundary class and the 297 overlap duplicates | 0 violations over 6,085 real mentions (pre-D8). All **gate** |
| M3 | **location content bans** — URL, self-referential opener, verb opener, cross-reference, empty | four measured defects the prompt forbids outright: URLs were 39–83%, openers 92.7%, cross-references 2.7% | 0 / 0 / 0 / 0 (pre-D8). All **gate** |
| M4 | **page exclusion** — mentions sitting on a dropped legal page | ~80k tokens once wasted on privacy pages | 0 (pre-D8). **Gates** |
| M5 | **identity** — `pv=` in every request id vs the header; `products` ≡ `contract_products` | a dump contradicting itself; the shared-fold wiring breaking | consistent (pre-D8). Both **gate** |
| M6 | **group shape** — groups, empty, collapsed, multi-form, single-mention share | what later stages actually receive | 2,248 groups, 115 empty, 67.1% single-mention (pre-D8) |
| M7 | **entry pressure** — `max_entries_in_a_group`, `groups_over_50_entries` | where decentralization meets synthesis's 50-entry packing limit and becomes cross-record evidence bleed | max 60, one group over 50 (pre-D8, steelcraft products). Handed to synthesis |
| M8 | **nesting** — `nested_occurrences`, `nested_groups` | the population the D8 reversal created; the denominator for J-I1 | **0 by construction pre-D8**; unmeasured after |
| M9 | **location shape** — median / p90 / max length, total chars, share of payload | verbosity; locations were 61.1% of the synthesis evidence payload | median 154, p90 224 (pre-D8) |
| M10 | **attribution slot** — share of locations containing "site's own copy" | sizes the population J-S4 must judge; the slot is filled ~100% but 99.5% is one phrase | 0.995 (pre-D8) |
| M11 | **casing rescue** — mentions found under a casing search never sent | the mechanical replacement for recursive search | 722 (11.9%) on an earlier run |
| M12 | **cost & delivery** — tokens, USD, request count, latency | this is the pipeline's most expensive stage | 942,984 in / 234,723 out ≈ $3.76; p50 7.9 s (pre-D8) |
| M13 | **coverage** — `has_fold` per field | whether the instrument can see the stage at all | a pre-2026-08-27 full-run dump is `BLIND`, not passing |

## Golden (computed against `goldens/`)

| id | metric | what it tracks | state |
|---|---|---|---|
| G1 | **must-not-occur violations** | a form absent from the text that was nevertheless collected — a word-boundary or short-form defect. **Exact**, and therefore **gates** | 253 zero-occurrence labels + 142 adversarial; 171 substring traps |
| G2 | **over-count** | more occurrences than the full text holds; the trimmed, chunk-capped text cannot exceed the whole. **Gates** | 14,713 labels covering 67,592 occurrences |
| G3 | **coverage of the check** | how many labels a run actually exercised (`occurrence_labels_checked` vs `_not_sent`) | reported per field; a low number means search sent few labelled forms, not that the stage is clean |

**Deliberate leniency:** a label is only checked when its form was *sent* in
that run, and counts are compared as an upper bound rather than an equality.
The pipeline reads a page-trimmed copy under a two-chunk cap, so a full-text
golden count is a ceiling. Only a must-not-occur violation is exact.

## Judged (the agent census — `TAXONOMY.md`)

| id | metric | what it tracks | state |
|---|---|---|---|
| J-S1 | **snippet extent** | clips that cut the sense — the fragment class synthesis's candidate C1 blames on this stage's clipping radius | never measured |
| J-S2/S3/S4 | **location: kind of text / belongs to / whose words** | whether the location is *correct*, not merely well-formed | never measured |
| **J-LOC** | **location correctness = S2 ∧ S3 ∧ S4** | **the headline number of this instrument** | never measured |
| J-S5 | **self-containment** | leaning on another mention, restating the snippet | 0 mechanically (M3); the judged half is unmeasured |
| J-G1…G4 | **grouping** — wrong merge, under-merge, focal form, status | 0 wrong merges found to date across every prior audit | unmeasured under this taxonomy |
| J-I1 | **inheritance dilution** | whether a generic form inheriting a specific occurrence is sound evidence — created by the D8 reversal | **impossible before 2026-08-27; never measured** |
| J-AGREE | **judge agreement** | the floor under every J number; print it beside them | not yet run |

## Gating rule

**RED** = a mechanical tripwire (M1–M5) or a golden violation (G1, G2).
Everything else is tracked. Judged rates never gate
(`config/common.yaml: gate_on_judged_rates: false`) and will not until this
stage's own reproducibility floor exists — see below.

**BLIND** is not a pass. A dump without a `fold` block cannot show the stage;
reporting its metrics as zeros would be a lie.

## Noise floors

**This stage's LLM half has never had its floor measured.** Until
`checks/aa_probe.py` exists and has run, no location metric gates and no
location delta under ~15% relative is reported as a result
(`config/common.yaml: noise.fallback_relative_band`).

For calibration, the two floors measured elsewhere on this pipeline: freehand
grounding reproduces at **77.7–90.1%** on byte-identical payloads at
temperature 0, and search's whole-window set identity is **28.4%** (per-form
Jaccard 0.769). Assume this stage's Location output is no more stable than that
until shown otherwise.

**The mechanical half has no noise floor at all.** It is deterministic, so M1–M5
and G1–G2 are exact and any movement in them is real.

## Comparison discipline

Two runs are comparable only within one `config_digest` (chunk geometry,
normalizer version, snippet radius, prompt version) — it is on every scorecard.
Pair snippets by `mention_id`, groups by `group_id`, requests by `ud=`, and
**always with `chunk_bounds` in the key**: 242 group ids appeared in both chunks
of one field on a real run, and a pairing without bounds silently dropped 114
records.

Unpaired deltas are upstream drift, not stage change. Search phrase sets share
only ~43–64% across window layouts, so a re-chunked run reshuffles this stage's
inputs wholesale.
