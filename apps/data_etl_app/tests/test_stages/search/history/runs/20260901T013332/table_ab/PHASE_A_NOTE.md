# Phase A results — the three fork-settling measurements (2026-09-02)

Roadmap: `docs_local/SEARCH_RECALL_ROADMAP_2026-09-02.md`. All calls via the
litellm proxy, production params, ~$2 total. Scripts + raw results in this
directory (`synthesis_probe.py`, `prose_clause_ab.py`, `divisor_ab.py`).

## A1 — synthesis specifics-preservation probe (the keystone) — VERDICT: viable WITH hardening

Wire-faithful records (real `collect_window` snippets, real
`render_synthesis_record_blocks`, snippet-only arm, real response schema).
Gates = designation-shaped tokens (letter+digit) in the record's snippets.

| record | entries | gates | baseline prompt | hardened sentence |
|---|---|---|---|---|
| Aluminum @ mathewsco | 5 | 5 | 3/5 (both models; grades 319/356/A357 all kept, ASTM ids dropped) | — |
| JET slice | 50 | 5 | 5/5 | — |
| JET slice | 60 | 12 | 1/12 | **12/12** |
| JET slice | 80 | 29 | 4/29 | **29/29** |
| JET slice | 100 | 48 | 0/48 | 12/48 once, then **48/48, 48/48** (nonce repeats) |
| JET full | 401 | 333 | 0/333 (both models; "numerous codes") | **333/333, 333/333** (gpt-4.1); **67/333** (mini) |

- The CURRENT synthesis prompt collapses to summary beyond ~a dozen
  designations despite its preserve-specifics sentence; output stayed ~200
  tokens against a 20k budget.
- ONE added sentence (drafted, unapplied) fixes it on gpt-4.1 at every size
  tested, replicated: "This holds at any count: when the entries carry many
  designations, the synthesis names every one of them, in full, as a list
  inside the synthesis — a synthesis that names fewer designations than the
  entries carry is wrong, and phrases like among others, various models, or
  numerous codes are never a substitute for the designations themselves."
- One 12/48 outlier under repeat shows the tail failure persists →
  pair the sentence with a MECHANICAL under-enumeration check (designation
  count in entries vs synthesis) wired into the existing assess+retry pass.
- gpt-4.1-mini caps at ~67 enumerated designations even hardened →
  mega-record synthesis stays on gpt-4.1, or records split (~50 designations).

## A2 — prose mention-neutrality clause (E5) — VERDICT: NEGATIVE, drop it

3 prose danger windows (blackadvtech industries blog, agstech conformity
standards encyclopedia, lucasmilhaupt process Brazing Academy), clause
appended to What-qualifies vs baseline, 2 models x 2 reps:
gates moved nowhere outside churn (e.g. conformity gpt-4.1 baseline 5,3/14 vs
clause 3,3/14; volume 125→106). The prose blindness is not
instruction-shaped; remedies remain retry-union, boundary rulings,
acceptance. Confirms the user's instruction-noise stance a second time.

## A3 — 2.5k vs 8/5k sub-windows — VERDICT: no winner, keep 5k

Two real chunks re-run fresh at divisor 4 vs 8 with the production chunker
(`derive_search_sub_bounds`, overlap 0):
howco materials favored 5k (4,6/9 vs 2,3/9 gates), alecmfg process favored
2.5k (3,0/10 vs 6,5/10) — opposite directions, rep-to-rep churn as large as
the arm gap, +14% input tokens at divisor 8. The recall-decay-at-5k
hypothesis is unsupported here. Catalog output-cap relief should come from a
cap raise or targeted handling, not a global divisor change.
Side-evidence AGAIN for retry-and-union: union across the two reps beats
either single rep in every arm.

## Consequences for the Phase B bundle (pending user's fork decision)

- Fork (a) general-only: retire drafted search E3/E4; ship metrology edit
  only at search; ship hardened synthesis sentence (all six statics,
  field-translated) + under-enumeration retry; heading propagation carries
  the catalog class; search possibly mini-viable, synthesis of mega-records
  is not.
- Fork (b) specifics-at-search: E3 ships; catalog class still needs heading
  propagation (augmentation alone already measured 332/335 with the drafted
  clause); synthesis hardening still advisable.
- Either fork: E5 dropped, divisor stays 4, retry-and-union reinforced.
