"""Appendix E / Phase 2 gate tool: dry grouping run of the v3 normalizer over the
dump corpus, per layer, listing every multi-member group each layer creates so a
wrong merge is visible (the appendix-B method).

Run from the dumps dir with the repo venv:
    cd packages/logs/extraction_dumps && ../../../.venv/bin/python ../../../pipeline_v3_evidence/normalize_dry_run.py

Uses the PRODUCTION normalizer (core.utils.form_normalizer), so re-running after
a rule change shows the delta. The 2026-08-21 output (prototype rules, identical
to what shipped) is 2026-08-21_normalize_dry_run_output.txt next to this file.
"""
import collections
import json
import pathlib

from core.utils.form_normalizer import normalize

FIELDS = {"material_caps", "process_caps", "industries", "equipments", "products",
          "contract_products", "conformity_attestations"}
L2_FIELDS = {"material_caps", "process_caps"}  # the verb-fold dial (app-side mapping at Phase 3)

by_field: dict[str, set[str]] = collections.defaultdict(set)
for run in pathlib.Path(".").iterdir():
    if not run.is_dir():
        continue
    for f in run.glob("*__partial.json"):
        fld = f.name.split("__")[1]
        if fld not in FIELDS:
            continue
        try:
            d = json.loads(f.read_text())
        except Exception:
            continue
        for c in d.get("chunks", {}).values():
            for r in c.get("rows", []):
                if r.get("phrase"):
                    by_field[fld].add(r["phrase"])


def groups(phrases, *, verb_fold):
    g = collections.defaultdict(set)
    for p in phrases:
        g[normalize(p, verb_fold=verb_fold)].add(p)
    return {k: v for k, v in g.items() if len(v) > 1}


total = sum(len(v) for v in by_field.values())
print(f"corpus: {total} (field,phrase) pairs, fields={len(by_field)}")
base = {}
for fld, ph in by_field.items():
    for k, v in groups(ph, verb_fold=False).items():
        base[(fld, k)] = v
print(f"\n== L0+L1 (+fallback): {len(base)} multi-member groups")
for (fld, k), v in sorted(base.items()):
    print(f"  [{fld}] {k!r} <- {sorted(v)}")
l2 = {}
for fld, ph in by_field.items():
    for k, v in groups(ph, verb_fold=(fld in L2_FIELDS)).items():
        l2[(fld, k)] = v
new = {k: v for k, v in l2.items() if k not in base or base[k] != v}
print(f"\n== +L2 (process/material only): {len(l2)} groups; new/changed: {len(new)}")
for (fld, k), v in sorted(new.items()):
    print(f"  [{fld}] {k!r} <- {sorted(v)}")
