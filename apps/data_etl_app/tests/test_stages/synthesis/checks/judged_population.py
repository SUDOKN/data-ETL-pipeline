"""Build the judged population and the judged work orders for a run.

  .venv/bin/python checks/judged_population.py --run <run_id> --baseline <run_id>
        [--singletons 3000] [--seed 20260911] [--target 175] [--max 250]

The population the redesign's Step 1 readout uses (design doc §7; first
built inline on 2026-09-11 for run 20260911T003500):

- every record in an IDENTICAL-SNIPPET-SET cluster: records of one
  (subject, field, chunk) whose set of snippets is byte-identical (the retry
  memo's cluster key; 2,635 clusters / 7,467 records on the census subjects);
- stratum A of the singletons: every singleton the BASELINE run re-asked
  (``retried`` on the baseline dump) — the retry axis, paired;
- stratum B: random singletons, field-proportional, deterministic seed, to
  fill ``--singletons`` in total.

Writes ``history/runs/<run>/pending/judged/<subject>__<field>.json`` (the
work-order subset, original record order, each record labelled
``population`` and ``cluster_id``), ``POPULATION.json`` (key → label) and
``SLICES.json`` (judge slices cut at request boundaries, ``slices.cut``).
Needs the run's evidence snapshot (``--pull``) and the baseline's dumps.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from checks import loading, paired_readout, pull, scorecard, slices  # type: ignore[no-redef]
else:
    from . import loading, paired_readout, pull, scorecard, slices

Key = tuple[str, str, str, str]


def cluster_ids(index: dict[str, Any]) -> dict[Key, str]:
    clusters: dict[tuple[str, str, str, str], list[Key]] = defaultdict(list)
    for k, v in index.items():
        subject, field, chunk_bounds, group_id = k.split("|")
        if field == loading.SHARED_DUPLICATE:
            continue
        digest = hashlib.sha256("\n".join(sorted(set(v["snippets"]))).encode()).hexdigest()[:16]
        clusters[(subject, field, chunk_bounds, digest)].append((subject, field, chunk_bounds, group_id))
    out: dict[Key, str] = {}
    for ck, members in clusters.items():
        if len(members) > 1:
            cid = hashlib.sha256("|".join(ck).encode()).hexdigest()[:10]
            for m in members:
                out[m] = cid
    return out


def baseline_retried(baseline_run: str) -> set[Key]:
    out: set[Key] = set()
    for (subject, field), path in loading.dump_files(baseline_run).items():
        if field == loading.SHARED_DUPLICATE:
            continue
        for r in loading.iter_records(subject, field, loading.load_dump(path)):
            if r.retried:
                out.add((subject, field, r.chunk_bounds, r.group_id))
    return out


def build(
    run_id: str,
    baseline_run: str,
    singletons: int,
    seed: int,
    target: int,
    max_records: int,
    from_baseline_judged: bool = False,
) -> dict[str, Any]:
    index = pull.load_evidence_index(run_id)
    if index is None:
        raise SystemExit(f"run {run_id}: no evidence snapshot — run run_eval.py --pull first")
    clusters = cluster_ids(index)
    retried = baseline_retried(baseline_run)
    all_keys: list[Key] = [
        tuple(k.split("|"))  # type: ignore[misc]
        for k in index
        if k.split("|")[1] != loading.SHARED_DUPLICATE
    ]
    singles = [k for k in all_keys if k not in clusters]
    stratum_a = [k for k in singles if k in retried]
    rest = [k for k in singles if k not in retried]
    if from_baseline_judged:
        # Stratum B = the singletons the BASELINE run judged (its ledger rows), so
        # every singleton pairs on identical evidence. Needed once the baseline is
        # itself a judged SUBSET (2026-09-11: run 20260911T003500 judged 10,467 of
        # 22,162); a fresh random draw would pair almost nothing outside clusters.
        judged = paired_readout.load_baseline(baseline_run)
        rest = [k for k in rest if k in judged]
    need = max(0, singletons - len(stratum_a))
    by_field: dict[str, list[Key]] = defaultdict(list)
    for k in rest:
        by_field[k[1]].append(k)
    rng = random.Random(seed)
    stratum_b: list[Key] = []
    for field, pool in sorted(by_field.items()):
        pool.sort()
        rng.shuffle(pool)
        take = round(need * len(pool) / len(rest)) if rest else 0
        stratum_b.extend(pool[:take])
    stratum_b = stratum_b[:need]
    population: dict[Key, str] = {k: "cluster" for k in clusters}
    for k in stratum_a:
        population[k] = "single_base_retried"
    for k in stratum_b:
        population[k] = "single_random"

    pending_dir = scorecard.history_dir(run_id) / "pending"
    out_dir = pending_dir / "judged"
    out_dir.mkdir(exist_ok=True)
    written = 0
    orders = 0
    for path in sorted(pending_dir.glob("*__*.json")):
        order = json.loads(path.read_text(encoding="utf-8"))
        subject, field = order["subject"], order["field"]
        records = []
        for r in order["records"]:
            key = (subject, field, r["chunk_bounds"], r["group_id"])
            if key in population:
                r = dict(r)
                r["population"] = population[key]
                r["cluster_id"] = clusters.get(key)
                records.append(r)
        if not records:
            continue
        order["records"] = records
        order["population_note"] = (
            "population = cluster (identical-snippet-set cluster member; cluster_id shared by its "
            "members), single_base_retried (a singleton the baseline run re-asked), single_random "
            "(field-proportional sample). Judge every record identically; the label is for the readout."
        )
        (out_dir / path.name).write_text(json.dumps(order, indent=1, ensure_ascii=False, default=str), encoding="utf-8")
        written += len(records)
        orders += 1
    (out_dir / "POPULATION.json").write_text(
        json.dumps({"|".join(k): {"population": v, "cluster_id": clusters.get(k)} for k, v in population.items()}, indent=0),
        encoding="utf-8",
    )
    slice_rows: list[dict[str, Any]] = []
    for path in sorted(out_dir.glob("*__*.json")):
        order = json.loads(path.read_text(encoding="utf-8"))
        for part, (a, b) in enumerate(slices.cut(order["records"], target, max_records), start=1):
            slice_rows.append({"subject": order["subject"], "field": order["field"], "part": part, "start": a, "end": b, "records": b - a, "path": str(path)})
    (out_dir / "SLICES.json").write_text(json.dumps(slice_rows, indent=1), encoding="utf-8")
    return {
        "clusters": len(set(clusters.values())),
        "cluster_records": len(clusters),
        "singletons": len(singles),
        "stratum_a_baseline_retried": len(stratum_a),
        "stratum_b_random": len(stratum_b),
        "population": len(population),
        "orders_written": orders,
        "records_written": written,
        "slices": len(slice_rows),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True)
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--singletons", type=int, default=3000)
    parser.add_argument("--seed", type=int, default=20260911)
    parser.add_argument("--target", type=int, default=175)
    parser.add_argument("--max", dest="max_records", type=int, default=250)
    parser.add_argument(
        "--singletons-from-baseline",
        action="store_true",
        help="draw the random singleton stratum only from singletons the baseline run judged, so every singleton pairs",
    )
    args = parser.parse_args()
    print(
        json.dumps(
            build(
                args.run,
                args.baseline,
                args.singletons,
                args.seed,
                args.target,
                args.max_records,
                from_baseline_judged=args.singletons_from_baseline,
            ),
            indent=1,
        )
    )


if __name__ == "__main__":
    main()
