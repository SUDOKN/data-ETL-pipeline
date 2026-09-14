"""Paired readout of one run's verdicts against a baseline run's ledger rows.

  .venv/bin/python checks/paired_readout.py --run <run_id> --baseline <run_id>
        [--population history/runs/<run>/pending/judged/POPULATION.json]

Pairs records by (subject, field, chunk_bounds, group_id) — the only safe key
(README trap 2) — and keeps a pair only where the two sides judged the SAME
evidence (``evidence_sha256`` equal), so a prompt change is the only thing
between the two verdicts. Reads the run's ``history/runs/<run>/verdicts/*.jsonl``
directly (before or after ``--finalize``) and the baseline from the
cumulative ledger, restricted to rows whose ``first_judged_run`` is the
baseline. Prints, per population stratum (``POPULATION.json`` from the judged
subset builder, else "all"): records paired, any-fail and any-major rates on
both sides with the McNemar-style discordant counts, per-dimension fail
counts, and the cluster readouts (mixed-verdict clusters, identical-evidence
twin flips) when cluster ids are present. Written 2026-09-11 for the
synthesis focus run (design doc D3/D10).

``--by-subject`` (2026-09-12, design doc §24) adds the per-subject and
per-(subject, field) FLIP readout: of the pairs judged on identical evidence,
how many records changed their any-fail verdict and how many their major
verdict between the two sides. This is the number the A/A floor is quoted
in (run 20260912T225723 against 191548: mathewsco 29/353 = 8.2%, tanfel
34/1,118 = 3.0%), and the number every variance lever (packing, N-sample
choice) is read against.

``--by-key`` (2026-09-14, for the snippet-radius experiment) pairs on the
key ALONE and keeps pairs whose evidence CHANGED between the runs — a fold
change (radius 0 → 1) re-digests every record's snippets, so the
identical-evidence rule would pair nothing. Each stratum then also prints how
many of its pairs carry identical vs changed evidence, and the flip readout
is split the same way: flips on identical evidence read against the floor as
before; flips on changed evidence carry the lever AND the floor together.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Optional

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from checks import loading, scorecard  # type: ignore[no-redef]
else:
    from . import loading, scorecard

DIMS = ["J1", "J2", "J3", "J4", "J5", "J6", "J7"]
Key = tuple[str, str, str, str]


def _key(row: dict[str, Any]) -> Key:
    return (row["subject"], row["field"], row["chunk_bounds"], row["group_id"])


def _fails(row: dict[str, Any]) -> dict[str, tuple[str, Optional[str]]]:
    checks = row.get("checks") or {}
    return {
        d: ((checks.get(d) or {}).get("verdict") or "pass", (checks.get(d) or {}).get("severity"))
        for d in DIMS
        if d in checks
    }


def paired_keys(
    new: dict[Key, dict[str, Any]],
    base: dict[Key, dict[str, Any]],
    keys: Iterable[Key],
    by_key: bool,
) -> list[Key]:
    """The keys judged on both sides; with ``by_key`` False only where the two
    sides judged the SAME evidence (``evidence_sha256`` equal)."""
    return [
        k for k in keys
        if k in base and (by_key or base[k].get("evidence_sha256") == new[k].get("evidence_sha256"))
    ]


def same_evidence(new: dict[Key, dict[str, Any]], base: dict[Key, dict[str, Any]], k: Key) -> bool:
    return base[k].get("evidence_sha256") == new[k].get("evidence_sha256")


def any_fail(row: dict[str, Any]) -> bool:
    return any(v == "fail" for v, _s in _fails(row).values())


def any_major(row: dict[str, Any]) -> bool:
    return any(v == "fail" and s == "major" for v, s in _fails(row).values())


def note_flag(row: dict[str, Any], flag: str) -> bool:
    checks = row.get("checks") or {}
    return any(flag in ((c or {}).get("note") or "") for c in checks.values())


def load_run_verdicts(run_id: str) -> dict[Key, dict[str, Any]]:
    out: dict[Key, dict[str, Any]] = {}
    malformed = 0
    for path in sorted((scorecard.history_dir(run_id) / "verdicts").glob("*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
                out[_key(row)] = row
            except Exception:
                malformed += 1
    if malformed:
        print(f"(skipped {malformed} malformed verdict line(s))")
    return out


def load_baseline(baseline_run: str) -> dict[Key, dict[str, Any]]:
    out: dict[Key, dict[str, Any]] = {}
    for path in (loading.EVAL_ROOT / "history" / "judgments").glob("*.jsonl"):
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if row.get("first_judged_run") == baseline_run:
                out[_key(row)] = row
    return out


def rate(n: int, d: int) -> str:
    return f"{n}/{d} = {100 * n / d:.1f}%" if d else f"{n}/{d}"


def readout(
    new: dict[Key, dict[str, Any]],
    base: dict[Key, dict[str, Any]],
    population: dict[Key, dict[str, Any]],
    by_key: bool = False,
) -> None:
    strata: dict[str, list[Key]] = defaultdict(list)
    for key in new:
        strata[(population.get(key) or {}).get("population") or "all"].append(key)
    strata["ALL"] = list(new)
    for name, keys in strata.items():
        paired = paired_keys(new, base, keys, by_key)
        if by_key:
            same = sum(1 for k in paired if same_evidence(new, base, k))
            print(f"\n== {name}: judged {len(keys)}, paired BY KEY {len(paired)} "
                  f"(identical evidence {same}, changed evidence {len(paired) - same})")
        else:
            print(f"\n== {name}: judged {len(keys)}, paired on identical evidence {len(paired)}")
        if not paired:
            continue
        for label, fn in (("any fail", any_fail), ("any major", any_major)):
            b = sum(fn(base[k]) for k in paired)
            n = sum(fn(new[k]) for k in paired)
            worse = sum(1 for k in paired if fn(new[k]) and not fn(base[k]))
            better = sum(1 for k in paired if fn(base[k]) and not fn(new[k]))
            print(f"  {label:9s} baseline {rate(b, len(paired))} -> new {rate(n, len(paired))}  "
                  f"(newly failing {worse}, newly passing {better})")
        print("  per dimension (fail counts, baseline -> new; J7 is new):")
        for d in DIMS:
            b = sum(1 for k in paired if _fails(base[k]).get(d, ("pass", None))[0] == "fail")
            n = sum(1 for k in paired if _fails(new[k]).get(d, ("pass", None))[0] == "fail")
            print(f"    {d}: {b} -> {n}")
        gi = sum(1 for k in paired if note_flag(new[k], "grounded_import"))
        cb_b = sum(1 for k in paired if note_flag(base[k], "containment_breach"))
        cb_n = sum(1 for k in paired if note_flag(new[k], "containment_breach"))
        unclear = sum(1 for k in paired if any(v == "unclear" for v, _s in _fails(new[k]).values()))
        print(f"  grounded_import notes (new) {gi}; containment_breach {cb_b} -> {cb_n}; records with an unclear (new) {unclear}")
    # clusters
    by_cluster: dict[str, list[Key]] = defaultdict(list)
    for key, meta in population.items():
        if meta.get("cluster_id") and key in new:
            by_cluster[meta["cluster_id"]].append(key)
    full = {cid: ks for cid, ks in by_cluster.items() if len(ks) > 1}
    if full:
        mixed_new = sum(1 for ks in full.values() if len({any_fail(new[k]) for k in ks}) > 1)
        failing_new = sum(1 for ks in full.values() if any(any_fail(new[k]) for k in ks))
        base_full = {cid: ks for cid, ks in full.items() if all(k in base for k in ks)}
        mixed_base = sum(1 for ks in base_full.values() if len({any_fail(base[k]) for k in ks}) > 1)
        failing_base = sum(1 for ks in base_full.values() if any(any_fail(base[k]) for k in ks))
        print(f"\n== identical-evidence clusters judged in full: {len(full)}; "
              f"clusters with a failing member: baseline {failing_base}/{len(base_full)} -> new {failing_new}/{len(full)}; "
              f"MIXED (pass and fail inside one cluster): baseline {mixed_base} -> new {mixed_new}")
        # twins: same group_id in two chunks with identical evidence
        twins: dict[tuple[str, str, str, str], list[Key]] = defaultdict(list)
        for k in new:
            twins[(k[0], k[1], k[3], new[k].get("evidence_sha256") or "")].append(k)
        pairs = [ks for ks in twins.values() if len(ks) == 2]
        flips_new = sum(1 for a, b in pairs if any_fail(new[a]) != any_fail(new[b]))
        bp = [(a, b) for a, b in pairs if a in base and b in base]
        flips_base = sum(1 for a, b in bp if any_fail(base[a]) != any_fail(base[b]))
        print(f"== identical-evidence twins judged on both sides: new {flips_new}/{len(pairs)} flip; baseline {flips_base}/{len(bp)}")


def flip_readout(
    new: dict[Key, dict[str, Any]],
    base: dict[Key, dict[str, Any]],
    by_key: bool = False,
) -> None:
    """Per subject, then per (subject, field): pairs on identical evidence,
    any-fail and major counts on both sides, and the FLIPS — records whose
    verdict differs between the sides (newly failing + newly passing). A
    flip rate is only readable against a floor measured the same way."""
    paired = paired_keys(new, base, list(new), by_key)
    if not paired:
        print("\n== per subject: nothing paired" + ("" if by_key else " on identical evidence"))
        return

    def line(label: str, keys: list[Key]) -> None:
        n = len(keys)
        b_f = sum(any_fail(base[k]) for k in keys)
        n_f = sum(any_fail(new[k]) for k in keys)
        worse_f = sum(1 for k in keys if any_fail(new[k]) and not any_fail(base[k]))
        better_f = sum(1 for k in keys if any_fail(base[k]) and not any_fail(new[k]))
        b_m = sum(any_major(base[k]) for k in keys)
        n_m = sum(any_major(new[k]) for k in keys)
        flips_m = sum(1 for k in keys if any_major(new[k]) != any_major(base[k]))
        flips_f = worse_f + better_f
        print(
            f"  {label:42s} pairs {n:5d} | any-fail {b_f:4d} -> {n_f:4d}, "
            f"flips {flips_f:4d} = {100 * flips_f / n:4.1f}% (+{worse_f} -{better_f}) | "
            f"major {b_m:4d} -> {n_m:4d}, flips {flips_m:4d} = {100 * flips_m / n:4.1f}%"
        )

    def block(title: str, pool: list[Key]) -> None:
        print(f"\n== {title} (baseline -> new; flips = records whose verdict differs)")
        for subject in sorted({k[0] for k in pool}):
            keys = [k for k in pool if k[0] == subject]
            line(subject, keys)
            for field_name in sorted({k[1] for k in keys}):
                line(f"    {subject}/{field_name}", [k for k in keys if k[1] == field_name])
        line("ALL", pool)

    if not by_key:
        block("per subject / field, pairs on identical evidence", paired)
        return
    same = [k for k in paired if same_evidence(new, base, k)]
    changed = [k for k in paired if not same_evidence(new, base, k)]
    if same:
        block("per subject / field, pairs on IDENTICAL evidence (read against the floor)", same)
    if changed:
        block("per subject / field, pairs on CHANGED evidence (the lever plus the floor)", changed)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True)
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--population", default=None)
    parser.add_argument(
        "--by-subject",
        action="store_true",
        help="also print the per-subject and per-field flip readout (the A/A floor's number)",
    )
    parser.add_argument(
        "--by-key",
        action="store_true",
        help="pair on the key alone and keep evidence-changed pairs (fold/radius experiments)",
    )
    args = parser.parse_args()
    new = load_run_verdicts(args.run)
    base = load_baseline(args.baseline)
    pop_path = Path(args.population) if args.population else (
        scorecard.history_dir(args.run) / "pending" / "judged" / "POPULATION.json"
    )
    population: dict[Key, dict[str, Any]] = {}
    if pop_path.is_file():
        for k, v in json.loads(pop_path.read_text()).items():
            parts = k.split("|")
            population[(parts[0], parts[1], parts[2], parts[3])] = v
    print(f"run {args.run}: {len(new)} verdict rows; baseline {args.baseline}: {len(base)} ledger rows; population labels {len(population)}")
    readout(new, base, population, by_key=args.by_key)
    if args.by_subject:
        flip_readout(new, base, by_key=args.by_key)
    counts = Counter((population.get(k) or {}).get("population") or "all" for k in population)
    print(f"\npopulation planned: {dict(counts)}; judged so far: {len(new)}")


if __name__ == "__main__":
    main()
