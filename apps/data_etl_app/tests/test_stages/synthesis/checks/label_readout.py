"""The LABEL readout — distribution of the model's own party/capacity
decisions, and their per-record stability between two draws, with no judge.

  .venv/bin/python checks/label_readout.py --run <run_id> [--baseline <run_id>] [--subject <s>]...

Since 2026-09-13 every synthesis answer carries four labels the model decides
BEFORE writing the paragraph (synthesis design doc §31.3/§33): ``doer`` (who
the snippets give the doing to — the manufacturer / another party / nobody /
not shown), ``doer_name``, ``capacity`` (the manufacturer's own dealing, ten
values incl. "unstated" and "none") and ``dealing_words`` (the snippets' own
words). The dump carries them per record (``synthesis.records[].labels``).

For one run: the doer and capacity distributions per subject/field. With a
``--baseline`` run of the SAME subjects on the SAME pins (a second draw): per
record, whether ``doer`` and ``capacity`` agree between the draws — the
mechanical counterpart of the judged verdict flip (``paired_readout.py
--by-subject``), readable without judging — plus how the disagreements
cluster by request (``request_custom_id`` from the evidence snapshot). Pairs
only records whose evidence digest is equal on both sides. Read-only; no gate.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Optional

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from checks import loading, pull  # type: ignore[no-redef]
else:
    from . import loading, pull

Key = tuple[str, str, str, str]


def labels_of_run(
    run_id: str, subjects: tuple[str, ...]
) -> tuple[dict[Key, dict[str, str]], dict[Key, str], dict[Key, str]]:
    """``key → labels`` from the run's dumps, ``key → evidence digest`` and
    ``key → request_custom_id`` from its evidence snapshot (when pulled)."""
    labels: dict[Key, dict[str, str]] = {}
    for (subject, field_name), path in loading.dump_files(run_id).items():
        if field_name == loading.SHARED_DUPLICATE:
            continue
        if subjects and subject not in subjects:
            continue
        for record in loading.iter_records(subject, field_name, loading.load_dump(path)):
            row_labels = record.raw_row.get("labels")
            if isinstance(row_labels, dict) and row_labels.get("doer"):
                labels[record.pair_key] = {k: str(v) for k, v in row_labels.items()}
    digests: dict[Key, str] = {}
    requests: dict[Key, str] = {}
    index = pull.load_evidence_index(run_id) or {}
    for index_key, entry in index.items():
        parts = index_key.split("|")
        if len(parts) != 4:
            continue
        key: Key = (parts[0], parts[1], parts[2], parts[3])
        digests[key] = entry.get("evidence_sha256") or ""
        requests[key] = entry.get("request_custom_id") or ""
    return labels, digests, requests


def _share(n: int, d: int) -> str:
    return f"{n}/{d} = {100 * n / d:.1f}%" if d else f"{n}/{d}"


def distribution(labels: dict[Key, dict[str, str]]) -> None:
    by_sf: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for key, lab in labels.items():
        by_sf[(key[0], key[1])].append(lab)
    print(f"labelled records: {len(labels)}")
    for (subject, field_name), rows in sorted(by_sf.items()):
        doers = Counter(r["doer"] for r in rows)
        caps = Counter(r["capacity"] for r in rows)
        print(f"  {subject}/{field_name}: {len(rows)} records")
        print(f"      doer: {dict(doers.most_common())}")
        print(f"      capacity: {dict(caps.most_common())}")


def agreement(
    new: dict[Key, dict[str, str]],
    base: dict[Key, dict[str, str]],
    new_digests: dict[Key, str],
    base_digests: dict[Key, str],
    requests: dict[Key, str],
) -> None:
    paired = [
        k for k in new
        if k in base and new_digests.get(k) and new_digests.get(k) == base_digests.get(k)
    ]
    if not paired:
        print("\n== no records paired on identical evidence (pull both runs first)")
        return
    print(f"\n== label agreement between the two draws, {len(paired)} records paired on identical evidence")

    def line(label: str, keys: list[Key]) -> None:
        n = len(keys)
        d_flip = sum(1 for k in keys if new[k]["doer"] != base[k]["doer"])
        c_flip = sum(1 for k in keys if new[k]["capacity"] != base[k]["capacity"])
        either = sum(
            1 for k in keys
            if new[k]["doer"] != base[k]["doer"] or new[k]["capacity"] != base[k]["capacity"]
        )
        print(
            f"  {label:42s} pairs {n:5d} | doer flips {d_flip:4d} = {100 * d_flip / n:4.1f}% | "
            f"capacity flips {c_flip:4d} = {100 * c_flip / n:4.1f}% | either {either:4d} = {100 * either / n:4.1f}%"
        )

    for subject in sorted({k[0] for k in paired}):
        keys = [k for k in paired if k[0] == subject]
        line(subject, keys)
        for field_name in sorted({k[1] for k in keys}):
            line(f"    {subject}/{field_name}", [k for k in keys if k[1] == field_name])
    line("ALL", paired)
    # the most common transitions
    trans: Counter[tuple[str, str]] = Counter()
    for k in paired:
        if new[k]["capacity"] != base[k]["capacity"]:
            trans[(base[k]["capacity"], new[k]["capacity"])] += 1
    if trans:
        print("  most common capacity transitions (baseline → new):")
        for (a, b), n in trans.most_common(8):
            print(f"      {n:4d}  {a!r} → {b!r}")
    # per-request clustering of label flips (requests of the NEW run)
    by_req: dict[str, list[bool]] = defaultdict(list)
    for k in paired:
        req = requests.get(k)
        if req:
            by_req[req].append(
                new[k]["doer"] != base[k]["doer"] or new[k]["capacity"] != base[k]["capacity"]
            )
    multi = {r: v for r, v in by_req.items() if len(v) >= 2}
    flips = sum(sum(v) for v in multi.values())
    in_majority = sum(sum(v) for v in multi.values() if sum(v) * 2 > len(v))
    whole = sum(1 for v in multi.values() if all(v))
    print(
        f"  by request (new run, requests with >=2 paired records {len(multi)}): flipped records {flips}, "
        f"in majority-flipped requests {in_majority} ({_share(in_majority, flips) if flips else 'n/a'}), "
        f"whole-request flips {whole}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True)
    parser.add_argument("--baseline", default=None, help="a second draw of the same subjects on the same pins")
    parser.add_argument("--subject", action="append", default=[], help="restrict to a subject (repeatable)")
    args = parser.parse_args()
    subjects = tuple(args.subject)
    new, new_digests, requests = labels_of_run(args.run, subjects)
    distribution(new)
    if args.baseline:
        base, base_digests, _ = labels_of_run(args.baseline, subjects)
        agreement(new, base, new_digests, base_digests, requests)


if __name__ == "__main__":
    main()
