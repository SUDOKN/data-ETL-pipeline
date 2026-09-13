"""Offline N-sample MEDOID readout — what a majority over repeated generations
would buy, measured from draws that already exist.

  .venv/bin/python checks/medoid_readout.py --runs <run_a> <run_b> <run_c> [--subject <s>]...

Design doc §25/§29: gpt-4.1 lands a whole synthesis request in one reading, so
the user's N=3 idea is to generate each request three times and keep, per
record, the paragraph most similar to the other two (the medoid). Before that
is built, this readout simulates it from N judged draws of the same subjects
on the same pins (2026-09-12/13: runs 20260912T191548, 20260912T225723,
20260913T041535 at cap 50): for every record judged in all N draws it takes
the N paragraphs from the runs' judged work orders, chooses the TEXT medoid
(highest mean token-Jaccard similarity to the others, ties to the earliest
draw — D2's first-answer rule), and looks the chosen paragraph's verdict up in
the ledger by content key (no new judging). Prints, overall and per subject:
each single draw's any-fail and major rates, the medoid draw's rates, the
verdict-majority oracle's rates, how often the text medoid lands on the
verdict majority, the unanimity/split counts across the draws (the per-record
mode probability, p, seen directly), the mean pairwise disagreement between
single draws (the floor), and the mean disagreement between the medoid and a
single draw (what N=3 buys against today's one draw). Read-only; no gate.
"""

from __future__ import annotations

import argparse
import itertools
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from checks import loading, paired_readout, scorecard  # type: ignore[no-redef]
else:
    from . import loading, paired_readout, scorecard

Key = tuple[str, str, str, str]
_TOKEN_RE = re.compile(r"[a-z0-9]+")


def tokens(text: str) -> set[str]:
    return set(_TOKEN_RE.findall(text.lower()))


def jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    return len(a & b) / len(a | b)


def draw_paragraphs(run_id: str, subjects: tuple[str, ...]) -> dict[Key, tuple[str, str]]:
    """``key → (content_key, paragraph)`` from the run's judged work orders."""
    out: dict[Key, tuple[str, str]] = {}
    judged_dir = scorecard.history_dir(run_id) / "pending" / "judged"
    for path in sorted(judged_dir.glob("*__*.json")):
        order = json.loads(path.read_text(encoding="utf-8"))
        subject, field = order["subject"], order["field"]
        if subjects and subject not in subjects:
            continue
        for record in order.get("records") or []:
            paragraph = record.get("synthesis")
            if not paragraph or not record.get("content_key"):
                continue
            out[(subject, field, record["chunk_bounds"], record["group_id"])] = (
                record["content_key"],
                paragraph,
            )
    return out


def ledger_by_content_key(runs: tuple[str, ...]) -> dict[str, dict[str, Any]]:
    """Every judged row, keyed by content key: the cumulative ledger plus the
    runs' own verdict files (pre-finalize rows and corrected rows)."""
    rows: dict[str, dict[str, Any]] = {}
    for path in (loading.EVAL_ROOT / "history" / "judgments").glob("*.jsonl"):
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if row.get("content_key"):
                rows[row["content_key"]] = row
    for run_id in runs:
        for row in paired_readout.load_run_verdicts(run_id).values():
            if row.get("content_key"):
                rows[row["content_key"]] = row
    return rows


def rate(n: int, d: int) -> str:
    return f"{n}/{d} = {100 * n / d:.1f}%" if d else f"{n}/{d}"


def readout(runs: tuple[str, ...], subjects: tuple[str, ...]) -> None:
    draws = [draw_paragraphs(r, subjects) for r in runs]
    ledger = ledger_by_content_key(runs)
    common = [k for k in draws[0] if all(k in d for d in draws)]
    judged = [k for k in common if all(d[k][0] in ledger for d in draws)]
    print(
        f"draws {list(runs)}: records with a paragraph in every draw {len(common)}, "
        f"judged in every draw {len(judged)} (missing verdicts: "
        f"{[sum(1 for k in common if d[k][0] not in ledger) for d in draws]})"
    )
    if not judged:
        return
    n = len(runs)
    per_subject: dict[str, list[Key]] = defaultdict(list)
    for k in judged:
        per_subject[k[0]].append(k)
    per_subject["ALL"] = list(judged)

    # per record: verdict vector (any-fail, major) per draw, text medoid index, majority
    fail_vec: dict[Key, list[bool]] = {}
    major_vec: dict[Key, list[bool]] = {}
    medoid_idx: dict[Key, int] = {}
    text_split: dict[Key, bool] = {}
    for k in judged:
        rows = [ledger[d[k][0]] for d in draws]
        fail_vec[k] = [paired_readout.any_fail(r) for r in rows]
        major_vec[k] = [paired_readout.any_major(r) for r in rows]
        toks = [tokens(d[k][1]) for d in draws]
        sims = [[jaccard(toks[i], toks[j]) for j in range(n)] for i in range(n)]
        mean_sim = [sum(sims[i][j] for j in range(n) if j != i) / (n - 1) for i in range(n)]
        best = max(mean_sim)
        medoid_idx[k] = next(i for i in range(n) if mean_sim[i] == best)  # ties → earliest draw
        text_split[k] = min(sims[i][j] for i in range(n) for j in range(n) if i < j) < 0.5

    pairs = list(itertools.combinations(range(n), 2))
    for label, keys in per_subject.items():
        m = len(keys)
        print(f"\n== {label}: {m} records judged in all {n} draws")
        for name, vec in (("any-fail", fail_vec), ("major", major_vec)):
            singles = [sum(vec[k][i] for k in keys) for i in range(n)]
            medoid = sum(vec[k][medoid_idx[k]] for k in keys)
            majority = sum(1 for k in keys if sum(vec[k]) * 2 > n)
            unanimous_fail = sum(1 for k in keys if all(vec[k]))
            unanimous_pass = sum(1 for k in keys if not any(vec[k]))
            split = m - unanimous_fail - unanimous_pass
            pair_disagree = sum(sum(1 for k in keys if vec[k][i] != vec[k][j]) for i, j in pairs) / len(pairs)
            medoid_vs_single = sum(sum(1 for k in keys if vec[k][i] != vec[k][medoid_idx[k]]) for i in range(n)) / n
            on_majority = sum(1 for k in keys if vec[k][medoid_idx[k]] == (sum(vec[k]) * 2 > n))
            print(
                f"  {name:8s} single draws {[rate(s, m) for s in singles]} | text-medoid {rate(medoid, m)} | "
                f"verdict-majority oracle {rate(majority, m)}"
            )
            print(
                f"           unanimous pass {unanimous_pass}, unanimous fail {unanimous_fail}, SPLIT {split} "
                f"({100 * split / m:.1f}%) | floor: single-vs-single disagreement {pair_disagree:.1f}/{m} = "
                f"{100 * pair_disagree / m:.1f}% | medoid-vs-single {medoid_vs_single:.1f}/{m} = "
                f"{100 * medoid_vs_single / m:.1f}% | medoid on the majority verdict {on_majority}/{m}"
            )
        ts = sum(1 for k in keys if text_split[k])
        print(f"  text: records where some pair of draws has token-Jaccard < 0.5: {ts}/{m} = {100 * ts / m:.1f}%; medoid drawn from {[sum(1 for k in keys if medoid_idx[k] == i) for i in range(n)]}")
    # per field split table (any-fail), all subjects
    print("\n== split records by subject/field (any-fail; records where the draws disagree):")
    by_field: dict[tuple[str, str], list[Key]] = defaultdict(list)
    for k in judged:
        by_field[(k[0], k[1])].append(k)
    for (s, f), keys in sorted(by_field.items()):
        split = sum(1 for k in keys if any(fail_vec[k]) and not all(fail_vec[k]))
        if split:
            print(f"    {s}/{f}: {split}/{len(keys)} split ({100 * split / len(keys):.1f}%), unanimous fail {sum(1 for k in keys if all(fail_vec[k]))}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", nargs="+", required=True, help="two or more judged runs of the same subjects on the same pins")
    parser.add_argument("--subject", action="append", default=[], help="restrict to a subject (repeatable)")
    args = parser.parse_args()
    if len(args.runs) < 2:
        raise SystemExit("--runs needs at least two runs")
    readout(tuple(args.runs), tuple(args.subject))


if __name__ == "__main__":
    main()
