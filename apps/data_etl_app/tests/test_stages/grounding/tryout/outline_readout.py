"""Read the judges' verdicts of the OUTLINE tryout sample back against the six
arms (2026-09-21): round 5's arm ``b`` (the outline below the records) and the
five outline arms ``b-dash`` / ``b-today`` / ``b-defs2`` / ``b-layered`` /
``b-sandwich`` in the cacheable layout.

Joins ``out/judge_outline/verdicts/*.jsonl`` to ``out/judge_outline/sample.jsonl``
by item_id (last row wins on a resumed judge; coverage checked first). Each
sampled row is one (record, label) pair that some arms' modes carry and others
do not, coded once. Per arm, this prints: the disputed pairs it carries by code
(what the arm alone adds is clean or defective), the clean disputed pairs it
LACKS (recall it loses to the other arms), and the same split by field. Pairs
every arm carries are shared and were not judged; their count comes from
``CHECK.json`` (``mode_vs_round5_b``) and is printed for scale.

Usage (from the repo root, after the judges):
    .venv/bin/python apps/data_etl_app/tests/test_stages/grounding/tryout/outline_readout.py
"""

from __future__ import annotations

import json
import pathlib
from collections import Counter, defaultdict

HERE = pathlib.Path(__file__).resolve().parent
OUT = HERE / "out"
JUDGE = OUT / "judge_outline"

CLEAN = {"D", "N"}


def main() -> None:
    sample = {json.loads(l)["item_id"]: json.loads(l) for l in (JUDGE / "sample.jsonl").read_text().splitlines() if l.strip()}
    verdicts: dict[str, dict] = {}
    for p in sorted((JUDGE / "verdicts").glob("*.jsonl")):
        for line in p.read_text().splitlines():
            if line.strip():
                v = json.loads(line)
                verdicts[v["item_id"]] = v
    missing = sorted(set(sample) - set(verdicts))
    unknown = sorted(set(verdicts) - set(sample))
    print(f"sample {len(sample)}, judged {len(verdicts)}, missing {len(missing)}, unknown {len(unknown)}")
    if missing:
        print("  missing:", missing[:20])

    arms: list[str] = sorted({a for r in sample.values() for a in r["arms"]} | {"b"})
    carried: dict[str, Counter[str]] = defaultdict(Counter)
    lacked_clean: Counter[str] = Counter()
    by_field: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    quote_ok: dict[str, Counter[str]] = defaultdict(Counter)
    for item_id, row in sample.items():
        v = verdicts.get(item_id)
        if v is None:
            continue
        code = v.get("code", "?")
        clean = code in CLEAN
        for arm in arms:
            if arm in row["arms"]:
                carried[arm][code] += 1
                by_field[(arm, row["field"])]["clean" if clean else "defective"] += 1
                if v.get("quote_ok"):
                    quote_ok[arm][v["quote_ok"]] += 1
            elif clean:
                lacked_clean[arm] += 1
                by_field[(arm, row["field"])]["lacked_clean"] += 1

    print("\n## Per arm: disputed pairs carried, by code; clean disputed pairs the arm lacks")
    print("| arm | carried | clean | defective | clean share | lacks clean | codes |")
    print("|---|---|---|---|---|---|---|")
    for arm in arms:
        c = carried[arm]
        n = sum(c.values())
        cl = sum(v for k, v in c.items() if k in CLEAN)
        print(f"| {arm} | {n} | {cl} | {n - cl} | {100 * cl / n if n else 0:.0f}% | {lacked_clean[arm]} | {dict(sorted(c.items()))} |")

    print("\n## Per arm × field: clean / defective carried, clean lacked")
    fields = sorted({f for (_, f) in by_field})
    print("| arm | " + " | ".join(fields) + " |")
    print("|---|" + "---|" * len(fields))
    for arm in arms:
        cells = []
        for f in fields:
            c = by_field[(arm, f)]
            cells.append(f"{c['clean']} / {c['defective']} / lacks {c['lacked_clean']}")
        print(f"| {arm} | " + " | ".join(cells) + " |")

    print("\n## Shared pairs (every arm agrees; not judged) — from CHECK.json")
    check = OUT / "CHECK.json"
    if check.exists():
        shared: Counter[str] = Counter()
        for rep in json.loads(check.read_text()):
            for arm, mo in (rep.get("mode_vs_round5_b") or {}).items():
                shared[arm] += mo["both"]
        print("  pairs in both round-5 b and the arm's mode, summed over targets:", dict(sorted(shared.items())))

    print("\n## Proposals among the disputed pairs, by code and arm")
    prop: dict[str, Counter[str]] = defaultdict(Counter)
    for item_id, row in sample.items():
        v = verdicts.get(item_id)
        if v is None or not row["proposal"]:
            continue
        for arm in row["arms"]:
            prop[arm][v.get("code", "?")] += 1
    for arm in arms:
        if prop[arm]:
            print(f"  {arm}: {dict(sorted(prop[arm].items()))}")

    print("\n## Quote verdicts per arm (quote_ok)")
    for arm in arms:
        if quote_ok[arm]:
            print(f"  {arm}: {dict(sorted(quote_ok[arm].items()))}")


if __name__ == "__main__":
    main()
