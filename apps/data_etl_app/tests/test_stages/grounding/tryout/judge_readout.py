"""Read the judges' verdicts of the Step 2 tryout sample back against the arms.

Joins ``out/judge/verdicts/*.jsonl`` to ``out/judge/sample.jsonl`` by item_id
and prints, per kind: the code distribution per arm (what today's arm alone
produced vs what the Step 2 arm alone produced — the recall-vs-defect question
for grounding and freehand), the quote verdicts for arm b, and for screening
the confusion of each screening arm's mode verdict against the judged truth
plus the evidence-distance calibration (the model's named/inferred against the
judge's distance). Coverage is checked first: every sampled item once.

Usage (from the repo root):
    .venv/bin/python apps/data_etl_app/tests/test_stages/grounding/tryout/judge_readout.py
"""

from __future__ import annotations

import json
import pathlib
from collections import Counter, defaultdict

HERE = pathlib.Path(__file__).resolve().parent
JUDGE = HERE / "out" / "judge"

CLEAN = {"D", "N"}


def main() -> None:
    sample = {json.loads(l)["item_id"]: json.loads(l) for l in (JUDGE / "sample.jsonl").read_text().splitlines() if l.strip()}
    verdicts: dict[str, dict] = {}
    dups: Counter[str] = Counter()
    for p in sorted((JUDGE / "verdicts").glob("*.jsonl")):
        for line in p.read_text().splitlines():
            if not line.strip():
                continue
            v = json.loads(line)
            dups[v["item_id"]] += 1
            verdicts[v["item_id"]] = v  # last wins on a resumed judge
    missing = sorted(set(sample) - set(verdicts))
    unknown = sorted(set(verdicts) - set(sample))
    print(f"sample {len(sample)}, judged {len(verdicts)}, missing {len(missing)}, unknown {len(unknown)}, duplicated {sum(1 for n in dups.values() if n > 1)}")
    if missing:
        print("  missing:", missing[:20])

    lines: list[str] = ["# Step 2 tryout — judged sample readout", ""]

    for kind, arm_key in (("grounding", "arms"), ("freehand", "arms")):
        by_arm: dict[str, Counter[str]] = defaultdict(Counter)
        by_field_arm: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
        quote_ok: Counter[str] = Counter()
        for iid, row in sample.items():
            if row["kind"] != kind or iid not in verdicts:
                continue
            v = verdicts[iid]
            code = v.get("code", "?")
            if code == "X" and v.get("sub_kind"):
                code = v["sub_kind"]
            by_arm[row[arm_key]][code] += 1
            by_field_arm[(row["field"], row[arm_key])][code] += 1
            if row[arm_key] == "b" and v.get("quote_ok"):
                quote_ok[v["quote_ok"]] += 1
        lines.append(f"## {kind}: codes by arm (single-arm rows = what only that arm produced)")
        for arm, c in sorted(by_arm.items()):
            n = sum(c.values())
            clean = sum(v for k, v in c.items() if k in CLEAN)
            lines.append(f"- arm {arm}: n={n}, clean {clean} ({clean / n:.0%}), {dict(sorted(c.items()))}")
        lines.append("  by field:")
        for (field, arm), c in sorted(by_field_arm.items()):
            n = sum(c.values())
            clean = sum(v for k, v in c.items() if k in CLEAN)
            lines.append(f"  - {field} / {arm}: n={n}, clean {clean / n:.0%}, {dict(sorted(c.items()))}")
        if quote_ok:
            lines.append(f"- arm b quote_ok: {dict(quote_ok)}")
        lines.append("")

    # screening
    conf: dict[str, Counter[str]] = {"sa": Counter(), "sb": Counter()}
    calib: Counter[str] = Counter()
    reason_ok: Counter[str] = Counter()
    by_why: Counter[str] = Counter()
    for iid, row in sample.items():
        if row["kind"] != "screening" or iid not in verdicts:
            continue
        v = verdicts[iid]
        truth = v.get("truth")
        by_why[row["why"]] += 1
        for arm in ("sa", "sb"):
            got = "accept" if row[f"{arm}_accepts"] else "reject"
            conf[arm][f"truth={truth}/{arm}={got}"] += 1
        if row["sb_accepts"]:
            model = Counter(d.get("evidence") for d in row.get("sb_detail", []) if d.get("accepted")).most_common(1)
            model_label = model[0][0] if model else "?"
            calib[f"model={model_label}/judge={v.get('distance')}"] += 1
        elif v.get("reason_ok"):
            reason_ok[v["reason_ok"]] += 1
    lines.append("## screening: mode verdict vs judged truth (rows: disagreements, sb 'inferred' acceptances, agreed slice)")
    lines.append(f"- rows by why: {dict(by_why)}")
    for arm in ("sa", "sb"):
        c = conf[arm]
        n = sum(c.values())
        right = c[f"truth=accept/{arm}=accept"] + c[f"truth=reject/{arm}=reject"]
        wrong_acc = c[f"truth=reject/{arm}=accept"]
        wrong_rej = c[f"truth=accept/{arm}=reject"]
        lines.append(f"- {arm}: n={n}, right {right} ({right / n:.0%}), wrong accepts {wrong_acc}, wrong rejects {wrong_rej}, {dict(sorted(c.items()))}")
    lines.append(f"- sb evidence-distance calibration (accepted rows): {dict(sorted(calib.items()))}")
    lines.append(f"- sb rejection reason judged: {dict(reason_ok)}")
    lines.append("")

    # the relationship-field packet (products / contract_products), if judged
    rel = HERE / "out" / "judge_rel"
    if (rel / "sample.jsonl").exists() and (rel / "verdicts").exists():
        rsample = {json.loads(l)["item_id"]: json.loads(l) for l in (rel / "sample.jsonl").read_text().splitlines() if l.strip()}
        rverd: dict[str, dict] = {}
        for p in sorted((rel / "verdicts").glob("*.jsonl")):
            for line in p.read_text().splitlines():
                if line.strip():
                    v = json.loads(line)
                    rverd[v["item_id"]] = v
        lines.append(f"## relationship fields (products / contract_products): {len(rverd)} of {len(rsample)} rows judged on the field's own relationship")
        per: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
        for iid, row in rsample.items():
            if iid not in rverd:
                continue
            truth = rverd[iid].get("truth")
            for arm in ("sa", "sb"):
                got = "accept" if row[f"{arm}_accepts"] else "reject"
                per[(row["field"], arm)][f"truth={truth}/{arm}={got}"] += 1
        for (field, arm), c in sorted(per.items()):
            n = sum(c.values())
            right = c[f"truth=accept/{arm}=accept"] + c[f"truth=reject/{arm}=reject"]
            lines.append(f"- {field} / {arm}: n={n}, right {right} ({right / n:.0%}), {dict(sorted(c.items()))}")
        lines.append("")

    text = "\n".join(lines) + "\n"
    (JUDGE / "READOUT.md").write_text(text)
    print(text)


if __name__ == "__main__":
    main()
