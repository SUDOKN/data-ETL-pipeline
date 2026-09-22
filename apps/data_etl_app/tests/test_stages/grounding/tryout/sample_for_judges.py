"""Draw the judged sample of the Step 2 tryout: the rows a reader has to code.

The mechanical check (``check_tryout.py``) settles coverage, membership,
quotes, stability and cost. What it cannot settle is whether a label is RIGHT
— the census codes (D/N clean, X wrong kind, V vague, B bridge, P wrong
party, F fabricated) and, for screening, whether the verdict and the
evidence-distance label are right. This draws those rows from the arms'
MODES (the labels a majority of repeats agree on), stratified so the shapes
the design predicts should move are over-represented:

- grounding, concept fields: every (record, label) pair in exactly one arm's
  mode (a-only, b-only) plus a random slice of pairs in both; b's proposals.
- grounding, freehand fields: every candidate in exactly one arm's mode plus a
  slice of both; every candidate whose words the record does not carry.
- screening: every pair the two screening arms' modes disagree on, every
  ``sb`` acceptance labelled "inferred", and a slice of agreed pairs.

Output: ``out/judge/sample.jsonl`` (one row per item with the record as the
model saw it, the arm(s), the label, the quote where the arm gave one, and the
screening verdicts) plus ``out/judge/packet_<k>.jsonl`` cuts of 40 rows and
the brief ``JUDGE_BRIEF.md`` beside them.

Usage (from the repo root, after check_tryout.py):
    .venv/bin/python apps/data_etl_app/tests/test_stages/grounding/tryout/sample_for_judges.py [--size 300] [--seed 7]
"""

from __future__ import annotations

import argparse
import json
import pathlib
import random
import sys
from collections import defaultdict
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
OUT = HERE / "out"
JUDGE = OUT / "judge"
sys.path.insert(0, str(HERE))
from check_tryout import (  # noqa: E402
    _series,
    mode_labels,
    parse_arm_a,
    parse_arm_b,
    parse_freehand,
    parse_sa,
    parse_sb,
    verdict_stability,
)
from run_grounding_tryout import (  # noqa: E402
    CONCEPT_FIELDS,
    Target,
    build_units,
    concept_by_label,
    request_groups,
)


def _quotes_b(d: pathlib.Path, labels: dict[str, Any]) -> dict[tuple[str, str], list[str]]:
    """(record, label) -> the quotes arm b gave across repeats."""
    from check_tryout import _canonical  # noqa: E402

    quotes: dict[tuple[str, str], list[str]] = defaultdict(list)
    for p in _series(d, "b_{k}.json"):
        for entry in json.loads(p.read_text()).get("groundings") or []:
            rid = entry["record_id"]
            for o in entry.get("options") or []:
                canon = _canonical(o["option"], labels)
                quotes[(rid, canon or f"OOV:{o['option']}")].append(o.get("quote", ""))
            for pr in entry.get("proposals") or []:
                canon = _canonical(pr["label"], labels)
                quotes[(rid, canon or f"OOV:{pr['label']}")].append(pr.get("quote", ""))
    return quotes


def _quotes_freehand(d: pathlib.Path, arm: str) -> dict[tuple[str, str], list[str]]:
    quotes: dict[tuple[str, str], list[str]] = defaultdict(list)
    for p in _series(d, arm + "_{k}.json"):
        for entry in json.loads(p.read_text()).get("groundings") or []:
            for unit in entry.get("candidates") or []:
                e1 = unit.get("FGR-E1") or {}
                quotes[(entry["record_id"], unit["candidate"])].append(e1.get("explanation", ""))
    return quotes


def _sb_detail(d: pathlib.Path) -> dict[tuple[str, str], list[dict[str, Any]]]:
    detail: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for p in _series(d, "sb_{k}.json"):
        for entry in json.loads(p.read_text()).get("screenings") or []:
            for r in entry.get("accepted") or []:
                detail[(r["record_id"], entry["option"])].append({"accepted": True, "evidence": r.get("evidence"), "quote": r.get("quote")})
            for r in entry.get("not_accepted") or []:
                detail[(r["record_id"], entry["option"])].append({"accepted": False, "failed_rule": r.get("failed_rule"), "quote": r.get("quote")})
    return detail


def rows_for_target(tag: str, rng: random.Random) -> list[dict[str, Any]]:
    d = OUT / tag
    meta = json.loads((d / "request_meta.json").read_text())
    field, sent = meta["field"], meta["record_ids"]
    records = request_groups(meta["run"], meta["subject"], field, meta["chunk"])[meta["group_index"]]
    concept = field in CONCEPT_FIELDS
    labels = concept_by_label(field) if concept else {}
    base = {"tag": tag, "subject": meta["subject"], "field": field}
    rows: list[dict[str, Any]] = []

    def record_view(rid: str) -> dict[str, Any]:
        return {"record_id": rid, "focal_form": records[rid]["focal_form"], "synthesis": records[rid]["synthesis"]}

    if concept:
        runs_a = [parse_arm_a(iv.read_text(), (d / f"a_oov_{k}.json").read_text(), labels) for k, iv in enumerate(_series(d, "a_iv_{k}.json")) if (d / f"a_oov_{k}.json").exists()]
        runs_b = [parse_arm_b(p.read_text(), sent, records, labels) for p in _series(d, "b_{k}.json")]
        ma, mb = mode_labels(runs_a), mode_labels(runs_b)
        qb = _quotes_b(d, labels)
        pa = {(r, l) for r, ls in ma.items() for l in ls}
        pb = {(r, l) for r, ls in mb.items() for l in ls}
        both = sorted(pa & pb)
        rng.shuffle(both)
        for pair, arms in [*((p, "a") for p in sorted(pa - pb)), *((p, "b") for p in sorted(pb - pa)), *((p, "ab") for p in both[: max(5, len(both) // 5)])]:
            rid, label = pair
            rows.append({**base, "kind": "grounding", "arms": arms, "label": label, "proposal": label.startswith("OOV:"), **record_view(rid), "quotes_b": qb.get(pair, [])[:3]})
    else:
        runs_fa = [parse_freehand(p.read_text(), sent, records) for p in _series(d, "fa_{k}.json")]
        runs_fb = [parse_freehand(p.read_text(), sent, records) for p in _series(d, "fb_{k}.json")]
        ma, mb = mode_labels(runs_fa), mode_labels(runs_fb)
        qa, qb2 = _quotes_freehand(d, "fa"), _quotes_freehand(d, "fb")
        pa = {(r, l) for r, ls in ma.items() for l in ls}
        pb = {(r, l) for r, ls in mb.items() for l in ls}
        both = sorted(pa & pb)
        rng.shuffle(both)
        for pair, arms in [*((p, "fa") for p in sorted(pa - pb)), *((p, "fb") for p in sorted(pb - pa)), *((p, "fafb") for p in both[: max(5, len(both) // 3)])]:
            rid, label = pair
            rows.append({**base, "kind": "freehand", "arms": arms, "label": label, **record_view(rid), "explanations_fa": qa.get(pair, [])[:2], "explanations_fb": qb2.get(pair, [])[:2]})

    cand_files = sorted(d.glob("candidates_from_*.json"))
    sa_files, sb_files = _series(d, "sa_{k}.json"), _series(d, "sb_{k}.json")
    if cand_files and sa_files and sb_files:
        candidates = json.loads(cand_files[-1].read_text())
        units = build_units(Target(f"{meta['subject']}:{field}:{meta['chunk']}:{meta['group_index']}"), candidates)
        vsa = verdict_stability([parse_sa(p.read_text()) for p in sa_files])["mode"]
        vsb = verdict_stability([parse_sb(p.read_text(), units) for p in sb_files])["mode"]
        detail = _sb_detail(d)
        shared = sorted(set(vsa) & set(vsb))
        picked: list[tuple[tuple[str, str], str]] = []
        for pair in shared:
            if vsa[pair] != vsb[pair]:
                picked.append((pair, "disagree"))
            elif vsb[pair] and any(x.get("evidence") == "inferred" for x in detail.get(pair, [])):
                picked.append((pair, "inferred"))
        agreed = [p for p in shared if vsa[p] == vsb[p] and (p, "inferred") not in picked]
        rng.shuffle(agreed)
        picked.extend((p, "agree") for p in agreed[: max(4, len(agreed) // 6)])
        for (rid, label), why in picked:
            rows.append({**base, "kind": "screening", "why": why, "label": label, **record_view(rid), "sa_accepts": vsa[(rid, label)], "sb_accepts": vsb[(rid, label)], "sb_detail": detail.get((rid, label), [])[:3]})
    return rows


def relationship_rows(tag: str) -> list[dict[str, Any]]:
    """EVERY screened (record, label) pair of a products / contract_products
    target, for the relationship-field judge packet (JUDGE_BRIEF_RELATIONSHIP.md):
    the earlier sample judged these fields on the generic dealing question."""
    d = OUT / tag
    meta = json.loads((d / "request_meta.json").read_text())
    field = meta["field"]
    records = request_groups(meta["run"], meta["subject"], field, meta["chunk"])[meta["group_index"]]
    cand_files = sorted(d.glob("candidates_from_*.json"))
    sa_files, sb_files = _series(d, "sa_{k}.json"), _series(d, "sb_{k}.json")
    if not (cand_files and sa_files and sb_files):
        return []
    candidates = json.loads(cand_files[-1].read_text())
    units = build_units(Target(f"{meta['subject']}:{field}:{meta['chunk']}:{meta['group_index']}"), candidates)
    vsa = verdict_stability([parse_sa(p.read_text()) for p in sa_files])["mode"]
    vsb = verdict_stability([parse_sb(p.read_text(), units) for p in sb_files])["mode"]
    detail = _sb_detail(d)
    names = json.loads((HERE / "subject_names.json").read_text())
    rows = []
    for rid, label in sorted(set(vsa) & set(vsb)):
        rows.append({"tag": tag, "subject": meta["subject"], "manufacturer": names.get(meta["subject"]), "field": field, "kind": "screening_relationship",
                     "label": label, "record_id": rid, "focal_form": records[rid]["focal_form"], "synthesis": records[rid]["synthesis"],
                     "sa_accepts": vsa[(rid, label)], "sb_accepts": vsb[(rid, label)], "sb_detail": detail.get((rid, label), [])[:3]})
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--size", type=int, default=300)
    ap.add_argument("--packet", type=int, default=40)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--targets", nargs="*", default=[])
    ap.add_argument("--relationship", action="store_true", help="draw every screened pair of the products / contract_products targets into out/judge_rel/")
    args = ap.parse_args()
    if args.relationship:
        rows: list[dict[str, Any]] = []
        for p in sorted(OUT.iterdir()):
            if p.is_dir() and (p / "request_meta.json").exists() and json.loads((p / "request_meta.json").read_text())["field"] in ("products", "contract_products"):
                rows.extend(relationship_rows(p.name))
        rel = OUT / "judge_rel"
        rel.mkdir(parents=True, exist_ok=True)
        for i, r in enumerate(rows):
            r["item_id"] = f"r_{i:04d}"
        for k in range(0, len(rows), args.packet):
            (rel / f"packet_{k // args.packet}.jsonl").write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows[k : k + args.packet]))
        (rel / "sample.jsonl").write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows))
        print(f"{len(rows)} relationship rows under {rel}")
        return
    rng = random.Random(args.seed)
    tags = args.targets or sorted(p.name for p in OUT.iterdir() if p.is_dir() and (p / "request_meta.json").exists())
    rows: list[dict[str, Any]] = []
    for tag in tags:
        rows.extend(rows_for_target(tag, rng))
    by_kind: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_kind[r["kind"]].append(r)
    # Cap per kind proportionally, keeping every "disagree"/single-arm row first.
    def priority(r: dict[str, Any]) -> int:
        if r["kind"] == "screening":
            return {"disagree": 0, "inferred": 1}.get(r.get("why", ""), 2)
        return 0 if r["arms"] in ("a", "b", "fa", "fb") else 1
    chosen: list[dict[str, Any]] = []
    share = {"grounding": 0.45, "freehand": 0.25, "screening": 0.30}
    for kind, xs in by_kind.items():
        xs.sort(key=priority)
        chosen.extend(xs[: int(args.size * share.get(kind, 0.3))])
    rng.shuffle(chosen)
    JUDGE.mkdir(parents=True, exist_ok=True)
    for i, r in enumerate(chosen):
        r["item_id"] = f"j_{i:04d}"
    (JUDGE / "sample.jsonl").write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in chosen))
    for k in range(0, len(chosen), args.packet):
        (JUDGE / f"packet_{k // args.packet}.jsonl").write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in chosen[k : k + args.packet]))
    counts = {kind: sum(1 for r in chosen if r["kind"] == kind) for kind in by_kind}
    print(f"{len(chosen)} rows drawn from {len(rows)} candidates: {counts}; packets of {args.packet} under {JUDGE}")


if __name__ == "__main__":
    main()
