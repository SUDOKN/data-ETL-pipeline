"""Mechanical readout of the Step 2 tryout outputs (no judgment).

Per target and arm, over the N repeats:

- grounding arms ``a`` (today's two calls) and ``b`` (the one three-list call):
  parse success; coverage of the sent records (b: every sent id in at least
  one records list or in ``unmatched``, no unknown ids, no id both supported
  and unmatched); membership (b: matched options that are vocabulary labels
  after the parenthetical strip and casing repair — a non-label is a reroute
  to ``proposed`` under V3, a proposal that IS a label folds into matched);
  quotes (b: every listed record carries a non-empty quote found in that
  record's focal form or synthesis after whitespace and case folding — the V1
  evidence test the parser will enforce); multi-record entries (b: matched
  entries citing two or more records); per-record label maps and their
  STABILITY across repeats (mean pairwise Jaccard over (record, label) pairs
  and the share of records whose label set is identical in every repeat); and
  the label-set overlap between the two arms' modes.
- freehand arms ``fa`` (today's text) and ``fb`` (the rewording): candidates
  per record; the share of candidates whose words the record carries
  (``candidate_in_record``: the whole candidate string; ``head_in_record``:
  its last word) — a mechanical proxy for the naming rule, not a verdict;
  stability and mode overlap.
- screening arms ``sa`` (today's prompt) and ``sb`` (unit screening), both on
  the same mode candidates: per (record, label) verdicts; acceptance rates;
  ``sb``'s evidence-distance and failed-rule distributions; the agreement
  matrix on shared pairs; verdict stability across repeats; ``sb``'s unit
  coverage (every unit answered once, each record of a unit in exactly one of
  accepted / not_accepted).
- cost per arm from the ``.usage.json`` files, priced at gpt-4.1 list.

Everything that needs reading — multi-record inference quality, proposal
quality, fabricated machines, own-line sectors, evidence-distance calibration
— goes to judges; ``sample_for_judges.py`` draws that sample from CHECK.json.

Usage (from the repo root):
    .venv/bin/python apps/data_etl_app/tests/test_stages/grounding/tryout/check_tryout.py [--targets tag ...]
"""

from __future__ import annotations

import argparse
import itertools
import json
import pathlib
import re
import sys
from collections import Counter, defaultdict
from typing import Any, Optional

HERE = pathlib.Path(__file__).resolve().parent
OUT = HERE / "out"
sys.path.insert(0, str(HERE))
from run_grounding_tryout import (  # noqa: E402
    CONCEPT_FIELDS,
    chunk_records,
    concept_by_label,
    request_groups,
)

PRICE_PER_M_INPUT = 2.0  # gpt-4.1 list, USD
PRICE_PER_M_OUTPUT = 8.0

_WS = re.compile(r"\s+")
_ELLIPSIS = re.compile(r"\s*(?:\.\.\.|…)\s*")


def _norm(text: str) -> str:
    return _WS.sub(" ", text.replace("’", "'").replace("“", '"').replace("”", '"')).strip().casefold()


def quote_found(quote: str, record: dict[str, Any]) -> bool:
    """Every ellipsis-separated part of the quote occurs in the record's focal
    form or synthesis after whitespace/case folding. The parser's rule."""
    haystack = _norm(f"{record.get('focal_form', '')}\n{record.get('synthesis', '')}")
    parts = [p for p in _ELLIPSIS.split(_norm(quote)) if p]
    return bool(parts) and all(p in haystack for p in parts)


def _canonical(option: str, labels: dict[str, Any]) -> Optional[str]:
    from core.services.pipeline_nodes.multi_stage.llm_grounding_node_service import (  # noqa: E402
        _strip_trailing_parenthetical,
    )

    concept = labels.get(option.strip().casefold())
    if concept is None:
        concept = labels.get(_strip_trailing_parenthetical(option).casefold())
    return concept.name if concept is not None else None


# --- grounding parsers ------------------------------------------------------------


def parse_arm_a(iv_text: str, oov_text: str, labels: dict[str, Any]) -> dict[str, Any]:
    per_record: dict[str, set[str]] = defaultdict(set)
    proposals: set[str] = set()
    drops = 0
    for text, minted in ((iv_text, False), (oov_text, True)):
        for entry in json.loads(text).get("groundings") or []:
            rid = entry["record_id"]
            per_record.setdefault(rid, set())
            for unit in entry.get("options") or entry.get("candidates") or []:
                label = unit.get("option") or unit.get("candidate")
                canon = _canonical(label, labels)
                if canon:
                    per_record[rid].add(canon)
                elif minted:
                    per_record[rid].add(f"OOV:{label}")
                    proposals.add(label)
                else:
                    drops += 1
    return {"per_record": {k: sorted(v) for k, v in per_record.items()}, "proposals": sorted(proposals), "drops": drops}


def parse_arm_b(text: str, sent: list[str], records: dict[str, dict[str, Any]], labels: dict[str, Any]) -> dict[str, Any]:
    doc = json.loads(text)
    per_record: dict[str, set[str]] = {rid: set() for rid in sent}
    unknown: set[str] = set()
    supported: set[str] = set()
    unmatched: set[str] = set()
    reroutes: list[str] = []
    folds: list[str] = []
    proposals: list[str] = []
    quotes_total = quotes_empty = quotes_found = 0
    options_seen: Counter[str] = Counter()
    multi = 0
    chosen: Counter[str] = Counter()

    def take(rid: str, label: str, quote: str) -> None:
        nonlocal quotes_total, quotes_empty, quotes_found
        if rid not in per_record:
            unknown.add(rid)
            return
        supported.add(rid)
        quotes_total += 1
        if not quote.strip():
            quotes_empty += 1
        elif quote_found(quote, records[rid]):
            quotes_found += 1
        per_record[rid].add(label)

    for entry in doc.get("matched") or []:
        canon = _canonical(entry["option"], labels)
        options_seen[(canon or entry["option"]).casefold()] += 1
        if canon is None:
            reroutes.append(entry["option"])
        chosen[(entry.get("chosen") or {}).get("rule_id", "?")] += 1
        recs = entry.get("records") or []
        if len(recs) >= 2:
            multi += 1
        for rec in recs:
            take(rec["record_id"], canon or f"OOV:{entry['option']}", rec.get("quote", ""))
    for entry in doc.get("proposed") or []:
        canon = _canonical(entry["label"], labels)
        if canon:
            folds.append(entry["label"])
        else:
            proposals.append(entry["label"])
        for rec in entry.get("records") or []:
            take(rec["record_id"], canon or f"OOV:{entry['label']}", rec.get("quote", ""))
    for entry in doc.get("unmatched") or []:
        rid = entry["record_id"]
        if rid not in per_record:
            unknown.add(rid)
            continue
        unmatched.add(rid)
    covered = supported | unmatched
    return {
        "per_record": {k: sorted(v) for k, v in per_record.items()},
        "proposals": sorted(set(proposals)),
        "folds": sorted(set(folds)),
        "reroutes": reroutes,
        "coverage": {
            "missing": [rid for rid in sent if rid not in covered],
            "unknown": sorted(unknown),
            "contradictions": sorted(supported & unmatched),
        },
        "duplicate_options": sorted(k for k, n in options_seen.items() if n > 1),
        "quotes": {"total": quotes_total, "empty": quotes_empty, "found": quotes_found},
        "matched_entries": len(doc.get("matched") or []),
        "multi_record_entries": multi,
        "unmatched_entries": len(unmatched),
        "chosen": dict(chosen),
    }


def parse_freehand(text: str, sent: list[str], records: dict[str, dict[str, Any]]) -> dict[str, Any]:
    per_record: dict[str, set[str]] = {rid: set() for rid in sent}
    unknown: set[str] = set()
    total = whole = head = 0
    for entry in json.loads(text).get("groundings") or []:
        rid = entry["record_id"]
        if rid not in per_record:
            unknown.add(rid)
            continue
        hay = _norm(f"{records[rid]['focal_form']}\n{records[rid]['synthesis']}")
        for unit in entry.get("candidates") or []:
            cand = unit["candidate"]
            per_record[rid].add(cand)
            total += 1
            whole += int(_norm(cand) in hay)
            words = _norm(cand).split()
            head += int(bool(words) and words[-1].rstrip("s") in hay)
    return {
        "per_record": {k: sorted(v) for k, v in per_record.items()},
        "candidates": total,
        "candidate_in_record": whole,
        "head_in_record": head,
        "unknown": sorted(unknown),
        "answered": sum(1 for rid in sent if rid in {e["record_id"] for e in json.loads(text).get("groundings") or []}),
    }


# --- screening parsers ------------------------------------------------------------


def parse_sa(text: str) -> dict[str, Any]:
    """Today's shape: per record, per candidate, conditions + guards."""
    verdicts: dict[tuple[str, str], bool] = {}
    failed_at: Counter[str] = Counter()
    for entry in json.loads(text).get("screenings") or []:
        rid = entry["record_id"]
        for unit in entry.get("candidates") or []:
            conditions = {k: v for k, v in unit.items() if isinstance(v, dict) and "outcome" in v}
            guards = unit.get("guards") or []
            passed = all(v["outcome"] == "satisfied" for v in conditions.values()) and not guards
            verdicts[(rid, unit["candidate"])] = passed
            if not passed:
                first = next((k for k, v in conditions.items() if v["outcome"] != "satisfied"), None)
                failed_at[first or (guards[0].get("rule_id", "guard") if guards else "?")] += 1
    return {"verdicts": verdicts, "failed_at": dict(failed_at)}


def parse_sb(text: str, units: list[dict[str, Any]]) -> dict[str, Any]:
    verdicts: dict[tuple[str, str], bool] = {}
    evidence: Counter[str] = Counter()
    failed_at: Counter[str] = Counter()
    problems: list[str] = []
    expected = {u["option"]: set(u["records"]) for u in units}
    seen: Counter[str] = Counter()
    for entry in json.loads(text).get("screenings") or []:
        option = entry["option"]
        seen[option] += 1
        if option not in expected:
            problems.append(f"unit never sent: {option!r}")
            continue
        acc = {r["record_id"] for r in entry.get("accepted") or []}
        rej = {r["record_id"] for r in entry.get("not_accepted") or []}
        if acc & rej:
            problems.append(f"{option!r}: records both accepted and not: {sorted(acc & rej)}")
        if (acc | rej) != expected[option]:
            problems.append(f"{option!r}: records answered {sorted(acc | rej)} != sent {sorted(expected[option])}")
        for r in entry.get("accepted") or []:
            verdicts[(r["record_id"], option)] = True
            evidence[r.get("evidence", "?")] += 1
        for r in entry.get("not_accepted") or []:
            verdicts[(r["record_id"], option)] = False
            failed_at[r.get("failed_rule", "?")] += 1
    for option, n in seen.items():
        if n > 1:
            problems.append(f"{option!r} answered {n} times")
    for option in expected:
        if seen[option] == 0:
            problems.append(f"{option!r} never answered")
    return {"verdicts": verdicts, "evidence": dict(evidence), "failed_at": dict(failed_at), "problems": problems}


# --- stability and modes ---------------------------------------------------------


def _pairs(per_record: dict[str, list[str]]) -> set[tuple[str, str]]:
    return {(rid, label) for rid, labels in per_record.items() for label in labels}


def stability(runs: list[dict[str, Any]], sent: list[str]) -> dict[str, Any]:
    if len(runs) < 2:
        return {"pairs": 0}
    jaccards: list[float] = []
    for a, b in itertools.combinations(runs, 2):
        pa, pb = _pairs(a["per_record"]), _pairs(b["per_record"])
        jaccards.append(len(pa & pb) / len(pa | pb) if (pa | pb) else 1.0)
    identical = sum(1 for rid in sent if len({tuple(r["per_record"].get(rid, [])) for r in runs}) == 1)
    return {"pairs": len(jaccards), "mean_jaccard": round(sum(jaccards) / len(jaccards), 3), "records_identical_in_every_repeat": identical, "records": len(sent)}


def mode_labels(runs: list[dict[str, Any]]) -> dict[str, list[str]]:
    counts: dict[str, Counter[str]] = defaultdict(Counter)
    for r in runs:
        for rid, labels in r["per_record"].items():
            for label in labels:
                counts[rid][label] += 1
    need = len(runs) / 2
    return {rid: sorted(l for l, n in c.items() if n > need) for rid, c in counts.items()}


def verdict_stability(runs: list[dict[str, Any]]) -> dict[str, Any]:
    """Per (record, label) pair: the majority verdict and the flip share."""
    votes: dict[tuple[str, str], list[bool]] = defaultdict(list)
    for r in runs:
        for pair, v in r["verdicts"].items():
            votes[pair].append(v)
    mode = {pair: (sum(vs) > len(vs) / 2) for pair, vs in votes.items()}
    unanimous = sum(1 for vs in votes.values() if len(set(vs)) == 1 and len(vs) == len(runs))
    return {"pairs": len(votes), "unanimous": unanimous, "mode": mode}


def usage(d: pathlib.Path, prefix: str) -> dict[str, Any]:
    prompt = completion = calls = 0
    seconds = 0.0
    for p in d.glob(f"{prefix}*.usage.json"):
        u = json.loads(p.read_text())
        prompt += u.get("prompt_tokens") or 0
        completion += u.get("completion_tokens") or 0
        seconds += u.get("seconds") or 0
        calls += 1
    return {
        "calls": calls,
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "usd": round(prompt / 1e6 * PRICE_PER_M_INPUT + completion / 1e6 * PRICE_PER_M_OUTPUT, 3),
        "mean_seconds": round(seconds / calls, 1) if calls else None,
    }


# --- per target ---------------------------------------------------------------------


def _series(d: pathlib.Path, pattern: str) -> list[pathlib.Path]:
    out: list[pathlib.Path] = []
    for k in itertools.count():
        p = d / pattern.format(k=k)
        if not p.exists():
            break
        out.append(p)
    return out


def check_target(tag: str) -> dict[str, Any]:
    d = OUT / tag
    meta = json.loads((d / "request_meta.json").read_text())
    field, sent = meta["field"], meta["record_ids"]
    groups = request_groups(meta["run"], meta["subject"], field, meta["chunk"])
    records = groups[meta["group_index"]]
    concept = field in CONCEPT_FIELDS
    labels = concept_by_label(field) if concept else {}
    report: dict[str, Any] = {"tag": tag, "field": field, "subject": meta["subject"], "records": len(sent), "arms": {}}
    failures: Counter[str] = Counter()

    runs: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if concept:
        for k, iv in enumerate(_series(d, "a_iv_{k}.json")):
            oov = d / f"a_oov_{k}.json"
            if not oov.exists():
                break
            try:
                runs["a"].append(parse_arm_a(iv.read_text(), oov.read_text(), labels))
            except Exception as exc:
                failures["a"] += 1
                print(f"{tag} a#{k}: parse failure {exc}", file=sys.stderr)
        for k, p in enumerate(_series(d, "b_{k}.json")):
            try:
                runs["b"].append(parse_arm_b(p.read_text(), sent, records, labels))
            except Exception as exc:
                failures["b"] += 1
                print(f"{tag} b#{k}: parse failure {exc}", file=sys.stderr)
    else:
        for arm in ("fa", "fb"):
            for k, p in enumerate(_series(d, arm + "_{k}.json")):
                try:
                    runs[arm].append(parse_freehand(p.read_text(), sent, records))
                except Exception as exc:
                    failures[arm] += 1
                    print(f"{tag} {arm}#{k}: parse failure {exc}", file=sys.stderr)

    if runs["a"]:
        report["arms"]["a"] = {
            "repeats": len(runs["a"]),
            "parse_failures": failures["a"],
            "labels_per_repeat": [sum(len(v) for v in r["per_record"].values()) for r in runs["a"]],
            "proposals_per_repeat": [len(r["proposals"]) for r in runs["a"]],
            "drops_per_repeat": [r["drops"] for r in runs["a"]],
            "stability": stability(runs["a"], sent),
            "usage": usage(d, "a_"),
        }
    if runs["b"]:
        cov = [r["coverage"] for r in runs["b"]]
        q_total = sum(r["quotes"]["total"] for r in runs["b"])
        q_found = sum(r["quotes"]["found"] for r in runs["b"])
        q_empty = sum(r["quotes"]["empty"] for r in runs["b"])
        report["arms"]["b"] = {
            "repeats": len(runs["b"]),
            "parse_failures": failures["b"],
            "labels_per_repeat": [sum(len(v) for v in r["per_record"].values()) for r in runs["b"]],
            "matched_entries_per_repeat": [r["matched_entries"] for r in runs["b"]],
            "multi_record_entries_per_repeat": [r["multi_record_entries"] for r in runs["b"]],
            "unmatched_per_repeat": [r["unmatched_entries"] for r in runs["b"]],
            "proposals_per_repeat": [len(r["proposals"]) for r in runs["b"]],
            "folds_per_repeat": [len(r["folds"]) for r in runs["b"]],
            "reroutes_per_repeat": [len(r["reroutes"]) for r in runs["b"]],
            "coverage_violations": {
                "repeats_with_missing": sum(1 for c in cov if c["missing"]),
                "missing_ids_total": sum(len(c["missing"]) for c in cov),
                "unknown_ids_total": sum(len(c["unknown"]) for c in cov),
                "contradictions_total": sum(len(c["contradictions"]) for c in cov),
            },
            "duplicate_option_entries": sum(len(r["duplicate_options"]) for r in runs["b"]),
            "quotes": {"total": q_total, "found": q_found, "empty": q_empty, "found_share": round(q_found / q_total, 3) if q_total else None},
            "chosen": dict(sum((Counter(r["chosen"]) for r in runs["b"]), Counter())),
            "stability": stability(runs["b"], sent),
            "usage": usage(d, "b_"),
        }
    for arm in ("fa", "fb"):
        if runs[arm]:
            total = sum(r["candidates"] for r in runs[arm])
            report["arms"][arm] = {
                "repeats": len(runs[arm]),
                "parse_failures": failures[arm],
                "candidates_per_repeat": [r["candidates"] for r in runs[arm]],
                "records_with_candidates_per_repeat": [sum(1 for v in r["per_record"].values() if v) for r in runs[arm]],
                "candidate_in_record_share": round(sum(r["candidate_in_record"] for r in runs[arm]) / total, 3) if total else None,
                "head_in_record_share": round(sum(r["head_in_record"] for r in runs[arm]) / total, 3) if total else None,
                "unknown_ids_total": sum(len(r["unknown"]) for r in runs[arm]),
                "stability": stability(runs[arm], sent),
                "usage": usage(d, arm + "_"),
            }

    pair_arms = ("a", "b") if concept else ("fa", "fb")
    if runs[pair_arms[0]] and runs[pair_arms[1]]:
        ma, mb = mode_labels(runs[pair_arms[0]]), mode_labels(runs[pair_arms[1]])
        pa, pb = _pairs(ma), _pairs(mb)
        report["mode_overlap"] = {
            "arms": list(pair_arms),
            "both": len(pa & pb),
            "first_only": sorted(f"{r}:{l}" for r, l in pa - pb),
            "second_only": sorted(f"{r}:{l}" for r, l in pb - pa),
        }

    # screening
    cand_files = sorted(d.glob("candidates_from_*.json"))
    if cand_files:
        candidates = json.loads(cand_files[-1].read_text())
        from run_grounding_tryout import Target, build_units  # noqa: E402

        t = Target(f"{meta['subject']}:{field}:{meta['chunk']}:{meta['group_index']}")
        units = build_units(t, candidates)
        sruns: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for k, p in enumerate(_series(d, "sa_{k}.json")):
            try:
                sruns["sa"].append(parse_sa(p.read_text()))
            except Exception as exc:
                failures["sa"] += 1
                print(f"{tag} sa#{k}: parse failure {exc}", file=sys.stderr)
        for k, p in enumerate(_series(d, "sb_{k}.json")):
            try:
                sruns["sb"].append(parse_sb(p.read_text(), units))
            except Exception as exc:
                failures["sb"] += 1
                print(f"{tag} sb#{k}: parse failure {exc}", file=sys.stderr)
        n_pairs = sum(len(v) for v in candidates.values())
        screening: dict[str, Any] = {"source": cand_files[-1].stem.replace("candidates_from_", ""), "units": len(units), "pairs": n_pairs}
        modes: dict[str, dict[tuple[str, str], bool]] = {}
        for arm in ("sa", "sb"):
            if not sruns[arm]:
                continue
            vs = verdict_stability(sruns[arm])
            modes[arm] = vs["mode"]
            entry: dict[str, Any] = {
                "repeats": len(sruns[arm]),
                "parse_failures": failures[arm],
                "accepted_per_repeat": [sum(r["verdicts"].values()) for r in sruns[arm]],
                "judged_per_repeat": [len(r["verdicts"]) for r in sruns[arm]],
                "failed_at": dict(sum((Counter(r["failed_at"]) for r in sruns[arm]), Counter())),
                "pairs_unanimous": vs["unanimous"],
                "pairs_seen": vs["pairs"],
                "mode_accepted": sum(vs["mode"].values()),
                "usage": usage(d, arm + "_"),
            }
            if arm == "sb":
                entry["evidence"] = dict(sum((Counter(r["evidence"]) for r in sruns[arm]), Counter()))
                entry["unit_problems_total"] = sum(len(r["problems"]) for r in sruns[arm])
                entry["unit_problems_sample"] = [p for r in sruns[arm] for p in r["problems"]][:5]
            screening[arm] = entry
        if "sa" in modes and "sb" in modes:
            shared = set(modes["sa"]) & set(modes["sb"])
            agree = Counter()
            for pair in shared:
                agree[f"sa={'accept' if modes['sa'][pair] else 'reject'}/sb={'accept' if modes['sb'][pair] else 'reject'}"] += 1
            screening["mode_agreement"] = {"shared_pairs": len(shared), **dict(agree)}
            screening["disagreements"] = sorted(
                f"{r}:{l} sa={'A' if modes['sa'][(r, l)] else 'R'} sb={'A' if modes['sb'][(r, l)] else 'R'}"
                for (r, l) in shared if modes["sa"][(r, l)] != modes["sb"][(r, l)]
            )
        report["screening"] = screening
    return report


def _md(reports: list[dict[str, Any]]) -> str:
    lines = ["# Step 2 tryout — mechanical check", ""]
    tot: dict[str, Counter[str]] = defaultdict(Counter)
    for r in reports:
        lines.append(f"## {r['tag']} ({r['field']}, {r['records']} records)")
        for arm, a in r["arms"].items():
            lines.append(f"- arm {arm}: {json.dumps({k: v for k, v in a.items() if k != 'usage'})}")
            lines.append(f"  usage {json.dumps(a['usage'])}")
            for k in ("prompt_tokens", "completion_tokens", "calls"):
                tot[arm][k] += a["usage"][k]
            tot[arm]["usd"] += a["usage"]["usd"]
        if "mode_overlap" in r:
            mo = r["mode_overlap"]
            lines.append(f"- mode overlap {mo['arms']}: both {mo['both']}, {mo['arms'][0]}-only {len(mo['first_only'])}, {mo['arms'][1]}-only {len(mo['second_only'])}")
        if "screening" in r:
            s = r["screening"]
            lines.append(f"- screening on mode candidates of {s['source']}: {s['units']} units, {s['pairs']} pairs")
            for arm in ("sa", "sb"):
                if arm in s:
                    a = s[arm]
                    lines.append(f"  - {arm}: {json.dumps({k: v for k, v in a.items() if k != 'usage'})}")
                    lines.append(f"    usage {json.dumps(a['usage'])}")
                    for k in ("prompt_tokens", "completion_tokens", "calls"):
                        tot[arm][k] += a["usage"][k]
                    tot[arm]["usd"] += a["usage"]["usd"]
            if "mode_agreement" in s:
                lines.append(f"  - mode agreement: {json.dumps(s['mode_agreement'])}")
        lines.append("")
    lines.append("## Cost by arm (gpt-4.1 list price)")
    for arm, c in sorted(tot.items()):
        lines.append(f"- {arm}: calls {c['calls']}, input {c['prompt_tokens']:,}, output {c['completion_tokens']:,}, ≈ ${c['usd']:.2f}")
    return "\n".join(lines) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--targets", nargs="*", default=[])
    args = ap.parse_args()
    tags = args.targets or sorted(p.name for p in OUT.iterdir() if p.is_dir() and (p / "request_meta.json").exists())
    reports = [check_target(t) for t in tags]
    (OUT / "CHECK.json").write_text(json.dumps(reports, indent=1, ensure_ascii=False, default=str))
    md = _md(reports)
    (OUT / "CHECK.md").write_text(md)
    print(md)


if __name__ == "__main__":
    main()
