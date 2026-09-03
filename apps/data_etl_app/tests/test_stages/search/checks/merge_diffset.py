"""Fold diff-set judgments into scorecards (the standing-rule pass's step 5).

Reads history/runs/<run>/diffset_judgments/<slug>__<field>[.partN].jsonl
(+ .judge2.jsonl doubles) against raw/diffset_packets/diffset_index.json, and
writes a `diffset` section into each unit's scorecard, DIFFSET.md, and
per-subject eval-evolution files under history/runs/<run>/evolution/:

  <slug>.jsonl        apply_verifications.py rows — candidate promotions
                      (`confirm`) and invalidated misses (`dispute`/`retire`)
  <slug>__gaps.jsonl  build_expectations.py --merge seed rows for unmatched
                      forms judged in-field (V/B), evidence quote derived
                      MECHANICALLY from the form's match in the window text —
                      judges mistype quotes, the matcher does not.

Precision here is an ESTIMATE, stated with its parts: forms credited to
confirmed entries count as text-verified credit (the spot-check sample reports
how often that containment credit is false), candidate credits count only when
this run's judge confirmed the entry, disputed credits never count, and the
unmatched remainder contributes its judged in-field share. Recall is restated
after miss re-validation: an `entry_wrong` miss leaves the denominator — the
standing rule made auditable.

Usage: .venv/bin/python merge_diffset.py --run 20260828T163338
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from _shared.text_matching import collapse_whitespace, flexible_pattern, is_short_form  # noqa: E402
from loading import load_run  # noqa: E402
from merge_judgments import ROLLUP, agreement, normalize_window  # noqa: E402
from paths import run_dir, subject_slug  # noqa: E402
from scorecard import append_history  # noqa: E402

QUOTE_RADIUS = 60  # chars of context on each side of a derived gap quote
QUOTE_CAP = 200    # EXPECTATIONS_SCHEMA's quote length cap


def read_rows(paths: list[Path]) -> dict[str, list[dict[str, Any]]]:
    by_type: dict[str, list[dict[str, Any]]] = {
        "form": [], "candidate_check": [], "miss_validation": [], "credit_check": []
    }
    for path in paths:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            row = json.loads(line)
            kind = row.get("type") or ("form" if row.get("code") else None)
            if kind not in by_type:
                continue
            if "window" in row:
                row["window"] = normalize_window(row.get("window"))
            by_type[kind].append(row)
    return by_type


def derive_gap_quote(form: str, window_text: str) -> Optional[str]:
    """A verbatim evidence quote around the form's first match in the window.

    None when the form does not occur (a judge coded a fabricated form V —
    that is a finding, not an entry)."""
    normalized = collapse_whitespace(form)
    if not normalized:
        return None
    pattern = flexible_pattern(normalized, case_sensitive=is_short_form(normalized))
    if pattern is None:
        return None
    from _shared.text_matching import normalize_spaces  # local: keep import surface small
    haystack = normalize_spaces(window_text)
    match = pattern.search(haystack)
    if match is None and not is_short_form(normalized):
        loose = flexible_pattern(normalized, case_sensitive=False)
        match = loose.search(haystack) if loose else None
    if match is None:
        return None
    start = max(0, match.start() - QUOTE_RADIUS)
    end = min(len(window_text), match.end() + QUOTE_RADIUS)
    # Slice the ORIGINAL text (normalize_spaces is length-preserving, so the
    # offsets line up) and let whitespace-collapse comparison do the rest.
    quote = window_text[start:end].strip()
    if len(quote) > QUOTE_CAP:
        margin = (len(quote) - QUOTE_CAP) // 2 + 1
        quote = quote[margin:-margin].strip()
    return quote or None


def summarize_unit(
    unit_index: dict[str, Any],
    rows: dict[str, list[dict[str, Any]]],
    second_forms: list[dict[str, Any]],
) -> dict[str, Any]:
    forms = rows["form"]
    codes = Counter(str(r.get("code")) for r in forms)
    rollup = Counter(ROLLUP.get(str(r.get("code")), "junk") for r in forms)
    judged = len(forms)
    actors = Counter(str(r.get("actor")) for r in forms if r.get("actor"))

    cand_verdicts = Counter(str(r.get("verdict")) for r in rows["candidate_check"])
    miss_verdicts = Counter(str(r.get("verdict")) for r in rows["miss_validation"])
    credit_verdicts = Counter(str(r.get("verdict")) for r in rows["credit_check"])

    credited = unit_index["credited"]
    forms_total = unit_index["forms_total"]
    unmatched_in_field = rollup.get("in_field", 0)
    # Candidate credit counts only for entries this run's judge confirmed.
    confirmed_entry_ids = {
        str(r.get("entry_id")) for r in rows["candidate_check"]
        if r.get("verdict") == "confirm"
    }
    cand_occurrences = unit_index.get("candidate_occurrences") or {}
    cand_confirmed_forms = sum(
        n for entry_id, n in cand_occurrences.items() if entry_id in confirmed_entry_ids
    )
    numerator = credited["confirmed"] + cand_confirmed_forms + unmatched_in_field
    precision_estimate = round(numerator / forms_total, 4) if forms_total else None

    valid_miss = miss_verdicts.get("valid_miss", 0)
    entry_wrong = miss_verdicts.get("entry_wrong", 0)

    fc_total = sum(credit_verdicts.values())
    false_credit = credit_verdicts.get("false_credit", 0)

    return {
        "mechanical": {
            "forms_total": forms_total,
            "credited": credited,
            "unmatched": unit_index["unmatched"],
        },
        "unmatched_judged": {
            "judged": judged,
            "coverage": round(judged / unit_index["unmatched"], 4) if unit_index["unmatched"] else None,
            "codes": dict(sorted(codes.items())),
            "rollup": dict(sorted(rollup.items())),
            "in_field": unmatched_in_field,
            "junk_rate": round(rollup.get("junk", 0) / judged, 4) if judged else None,
            "actors": dict(sorted(actors.items())),
        },
        "precision_estimate": precision_estimate,
        "candidate_checks": dict(sorted(cand_verdicts.items())),
        "miss_validation": {
            "judged": sum(miss_verdicts.values()),
            "expected": unit_index["miss_validations"],
            "valid_miss": valid_miss,
            "entry_wrong": entry_wrong,
        },
        "credit_sample": {
            "checked": fc_total,
            "false_credit": false_credit,
            "false_credit_rate": round(false_credit / fc_total, 4) if fc_total else None,
        },
        "judge_agreement": agreement(forms, second_forms),
    }


def adjusted_recall(scorecard: dict[str, Any], diffset: dict[str, Any]) -> Optional[dict[str, Any]]:
    exp = (scorecard.get("metrics") or {}).get("expectations") or {}
    covered = exp.get("confirmed_covered")
    if covered is None:
        return None
    mv = diffset["miss_validation"]
    if not mv["judged"]:
        return None
    denominator = covered + mv["valid_miss"]
    return {
        "raw_recall": exp.get("confirmed_recall"),
        "valid_misses": mv["valid_miss"],
        "invalidated_misses": mv["entry_wrong"],
        "adjusted_recall": round(covered / denominator, 4) if denominator else None,
    }


def evolution_rows(
    subject: str,
    field: str,
    rows: dict[str, list[dict[str, Any]]],
    window_texts: dict[str, str],
    run_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    """(apply_verifications rows, gap seed rows, problems)."""
    verdicts: list[dict[str, Any]] = []
    problems: list[str] = []
    for r in rows["candidate_check"]:
        if r.get("verdict") == "confirm":
            verdicts.append(
                {"id": r.get("entry_id"), "verdict": "confirm",
                 "reason": f"diffset judge, run {run_id}: {r.get('note') or 'entity re-read in window'}"}
            )
    for r in rows["miss_validation"]:
        if r.get("verdict") == "entry_wrong":
            suggested = r.get("suggested_status")
            verdict = {"disputed": "dispute", "retired": "retire"}.get(str(suggested))
            if verdict is None:
                problems.append(
                    f"{subject}/{field}: entry_wrong for {r.get('entry_id')} without a "
                    f"usable suggested_status ({suggested!r}) — left un-applied"
                )
                continue
            verdicts.append(
                {"id": r.get("entry_id"), "verdict": verdict,
                 "reason": f"diffset miss re-validation, run {run_id}: {r.get('reason') or r.get('note') or ''}"}
            )

    gaps: list[dict[str, Any]] = []
    for r in rows["form"]:
        if str(r.get("code")) not in {"V", "B"}:
            continue
        form = str(r.get("form") or "")
        window = str(r.get("window") or "")
        quote = derive_gap_quote(form, window_texts.get(window, ""))
        if quote is None:
            problems.append(
                f"{subject}/{field}: in-field unmatched form {form!r} not locatable "
                f"in window {window} — possible fabrication credit, NOT seeded"
            )
            continue
        gaps.append(
            {"type": "entry", "field": field, "name": form,
             "acceptable_forms": [form],
             "evidence": [{"quote": quote}],
             "actor": r.get("actor") or "own",
             "notes": f"diffset gap, run {run_id}, window {window}, judge code {r.get('code')}"
             + (f"; {r.get('note')}" if r.get("note") else "")}
        )
    return verdicts, gaps, problems


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", required=True)
    args = parser.parse_args()
    rdir = run_dir(args.run)

    index_path = rdir / "raw" / "diffset_packets" / "diffset_index.json"
    index = json.loads(index_path.read_text(encoding="utf-8"))
    judgments_dir = rdir / "diffset_judgments"
    if not judgments_dir.is_dir():
        print(f"no diffset judgments at {judgments_dir}", file=sys.stderr)
        raise SystemExit(1)

    pulled = rdir / "raw" / "search_requests.json"
    field_runs = load_run(args.run, pulled_file=pulled if pulled.is_file() else None)
    texts_by_unit: dict[tuple[str, str], dict[str, str]] = {}
    for fr in field_runs:
        texts_by_unit[(fr.subject, fr.field)] = {
            w.sub_bounds: w.wire_text or ""
            for w in fr.windows
            if w.round_index is None and w.wire_text is not None
        }

    part_re = re.compile(r"\.part\d+$")
    evolution: dict[str, dict[str, list[dict[str, Any]]]] = {}
    all_problems: list[str] = []
    report_rows: list[str] = []
    merged = 0
    for unit in index["units"]:
        slug = subject_slug(unit["subject"])
        stem = f"{slug}__{unit['field']}"
        primaries = sorted(
            p for p in judgments_dir.glob(f"{stem}*.jsonl")
            if not p.name.endswith(".judge2.jsonl")
            and part_re.sub("", p.stem) == stem
        )
        if not primaries and unit["judged_items"] == 0:
            continue
        rows = read_rows(primaries)
        seconds = sorted(judgments_dir.glob(f"{stem}*.judge2.jsonl"))
        second_forms = read_rows(seconds)["form"] if seconds else []
        summary = summarize_unit(unit, rows, second_forms)

        scorecard_path = rdir / f"scorecard_{slug}_{unit['field']}.json"
        if scorecard_path.is_file():
            scorecard = json.loads(scorecard_path.read_text(encoding="utf-8"))
            summary["recall_restated"] = adjusted_recall(scorecard, summary)
            scorecard["metrics"]["diffset"] = summary
            scorecard_path.write_text(
                json.dumps(scorecard, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
            )
            append_history(scorecard)
            merged += 1
        else:
            summary["recall_restated"] = None
            all_problems.append(f"no scorecard for {stem}")

        verdicts, gaps, problems = evolution_rows(
            unit["subject"], unit["field"], rows,
            texts_by_unit.get((unit["subject"], unit["field"]), {}), args.run,
        )
        all_problems.extend(problems)
        bucket = evolution.setdefault(slug, {"verdicts": [], "gaps": []})
        bucket["verdicts"].extend(verdicts)
        bucket["gaps"].extend(gaps)

        uj = summary["unmatched_judged"]
        mv = summary["miss_validation"]
        cs = summary["credit_sample"]
        floor = summary["judge_agreement"]
        floor_text = (
            f"{floor['agreement']:.0%}/{floor['compared']}"
            if floor and floor.get("agreement") is not None else "—"
        )
        pe = summary["precision_estimate"]
        rr = summary.get("recall_restated") or {}
        report_rows.append(
            f"| {slug} | {unit['field']} | {unit['forms_total']} | "
            f"{uj['judged']}/{unit['unmatched']} | {uj['in_field']} | "
            f"{(pe if pe is not None else float('nan')):.0%} | "
            f"{mv['valid_miss']}/{mv['judged']} | {mv['entry_wrong']} | "
            f"{cs['false_credit']}/{cs['checked']} | "
            f"{(rr.get('adjusted_recall') if rr else None) if rr else None} | {floor_text} |"
        )

    evo_dir = rdir / "evolution"
    evo_dir.mkdir(exist_ok=True)
    for slug, bucket in sorted(evolution.items()):
        if bucket["verdicts"]:
            (evo_dir / f"{slug}.jsonl").write_text(
                "\n".join(json.dumps(r, ensure_ascii=False) for r in bucket["verdicts"]) + "\n",
                encoding="utf-8",
            )
        if bucket["gaps"]:
            (evo_dir / f"{slug}__gaps.jsonl").write_text(
                "\n".join(json.dumps(r, ensure_ascii=False) for r in bucket["gaps"]) + "\n",
                encoding="utf-8",
            )

    header = [
        f"# Diff-set judgments — run {args.run}",
        "",
        "The standing rule made numbers: unmatched forms judged, misses",
        "re-validated, candidate matches re-read, containment credit sampled.",
        "`precision est` counts confirmed credits + judge-confirmed candidate",
        "credits + in-field unmatched, over all returned forms; read it beside",
        "the false-credit sample. `misses valid/judged` restates recall gates:",
        "an invalidated miss is an EVAL-SET defect, not a stage defect.",
        "",
        "| subject | field | forms | unmatched judged | in-field (gaps) | precision est | misses valid/judged | misses invalidated | false credit | adjusted recall | agreement |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    out = rdir / "DIFFSET.md"
    body = header + report_rows
    if all_problems:
        body += ["", "## Problems", ""] + [f"- {p}" for p in all_problems]
    out.write_text("\n".join(body) + "\n", encoding="utf-8")
    print(f"merged {merged} diffset summaries; wrote {out}")
    if all_problems:
        print(f"{len(all_problems)} problems (see DIFFSET.md)")
    print(f"evolution files -> {evo_dir} (apply with apply_verifications.py / build_expectations.py --merge; user-vetoable)")


if __name__ == "__main__":
    main()
