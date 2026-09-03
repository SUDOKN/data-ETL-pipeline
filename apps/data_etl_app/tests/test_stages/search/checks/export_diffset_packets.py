"""Export difference-set judge packets — the standing-rule pass (RUNBOOK step 3).

The full census codes EVERY returned form. This exporter serves the cheaper
depth the user chose for the first 20-subject run (2026-08-28): judge only the
two DIFFERENCE SETS between the run and the eval set, plus the samples that
keep the credited remainder honest.

Per (subject, field) it emits:

  A. UNMATCHED FORMS — returned forms creditable against NO eval entry
     (confirmed, candidate or disputed; retired never credits). Judged with the
     census code table: a valid form here is an EVAL-SET GAP, junk is a stage
     precision finding.
  B. CANDIDATE CHECKS — entries seen by one reader only, which this run's
     output matched. The judge re-reads the text: a `confirm` verdict is the
     independent second pass that promotes the entry.
  C. MISS RE-VALIDATION — every confirmed entry whose windows returned no
     covering form. The judge validates the ENTRY before it counts as a recall
     miss (the standing rule: a wrong entry becomes disputed/retired, never a
     silent miss).
  D. CREDIT SPOT-CHECKS — a seeded sample of confirmed-credited (form, entry)
     pairs, guarding against containment false credit (the `Lead` class).

Credit is resolved at SUBJECT/FIELD level, not per window: the user's
instruction reads "phrases absent in the evaluation set", and window-scope
nuances are the documented one-quote-per-entry floor (HANDOFF §6.5), not new
information. Strongest status wins: confirmed > candidate > disputed.

Packets embed site text → history/runs/<run>/raw/diffset_packets/, gitignored.
Judgments go to history/runs/<run>/diffset_judgments/ (NOT judgments/ — the
census merger reads that folder and would misread a diff-set file as a census).
A `diffset_index.json` beside the packets carries the mechanical counts that
merge_diffset.py folds into scorecards.

Usage: .venv/bin/python export_diffset_packets.py --run 20260828T163338
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
from dataclasses import dataclass, field as dc_field
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from _shared.text_matching import (  # noqa: E402
    collapse_whitespace,
    covering_pattern,
    is_short_form,
    normalize_spaces,
)
from expectations import Expectations, load_expectations  # noqa: E402
from export_judge_packets import CODE_TABLES  # noqa: E402
from loading import FieldRun, WindowRecord, load_run  # noqa: E402
from paths import run_dir, subject_slug  # noqa: E402

# One judged "item" is one form to code or one entry to verify; parts stay
# under both caps so a single agent can hold a part in context.
MAX_ITEMS_PER_PART = 150
MAX_TEXT_BYTES_PER_PART = 180_000
CREDIT_STRENGTH = {"confirmed": 3, "candidate": 2, "disputed": 1}

ACTOR_VOCAB = "own | client | supplier | lab | parent_sibling | reseller_inventory | unclear"
EVIDENCE_VOCAB = "prose | list | table | title | footer | other"


@dataclass
class _Acceptable:
    """One acceptable form of one entry, precomputed for cheap overlap tests."""

    entry_id: str
    entry_name: str
    status: str
    collapsed: str
    casefolded: str
    short: bool
    # `form_covers`'s own pattern, compiled once — imported rather than rebuilt
    # here so the two can never drift apart.
    pattern: Optional[re.Pattern[str]]


@dataclass
class UnitDiffset:
    """Everything the packets and the index need for one (subject, field)."""

    subject: str
    field: str
    forms_total: int = 0
    credited: dict[str, int] = dc_field(
        default_factory=lambda: {"confirmed": 0, "candidate": 0, "disputed": 0}
    )
    # window sub_bounds -> [form, ...] preserving return order
    unmatched: dict[str, list[str]] = dc_field(default_factory=dict)
    candidate_checks: list[dict[str, Any]] = dc_field(default_factory=list)
    miss_validations: list[dict[str, Any]] = dc_field(default_factory=list)
    credit_samples: list[dict[str, Any]] = dc_field(default_factory=list)
    # entry_id -> credited form occurrences; the merger weighs judge-confirmed
    # candidate entries by how many returned forms they credited
    candidate_occurrences: dict[str, int] = dc_field(default_factory=dict)

    @property
    def unmatched_count(self) -> int:
        return sum(len(v) for v in self.unmatched.values())

    @property
    def judged_items(self) -> int:
        return (
            self.unmatched_count
            + len(self.candidate_checks)
            + len(self.miss_validations)
            + len(self.credit_samples)
        )


def _overlap(a: _Acceptable, form_collapsed: str, form_cf: str) -> bool:
    """`form_covers(acceptable, returned)`, with the acceptable form's pattern
    precompiled (28k forms times thousands of acceptable forms makes the naive
    call graph the bottleneck).

    DIRECTIONAL since 2026-08-28 (trap 5): the acceptable form must occur inside
    the RETURNED form, not the reverse. Before that a returned fragment credited
    the entry it was a fragment of, which is what the credit spot-checks caught.
    """
    if not a.collapsed or not form_collapsed:
        return False
    if a.casefolded == form_cf:
        return True
    return bool(a.pattern and a.pattern.search(normalize_spaces(form_collapsed)))


def _acceptables(exp: Expectations) -> list[_Acceptable]:
    out: list[_Acceptable] = []
    for entry in exp.entries:
        status = str(entry.get("status"))
        if status not in CREDIT_STRENGTH:
            continue  # retired never credits
        for acc in entry.get("acceptable_forms") or []:
            collapsed = collapse_whitespace(str(acc))
            if not collapsed:
                continue
            out.append(
                _Acceptable(
                    entry_id=str(entry.get("id")),
                    entry_name=str(entry.get("name")),
                    status=status,
                    collapsed=collapsed,
                    casefolded=collapsed.casefold(),
                    short=is_short_form(collapsed),
                    pattern=covering_pattern(collapsed),
                )
            )
    # Strongest statuses first so the first hit at strength 3 can stop the scan.
    out.sort(key=lambda a: -CREDIT_STRENGTH[a.status])
    return out


def _best_credit(
    acceptables: list[_Acceptable], form: str
) -> Optional[_Acceptable]:
    collapsed = collapse_whitespace(form)
    cf = collapsed.casefold()
    best: Optional[_Acceptable] = None
    for acc in acceptables:
        if best is not None and CREDIT_STRENGTH[acc.status] <= CREDIT_STRENGTH[best.status]:
            break  # sorted by strength — nothing better can follow
        if _overlap(acc, collapsed, cf):
            best = acc
            if best.status == "confirmed":
                break
    return best


def _scope_windows(
    entry: dict[str, Any], collapsed_windows: list[tuple[str, str, str]]
) -> list[str]:
    """sub_bounds of windows whose wire text contains one of the entry's quotes
    — the same scoping rule expectation_metrics uses. ``collapsed_windows`` is
    [(sub_bounds, collapsed_text, collapsed_text_casefolded)], collapsed ONCE
    per unit — re-collapsing per entry is the quadratic trap that once kept
    agstech from ever finishing (HANDOFF §9)."""
    quotes = [collapse_whitespace(e.get("quote", "")) for e in entry.get("evidence") or []]
    quotes = [q for q in quotes if q]
    hits: list[str] = []
    for sub_bounds, haystack, haystack_cf in collapsed_windows:
        for q in quotes:
            if q in haystack or q.casefold() in haystack_cf:
                hits.append(sub_bounds)
                break
    return hits


def build_unit(fr: FieldRun, exp: Optional[Expectations], sample_size: int) -> Optional[UnitDiffset]:
    windows = [
        w for w in fr.windows
        if w.round_index is None and w.phrases is not None and w.wire_text is not None
    ]
    if not windows:
        return None
    unit = UnitDiffset(subject=fr.subject, field=fr.field)
    acceptables = _acceptables(exp) if exp else []
    entries_by_id = {str(e.get("id")): e for e in (exp.entries if exp else [])}
    collapsed_windows: list[tuple[str, str, str]] = []
    for w in windows:
        collapsed = collapse_whitespace(w.wire_text or "")
        collapsed_windows.append((w.sub_bounds, collapsed, collapsed.casefold()))

    credit_cache: dict[str, Optional[_Acceptable]] = {}
    confirmed_credits: list[dict[str, Any]] = []
    candidate_hits: dict[str, dict[str, Any]] = {}
    for w in windows:
        for form in w.phrases or []:
            unit.forms_total += 1
            key = collapse_whitespace(form).casefold()
            if key not in credit_cache:
                credit_cache[key] = _best_credit(acceptables, form)
            best = credit_cache[key]
            if best is None:
                unit.unmatched.setdefault(w.sub_bounds, []).append(form)
                continue
            unit.credited[best.status] += 1
            if best.status == "candidate":
                unit.candidate_occurrences[best.entry_id] = (
                    unit.candidate_occurrences.get(best.entry_id, 0) + 1
                )
            if best.status == "confirmed":
                confirmed_credits.append(
                    {"form": form, "window": w.sub_bounds,
                     "entry_id": best.entry_id, "entry_name": best.entry_name}
                )
            elif best.status == "candidate":
                hit = candidate_hits.setdefault(
                    best.entry_id,
                    {"entry_id": best.entry_id, "entry_name": best.entry_name,
                     "forms": [], "windows": []},
                )
                if form not in hit["forms"]:
                    hit["forms"].append(form)
                if w.sub_bounds not in hit["windows"]:
                    hit["windows"].append(w.sub_bounds)

    for hit in candidate_hits.values():
        entry = entries_by_id.get(hit["entry_id"], {})
        hit["quotes"] = [e.get("quote", "") for e in entry.get("evidence") or []][:3]
        hit["scope_windows"] = _scope_windows(entry, collapsed_windows) or hit["windows"]
        unit.candidate_checks.append(hit)

    if exp:
        for entry in exp.entries_with_status("confirmed"):
            scope = _scope_windows(entry, collapsed_windows)
            if not scope:
                continue  # out of coverage — not a miss, not validatable here
            covered = False
            acc_forms = entry.get("acceptable_forms") or []
            entry_accs = _acceptables_for_entry(entry, acc_forms)
            scope_set = set(scope)
            for w in windows:
                if w.sub_bounds not in scope_set:
                    continue
                for form in w.phrases or []:
                    collapsed = collapse_whitespace(form)
                    cf = collapsed.casefold()
                    if any(_overlap(a, collapsed, cf) for a in entry_accs):
                        covered = True
                        break
                if covered:
                    break
            if not covered:
                unit.miss_validations.append(
                    {"entry_id": str(entry.get("id")), "entry_name": str(entry.get("name")),
                     "acceptable_forms": acc_forms,
                     "quotes": [e.get("quote", "") for e in entry.get("evidence") or []][:3],
                     "scope_windows": scope}
                )

    rng = random.Random(f"{fr.run_id}|{fr.subject}|{fr.field}")
    if confirmed_credits and sample_size > 0:
        unit.credit_samples = rng.sample(
            confirmed_credits, min(sample_size, len(confirmed_credits))
        )
    return unit


def _acceptables_for_entry(
    entry: dict[str, Any], acc_forms: list[Any]
) -> list[_Acceptable]:
    status = str(entry.get("status"))
    out = []
    for acc in acc_forms:
        collapsed = collapse_whitespace(str(acc))
        if collapsed:
            out.append(
                _Acceptable(
                    entry_id=str(entry.get("id")), entry_name=str(entry.get("name")),
                    status=status, collapsed=collapsed,
                    casefolded=collapsed.casefold(), short=is_short_form(collapsed),
                    pattern=covering_pattern(collapsed),
                )
            )
    return out


# ------------------------------------------------------------------ packets

def _task_header(unit: UnitDiffset, run_id: str, part: int, parts: int) -> list[str]:
    out_name = f"{subject_slug(unit.subject)}__{unit.field}"
    if parts > 1:
        out_name += f".part{part}"
    return [
        f"# Diff-set judge packet — {unit.subject} / {unit.field} — run {run_id}"
        + (f" (part {part}/{parts})" if parts > 1 else ""),
        "",
        "## The task",
        "",
        "The eval set is ITSELF under test (standing user rule 2026-08-28):",
        "your verdicts decide both what the stage got wrong AND what the eval",
        "set got wrong. Judge by the PLAIN MEANING of the field against the",
        "window text — not by what you think the pipeline intended. The stage",
        "is RECALL-FIRST: a thing named in the text is in-field even when it",
        "belongs to a client, supplier, lab or parent company — record that in",
        "`actor`, never as a lower code.",
        "",
        f"Code table for {unit.field}:",
        "",
        f"    {CODE_TABLES.get(unit.field, 'see TAXONOMY.md')}",
        "",
        f"Actors: {ACTOR_VOCAB}",
        f"Evidence kinds: {EVIDENCE_VOCAB}",
        "",
        "Write one JSON object per line to",
        f"`history/runs/{run_id}/diffset_judgments/{out_name}.jsonl`:",
        "",
        '    {"type": "form", "window": "0:23771", "form": "...", "code": "V",',
        '     "actor": "own", "evidence_kind": "prose", "note": ""}',
        '    {"type": "candidate_check", "entry_id": "...",',
        '     "verdict": "confirm|dispute|not_found", "note": ""}',
        '    {"type": "miss_validation", "entry_id": "...",',
        '     "verdict": "valid_miss|entry_wrong",',
        '     "suggested_status": "disputed|retired",  # entry_wrong only',
        '     "reason": ""}',
        '    {"type": "credit_check", "form": "...", "entry_id": "...",',
        '     "window": "0:23771", "verdict": "true_credit|false_credit",',
        '     "note": ""}',
        "",
        "Every listed item gets exactly one line. Quote-check against the",
        "window texts at the bottom of this packet; never against memory.",
        "",
    ]


def _section_a(unit: UnitDiffset, window_ids: list[str]) -> list[str]:
    lines = [
        "## A — unmatched forms (code EVERY one)",
        "",
        "These forms matched no eval entry. A valid in-field form here is an",
        "eval-set gap; junk here is a stage precision finding. Either way the",
        "code says which.",
        "",
    ]
    for wid in window_ids:
        forms = unit.unmatched.get(wid) or []
        if not forms:
            continue
        lines += [f"### Window {wid}", ""]
        lines += [f"{i + 1}. {form!r}" for i, form in enumerate(forms)]
        lines += [""]
    return lines


def _section_b(checks: list[dict[str, Any]]) -> list[str]:
    if not checks:
        return []
    lines = [
        "## B — candidate-entry checks (one verdict each)",
        "",
        "Each entry below was seen by ONE prior reader; this run's output",
        "matched it. Re-read the text: `confirm` = the entity is really named",
        "there in-field (this promotes it), `dispute` = real text but wrong",
        "field/reading, `not_found` = you cannot locate it at all.",
        "",
    ]
    for c in checks:
        lines += [
            f"- entry `{c['entry_id']}` — **{c['entry_name']}**",
            f"  - matched run forms: {', '.join(repr(f) for f in c['forms'][:6])}",
            f"  - prior evidence quotes: {', '.join(repr(q) for q in c['quotes'] if q)}",
            f"  - look in windows: {', '.join(c['scope_windows'])}",
        ]
    lines += [""]
    return lines


def _section_c(misses: list[dict[str, Any]]) -> list[str]:
    if not misses:
        return []
    lines = [
        "## C — miss re-validation (one verdict each)",
        "",
        "No returned form covered these confirmed entries in their windows.",
        "Before this counts as a stage recall miss, validate the ENTRY itself:",
        "is the quote really in the window text, and is the entity in-field",
        "under the code table's plain meaning? `valid_miss` = entry stands,",
        "stage missed it. `entry_wrong` = the eval set is at fault — say why",
        "and suggest `disputed` (arguable) or `retired` (plainly wrong).",
        "",
    ]
    for m in misses:
        lines += [
            f"- entry `{m['entry_id']}` — **{m['entry_name']}**",
            f"  - acceptable forms: {', '.join(repr(str(a)) for a in m['acceptable_forms'])}",
            f"  - evidence quotes: {', '.join(repr(q) for q in m['quotes'] if q)}",
            f"  - in-scope windows: {', '.join(m['scope_windows'])}",
        ]
    lines += [""]
    return lines


def _section_d(samples: list[dict[str, Any]]) -> list[str]:
    if not samples:
        return []
    lines = [
        "## D — credit spot-checks (one verdict each)",
        "",
        "The matcher credited each returned form below to a confirmed entry by",
        "containment. Verify in the window: does the form actually refer to",
        "that entity there (`true_credit`), or is this containment false",
        "credit — the `Lead`-inside-`lead time` class (`false_credit`)?",
        "",
    ]
    for s in samples:
        lines += [
            f"- form {s['form']!r} credited to entry `{s['entry_id']}`"
            f" (**{s['entry_name']}**) in window {s['window']}",
        ]
    lines += [""]
    return lines


def _window_texts(windows: dict[str, str], ids: list[str]) -> list[str]:
    lines = ["## Window texts", ""]
    for wid in ids:
        lines += [f"### Window {wid}", "", "```", windows.get(wid, "<text unavailable>").rstrip(), "```", ""]
    return lines


def _split_parts(unit: UnitDiffset, window_order: list[str], texts: dict[str, str]) -> list[dict[str, Any]]:
    """Partition the unit's work into self-contained parts.

    Greedy over windows in document order; every item lands in the part that
    embeds its FIRST scope window, so no part depends on another's text."""
    first_window: dict[str, str] = {}
    for c in unit.candidate_checks:
        first_window[f"B:{c['entry_id']}"] = (c["scope_windows"] or window_order)[0]
    for m in unit.miss_validations:
        first_window[f"C:{m['entry_id']}"] = (m["scope_windows"] or window_order)[0]
    for i, s in enumerate(unit.credit_samples):
        first_window[f"D:{i}"] = s["window"]

    items_per_window: dict[str, int] = {w: len(unit.unmatched.get(w, [])) for w in window_order}
    for key, w in first_window.items():
        items_per_window[w] = items_per_window.get(w, 0) + 1

    parts: list[dict[str, Any]] = []
    current: list[str] = []
    items = 0
    text_bytes = 0
    for w in window_order:
        w_items = items_per_window.get(w, 0)
        w_bytes = len(texts.get(w, "").encode("utf-8"))
        if current and (
            items + w_items > MAX_ITEMS_PER_PART
            or text_bytes + w_bytes > MAX_TEXT_BYTES_PER_PART
        ):
            parts.append({"windows": current})
            current, items, text_bytes = [], 0, 0
        current.append(w)
        items += w_items
        text_bytes += w_bytes
    if current:
        parts.append({"windows": current})

    for part in parts:
        wset = set(part["windows"])
        part["candidate_checks"] = [
            c for c in unit.candidate_checks
            if first_window[f"B:{c['entry_id']}"] in wset
        ]
        part["miss_validations"] = [
            m for m in unit.miss_validations
            if first_window[f"C:{m['entry_id']}"] in wset
        ]
        part["credit_samples"] = [
            s for i, s in enumerate(unit.credit_samples)
            if first_window[f"D:{i}"] in wset
        ]
    return parts


def export(run_id: str, only_subject: Optional[str] = None, sample_size: int = 4) -> Path:
    raw = run_dir(run_id) / "raw"
    out_dir = raw / "diffset_packets"
    out_dir.mkdir(parents=True, exist_ok=True)
    pulled = raw / "search_requests.json"
    field_runs = load_run(run_id, pulled_file=pulled if pulled.is_file() else None)

    index: dict[str, Any] = {"run": run_id, "sample_size": sample_size, "units": []}
    for fr in field_runs:
        if only_subject and fr.subject != only_subject:
            continue
        exp = load_expectations(fr.subject, fr.field)
        unit = build_unit(fr, exp, sample_size)
        if unit is None:
            continue
        windows = [
            w for w in fr.windows
            if w.round_index is None and w.phrases is not None and w.wire_text is not None
        ]
        texts = {w.sub_bounds: w.wire_text or "" for w in windows}
        window_order = [w.sub_bounds for w in windows]
        parts = _split_parts(unit, window_order, texts)

        files = []
        for i, part in enumerate(parts, start=1):
            needed = [
                w for w in part["windows"]
                if unit.unmatched.get(w)
                or any(w in c["scope_windows"] for c in part["candidate_checks"])
                or any(w in m["scope_windows"] for m in part["miss_validations"])
                or any(s["window"] == w for s in part["credit_samples"])
            ]
            if not needed and not (
                part["candidate_checks"] or part["miss_validations"] or part["credit_samples"]
            ):
                continue
            lines = _task_header(unit, run_id, i, len(parts))
            lines += _section_a(unit, [w for w in part["windows"] if unit.unmatched.get(w)])
            lines += _section_b(part["candidate_checks"])
            lines += _section_c(part["miss_validations"])
            lines += _section_d(part["credit_samples"])
            lines += _window_texts(texts, needed)
            name = f"{subject_slug(fr.subject)}__{fr.field}"
            if len(parts) > 1:
                name += f".part{i}"
            path = out_dir / f"{name}.md"
            path.write_text("\n".join(lines), encoding="utf-8")
            files.append(path.name)

        index["units"].append(
            {"subject": fr.subject, "field": fr.field,
             "forms_total": unit.forms_total, "credited": unit.credited,
             "unmatched": unit.unmatched_count,
             "candidate_checks": len(unit.candidate_checks),
             "candidate_occurrences": unit.candidate_occurrences,
             "miss_validations": len(unit.miss_validations),
             "credit_samples": len(unit.credit_samples),
             "judged_items": unit.judged_items, "packets": files}
        )

    (out_dir / "diffset_index.json").write_text(
        json.dumps(index, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    total_items = sum(u["judged_items"] for u in index["units"])
    total_packets = sum(len(u["packets"]) for u in index["units"])
    print(f"{len(index['units'])} units, {total_items} judged items, {total_packets} packets -> {out_dir}")
    print(f"{'subject':32s} {'field':24s} {'forms':>6s} {'unmat':>6s} {'cand':>5s} {'miss':>5s} {'smpl':>5s} pkts")
    for u in index["units"]:
        print(
            f"{u['subject']:32s} {u['field']:24s} {u['forms_total']:6d} "
            f"{u['unmatched']:6d} {u['candidate_checks']:5d} "
            f"{u['miss_validations']:5d} {u['credit_samples']:5d} {len(u['packets'])}"
        )
    return out_dir


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", required=True)
    parser.add_argument("--subject", default=None)
    parser.add_argument("--sample", type=int, default=4, help="credit spot-checks per unit")
    args = parser.parse_args()
    export(args.run, args.subject, args.sample)
