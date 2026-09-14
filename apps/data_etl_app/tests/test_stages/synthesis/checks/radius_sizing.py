"""Offline sizing of the fold's snippet radius (2026-09-14, for the radius-1 experiment).

Re-folds every chunk of the given subjects from a judged run's dump + evidence snapshot — no run, no
LLM — at radius 0 (must reproduce the dump's mention snippets byte for byte, which proves the re-fold
is exact) and at the target radius, and reports per (subject, field): records, records whose snippet
SET changes (their `|ud=` digest changes → they regenerate; the rest replay), snippet characters per
record before and after, and the distinct-snippet count. The snippet is the evidence the model reads
and the wire item the judges see, so this is the size of the evidence change the run will carry.

Inputs: `packages/logs/extraction_dumps/<run>/<subject>__<field>__partial.json` (fold block: windows
with sub_bounds, groups with mentions) and `evidence_snapshots/<run>/chunk_text/<subject>__<a>-<b>.md`
(the chunk's wire text = subject_text[a:b] when it carries no excluded-page marker and no
continued-page header — checked; otherwise the chunk is skipped and reported).

    .venv/bin/python checks/radius_sizing.py --run 20260912T191548 --subjects mathewsco_com tanfel_com [--radius 1]
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import loading  # type: ignore[import-not-found]
else:
    from . import loading

sys.path.insert(0, str(loading.REPO_ROOT / "packages" / "core" / "src"))
from core.utils.aggregation_fold import collect_window  # noqa: E402
from core.utils.floor_scan import CONTINUED_PAGE_MARKER, EXCLUDED_PAGE_MARKER  # noqa: E402

FIELDS = ("products", "equipments", "industries", "conformity_attestations", "material_caps", "process_caps")


def _bounds(s: str) -> tuple[int, int]:
    a, b = s.split(":")
    return int(a), int(b)


def _chunk_text(run: str, subject: str, chunk_bounds: str) -> str | None:
    a, b = _bounds(chunk_bounds)
    path = loading.EVAL_ROOT / "evidence_snapshots" / run / "chunk_text" / f"{subject}__{a}-{b}.md"
    if not path.is_file():
        return None
    text = path.read_text(encoding="utf-8")
    if EXCLUDED_PAGE_MARKER in text or CONTINUED_PAGE_MARKER in text:
        return None
    # the snapshot rstripped the newlines; the slice ends with one (run_tryout_b.py --check)
    return text.rstrip("\n") + "\n"


def size_chunk(chunk: dict[str, Any], text: str, chunk_start: int, radius: int) -> dict[str, Any]:
    fold = chunk["fold"]
    # forms per window = the forms the dump's mentions found there + the zero-hit forms
    forms_by_window: dict[int, set[str]] = defaultdict(set)
    r0_snippets: dict[str, list[str]] = defaultdict(list)  # group_id -> dump snippets (radius 0)
    form_to_group: dict[str, str] = {}
    for g in fold["groups"]:
        for m in g["mentions"]:
            w = int(m["window"])
            forms_by_window[w].add(m["form"])
            r0_snippets[g["group_id"]].append(m["snippet"])
            form_to_group.setdefault(m["form"].lower(), g["group_id"])
        for f in g["forms"]:
            form_to_group.setdefault(f.lower(), g["group_id"])
    for w in fold["windows"]:
        forms_by_window[int(w["window"])].update(w.get("zero_hit_forms") or [])
    synthesized = {row["group_id"] for row in chunk["rows"]}
    recollected: dict[int, dict[str, list[str]]] = {0: defaultdict(list), radius: defaultdict(list)}
    for w in fold["windows"]:
        idx = int(w["window"])
        a, b = _bounds(w["sub_bounds"])
        window_text = text[a - chunk_start : b - chunk_start]
        forms = sorted(forms_by_window[idx])
        for r in (0, radius):
            coll = collect_window(window_text, forms, snippet_radius=r)
            for m in coll.mentions:
                gid = form_to_group.get(m.form.lower()) or form_to_group.get(m.sent_form.lower())
                if gid is None:
                    continue
                recollected[r][gid].append(m.snippet)
    # radius-0 reproduction: per synthesized group, the dump's snippet multiset equals the re-fold's
    reproduced = sum(1 for gid in synthesized if sorted(r0_snippets[gid]) == sorted(recollected[0][gid]))
    changed = 0
    chars0 = chars1 = 0
    distinct0 = distinct1 = 0
    for gid in synthesized:
        s0 = sorted(set(recollected[0][gid]))
        s1 = sorted(set(recollected[radius][gid]))
        if s0 != s1:
            changed += 1
        chars0 += sum(len(s) for s in s0)
        chars1 += sum(len(s) for s in s1)
        distinct0 += len(s0)
        distinct1 += len(s1)
    return {
        "records": len(synthesized),
        "reproduced_r0": reproduced,
        "changed": changed,
        "chars0": chars0,
        "chars1": chars1,
        "distinct0": distinct0,
        "distinct1": distinct1,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run", required=True)
    ap.add_argument("--subjects", nargs="+", required=True, help="dump-safe names, e.g. mathewsco_com")
    ap.add_argument("--radius", type=int, default=1)
    ap.add_argument("--fields", nargs="+", default=list(FIELDS))
    args = ap.parse_args()
    dumps = loading.DUMPS_ROOT / args.run
    total = defaultdict(int)
    print(f"run {args.run}, radius 0 -> {args.radius}; per subject/field: records | re-fold reproduces the dump's "
          f"radius-0 snippets | records whose snippet set CHANGES | snippet chars per record | distinct snippets")
    for subject in args.subjects:
        for field in args.fields:
            path = dumps / f"{subject}__{field}__partial.json"
            if not path.is_file():
                print(f"  {subject}/{field}: no dump")
                continue
            dump = json.loads(path.read_text(encoding="utf-8"))
            agg = defaultdict(int)
            skipped = []
            for cb, chunk in dump["chunks"].items():
                text = _chunk_text(args.run, subject, cb)
                if text is None:
                    skipped.append(cb)
                    continue
                r = size_chunk(chunk, text, _bounds(cb)[0], args.radius)
                for k, v in r.items():
                    agg[k] += v
            n = agg["records"] or 1
            print(f"  {subject:16s} {field:24s} {agg['records']:5d} | r0 reproduced {agg['reproduced_r0']}/{agg['records']} | "
                  f"changed {agg['changed']:5d} = {100 * agg['changed'] / n:5.1f}% | chars {agg['chars0'] / n:6.0f} -> "
                  f"{agg['chars1'] / n:6.0f} (x{agg['chars1'] / max(1, agg['chars0']):.2f}) | distinct {agg['distinct0']} -> {agg['distinct1']}"
                  + (f" | SKIPPED chunks {skipped}" if skipped else ""))
            for k, v in agg.items():
                total[k] += v
    n = total["records"] or 1
    print(f"  {'ALL':41s} {total['records']:5d} | r0 reproduced {total['reproduced_r0']}/{total['records']} | "
          f"changed {total['changed']:5d} = {100 * total['changed'] / n:5.1f}% | chars {total['chars0'] / n:6.0f} -> "
          f"{total['chars1'] / n:6.0f} (x{total['chars1'] / max(1, total['chars0']):.2f}) | distinct {total['distinct0']} -> {total['distinct1']}")


if __name__ == "__main__":
    main()
