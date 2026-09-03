"""Drop misses that a split-window judge could not know were already covered.

Where one window held more forms than the per-task cap, its form LIST was
divided across parts (the window text was repeated in full). A judge holding
forms 1-150 of a 227-form window cannot see forms 151-227, so it files a miss
for an entity the unseen half may already cover.

Confirmed live on run 20260901T013332: the steelcraft products part03 judge
cross-checked part04's filed misses and found two already covered by forms in
its own half - 'F Series frames' (part03 form #6) and 'wedge-lock corner clips'
(part03 form #138).

This recomputes, per (unit, window), the UNION of forms across every part, and
removes any miss whose entity is covered by a form its own judge could not see.
Coverage uses the same `form_covers` the scorer uses, so a removal here means
the scorer would have credited it.

Usage: .venv/bin/python checks/fix_split_window_misses.py --run <id> [--apply]
"""
from __future__ import annotations
import argparse, json, re, sys
from collections import defaultdict
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from paths import run_dir  # noqa: E402
from split_judge_packets import WIN_SPLIT_RE, forms_in  # noqa: E402
from _shared.text_matching import form_covers  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    rd = run_dir(args.run)
    split, jdir = rd / "raw" / "judge_packets_split", rd / "judgments"

    # (unit, window) -> {part_stem: [forms]}
    per_part: dict[tuple[str, str], dict[str, list[str]]] = defaultdict(dict)
    for pf in sorted(split.glob("*__part*.md")):
        unit = pf.stem.split("__part")[0]
        for block in WIN_SPLIT_RE.split(pf.read_text(encoding="utf-8"))[1:]:
            window = re.match(r"## Window (\d+:\d+)", block).group(1)
            forms = forms_in(block.split("### The window text", 1)[0])
            per_part[(unit, window)][pf.stem] = forms

    shared = {k: v for k, v in per_part.items() if len(v) > 1}
    print(f"{len(shared)} window(s) split across parts\n")

    removed_total = 0
    for (unit, window), parts in sorted(shared.items()):
        union = [f for fs in parts.values() for f in fs]
        for stem, own in parts.items():
            jf = jdir / f"{stem}.jsonl"
            if not jf.is_file():
                continue
            lines = jf.read_text(encoding="utf-8").splitlines()
            keep, dropped = [], []
            for line in lines:
                s = line.strip()
                if not s:
                    continue
                try:
                    rec = json.loads(s)
                except json.JSONDecodeError:
                    keep.append(line); continue
                if rec.get("type") != "miss" or rec.get("window") != window:
                    keep.append(line); continue
                entity = rec.get("entity") or ""
                unseen = [f for f in union if f not in own]
                if any(form_covers(entity, f) for f in unseen):
                    dropped.append(entity)
                else:
                    keep.append(line)
            if dropped:
                removed_total += len(dropped)
                print(f"{stem}  window {window}")
                for d in dropped:
                    print(f"    drop miss: {d!r} (covered by a form in a sibling part)")
                if args.apply:
                    jf.write_text("\n".join(keep) + "\n", encoding="utf-8")
    print(f"\n{removed_total} spurious miss(es) "
          + ("removed" if args.apply else "found (dry run — pass --apply)"))


if __name__ == "__main__":
    main()
