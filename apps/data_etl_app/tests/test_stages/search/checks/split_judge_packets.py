"""Split oversized census packets into bounded parts.

A judge agent asked to emit 755 JSONL records in one response quietly stops
early: the file parses, merges, and yields a precision number computed on a
subset the agent chose itself. Measured on run 20260901T013332, the first three
returning agents skipped 0%, 34% and 72% of their forms, and the skipping
scaled with packet size.

So any unit above MAX_FORMS is cut into parts along WINDOW boundaries (never
inside one - a form must be judged with the whole window in view). Each part is
a self-contained packet carrying the shared header. Parts are judged
independently and concatenated back into <unit>.jsonl before the merge.

Usage: .venv/bin/python checks/split_judge_packets.py --run <run_id> [--max-forms 150]
"""
from __future__ import annotations
import argparse, re, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from paths import run_dir  # noqa: E402

WIN_SPLIT_RE = re.compile(r"(?=^## Window \d+:\d+)", re.M)
# A form is printed in single quotes, or in DOUBLE quotes when it contains an
# apostrophe (exporter behaviour). Matching only the first drops every
# possessive - 47 forms on run 20260901T013332, silently.
FORM_RE = re.compile(r"""^\s*\d+\.\s+(?:'(?P<sq>.*)'|"(?P<dq>.*)")\s*$""", re.M)


def forms_in(text: str) -> list[str]:
    return [m.group("sq") if m.group("sq") is not None else m.group("dq")
            for m in FORM_RE.finditer(text)]


def count_forms(block: str) -> int:
    body = block.split("### The window text", 1)[0]
    return len(forms_in(body))


def split_window(block: str, max_forms: int) -> list[str]:
    """Cut ONE oversized window into parts that share its full text.

    The window text is what a form must be judged against, so every part
    repeats it verbatim; only the numbered form list is divided. Original
    numbering is preserved so a part's forms stay traceable to the packet.
    """
    head, _, tail = block.partition("### The window text")
    forms = forms_in(head)
    if len(forms) <= max_forms:
        return [block]
    intro = head.split("### Forms returned by search", 1)[0].rstrip()
    text_block = "### The window text" + tail
    parts = []
    for start in range(0, len(forms), max_forms):
        group = forms[start:start + max_forms]
        listing = "\n".join(
            (f'{start + i + 1}. "{f}"' if "'" in f else f"{start + i + 1}. '{f}'")
            for i, f in enumerate(group)
        )
        parts.append(
            f"{intro}\n\n### Forms returned by search "
            f"({len(group)} of {len(forms)}, numbers {start + 1}-{start + len(group)})\n\n"
            f"{listing}\n\n{text_block}"
        )
    return parts


def split_packet(text: str, max_forms: int) -> list[str]:
    pieces = WIN_SPLIT_RE.split(text)
    header, windows = pieces[0], pieces[1:]
    if not windows:
        return [text]
    total = sum(count_forms(w) for w in windows)
    if total <= max_forms:
        return [text]
    # Oversized windows are divided first, so no unit is left over the cap.
    expanded: list[str] = []
    for w in windows:
        expanded.extend(split_window(w, max_forms))
    parts, cur, cur_n = [], [], 0
    for w in expanded:
        n = count_forms(w)
        if cur and cur_n + n > max_forms:
            parts.append(header + "".join(cur))
            cur, cur_n = [], 0
        cur.append(w)
        cur_n += n
    if cur:
        parts.append(header + "".join(cur))
    return parts


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--max-forms", type=int, default=150)
    args = ap.parse_args()
    rd = run_dir(args.run)
    src = rd / "raw" / "judge_packets"
    out = rd / "raw" / "judge_packets_split"
    out.mkdir(parents=True, exist_ok=True)
    manifest, n_units, n_parts = [], 0, 0
    for pf in sorted(src.glob("*.md")):
        text = pf.read_text(encoding="utf-8")
        parts = split_packet(text, args.max_forms)
        n_units += 1
        if len(parts) == 1:
            dest = out / pf.name
            dest.write_text(text, encoding="utf-8")
            manifest.append((pf.stem, dest.name, count_all(text)))
            n_parts += 1
            continue
        for i, part in enumerate(parts, 1):
            name = f"{pf.stem}__part{i:02d}.md"
            (out / name).write_text(part, encoding="utf-8")
            manifest.append((pf.stem, name, count_all(part)))
            n_parts += 1
    lines = [f"{u}\t{n}\t{c}" for u, n, c in manifest]
    (out / "manifest.tsv").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"{n_units} units -> {n_parts} judging tasks (cap {args.max_forms} forms)")
    big = [m for m in manifest if m[2] > args.max_forms]
    if big:
        print(f"{len(big)} task(s) still over cap (single oversized window):")
        for u, n, c in big[:10]:
            print(f"  {n}  {c} forms")


def count_all(text: str) -> int:
    pieces = WIN_SPLIT_RE.split(text)
    return sum(count_forms(w) for w in pieces[1:])


if __name__ == "__main__":
    main()
