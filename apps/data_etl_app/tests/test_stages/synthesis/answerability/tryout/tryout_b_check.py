"""Mechanical check of run_tryout_b.py outputs (no judging): every output parses, covers exactly the
sent record ids once, and the mean paragraph length per arm and repeat. Run before the judges so a
truncated or one-and-done output is caught as a delivery fault, not read as a prompt effect.

    tryout_b_check.py            → one line per request tag
"""

from __future__ import annotations

import collections
import json
import pathlib
import re
import sys

P2 = pathlib.Path(__file__).resolve().parent / "pass2"
_IDS_RE = re.compile(r"<<<RECORD_IDS\n(\[.*?\])\nRECORD_IDS>>>", re.S)
_OUT_RE = re.compile(r"b_out_(.+)_(c16|b)_(\d+)\.json")


def main() -> int:
    sent: dict[str, set[str]] = {}
    for path in P2.glob("reqb_*_user.txt"):
        tag = path.name[len("reqb_") : -len("_user.txt")]
        match = _IDS_RE.search(path.read_text(encoding="utf-8"))
        if not match:
            print(f"{path.name}: no RECORD_IDS block", file=sys.stderr)
            return 1
        sent[tag] = set(json.loads(match.group(1)))
    rows: dict[str, dict[tuple[str, int], str]] = collections.defaultdict(dict)
    bad = 0
    for path in sorted(P2.glob("b_out_*.json")):
        match = _OUT_RE.match(path.name)
        if not match:
            continue
        tag, arm, k = match.group(1), match.group(2), int(match.group(3))
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            ids = [r["record_id"] for r in data["syntheses"]]
        except (ValueError, KeyError, TypeError) as exc:
            rows[tag][(arm, k)] = f"PARSE-ERR {type(exc).__name__}"
            bad += 1
            continue
        want = sent.get(tag, set())
        flags = ""
        if len(ids) != len(set(ids)):
            flags += " DUP"
        if set(ids) - want:
            flags += " EXTRA"
        if want - set(ids):
            flags += " MISSING"
        if flags:
            bad += 1
        chars = sum(len(r["synthesis"]) for r in data["syntheses"]) // max(1, len(ids))
        rows[tag][(arm, k)] = f"{len(set(ids))}/{len(want)}{flags} {chars}c"
    for tag in sorted(rows):
        cells = "  ".join(f"{a}#{k}:{v}" for (a, k), v in sorted(rows[tag].items()))
        print(f"{tag} (sent {len(sent.get(tag, ()))}) | {cells}")
    n = sum(len(v) for v in rows.values())
    print(f"{n} outputs, {bad} with a fault")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
