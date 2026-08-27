"""Export a STRIPPED packet of a subject's entries for the verification pass.

The verification pass only means something if the second reader re-derives each
judgment from the text instead of ratifying the first reader's argument. The
seed's `notes`, `actor` labels and `provenance` carry that argument, so they are
withheld here: the packet holds the id, the name, the acceptable forms and the
evidence quote, and nothing else.

Writes one markdown file per subject. Quotes are fenced individually so that a
non-breaking space or a double space inside one survives the round trip
visibly rather than being "tidied" by a markdown renderer.

Usage:
    .venv/bin/python checks/export_verify_packet.py --slug tanfel_com --out <dir>
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))
from paths import EXPECTATIONS_DIR, REPO_ROOT, SEARCH_FIELDS  # noqa: E402

BOUNDARY_SOURCE = (
    "apps/data_etl_app/src/data_etl_app/knowledge/prompts/final_texts/"
    "static/multi_stage/1_phrase_search/"
)


def _line_of(offset: int, line_starts: list[int]) -> int:
    """1-indexed line holding a character offset, by binary search."""
    import bisect
    return bisect.bisect_right(line_starts, offset)


def export(slug: str, out_dir: Path, max_entries: int = 0) -> list[Path]:
    subject_dir = EXPECTATIONS_DIR / slug
    if not subject_dir.is_dir():
        raise SystemExit(f"no expectations for {slug!r}")
    subject_payload = yaml.safe_load((subject_dir / "subject.yaml").read_text()) or {}
    text_rel = (subject_payload.get("snapshot") or {}).get("file", "")

    # Offsets -> line numbers, so a chunked packet can tell its agent WHERE in a
    # very large file its entries live. Without that hint a verifier of a 1.8 MB
    # subject has no way to read only the part its packet is about.
    line_starts: list[int] = []
    if text_rel:
        text_path = REPO_ROOT / text_rel
        if text_path.is_file():
            position = 0
            for line in text_path.read_text(errors="replace").splitlines(keepends=True):
                line_starts.append(position)
                position += len(line)

    header: list[str] = [
        f"# Verification packet — {subject_payload.get('subject', slug)}",
        "",
        f"Scraped text: `{text_rel}`",
        f"Field boundaries: `{BOUNDARY_SOURCE}` (quoted in `SEEDING_BRIEF.md` §4)",
        "",
        "Every entry below was written by ONE reader. Re-derive each judgment from",
        "the text. The seed's notes and actor labels are deliberately withheld.",
        "",
    ]

    # Flatten to (field, entry) so chunking can cut anywhere without splitting
    # an entry, while entries stay in document order within each field.
    flat: list[tuple[str, dict]] = []
    empty_fields: list[str] = []
    for field_name in SEARCH_FIELDS:
        path = subject_dir / f"{field_name}.yaml"
        if not path.is_file():
            continue
        payload = yaml.safe_load(path.read_text()) or {}
        entries = list(payload.get("entries") or [])
        if payload.get("expected_empty"):
            empty_fields.append(field_name)
        for entry in entries:
            flat.append((field_name, entry))

    def render(chunk: list[tuple[str, dict]]) -> tuple[list[str], list[int]]:
        body: list[str] = []
        offsets: list[int] = []
        current_field = None
        for field_name, entry in chunk:
            if field_name != current_field:
                current_field = field_name
                body.append(f"## {field_name}")
                if field_name in empty_fields:
                    body.append("")
                    body.append("The seed marked this field **expected_empty**. Judge that claim")
                    body.append("too: is the correct answer here really the empty list?")
                body.append("")
            body.append(f"### `{entry.get('id')}` — {entry.get('name')}")
            body.append("")
            body.append("Acceptable forms:")
            for form in entry.get("acceptable_forms") or []:
                body.append(f"- `{form}`")
            body.append("")
            body.append("Evidence:")
            for item in entry.get("evidence") or []:
                body.append("")
                body.append("```")
                body.append(str(item.get("quote", "")))
                body.append("```")
                if item.get("approx_offset") is not None:
                    offsets.append(int(item["approx_offset"]))
            body.append("")
        return body, offsets

    out_dir.mkdir(parents=True, exist_ok=True)
    if max_entries:
        # Chunk by LOCATION, not by field. Chunking in field order hands each
        # agent entries scattered over the whole document, so on a 1.8 MB
        # subject every packet needs the whole file read - which defeats the
        # split. Sorting by first evidence offset gives each packet a
        # contiguous region it can actually read; fields are regrouped inside
        # the packet so it still reads field by field.
        def first_offset(item: tuple[str, dict]) -> int:
            offsets = [
                e["approx_offset"]
                for e in item[1].get("evidence") or []
                if e.get("approx_offset") is not None
            ]
            return min(offsets) if offsets else 0

        flat.sort(key=first_offset)
        chunks = [flat[i:i + max_entries] for i in range(0, len(flat), max_entries)]
        field_order = {name: index for index, name in enumerate(SEARCH_FIELDS)}
        chunks = [
            sorted(chunk, key=lambda item: (field_order[item[0]], first_offset(item)))
            for chunk in chunks
        ]
    else:
        chunks = [flat]
    written: list[Path] = []
    for index, chunk in enumerate(chunks, 1):
        body, offsets = render(chunk)
        prelude = list(header)
        if len(chunks) > 1:
            prelude.insert(1, "")
            prelude.insert(2, f"**Packet {index} of {len(chunks)}** — {len(chunk)} entries. "
                              "Other packets cover the rest of this subject; judge only what is here.")
            if offsets and line_starts:
                low = _line_of(min(offsets), line_starts)
                high = _line_of(max(offsets), line_starts)
                prelude.insert(3, f"Its evidence lies between roughly line {low:,} and "
                                  f"line {high:,} of the scraped text.")
        suffix = f".{index}" if len(chunks) > 1 else ""
        out_path = out_dir / f"{slug}.verify_packet{suffix}.md"
        out_path.write_text("\n".join(prelude + body))
        written.append(out_path)
        print(f"{slug}: packet {index}/{len(chunks)}, {len(chunk)} entries -> {out_path}")
    return written


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--slug", required=True)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--max-entries", type=int, default=0,
                        help="split into numbered packets of at most N entries")
    args = parser.parse_args()
    export(args.slug, args.out, args.max_entries)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
