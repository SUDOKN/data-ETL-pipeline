"""Build a subject's expectation YAML files from a seeding agent's JSONL.

WHY THIS EXISTS. Seeding a subject means reading its whole scraped text and
writing six schema-conformant YAML files. Asking an agent to emit the YAML
directly puts four mechanical concerns (id numbering, snapshot header, sha256,
`approx_offset`) inside a judgment task, where they get quietly wrong. This
script owns all four, so the agent only has to do the part that needs reading:
which entities the text names, in which words, with which quote.

INPUT — one JSON object per line (see SEEDING_BRIEF for the agent's copy):

    {"type": "subject", "role": "...", "context": "...",
     "scrape_hazards": [{"hazard": "...", "quote": "..."}],
     "sampling_notes": "...", "notes": "..."}

    {"type": "field_meta", "field": "products", "expected_empty": false,
     "notes": "..."}

    {"field": "products", "name": "thread plug gage",
     "acceptable_forms": ["thread plug gage", "thread plug gages"],
     "evidence": [{"quote": "our own line of thread plug gages"}],
     "actor": "own", "notes": "..."}

    {"type": "false_friend", "field": "products", "form": "Aerospace",
     "reason": "an industry, not an artifact"}

Entries default to `status: candidate` - promotion to `confirmed` is the
verification pass's job (`apply_verifications.py`), never the seed's.

Usage:
    .venv/bin/python checks/build_expectations.py --slug tanfel_com \
        --jsonl <path>.jsonl [--merge]     # --merge folds into existing files
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from _shared.text_matching import (  # noqa: E402
    collapse_whitespace,
    flexible_pattern,
    normalize_spaces,
)
from paths import EXPECTATIONS_DIR, REPO_ROOT, SEARCH_FIELDS  # noqa: E402

SAMPLE_TEXTS_REL = "apps/data_etl_app/tests/test_stages/sample_scraped_texts"
# An evidence quote is meant to be a short verbatim span - roughly one line -
# that locates the entity precisely. The harness's own schema test enforces this
# cap, so catching it here turns a puzzling test failure later into a named
# complaint at seeding time.
MAX_QUOTE_LENGTH = 200
VALID_ACTORS = {
    "own", "client", "supplier", "lab", "parent_sibling",
    "reseller_inventory", "unknown",
}


def _text_path_for(slug: str) -> Path:
    """`tanfel_com` -> the .txt whose name matches once dots are restored."""
    directory = REPO_ROOT / SAMPLE_TEXTS_REL
    for candidate in sorted(directory.glob("*.txt")):
        if candidate.stem.replace(".", "_") == slug:
            return candidate
    raise SystemExit(f"no scraped text found for slug {slug!r} in {directory}")


def _offset_of(quote: str, normalized_text: str) -> int | None:
    """Advisory offset: where the quote's first character lands, or None.

    Takes text that is ALREADY normalized - one character for one character, so
    the offset indexes the raw text too. Normalizing per quote instead made the
    1.8 MB subject quadratic and it never finished; the caller normalizes once.
    """
    pattern = flexible_pattern(quote, case_sensitive=True)
    if pattern is None:
        return None
    match = pattern.search(normalized_text)
    return match.start() if match else None


def _trim_quote(quote: str, forms: list[str], normalized_text: str) -> str | None:
    """Shrink an over-long quote to a still-verbatim window around a covering form.

    A quote exists to LOCATE the entity, so a window around the form serves the
    same purpose as the whole sentence. Trimming is deterministic and the result
    is still a verbatim span of one line, so nothing about the evidence weakens.
    Returns None when no window works, which is a real complaint.
    """
    for form in forms:
        pattern = flexible_pattern(form, case_sensitive=False)
        if pattern is None:
            continue
        match = pattern.search(quote)
        if match is None:
            continue
        span = match.end() - match.start()
        for pad in range((MAX_QUOTE_LENGTH - span) // 2, 0, -10):
            low, high = max(0, match.start() - pad), min(len(quote), match.end() + pad)
            # never cut a word in half - widen to the nearest boundary
            while low > 0 and quote[low - 1].isalnum():
                low -= 1
            while high < len(quote) and quote[high].isalnum():
                high += 1
            candidate = quote[low:high].strip()
            if (
                len(candidate) <= MAX_QUOTE_LENGTH
                and _offset_of(candidate, normalized_text) is not None
                and pattern.search(candidate) is not None
            ):
                return candidate
    return None


def _sorted_by_first_appearance(
    entries: list[dict[str, Any]], text: str
) -> list[dict[str, Any]]:
    """Document order makes a hand review of the YAML follow the site."""

    def key(entry: dict[str, Any]) -> int:
        offsets = [
            e["approx_offset"]
            for e in entry.get("evidence", [])
            if e.get("approx_offset") is not None
        ]
        return min(offsets) if offsets else len(text)

    return sorted(entries, key=key)


def build(slug: str, jsonl_path: Path, merge: bool) -> int:
    text_path = _text_path_for(slug)
    text = text_path.read_text(errors="replace")
    normalized_text = normalize_spaces(text)
    sha = hashlib.sha256(text_path.read_bytes()).hexdigest()
    subject_name = text_path.stem

    subject_meta: dict[str, Any] = {}
    field_meta: dict[str, dict[str, Any]] = {f: {} for f in SEARCH_FIELDS}
    by_field: dict[str, list[dict[str, Any]]] = {f: [] for f in SEARCH_FIELDS}
    friends: dict[str, list[dict[str, Any]]] = {f: [] for f in SEARCH_FIELDS}
    complaints: list[str] = []
    trims: list[str] = []
    dropped_friends: list[str] = []

    for line_number, raw in enumerate(jsonl_path.read_text().splitlines(), 1):
        raw = raw.strip()
        if not raw or raw.startswith("//"):
            continue
        try:
            row = json.loads(raw)
        except json.JSONDecodeError as exc:
            complaints.append(f"line {line_number}: unparseable JSON ({exc})")
            continue

        kind = row.get("type", "entry")
        if kind == "subject":
            subject_meta = row
            continue
        field_name = row.get("field")
        if field_name not in by_field:
            complaints.append(f"line {line_number}: unknown field {field_name!r}")
            continue
        if kind == "field_meta":
            field_meta[field_name] = row
        elif kind == "false_friend":
            friends[field_name].append(
                {"form": row.get("form", ""), "reason": row.get("reason", "")}
            )
        elif kind == "entry":
            actor = row.get("actor")
            if actor and actor not in VALID_ACTORS:
                complaints.append(f"line {line_number}: unknown actor {actor!r}")
            evidence = []
            for item in row.get("evidence") or []:
                quote = item.get("quote", "") if isinstance(item, dict) else str(item)
                record: dict[str, Any] = {"quote": quote}
                offset = _offset_of(quote, normalized_text)
                if offset is None:
                    complaints.append(
                        f"line {line_number}: quote absent from text: {quote[:60]!r}"
                    )
                else:
                    record["approx_offset"] = offset
                if len(quote) > MAX_QUOTE_LENGTH:
                    forms = [str(f) for f in (row.get("acceptable_forms") or [])]
                    trimmed = _trim_quote(quote, forms, normalized_text)
                    if trimmed is None:
                        complaints.append(
                            f"line {line_number}: quote is {len(quote)} chars, over the "
                            f"{MAX_QUOTE_LENGTH}-char cap, and no window around an "
                            f"acceptable form fits: {quote[:60]!r}"
                        )
                    else:
                        trims.append(f"line {line_number}: {len(quote)} -> {len(trimmed)} chars")
                        record["quote"] = trimmed
                        offset = _offset_of(trimmed, normalized_text)
                        if offset is not None:
                            record["approx_offset"] = offset
                evidence.append(record)
            # A seed may argue that an entity sits outside its field's clause;
            # `disputed` keeps the argument on the record without gating recall.
            # It may never promote itself past that - `confirmed` is the
            # verification pass's word alone.
            status_hint = row.get("status_hint")
            # Fallback for the convention two seeding agents converged on
            # independently while `status_hint` was still being dropped: a
            # dispute argument written into `notes` behind a DISPUTED marker.
            # Kept because it is self-documenting in the YAML either way.
            if status_hint is None and str(row.get("notes", "")).upper().startswith("DISPUTED"):
                status_hint = "disputed"
            if status_hint not in (None, "candidate", "disputed"):
                complaints.append(
                    f"line {line_number}: a seed may not set status {status_hint!r} "
                    f"(only 'disputed'); left as candidate"
                )
                status_hint = None
            entry = {
                "status": status_hint or "candidate",
                "name": row.get("name", ""),
                "acceptable_forms": list(row.get("acceptable_forms") or []),
                "evidence": evidence,
            }
            if actor and actor != "own":
                entry["actor"] = actor
            if row.get("notes"):
                entry["notes"] = row["notes"]
            by_field[field_name].append(entry)
        else:
            complaints.append(f"line {line_number}: unknown type {kind!r}")

    subject_dir = EXPECTATIONS_DIR / slug
    subject_dir.mkdir(parents=True, exist_ok=True)
    seeder = f"seed-agent-{slug.replace('_', '-')}"
    date = subject_meta.get("date", "2026-08-27")
    written = 0

    for field_name in SEARCH_FIELDS:
        path = subject_dir / f"{field_name}.yaml"
        existing: list[dict[str, Any]] = []
        version = 1
        if merge and path.is_file():
            payload = yaml.safe_load(path.read_text()) or {}
            existing = list(payload.get("entries") or [])
            version = int(payload.get("eval_set_version") or 1)

        seen = {collapse_whitespace(e.get("name", "")).casefold() for e in existing}
        fresh = []
        for entry in _sorted_by_first_appearance(by_field[field_name], text):
            key = collapse_whitespace(entry["name"]).casefold()
            if key in seen:
                continue
            seen.add(key)
            fresh.append(entry)

        entries = existing + fresh
        for index, entry in enumerate(entries, 1):
            entry.setdefault("id", "")
            if not entry["id"]:
                entry["id"] = f"{slug}-{field_name}-{index:04d}"
            entry.setdefault(
                "provenance",
                [{"action": "added", "by": seeder, "date": date, "source": "corpus-seed"}],
            )
            # Key order that reads well in review, not YAML's alphabetical default.
            ordered = {}
            for key_name in ("id", "name", "status", "acceptable_forms", "evidence",
                             "actor", "notes", "provenance"):
                if key_name in entry:
                    ordered[key_name] = entry.pop(key_name)
            ordered.update(entry)
            entries[index - 1] = ordered

        # A false friend that is ALSO an entry's acceptable form in the same
        # field contradicts itself: the judged pass would count the same string
        # as junk and as a find. Real cause, seen live: on a sliced subject one
        # agent writes a form off as anticipatory because it is absent from ITS
        # slice, while another seeds it as an entry from a page it can see.
        # The entry wins - it has a witnessed occurrence behind it.
        seeded_forms = {
            collapse_whitespace(form).casefold()
            for entry in entries
            for form in entry.get("acceptable_forms", [])
        }
        kept_friends = []
        for friend in friends[field_name]:
            key = collapse_whitespace(str(friend.get("form", ""))).casefold()
            if key in seeded_forms:
                dropped_friends.append(f"{field_name}: {friend.get('form')!r}")
            else:
                kept_friends.append(friend)
        friends[field_name] = kept_friends

        meta = field_meta[field_name]
        document: dict[str, Any] = {
            "subject": subject_name,
            "field": field_name,
            "snapshot": {"file": f"{SAMPLE_TEXTS_REL}/{text_path.name}", "sha256": sha},
            "eval_set_version": version,
            "expected_empty": bool(meta.get("expected_empty", False)),
        }
        if meta.get("notes"):
            document["notes"] = meta["notes"]
        document["entries"] = entries
        if friends[field_name]:
            document["false_friends"] = friends[field_name]
        path.write_text(
            yaml.safe_dump(document, sort_keys=False, allow_unicode=True, width=110)
        )
        written += 1
        print(f"  {field_name:<24} {len(entries):>4} entries ({len(fresh)} new)")

    if subject_meta:
        subject_document: dict[str, Any] = {
            "subject": subject_name,
            "slug": slug,
            "role": subject_meta.get("role", "full_unit"),
            "snapshot": {"file": f"{SAMPLE_TEXTS_REL}/{text_path.name}", "sha256": sha},
            "eval_set_version": 1,
        }
        for key_name in ("context", "scrape_hazards", "sampling_notes", "notes"):
            if subject_meta.get(key_name):
                subject_document[key_name] = subject_meta[key_name]
        subject_document["provenance"] = [
            {"action": "added", "by": seeder, "date": date, "source": "corpus-seed"}
        ]
        (subject_dir / "subject.yaml").write_text(
            yaml.safe_dump(subject_document, sort_keys=False, allow_unicode=True, width=110)
        )
        written += 1

    print(f"\n{slug}: wrote {written} file(s) under {subject_dir}")
    if trims:
        print(f"{len(trims)} over-long quote(s) trimmed to a window around a covering form.")
    if dropped_friends:
        print(
            f"{len(dropped_friends)} false friend(s) dropped for colliding with a "
            f"seeded acceptable form in the same field:"
        )
        for message in dropped_friends:
            print(f"  - {message}")
    if complaints:
        print(f"\n{len(complaints)} complaint(s) from the seed JSONL:")
        for message in complaints:
            print(f"  - {message}")
    return 1 if complaints else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--slug", required=True)
    parser.add_argument("--jsonl", required=True, type=Path)
    parser.add_argument("--merge", action="store_true",
                        help="fold into existing files instead of replacing entries")
    args = parser.parse_args()
    return build(args.slug, args.jsonl, args.merge)


if __name__ == "__main__":
    raise SystemExit(main())
