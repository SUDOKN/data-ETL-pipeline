"""Mechanically re-check the expectation set against the snapshot texts.

This is the check the VERIFICATION_LEDGER describes as "the assistant
mechanically re-checked EVERY quote through the harness's own matcher". It was
ad hoc for the first 8 subjects; making it a script means a later session can
re-run it after any eval-set edit instead of re-deriving it.

It answers five questions per (subject, field) file, using the SAME matcher the
scorecard uses (`_shared/text_matching`) so a quote that passes here cannot fail
there:

  1. Does the snapshot sha256 still describe the file on disk?
  2. Does the file satisfy the loader's schema (required keys, valid status,
     unique ids)?
  3. Does every evidence quote occur VERBATIM in the snapshot text?  (RETIRED
     entries are exempt - they are kept as a record, and some are retired
     exactly because their evidence turned out to be wrong.)
  4. Is at least one `acceptable_forms` entry actually covered by that entry's
     own evidence?  (An entry whose forms appear in no quote can never credit.)
  5. Can each entry ever INDEPENDENTLY miss?  `forms_overlap` credits an entry
     when a returned form contains one of its acceptable forms, so an entry all
     of whose forms are substrings of SIBLING entries' forms is credited
     whenever any sibling is returned. Its recall number then carries no
     information of its own - a silent hole in the denominator, not a wrong
     entry. Flagged three times by hand (2026-08-26 ledger, and again by the
     mathewsco / howcogroup / lucasmilhaupt verification passes) before being
     made mechanical here.
  6. Do the `false_friends` forms occur in the text at all?  Search returns
     verbatim spans, so a false friend absent from the text is ANTICIPATORY: it
     can only be hit by fabrication, never by the trap it was written for. Worth
     knowing which of the two a given entry is; not a defect either way.

Failures on 1-4 are ERRORS (the eval set is wrong); 5 and 6 are WARNINGS.

Usage:
    .venv/bin/python checks/validate_expectations.py            # all subjects
    .venv/bin/python checks/validate_expectations.py --subject med-tekinc.com
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from _shared.text_matching import (  # noqa: E402
    SHORT_FORM_MAX_LENGTH,
    collapse_whitespace,
    flexible_pattern,
    normalize_spaces,
)
from expectations import load_expectations  # noqa: E402
from paths import EXPECTATIONS_DIR, REPO_ROOT, SEARCH_FIELDS  # noqa: E402


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _occurs(form: str, normalized_text: str, *, case_sensitive: bool = True) -> bool:
    """`occurs_in` against text that is ALREADY normalized.

    `occurs_in` normalizes its haystack on every call, which is fine for a
    window and quadratic for a 1.8 MB subject with several thousand quotes -
    that build never finished. Same matcher, hoisted normalization.
    """
    if len(form.strip()) <= 3:
        case_sensitive = True
    pattern = flexible_pattern(form, case_sensitive=case_sensitive)
    return bool(pattern and pattern.search(normalized_text))


def _snapshot_text(payload: dict) -> tuple[Path | None, str | None]:
    """Resolve a file's `snapshot.file` to (path, sha) without reading it."""
    snapshot = payload.get("snapshot") or {}
    rel = snapshot.get("file")
    if not rel:
        return None, None
    return REPO_ROOT / rel, snapshot.get("sha256")


def validate_subject(slug: str) -> tuple[list[str], list[str], dict[str, int]]:
    errors: list[str] = []
    warnings: list[str] = []
    counts = {"entries": 0, "quotes": 0, "confirmed": 0, "disputed": 0, "candidate": 0}
    subject_dir = EXPECTATIONS_DIR / slug
    subject_yaml = subject_dir / "subject.yaml"
    if not subject_yaml.is_file():
        return [f"{slug}: no subject.yaml"], warnings, counts

    subject_payload = yaml.safe_load(subject_yaml.read_text()) or {}
    subject_name = subject_payload.get("subject")
    if not subject_name:
        errors.append(f"{slug}/subject.yaml: no `subject:` key")
        return errors, warnings, counts

    text_cache: dict[Path, str] = {}
    seen_ids: set[str] = set()

    for field_name in SEARCH_FIELDS:
        path = subject_dir / f"{field_name}.yaml"
        if not path.is_file():
            errors.append(f"{slug}/{field_name}.yaml: missing")
            continue
        payload = yaml.safe_load(path.read_text()) or {}
        text_path, recorded_sha = _snapshot_text(payload)
        if text_path is None:
            errors.append(f"{slug}/{field_name}.yaml: no snapshot.file")
            continue
        if not text_path.is_file():
            errors.append(f"{slug}/{field_name}.yaml: snapshot.file not found: {text_path}")
            continue
        actual_sha = _sha256(text_path)
        if recorded_sha and recorded_sha != actual_sha:
            errors.append(
                f"{slug}/{field_name}.yaml: snapshot sha256 mismatch "
                f"(recorded {recorded_sha[:12]}..., actual {actual_sha[:12]}...)"
            )
        if text_path not in text_cache:
            text_cache[text_path] = normalize_spaces(
                text_path.read_text(errors="replace")
            )
        text = text_cache[text_path]

        exp = load_expectations(subject_name, field_name)
        if exp is None:
            errors.append(f"{slug}/{field_name}.yaml: loader returned nothing")
            continue
        for problem in exp.problems:
            errors.append(f"{slug}/{field_name}: schema: {problem}")

        for entry in exp.entries:
            counts["entries"] += 1
            status = entry.get("status")
            if status in counts:
                counts[status] += 1
            eid = str(entry.get("id", "<no id>"))
            if eid in seen_ids:
                errors.append(f"{slug}: duplicate entry id across files: {eid}")
            seen_ids.add(eid)
            if not eid.startswith(f"{slug}-{field_name}-"):
                warnings.append(f"{slug}/{field_name}: id off-convention: {eid}")

            # A RETIRED entry is kept as a record of a judgment, never used.
            # Several are retired precisely BECAUSE their evidence was wrong, so
            # holding them to the quote contract would make the ledger's own
            # "retire, never delete" rule impossible to satisfy.
            if status == "retired":
                continue

            forms = [str(f) for f in (entry.get("acceptable_forms") or [])]
            if not forms:
                errors.append(f"{eid}: no acceptable_forms")
            quotes = [str(q.get("quote", "")) for q in (entry.get("evidence") or [])]
            if not quotes:
                errors.append(f"{eid}: no evidence")

            covered_by_own_evidence = False
            for quote in quotes:
                counts["quotes"] += 1
                if not quote.strip():
                    errors.append(f"{eid}: empty evidence quote")
                    continue
                if not _occurs(quote, text, case_sensitive=True):
                    if _occurs(quote, text, case_sensitive=False):
                        errors.append(f"{eid}: quote matches only case-INSENSITIVELY: {quote[:70]!r}")
                    else:
                        errors.append(f"{eid}: quote not found in snapshot text: {quote[:70]!r}")
                    continue
                if any(_occurs(form, normalize_spaces(quote), case_sensitive=False) for form in forms):
                    covered_by_own_evidence = True
            if quotes and not covered_by_own_evidence:
                errors.append(
                    f"{eid}: no acceptable_form is covered by its own evidence "
                    f"(forms={forms[:3]})"
                )

        # (5) entries that can never independently miss -- see the docstring.
        blob = "\n".join(
            form
            for entry in exp.entries
            for form in (entry.get("acceptable_forms") or [])
        ).casefold()
        for entry in exp.entries:
            if entry.get("status") in {"retired", "disputed"}:
                continue
            forms = [str(f) for f in (entry.get("acceptable_forms") or [])]
            shadowed = []
            for form in forms:
                key = collapse_whitespace(form).casefold()
                if len(key) <= SHORT_FORM_MAX_LENGTH or not key:
                    continue  # short forms are word-boundary matched; not at risk
                own = sum(1 for f in forms if collapse_whitespace(f).casefold() == key)
                if blob.count(key) > own:
                    shadowed.append(form)
            if forms and len(shadowed) == len(
                [f for f in forms
                 if len(collapse_whitespace(f).casefold()) > SHORT_FORM_MAX_LENGTH]
            ) and shadowed:
                warnings.append(
                    f"{entry.get('id')}: every form is a substring of a sibling entry's "
                    f"form, so it can never independently miss: {shadowed[:3]}"
                )

        for friend in exp.false_friends:
            form = str(friend.get("form", ""))
            if not form.strip():
                errors.append(f"{slug}/{field_name}: empty false_friend form")
                continue
            if not _occurs(form, text, case_sensitive=False):
                warnings.append(
                    f"{slug}/{field_name}: anticipatory false friend "
                    f"(absent from text, so only fabrication can hit it): {form!r}"
                )

    return errors, warnings, counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--subject", help="subject unique id or slug; default = all")
    args = parser.parse_args()

    if args.subject:
        slugs = [args.subject.replace(".", "_")]
    else:
        slugs = sorted(p.name for p in EXPECTATIONS_DIR.iterdir() if p.is_dir())

    total_errors = 0
    for slug in slugs:
        errors, warnings, counts = validate_subject(slug)
        total_errors += len(errors)
        status = "FAIL" if errors else "ok"
        print(
            f"[{status:>4}] {slug}: {counts['entries']} entries "
            f"({counts['confirmed']} confirmed / {counts['candidate']} candidate / "
            f"{counts['disputed']} disputed), {counts['quotes']} quotes, "
            f"{len(errors)} errors, {len(warnings)} warnings"
        )
        for message in errors:
            print(f"         ERROR   {message}")
        for message in warnings:
            print(f"         warn    {message}")
    print(f"\n{len(slugs)} subject(s), {total_errors} error(s).")
    return 1 if total_errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
