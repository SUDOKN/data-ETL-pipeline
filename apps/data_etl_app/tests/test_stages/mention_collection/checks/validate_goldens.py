"""Mechanically re-check the golden corpus.

    python checks/validate_goldens.py            # every subject
    python checks/validate_goldens.py --subject steelcraft.com
    python checks/validate_goldens.py --quotes   # also re-verify every quote

This is the check the VERIFICATION_LEDGER quotes rather than describing in
prose: it re-derives every count from the files, so the ledger cannot drift
away from the corpus it documents.

What it enforces:

* every subject directory has a `subject.yaml` and all seven field files
  (an empty `labels: []` is required, never an absent file — "this field is
  genuinely empty" is a real answer and must be distinguishable from
  "nobody labelled this field");
* every snapshot sha256 matches the pinned text on disk;
* every label satisfies `goldens.validate_label` — including the rule that a
  `confirmed` label carries a `verified` provenance row;
* label ids are unique within a file;
* `--quotes` re-reads each evidence quote out of the snapshot and confirms it
  is there verbatim, whitespace-elastically.

Exit code is non-zero when anything fails, so it can gate a commit.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Optional

import yaml

if __package__ in (None, ""):  # pragma: no cover - script invocation
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import goldens  # type: ignore[import-not-found]
    import loading  # type: ignore[import-not-found]
    import paths  # type: ignore[import-not-found]
else:
    from . import goldens, loading, paths

sys.path.insert(0, str(paths.TEST_STAGES_ROOT))
from _shared.text_matching import collapse_whitespace  # noqa: E402


def _quote_present(quote: str, text: str, collapsed: str) -> bool:
    """Is this quote really in the snapshot?

    Compared with whitespace collapsed on both sides, because the corpus is
    full of non-breaking spaces and double spaces mid-phrase; an exact-bytes
    check here would report faithful quotes as missing, which is the same
    false-alarm class the shared matcher exists to prevent.
    """
    if quote in text:
        return True
    return collapse_whitespace(quote) in collapsed


def validate(
    subjects: Optional[list[str]] = None, *, check_quotes: bool = False
) -> tuple[dict[str, Any], list[str]]:
    errors: list[str] = []
    stats: Counter[str] = Counter()
    per_subject: dict[str, dict[str, int]] = {}

    directories = sorted(
        d
        for d in paths.GOLDENS_ROOT.iterdir()
        if d.is_dir() and not d.name.startswith("_")
    )
    if subjects:
        wanted = {paths.subject_slug(s) for s in subjects}
        directories = [d for d in directories if d.name in wanted]

    for directory in directories:
        slug = directory.name
        subject = paths.subject_of_slug(slug)
        stats["subjects"] += 1
        counts: Counter[str] = Counter()

        subject_file = directory / "subject.yaml"
        if not subject_file.is_file():
            errors.append(f"{slug}: no subject.yaml")
        else:
            try:
                profile = yaml.safe_load(subject_file.read_text(encoding="utf-8")) or {}
            except yaml.YAMLError as error:
                errors.append(f"{slug}/subject.yaml: unparseable — {error}")
                profile = {}
            snapshot = profile.get("snapshot") or {}
            declared = str(snapshot.get("sha256", ""))
            text_file = paths.text_path(subject)
            if not text_file.is_file():
                errors.append(f"{slug}: no pinned snapshot at {text_file}")
            elif declared != paths.sha256_of(text_file):
                errors.append(
                    f"{slug}/subject.yaml: sha256 does not match the snapshot on disk"
                )

        text = loading.load_text(subject) if check_quotes else None
        collapsed = collapse_whitespace(text) if text is not None else ""

        for field in paths.PHRASE_FIELDS:
            path = directory / f"{field}.yaml"
            if not path.is_file():
                errors.append(f"{slug}: missing {field}.yaml (an empty list is required)")
                continue
            try:
                payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            except yaml.YAMLError as error:
                errors.append(f"{slug}/{field}.yaml: unparseable — {error}")
                continue
            if "labels" not in payload:
                errors.append(f"{slug}/{field}.yaml: no `labels` key")
                continue

            seen_ids: set[str] = set()
            for raw in payload.get("labels") or []:
                counts["labels"] += 1
                label_id = str(raw.get("id", ""))
                if label_id in seen_ids:
                    errors.append(f"{slug}/{field}.yaml: duplicate label id {label_id}")
                seen_ids.add(label_id)
                for problem in goldens.validate_label(raw):
                    errors.append(f"{slug}/{field}.yaml [{label_id}]: {problem}")
                counts[str(raw.get("status", "?"))] += 1
                expect = raw.get("expect") or {}
                counts["occurrences"] += int(expect.get("occurrences", 0) or 0)
                if expect.get("must_not_occur"):
                    counts["zero_occurrence"] += 1
                if expect.get("naive_substring_hits"):
                    counts["substring_traps"] += 1
                for evidence in raw.get("evidence") or []:
                    counts["quotes"] += 1
                    quote = str(evidence.get("quote", ""))
                    if check_quotes and text is not None and quote:
                        if not _quote_present(quote, text, collapsed):
                            errors.append(
                                f"{slug}/{field}.yaml [{label_id}]: quote not found "
                                f"in the snapshot: {quote[:70]!r}"
                            )
        per_subject[slug] = dict(counts)
        stats.update(counts)

    return {"totals": dict(stats), "per_subject": per_subject}, errors


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--subject", action="append", default=None)
    parser.add_argument(
        "--quotes",
        action="store_true",
        help="also re-verify every evidence quote against the snapshot (slower)",
    )
    args = parser.parse_args(argv)

    report, errors = validate(args.subject, check_quotes=args.quotes)
    totals = report["totals"]

    print(
        f"{totals.get('subjects', 0)} subjects, "
        f"{totals.get('labels', 0)} labels, "
        f"{totals.get('quotes', 0)} quotes"
    )
    for status in ("confirmed", "candidate", "disputed", "retired"):
        if totals.get(status):
            share = totals[status] / max(totals.get("labels", 1), 1)
            print(f"  {status:10s} {totals[status]:6d}  ({share:.1%})")
    print(
        f"  occurrences {totals.get('occurrences', 0)}, "
        f"zero-occurrence {totals.get('zero_occurrence', 0)}, "
        f"substring traps {totals.get('substring_traps', 0)}"
    )
    if not args.quotes:
        print("  (quotes NOT re-verified — pass --quotes for the full check)")

    if errors:
        print(f"\n{len(errors)} ERRORS:")
        for error in errors[:40]:
            print(f"  {error}")
        if len(errors) > 40:
            print(f"  … and {len(errors) - 40} more")
        return 1
    print("\n0 errors")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
