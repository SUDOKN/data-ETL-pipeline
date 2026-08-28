"""Generate the occurrence half of the golden corpus.

    python checks/seed_goldens.py                 # every subject, every field
    python checks/seed_goldens.py --subject steelcraft.com
    python checks/seed_goldens.py --limit-chars 400000    # estimate mode

The anchor set is the search harness's CONFIRMED entities: their
`acceptable_forms` are precisely the strings this stage is handed, so grading
"given the right forms, does the stage collect them correctly" needs no
invention. Adversarial forms — junk, polysemes, substring traps — live
separately in `goldens/_adversarial/` and are merged in by the same loader.

Written labels are `candidate`. A reader's second pass promotes them, per
GOLDEN_LABELS_SCHEMA.md; nothing gates until then.
"""

from __future__ import annotations

import argparse
import sys
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

SEARCH_EXPECTATIONS = paths.TEST_STAGES_ROOT / "search" / "expectations"


def confirmed_forms(slug: str, field: str) -> list[str]:
    """Every acceptable form of every confirmed entity of one (subject, field).

    Only `confirmed` is taken: a candidate entity has not had its second pass,
    and seeding this corpus from unverified upstream labels would inherit their
    uncertainty without recording it.
    """
    path = SEARCH_EXPECTATIONS / slug / f"{field}.yaml"
    if not path.is_file():
        return []
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    forms: set[str] = set()
    for entry in payload.get("entries") or []:
        if entry.get("status") != "confirmed":
            continue
        for form in entry.get("acceptable_forms") or [entry.get("name")]:
            if form:
                forms.add(str(form))
    return sorted(forms)


def seed_field(
    subject: str,
    field: str,
    *,
    text: str,
    limit_chars: Optional[int] = None,
) -> Optional[dict[str, Any]]:
    slug = paths.subject_slug(subject)
    forms = confirmed_forms(slug, field)
    labels = (
        goldens.generate_occurrence_labels(
            subject, field, forms, text=text, limit_chars=limit_chars
        )
        if forms
        else []
    )

    snapshot_path = paths.text_path(subject)
    document = {
        "subject": subject,
        "field": field,
        "snapshot": {
            "file": str(snapshot_path.relative_to(paths.REPO_ROOT)),
            "sha256": paths.sha256_of(snapshot_path),
        },
        "golden_set_version": 1,
        "anchored_on": f"search/expectations/{slug}/{field}.yaml",
        # An empty list is REQUIRED, never omitted: an absent file cannot be
        # told apart from an unlabelled one, and "this field is genuinely empty"
        # is a real, valuable answer on several corpus subjects.
        "labels": labels,
    }
    out_path = paths.goldens_dir(subject) / f"{field}.yaml"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        yaml.safe_dump(document, sort_keys=False, allow_unicode=True, width=100),
        encoding="utf-8",
    )
    occurring = sum(1 for label in labels if not label["expect"].get("must_not_occur"))
    return {
        "subject": subject,
        "field": field,
        "forms": len(forms),
        "labels": len(labels),
        "with_occurrences": occurring,
        "zero_occurrence": len(labels) - occurring,
        "traps": sum(
            1 for label in labels if label["expect"].get("naive_substring_hits")
        ),
        "occurrences": sum(
            int(label["expect"].get("occurrences", 0)) for label in labels
        ),
    }


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--subject", action="append", default=None)
    parser.add_argument("--field", action="append", default=None)
    parser.add_argument("--limit-chars", type=int, default=None,
                        help="truncate each text (estimate mode only — a "
                             "truncated corpus must never be promoted)")
    args = parser.parse_args(argv)

    if args.subject:
        subjects = list(args.subject)
    else:
        subjects = sorted(
            path.name[: -len(".txt")]
            for path in paths.TEXT_ROOT.glob("*.txt")
        )
    fields = args.field or list(paths.PHRASE_FIELDS)

    grand_labels = grand_occurrences = grand_traps = 0
    for subject in subjects:
        text = loading.load_text(subject)
        if text is None:
            print(f"{subject:32s} SKIPPED — no pinned snapshot")
            continue
        for field in fields:
            result = seed_field(
                subject, field, text=text, limit_chars=args.limit_chars
            )
            if result is None:
                continue
            grand_labels += result["labels"]
            grand_occurrences += result["occurrences"]
            grand_traps += result["traps"]
            print(
                f"{subject:30s} {field:24s} forms={result['forms']:5d} "
                f"labels={result['labels']:5d} occ={result['occurrences']:6d} "
                f"zero={result['zero_occurrence']:4d} traps={result['traps']:3d}",
                flush=True,
            )
    print(
        f"\nTOTAL labels={grand_labels} occurrences={grand_occurrences} "
        f"substring_traps={grand_traps}"
    )
    if args.limit_chars:
        print("ESTIMATE MODE — texts were truncated; do not promote these labels.")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
