"""Carry the legacy (innerText) expectation set onto the markdown_v2 texts.

WHY THIS EXISTS. The scraper switched from innerText to Markdown on
2026-08-28/29 and the sites were re-crawled. Re-deriving 9,075 entries from
scratch would have cost a large agent fan-out AND risked a WORSE corpus than
the one already two-pass verified: the VERIFICATION_LEDGER records that the
verification pass on agstech alone recovered 512 entities its seeding agents
had dropped. So the entries are ported MECHANICALLY, and agents are spent only
where the text actually changed.

Measured before writing this (2026-08-29): 8,281 of 9,075 live entries still
have an evidence quote that occurs VERBATIM in the new text -- 91.3%. The
damage is concentrated, not spread:

    100%  agstech, fzemanufacturing, mathewsco, pradeepmetals, superiortech,
          acimachine, 101machine, taylordunn, med-tekinc, alecmfg
    91-99% howcogroup, ableengineering, steelcraft, tanfel, decimal
    41.8% blackadvtech   (dropped /capabilities/* and /about-us/* for blog articles)
    39.5% anchor-mfg     (new CMS, ?lang=en URLs, prose rewritten)
     7.0% lucasmilhaupt  (lost its whole /EN/Industries and /EN/Products tree)
     --   austinelectricservices, sterlingmfg: dead domains, not re-crawled

WHAT PORTING DOES AND DOES NOT DECIDE. An entry is carried over only when it
still passes the two checks `validate_expectations.py` would ERROR on:

    (3) at least one evidence quote occurs verbatim in the new text, and
    (4) at least one acceptable_form is covered by one of those surviving quotes.

An entry failing either is NOT written and NOT retired -- it goes to a repair
worklist for an agent to re-anchor against the new text or retire with a
reason. Silently retiring it here would destroy a verified judgment on the
strength of a string match, which is exactly the kind of unexamined machine
verdict the ledger exists to prevent.

RETIRED entries are carried over untouched. They are a record of a judgment
("retire, never delete") and the validator exempts them from the quote
contract, so their legacy evidence does no harm.

Usage:
    .venv/bin/python checks/port_corpus.py --out-dir <worklist dir>
    .venv/bin/python checks/port_corpus.py --subject lucasmilhaupt.com --dry-run
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import date as date_cls
from pathlib import Path
from typing import Any

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from _shared.text_matching import (  # noqa: E402
    flexible_pattern,
    normalize_spaces,
)
from paths import (  # noqa: E402
    CORPORA,
    REPO_ROOT,
    SEARCH_EVAL_DIR,
    SEARCH_FIELDS,
    STAGES_DIR,
)

PORTED_BY = "port_corpus.py"
PORT_SOURCE = "markdown-cutover"

LEGACY_TEXTS_SUBDIR, LEGACY_EXPECTATIONS_SUBDIR = CORPORA["legacy"]
MARKDOWN_TEXTS_SUBDIR, MARKDOWN_EXPECTATIONS_SUBDIR = CORPORA["markdown"]

LEGACY_TEXTS_DIR = STAGES_DIR / LEGACY_TEXTS_SUBDIR
MARKDOWN_TEXTS_DIR = STAGES_DIR / MARKDOWN_TEXTS_SUBDIR
LEGACY_EXPECTATIONS_DIR = SEARCH_EVAL_DIR / LEGACY_EXPECTATIONS_SUBDIR
MARKDOWN_EXPECTATIONS_DIR = SEARCH_EVAL_DIR / MARKDOWN_EXPECTATIONS_SUBDIR
MARKDOWN_TEXTS_REL = f"apps/data_etl_app/tests/test_stages/{MARKDOWN_TEXTS_SUBDIR}"

PAGE_BARRIER = "#" * 50
# Key order that reads well in review, matching build_expectations.py.
ENTRY_KEY_ORDER = (
    "id", "name", "status", "acceptable_forms", "evidence",
    "actor", "notes", "provenance",
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _occurs(form: str, normalized_text: str, *, case_sensitive: bool = True) -> bool:
    """`occurs_in` against text that is ALREADY normalized.

    Mirrors `validate_expectations._occurs` exactly, including its short-form
    rule, so an entry this script keeps cannot fail the validator afterwards.
    """
    if len(form.strip()) <= 3:
        case_sensitive = True
    pattern = flexible_pattern(form, case_sensitive=case_sensitive)
    return bool(pattern and pattern.search(normalized_text))


def _offset_of(quote: str, normalized_text: str) -> int | None:
    pattern = flexible_pattern(quote, case_sensitive=True)
    if pattern is None:
        return None
    match = pattern.search(normalized_text)
    return match.start() if match else None


def _page_urls(text: str) -> list[tuple[int, str]]:
    """(line number, url) for every page block in a scraped text file."""
    pages: list[tuple[int, str]] = []
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if line.rstrip() == PAGE_BARRIER and index + 1 < len(lines):
            url = lines[index + 1].strip()
            if url.startswith("http"):
                pages.append((index + 2, url))
    return pages


def _canonical_url(url: str) -> str:
    """Compare pages across crawls without punishing cosmetic URL drift."""
    url = re.sub(r"^https?://", "", url.strip())
    url = re.sub(r"^www\.", "", url)
    url = url.split("#", 1)[0]
    url = re.sub(r"[?&]lang=[a-z-]+", "", url)
    return url.rstrip("/").casefold()


def _ordered_entry(entry: dict[str, Any]) -> dict[str, Any]:
    rest = dict(entry)
    ordered = {k: rest.pop(k) for k in ENTRY_KEY_ORDER if k in rest}
    ordered.update(rest)
    return ordered


def port_subject(
    slug: str,
    *,
    today: str,
    dry_run: bool,
) -> dict[str, Any]:
    """Port one subject. Returns a report dict; writes YAML unless dry_run."""
    legacy_dir = LEGACY_EXPECTATIONS_DIR / slug
    report: dict[str, Any] = {
        "slug": slug,
        "status": "ported",
        "kept": 0,
        "repair": 0,
        "retired_carried": 0,
        "quotes_dropped": 0,
        "fields": {},
        "repair_rows": [],
        "new_only_pages": [],
        "gone_pages": [],
        "notes": [],
    }

    subject_yaml = legacy_dir / "subject.yaml"
    if not subject_yaml.is_file():
        report["status"] = "skipped: no subject.yaml"
        return report
    subject_payload = yaml.safe_load(subject_yaml.read_text()) or {}
    subject_name = subject_payload.get("subject")
    if not subject_name:
        report["status"] = "skipped: subject.yaml has no `subject:` key"
        return report

    matches = [
        p for p in sorted(MARKDOWN_TEXTS_DIR.glob("*.txt"))
        if p.stem.replace(".", "_") == slug
    ]
    if not matches:
        report["status"] = "retired: no markdown text (domain not in the new crawl)"
        return report
    text_path = matches[0]

    raw = text_path.read_text(errors="replace")
    normalized = normalize_spaces(raw)
    sha = _sha256(text_path)

    # Page-level diff drives the top-up worklist: pages the new crawl has and
    # the legacy corpus never saw are where genuinely new entities live.
    new_pages = _page_urls(raw)
    legacy_matches = [
        p for p in sorted(LEGACY_TEXTS_DIR.glob("*.txt"))
        if p.stem.replace(".", "_") == slug
    ]
    if legacy_matches:
        legacy_pages = _page_urls(legacy_matches[0].read_text(errors="replace"))
        legacy_keys = {_canonical_url(u) for _, u in legacy_pages}
        new_keys = {_canonical_url(u) for _, u in new_pages}
        report["new_only_pages"] = [
            {"line": line, "url": url}
            for line, url in new_pages
            if _canonical_url(url) not in legacy_keys
        ]
        report["gone_pages"] = sorted(
            url for _, url in legacy_pages if _canonical_url(url) not in new_keys
        )

    out_dir = MARKDOWN_EXPECTATIONS_DIR / slug
    documents: dict[Path, dict[str, Any]] = {}

    for field_name in SEARCH_FIELDS:
        path = legacy_dir / f"{field_name}.yaml"
        if not path.is_file():
            report["notes"].append(f"{field_name}: missing in the legacy set")
            continue
        payload = yaml.safe_load(path.read_text()) or {}

        kept: list[dict[str, Any]] = []
        field_repair = 0
        for entry in payload.get("entries") or []:
            if entry.get("status") == "retired":
                kept.append(_ordered_entry(dict(entry)))
                report["retired_carried"] += 1
                continue

            forms = [str(f) for f in (entry.get("acceptable_forms") or [])]
            survivors: list[dict[str, Any]] = []
            covered = False
            for evidence in entry.get("evidence") or []:
                quote = str(evidence.get("quote", ""))
                if not quote.strip() or not _occurs(quote, normalized, case_sensitive=True):
                    continue
                record: dict[str, Any] = {"quote": quote}
                offset = _offset_of(quote, normalized)
                if offset is not None:
                    record["approx_offset"] = offset
                survivors.append(record)
                if any(
                    _occurs(form, normalize_spaces(quote), case_sensitive=False)
                    for form in forms
                ):
                    covered = True

            dropped = len(entry.get("evidence") or []) - len(survivors)
            if not survivors or not covered:
                # Not written and not retired -- an agent decides, see docstring.
                field_repair += 1
                report["repair_rows"].append({
                    "slug": slug,
                    "subject": subject_name,
                    "field": field_name,
                    "id": entry.get("id"),
                    "name": entry.get("name"),
                    "status": entry.get("status"),
                    "acceptable_forms": forms,
                    "legacy_evidence": [
                        str(e.get("quote", "")) for e in (entry.get("evidence") or [])
                    ],
                    "actor": entry.get("actor"),
                    "notes": entry.get("notes"),
                    "reason": (
                        "no evidence quote survives" if not survivors
                        else "surviving quotes cover no acceptable_form"
                    ),
                })
                continue

            report["quotes_dropped"] += dropped
            ported = dict(entry)
            ported["evidence"] = survivors
            provenance = list(ported.get("provenance") or [])
            provenance.append({
                "action": "ported",
                "by": PORTED_BY,
                "date": today,
                "source": PORT_SOURCE,
            })
            ported["provenance"] = provenance
            kept.append(_ordered_entry(ported))
            report["kept"] += 1

        report["repair"] += field_repair
        report["fields"][field_name] = {
            "kept": len(kept),
            "repair": field_repair,
        }

        document: dict[str, Any] = {
            "subject": subject_name,
            "field": field_name,
            "snapshot": {
                "file": f"{MARKDOWN_TEXTS_REL}/{text_path.name}",
                "sha256": sha,
            },
            # A new corpus gets its own version line; the legacy version is
            # preserved in the port provenance rather than continued here.
            "eval_set_version": 1,
            "expected_empty": bool(payload.get("expected_empty", False)),
        }
        if payload.get("notes"):
            document["notes"] = payload["notes"]
        document["entries"] = kept
        if payload.get("false_friends"):
            document["false_friends"] = payload["false_friends"]
        documents[out_dir / f"{field_name}.yaml"] = document

    # subject.yaml: carry the profile and hazards, re-pin the snapshot, and
    # re-resolve any recorded page samples onto the new file's line numbers.
    subject_document = dict(subject_payload)
    subject_document["snapshot"] = {
        "file": f"{MARKDOWN_TEXTS_REL}/{text_path.name}",
        "sha256": sha,
    }
    subject_document["eval_set_version"] = 1
    sampling = subject_document.get("sampling")
    if isinstance(sampling, dict) and sampling.get("pages_read"):
        by_url = {_canonical_url(u): line for line, u in new_pages}
        resolved, lost = [], []
        for row in sampling["pages_read"]:
            url = str(row.get("url", ""))
            line = by_url.get(_canonical_url(url))
            if line is None:
                lost.append(url)
            else:
                resolved.append({"line": line, "url": url})
        sampling["pages_read"] = resolved
        if lost:
            sampling["pages_dropped_at_port"] = lost
            report["notes"].append(
                f"sampling: {len(lost)} previously-read page(s) are gone from the "
                f"new crawl; the sample needs re-drawing"
            )
    provenance = list(subject_document.get("provenance") or [])
    provenance.append({
        "action": "ported",
        "by": PORTED_BY,
        "date": today,
        "source": PORT_SOURCE,
    })
    subject_document["provenance"] = provenance
    documents[out_dir / "subject.yaml"] = subject_document

    if not dry_run:
        out_dir.mkdir(parents=True, exist_ok=True)
        for path, document in documents.items():
            path.write_text(
                yaml.safe_dump(document, sort_keys=False, allow_unicode=True, width=110)
            )

    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--subject", action="append", default=[],
                        help="domain or slug; repeatable. Default: every legacy subject.")
    parser.add_argument("--out-dir", type=Path,
                        help="where the repair/top-up worklists are written")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--date", default=date_cls.today().isoformat())
    args = parser.parse_args()

    if args.subject:
        slugs = [s.replace(".", "_") for s in args.subject]
    else:
        slugs = sorted(
            p.name for p in LEGACY_EXPECTATIONS_DIR.iterdir()
            if p.is_dir() and (p / "subject.yaml").is_file()
        )

    reports = [port_subject(s, today=args.date, dry_run=args.dry_run) for s in slugs]

    total_kept = sum(r["kept"] for r in reports)
    total_repair = sum(r["repair"] for r in reports)
    total_retired = sum(r["retired_carried"] for r in reports)

    print(f"{'subject':<28}{'kept':>7}{'repair':>8}{'retired':>9}  status")
    for r in reports:
        print(
            f"{r['slug']:<28}{r['kept']:>7}{r['repair']:>8}{r['retired_carried']:>9}"
            f"  {r['status']}"
        )
        for note in r["notes"]:
            print(f"    - {note}")
    print(f"\n{'TOTAL':<28}{total_kept:>7}{total_repair:>8}{total_retired:>9}")

    if args.out_dir:
        args.out_dir.mkdir(parents=True, exist_ok=True)
        for r in reports:
            if r["repair_rows"]:
                path = args.out_dir / f"repair_{r['slug']}.jsonl"
                path.write_text(
                    "\n".join(json.dumps(row, ensure_ascii=False) for row in r["repair_rows"])
                    + "\n"
                )
            if r["new_only_pages"]:
                path = args.out_dir / f"topup_{r['slug']}.json"
                path.write_text(json.dumps(
                    {"slug": r["slug"], "new_only_pages": r["new_only_pages"],
                     "gone_pages": r["gone_pages"]},
                    ensure_ascii=False, indent=2,
                ))
        summary = args.out_dir / "port_summary.json"
        summary.write_text(json.dumps(
            [{k: v for k, v in r.items() if k != "repair_rows"} for r in reports],
            ensure_ascii=False, indent=2,
        ))
        print(f"\nworklists -> {args.out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
