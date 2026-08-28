"""The content-keyed verdict ledger.

The user's decision is **exhaustive coverage every run**: every snippet, group
and inheritance pair is judged, never sampled. The cache is what makes that
affordable rather than what makes it partial — a verdict is keyed by the
CONTENT it was made about, so when a stage replays byte-identically (which it
does routinely, since a run reuses stored answers) the same judgement applies
and no reader is asked to read the same sentence twice.

What invalidates a verdict, by construction:

* the snippet or the location text changing (a different thing was judged),
* the prompt version changing (`pv` — a different asker),
* TAXONOMY.md changing (`taxonomy_version` — a different contract).

The third is deliberate and expensive: promoting a candidate dimension into the
taxonomy sends that field's whole population back to pending. Batch taxonomy
changes between runs, never mid-pass.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Optional

try:
    from . import paths
except ImportError:  # pragma: no cover - script/package dual import
    import paths  # type: ignore[no-redef]

ITEM_SNIPPET = "snippet"
ITEM_GROUP = "group"
ITEM_INHERITANCE = "inheritance"
ITEM_KINDS = (ITEM_SNIPPET, ITEM_GROUP, ITEM_INHERITANCE)

_UNIT_SEPARATOR = "\x1f"


def content_key(
    *,
    item: str,
    identity: str,
    judged_content: str,
    pv: Optional[str],
    taxonomy_version: str,
) -> str:
    """Stable key for one judgeable item.

    ``identity`` addresses the item (a mention id, a group id, an inner:outer
    pair); ``judged_content`` is everything a reader actually looks at, so a
    changed location or a changed member list yields a new key.
    """
    if item not in ITEM_KINDS:
        raise ValueError(f"unknown item kind {item!r}; expected one of {ITEM_KINDS}")
    payload = _UNIT_SEPARATOR.join(
        [item, identity, judged_content, pv or "-", taxonomy_version]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]


def ledger_path(subject: str, field: str) -> Path:
    return paths.JUDGMENTS_ROOT / f"{paths.subject_slug(subject)}__{field}.jsonl"


def load_ledger(subject: str, field: str) -> dict[str, dict[str, Any]]:
    """Every verdict recorded for this (subject, field), last write winning."""
    path = ledger_path(subject, field)
    if not path.is_file():
        return {}
    out: dict[str, dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            # A judge agent killed mid-write leaves a partial last line. Skip it
            # rather than discarding the whole file: the shell-append protocol
            # exists precisely so a truncated run is still usable.
            continue
        key = row.get("content_key")
        if isinstance(key, str):
            out[key] = row
    return out


def append_verdicts(
    subject: str, field: str, rows: Iterable[dict[str, Any]]
) -> tuple[int, int]:
    """Append verdicts not already present. Returns (written, skipped).

    Idempotent on ``content_key``: re-ingesting a verdict file is a no-op, so a
    resumed pass cannot double-count. Re-keying the ledger (a taxonomy change)
    must also re-key the source verdict files, or the next ingest appends the
    stale keys again.
    """
    existing = load_ledger(subject, field)
    path = ledger_path(subject, field)
    path.parent.mkdir(parents=True, exist_ok=True)
    written = skipped = 0
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            key = row.get("content_key")
            if not isinstance(key, str):
                skipped += 1
                continue
            if key in existing:
                skipped += 1
                continue
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
            existing[key] = row
            written += 1
    return written, skipped


def split_cached_pending(
    items: Iterable[tuple[str, dict[str, Any]]],
    ledger: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Partition (content_key, item) pairs into already-judged and pending."""
    cached: list[dict[str, Any]] = []
    pending: list[dict[str, Any]] = []
    for key, item in items:
        entry = dict(item)
        entry["content_key"] = key
        (cached if key in ledger else pending).append(entry)
    return cached, pending


def judged_rates(
    ledger: dict[str, dict[str, Any]], keys: Iterable[str]
) -> dict[str, dict[str, Any]]:
    """Per-dimension pass rates over the verdicts for ``keys``.

    `unclear` is reported and excluded from the denominator — it is an honest
    answer, not half a failure. The S2/S3/S4 conjunction (location correctness)
    is computed here because it is the headline number and must never be
    derived by eye from three separate rates.
    """
    wanted = set(keys)
    rows = [row for key, row in ledger.items() if key in wanted]
    out: dict[str, dict[str, Any]] = {}
    location_dims = ("S2", "S3", "S4")

    per_dim: dict[str, dict[str, int]] = {}
    location_all_pass = location_judged = 0
    for row in rows:
        checks = row.get("checks") or {}
        for dim, verdict in checks.items():
            if not isinstance(verdict, dict):
                continue
            bucket = per_dim.setdefault(dim, {"pass": 0, "fail": 0, "unclear": 0})
            value = str(verdict.get("verdict", "")).lower()
            if value in bucket:
                bucket[value] += 1
        present = [d for d in location_dims if isinstance(checks.get(d), dict)]
        if len(present) == len(location_dims):
            location_judged += 1
            if all(
                str(checks[d].get("verdict", "")).lower() == "pass"
                for d in location_dims
            ):
                location_all_pass += 1

    for dim, bucket in sorted(per_dim.items()):
        denominator = bucket["pass"] + bucket["fail"]
        out[dim] = {
            **bucket,
            "rate": round(bucket["pass"] / denominator, 4) if denominator else None,
            "n": denominator,
        }
    out["location_correctness"] = {
        "pass": location_all_pass,
        "n": location_judged,
        "rate": round(location_all_pass / location_judged, 4) if location_judged else None,
        "note": "S2 AND S3 AND S4 all pass; the headline number",
    }
    return out
