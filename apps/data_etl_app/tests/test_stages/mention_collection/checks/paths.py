"""Locations, field names and slugs for the mention-stage evaluation.

Everything is derived from this file's own position, so the instrument works
from a fresh clone with no configuration and no environment variables.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Optional

EVAL_ROOT = Path(__file__).resolve().parent.parent
TEST_STAGES_ROOT = EVAL_ROOT.parent
REPO_ROOT = TEST_STAGES_ROOT.parents[3]

GOLDENS_ROOT = EVAL_ROOT / "goldens"
ADVERSARIAL_ROOT = GOLDENS_ROOT / "_adversarial"
CONFIG_ROOT = EVAL_ROOT / "config"
HISTORY_ROOT = EVAL_ROOT / "history"
RUNS_ROOT = HISTORY_ROOT / "runs"
JUDGMENTS_ROOT = HISTORY_ROOT / "judgments"
METRICS_FILE = HISTORY_ROOT / "metrics.jsonl"
TAXONOMY_FILE = EVAL_ROOT / "TAXONOMY.md"

# The sha256-pinned text snapshots every golden label is anchored to. This is
# the evaluation copy under test_stages/, NOT knowledge/sample_scraped_texts/ —
# the two have drifted and only this one is pinned by the eval sets.
TEXT_ROOT = TEST_STAGES_ROOT / "sample_scraped_texts"

DUMP_ROOT = REPO_ROOT / "packages" / "logs" / "extraction_dumps"

# The seven phrase fields, from data_etl_app.models.types_and_enums. Named here
# rather than imported so the harness never drags the pipeline packages (and
# their Beanie/Mongo import chain) into a plain pytest run.
KEYWORD_FIELDS = ("products", "contract_products", "equipments")
CONCEPT_FIELDS = (
    "industries",
    "conformity_attestations",
    "material_caps",
    "process_caps",
)
PHRASE_FIELDS = tuple(sorted(KEYWORD_FIELDS + CONCEPT_FIELDS))

# contract_products is a byte-copy of products through this stage: the contract
# node mints the products custom_id, so both fields share one physical request,
# one fold and one set of mentions. Judging or summing both double-counts by
# ~32%. It is still LOADED (the byte-identity is an invariant worth checking)
# and never judged.
SHARED_DUPLICATE = "contract_products"
SHARED_SOURCE = "products"

JUDGED_FIELDS = tuple(f for f in PHRASE_FIELDS if f != SHARED_DUPLICATE)

# Verb-fold applies to these two fields' grouping only (ExtractionPipelineFactory
# .VERB_FOLD_FIELDS); it changes which forms share a group, so a grouping label
# is only comparable within the same setting.
VERB_FOLD_FIELDS = frozenset({"material_caps", "process_caps"})

# The stage's token in a request custom_id.
STAGE_REQUEST_TOKEN = "llm_phrase_mention_collection"
RETRY_REQUEST_TOKEN = "llm_phrase_mention_collection_retry"

# What the fold writes when the model did not describe a snippet.
DEFAULT_LOCATION = "(location not described)"
LOCATION_SOURCE_LLM = "llm"
LOCATION_SOURCE_NONE = "none"

# Bundle statuses the fold can emit. `collapsed` arrived 2026-08-27 (D21) and is
# NOT yet known to the grounding harness's KNOWN_STATUSES — see BOUNDARY.md.
BUNDLE_STATUSES = ("ok", "no_mentions", "collapsed")


def subject_slug(subject: str) -> str:
    """`steelcraft.com` -> `steelcraft_com`, matching the sibling eval sets.

    Only dots become underscores: hyphens are kept, because the existing
    directories are `anchor-mfg_com`, not `anchor_mfg_com`.
    """
    return subject.replace(".", "_")


def subject_of_slug(slug: str) -> str:
    """Inverse of :func:`subject_slug` for the domains in this corpus.

    Only the LAST underscore is a dot — every corpus domain is `name.tld`, and
    `anchor-mfg_com` must not become `anchor.mfg.com`.
    """
    head, _, tld = slug.rpartition("_")
    return f"{head}.{tld}" if head else slug


def text_path(subject: str) -> Path:
    return TEXT_ROOT / f"{subject}.txt"


def goldens_dir(subject: str) -> Path:
    return GOLDENS_ROOT / subject_slug(subject)


def run_dir(run_id: str) -> Path:
    return RUNS_ROOT / run_id


def dump_dir(run_id: str) -> Path:
    return DUMP_ROOT / run_id


def sha256_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def taxonomy_version() -> str:
    """Hash of the judgment contract, stamped into every verdict.

    Editing TAXONOMY.md invalidates every cached verdict. That is the intent:
    a verdict recorded under a different contract is not the same verdict.
    """
    if not TAXONOMY_FILE.is_file():
        return "no-taxonomy"
    return hashlib.sha256(TAXONOMY_FILE.read_bytes()).hexdigest()[:12]


def digest(payload: Any) -> str:
    """Stable short digest of any JSON-serializable payload."""
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


def parse_custom_id(custom_id: str) -> Optional[dict[str, str]]:
    """Address half of a mention-collection custom id, or None if it is not one.

    Shape (``>`` separates the address, ``|`` the settings)::

        <subject> > <field> > llm_phrase_mention_collection > chunk > <bounds>
        > sub > <bounds> [> retry > <n>] > group > <i> > <model> | ... | pv=... | ud=...

    Returns None for every other stage's ids, so a caller can filter a mixed
    request list by calling this alone.
    """
    address, _, settings = custom_id.partition(">gpt-")
    parts = [p.strip() for p in custom_id.split(">")]
    if STAGE_REQUEST_TOKEN not in parts:
        return None
    out: dict[str, str] = {"custom_id": custom_id}
    out["subject"] = parts[0] if parts else ""
    out["field"] = parts[1] if len(parts) > 1 else ""
    for key in ("chunk", "sub", "group", "retry"):
        if key in parts:
            i = parts.index(key)
            if i + 1 < len(parts):
                out[key] = parts[i + 1]
    for token in custom_id.split("|")[1:]:
        name, _, value = token.partition("=")
        name = name.strip()
        if name in ("pv", "ud", "gs", "rad"):
            out[name] = value.strip()
    # `address` is only used to prove the split point exists; the model segment
    # itself is already captured by the settings loop above.
    del address, settings
    return out


def iter_golden_files(subjects: Optional[Iterable[str]] = None) -> list[Path]:
    """Every per-field golden file, optionally restricted to some subjects."""
    if not GOLDENS_ROOT.is_dir():
        return []
    wanted = {subject_slug(s) for s in subjects} if subjects else None
    found: list[Path] = []
    for sub_dir in sorted(GOLDENS_ROOT.iterdir()):
        if not sub_dir.is_dir() or sub_dir.name.startswith("_"):
            continue
        if wanted is not None and sub_dir.name not in wanted:
            continue
        for field in PHRASE_FIELDS:
            path = sub_dir / f"{field}.yaml"
            if path.is_file():
                found.append(path)
    return found
