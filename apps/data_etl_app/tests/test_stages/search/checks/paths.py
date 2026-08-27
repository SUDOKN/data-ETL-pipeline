"""Shared locations for the search-stage eval harness. Everything is derived
from this file's own position so the harness works from any cwd."""

from __future__ import annotations

import hashlib
from pathlib import Path

CHECKS_DIR = Path(__file__).resolve().parent
SEARCH_EVAL_DIR = CHECKS_DIR.parent
REPO_ROOT = SEARCH_EVAL_DIR.parents[4]

CONFIG_DIR = SEARCH_EVAL_DIR / "config"
FIELDS_CONFIG_DIR = CONFIG_DIR / "fields"
EXPECTATIONS_DIR = SEARCH_EVAL_DIR / "expectations"
HISTORY_DIR = SEARCH_EVAL_DIR / "history"
RUNS_DIR = HISTORY_DIR / "runs"
METRICS_JSONL = HISTORY_DIR / "metrics.jsonl"

DUMP_ROOT = REPO_ROOT / "packages" / "logs" / "extraction_dumps"
SAMPLE_TEXTS_DIR = (
    REPO_ROOT
    / "apps/data_etl_app/tests/test_stages/sample_scraped_texts"
)

# The six fields with search output. contract_products SHARES products'
# physical search requests (its node reuses the products request identity),
# so it is deliberately absent: evaluating it would double-count the stage.
SEARCH_FIELDS = (
    "industries",
    "process_caps",
    "material_caps",
    "equipments",
    "conformity_attestations",
    "products",
)


def subject_slug(subject_unique_id: str) -> str:
    """Dump filename convention: dots become underscores."""
    return subject_unique_id.replace(".", "_")


def run_dir(run_id: str) -> Path:
    return RUNS_DIR / run_id


def fingerprint_hash(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
