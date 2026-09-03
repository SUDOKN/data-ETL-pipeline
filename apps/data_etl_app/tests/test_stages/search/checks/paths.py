"""Shared locations for the search-stage eval harness. Everything is derived
from this file's own position so the harness works from any cwd."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

CHECKS_DIR = Path(__file__).resolve().parent
SEARCH_EVAL_DIR = CHECKS_DIR.parent
REPO_ROOT = SEARCH_EVAL_DIR.parents[4]

CONFIG_DIR = SEARCH_EVAL_DIR / "config"
FIELDS_CONFIG_DIR = CONFIG_DIR / "fields"
HISTORY_DIR = SEARCH_EVAL_DIR / "history"
RUNS_DIR = HISTORY_DIR / "runs"
METRICS_JSONL = HISTORY_DIR / "metrics.jsonl"

DUMP_ROOT = REPO_ROOT / "packages" / "logs" / "extraction_dumps"

# ---------------------------------------------------------------- the corpus
# The scraper switched from innerText to Markdown on 2026-08-28/29, and the
# re-crawl that followed is a DIFFERENT SAMPLE of the same 18 sites, not the
# same pages re-rendered: lucasmilhaupt lost its whole /EN/ product tree,
# blackadvtech dropped /capabilities/* for blog articles, anchor-mfg moved to a
# new CMS. So the ground truth cannot be shared between the two renderings, and
# each corpus carries its own expectation set.
#
# BOTH ARE KEPT (user decision 2026-08-29). Every number already in
# history/metrics.jsonl was computed against the legacy set, and deleting that
# set would make those runs uninterpretable rather than merely superseded.
#
# Select with SEARCH_EVAL_CORPUS=legacy; `markdown` is the live default.
CORPORA = {
    "markdown": ("sample_scraped_markdowns", "expectations_markdown"),
    "legacy": ("sample_scraped_texts", "expectations"),
}
CORPUS = os.environ.get("SEARCH_EVAL_CORPUS", "markdown").strip().lower()
if CORPUS not in CORPORA:
    raise SystemExit(
        f"SEARCH_EVAL_CORPUS={CORPUS!r} is not one of {sorted(CORPORA)}"
    )

_TEXTS_SUBDIR, _EXPECTATIONS_SUBDIR = CORPORA[CORPUS]
STAGES_DIR = REPO_ROOT / "apps/data_etl_app/tests/test_stages"

SAMPLE_TEXTS_REL = f"apps/data_etl_app/tests/test_stages/{_TEXTS_SUBDIR}"
SAMPLE_TEXTS_DIR = REPO_ROOT / SAMPLE_TEXTS_REL
EXPECTATIONS_DIR = SEARCH_EVAL_DIR / _EXPECTATIONS_SUBDIR

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


def text_path_for(slug: str, texts_dir: Path | None = None) -> Path:
    """`tanfel_com` -> the .txt whose name matches once dots are restored."""
    directory = texts_dir or SAMPLE_TEXTS_DIR
    for candidate in sorted(directory.glob("*.txt")):
        if candidate.stem.replace(".", "_") == slug:
            return candidate
    raise SystemExit(f"no scraped text found for slug {slug!r} in {directory}")
