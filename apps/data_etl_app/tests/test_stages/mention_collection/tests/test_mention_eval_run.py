"""Structural invariants over a REAL run's dumps.

Marked `integration` and deselected by default: a fresh checkout has no dumps
(they live under `packages/logs/extraction_dumps/`, which is not in git). Run it
against a specific run with

    MENTION_EVAL_RUN=20260824T020729 .venv/bin/pytest <this file> -m integration

or with no env var to take the newest run on disk.

These assertions are about the DUMP, not about the harness's logic — that is the
unit half's job. They are what catches a pipeline change that silently reshapes
the thing this instrument reads, which has now happened twice (the fold block
arriving in full-run dumps, and the `collapsed` status).
"""

from __future__ import annotations

import os

import pytest

# See conftest.py: `mention_checks`, never a bare `checks`.
# `mention_checks` is registered at runtime by conftest.py, so no static
# analyser can resolve it; the alias is what keeps this stage from
# shadowing a sibling's `checks` package.
from mention_checks import loading, mechanical, paths  # type: ignore[import-not-found]  # noqa: E402

pytestmark = pytest.mark.integration

RUN_ID = os.environ.get("MENTION_EVAL_RUN") or loading.latest_run()


@pytest.fixture(scope="module")
def runs() -> list[loading.FieldRun]:
    if not RUN_ID:
        pytest.skip("no extraction dumps on disk")
    return loading.load_run(RUN_ID)


def test_run_exists(runs):
    assert runs, f"run {RUN_ID} holds no phrase-field dumps"


def test_every_field_dump_parses(runs):
    for run in runs:
        assert run.subject, f"{run.path} has no subject"
        assert run.field in paths.PHRASE_FIELDS


def test_fold_bearing_fields_have_mentions(runs):
    """A fold block with zero mentions across every field means the loader is
    reading the wrong shape, not that the run found nothing."""
    with_fold = [r for r in runs if r.has_fold]
    if not with_fold:
        pytest.skip(f"run {RUN_ID} predates the fold block in dumps")
    assert any(list(r.mentions()) for r in with_fold)


def test_mention_ids_are_snippet_hashes(runs):
    for run in runs:
        by_id: dict[str, str] = {}
        for mention in run.mentions():
            known = by_id.setdefault(mention.mention_id, mention.snippet)
            assert known == mention.snippet, (
                f"{run.key}: mention id {mention.mention_id} covers two snippets"
            )


def test_every_form_occurs_in_its_own_snippet(runs):
    """The `Steel`-inside-`Steelcraft` class, over real data.

    Uses the shared whitespace-tolerant matcher: this corpus's non-breaking
    spaces would otherwise make faithful snippets look defective.
    """
    for run in runs:
        report = mechanical.FieldReport(
            key=run.key, subject=run.subject, field=run.field,
            judged=run.judged, has_fold=run.has_fold,
        )
        if not run.has_fold:
            continue
        mechanical.check_span_integrity(report, run)
        assert "span.form_in_own_snippet" not in report.reds, (
            f"{run.key}: "
            + next(
                f.detail + " " + "; ".join(f.examples)
                for f in report.findings
                if f.check == "span.form_in_own_snippet"
            )
        )


def test_group_statuses_are_all_known(runs):
    """Fails the moment the pipeline adds a bundle status this instrument does
    not know — which is exactly how `collapsed` (D21) arrived on 2026-08-27."""
    for run in runs:
        for group in run.groups:
            assert group.status in paths.BUNDLE_STATUSES, (
                f"{run.key}: unknown status {group.status!r} — the fold gained a "
                f"status; update paths.BUNDLE_STATUSES and tell the sibling evals "
                f"(see BOUNDARY.md)"
            )


def test_products_and_contract_products_share_one_fold(runs):
    by_subject: dict[str, dict[str, loading.FieldRun]] = {}
    for run in runs:
        by_subject.setdefault(run.subject, {})[run.field] = run
    checked = 0
    for subject, fields in by_subject.items():
        left, right = fields.get("products"), fields.get("contract_products")
        if not left or not right or not (left.has_fold and right.has_fold):
            continue
        checked += 1
        assert [(m.mention_id, m.span) for m in left.mentions()] == [
            (m.mention_id, m.span) for m in right.mentions()
        ], f"{subject}: contract_products diverged from products"
    if not checked:
        pytest.skip("no subject in this run carries both product fields")


def test_the_whole_deterministic_pass_runs(runs):
    reports = mechanical.evaluate_run(runs)
    assert set(reports) == {r.key for r in runs}
    for report in reports.values():
        assert report.status in {"OK", "WARN", "RED", "BLIND"}
