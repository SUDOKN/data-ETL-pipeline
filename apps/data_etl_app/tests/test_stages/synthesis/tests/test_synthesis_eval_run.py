"""Integration half of the synthesis evaluation: structural invariants over a
real run's dumps.

Runs only under ``-m integration`` (dumps are local artifacts, not fixtures):

  SYNTH_EVAL_RUN=20260825T194457 .venv/bin/python -m pytest \
      apps/data_etl_app/tests/test_stages/synthesis -m integration -q

Without SYNTH_EVAL_RUN the newest run on disk is used. Metrics, enumerators,
scorecards and agent judgment are the run_eval CLI's job (see README.md);
these tests fail only on facts that must never be false.
"""

from __future__ import annotations

import os

import pytest

from checks import loading, mechanical

pytestmark = pytest.mark.integration


def _run_id() -> str | None:
    return os.environ.get("SYNTH_EVAL_RUN") or loading.latest_run()


def _params() -> list:
    run_id = _run_id()
    if not run_id:
        return []
    return [
        pytest.param(run_id, subject, field_name, id=f"{run_id}-{subject}-{field_name}")
        for (subject, field_name) in sorted(loading.dump_files(run_id))
    ]


@pytest.mark.parametrize("run_id,subject,field_name", _params())
def test_structural_invariants(run_id: str, subject: str, field_name: str) -> None:
    dump = loading.load_dump(loading.dump_files(run_id)[(subject, field_name)])
    records = list(loading.iter_records(subject, field_name, dump))
    if field_name == loading.SHARED_DUPLICATE:
        products_path = loading.dump_files(run_id).get((subject, "products"))
        assert products_path is not None, "contract_products without products dump"
        products_records = list(
            loading.iter_records(
                subject, "products", loading.load_dump(products_path)
            )
        )
        result = mechanical.contract_products_invariant(products_records, records)
        assert result["status"] == "pass", result["detail"]
        return
    failures = [
        inv
        for inv in mechanical.run_invariants(subject, field_name, dump, records)
        if inv["status"] == "FAIL"
    ]
    assert not failures, failures


def test_run_exists() -> None:
    assert _run_id(), "no extraction dumps on disk and SYNTH_EVAL_RUN unset"
