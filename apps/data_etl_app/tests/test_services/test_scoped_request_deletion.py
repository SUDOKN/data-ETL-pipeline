"""What the manufacturer-scoped delete asks the request store for.

The wrapper's whole job is resolving a (field, stage, and_downstream) scope into
the field name and stage tokens the store understands, so that is what is pinned
here — the store's own selection rules are covered in
``test_bulk_delete_scoping``.
"""

from __future__ import annotations

from typing import Any, Optional

import pytest

from core.models.pipeline_nodes.base.pipeline_stage import PipelineStage
from data_etl_app.models.types_and_enums import ConceptTypeEnum
from data_etl_app.services import gpt_batch_request_service
from data_etl_app.services.gpt_batch_request_service import (
    bulk_delete_gpt_batch_requests_by_mfg_etld1_and_field,
)

_MFG = "alecmfg.com"


@pytest.fixture
def asked(monkeypatch) -> dict[str, Any]:
    """Captures the arguments the wrapper hands to the request store."""
    captured: dict[str, Any] = {}

    async def _capture(
        subject_unique_id: str,
        field_name: Optional[str],
        stage_request_id_tokens: Optional[list[str]] = None,
    ) -> int:
        captured.update(
            subject_unique_id=subject_unique_id,
            field_name=field_name,
            stage_request_id_tokens=stage_request_id_tokens,
        )
        return 0

    monkeypatch.setattr(
        gpt_batch_request_service,
        "bulk_delete_gpt_batch_requests_by_subject_id_and_field",
        _capture,
    )
    return captured


@pytest.mark.asyncio
async def test_no_scope_narrows_nothing(asked):
    await bulk_delete_gpt_batch_requests_by_mfg_etld1_and_field(_MFG)

    assert asked == {
        "subject_unique_id": _MFG,
        "field_name": None,
        "stage_request_id_tokens": None,
    }


@pytest.mark.asyncio
async def test_a_field_alone_narrows_to_its_name(asked):
    await bulk_delete_gpt_batch_requests_by_mfg_etld1_and_field(
        _MFG, ConceptTypeEnum.material_caps
    )

    assert asked["field_name"] == "material_caps"
    assert asked["stage_request_id_tokens"] is None


@pytest.mark.asyncio
async def test_a_stage_alone_narrows_to_its_token_across_fields(asked):
    await bulk_delete_gpt_batch_requests_by_mfg_etld1_and_field(
        _MFG, stage=PipelineStage.screening
    )

    assert asked["field_name"] is None
    assert asked["stage_request_id_tokens"] == ["llm_phrase_relationship_screening"]


@pytest.mark.asyncio
async def test_field_and_stage_narrow_to_the_pair(asked):
    await bulk_delete_gpt_batch_requests_by_mfg_etld1_and_field(
        _MFG, ConceptTypeEnum.material_caps, stage=PipelineStage.phrase_search
    )

    assert asked["field_name"] == "material_caps"
    assert asked["stage_request_id_tokens"] == ["llm_search"]


@pytest.mark.asyncio
async def test_and_downstream_widens_the_stage(asked):
    await bulk_delete_gpt_batch_requests_by_mfg_etld1_and_field(
        _MFG,
        ConceptTypeEnum.material_caps,
        stage=PipelineStage.screening,
        and_downstream=True,
    )

    tokens = asked["stage_request_id_tokens"]
    assert "llm_phrase_relationship_screening" in tokens
    assert "llm_phrase_recursive_grounding" in tokens
    # The upstream is what a rewind is supposed to keep.
    assert "llm_phrase_relationship" not in tokens
    assert "llm_search" not in tokens


@pytest.mark.asyncio
async def test_and_downstream_without_a_stage_is_refused(asked):
    """It would otherwise read as "delete everything", which is what dropping
    the flag already means — an easy way to wipe a subject by typo."""
    with pytest.raises(ValueError, match="needs a stage"):
        await bulk_delete_gpt_batch_requests_by_mfg_etld1_and_field(
            _MFG, ConceptTypeEnum.material_caps, and_downstream=True
        )

    assert asked == {}
