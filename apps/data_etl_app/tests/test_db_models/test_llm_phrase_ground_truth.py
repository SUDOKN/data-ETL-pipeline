"""The document's self-proving contract.

Every claim the unique index relies on is validated at construction: the
digest caches the identity, the identity projects the metadata, the field
family matches the metadata family, and chunk keys follow the stats
convention. A document that constructs is a document the index can trust.
"""

from datetime import datetime, timezone
from typing import Any

import pytest
from pydantic import ValidationError

from llm_providers.models.llm_model import GPT_4o_mini
from llm_providers.models.open_ai.gpt_model_params import GPTModelParams

from core.models.chunking_strat import ChunkingStrategy
from core.models.extraction_results.concept_extraction_results import (
    BatchedInitialGroundingNodeMetadata,
    ConceptExtractionMetadata,
)
from core.models.extraction_results.keyword_extraction_results import (
    BatchedFreehandGroundingNodeMetadata,
    KeywordExtractionMetadata,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    BatchedRelationshipNodeMetadata,
    BatchedScreeningNodeMetadata,
    ExtractionNodeMetadata,
    RecursiveSearchNodeMetadata,
)
from core.models.ground_truth.audits import AuditVerdict, TextFieldAudit
from core.models.ground_truth.rule_tree import AuditedRule, AuditedSection
from core.models.ground_truth.run_identity import ExplicitRunIdentity
from core.models.ground_truth.stage_blocks import (
    ChunkGT,
    ExtractedPhraseGT,
    GroundingDerivation,
    HumanScreeningDerivation,
    MissedPhraseEntry,
    RelationshipGT,
)
from data_etl_app.db_models.llm_phrase_ground_truth import (
    LLM_PHRASE_GT_UNIQUE_INDEX_KEYS,
    LLM_PHRASE_GT_UNIQUE_INDEX_NAME,
    LLMPhraseGroundTruth,
)
from data_etl_app.models.types_and_enums import ConceptTypeEnum, KeywordTypeEnum

_COMMON: dict[str, Any] = dict(
    llm_model=GPT_4o_mini,
    model_params=GPTModelParams.with_defaults(),
    prompt_name="some_prompt",
    prompt_version_id="s3-version-1",
    created_at=datetime(2026, 8, 15),
)

_BASE: dict[str, Any] = dict(
    created_at=datetime(2026, 8, 15),
    chunk_strat=ChunkingStrategy(overlap=0.1, max_chunks=5, max_tokens_per_chunk=1000),
    ontology_version_id="onto-v7",
    llm_phrase_search=ExtractionNodeMetadata(**_COMMON),
    llm_phrase_recursive_search=RecursiveSearchNodeMetadata(**_COMMON, max_rounds=3),
    llm_phrase_relationship=BatchedRelationshipNodeMetadata(
        **_COMMON, max_phrases_per_request=50
    ),
    llm_phrase_relationship_screening=BatchedScreeningNodeMetadata(
        **_COMMON, max_pairs_per_request=15
    ),
)

_KEYWORD_METADATA = KeywordExtractionMetadata(
    **_BASE,
    llm_phrase_freehand_grounding=BatchedFreehandGroundingNodeMetadata(
        **_COMMON, max_pairs_per_request=50
    ),
)

_CONCEPT_METADATA = ConceptExtractionMetadata(
    **_BASE,
    llm_phrase_initial_grounding=BatchedInitialGroundingNodeMetadata(
        **_COMMON, max_pairs_per_request=15
    ),
    llm_phrase_recursive_grounding=ExtractionNodeMetadata(**_COMMON),
)


def _sections() -> list[AuditedSection]:
    return [
        AuditedSection(
            section_id="conditions",
            combinator="all",
            applied_rules=[
                AuditedRule(
                    rule_id="SCR-1",
                    kind="condition",
                    outcome="satisfied",
                    explanation="named as something they make",
                    reported=False,
                )
            ],
        )
    ]


def _chunk() -> ChunkGT:
    return ChunkGT(
        extracted_phrases={
            "wire EDM": ExtractedPhraseGT(
                search_round=1,
                llm_relationship=RelationshipGT(
                    llm_result="a process they perform in-house",
                    audits=[
                        TextFieldAudit(
                            type=AuditVerdict.AGREE,
                            author_email="annotator@example.com",
                            at=datetime(2026, 8, 15, tzinfo=timezone.utc),
                            source="api",
                        )
                    ],
                ),
            )
        },
        missed_phrases=[
            MissedPhraseEntry(
                phrase="laser cutting",
                author_email="annotator@example.com",
                at=datetime(2026, 8, 15, tzinfo=timezone.utc),
                source="api",
                relationship_text="a service they advertise",
                screening=HumanScreeningDerivation(
                    identified_entity="laser cutting", sections=_sections()
                ),
                groundings=[
                    GroundingDerivation(tag="Laser Cutting", sections=_sections())
                ],
            )
        ],
    )


def _document(**overrides) -> LLMPhraseGroundTruth:
    metadata = overrides.pop("metadata", _KEYWORD_METADATA)
    identity = overrides.pop(
        "run_identity", ExplicitRunIdentity.from_metadata(metadata)
    )
    fields: dict[str, Any] = dict(
        mfg_etld1="steelcraft.com",
        field_type=KeywordTypeEnum.equipments,
        scraped_text_file_version_id="text-v3",
        scraped_text_sha256="a" * 64,
        scraped_text_char_len=52_000,
        run_identity=identity,
        identity_digest=identity.canonical_digest(),
        metadata=metadata,
        chunks={"0:1000": _chunk()},
    )
    fields.update(overrides)
    return LLMPhraseGroundTruth(**fields)


def test_document_round_trips():
    doc = _document()
    dumped = doc.model_dump(mode="json")
    assert LLMPhraseGroundTruth.model_validate(dumped).model_dump(mode="json") == dumped


def test_concept_shaped_document_is_valid():
    doc = _document(
        metadata=_CONCEPT_METADATA, field_type=ConceptTypeEnum.process_caps
    )
    assert doc.run_identity.llm_phrase_initial_grounding is not None


def test_wrong_digest_is_rejected():
    with pytest.raises(ValidationError, match="validated cache"):
        _document(identity_digest="0" * 64)


def test_identity_that_disagrees_with_the_metadata_copy_is_rejected():
    tampered = ExplicitRunIdentity.from_metadata(_KEYWORD_METADATA).model_copy(
        update={"ontology_version_id": "onto-v8"}
    )
    with pytest.raises(ValidationError, match="projection of the embedded copy"):
        _document(run_identity=tampered, identity_digest=tampered.canonical_digest())


def test_field_family_must_match_metadata_family():
    with pytest.raises(ValidationError, match="different field families"):
        _document(field_type=ConceptTypeEnum.industries)


def test_non_bounds_chunk_key_is_rejected():
    with pytest.raises(ValidationError, match="'start:end' bounds key"):
        _document(chunks={"chunk-1": _chunk()})


def test_unique_index_keys_are_the_identity_key():
    """The index IS the identity contract (PH-11): subject + field + text
    version + the identity digest, nothing less."""
    assert LLM_PHRASE_GT_UNIQUE_INDEX_KEYS == [
        ("mfg_etld1", 1),
        ("field_type", 1),
        ("scraped_text_file_version_id", 1),
        ("identity_digest", 1),
    ]
    assert LLM_PHRASE_GT_UNIQUE_INDEX_NAME == "llm_phrase_gt_unique_idx"


def test_registered_in_document_models():
    from data_etl_app.db_models import APP_DOCUMENT_MODELS

    assert LLMPhraseGroundTruth in APP_DOCUMENT_MODELS
