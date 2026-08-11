from __future__ import annotations

from pydantic import BaseModel
from datetime import datetime
from typing import Optional, TypeVar


from core.field_types import (
    OntologyVersionIDType,
)
from infra.field_types import (
    S3FileVersionIDType,
)
from core.models.extraction_schemas.search import LLMSearchResults
from core.models.extraction_schemas.relationship import (
    LLMPhraseRelationshipResults,
)
from core.models.extraction_schemas.screening import (
    LiveScreeningResults,
)
from llm_providers.models.llm_model import LLM_Model
from core.models.chunking_strat import ChunkingStrategy
from llm_providers.models.open_ai.gpt_model_params import (
    GPTModelParams,
)

_T = TypeVar("_T")


def partition_by_search_round(
    flat_results: dict[str, _T],
    search_rounds: dict[int, LLMSearchResults],
) -> dict[int, dict[str, _T]]:
    """Assign each phrase in *flat_results* to its earliest search round.

    Iterates *search_rounds* in ascending key order; the first round a phrase
    appears in wins. Round 0 is reserved for brute-force search survivors
    (empty for keyword-type fields). Phrases absent from all rounds (an
    anomaly, since every phrase reaching *flat_results* should already be in
    brute or an LLM search round) fall back to round 0 as a defensive default.
    Returns a dict mapping round index → {phrase: value}.
    """
    phrase_to_round: dict[str, int] = {}
    for round_idx in sorted(search_rounds.keys()):
        for phrase in search_rounds[round_idx]:
            if phrase not in phrase_to_round:
                phrase_to_round[phrase] = round_idx

    rounds: dict[int, dict[str, _T]] = {}
    for phrase, value in flat_results.items():
        round_idx = phrase_to_round.get(phrase, 0)
        rounds.setdefault(round_idx, {})[phrase] = value
    # Round 0 (brute survivors / unassigned fallback) is a reserved bucket; keep
    # it present even when empty so consumers can index it unconditionally.
    rounds.setdefault(0, {})
    return rounds


class BaseExtractionMetadata(BaseModel):
    created_at: datetime
    chunk_strat: ChunkingStrategy
    ontology_version_id: OntologyVersionIDType


class ExtractionNodeMetadata(BaseModel):
    llm_model: LLM_Model
    model_params: GPTModelParams
    prompt_name: str
    prompt_version_id: S3FileVersionIDType
    # Which rule catalog produced this prompt. Stored alongside the S3 version so
    # an applied_rule record can be joined back to the rule text that asked for
    # it, even after the catalog has since been edited. None for prompts that
    # have no catalog (search, relationship, single-stage).
    catalog_version: Optional[str] = None
    created_at: datetime


class RecursiveSearchNodeMetadata(ExtractionNodeMetadata):
    # Hard cap on the number of recursive search rounds (beyond the first search).
    # Recursion also stops early when a round yields no new phrases.
    max_rounds: int


class BatchedScreeningNodeMetadata(ExtractionNodeMetadata):
    # Hard cap on the number of phrase-relationship pairs sent to the LLM in a
    # single screening request. The full set of pairs for a chunk is split into
    # ceil(num_pairs / max_pairs_per_request) groups, each screened independently
    # and merged back into one flat result.
    max_pairs_per_request: int


class LLMPhraseExtractionMetadata(BaseExtractionMetadata):
    llm_phrase_search: ExtractionNodeMetadata
    llm_phrase_recursive_search: RecursiveSearchNodeMetadata
    llm_phrase_relationship: ExtractionNodeMetadata
    llm_phrase_relationship_screening: BatchedScreeningNodeMetadata


class LLMPhraseExtractionStats(BaseModel):
    # round -> phrases found independently that round (NOT cumulative).
    # Round 0 is reserved for brute-force search survivors (overlap-filtered
    # against LLM results; empty for keyword-type fields, which have no
    # brute-force phase). Round 1 is the first LLM search; round N (N>=2) is
    # recursive round N-1.
    llm_phrase_search: dict[int, LLMSearchResults]
    # round → {phrase: relationship} — each phrase assigned to its earliest search round
    llm_phrase_relationship: dict[int, LLMPhraseRelationshipResults]
    # round → {phrase: screening verdict} — each phrase assigned to its earliest search round
    llm_phrase_screening: dict[int, LiveScreeningResults]
