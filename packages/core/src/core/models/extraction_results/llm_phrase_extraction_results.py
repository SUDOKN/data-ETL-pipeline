from __future__ import annotations

from pydantic import BaseModel
from datetime import datetime
from typing import Optional


from core.field_types import (
    OntologyVersionIDType,
)
from infra.field_types import (
    S3FileVersionIDType,
)
from llm_providers.models.llm_model import LLM_Model
from core.models.chunking_strat import ChunkingStrategy
from llm_providers.models.open_ai.gpt_model_params import (
    GPTModelParams,
)

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

    def to_custom_id_segment(self) -> str:
        """Model, non-default params, and the pinned prompt version — e.g.
        ``gpt-4.1|temperature=0.0|pv=<s3-version-id>``. The prompt version makes
        the prompt part of request identity: without it, a re-run after a prompt
        edit finds the old requests complete and replays responses the old
        prompt produced.

        Everything that decides what a request *contains* belongs in this segment,
        for that same reason. Batched stages append their group size (``|gs=50``)
        on top: it decides which phrases share a request, so the same group index
        at a different cap is a different question with a different answer."""
        return (
            f"{self.model_params.to_custom_id_segment(self.llm_model.name)}"
            f"|pv={self.prompt_version_id}"
        )


class RecursiveSearchNodeMetadata(ExtractionNodeMetadata):
    # Hard cap on the number of recursive search rounds (beyond the first search).
    # Recursion also stops early when a round yields no new phrases.
    max_rounds: int


class BatchedRelationshipNodeMetadata(ExtractionNodeMetadata):
    # Hard cap on the number of candidate phrases sent to the LLM in a single
    # relationship request. The full candidate list for a chunk is split into
    # ceil(num_phrases / max_phrases_per_request) groups, each described
    # independently — against the same full chunk text — and merged back into one
    # flat result. Unit is phrases, not pairs: relationship is what *produces* the
    # phrase→description pairs the screening stage then batches.
    max_phrases_per_request: int

    def to_custom_id_segment(self) -> str:
        return f"{super().to_custom_id_segment()}|gs={self.max_phrases_per_request}"


class BatchedScreeningNodeMetadata(ExtractionNodeMetadata):
    # Hard cap on the number of phrase-relationship pairs sent to the LLM in a
    # single screening request. The full set of pairs for a chunk is split into
    # ceil(num_pairs / max_pairs_per_request) groups, each screened independently
    # and merged back into one flat result.
    max_pairs_per_request: int

    def to_custom_id_segment(self) -> str:
        return f"{super().to_custom_id_segment()}|gs={self.max_pairs_per_request}"
