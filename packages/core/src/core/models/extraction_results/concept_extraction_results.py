from __future__ import annotations
from pydantic import BaseModel

from core.models.extraction_results.llm_phrase_extraction_results import (
    ExtractionNodeMetadata,
)


class ConceptsFound(BaseModel):
    in_vocab: set[str]
    out_of_vocab: set[str]


class BatchedInitialGroundingNodeMetadata(ExtractionNodeMetadata):
    # Hard cap on evidence-bearing RECORDS per grounding request (v2: the
    # in-vocab and OOV passes run on records, before screening). The chunk's
    # records are split into ceil(num_records / max_pairs_per_request) groups,
    # each grounded independently and merged back into one flat result.
    max_pairs_per_request: int

    def to_custom_id_segment(self) -> str:
        return f"{super().to_custom_id_segment()}|gs={self.max_pairs_per_request}"
