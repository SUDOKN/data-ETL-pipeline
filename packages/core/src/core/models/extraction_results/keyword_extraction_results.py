from core.models.extraction_results.extraction_node_metadata import (
    ExtractionNodeMetadata,
)


class BatchedFreehandGroundingNodeMetadata(ExtractionNodeMetadata):
    # Hard cap on evidence-bearing RECORDS per freehand-grounding request (v2:
    # the pass runs on records, before screening). The chunk's records are
    # split into ceil(num_records / max_pairs_per_request) groups, each
    # grounded independently and merged back into one flat result.
    max_pairs_per_request: int

    def to_custom_id_segment(self) -> str:
        return f"{super().to_custom_id_segment()}|gs={self.max_pairs_per_request}"
