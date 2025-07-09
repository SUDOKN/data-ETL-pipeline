from beanie import Document
from datetime import datetime
from pydantic import BaseModel, computed_field, Field, field_validator
from typing import Optional

from shared.utils.time_util import get_current_time
from shared.models.types import LLMMappingType, MfgURLType, OntologyVersionIDType
from shared.models.db.extraction_results import ChunkSearchStats

from data_etl_app.models.types import ConceptTypeEnum


class ResultCorrection(BaseModel):
    add: LLMMappingType
    remove: list[str]


class KeywordGroundTruth(Document):  # TODO: add authors
    created_at: datetime = Field(default_factory=lambda: get_current_time())
    scraped_text_file_version_id: (
        str  # copy of mfg's scraped_text_file_version_id, for independent consistency
    )
    ontology_version_id: OntologyVersionIDType  # copy of mfg's ontology version id at the time of creating new KeywordGroundTruth; used as receipt log
    mfg_url: MfgURLType  # foreign key to Manufacturer

    concept_type: ConceptTypeEnum
    chunk_bounds: str  # foreign key to chunk in mfg
    chunk_text: str  # can be derived but stored for guaranteed consistency in case the associated mfg is modified/reprocessed

    # following fields are more like receipt and to enhance API interaction
    chunk_no: int
    last_chunk_no: int
    chunk_search_stats: ChunkSearchStats

    # corrections made by experts, to be applied on chunk_search_stats.results to get final ground truth results
    result_correction: Optional[ResultCorrection] = None

    # validator to check chunk_no ge 1 and le last_chunk_no
    @field_validator("chunk_no")
    def check_chunk_no(cls, v, values):
        last_chunk_no = values.get("last_chunk_no")
        if last_chunk_no is None:
            raise ValueError("last_chunk_no must be set before validating chunk_no.")
        if v < 1 or v > last_chunk_no:
            raise ValueError("chunk_no must be between 1 and last_chunk_no.")
        return v

    @computed_field
    @property
    def corrected_results(self) -> list[str] | None:
        if not self.result_correction:
            return None

        final_results = set(self.chunk_search_stats.results) - set(
            self.result_correction.remove
        )  # ensure beforehand that every element in remove must be present in results
        final_results |= set(self.result_correction.add.keys())
        return list(final_results)

    class Settings:
        name = "keyword_ground_truth"


"""
TODO: 

- add compound unique index on (mfg_url, scraped_text_file_version_id, ontology_version_id, concept_type, chunk_bounds)
"""
