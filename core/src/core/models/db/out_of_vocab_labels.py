from beanie import Document
from pydantic import Field

from data_etl_app.models.types_and_enums import ConceptTypeEnum


class OutOfVocabLabel(Document):
    """
    Tracks keywords proposed in mapping corrections that are not present in the
    ontology vocabulary, keyed by (ontology_version_id, concept_type).

    Only keywords submitted with a "Correct, " prefix reason are stored here.
    These keywords are candidates for future ontology expansion.

    Note: keywords excluded from accuracy metrics — see get_corrected_results().
    """

    ontology_version_id: str
    concept_type: ConceptTypeEnum
    labels: set[str] = Field(default_factory=set)

    class Settings:
        name = "out_of_vocab_labels"


"""
Indexes in MongoDB for OutOfVocabLabel:

db.out_of_vocab_labels.createIndex(
  {
    ontology_version_id: 1,
    concept_type: 1,
  },
  {
    name: "out_of_vocab_labels_unique_idx",
    unique: true
  }
)
"""
