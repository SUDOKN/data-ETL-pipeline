"""Beanie document models owned by `core`.

Apps append this list to their own before calling `infra...mongo_client.init_db`.
"""

from beanie import Document

from core.db_models.extraction_run import ExtractionRun
from core.db_models.out_of_vocab_labels import OutOfVocabLabel
from core.db_models.vocabulary_candidates import VocabularyCandidate

DOCUMENT_MODELS: list[type[Document]] = [
    ExtractionRun,
    OutOfVocabLabel,
    VocabularyCandidate,
]
