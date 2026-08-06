"""Beanie document models owned by `core`.

Apps append this list to their own before calling `infra...mongo_client.init_db`.
"""

from beanie import Document

from core.db_models.out_of_vocab_labels import OutOfVocabLabel

DOCUMENT_MODELS: list[type[Document]] = [
    OutOfVocabLabel,
]
