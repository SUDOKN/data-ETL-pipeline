from typing import Optional

from core.models.db.out_of_vocab_labels import OutOfVocabLabel
from data_etl_app.models.types_and_enums import ConceptTypeEnum


async def upsert_out_of_vocab_labels(
    ontology_version_id: str,
    concept_type: ConceptTypeEnum,
    new_labels: set[str],
) -> None:
    """
    Add new_labels into the (ontology_version_id, concept_type) document,
    creating it if it does not yet exist. The labels set grows monotonically.
    """
    if not new_labels:
        return

    existing = await OutOfVocabLabel.find_one(
        OutOfVocabLabel.ontology_version_id == ontology_version_id,
        OutOfVocabLabel.concept_type == concept_type,
    )
    if existing:
        lowered_existing_labels = {label.lower() for label in existing.labels}
        for label in new_labels:
            if label.lower() not in lowered_existing_labels:
                existing.labels.add(label)
        await existing.save()
    else:
        await OutOfVocabLabel(
            ontology_version_id=ontology_version_id,
            concept_type=concept_type,
            labels=new_labels,
        ).insert()


async def get_out_of_vocab_labels(
    concept_type: ConceptTypeEnum,
    ontology_version_id: str,
) -> OutOfVocabLabel | None:
    """
    Return the OutOfVocabLabels document for the given
    (ontology_version_id, concept_type) pair, or None if it does not exist.
    """
    return await OutOfVocabLabel.find_one(
        OutOfVocabLabel.ontology_version_id == ontology_version_id,
        OutOfVocabLabel.concept_type == concept_type,
    )


async def get_all_out_of_vocab_labels_for_version(
    ontology_version_id: str,
) -> list[OutOfVocabLabel]:
    """
    Return all OutOfVocabLabels documents for the given ontology_version_id,
    across all concept types.
    """
    return await OutOfVocabLabel.find(
        OutOfVocabLabel.ontology_version_id == ontology_version_id,
    ).to_list()


def get_case_matched_existing_label(
    existing_doc: Optional[OutOfVocabLabel],
    label: str,
) -> str | None:
    """
    Return the existing label from the (ontology_version_id, concept_type)
    document that matches the given label case-insensitively, or None if no
    match is found. This is used to prevent storing multiple case variants of
    the same label.
    """
    if not existing_doc:
        return None

    for existing_kw in existing_doc.labels:
        if existing_kw.lower() == label.lower():
            return existing_kw

    return None
