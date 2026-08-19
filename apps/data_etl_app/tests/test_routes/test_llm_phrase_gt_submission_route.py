"""The atomic batch POST (P3.6), direct-call convention with fakes.

The apply semantics live in the submission-service tests; here: the route's
conditional fetches (S3 only for missed phrases, ontology only for concept
grounding/divergence), the error-code mapping, the persist seam, and the
response shape including the verdict-6d tag-classification echo.
"""

import copy
import json
from datetime import datetime, timezone
from typing import cast

import pytest
from beanie import PydanticObjectId
from fastapi import HTTPException
from pydantic import ValidationError

import data_etl_app.api.routes.ground_truth.llm_phrase_ground_truth as route_mod
from core.models.ground_truth.audits import AuditVerdict, TextFieldAudit
from core.models.ground_truth.stage_blocks import (
    GroundingDerivation,
    HumanScreeningDerivation,
)
from data_etl_app.api.routes.ground_truth.llm_phrase_ground_truth import (
    LLMPhraseGTSubmissionRequest,
    submit_llm_phrase_gt_batch,
)
from data_etl_app.db_models.user import User, UserRole
from data_etl_app.models.types_and_enums import ConceptTypeEnum, KeywordTypeEnum
from data_etl_app.services.ground_truth.llm_phrase_gt_submission_service import (
    OovGroundingItem,
    RelationshipAuditItem,
    ScreeningDerivationItem,
    children_from_concept_map,
)
from data_etl_app.services.ground_truth.llm_phrase_gt_template_service import (
    build_llm_phrase_gt_template,
)

ALICE = "alice@example.com"


def _annotator(email: str = ALICE) -> User:
    return User(
        firstName="Ada",
        lastName="Annotator",
        email=email,
        role=UserRole.ANNOTATOR,
        companyURL=None,
        salt="salt",
        hashedPassword="hashed",
    )


def _audit(author=ALICE, type=AuditVerdict.AGREE, corrected_text=None):
    return TextFieldAudit(
        type=type,
        corrected_text=corrected_text,
        author_email=author,
        at=datetime(2026, 8, 16, tzinfo=timezone.utc),
        source="api",
    )


@pytest.fixture
def keyword_doc(
    make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
):
    mfg = make_gt_manufacturer(equipments=make_keyword_gt_results())
    doc = build_llm_phrase_gt_template(
        mfg, KeywordTypeEnum.equipments, gt_scraped_text, catalog_lookup=gt_catalog_lookup
    )
    doc.chunks[f"0:{len(gt_scraped_text)}"] = doc.chunks.pop("0:1000")
    doc.id = PydanticObjectId()
    return doc


@pytest.fixture
def concept_doc(
    make_gt_manufacturer, make_concept_gt_results, gt_catalog_lookup, gt_scraped_text
):
    mfg = make_gt_manufacturer(industries=make_concept_gt_results())
    doc = build_llm_phrase_gt_template(
        mfg, ConceptTypeEnum.industries, gt_scraped_text, catalog_lookup=gt_catalog_lookup
    )
    doc.id = PydanticObjectId()
    return doc


class Seams:
    def __init__(self, doc, text: str):
        self.doc = doc
        self.text = text
        self.replaced: list = []
        self.s3_calls = 0
        self.ontology_builds = 0

    async def get_by_id(self, object_id):
        return self.doc if object_id == self.doc.id else None

    async def download(self, subject_unique_id: str, version_id: str):
        self.s3_calls += 1
        return self.text, version_id

    async def build_ontology(self, ontology_version_id: str, field_type):
        self.ontology_builds += 1
        class _Concept:
            def __init__(self, children):
                self.children = children
        return children_from_concept_map(
            {"Aerospace": _Concept(["Satellites"]), "Satellites": _Concept([])}
        )

    async def replace(self, doc) -> None:
        self.replaced.append(doc)


@pytest.fixture
def seams(monkeypatch: pytest.MonkeyPatch, request):
    def _install(doc, text: str) -> Seams:
        seams = Seams(doc, text)
        monkeypatch.setattr(route_mod, "get_llm_phrase_gt_by_id", seams.get_by_id)
        monkeypatch.setattr(
            route_mod,
            "download_scraped_text_from_s3_by_subject_unique_id",
            seams.download,
        )
        monkeypatch.setattr(
            route_mod, "build_ontology_children", seams.build_ontology
        )
        monkeypatch.setattr(route_mod, "replace_llm_phrase_gt", seams.replace)
        return seams

    return _install


def _rel_item(doc, author=ALICE) -> RelationshipAuditItem:
    return RelationshipAuditItem(
        surface="relationship_audit",
        chunk_key=next(iter(doc.chunks)),
        phrase="cnc mill",
        audit=_audit(author, AuditVerdict.AGREE_BUT, "five-axis work too"),
    )


@pytest.mark.asyncio
async def test_happy_batch_persists_and_reports(
    keyword_doc, seams, gt_scraped_text
):
    installed = seams(keyword_doc, gt_scraped_text)
    before = keyword_doc.updated_at

    response = await submit_llm_phrase_gt_batch(
        request=LLMPhraseGTSubmissionRequest(
            document_id=str(keyword_doc.id),
            items=[_rel_item(keyword_doc)],
        ),
        user=_annotator(),
    )

    (persisted,) = installed.replaced
    key = next(iter(keyword_doc.chunks))
    assert (
        persisted.chunks[key].extracted_phrases["cnc mill"].llm_relationship.audits
        != []
    )
    assert persisted.updated_at >= before
    assert installed.s3_calls == 0  # no missed-phrase items
    assert installed.ontology_builds == 0  # keyword field
    assert response["applied_items"] == [
        {
            "index": 0,
            "surface": "relationship_audit",
            "chunk_key": key,
            "phrase": "cnc mill",
        }
    ]
    assert response["unreviewed_remaining"] == 0
    assert key in response["truth"]
    assert "document" not in response
    json.dumps(response)


@pytest.mark.asyncio
async def test_failing_item_maps_to_400_and_persists_nothing(
    keyword_doc, seams, gt_scraped_text
):
    installed = seams(keyword_doc, gt_scraped_text)
    key = next(iter(keyword_doc.chunks))
    llm = keyword_doc.chunks[key].extracted_phrases["cnc mill"].llm_screening
    assert llm is not None
    silent_edit = copy.deepcopy(llm.llm_result.sections)
    for section in silent_edit:
        for rule in section.applied_rules:
            if rule.outcome == "satisfied":
                rule.outcome = "failed"
    bad = ScreeningDerivationItem(
        surface="screening_derivation",
        chunk_key=key,
        phrase="cnc mill",
        derivation=HumanScreeningDerivation(
            identified_entity=llm.llm_result.identified_entity,
            audits=[_audit()],
            sections=silent_edit,
        ),
    )

    with pytest.raises(HTTPException) as exc_info:
        await submit_llm_phrase_gt_batch(
            request=LLMPhraseGTSubmissionRequest(
                document_id=str(keyword_doc.id),
                items=[_rel_item(keyword_doc), bad],
            ),
            user=_annotator(),
        )

    assert exc_info.value.status_code == 400
    # starlette types .detail as str; the route puts a dict there.
    detail = cast(dict, exc_info.value.detail)
    assert detail["item_index"] == 1
    assert detail["surface"] == "screening_derivation"
    assert installed.replaced == []  # nothing persisted


@pytest.mark.asyncio
async def test_invalid_and_unknown_document_ids(keyword_doc, seams, gt_scraped_text):
    seams(keyword_doc, gt_scraped_text)

    with pytest.raises(HTTPException) as exc_info:
        await submit_llm_phrase_gt_batch(
            request=LLMPhraseGTSubmissionRequest(
                document_id="not-an-id", items=[_rel_item(keyword_doc)]
            ),
            user=_annotator(),
        )
    assert exc_info.value.status_code == 400

    with pytest.raises(HTTPException) as exc_info:
        await submit_llm_phrase_gt_batch(
            request=LLMPhraseGTSubmissionRequest(
                document_id=str(PydanticObjectId()),
                items=[_rel_item(keyword_doc)],
            ),
            user=_annotator(),
        )
    assert exc_info.value.status_code == 404


def test_empty_batches_are_rejected_at_the_model(keyword_doc):
    with pytest.raises(ValidationError):
        LLMPhraseGTSubmissionRequest(document_id=str(keyword_doc.id), items=[])


@pytest.mark.asyncio
async def test_missed_phrase_batch_fetches_s3_once(
    keyword_doc, seams, gt_catalog_lookup, gt_fresh_happy, gt_scraped_text
):
    from core.models.ground_truth.stage_blocks import MissedPhraseEntry
    from data_etl_app.services.ground_truth.llm_phrase_gt_submission_service import (
        MissedPhraseItem,
    )

    installed = seams(keyword_doc, gt_scraped_text)
    screening = gt_catalog_lookup("phrase_relationship_screening", "equipments")
    freehand = gt_catalog_lookup("phrase_freehand_grounding", "equipments")
    item = MissedPhraseItem(
        surface="missed_phrase",
        chunk_key=next(iter(keyword_doc.chunks)),
        entry=MissedPhraseEntry(
            phrase="CNC mills",
            author_email=ALICE,
            at=datetime(2026, 8, 16, tzinfo=timezone.utc),
            source="api",
            relationship_text="machines they run in-house",
            screening=HumanScreeningDerivation(
                identified_entity="CNC mills",
                audits=[_audit()],
                sections=gt_fresh_happy(screening),
            ),
            groundings=[
                GroundingDerivation(
                    tag="CNC Milling Machine",
                    audits=[_audit()],
                    sections=gt_fresh_happy(freehand),
                )
            ],
        ),
    )

    response = await submit_llm_phrase_gt_batch(
        request=LLMPhraseGTSubmissionRequest(
            document_id=str(keyword_doc.id), items=[item]
        ),
        user=_annotator(),
    )

    assert installed.s3_calls == 1
    (persisted,) = installed.replaced
    key = next(iter(keyword_doc.chunks))
    assert [e.phrase for e in persisted.chunks[key].missed_phrases] == ["CNC mills"]
    assert response["applied_items"][0]["phrase"] == "CNC mills"


@pytest.mark.asyncio
async def test_concept_oov_item_builds_ontology_and_echoes_classification(
    concept_doc, seams, gt_catalog_lookup, gt_fresh_happy, gt_scraped_text
):
    installed = seams(concept_doc, gt_scraped_text)
    initial = gt_catalog_lookup("phrase_initial_grounding", "industries")
    llm_entry = concept_doc.chunks["0:1000"].extracted_phrases[
        "aerospace parts"
    ].oov_grounding
    assert llm_entry is not None
    # Replace the initial tag with a NON-ontology name: an OOV assertion.
    item = OovGroundingItem(
        surface="oov_grounding_derivation",
        chunk_key="0:1000",
        phrase="aerospace parts",
        tag="Aerospace",
        derivation=GroundingDerivation(
            tag="orbital logistics",
            audits=[_audit(ALICE, AuditVerdict.DISAGREE, "orbital logistics")],
            sections=gt_fresh_happy(initial),
        ),
    )

    response = await submit_llm_phrase_gt_batch(
        request=LLMPhraseGTSubmissionRequest(
            document_id=str(concept_doc.id), items=[item]
        ),
        user=_annotator(),
    )

    assert installed.ontology_builds == 1
    assert response["applied_items"][0]["tag_classification"] == "out_of_vocab"


@pytest.mark.asyncio
async def test_pin_drift_maps_to_409(keyword_doc, seams, gt_scraped_text):
    seams(keyword_doc, gt_scraped_text)
    keyword_doc.metadata.llm_phrase_relationship_screening.catalog_version = (
        "stale.0"
    )

    with pytest.raises(HTTPException) as exc_info:
        await submit_llm_phrase_gt_batch(
            request=LLMPhraseGTSubmissionRequest(
                document_id=str(keyword_doc.id), items=[_rel_item(keyword_doc)]
            ),
            user=_annotator(),
        )
    assert exc_info.value.status_code == 409
    assert "stale.0" in exc_info.value.detail


@pytest.mark.asyncio
async def test_return_full_includes_the_document(
    keyword_doc, seams, gt_scraped_text
):
    seams(keyword_doc, gt_scraped_text)

    response = await submit_llm_phrase_gt_batch(
        request=LLMPhraseGTSubmissionRequest(
            document_id=str(keyword_doc.id),
            items=[_rel_item(keyword_doc)],
            return_full=True,
        ),
        user=_annotator(),
    )

    assert "document" in response
    assert response["document"]["identity_digest"] == keyword_doc.identity_digest
    json.dumps(response)
