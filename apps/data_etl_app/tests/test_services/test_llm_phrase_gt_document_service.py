"""The get-or-create decision logic (P3.2), offline via injected fakes.

The default Beanie/S3 callables are thin I/O one-liners exercised through the
routes; what is tested here is every branch of the decision: create, found
(and its no-S3-no-inflation short-circuit), lost race, vanished winner,
nothing-to-audit.
"""

from typing import Optional

import pytest
from pymongo.errors import DuplicateKeyError

from data_etl_app.db_models.llm_phrase_ground_truth import LLMPhraseGroundTruth
from data_etl_app.models.types_and_enums import KeywordTypeEnum
from data_etl_app.services.ground_truth.llm_phrase_gt_document_service import (
    get_or_create_llm_phrase_gt,
)
from data_etl_app.services.ground_truth.llm_phrase_gt_template_service import (
    TemplateAssemblyError,
    build_llm_phrase_gt_template,
)


class Spies:
    """Injectable fakes recording every interaction."""

    def __init__(
        self,
        text: str,
        existing_sequence: list[Optional[LLMPhraseGroundTruth]],
        insert_raises: Optional[Exception] = None,
    ):
        self.text = text
        self.existing_sequence = list(existing_sequence)
        self.insert_raises = insert_raises
        self.find_calls: list[tuple] = []
        self.fetch_calls: list[dict] = []
        self.inserted: list[LLMPhraseGroundTruth] = []

    async def find_existing(self, *args) -> Optional[LLMPhraseGroundTruth]:
        self.find_calls.append(args)
        return self.existing_sequence.pop(0)

    async def fetch_text(self, **kwargs) -> tuple[str, str]:
        self.fetch_calls.append(kwargs)
        return self.text, kwargs["version_id"]

    async def insert(self, doc: LLMPhraseGroundTruth) -> object:
        if self.insert_raises is not None:
            raise self.insert_raises
        self.inserted.append(doc)
        return doc


@pytest.mark.asyncio
async def test_first_read_creates_and_saves_the_template(
    make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
):
    mfg = make_gt_manufacturer(equipments=make_keyword_gt_results())
    spies = Spies(gt_scraped_text, existing_sequence=[None])

    doc, created = await get_or_create_llm_phrase_gt(
        mfg,
        KeywordTypeEnum.equipments,
        catalog_lookup=gt_catalog_lookup,
        fetch_text=spies.fetch_text,
        find_existing=spies.find_existing,
        insert=spies.insert,
    )

    assert created is True
    assert spies.inserted == [doc]
    assert spies.fetch_calls == [
        {"subject_unique_id": "steelcraft.com", "version_id": "text-v3"}
    ]
    # The digest used for the existence check is the same one the built
    # template carries — the early computation cannot drift from the document.
    (find_call,) = spies.find_calls
    assert find_call == (
        "steelcraft.com",
        KeywordTypeEnum.equipments,
        "text-v3",
        doc.identity_digest,
    )


@pytest.mark.asyncio
async def test_existing_document_short_circuits_without_s3_or_inflation(
    make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
):
    mfg = make_gt_manufacturer(equipments=make_keyword_gt_results())
    stored = build_llm_phrase_gt_template(
        mfg, KeywordTypeEnum.equipments, gt_scraped_text, catalog_lookup=gt_catalog_lookup
    )
    spies = Spies(gt_scraped_text, existing_sequence=[stored])

    doc, created = await get_or_create_llm_phrase_gt(
        mfg,
        KeywordTypeEnum.equipments,
        catalog_lookup=gt_catalog_lookup,
        fetch_text=spies.fetch_text,
        find_existing=spies.find_existing,
        insert=spies.insert,
    )

    assert created is False
    assert doc is stored
    assert spies.fetch_calls == []  # no S3 fetch
    assert spies.inserted == []  # no write


@pytest.mark.asyncio
async def test_lost_insert_race_returns_the_winner(
    make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
):
    mfg = make_gt_manufacturer(equipments=make_keyword_gt_results())
    winner = build_llm_phrase_gt_template(
        mfg, KeywordTypeEnum.equipments, gt_scraped_text, catalog_lookup=gt_catalog_lookup
    )
    spies = Spies(
        gt_scraped_text,
        existing_sequence=[None, winner],
        insert_raises=DuplicateKeyError("E11000 duplicate key"),
    )

    doc, created = await get_or_create_llm_phrase_gt(
        mfg,
        KeywordTypeEnum.equipments,
        catalog_lookup=gt_catalog_lookup,
        fetch_text=spies.fetch_text,
        find_existing=spies.find_existing,
        insert=spies.insert,
    )

    assert created is False
    assert doc is winner
    assert len(spies.find_calls) == 2


@pytest.mark.asyncio
async def test_race_with_a_vanished_winner_surfaces_the_error(
    make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
):
    mfg = make_gt_manufacturer(equipments=make_keyword_gt_results())
    spies = Spies(
        gt_scraped_text,
        existing_sequence=[None, None],
        insert_raises=DuplicateKeyError("E11000 duplicate key"),
    )

    with pytest.raises(DuplicateKeyError):
        await get_or_create_llm_phrase_gt(
            mfg,
            KeywordTypeEnum.equipments,
            catalog_lookup=gt_catalog_lookup,
            fetch_text=spies.fetch_text,
            find_existing=spies.find_existing,
            insert=spies.insert,
        )


@pytest.mark.asyncio
async def test_field_without_stored_results_is_refused_before_any_io(
    make_gt_manufacturer, gt_catalog_lookup, gt_scraped_text
):
    spies = Spies(gt_scraped_text, existing_sequence=[])

    with pytest.raises(TemplateAssemblyError, match="nothing to audit"):
        await get_or_create_llm_phrase_gt(
            make_gt_manufacturer(),
            KeywordTypeEnum.equipments,
            catalog_lookup=gt_catalog_lookup,
            fetch_text=spies.fetch_text,
            find_existing=spies.find_existing,
            insert=spies.insert,
        )

    assert spies.find_calls == [] and spies.fetch_calls == []
