"""Verification of the provenance stamp `publish` writes onto a prompt object.

The stamp is what lets a run prove the prompt it fetched from S3 was rendered
from the catalog the deployed code validates `applied_rules` against. Each check
below covers a drift that the other checks cannot see.
"""

import hashlib

import pytest

from llm_providers.models.llm_model import GPT_4o_mini
from infra.utils.aws.s3.prompt_s3_util import (
    CATALOG_VERSION_METADATA_KEY,
    RENDERED_SHA256_METADATA_KEY,
    PromptObject,
)

from data_etl_app.services.prompt_service import (
    PromptProvenanceError,
    PromptService,
    _CatalogPin,
    _verify_provenance,
)

TEXT = "rendered prompt text"
DIGEST = hashlib.sha256(TEXT.encode("utf-8")).hexdigest()
CATALOG_VERSION = "industry_phrase_recursive_grounding.3"


def _stamped(
    text: str = TEXT,
    catalog_version: str = CATALOG_VERSION,
    digest: str = DIGEST,
    version_id: str = "v1",
) -> PromptObject:
    return PromptObject(
        text=text,
        version_id=version_id,
        metadata={
            CATALOG_VERSION_METADATA_KEY: catalog_version,
            RENDERED_SHA256_METADATA_KEY: digest,
        },
    )


def _pin(
    catalog_version: str = CATALOG_VERSION, digest: str | None = DIGEST
) -> _CatalogPin:
    return _CatalogPin(
        catalog_version=catalog_version, s3_version_id="v1", rendered_sha256=digest
    )


def test_matching_stamp_passes():
    _verify_provenance("p", _stamped(), _pin())


def test_unstamped_object_is_rejected():
    """Objects published before stamping existed, or uploaded out of band. There is
    no way to add metadata to an existing version, so these must be re-published."""
    unstamped = PromptObject(text=TEXT, version_id="v1", metadata={})

    with pytest.raises(PromptProvenanceError, match="no provenance stamp"):
        _verify_provenance("p", unstamped, _pin())


def test_prompt_rendered_from_a_different_catalog_version_is_rejected():
    """The deployed catalog is what `core` validates applied_rules against, so a
    prompt built from a different one would have every rule report checked against
    the wrong rule set."""
    with pytest.raises(PromptProvenanceError, match="deployed catalog is"):
        _verify_provenance("p", _stamped(catalog_version="other.1"), _pin())


def test_skeleton_edit_is_caught_even_though_the_catalog_version_is_unchanged():
    """A skeleton edit changes the rendered bytes without touching catalog_version.
    This is the case the digest exists for: the version check above sees nothing."""
    edited = "rendered prompt text, with a reworded skeleton"
    obj = _stamped(text=edited, digest=hashlib.sha256(edited.encode()).hexdigest())

    with pytest.raises(PromptProvenanceError, match="the catalog recorded"):
        _verify_provenance("p", obj, _pin())


def test_body_that_does_not_hash_to_its_own_stamp_is_rejected():
    with pytest.raises(PromptProvenanceError, match="does not hash to its own"):
        _verify_provenance("p", _stamped(text="tampered"), _pin())


def test_unpublished_catalog_still_has_its_version_checked():
    """A catalog with nothing in `published` falls back to S3 latest. There is no
    recorded digest to compare against, but latest can still turn out to have been
    rendered from a different catalog, and that is the corrupting case."""
    unpublished = _CatalogPin(
        catalog_version=CATALOG_VERSION, s3_version_id=None, rendered_sha256=None
    )

    _verify_provenance("p", _stamped(), unpublished)
    with pytest.raises(PromptProvenanceError, match="deployed catalog is"):
        _verify_provenance("p", _stamped(catalog_version="other.1"), unpublished)


@pytest.mark.asyncio
async def test_download_carries_the_stamped_version_onto_the_prompt(monkeypatch):
    """What the factory records as catalog_version comes from the bytes in hand,
    not from a local file read at pipeline-build time."""
    import data_etl_app.services.prompt_service as prompt_service

    async def fake_download(filename, version_id=None):
        return _stamped()

    monkeypatch.setattr(prompt_service, "download_prompt", fake_download)
    monkeypatch.setattr(prompt_service.litellm, "token_counter", lambda **kw: 7)

    prompt = await PromptService()._download_prompt("p", GPT_4o_mini, _pin())

    assert prompt.catalog_version == CATALOG_VERSION
    assert prompt.s3_version_id == "v1"
    assert prompt.num_tokens == 7


@pytest.mark.asyncio
async def test_prompt_without_a_catalog_is_not_provenance_checked(monkeypatch):
    """Search, relationship and single-stage prompts are hand-written and have no
    catalog, so there is nothing to check them against."""
    import data_etl_app.services.prompt_service as prompt_service

    async def fake_download(filename, version_id=None):
        return PromptObject(text=TEXT, version_id="v9", metadata={})

    monkeypatch.setattr(prompt_service, "download_prompt", fake_download)
    monkeypatch.setattr(prompt_service.litellm, "token_counter", lambda **kw: 7)

    prompt = await PromptService()._download_prompt("p", GPT_4o_mini, None)

    assert prompt.catalog_version is None
    assert prompt.s3_version_id == "v9"
