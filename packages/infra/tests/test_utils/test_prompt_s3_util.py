"""The provenance stamp round-trip.

`publish` writes the catalog version and rendered digest as S3 user metadata
rather than object tags, because metadata comes back inline with GetObject and is
fixed for a given version. These tests cover the two ends of that trip.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from infra.utils.aws.s3.prompt_s3_util import (
    CATALOG_VERSION_METADATA_KEY,
    RENDERED_SHA256_METADATA_KEY,
    download_prompt,
    upload_prompt,
)

STAMP = {
    CATALOG_VERSION_METADATA_KEY: "industry_phrase_recursive_grounding.3",
    RENDERED_SHA256_METADATA_KEY: "abc123",
}


def _client_returning(get_object_response: dict) -> MagicMock:
    client = MagicMock()
    client.put_object = AsyncMock(return_value={"VersionId": "v1"})
    client.get_object = AsyncMock(return_value=get_object_response)
    return client


def _body(text: str) -> MagicMock:
    body = MagicMock()
    body.read = AsyncMock(return_value=text.encode("utf-8"))
    return body


@pytest.fixture
def s3_client():
    client = _client_returning({})
    with patch(
        "infra.utils.aws.s3.prompt_s3_util.get_prompt_rdf_s3_client",
        return_value=client,
    ), patch(
        "infra.utils.aws.s3.prompt_s3_util._prompt_bucket", return_value="test-bucket"
    ):
        yield client


@pytest.mark.asyncio
async def test_upload_stamps_the_object_with_the_metadata_it_is_given(s3_client):
    version_id = await upload_prompt("a/b.txt", "text", metadata=STAMP)

    assert version_id == "v1"
    assert s3_client.put_object.await_args.kwargs["Metadata"] == STAMP


@pytest.mark.asyncio
async def test_upload_without_metadata_sends_an_empty_stamp(s3_client):
    await upload_prompt("a/b.txt", "text")

    assert s3_client.put_object.await_args.kwargs["Metadata"] == {}


@pytest.mark.asyncio
async def test_download_exposes_the_stamp(s3_client):
    s3_client.get_object.return_value = {
        "Body": _body("text"),
        "VersionId": "v1",
        # S3 hands metadata keys back lower-cased; upper-case them here to prove
        # the lookups do not depend on the casing the object was written with.
        "Metadata": {key.upper(): value for key, value in STAMP.items()},
    }

    obj = await download_prompt("a/b.txt")

    assert obj.text == "text"
    assert obj.version_id == "v1"
    assert obj.catalog_version == STAMP[CATALOG_VERSION_METADATA_KEY]
    assert obj.rendered_sha256 == STAMP[RENDERED_SHA256_METADATA_KEY]


@pytest.mark.asyncio
async def test_download_of_an_unstamped_object_reports_no_provenance(s3_client):
    """Objects uploaded before stamping existed, or out of band. PromptService
    turns this into a hard failure for any prompt that has a catalog."""
    s3_client.get_object.return_value = {"Body": _body("text"), "VersionId": "v1"}

    obj = await download_prompt("a/b.txt")

    assert obj.metadata == {}
    assert obj.catalog_version is None
    assert obj.rendered_sha256 is None
