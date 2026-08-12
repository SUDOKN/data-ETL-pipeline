"""Verification of the provenance stamp `publish` writes onto a prompt object.

The stamp is what lets a run prove the prompt it fetched from S3 was rendered
from the catalog the deployed code validates `applied_rules` against. Each check
below covers a drift that the other checks cannot see.
"""

import hashlib
import json

import pytest

from llm_providers.models.llm_model import GPT_4o_mini
from infra.utils.aws.s3.prompt_s3_util import (
    CATALOG_VERSION_METADATA_KEY,
    RENDERED_SHA256_METADATA_KEY,
    PromptObject,
)

from data_etl_app.services.prompt_assembly_service import PromptPin
from data_etl_app.services.prompt_service import (
    PromptProvenanceError,
    PromptService,
    _verify_provenance,
)

TEXT = "rendered prompt text"
DIGEST = hashlib.sha256(TEXT.encode("utf-8")).hexdigest()
CATALOG_VERSION = "industry_phrase_recursive_grounding.3"


def _stamped(
    text: str = TEXT,
    catalog_version: str | None = CATALOG_VERSION,
    digest: str = DIGEST,
    version_id: str = "v1",
) -> PromptObject:
    metadata = {RENDERED_SHA256_METADATA_KEY: digest}
    if catalog_version is not None:
        metadata[CATALOG_VERSION_METADATA_KEY] = catalog_version
    return PromptObject(text=text, version_id=version_id, metadata=metadata)


def _pin(
    catalog_version: str = CATALOG_VERSION, digest: str | None = DIGEST
) -> PromptPin:
    return PromptPin(
        s3_key="p.txt",
        s3_version_id="v1",
        rendered_sha256=digest,
        catalog_version=catalog_version,
    )


def _static_pin(digest: str | None = DIGEST) -> PromptPin:
    """A hand-written prompt's pin: a digest and nothing else to check."""
    return PromptPin(s3_key="p.txt", s3_version_id="v1", rendered_sha256=digest)


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


def test_static_prompt_is_verified_against_its_digest():
    """A hand-written prompt has no catalog version, so the digest is the whole
    check. It is still a check: this is the drift that went unseen for as long as
    static prompts had no pin at all."""
    _verify_provenance("p", _stamped(catalog_version=None), _static_pin())

    edited = "the relationship prompt, with grouping removed"
    obj = _stamped(
        text=edited,
        catalog_version=None,
        digest=hashlib.sha256(edited.encode()).hexdigest(),
    )
    with pytest.raises(PromptProvenanceError, match="the static pin file recorded"):
        _verify_provenance("p", obj, _static_pin())


def test_unstamped_static_object_is_rejected():
    """Every static prompt in S3 predates stamping, so this is the state they are
    all in until `publish --force` re-uploads them."""
    unstamped = PromptObject(text=TEXT, version_id="v1", metadata={})

    with pytest.raises(PromptProvenanceError, match="no provenance stamp"):
        _verify_provenance("p", unstamped, _static_pin())


def test_static_pin_over_a_catalog_rendered_object_is_rejected():
    """The key has been taken over by a rendered prompt while the static pin
    survived. Trusting the pin would mean checking a catalog prompt against no
    catalog at all — the check would pass and mean nothing."""
    with pytest.raises(PromptProvenanceError, match="pinned as hand-written"):
        _verify_provenance("p", _stamped(), _static_pin())


def test_unpublished_catalog_still_has_its_version_checked():
    """A catalog with nothing in `published` falls back to S3 latest. There is no
    recorded digest to compare against, but latest can still turn out to have been
    rendered from a different catalog, and that is the corrupting case."""
    unpublished = PromptPin(
        s3_key="p.txt",
        catalog_version=CATALOG_VERSION,
        s3_version_id=None,
        rendered_sha256=None,
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
async def test_prompt_with_no_pin_is_not_provenance_checked(monkeypatch):
    """An unpublished prompt falls back to S3 latest. Nothing recorded it, so there
    is nothing to check the bytes against — the state every static prompt was in
    before the pin file existed."""
    import data_etl_app.services.prompt_service as prompt_service

    async def fake_download(filename, version_id=None):
        return PromptObject(text=TEXT, version_id="v9", metadata={})

    monkeypatch.setattr(prompt_service, "download_prompt", fake_download)
    monkeypatch.setattr(prompt_service.litellm, "token_counter", lambda **kw: 7)

    prompt = await PromptService()._download_prompt("p", GPT_4o_mini, None)

    assert prompt.catalog_version is None
    assert prompt.s3_version_id == "v9"


@pytest.mark.asyncio
async def test_pin_pointing_at_a_different_key_than_the_service_reads_is_rejected(
    monkeypatch,
):
    """The publish-side key and the runtime path map are separate maps over the same
    prompts. Let them drift and one of them addresses an object nobody maintains,
    with the pin silently vouching for a file the service never fetches."""
    import data_etl_app.services.prompt_service as prompt_service

    async def fake_download(filename, version_id=None):
        raise AssertionError("must fail before fetching anything")

    monkeypatch.setattr(prompt_service, "download_prompt", fake_download)
    elsewhere = _pin()._replace(s3_key="multi_stage/1_phrase_search/other.txt")

    with pytest.raises(PromptProvenanceError, match="but this service reads"):
        await PromptService()._download_prompt("p", GPT_4o_mini, elsewhere)


def test_pins_merge_both_records_into_one_map(tmp_path):
    """`PromptService` asks one question — what should be in S3 for this name — and
    the two records answer it in the same shape."""
    from data_etl_app.services.prompt_assembly_service import load_prompt_pins

    pin_file = tmp_path / "static_prompt_pins.config.json"
    pin_file.write_text(
        json.dumps(
            {
                "prompts": {
                    "equipment_phrase_relationship": {
                        "s3_key": "multi_stage/3_phrase_relationship/equipment_phrase_relationship.txt",
                        "s3_version_id": "v42",
                        "rendered_sha256": DIGEST,
                        "uploaded_at": "2026-08-11T00:00:00+00:00",
                    }
                }
            }
        )
    )

    pins = load_prompt_pins(pin_file=pin_file)
    static = pins["equipment_phrase_relationship"]
    assert (static.s3_version_id, static.rendered_sha256) == ("v42", DIGEST)
    assert static.catalog_version is None

    catalog = pins["equipment_phrase_relationship_screening"]
    assert catalog.catalog_version is not None


def test_a_name_pinned_as_both_kinds_is_refused(tmp_path):
    """A prompt that gains a catalog keeps its name and its S3 key, so a leftover
    static entry leaves two records claiming one object with no way to rank them."""
    from data_etl_app.services.prompt_assembly_service import (
        PromptAssemblyError,
        load_prompt_pins,
    )

    pin_file = tmp_path / "static_prompt_pins.config.json"
    pin_file.write_text(
        json.dumps(
            {
                "prompts": {
                    "equipment_phrase_relationship_screening": {
                        "s3_key": "multi_stage/4_phrase_relationship_screening/equipment_phrase_relationship_screening.txt",
                        "s3_version_id": "v42",
                        "rendered_sha256": DIGEST,
                        "uploaded_at": "2026-08-11T00:00:00+00:00",
                    }
                }
            }
        )
    )

    with pytest.raises(PromptAssemblyError, match="pinned as both"):
        load_prompt_pins(pin_file=pin_file)
