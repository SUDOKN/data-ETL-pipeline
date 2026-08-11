from typing import NamedTuple, Optional

from pure_utils.env_util import require_env

from infra.utils.aws.clients import (
    get_prompt_rdf_s3_client,
)

# Provenance stamped onto a prompt object at upload time, as S3 user metadata
# rather than object tags: metadata comes back inline with GetObject (tags need a
# second GetObjectTagging call) and is fixed for a given version, whereas tags can
# be rewritten without producing a new version. A stamp that could be edited after
# the fact would not be provenance.
CATALOG_VERSION_METADATA_KEY = "catalog-version"
RENDERED_SHA256_METADATA_KEY = "rendered-sha256"


class PromptObject(NamedTuple):
    """A prompt as read back from S3, with the provenance stamped on it."""

    text: str
    version_id: str
    metadata: dict[str, str]

    @property
    def catalog_version(self) -> Optional[str]:
        return self.metadata.get(CATALOG_VERSION_METADATA_KEY)

    @property
    def rendered_sha256(self) -> Optional[str]:
        return self.metadata.get(RENDERED_SHA256_METADATA_KEY)


def _prompt_bucket() -> str:
    return require_env("PROMPT_BUCKET")


def get_prompt_filename(prompt_name: str) -> str:
    """Get the S3 key (filename) for a given prompt name."""
    if not prompt_name:
        raise ValueError("Prompt name must be provided")

    return f"{prompt_name}.txt"


async def does_prompt_version_exist(prompt_filename: str, version_id: str) -> bool:
    """Checks if a specific version of the prompt file exists in the S3 bucket.
    :param prompt_filename: The name of the prompt file to check.
    :param version_id: Version ID to check for a specific version of the file.
    :return: True if the file version exists, False otherwise.
    """
    assert prompt_filename, "Prompt filename is not set"
    bucket = _prompt_bucket()
    s3_client = get_prompt_rdf_s3_client()
    from botocore.exceptions import ClientError

    try:
        await s3_client.head_object(
            Bucket=bucket, Key=prompt_filename, VersionId=version_id
        )
        return True
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code in ("404", "403", "NoSuchKey", "NoSuchVersion"):
            return False
        raise  # Re-raise other exceptions


async def upload_prompt(
    prompt_filename: str, content: str, metadata: Optional[dict[str, str]] = None
) -> str:
    """Write a prompt file to the S3 bucket and return the new object version ID.

    The returned version ID is what a rule catalog records in ``published``, and
    what ``download_prompt`` is later pinned to, so a run reads back exactly the
    bytes that were published.

    :param prompt_filename: The S3 key to write to.
    :param content: The rendered prompt text.
    :param metadata: Provenance to stamp on the object, read back by
        ``download_prompt``. Values must be US-ASCII and total under 2 KB.
    :return: The version ID of the newly written object.
    """
    assert prompt_filename, "Prompt filename is not set"
    bucket = _prompt_bucket()
    s3_client = get_prompt_rdf_s3_client()

    response = await s3_client.put_object(
        Bucket=bucket,
        Key=prompt_filename,
        Body=content.encode("utf-8"),
        ContentType="text/plain; charset=utf-8",
        Metadata=metadata or {},
    )

    version_id = response.get("VersionId")
    if not version_id:
        raise ValueError(
            f"Version ID not returned when writing {prompt_filename}. Ensure that "
            f"versioning is enabled on the {bucket} bucket."
        )
    return version_id


async def download_prompt(
    prompt_filename: str, version_id: Optional[str] = None
) -> PromptObject:
    """
    Read a prompt file from an AWS S3 bucket, with whatever provenance was
    stamped on it at upload time.

    :param prompt_filename: The name of the prompt file to download.
    :param version_id: Optional version ID to download a specific version
    :return: The text, the version ID actually read, and the object's metadata
        (empty for objects uploaded before stamping, or out of band).
    """
    assert prompt_filename, "Prompt filename is not set"
    bucket = _prompt_bucket()
    s3_client = get_prompt_rdf_s3_client()

    if version_id:
        obj = await s3_client.get_object(
            Bucket=bucket, Key=prompt_filename, VersionId=version_id
        )
    else:
        obj = await s3_client.get_object(Bucket=bucket, Key=prompt_filename)

    # check if the object does not exist
    if "Body" not in obj:
        raise ValueError(
            f"Object {prompt_filename}:{version_id} not found in bucket {bucket}. Please check the bucket and filename."
        )

    actual_version_id = obj.get("VersionId")
    if not actual_version_id:
        raise ValueError(
            f"Version ID not found for the file: {prompt_filename}. Ensure that versioning is enabled on the {bucket} bucket."
        )

    body_bytes = await obj["Body"].read()
    # S3 lower-cases metadata keys in transit; normalise so lookups by the
    # constants above work regardless of how the object was written.
    metadata = {
        key.lower(): value for key, value in (obj.get("Metadata") or {}).items()
    }
    return PromptObject(
        text=body_bytes.decode("utf-8"),
        version_id=actual_version_id,
        metadata=metadata,
    )
