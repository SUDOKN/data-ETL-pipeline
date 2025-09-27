import os
from typing import Optional
from data_etl_app.dependencies.aws_clients import get_prompt_rdf_s3_client

PROMPT_BUCKET = os.getenv("PROMPT_BUCKET")
if not PROMPT_BUCKET:
    raise ValueError("Prompt bucket is not set. Please check your .env file.")


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
    assert PROMPT_BUCKET and prompt_filename, "Prompt bucket or filename is not set"
    s3_client = get_prompt_rdf_s3_client()
    from botocore.exceptions import ClientError

    try:
        await s3_client.head_object(
            Bucket=PROMPT_BUCKET, Key=prompt_filename, VersionId=version_id
        )
        return True
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code in ("404", "403", "NoSuchKey", "NoSuchVersion"):
            return False
        raise  # Re-raise other exceptions


async def download_prompt(
    prompt_filename: str, version_id: Optional[str] = None
) -> tuple[str, str]:
    """
    Read a prompt file from an AWS S3 bucket and return its content as a string.

    :param prompt_filename: The name of the prompt file to download.
    :param version_id: Optional version ID to download a specific version
    :return: Tuple of (prompt_content, version_id)
    """
    assert PROMPT_BUCKET and prompt_filename, "Prompt bucket or filename is not set"
    s3_client = get_prompt_rdf_s3_client()

    if version_id:
        obj = await s3_client.get_object(
            Bucket=PROMPT_BUCKET, Key=prompt_filename, VersionId=version_id
        )
    else:
        obj = await s3_client.get_object(Bucket=PROMPT_BUCKET, Key=prompt_filename)

    # check if the object does not exist
    if "Body" not in obj:
        raise ValueError(
            f"Object {prompt_filename}:{version_id} not found in bucket {PROMPT_BUCKET}. Please check the bucket and filename."
        )

    actual_version_id = obj.get("VersionId")
    if not actual_version_id:
        raise ValueError(
            f"Version ID not found for the file: {prompt_filename}. Ensure that versioning is enabled on the {PROMPT_BUCKET} bucket."
        )

    body_bytes = await obj["Body"].read()
    return body_bytes.decode("utf-8"), actual_version_id
