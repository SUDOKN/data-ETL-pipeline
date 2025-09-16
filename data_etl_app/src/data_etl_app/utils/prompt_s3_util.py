import os
import boto3
from typing import Optional

USER_ACCESS_KEY_ID = os.getenv("AWS_RDF_AND_PROMPT_USER_ACCESS_KEY_ID")
USER_SECRET_ACCESS_KEY = os.getenv("AWS_RDF_AND_PROMPT_USER_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")

if not USER_ACCESS_KEY_ID or not USER_SECRET_ACCESS_KEY or not AWS_REGION:
    raise ValueError(
        "AWS credentials or region are not set. Please set them in your .env file."
    )

PROMPT_BUCKET = os.getenv("PROMPT_BUCKET")
if not PROMPT_BUCKET:
    raise ValueError("Prompt bucket is not set. Please check your .env file.")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=USER_ACCESS_KEY_ID,
    aws_secret_access_key=USER_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)


def does_prompt_version_exist(s3_client, prompt_filename: str, version_id: str) -> bool:
    """Checks if a specific version of the prompt file exists in the S3 bucket.
    :param s3_client: A boto3 S3 client to use for the check.
    :param version_id: Version ID to check for a specific version of the file.
    :return: True if the file version exists, False otherwise.
    """
    assert (
        PROMPT_BUCKET is not None and prompt_filename is not None
    ), "Prompt bucket or filename is not set"
    try:
        s3_client.head_object(
            Bucket=PROMPT_BUCKET, Key=prompt_filename, VersionId=version_id
        )
        return True
    except s3_client.exceptions.ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code in [
            "404",
            "403",
        ]:  # 404 = not found, 403 = forbidden (often means not found for versions)
            return False
        raise  # Re-raise other exceptions


def download_prompt(
    prompt_filename: str, version_id: Optional[str] = None
) -> tuple[str, str]:
    """
    Read a prompt file from an AWS S3 bucket and return its content as a string.
    Credentials are loaded from the .env file.

    :param version_id: Optional version ID to download a specific version
    :return: Tuple of (prompt_content, version_id)
    """
    assert (
        PROMPT_BUCKET is not None and prompt_filename is not None
    ), "Prompt bucket or filename is not set"

    if version_id:
        obj = s3_client.get_object(
            Bucket=PROMPT_BUCKET, Key=prompt_filename, VersionId=version_id
        )
    else:
        obj = s3_client.get_object(Bucket=PROMPT_BUCKET, Key=prompt_filename)

    # check if the object does not exist
    if "Body" not in obj:
        raise ValueError(
            f"Object {prompt_filename}:{version_id} not found in bucket {PROMPT_BUCKET}. Please check the bucket and filename."
        )

    version_id = obj.get("VersionId")
    if not version_id:
        raise ValueError(
            f"Version ID not found for the file: {prompt_filename}. Ensure that versioning is enabled on the {PROMPT_BUCKET} bucket."
        )

    return obj["Body"].read().decode("utf-8"), version_id
