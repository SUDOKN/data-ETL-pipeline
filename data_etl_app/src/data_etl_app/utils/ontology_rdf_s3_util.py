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

RDF_BUCKET = os.getenv("RDF_BUCKET")
RDF_FILENAME = os.getenv("RDF_FILENAME")
if not RDF_BUCKET or not RDF_FILENAME:
    raise ValueError("RDF bucket or filename is not set. Please check your .env file.")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=USER_ACCESS_KEY_ID,
    aws_secret_access_key=USER_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)


def does_ontology_version_exist(s3_client, version_id: str) -> bool:
    """Checks if a specific version of the ontology RDF file exists in the S3 bucket.
    :param s3_client: A boto3 S3 client to use for the check.
    :param version_id: Version ID to check for a specific version of the file.
    :return: True if the file version exists, False otherwise.
    """
    assert (
        RDF_BUCKET is not None and RDF_FILENAME is not None
    ), "RDF bucket or filename is not set"
    try:
        s3_client.head_object(Bucket=RDF_BUCKET, Key=RDF_FILENAME, VersionId=version_id)
        return True
    except s3_client.exceptions.ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code in [
            "404",
            "403",
        ]:  # 404 = not found, 403 = forbidden (often means not found for versions)
            return False
        raise  # Re-raise other exceptions


def download_ontology_rdf(version_id: Optional[str]) -> tuple[str, str]:
    """
    Read a file from an AWS S3 bucket and return its content as a string.
    Credentials are loaded from the .env file.
    """
    assert (
        RDF_BUCKET is not None and RDF_FILENAME is not None
    ), "RDF bucket or filename is not set"
    if version_id:
        obj = s3_client.get_object(
            Bucket=RDF_BUCKET, Key=RDF_FILENAME, VersionId=version_id
        )
    else:
        obj = s3_client.get_object(Bucket=RDF_BUCKET, Key=RDF_FILENAME)

    # check if the object does not exist
    if "Body" not in obj:
        raise ValueError(
            f"Object {RDF_FILENAME}:{version_id} not found in bucket {RDF_BUCKET}. Please check the bucket and filename."
        )

    version_id = obj.get("VersionId")
    if not version_id:
        raise ValueError(
            f"Version ID not found for the file: {RDF_FILENAME}. Ensure that versioning is enabled on the {RDF_BUCKET} bucket."
        )

    return obj["Body"].read().decode("utf-8"), version_id
