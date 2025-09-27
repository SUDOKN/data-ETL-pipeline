import os
from typing import Optional
from data_etl_app.dependencies.aws_clients import get_prompt_rdf_s3_client

RDF_BUCKET = os.getenv("RDF_BUCKET")
RDF_FILENAME = os.getenv("RDF_FILENAME")
if not RDF_BUCKET or not RDF_FILENAME:
    raise ValueError("RDF bucket or filename is not set. Please check your .env file.")


async def does_ontology_version_exist(version_id: str) -> bool:
    """Checks if a specific version of the ontology RDF file exists in the S3 bucket.
    :param version_id: Version ID to check for a specific version of the file.
    :return: True if the file version exists, False otherwise.
    """
    assert RDF_BUCKET and RDF_FILENAME, "RDF bucket or filename is not set"
    s3_client = get_prompt_rdf_s3_client()
    from botocore.exceptions import ClientError

    try:
        await s3_client.head_object(
            Bucket=RDF_BUCKET, Key=RDF_FILENAME, VersionId=version_id
        )
        return True
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code in ("404", "403", "NoSuchKey", "NoSuchVersion"):
            return False
        raise  # Re-raise other exceptions


async def download_ontology_rdf(version_id: Optional[str]) -> tuple[str, str]:
    """
    Read a file from an AWS S3 bucket and return its content as a string.
    """
    assert RDF_BUCKET and RDF_FILENAME, "RDF bucket or filename is not set"
    s3_client = get_prompt_rdf_s3_client()

    if version_id:
        obj = await s3_client.get_object(
            Bucket=RDF_BUCKET, Key=RDF_FILENAME, VersionId=version_id
        )
    else:
        obj = await s3_client.get_object(Bucket=RDF_BUCKET, Key=RDF_FILENAME)

    # check if the object does not exist
    if "Body" not in obj:
        raise ValueError(
            f"Object {RDF_FILENAME}:{version_id} not found in bucket {RDF_BUCKET}. Please check the bucket and filename."
        )

    actual_version_id = obj.get("VersionId")
    if not actual_version_id:
        raise ValueError(
            f"Version ID not found for the file: {RDF_FILENAME}. Ensure that versioning is enabled on the {RDF_BUCKET} bucket."
        )

    body_bytes = await obj["Body"].read()
    return body_bytes.decode("utf-8"), actual_version_id
