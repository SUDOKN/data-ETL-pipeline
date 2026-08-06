from typing import Optional

from pure_utils.env_util import require_env

from infra.utils.aws.clients import (
    get_prompt_rdf_s3_client,
)
from core.models.ontology import Ontology


def _rdf_bucket_and_filename() -> tuple[str, str]:
    return require_env("RDF_BUCKET"), require_env("RDF_FILENAME")


async def does_ontology_version_exist(version_id: str) -> bool:
    """Checks if a specific version of the ontology RDF file exists in the S3 bucket.
    :param version_id: Version ID to check for a specific version of the file.
    :return: True if the file version exists, False otherwise.
    """
    bucket, filename = _rdf_bucket_and_filename()
    s3_client = get_prompt_rdf_s3_client()
    from botocore.exceptions import ClientError

    try:
        await s3_client.head_object(Bucket=bucket, Key=filename, VersionId=version_id)
        return True
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code in ("404", "403", "NoSuchKey", "NoSuchVersion"):
            return False
        raise  # Re-raise other exceptions


async def download_ontology_rdf(version_id: Optional[str]) -> Ontology:
    """
    Read a file from an AWS S3 bucket and return its content as a string.
    """
    bucket, filename = _rdf_bucket_and_filename()
    s3_client = get_prompt_rdf_s3_client()

    if version_id:
        obj = await s3_client.get_object(
            Bucket=bucket, Key=filename, VersionId=version_id
        )
    else:
        obj = await s3_client.get_object(Bucket=bucket, Key=filename)

    # check if the object does not exist
    if "Body" not in obj:
        raise ValueError(
            f"Object {filename}:{version_id} not found in bucket {bucket}. Please check the bucket and filename."
        )

    actual_version_id = obj.get("VersionId")
    if not actual_version_id:
        raise ValueError(
            f"Version ID not found for the file: {filename}. Ensure that versioning is enabled on the {bucket} bucket."
        )

    body_bytes = await obj["Body"].read()
    return Ontology(s3_version_id=actual_version_id, rdf=body_bytes.decode("utf-8"))
