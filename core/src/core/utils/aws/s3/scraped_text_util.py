import os
import logging
import tldextract
from datetime import datetime
from typing import Optional

from core.dependencies.aws_clients import get_scraped_bucket_s3_client


SCRAPED_TEXT_BUCKET = os.getenv("SCRAPED_TEXT_BUCKET")
if not SCRAPED_TEXT_BUCKET:
    raise ValueError("SCRAPED_TEXT_BUCKET is not set. Please check your .env file.")


logger = logging.getLogger(__name__)


def get_file_name_from_mfg_etld(etld1: str) -> str:
    """Generates a file name for the scraped text based on the manufacturer etld1."""
    # check if the input is not exactly a etld1 (i.e., effective top level domain)

    extracted = tldextract.extract(etld1)  # because the etld1 passed may not exactly be
    if not (extracted.domain and extracted.suffix):
        raise ValueError("Invalid eTLD+1 format")

    # Reconstruct eTLD+1
    reconstructed_etld1 = (f"{extracted.domain}.{extracted.suffix}").lower()

    if reconstructed_etld1 != etld1:
        raise ValueError(
            f"etld1:{etld1} passed is inconsistent with reconstructed_etld1:{reconstructed_etld1}"
        )

    return f"{etld1}.txt"


async def get_scraped_text_file_exist_last_modified_on(
    file_name: str, version_id: str
) -> datetime | None:
    """Checks if a file exists in the S3 bucket.
    :param s3_client: An S3 client to use for the check.
    :param file_name: The name of the file to check in S3.
    :param version_id: Optional version ID to check for a specific version of the file.
    :return: Last modified date if the file exists, None otherwise.
    """
    s3_client = get_scraped_bucket_s3_client()
    assert SCRAPED_TEXT_BUCKET is not None, "SCRAPED_TEXT_BUCKET is None"
    logger.info(
        f"Checking existence of file: {file_name} in bucket: {SCRAPED_TEXT_BUCKET}"
    )
    if not version_id:
        return None

    try:
        response = await s3_client.head_object(
            Bucket=SCRAPED_TEXT_BUCKET, Key=file_name, VersionId=version_id
        )
        last_modified = response["LastModified"]
        logger.info(
            f"File {file_name} with version ID {version_id} exists in bucket {SCRAPED_TEXT_BUCKET}. Last modified: {last_modified}"
        )
        return last_modified
    except s3_client.exceptions.ClientError as e:
        error_code = e.response["Error"]["Code"]
        error_message = e.response["Error"]["Message"]
        http_status = e.response["ResponseMetadata"]["HTTPStatusCode"]
        logger.error(
            f"S3 ClientError checking file existence - "
            f"File: {file_name}, VersionId: {version_id}, "
            f"Error Code: {error_code}, HTTP Status: {http_status}, "
            f"Message: {error_message}, Full Response: {e.response}"
        )
        if error_code in ["404", "400"]:
            return None
        raise  # Re-raise other exceptions
    except Exception as e:
        logger.error(
            f"Unexpected error checking file existence - "
            f"File: {file_name}, VersionId: {version_id}, "
            f"Error Type: {type(e).__name__}, Error: {str(e)}"
        )
        raise


async def upload_scraped_text_to_s3(
    file_content: str, file_name: str, tags: dict[str, str]
) -> tuple[str, str]:
    """
    Uploads scraped text content to an S3 bucket using centralized client and returns the S3 URL.

    :param file_content: The content of the file to upload.
    :param file_name: The name of the file to be saved in S3.
    :param tags: Tags to apply to the S3 object. (ideally at least store the batch)
    :return: The S3 URL of the uploaded file.
    """
    s3_client = get_scraped_bucket_s3_client()
    assert SCRAPED_TEXT_BUCKET is not None, "SCRAPED_TEXT_BUCKET is None"
    tagging_string = "&".join([f"{k}={v}" for k, v in tags.items()])
    response = await s3_client.put_object(
        Bucket=SCRAPED_TEXT_BUCKET,
        Key=file_name,
        Body=file_content.encode("utf-8"),
        Tagging=tagging_string,
    )
    return response["VersionId"], f"s3://{SCRAPED_TEXT_BUCKET}/{file_name}"


async def get_latest_version_id_by_mfg_etld(
    etld1: str,
) -> Optional[str]:
    """
    Gets the latest version ID of a file based on the manufacturer etld1.

    :param etld1: The eTLD+1 of the manufacturer to get the latest version ID for.
    :return: The latest version ID of the file, or None if the file doesn't exist.
    """
    file_name = get_file_name_from_mfg_etld(etld1)
    return await get_latest_version_id_by_filename(file_name)


async def get_latest_version_id_by_filename(
    file_name: str,
) -> Optional[str]:
    """
    Gets the latest version ID of a file in the scraped text bucket.

    :param file_name: The name of the file to get the latest version ID for.
    :return: The latest version ID of the file, or None if the file doesn't exist.
    """
    s3_client = get_scraped_bucket_s3_client()
    assert SCRAPED_TEXT_BUCKET is not None, "SCRAPED_TEXT_BUCKET is None"
    logger.info(f"Getting latest version ID for file: {file_name}")

    try:
        # Use head_object to get the latest version info efficiently
        response = await s3_client.head_object(
            Bucket=SCRAPED_TEXT_BUCKET, Key=file_name
        )
        version_id = response.get("VersionId")

        if not version_id:
            logger.warning(
                f"Version ID not found for file: {file_name}. Ensure that versioning is enabled on the {SCRAPED_TEXT_BUCKET} bucket."
            )
            return None

        logger.info(f"Latest version ID for {file_name}: {version_id}")
        return version_id

    except s3_client.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            logger.info(f"File {file_name} not found in bucket {SCRAPED_TEXT_BUCKET}")
            return None
        raise  # Re-raise other exceptions


async def download_scraped_text_from_s3_by_mfg_etld1(
    etld1: str,
    version_id: str,
) -> tuple[str, str]:
    """
    Downloads a file from S3 based on the manufacturer etld1 and returns its content as a string.

    :param etld1: The eTLD+1 of the manufacturer to download the corresponding file from S3.
    :return: The content of the downloaded file as a string.
    """
    file_name = get_file_name_from_mfg_etld(etld1)
    return await download_scraped_text_from_s3_by_filename(file_name, version_id)


async def download_scraped_text_from_s3_by_filename(
    file_name: str,
    version_id: str,
) -> tuple[str, str]:
    """
    Downloads a file from S3 and returns its content as a string.

    :param file_name: The name of the file to download from S3.
    :return: tuple [the content of the downloaded file as a string, its version ID]
    """
    s3_client = get_scraped_bucket_s3_client()
    assert SCRAPED_TEXT_BUCKET is not None, "SCRAPED_TEXT_BUCKET is None"
    # Use centralized s3_client to download
    if version_id:
        logger.info(
            f"Attempting to download `{file_name}` with version ID `{version_id}` from S3"
        )
        obj = await s3_client.get_object(
            Bucket=SCRAPED_TEXT_BUCKET, Key=file_name, VersionId=version_id
        )
        logger.info(f"Streaming `{file_name}` from S3")
        async with obj["Body"] as stream:
            content = await stream.read()
    else:
        logger.info(
            f"Attempting to download `{file_name}` from S3 without specifying version ID"
        )
        obj = await s3_client.get_object(Bucket=SCRAPED_TEXT_BUCKET, Key=file_name)
        async with obj["Body"] as stream:
            content = await stream.read()

        version_id = obj.get(
            "VersionId"
        )  # This will be None if versioning is off or suspended
        if not version_id:
            raise ValueError(
                f"Version ID not found for the file: {file_name}. Ensure that versioning is enabled on the {SCRAPED_TEXT_BUCKET} bucket."
            )
        logger.info(f"Downloaded file {file_name} with version ID: {version_id}")
    return content.decode("utf-8"), version_id


async def get_scraped_text_object_tags_by_mfg_etld1(
    mfg_etld1: str, version_id: str
) -> dict[str, str]:
    """
    Gets the tags for a specific object version in the scraped text bucket based on the manufacturer etld1.

    :param mfg_etld1: The eTLD+1 of the manufacturer to get the corresponding file tags from S3.
    :param version_id: The version ID of the object.
    :return: Dictionary of tags with tag keys as dictionary keys and tag values as dictionary values.
    """
    file_name = get_file_name_from_mfg_etld(mfg_etld1)
    return await get_scraped_text_object_tags_by_filename(file_name, version_id)


async def get_scraped_text_object_tags_by_filename(
    file_name: str, version_id: str
) -> dict[str, str]:
    """
    Gets the tags for a specific object version in the scraped text bucket.

    :param file_name: The name of the file in S3.
    :param version_id: The version ID of the object.
    :return: Dictionary of tags with tag keys as dictionary keys and tag values as dictionary values.
    """
    s3_client = get_scraped_bucket_s3_client()
    assert SCRAPED_TEXT_BUCKET is not None, "SCRAPED_TEXT_BUCKET is None"

    tags_response = await s3_client.get_object_tagging(
        Bucket=SCRAPED_TEXT_BUCKET, Key=file_name, VersionId=version_id
    )
    return {tag["Key"]: tag["Value"] for tag in tags_response.get("TagSet", [])}


async def iterate_scraped_text_objects_and_versions(
    prefix: str = "", include_tags: bool = False
):
    """
    Generator that iterates over each object and its versions in the scraped text bucket.

    :param prefix: Optional prefix to filter objects (e.g., to limit to specific file patterns).
    :param include_tags: If True, fetches and includes custom tags for each object version.
    :yield: Dictionary containing object metadata with keys:
            - 'Key': The object key (file name)
            - 'VersionId': The version ID of the object
            - 'LastModified': The last modified timestamp
            - 'Size': The size of the object in bytes
            - 'IsLatest': Boolean indicating if this is the latest version
            - 'StorageClass': The storage class of the object
            - 'ETag': The entity tag of the object
            - 'Type': Either "Version" or "DeleteMarker"
            - 'Tags': Dictionary of custom tags (only if include_tags=True)
    """
    s3_client = get_scraped_bucket_s3_client()
    assert SCRAPED_TEXT_BUCKET is not None, "SCRAPED_TEXT_BUCKET is None"
    logger.info(
        f"Iterating over objects in bucket: {SCRAPED_TEXT_BUCKET} with prefix: '{prefix}', include_tags: {include_tags}"
    )

    paginator = s3_client.get_paginator("list_object_versions")

    async for page in paginator.paginate(Bucket=SCRAPED_TEXT_BUCKET, Prefix=prefix):
        # Process regular versions
        for version in page.get("Versions", []):
            obj_data = {
                "Key": version["Key"],
                "VersionId": version["VersionId"],
                "LastModified": version["LastModified"],
                "Size": version["Size"],
                "IsLatest": version["IsLatest"],
                "StorageClass": version.get("StorageClass", "STANDARD"),
                "ETag": version["ETag"],
                "Type": "Version",
            }

            # Fetch tags if requested
            if include_tags:
                obj_data["Tags"] = await get_scraped_text_object_tags_by_filename(
                    version["Key"], version["VersionId"]
                )

            yield obj_data

        # Process delete markers (if any)
        for delete_marker in page.get("DeleteMarkers", []):
            obj_data = {
                "Key": delete_marker["Key"],
                "VersionId": delete_marker["VersionId"],
                "LastModified": delete_marker["LastModified"],
                "Size": 0,  # Delete markers have no size
                "IsLatest": delete_marker["IsLatest"],
                "StorageClass": None,
                "ETag": None,
                "Type": "DeleteMarker",
            }

            # Delete markers don't have tags
            if include_tags:
                obj_data["Tags"] = {}

            yield obj_data


async def get_all_scraped_text_objects_summary(prefix: str = "") -> dict:
    """
    Gets a summary of all objects and versions in the scraped text bucket.

    :param prefix: Optional prefix to filter objects.
    :return: Dictionary containing summary statistics:
             - 'total_objects': Total number of unique objects (keys)
             - 'total_versions': Total number of versions across all objects
             - 'total_size_bytes': Total size of all versions in bytes
             - 'objects_with_multiple_versions': Number of objects that have multiple versions
             - 'delete_markers': Number of delete markers
    """
    assert SCRAPED_TEXT_BUCKET is not None, "SCRAPED_TEXT_BUCKET is None"

    objects_summary = {}
    total_size = 0
    delete_marker_count = 0

    async for obj_version in iterate_scraped_text_objects_and_versions(prefix):
        key = obj_version["Key"]

        if obj_version["Type"] == "DeleteMarker":
            delete_marker_count += 1
        else:
            total_size += obj_version["Size"]

        if key not in objects_summary:
            objects_summary[key] = {"version_count": 0, "latest_size": 0}

        objects_summary[key]["version_count"] += 1

        if obj_version["IsLatest"] and obj_version["Type"] == "Version":
            objects_summary[key]["latest_size"] = obj_version["Size"]

    objects_with_multiple_versions = sum(
        1 for obj_info in objects_summary.values() if obj_info["version_count"] > 1
    )

    return {
        "total_objects": len(objects_summary),
        "total_versions": sum(
            obj_info["version_count"] for obj_info in objects_summary.values()
        ),
        "total_size_bytes": total_size,
        "objects_with_multiple_versions": objects_with_multiple_versions,
        "delete_markers": delete_marker_count,
    }


async def delete_scraped_text_from_s3_by_etld1(
    etld1: str, version_id: Optional[str] = None
) -> None:
    """
    Deletes a file from S3 based on the manufacturer etld1. If version_id is None, deletes all versions of the file.

    :param etld1: The eTLD+1 of the manufacturer to delete the corresponding file from S3.
    :param version_id: Optional version ID to delete a specific version of the file. If None, deletes all versions.
    """
    file_name = get_file_name_from_mfg_etld(etld1)
    await delete_scraped_text_from_s3_by_filename(file_name, version_id)


async def delete_scraped_text_from_s3_by_filename(
    file_name: str, version_id: Optional[str] = None
) -> None:
    """
    Deletes a file from S3. If version_id is None, deletes all versions of the file.

    :param file_name: The name of the file to delete from S3.
    :param version_id: Optional version ID to delete a specific version of the file. If None, deletes all versions.
    """
    s3_client = get_scraped_bucket_s3_client()
    assert SCRAPED_TEXT_BUCKET is not None, "SCRAPED_TEXT_BUCKET is None"
    if version_id:
        logger.info(f"Attempting to delete {file_name} (version: {version_id}) from S3")
        await s3_client.delete_object(
            Bucket=SCRAPED_TEXT_BUCKET, Key=file_name, VersionId=version_id
        )
        logger.info(f"Successfully deleted {file_name} (version: {version_id}) from S3")
    else:
        # Delete all versions of the object
        logger.info(f"Attempting to delete all versions of {file_name} from S3")
        paginator = s3_client.get_paginator("list_object_versions")
        async for page in paginator.paginate(
            Bucket=SCRAPED_TEXT_BUCKET, Prefix=file_name
        ):
            versions = page.get("Versions", []) + page.get("DeleteMarkers", [])
            for v in versions:
                if v["Key"] == file_name:
                    await s3_client.delete_object(
                        Bucket=SCRAPED_TEXT_BUCKET,
                        Key=file_name,
                        VersionId=v["VersionId"],
                    )
                    logger.info(
                        f"Successfully deleted {file_name} (version: {v['VersionId']}) from S3"
                    )
        logger.info(f"Successfully deleted all versions of {file_name} from S3")
