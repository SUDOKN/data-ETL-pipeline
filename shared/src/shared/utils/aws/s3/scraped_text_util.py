import os
from typing import Optional

SCRAPED_TEXT_BUCKET = os.getenv("SCRAPED_TEXT_BUCKET")
if not SCRAPED_TEXT_BUCKET:
    raise ValueError("SCRAPED_TEXT_BUCKET is not set. Please check your .env file.")


def get_file_name_from_mfg_url(url: str) -> str:
    """Generates a file name for the scraped text based on the manufacturer URL."""
    return f"{url}.txt"


async def does_scraped_text_file_exist(
    s3_client, file_name: str, version_id: Optional[str] = None
) -> bool:
    """Checks if a file exists in the S3 bucket.
    :param s3_client: An aiobotocore or regular S3 client to use for the check.
    :param file_name: The name of the file to check in S3.
    :param version_id: Optional version ID to check for a specific version of the file.
    :return: True if the file exists, False otherwise.
    """
    assert SCRAPED_TEXT_BUCKET is not None, "SCRAPED_TEXT_BUCKET is None"
    try:
        if version_id:
            await s3_client.head_object(
                Bucket=SCRAPED_TEXT_BUCKET, Key=file_name, VersionId=version_id
            )
        else:
            await s3_client.head_object(Bucket=SCRAPED_TEXT_BUCKET, Key=file_name)
        return True
    except s3_client.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise  # Re-raise other exceptions


async def upload_scraped_text_to_s3(
    s3_client, file_content: str, file_name: str, tags: dict[str, str]
) -> tuple[str, str]:
    """
    Uploads scraped text content to an S3 bucket using an existing client and returns the S3 URL.

    :param s3_client: An aiobotocore or regular S3 client to use for the upload.
    :param file_content: The content of the file to upload.
    :param file_name: The name of the file to be saved in S3.
    :param tags: Tags to apply to the S3 object. (ideally at least store the batch)
    :return: The S3 URL of the uploaded file.
    """
    assert SCRAPED_TEXT_BUCKET is not None, "SCRAPED_TEXT_BUCKET is None"
    tagging_string = "&".join([f"{k}={v}" for k, v in tags.items()])
    response = await s3_client.put_object(
        Bucket=SCRAPED_TEXT_BUCKET,
        Key=file_name,
        Body=file_content.encode("utf-8"),
        Tagging=tagging_string,
    )
    return response["VersionId"], f"s3://{SCRAPED_TEXT_BUCKET}/{file_name}"


async def download_scraped_text_from_s3_by_filename(
    s3_client,
    file_name: str,
) -> tuple[str, str]:
    """
    Downloads a file from S3 and returns its content as a string.

    :param file_name: The name of the file to download from S3.
    :return: The content of the downloaded file as a string.
    """
    assert SCRAPED_TEXT_BUCKET is not None, "SCRAPED_TEXT_BUCKET is None"
    # Use provided s3_client to download
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
    return content.decode("utf-8"), version_id


async def delete_scraped_text_from_s3(
    s3_client, file_name: str, version_id: Optional[str] = None
) -> None:
    """
    Deletes a file from S3. If version_id is None, deletes all versions of the file.

    :param s3_client: An aiobotocore or regular S3 client to use for the deletion.
    :param file_name: The name of the file to delete from S3.
    :param version_id: Optional version ID to delete a specific version of the file. If None, deletes all versions.
    """
    assert SCRAPED_TEXT_BUCKET is not None, "SCRAPED_TEXT_BUCKET is None"
    if version_id:
        await s3_client.delete_object(
            Bucket=SCRAPED_TEXT_BUCKET, Key=file_name, VersionId=version_id
        )
    else:
        # Delete all versions of the object
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
