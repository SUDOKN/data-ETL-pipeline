pytest_plugins = ["pytest_asyncio"]
import uuid
import pytest
import pytest_asyncio
import aiobotocore.session
import os
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

# Load environment variables first
from core.dependencies.load_core_env import load_core_env

load_core_env()

from core.utils.aws.s3.scraped_text_util import (
    get_file_name_from_mfg_etld,
    get_scraped_text_file_exist_last_modified_on,
    upload_scraped_text_to_s3,
    download_scraped_text_from_s3_by_filename,
    delete_scraped_text_from_s3_by_filename,
    _get_scraped_text_object_tags_by_filename,
    iterate_scraped_text_objects_and_versions,
    SCRAPED_TEXT_BUCKET,
)

from core.dependencies.aws_clients import (
    initialize_core_aws_clients,
    cleanup_core_aws_clients,
)


def make_s3_client(session):
    """Create an S3 client for testing purposes."""
    aws_region = os.getenv("AWS_REGION", "us-east-1")
    aws_access_key_id = os.getenv("AWS_SCRAPED_BUCKET_USER_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SCRAPED_BUCKET_USER_SECRET_ACCESS_KEY")

    return session.create_client(
        "s3",
        region_name=aws_region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )


# Test helper functions that take an S3 client parameter
async def upload_scraped_text_to_s3_with_client(
    s3_client, file_content: str, file_name: str, tags: dict[str, str]
) -> tuple[str, str]:
    """Test helper that takes an S3 client and mimics the real function."""
    with patch(
        "core.utils.aws.s3.scraped_text_util.get_scraped_bucket_s3_client",
        return_value=s3_client,
    ):
        return await upload_scraped_text_to_s3(file_content, file_name, tags)


async def get_scraped_text_file_exist_last_modified_on_with_client(
    s3_client, file_name: str, version_id: str
) -> datetime | None:
    """Test helper that takes an S3 client and mimics the real function."""
    with patch(
        "core.utils.aws.s3.scraped_text_util.get_scraped_bucket_s3_client",
        return_value=s3_client,
    ):
        return await get_scraped_text_file_exist_last_modified_on(file_name, version_id)


async def download_scraped_text_from_s3_by_filename_with_client(
    s3_client, file_name: str, version_id: str
) -> tuple[str, str]:
    """Test helper that takes an S3 client and mimics the real function."""
    with patch(
        "core.utils.aws.s3.scraped_text_util.get_scraped_bucket_s3_client",
        return_value=s3_client,
    ):
        return await download_scraped_text_from_s3_by_filename(file_name, version_id)


async def delete_scraped_text_from_s3_by_filename_with_client(
    s3_client, file_name: str, version_id: str | None = None
) -> None:
    """Test helper that takes an S3 client and mimics the real function."""
    with patch(
        "core.utils.aws.s3.scraped_text_util.get_scraped_bucket_s3_client",
        return_value=s3_client,
    ):
        return await delete_scraped_text_from_s3_by_filename(file_name, version_id)


async def _get_scraped_text_object_tags_by_filename_with_client(
    s3_client, file_name: str, version_id: str
) -> dict[str, str]:
    """Test helper that takes an S3 client and mimics the real function."""
    with patch(
        "core.utils.aws.s3.scraped_text_util.get_scraped_bucket_s3_client",
        return_value=s3_client,
    ):
        return await _get_scraped_text_object_tags_by_filename(file_name, version_id)


async def iterate_scraped_text_objects_and_versions_with_client(
    s3_client, prefix: str = "", include_tags: bool = False
):
    """Test helper that takes an S3 client and mimics the real function."""
    with patch(
        "core.utils.aws.s3.scraped_text_util.get_scraped_bucket_s3_client",
        return_value=s3_client,
    ):
        async for item in iterate_scraped_text_objects_and_versions(
            prefix, include_tags
        ):
            yield item


class TestScrapedTextUtilS3Integration:
    @pytest_asyncio.fixture
    async def s3_client(self):
        # Initialize AWS clients first
        await initialize_core_aws_clients()
        try:
            session = aiobotocore.session.get_session()
            async with make_s3_client(session) as client:
                yield client
        finally:
            # Cleanup AWS clients
            await cleanup_core_aws_clients()

    @pytest.mark.asyncio
    async def test_upload_download_delete_cycle(self, s3_client):
        # Generate a unique file name using a proper eTLD+1 format
        # The function expects an eTLD+1 (like "example.com"), not a full URL
        etld1 = f"test-{uuid.uuid4().hex[:8]}.com"  # Generate unique domain like "test-a1b2c3d4.com"
        file_name = get_file_name_from_mfg_etld(etld1)
        file_content = "Integration test content"
        tags = {"test": "integration"}

        # Upload
        version_id, s3_url = await upload_scraped_text_to_s3_with_client(
            s3_client, file_content, file_name, tags
        )
        print(f"Uploaded file to S3: {s3_url} with version ID: {version_id}")
        assert version_id
        assert version_id != "null"
        assert s3_url.endswith(file_name)

        # Existence check
        exists = await get_scraped_text_file_exist_last_modified_on_with_client(
            s3_client, file_name, version_id
        )
        assert exists is not None  # Should return a datetime, not True/False

        # Download
        downloaded_content, downloaded_version_id = (
            await download_scraped_text_from_s3_by_filename_with_client(
                s3_client, file_name, version_id
            )
        )
        assert downloaded_content == file_content
        assert downloaded_version_id == version_id

        # Delete (handle permission issues gracefully)
        try:
            await delete_scraped_text_from_s3_by_filename_with_client(
                s3_client, file_name, version_id
            )
            print("✅ File successfully deleted from S3")

            # Confirm deletion (should not exist)
            exists_after = (
                await get_scraped_text_file_exist_last_modified_on_with_client(
                    s3_client, file_name, version_id
                )
            )
            assert exists_after is None  # Should return None when file doesn't exist

        except Exception as e:
            if "AccessDenied" in str(e) and "DeleteObjectVersion" in str(e):
                print(f"⚠️  Warning: No permission for versioned delete: {e}")
                print(f"Test file remains in S3: {s3_url}")
                # Don't fail the test due to cleanup permission issues
                pass
            else:
                # Re-raise other types of errors
                raise


class TestGetScrapedTextObjectTags:
    @pytest.mark.asyncio
    async def test_get_scraped_text_object_tags_success(self):
        """Test successful tag retrieval."""
        # Mock S3 client
        mock_s3_client = AsyncMock()
        mock_s3_client.get_object_tagging.return_value = {
            "TagSet": [
                {"Key": "urls_scraped", "Value": "5"},
                {"Key": "batch_title", "Value": "test_batch_2024"},
                {"Key": "environment", "Value": "production"},
            ]
        }

        file_name = "test.com.txt"
        version_id = "test-version-123"

        result = await _get_scraped_text_object_tags_by_filename_with_client(
            mock_s3_client, file_name, version_id
        )

        # Verify the call was made with correct parameters
        mock_s3_client.get_object_tagging.assert_called_once()
        call_args = mock_s3_client.get_object_tagging.call_args
        assert call_args.kwargs["Key"] == file_name
        assert call_args.kwargs["VersionId"] == version_id

        # Verify the returned tags
        expected_tags = {
            "urls_scraped": "5",
            "batch_title": "test_batch_2024",
            "environment": "production",
        }
        assert result == expected_tags

    @pytest.mark.asyncio
    async def test_get_scraped_text_object_tags_empty_tagset(self):
        """Test tag retrieval when TagSet is empty."""
        mock_s3_client = AsyncMock()
        mock_s3_client.get_object_tagging.return_value = {"TagSet": []}

        result = await _get_scraped_text_object_tags_by_filename_with_client(
            mock_s3_client, "test.txt", "version-1"
        )

        assert result == {}

    @pytest.mark.asyncio
    async def test_get_scraped_text_object_tags_no_tagset(self):
        """Test tag retrieval when TagSet key is missing."""
        mock_s3_client = AsyncMock()
        mock_s3_client.get_object_tagging.return_value = {}

        result = await _get_scraped_text_object_tags_by_filename_with_client(
            mock_s3_client, "test.txt", "version-1"
        )

        assert result == {}


class TestIterateScrapedTextObjectsAndVersions:
    @pytest.mark.asyncio
    async def test_iterate_objects_without_tags(self):
        """Test iteration without including tags."""
        # Mock S3 client and paginator
        mock_s3_client = AsyncMock()
        mock_paginator = MagicMock()  # Use MagicMock for paginator

        # Make get_paginator a regular (non-async) method that returns the mock paginator
        mock_s3_client.get_paginator = MagicMock(return_value=mock_paginator)

        # Mock pagination response
        mock_page = {
            "Versions": [
                {
                    "Key": "example.com.txt",
                    "VersionId": "version-1",
                    "LastModified": datetime(2024, 1, 1, 12, 0, 0),
                    "Size": 1024,
                    "IsLatest": True,
                    "StorageClass": "STANDARD",
                    "ETag": '"abc123"',
                },
                {
                    "Key": "example.com.txt",
                    "VersionId": "version-2",
                    "LastModified": datetime(2024, 1, 1, 11, 0, 0),
                    "Size": 512,
                    "IsLatest": False,
                    "StorageClass": "STANDARD",
                    "ETag": '"def456"',
                },
            ],
            "DeleteMarkers": [
                {
                    "Key": "deleted.com.txt",
                    "VersionId": "delete-marker-1",
                    "LastModified": datetime(2024, 1, 2, 10, 0, 0),
                    "IsLatest": True,
                }
            ],
        }

        # Create an async generator function for pagination
        async def mock_paginate_async():
            yield mock_page

        mock_paginator.paginate.return_value = mock_paginate_async()

        # Collect results
        results = []
        async for obj_version in iterate_scraped_text_objects_and_versions_with_client(
            mock_s3_client
        ):
            results.append(obj_version)

        # Verify paginator was called correctly
        mock_s3_client.get_paginator.assert_called_once_with("list_object_versions")
        mock_paginator.paginate.assert_called_once()

        # Verify results
        assert len(results) == 3

        # Check first version
        assert results[0]["Key"] == "example.com.txt"
        assert results[0]["VersionId"] == "version-1"
        assert results[0]["Size"] == 1024
        assert results[0]["IsLatest"] is True
        assert results[0]["Type"] == "Version"
        assert "Tags" not in results[0]

        # Check second version
        assert results[1]["Key"] == "example.com.txt"
        assert results[1]["VersionId"] == "version-2"
        assert results[1]["Size"] == 512
        assert results[1]["IsLatest"] is False
        assert results[1]["Type"] == "Version"

        # Check delete marker
        assert results[2]["Key"] == "deleted.com.txt"
        assert results[2]["VersionId"] == "delete-marker-1"
        assert results[2]["Size"] == 0
        assert results[2]["IsLatest"] is True
        assert results[2]["Type"] == "DeleteMarker"
        assert results[2]["StorageClass"] is None
        assert results[2]["ETag"] is None

    @pytest.mark.asyncio
    async def test_iterate_objects_with_tags(self):
        """Test iteration with tags included."""
        # Mock S3 client and paginator
        mock_s3_client = AsyncMock()
        mock_paginator = MagicMock()  # Use MagicMock for paginator

        # Make get_paginator a regular (non-async) method that returns the mock paginator
        mock_s3_client.get_paginator = MagicMock(return_value=mock_paginator)

        # Mock get_object_tagging responses
        mock_s3_client.get_object_tagging.side_effect = [
            {
                "TagSet": [
                    {"Key": "urls_scraped", "Value": "10"},
                    {"Key": "batch_title", "Value": "batch_1"},
                ]
            },
            {"TagSet": [{"Key": "urls_scraped", "Value": "5"}]},
        ]

        # Mock pagination response
        mock_page = {
            "Versions": [
                {
                    "Key": "example.com.txt",
                    "VersionId": "version-1",
                    "LastModified": datetime(2024, 1, 1, 12, 0, 0),
                    "Size": 1024,
                    "IsLatest": True,
                    "StorageClass": "STANDARD",
                    "ETag": '"abc123"',
                },
                {
                    "Key": "test.com.txt",
                    "VersionId": "version-2",
                    "LastModified": datetime(2024, 1, 1, 11, 0, 0),
                    "Size": 512,
                    "IsLatest": True,
                    "StorageClass": "STANDARD",
                    "ETag": '"def456"',
                },
            ],
            "DeleteMarkers": [
                {
                    "Key": "deleted.com.txt",
                    "VersionId": "delete-marker-1",
                    "LastModified": datetime(2024, 1, 2, 10, 0, 0),
                    "IsLatest": True,
                }
            ],
        }

        # Create an async generator function for pagination
        async def mock_paginate_async():
            yield mock_page

        mock_paginator.paginate.return_value = mock_paginate_async()

        # Collect results with tags
        results = []
        async for obj_version in iterate_scraped_text_objects_and_versions_with_client(
            mock_s3_client, include_tags=True
        ):
            results.append(obj_version)

        # Verify tag calls were made
        assert mock_s3_client.get_object_tagging.call_count == 2

        # Verify results
        assert len(results) == 3

        # Check first version with tags
        assert results[0]["Key"] == "example.com.txt"
        assert results[0]["Tags"] == {"urls_scraped": "10", "batch_title": "batch_1"}

        # Check second version with tags
        assert results[1]["Key"] == "test.com.txt"
        assert results[1]["Tags"] == {"urls_scraped": "5"}

        # Check delete marker (should have empty tags)
        assert results[2]["Type"] == "DeleteMarker"
        assert results[2]["Tags"] == {}

    @pytest.mark.asyncio
    async def test_iterate_objects_with_prefix(self):
        """Test iteration with prefix filter."""
        mock_s3_client = AsyncMock()
        mock_paginator = MagicMock()  # Use MagicMock for paginator

        # Make get_paginator a regular (non-async) method that returns the mock paginator
        mock_s3_client.get_paginator = MagicMock(return_value=mock_paginator)

        mock_page = {"Versions": [], "DeleteMarkers": []}

        # Create an async generator function for pagination
        async def mock_paginate_async():
            yield mock_page

        mock_paginator.paginate.return_value = mock_paginate_async()

        prefix = "test-prefix/"
        results = []
        async for obj_version in iterate_scraped_text_objects_and_versions_with_client(
            mock_s3_client, prefix=prefix
        ):
            results.append(obj_version)

        # Verify paginator was called with prefix
        call_args = mock_paginator.paginate.call_args
        assert call_args.kwargs["Prefix"] == prefix

    @pytest.mark.asyncio
    async def test_iterate_objects_empty_response(self):
        """Test iteration when S3 returns empty response."""
        mock_s3_client = AsyncMock()
        mock_paginator = MagicMock()  # Use MagicMock for paginator

        # Make get_paginator a regular (non-async) method that returns the mock paginator
        mock_s3_client.get_paginator = MagicMock(return_value=mock_paginator)

        # Empty response
        mock_page = {}

        # Create an async generator function for pagination
        async def mock_paginate_async():
            yield mock_page

        mock_paginator.paginate.return_value = mock_paginate_async()

        results = []
        async for obj_version in iterate_scraped_text_objects_and_versions_with_client(
            mock_s3_client
        ):
            results.append(obj_version)

        assert len(results) == 0


class TestScrapedTextUtilS3IntegrationWithTags:
    @pytest_asyncio.fixture
    async def s3_client(self):
        # Initialize AWS clients first
        await initialize_core_aws_clients()
        try:
            session = aiobotocore.session.get_session()
            async with make_s3_client(session) as client:
                yield client
        finally:
            # Cleanup AWS clients
            await cleanup_core_aws_clients()

    @pytest.mark.asyncio
    async def test_full_cycle_with_tags_and_iteration(self, s3_client):
        """Integration test for upload, tag retrieval, and iteration."""
        # Generate unique test data
        etld1 = f"test-{uuid.uuid4().hex[:8]}.com"
        file_name = get_file_name_from_mfg_etld(etld1)
        file_content = "Integration test content with tags"
        tags = {
            "urls_scraped": "15",
            "batch_title": "integration_test_batch",
            "environment": "test",
        }

        try:
            # Upload with tags
            version_id, s3_url = await upload_scraped_text_to_s3_with_client(
                s3_client, file_content, file_name, tags
            )

            # Test tag retrieval
            retrieved_tags = (
                await _get_scraped_text_object_tags_by_filename_with_client(
                    s3_client, file_name, version_id
                )
            )
            assert retrieved_tags == tags

            # Test iteration without tags
            found_without_tags = False
            async for (
                obj_version
            ) in iterate_scraped_text_objects_and_versions_with_client(
                s3_client, prefix=file_name
            ):
                if (
                    obj_version["Key"] == file_name
                    and obj_version["VersionId"] == version_id
                ):
                    found_without_tags = True
                    assert "Tags" not in obj_version
                    assert obj_version["Type"] == "Version"
                    assert obj_version["IsLatest"] is True
                    break

            assert found_without_tags, "Object not found in iteration without tags"

            # Test iteration with tags
            found_with_tags = False
            async for (
                obj_version
            ) in iterate_scraped_text_objects_and_versions_with_client(
                s3_client, prefix=file_name, include_tags=True
            ):
                if (
                    obj_version["Key"] == file_name
                    and obj_version["VersionId"] == version_id
                ):
                    found_with_tags = True
                    assert obj_version["Tags"] == tags
                    assert obj_version["Type"] == "Version"
                    break

            assert found_with_tags, "Object not found in iteration with tags"

        finally:
            # Cleanup
            try:
                await delete_scraped_text_from_s3_by_filename_with_client(
                    s3_client, file_name, version_id
                )
            except Exception as e:
                if "AccessDenied" not in str(e):
                    raise
