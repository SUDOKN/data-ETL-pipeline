"""
Download batch results from OpenAI for completed batches.

This script:
1. Retrieves all GPTBatch documents from the database
2. Checks the status of each batch via OpenAI API
3. Downloads output files for completed batches
4. Saves results to output directory
5. Updates batch status in database
"""

import asyncio
import argparse
import logging
import json
from datetime import datetime, UTC
from pathlib import Path
from typing import Optional

from openai import OpenAI, OpenAIError

from core.dependencies.load_core_env import load_core_env
from scraper_app.dependencies.load_scraper_env import load_scraper_env
from open_ai_key_app.dependencies.load_open_ai_app_env import load_open_ai_app_env
from data_etl_app.dependencies.load_data_etl_env import load_data_etl_env

# Load environment variables
load_core_env()
load_scraper_env()
load_data_etl_env()
load_open_ai_app_env()

from core.utils.mongo_client import init_db
from core.models.db.gpt_batch import GPTBatch

logger = logging.getLogger(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

OPENAI_API_KEYS = {
    "sudokn.tool": "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",
    "sudokn.tool2": "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",
    "sudokn.tool3": "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",
    # "sudokn.tool4": "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",
    "sudokn.tool5": "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",
    "sudokn.tool6": "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",
    "sudokn.tool7": "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",
    "sudokn.tool20": "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",
    "sudokn.tool1-o": "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",
    "sudokn.tool2-o": "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",
    "sudokn.tool3-o": "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",
    "sudokn.tool4-o": "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",
    "sudokn.tool21-o": "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",
    "sudokn.tool22-o": "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",
    "sudokn.tool24-o": "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",
    "sudokn.tool25-o": "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",
    # plus series
    "sudokn.tool+1": "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",
    "sudokn.tool+2": "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",
    "sudokn.tool+3": "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",
    "sudokn.tool+4": "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",
    "sudokn.tool+5": "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",
    "sudokn.tool2+1": "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",
    "sudokn.tool2+2": "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",
    "sudokn.tool2+3": "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",
    "sudokn.tool2+4": "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",
    "sudokn.tool2+5": "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",
    "sudokn.tool3+1": "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",
}


class BatchDownloader:
    """Handles checking batch status and downloading results."""

    def __init__(self, api_keys: dict[str, str], output_dir: Path):
        self.api_keys = api_keys
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.clients: dict[str, OpenAI] = {}  # Cache clients by key label

        # Statistics
        self.total_batches_checked = 0
        self.total_completed = 0
        self.total_failed = 0
        self.total_processing = 0
        self.total_downloaded = 0
        self.total_download_failed = 0
        self.batches_by_key: dict[str, int] = {}  # Track usage per key

    def _get_client(self, api_key_label: str | None) -> OpenAI:
        """
        Get or create an OpenAI client for the given API key label.

        Args:
            api_key_label: Label of the API key to use

        Returns:
            OpenAI client instance

        Raises:
            ValueError: If the API key label is not found
        """
        # Use first available key if no label provided
        if not api_key_label:
            api_key_label = list(self.api_keys.keys())[0]
            logger.warning(
                f"No API key label found in batch, using default: {api_key_label}"
            )

        # Check if key exists
        if api_key_label not in self.api_keys:
            logger.error(
                f"API key label '{api_key_label}' not found in OPENAI_API_KEYS"
            )
            # Fallback to first available key
            api_key_label = list(self.api_keys.keys())[0]
            logger.warning(f"Falling back to: {api_key_label}")

        # Return cached client or create new one
        if api_key_label not in self.clients:
            api_key = self.api_keys[api_key_label]
            self.clients[api_key_label] = OpenAI(api_key=api_key)
            logger.info(f"Created OpenAI client for key: {api_key_label}")

        # Track usage
        self.batches_by_key[api_key_label] = (
            self.batches_by_key.get(api_key_label, 0) + 1
        )

        return self.clients[api_key_label]

    async def check_and_update_batch_status(self, gpt_batch: GPTBatch) -> GPTBatch:
        """
        Check the current status of a batch from OpenAI API and update database.

        Args:
            gpt_batch: GPTBatch document from database

        Returns:
            Updated GPTBatch document
        """
        try:
            # Get the appropriate client for this batch's API key
            client = self._get_client(gpt_batch.api_key_label)

            # Retrieve current batch status from OpenAI
            batch_response = client.batches.retrieve(gpt_batch.external_batch_id)

            # Update fields that may have changed
            gpt_batch.status = batch_response.status
            gpt_batch.output_file_id = batch_response.output_file_id
            gpt_batch.error_file_id = batch_response.error_file_id

            # Update timestamps
            if batch_response.in_progress_at:
                gpt_batch.in_progress_at = datetime.fromtimestamp(
                    batch_response.in_progress_at
                )
            if batch_response.completed_at:
                gpt_batch.completed_at = datetime.fromtimestamp(
                    batch_response.completed_at
                )
            if batch_response.failed_at:
                gpt_batch.failed_at = datetime.fromtimestamp(batch_response.failed_at)
            if batch_response.expired_at:
                gpt_batch.expired_at = datetime.fromtimestamp(batch_response.expired_at)

            # Update request counts
            if batch_response.request_counts:
                gpt_batch.request_counts = batch_response.request_counts.model_dump()

            # Save updates to database
            await gpt_batch.save()

            logger.info(
                f"Updated batch {gpt_batch.external_batch_id}: "
                f"status={gpt_batch.status}, "
                f"requests={gpt_batch.request_counts}"
            )

            return gpt_batch

        except OpenAIError as e:
            logger.error(f"Error checking batch {gpt_batch.external_batch_id}: {e}")
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error checking batch {gpt_batch.external_batch_id}: {e}",
                exc_info=True,
            )
            raise

    def download_batch_output(
        self, gpt_batch: GPTBatch, output_type: str = "output"
    ) -> Optional[Path]:
        """
        Download the output or error file for a batch.
        Skips download if file already exists.

        Args:
            gpt_batch: GPTBatch document
            output_type: "output" or "error"

        Returns:
            Path to downloaded file if successful, None otherwise
        """
        file_id = (
            gpt_batch.output_file_id
            if output_type == "output"
            else gpt_batch.error_file_id
        )

        if not file_id:
            logger.warning(
                f"No {output_type} file ID for batch {gpt_batch.external_batch_id}"
            )
            return None

        try:
            # Create filename based on batch ID and type
            filename = f"{gpt_batch.external_batch_id}_{output_type}.jsonl"
            output_path = self.output_dir / filename

            # Check if file already exists
            if output_path.exists():
                file_size = output_path.stat().st_size
                logger.info(
                    f"⏭️  Skipping download - file already exists: {output_path} "
                    f"({file_size:,} bytes)"
                )
                return output_path

            logger.info(
                f"Downloading {output_type} file {file_id} "
                f"for batch {gpt_batch.external_batch_id}..."
            )

            # Get the appropriate client for this batch's API key
            client = self._get_client(gpt_batch.api_key_label)

            # Download file content
            file_response = client.files.content(file_id)

            # Save to file
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(file_response.text)

            logger.info(
                f"✅ Downloaded {output_type} file to {output_path} "
                f"({len(file_response.text):,} bytes)"
            )

            return output_path

        except OpenAIError as e:
            logger.error(f"Error downloading {output_type} file {file_id}: {e}")
            return None
        except Exception as e:
            logger.error(
                f"Unexpected error downloading {output_type} file {file_id}: {e}",
                exc_info=True,
            )
            return None

    async def delete_input_file(self, gpt_batch: GPTBatch) -> bool:
        """
        Delete the uploaded input file from OpenAI for a given batch.

        Uses the same API key label that was used to create the batch.

        Returns True if deletion reported success, False otherwise.
        """
        input_file_id = gpt_batch.input_file_id
        if not input_file_id:
            logger.warning(f"No input_file_id for batch {gpt_batch.external_batch_id}")
            return False

        try:
            client = self._get_client(gpt_batch.api_key_label)
            logger.info(
                f"Deleting uploaded input file {input_file_id} for batch {gpt_batch.external_batch_id} using key {gpt_batch.api_key_label}"
            )
            resp = client.files.delete(input_file_id)
            # resp is expected to be a dict-like with 'deleted': True
            deleted = bool(
                getattr(
                    resp,
                    "deleted",
                    resp.get("deleted") if isinstance(resp, dict) else False,
                )
            )
            if deleted:
                logger.info(
                    f"Deleted input file {input_file_id} (batch {gpt_batch.external_batch_id})"
                )
                return True
            else:
                logger.warning(f"Deletion response for {input_file_id}: {resp}")
                return False
        except OpenAIError as e:
            logger.error(f"OpenAI error deleting file {input_file_id}: {e}")
            return False
        except Exception as e:
            logger.error(
                f"Unexpected error deleting file {input_file_id}: {e}", exc_info=True
            )
            return False

    async def process_batch(self, gpt_batch: GPTBatch, download: bool = True) -> dict:
        """
        Check status and optionally download results for a single batch.

        Args:
            gpt_batch: GPTBatch document
            download: Whether to download files for completed batches

        Returns:
            Dict with processing results
        """
        self.total_batches_checked += 1

        result = {
            "batch_id": gpt_batch.external_batch_id,
            "old_status": gpt_batch.status,
            "new_status": None,
            "output_file": None,
            "error_file": None,
            "input_file_deleted": False,
            "success": False,
        }

        try:
            # Check and update status
            updated_batch = await self.check_and_update_batch_status(gpt_batch)
            result["new_status"] = updated_batch.status

            # Track status counts
            if updated_batch.status == "completed":
                self.total_completed += 1
            elif updated_batch.status == "failed":
                self.total_failed += 1
            elif updated_batch.status in ["validating", "in_progress", "finalizing"]:
                self.total_processing += 1

            # Download files if batch is completed and download is enabled
            if download and updated_batch.status == "completed":
                # Download output file
                output_path = self.download_batch_output(updated_batch, "output")
                if output_path:
                    result["output_file"] = str(output_path)
                    self.total_downloaded += 1
                    # After successful download, delete the uploaded input file from OpenAI
                    try:
                        deleted = await self.delete_input_file(updated_batch)
                        result["input_file_deleted"] = deleted
                    except Exception:
                        result["input_file_deleted"] = False
                else:
                    self.total_download_failed += 1

                # Download error file if it exists
                if updated_batch.error_file_id:
                    error_path = self.download_batch_output(updated_batch, "error")
                    if error_path:
                        result["error_file"] = str(error_path)

            result["success"] = True

        except Exception as e:
            logger.error(
                f"Error processing batch {gpt_batch.external_batch_id}: {e}",
                exc_info=True,
            )
            result["error"] = str(e)

        return result


async def process_all_batches(
    api_keys: dict[str, str],
    output_dir: Path,
    status_filter: Optional[str] = None,
    limit: Optional[int] = None,
    download: bool = True,
):
    """
    Process all batches from the database.

    Args:
        api_keys: Dictionary of API key labels to API keys
        output_dir: Directory to save downloaded files
        status_filter: Optional status filter (e.g., "completed", "failed")
        limit: Optional limit on number of batches to process
        download: Whether to download files for completed batches
    """
    downloader = BatchDownloader(api_keys, output_dir)

    # Build query
    query = {}
    if status_filter:
        query["status"] = status_filter

    # Retrieve batches from database
    logger.info("Retrieving batches from database...")
    batches = await GPTBatch.find(query).to_list()

    if not batches:
        logger.warning("No batches found in database")
        return

    # Apply limit if specified
    if limit:
        batches = batches[:limit]

    logger.info(f"Found {len(batches)} batch(es) to process")

    # Process each batch
    results = []
    for idx, batch in enumerate(batches, 1):
        logger.info("=" * 70)
        logger.info(f"Processing batch {idx}/{len(batches)}: {batch.external_batch_id}")
        logger.info(f"Current status: {batch.status}")
        logger.info("=" * 70)

        result = await downloader.process_batch(batch, download=download)
        results.append(result)

        # Log result
        if result["success"]:
            status_change = (
                f"{result['old_status']} → {result['new_status']}"
                if result["old_status"] != result["new_status"]
                else result["new_status"]
            )
            logger.info(f"✅ Status: {status_change}")
            if result.get("output_file"):
                logger.info(f"   Output: {result['output_file']}")
            if result.get("error_file"):
                logger.info(f"   Errors: {result['error_file']}")
        else:
            logger.error(f"❌ Failed: {result.get('error', 'Unknown error')}")

    # Log final summary
    logger.info("=" * 70)
    logger.info("PROCESSING COMPLETE")
    logger.info("=" * 70)
    logger.info(f"Batches checked: {downloader.total_batches_checked}")
    logger.info(f"  Completed: {downloader.total_completed}")
    logger.info(f"  Failed: {downloader.total_failed}")
    logger.info(f"  Processing: {downloader.total_processing}")
    logger.info(f"Files downloaded: {downloader.total_downloaded}")
    logger.info(f"Download failures: {downloader.total_download_failed}")
    logger.info(f"Output directory: {output_dir}")

    # Log per-key usage
    if downloader.batches_by_key:
        logger.info("\nBatches processed per API key:")
        for key_label, count in sorted(downloader.batches_by_key.items()):
            logger.info(f"  {key_label}: {count}")

    logger.info("=" * 70)

    # Save summary to JSON
    summary_file = (
        output_dir
        / f"download_summary_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.json"
    )
    summary = {
        "timestamp": datetime.now(UTC).isoformat(),
        "total_batches_checked": downloader.total_batches_checked,
        "total_completed": downloader.total_completed,
        "total_failed": downloader.total_failed,
        "total_processing": downloader.total_processing,
        "total_downloaded": downloader.total_downloaded,
        "total_download_failed": downloader.total_download_failed,
        "batches_by_key": downloader.batches_by_key,
        "results": results,
    }

    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    logger.info(f"Summary saved to: {summary_file}")


async def async_main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Check batch status and download completed results"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./batch_results_output",
        help="Output directory for downloaded files (default: ./batch_results_output)",
    )
    parser.add_argument(
        "--status",
        type=str,
        choices=[
            "validating",
            "in_progress",
            "finalizing",
            "completed",
            "failed",
            "expired",
            "cancelled",
        ],
        help="Filter batches by status",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of batches to process (for testing)",
    )
    parser.add_argument(
        "--no-download",
        action="store_true",
        help="Only check status, don't download files",
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Skip confirmation prompt and proceed automatically",
    )

    args = parser.parse_args()

    # Convert output directory to Path
    output_dir = Path(args.output_dir)

    # Initialize MongoDB
    logger.info("Initializing MongoDB connection...")
    await init_db(
        max_pool_size=10,
        min_pool_size=2,
        max_idle_time_ms=60000,
        server_selection_timeout_ms=30000,
        connect_timeout_ms=30000,
        socket_timeout_ms=120000,
    )

    # Count batches
    query = {}
    if args.status:
        query["status"] = args.status

    total_batches = await GPTBatch.find(query).count()

    if total_batches == 0:
        logger.warning("No batches found matching criteria")
        return

    # Check if API keys are configured
    if not OPENAI_API_KEYS:
        print("\n⚠️  WARNING: No API keys configured!")
        print(
            "Please add your OpenAI API keys to the OPENAI_API_KEYS dictionary in the script."
        )
        return

    # Show configuration
    print("\n" + "=" * 70)
    print("BATCH RESULTS DOWNLOAD")
    print("=" * 70)
    print(f"Output directory: {output_dir}")
    print(f"Status filter: {args.status or 'All'}")
    print(f"Total batches found: {total_batches}")
    print(f"API keys configured: {len(OPENAI_API_KEYS)}")
    if args.limit:
        print(f"Limit: {args.limit} (testing mode)")
        print(f"Will process: {min(args.limit, total_batches)} batch(es)")
    else:
        print(f"Will process: {total_batches} batch(es) (ALL)")
    print(f"Download files: {'No (status check only)' if args.no_download else 'Yes'}")
    print("=" * 70)

    # Ask for confirmation (unless --yes flag is set)
    if not args.yes:
        response = input("\nDo you want to proceed? (yes/no): ").strip().lower()
        if response not in ["yes", "y"]:
            logger.info("Download cancelled by user")
            print("Download cancelled.")
            return
    else:
        logger.info("Auto-proceeding with --yes flag")

    print("\nStarting processing...\n")

    # Process batches
    await process_all_batches(
        api_keys=OPENAI_API_KEYS,
        output_dir=output_dir,
        status_filter=args.status,
        limit=args.limit,
        download=not args.no_download,
    )


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
