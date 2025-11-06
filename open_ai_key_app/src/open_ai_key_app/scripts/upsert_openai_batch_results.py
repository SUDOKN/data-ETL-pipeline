#!/usr/bin/env python3
"""
Batch Upsert GPT Batch Results to Database

This script processes batch result files from a directory and performs batch upserts to the database.
It uses the custom_id from each result to match and update the corresponding GPTBatchRequest.

The script:
1. Scans /batch_results_output directory for files matching pattern: batch_{batch_id}_output.jsonl
2. Extracts batch_id from each filename
3. Parses the JSONL file containing batch results
4. Performs bulk upserts using custom_id as the unique identifier
5. Updates response_blob, response_received_at, and batch_id fields

Usage:
    # Process all files in /batch_results_output
    python upsert_openai_batch_results.py

    # Process all files with custom batch size
    python upsert_openai_batch_results.py --batch-size 100

    # Process only first 5 files
    python upsert_openai_batch_results.py --limit 5

    # Process a specific file in the results directory
    python upsert_openai_batch_results.py --file batch_abc123_output.jsonl

    # Dry run to preview changes
    python upsert_openai_batch_results.py --dry-run

    # Use custom results directory
    python upsert_openai_batch_results.py --results-dir /custom/path

    # Process specific file from custom directory
    python upsert_openai_batch_results.py --results-dir /custom/path --file batch_xyz_output.jsonl

Reference:
    OpenAI Batch API Response Format
"""

import argparse
import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional
from data_etl_app.models.types_and_enums import ConceptTypeEnum
from pymongo import UpdateOne

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
from core.utils.time_util import get_current_time
from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.gpt_batch_response_blob import (
    GPTBatchResponseBlob,
    GPTBatchResponseBody,
    GPTResponseBlobBody,
    GPTBatchResponseBlobChoice,
    GPTBatchResponseBlobChoiceMessage,
    GPTBatchResponseBlobUsage,
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def parse_batch_response(raw_result: dict, batch_id: str) -> dict:
    """
    Parse raw batch result into structured response blob.

    Args:
        raw_result: Raw result dictionary from JSONL file
        batch_id: Batch ID to associate with this result

    Returns:
        Parsed response blob dictionary
    """
    try:
        if raw_result.get("error"):
            raise ValueError(raw_result.get("error"))

        response_data = raw_result["response"]
        body_data = response_data["body"]

        # Parse choices
        choices = []
        for choice_data in body_data["choices"]:
            message_data = choice_data["message"]
            choice = GPTBatchResponseBlobChoice(
                index=choice_data["index"],
                message=GPTBatchResponseBlobChoiceMessage(
                    role=message_data["role"],
                    content=message_data["content"],
                ),
                # logprobs=choice_data.get("logprobs"),
                # finish_reason=choice_data["finish_reason"],
            )
            choices.append(choice)

        # Parse usage
        usage_data = body_data["usage"]
        usage = GPTBatchResponseBlobUsage(
            prompt_tokens=usage_data["prompt_tokens"],
            completion_tokens=usage_data["completion_tokens"],
            total_tokens=usage_data["total_tokens"],
        )

        # Parse response body
        response_body = GPTResponseBlobBody(
            # completion_id=body_data["id"],
            # object=body_data["object"],
            created=datetime.fromtimestamp(body_data["created"]),
            # model=body_data["model"],
            choices=choices,
            usage=usage,
            # system_fingerprint=body_data.get("system_fingerprint"),
        )

        # Parse full response
        response = GPTBatchResponseBody(
            status_code=response_data["status_code"],
            # gpt_internal_request_id=response_data["request_id"],
            body=response_body,
        )

        # Create complete blob
        blob = GPTBatchResponseBlob(
            batch_id=batch_id,
            request_custom_id=raw_result["custom_id"],
            response=response,
            error=raw_result.get("error"),
        )

        return blob.model_dump()

    except Exception as e:
        logger.error(
            f"Failed to parse response for custom_id {raw_result.get('custom_id')}: {e}"
        )
        raise


async def batch_upsert_results(
    results_file: Path, batch_id: str, batch_size: int = 500, dry_run: bool = False
) -> dict:
    """
    Batch upsert results from a JSONL file into the database.

    Args:
        results_file: Path to the JSONL results file
        batch_id: Batch ID to associate with all results in this file
        batch_size: Number of operations per batch
        dry_run: If True, don't actually update the database

    Returns:
        Statistics about the upsert operation
    """
    logger.info(f"Processing results file: {results_file}")

    operations = []
    stats = {
        "total_results": 0,
        "successful_parses": 0,
        "failed_parses": 0,
        "upserted": 0,
        "errors": 0,
    }

    received_at = get_current_time()

    with open(results_file, "r") as f:
        for line_num, line in enumerate(f, 1):
            stats["total_results"] += 1

            try:
                # Parse JSON line
                raw_result = json.loads(line.strip())
                custom_id = raw_result.get("custom_id")

                if not custom_id:
                    logger.warning(f"Line {line_num}: Missing custom_id, skipping")
                    stats["failed_parses"] += 1
                    continue

                # Parse the response blob
                response_blob = parse_batch_response(raw_result, batch_id)
                stats["successful_parses"] += 1

                # Create update operation
                # Use custom_id to find and update the document
                operation = UpdateOne(
                    {"request.custom_id": custom_id},  # Filter
                    {
                        "$set": {
                            "batch_id": batch_id,
                            "response_blob": response_blob,
                        }
                    },
                    upsert=False,  # Create new documents if they don't exist
                )
                operations.append(operation)

                # Execute batch when size is reached
                if len(operations) >= batch_size:
                    if not dry_run:
                        # TODO: all succeed or none
                        result = (
                            await GPTBatchRequest.get_pymongo_collection().bulk_write(
                                operations, ordered=False
                            )
                        )
                        stats["upserted"] += result.modified_count
                    else:
                        logger.info(
                            f"[DRY RUN] Would upsert batch of {len(operations)} operations"
                        )
                        stats["upserted"] += len(operations)

                    operations = []

            except json.JSONDecodeError as e:
                logger.error(f"Line {line_num}: JSON decode error - {e}")
                stats["failed_parses"] += 1
                stats["errors"] += 1
            except Exception as e:
                logger.error(f"Line {line_num}: Error processing result - {e}")
                stats["errors"] += 1

    # Execute remaining operations
    if operations:
        if not dry_run:
            result = await GPTBatchRequest.get_pymongo_collection().bulk_write(
                operations, ordered=False
            )
            stats["upserted"] += result.modified_count
        else:
            logger.info(
                f"[DRY RUN] Would upsert final batch of {len(operations)} operations"
            )
            stats["upserted"] += len(operations)

    return stats


async def async_main():
    """Main async execution function."""
    parser = argparse.ArgumentParser(
        description="Batch upsert GPT batch results to database"
    )
    parser.add_argument(
        "--results-dir",
        "-d",
        type=str,
        default="./batch_results_output",
        help="Directory containing result files (default: /batch_results_output)",
    )
    parser.add_argument(
        "--file",
        "-f",
        type=str,
        help="Process a specific file in the results directory (optional)",
    )
    parser.add_argument(
        "--limit",
        "-l",
        type=int,
        help="Limit the number of files to process (optional, ignored if --file is specified)",
    )
    parser.add_argument(
        "--batch-size",
        "-b",
        type=int,
        default=100,
        help="Number of operations per batch (default: 100)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't actually update the database, just show what would be done",
    )

    args = parser.parse_args()

    # Initialize database
    await init_db()
    logger.info("✓ Database initialized successfully\n")

    try:
        # Determine results directory
        results_dir = Path(args.results_dir)

        if not results_dir.exists():
            logger.error(f"Results directory not found: {results_dir}")
            return

        if args.dry_run:
            logger.info("🔍 DRY RUN MODE - No database changes will be made\n")

        # Determine which files to process
        if args.file:
            # Process single specified file
            result_file = results_dir / args.file
            if not result_file.exists():
                logger.error(f"File not found: {result_file}")
                return
            result_files = [result_file]
            logger.info(f"Processing single file: {args.file}\n")
        else:
            # Find all batch result files matching the pattern batch_{batch_id}_output.jsonl
            result_files = sorted(results_dir.glob("batch_*_output.jsonl"))

            if not result_files:
                logger.error(f"No batch result files found in {results_dir}")
                logger.error("Expected file pattern: batch_{{batch_id}}_output.jsonl")
                return

            # Apply limit if specified
            if args.limit:
                result_files = result_files[: args.limit]
                logger.info(
                    f"Processing {len(result_files)} file(s) (limited by --limit argument)\n"
                )
            else:
                logger.info(f"Found {len(result_files)} file(s) to process\n")

        # Process all files
        total_stats = {
            "files_processed": 0,
            "total_results": 0,
            "successful_parses": 0,
            "failed_parses": 0,
            "upserted": 0,
            "errors": 0,
        }

        for result_file in result_files:
            # Extract batch_id from filename: batch_{batch_id}_output.jsonl
            filename = result_file.name
            if not filename.startswith("batch_") or not filename.endswith(
                "_output.jsonl"
            ):
                logger.warning(f"Skipping file with unexpected format: {filename}")
                continue

            # Extract batch_id from the filename
            batch_id = filename[
                6:-13
            ]  # Remove "batch_" prefix and "_output.jsonl" suffix

            logger.info(f"\n{'='*80}")
            logger.info(f"Processing file: {result_file.name}")
            logger.info(f"Batch ID: {batch_id}")
            logger.info(f"{'='*80}\n")

            stats = await batch_upsert_results(
                result_file, batch_id, args.batch_size, args.dry_run
            )

            # Accumulate stats
            total_stats["files_processed"] += 1
            total_stats["total_results"] += stats["total_results"]
            total_stats["successful_parses"] += stats["successful_parses"]
            total_stats["failed_parses"] += stats["failed_parses"]
            total_stats["upserted"] += stats["upserted"]
            total_stats["errors"] += stats["errors"]

            logger.info(f"\nFile Summary:")
            logger.info(f"  Total Results: {stats['total_results']}")
            logger.info(f"  Successfully Parsed: {stats['successful_parses']}")
            logger.info(f"  Failed to Parse: {stats['failed_parses']}")
            logger.info(f"  Upserted: {stats['upserted']}")
            logger.info(f"  Errors: {stats['errors']}")

        # Print overall summary
        logger.info(f"\n{'='*80}")
        logger.info("OVERALL UPSERT SUMMARY")
        logger.info(f"{'='*80}")
        logger.info(f"Files Processed: {total_stats['files_processed']}")
        logger.info(f"Total Results: {total_stats['total_results']}")
        logger.info(f"Successfully Parsed: {total_stats['successful_parses']}")
        logger.info(f"Failed to Parse: {total_stats['failed_parses']}")
        logger.info(f"Upserted: {total_stats['upserted']}")
        logger.info(f"Errors: {total_stats['errors']}")

        if args.dry_run:
            logger.info("\n🔍 DRY RUN COMPLETE - No changes were made to the database")

    except Exception as e:
        logger.error(f"Error during batch upsert: {e}", exc_info=True)
        raise


def main():
    """Main execution function."""
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
