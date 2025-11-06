"""
Phase 3: Collect incomplete GPT batch requests from deferred manufacturers.

This script:
1. Iterates through manufacturers sorted by scraped_text_file_num_tokens (ascending)
2. Finds linked deferred manufacturers using etld1
3. Collects all incomplete GPT batch requests (response_blob=None)
4. Writes them to a JSONL file with constraints:
   - Max 1,000,000 total tokens
   - Max 50,000 requests per file
"""

import asyncio
import argparse
import logging
import json
import csv
from datetime import datetime
from pathlib import Path
from typing import Optional
from pymongo.asynchronous.collection import AsyncCollection

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
from core.models.db.manufacturer import Manufacturer
from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.gpt_batch_request_blob import GPTBatchRequestBlob

logger = logging.getLogger(__name__)


# Constraints
MAX_TOKENS_PER_FILE = 20_000_000
MAX_REQUESTS_PER_FILE = 40_000
MAX_FILE_SIZE_BYTES = (
    120 * 1024 * 1024
)  # 120MB in bytes (keep buffer below 200MB limit)


class BatchFileWriter:
    """Handles writing batch requests to JSONL files with constraints."""

    def __init__(
        self, output_dir: Path, prefix: str = "batch", max_files: Optional[int] = None
    ):
        # Create timestamped subdirectory
        self.run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.output_dir = output_dir / self.run_timestamp
        self.prefix = prefix
        self.max_files = max_files  # Maximum number of files to create
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Current file state
        self.file_index = 0
        self.current_file_path: Optional[Path] = None
        self.current_file = None
        self.current_tokens = 0
        self.current_requests = 0
        self.current_manufacturers = 0  # Track manufacturers in current file
        self.current_file_size = 0  # Track approximate file size in bytes
        self.current_timestamp: Optional[str] = None  # Track timestamp for current file

        # Overall statistics
        self.total_requests = 0
        self.total_tokens = 0
        self.total_files = 0

        # Detailed tracking per batch file
        self.batch_file_metadata: dict[str, dict] = {}  # Maps filename to metadata

        self._start_new_file()

    def _start_new_file(self):
        """Start a new batch file."""
        if self.current_file and self.current_file_path and self.current_timestamp:
            self.current_file.close()

            # Save metadata for this file
            filename = self.current_file_path.name
            self.batch_file_metadata[filename] = {
                "manufacturers": self.current_manufacturers,
                "requests": self.current_requests,
                "tokens": self.current_tokens,
            }

            logger.info(
                f"Closed {self.current_file_path.name}: "
                f"{self.current_requests:,} requests, {self.current_tokens:,} tokens, "
                f"{self.current_manufacturers} manufacturers, "
                f"{self.current_file_size / (1024 * 1024):.2f} MB"
            )

        # Check if we've reached the max number of files
        if self.max_files is not None and self.file_index >= self.max_files:
            raise StopIteration(f"Reached maximum number of files: {self.max_files}")

        self.file_index += 1
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.current_timestamp = timestamp
        self.current_file_path = (
            self.output_dir / f"{timestamp}_{self.prefix}_{self.file_index}.jsonl"
        )
        self.current_file = open(self.current_file_path, "w", encoding="utf-8")
        self.current_tokens = 0
        self.current_requests = 0
        self.current_manufacturers = 0
        self.current_file_size = 0
        self.total_files += 1

        logger.info(f"Started new batch file: {self.current_file_path.name}")

    def _serialize_request(self, request_blob: GPTBatchRequestBlob) -> str:
        """Serialize a request blob to JSON string (without input_tokens)."""
        request_dict = request_blob.model_dump()
        request_dict["body"].pop("input_tokens", None)
        # Use separators for consistent, compact JSON output
        return json.dumps(request_dict, separators=(",", ":"), sort_keys=False)

    def can_add_manufacturer_requests(
        self, request_blobs: list[GPTBatchRequestBlob]
    ) -> bool:
        """Check if all requests from a manufacturer can be added to current file without exceeding constraints."""
        total_requests = len(request_blobs)

        # Calculate total tokens
        total_tokens = sum(blob.body.input_tokens for blob in request_blobs)

        # Calculate exact size by serializing each request
        total_size = 0
        for blob in request_blobs:
            json_str = self._serialize_request(blob)
            # Size includes the JSON string + newline character
            total_size += len(json_str.encode("utf-8")) + 1  # +1 for newline

        would_exceed_requests = (
            self.current_requests + total_requests
        ) > MAX_REQUESTS_PER_FILE

        would_exceed_tokens = (self.current_tokens + total_tokens) > MAX_TOKENS_PER_FILE

        would_exceed_file_size = (
            self.current_file_size + total_size
        ) > MAX_FILE_SIZE_BYTES

        # Log when we're about to start a new file due to size constraints
        if would_exceed_file_size and not would_exceed_requests:
            logger.info(
                f"Size limit check: current={self.current_file_size/(1024*1024):.2f}MB + "
                f"new={total_size/(1024*1024):.2f}MB = "
                f"{(self.current_file_size+total_size)/(1024*1024):.2f}MB "
                f"(limit: {MAX_FILE_SIZE_BYTES/(1024*1024):.0f}MB, {total_requests} requests)"
            )

        if (
            would_exceed_tokens
            and not would_exceed_requests
            and not would_exceed_file_size
        ):
            logger.info(
                f"Token limit check: current={self.current_tokens:,} + "
                f"new={total_tokens:,} = "
                f"{self.current_tokens+total_tokens:,} "
                f"(limit: {MAX_TOKENS_PER_FILE:,}, {total_requests} requests)"
            )

        return not (
            would_exceed_requests or would_exceed_file_size or would_exceed_tokens
        )

    def write_manufacturer_requests(self, request_blobs: list[GPTBatchRequestBlob]):
        """
        Write all requests from a manufacturer to the current file.
        If they don't fit, start a new file first.
        All requests from a manufacturer stay together in the same file.
        """
        if not request_blobs:
            return

        # Check if we need a new file for this manufacturer
        if not self.can_add_manufacturer_requests(request_blobs):
            self._start_new_file()

        # Write all requests from this manufacturer
        for request_blob in request_blobs:
            request_tokens = request_blob.body.input_tokens

            # Serialize to JSON using the same method as size calculation
            json_str = self._serialize_request(request_blob)
            json_line = json_str + "\n"

            # Write to file
            if self.current_file:
                self.current_file.write(json_line)

            # Update counters with exact byte size
            line_size = len(json_line.encode("utf-8"))
            self.current_file_size += line_size
            self.current_tokens += request_tokens
            self.current_requests += 1
            self.total_tokens += request_tokens
            self.total_requests += 1

        # Increment manufacturer count after all requests are written
        self.current_manufacturers += 1

    def close(self):
        """Close the current file and return statistics."""
        if self.current_file and self.current_file_path and self.current_timestamp:
            self.current_file.close()

            # Save metadata for the final file
            filename = self.current_file_path.name
            self.batch_file_metadata[filename] = {
                "manufacturers": self.current_manufacturers,
                "requests": self.current_requests,
                "tokens": self.current_tokens,
            }

            logger.info(
                f"Closed final file {self.current_file_path.name}: "
                f"{self.current_requests:,} requests, {self.current_tokens:,} tokens, "
                f"{self.current_manufacturers} manufacturers, "
                f"{self.current_file_size / (1024 * 1024):.2f} MB"
            )

        # Write batch file metadata to JSON file
        metadata_file = self.output_dir / "batch_metadata.json"
        with open(metadata_file, "w", encoding="utf-8") as f:
            json.dump(self.batch_file_metadata, f, indent=2)
            logger.info(f"Wrote batch metadata to: {metadata_file}")

        return {
            "total_files": self.total_files,
            "total_requests": self.total_requests,
            "total_tokens": self.total_tokens,
            "batch_file_metadata": self.batch_file_metadata,
            "output_dir": str(
                self.output_dir
            ),  # Include the actual output directory used
        }


async def collect_incomplete_batch_requests_for_deferred_mfg(
    mfg: Manufacturer,
    deferred_mfg: DeferredManufacturer,
) -> tuple[list[GPTBatchRequestBlob], list[str], list[str]]:
    """
    Collect all incomplete batch requests from a deferred manufacturer.

    Returns:
        Tuple of:
        - List of GPTBatchRequestBlob objects that don't have response_blob
        - List of custom IDs that have no corresponding GPTBatchRequest
        - List of validation errors (fields missing in both Manufacturer and DeferredManufacturer)
    """
    custom_ids = set()
    validation_errors = []

    # Check all fields for completeness (missing in both Manufacturer and DeferredManufacturer)
    if not mfg.is_manufacturer and not deferred_mfg.is_manufacturer:
        validation_errors.append("is_manufacturer")
    elif deferred_mfg.is_manufacturer:
        for (
            chunk_key,
            custom_id,
        ) in deferred_mfg.is_manufacturer.chunk_request_id_map.items():
            custom_ids.add(custom_id)

    if not mfg.is_contract_manufacturer and not deferred_mfg.is_contract_manufacturer:
        validation_errors.append("is_contract_manufacturer")
    elif deferred_mfg.is_contract_manufacturer:
        for (
            chunk_key,
            custom_id,
        ) in deferred_mfg.is_contract_manufacturer.chunk_request_id_map.items():
            custom_ids.add(custom_id)

    if not mfg.is_product_manufacturer and not deferred_mfg.is_product_manufacturer:
        validation_errors.append("is_product_manufacturer")
    elif deferred_mfg.is_product_manufacturer:
        for (
            chunk_key,
            custom_id,
        ) in deferred_mfg.is_product_manufacturer.chunk_request_id_map.items():
            custom_ids.add(custom_id)

    # Collect custom IDs from addresses (DeferredBasicExtraction object)
    if not mfg.addresses and not deferred_mfg.addresses:
        validation_errors.append("addresses")
    elif deferred_mfg.addresses:
        custom_ids.add(deferred_mfg.addresses.gpt_request_id)

    if not mfg.business_desc and not deferred_mfg.business_desc:
        validation_errors.append("business_desc")
    elif deferred_mfg.business_desc:
        custom_ids.add(deferred_mfg.business_desc.gpt_request_id)

    if not mfg.products and not deferred_mfg.products:
        validation_errors.append("products")
    elif deferred_mfg.products:
        for (
            chunk_key,
            custom_id,
        ) in deferred_mfg.products.chunk_request_id_map.items():
            custom_ids.add(custom_id)

    # Check certificates
    if not mfg.certificates and not deferred_mfg.certificates:
        validation_errors.append("certificates")
    elif deferred_mfg.certificates:
        for (
            chunk_key,
            bundle,
        ) in deferred_mfg.certificates.chunk_request_bundle_map.items():
            custom_ids.add(bundle.llm_search_request_id)
        if deferred_mfg.certificates.llm_mapping_request_id:
            custom_ids.add(deferred_mfg.certificates.llm_mapping_request_id)

    # Check industries
    if not mfg.industries and not deferred_mfg.industries:
        validation_errors.append("industries")
    elif deferred_mfg.industries:
        for (
            chunk_key,
            bundle,
        ) in deferred_mfg.industries.chunk_request_bundle_map.items():
            custom_ids.add(bundle.llm_search_request_id)
        if deferred_mfg.industries.llm_mapping_request_id:
            custom_ids.add(deferred_mfg.industries.llm_mapping_request_id)

    # Check process_caps
    if not mfg.process_caps and not deferred_mfg.process_caps:
        validation_errors.append("process_caps")
    elif deferred_mfg.process_caps:
        for (
            chunk_key,
            bundle,
        ) in deferred_mfg.process_caps.chunk_request_bundle_map.items():
            custom_ids.add(bundle.llm_search_request_id)
        if deferred_mfg.process_caps.llm_mapping_request_id:
            custom_ids.add(deferred_mfg.process_caps.llm_mapping_request_id)

    # Check material_caps
    if not mfg.material_caps and not deferred_mfg.material_caps:
        validation_errors.append("material_caps")
    elif deferred_mfg.material_caps:
        for (
            chunk_key,
            bundle,
        ) in deferred_mfg.material_caps.chunk_request_bundle_map.items():
            custom_ids.add(bundle.llm_search_request_id)
        if deferred_mfg.material_caps.llm_mapping_request_id:
            custom_ids.add(deferred_mfg.material_caps.llm_mapping_request_id)

    if not custom_ids:
        return [], [], validation_errors

    # Query ALL GPTBatchRequest for all custom IDs (regardless of response_blob status)
    # This helps us identify missing GPTBatchRequests
    all_requests = await GPTBatchRequest.find(
        {
            "request.custom_id": {"$in": list(custom_ids)},
        }
    ).to_list(length=None)

    # Create a set of custom IDs that exist in the database
    found_custom_ids = {req.request.custom_id for req in all_requests}

    # Find custom IDs that have NO corresponding GPTBatchRequest
    missing_custom_ids = list(custom_ids - found_custom_ids)

    # Filter for incomplete requests (response_blob is None)
    incomplete_requests = [
        req
        for req in all_requests
        if (req.response_blob is None and req.batch_id is None)
    ]

    # Extract the request blobs
    request_blobs = [req.request for req in incomplete_requests]

    return request_blobs, missing_custom_ids, validation_errors


async def process_manufacturers_and_collect_requests(
    query_filter: dict,
    output_dir: Path,
    limit: Optional[int] = None,
    max_files: Optional[int] = None,
):
    """
    Main processing function: iterate through manufacturers and collect incomplete requests.

    Args:
        query_filter: MongoDB query filter for manufacturers
        output_dir: Directory to write batch files
        limit: Optional limit on number of manufacturers to process
        max_files: Optional limit on number of batch files to create
    """
    # Initialize batch file writer
    writer = BatchFileWriter(output_dir, max_files=max_files)

    # Initialize NDJSON file for manufacturers with missing GPTBatchRequests
    timestamp = writer.run_timestamp  # Use the same timestamp as the batch writer
    missing_requests_ndjson_path = (
        writer.output_dir / f"{timestamp}_missing_batch_requests.ndjson"
    )
    missing_requests_ndjson = open(missing_requests_ndjson_path, "w", encoding="utf-8")
    logger.info(
        f"Logging missing batch requests to: {missing_requests_ndjson_path.name}"
    )

    # Initialize NDJSON file for validation errors (fields missing in both Manufacturer and DeferredManufacturer)
    validation_errors_ndjson_path = (
        writer.output_dir / f"{timestamp}_validation_errors.ndjson"
    )
    validation_errors_ndjson = open(
        validation_errors_ndjson_path, "w", encoding="utf-8"
    )
    logger.info(f"Logging validation errors to: {validation_errors_ndjson_path.name}")

    # Initialize CSV file for skipped manufacturers (no deferred manufacturer found)
    skipped_manufacturers_csv_path = (
        writer.output_dir / f"{timestamp}_skipped_manufacturers.csv"
    )
    skipped_manufacturers_csv = open(
        skipped_manufacturers_csv_path, "w", encoding="utf-8", newline=""
    )
    csv_writer = csv.writer(skipped_manufacturers_csv)
    csv_writer.writerow(["etld1"])  # Write header
    logger.info(
        f"Logging skipped manufacturers to: {skipped_manufacturers_csv_path.name}"
    )

    # Get collection and count
    collection: AsyncCollection = Manufacturer.get_pymongo_collection()
    total_count = await collection.count_documents(query_filter)

    if limit:
        total_count = min(total_count, limit)

    logger.info(
        f"Processing {total_count:,} manufacturers to collect incomplete batch requests"
    )

    # Statistics
    processed_count = 0
    deferred_found_count = 0
    deferred_not_found_count = 0
    manufacturers_with_incomplete_requests = 0
    manufacturers_with_missing_requests = 0
    manufacturers_with_validation_errors = 0
    skipped_manufacturers = []
    total_missing_custom_ids = 0
    total_validation_errors = 0

    # Process manufacturers one at a time, sorted by scraped_text_file_num_tokens
    cursor = (
        collection.find(query_filter)
        .sort("scraped_text_file_num_tokens", 1)  # 1 = ascending
        .limit(limit if limit else 0)
    )

    async for mfg_doc in cursor:
        # Create full Manufacturer object for potential checks
        mfg = Manufacturer(**mfg_doc)
        processed_count += 1

        # Find linked deferred manufacturer
        deferred_mfg = await DeferredManufacturer.find_one(
            {
                "mfg_etld1": mfg.etld1,
                "scraped_text_file_version_id": mfg.scraped_text_file_version_id,
            }
        )

        if not deferred_mfg:
            deferred_not_found_count += 1
            skipped_manufacturers.append(mfg.etld1)
            # Write to CSV
            csv_writer.writerow([mfg.etld1])
            if processed_count % 100 == 0:
                logger.info(
                    f"Progress: {processed_count}/{total_count} manufacturers "
                    f"({deferred_found_count} with deferred, {deferred_not_found_count} without)"
                )
            continue

        deferred_found_count += 1

        # Collect incomplete batch requests, check for missing GPTBatchRequests, and validate fields
        incomplete_requests, missing_custom_ids, validation_errors = (
            await collect_incomplete_batch_requests_for_deferred_mfg(mfg, deferred_mfg)
        )

        # If there are validation errors, log to NDJSON
        if validation_errors:
            manufacturers_with_validation_errors += 1
            total_validation_errors += len(validation_errors)

            # Write to validation errors NDJSON
            validation_record = {
                "etld1": mfg.etld1,
                "missing_fields": validation_errors,
            }
            validation_errors_ndjson.write(json.dumps(validation_record) + "\n")

            # Log warning
            logger.warning(
                f"Validation errors for {mfg.etld1}: {len(validation_errors)} fields missing in both Manufacturer and DeferredManufacturer: {validation_errors}"
            )

        # If there are missing custom IDs, log to NDJSON and skip this manufacturer
        if missing_custom_ids:
            manufacturers_with_missing_requests += 1
            total_missing_custom_ids += len(missing_custom_ids)

            # Write to NDJSON (one line per manufacturer with all missing custom IDs)
            missing_record = {
                "etld1": mfg.etld1,
                "missing_custom_ids": missing_custom_ids,
            }
            missing_requests_ndjson.write(json.dumps(missing_record) + "\n")

            # Log warning
            logger.warning(
                f"Skipping {mfg.etld1}: {len(missing_custom_ids)} custom IDs have no GPTBatchRequest. \n"
                f"Missing custom IDs: {missing_custom_ids}"
            )

            # Skip to next manufacturer
            if processed_count % 100 == 0:
                logger.info(
                    f"Progress: {processed_count}/{total_count} manufacturers | "
                    f"Deferred: {deferred_found_count} found, {deferred_not_found_count} not found | "
                    f"Missing requests: {manufacturers_with_missing_requests} | "
                    f"Validation errors: {manufacturers_with_validation_errors} | "
                    f"Requests collected: {writer.total_requests:,} | "
                    f"Files: {writer.total_files}"
                )
            continue

        if incomplete_requests:
            manufacturers_with_incomplete_requests += 1

            # Write all requests from this manufacturer together
            try:
                writer.write_manufacturer_requests(incomplete_requests)
            except StopIteration as e:
                logger.info(f"Stopping: {e}")
                logger.info(
                    f"Processed {processed_count} manufacturers before stopping"
                )
                break

        # Log progress
        if processed_count % 100 == 0:
            logger.info(
                f"Progress: {processed_count}/{total_count} manufacturers | "
                f"Deferred: {deferred_found_count} found, {deferred_not_found_count} not found | "
                f"Missing requests: {manufacturers_with_missing_requests} | "
                f"Validation errors: {manufacturers_with_validation_errors} | "
                f"Requests collected: {writer.total_requests:,} | "
                f"Files: {writer.total_files}"
            )

    # Close writer and get final stats
    writer_stats = writer.close()

    # Close NDJSON files
    missing_requests_ndjson.close()
    logger.info(f"Missing batch requests log saved to: {missing_requests_ndjson_path}")

    validation_errors_ndjson.close()
    logger.info(f"Validation errors log saved to: {validation_errors_ndjson_path}")

    # Close CSV file
    skipped_manufacturers_csv.close()
    logger.info(f"Skipped manufacturers CSV saved to: {skipped_manufacturers_csv_path}")

    # Log final summary
    logger.info("=" * 70)
    logger.info("COLLECTION COMPLETE")
    logger.info("=" * 70)
    logger.info(f"Manufacturers processed: {processed_count:,}")
    logger.info(f"Deferred manufacturers found: {deferred_found_count:,}")
    logger.info(f"Deferred manufacturers not found: {deferred_not_found_count:,}")
    logger.info(
        f"Manufacturers with validation errors: {manufacturers_with_validation_errors:,} "
        f"({total_validation_errors:,} missing fields)"
    )
    logger.info(
        f"Manufacturers with missing GPTBatchRequests: {manufacturers_with_missing_requests:,} "
        f"({total_missing_custom_ids:,} custom IDs)"
    )
    logger.info(
        f"Manufacturers with incomplete requests: {manufacturers_with_incomplete_requests:,}"
    )
    logger.info(
        f"Total incomplete requests collected: {writer_stats['total_requests']:,}"
    )
    logger.info(f"Total tokens: {writer_stats['total_tokens']:,}")
    logger.info(f"Total files created: {writer_stats['total_files']}")
    logger.info(f"Output directory: {writer_stats['output_dir']}")
    logger.info(f"Missing requests NDJSON: {missing_requests_ndjson_path}")
    logger.info(f"Validation errors NDJSON: {validation_errors_ndjson_path}")
    logger.info(f"Skipped manufacturers CSV: {skipped_manufacturers_csv_path}")
    logger.info("=" * 70)

    if skipped_manufacturers:
        logger.warning(
            f"Skipped {len(skipped_manufacturers)} manufacturers without deferred data"
        )
        logger.warning(f"First 10: {skipped_manufacturers[:10]}")

    return {
        "processed_count": processed_count,
        "deferred_found_count": deferred_found_count,
        "deferred_not_found_count": deferred_not_found_count,
        "manufacturers_with_validation_errors": manufacturers_with_validation_errors,
        "total_validation_errors": total_validation_errors,
        "manufacturers_with_missing_requests": manufacturers_with_missing_requests,
        "total_missing_custom_ids": total_missing_custom_ids,
        "manufacturers_with_incomplete_requests": manufacturers_with_incomplete_requests,
        "total_requests": writer_stats["total_requests"],
        "total_tokens": writer_stats["total_tokens"],
        "total_files": writer_stats["total_files"],
        "skipped_manufacturers": skipped_manufacturers,
        "missing_requests_ndjson": str(missing_requests_ndjson_path),
        "validation_errors_ndjson": str(validation_errors_ndjson_path),
        "skipped_manufacturers_csv": str(skipped_manufacturers_csv_path),
    }


async def async_main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Collect incomplete GPT batch requests from deferred manufacturers"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./batch_requests_output",
        help="Output directory for batch request JSONL files (default: ./batch_requests_output)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of manufacturers to process (for testing)",
    )
    parser.add_argument(
        "--etld1",
        type=str,
        help="Process only this specific manufacturer etld1",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        help="Maximum number of batch files to create before stopping",
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Skip confirmation prompt and proceed automatically",
    )

    args = parser.parse_args()

    # Validate arguments
    if args.etld1 and args.limit:
        print("Error: --etld1 and --limit cannot be used together")
        return

    # Convert output dir to Path
    output_dir = Path(args.output_dir)

    # Initialize MongoDB
    logger.info("Initializing MongoDB connection...")
    await init_db(
        max_pool_size=10,  # Reduced from 50 - this is primarily a read-only operation
        min_pool_size=2,  # Reduced from 10 - we don't need many idle connections
        max_idle_time_ms=60000,
        server_selection_timeout_ms=30000,
        connect_timeout_ms=30000,
        socket_timeout_ms=120000,
    )

    # Query filter: manufacturers with scraped text file
    query_filter = {
        "scraped_text_file_version_id": {"$exists": True},
        "scraped_text_file_num_tokens": {"$lt": 100000},
    }

    # Add etld1 filter if specified
    if args.etld1:
        query_filter["etld1"] = args.etld1

    # Get count
    collection = Manufacturer.get_pymongo_collection()
    matching_count = await collection.count_documents(query_filter)

    # Show configuration
    print("\n" + "=" * 70)
    print("INCOMPLETE BATCH REQUESTS COLLECTION")
    print("=" * 70)
    if args.etld1:
        print(f"Filter: manufacturer with etld1='{args.etld1}'")
    else:
        print(f"Filter: manufacturers with scraped_text_file_version_id")
    print(f"Total matching manufacturers: {matching_count:,}")
    if args.etld1:
        print(f"Processing specific manufacturer: {args.etld1}")
    elif args.limit:
        print(f"Limit: {args.limit:,} (testing mode)")
        print(f"Will process: {min(args.limit, matching_count):,} manufacturers")
    else:
        print(f"Will process: {matching_count:,} manufacturers (ALL)")
    if args.max_files:
        print(f"Max files: {args.max_files} (will stop after creating this many files)")
    print(f"Output directory: {output_dir}")
    print(f"Constraints:")
    print(f"  - Max {MAX_TOKENS_PER_FILE:,} tokens per file")
    print(f"  - Max {MAX_REQUESTS_PER_FILE:,} requests per file")
    print(f"  - Max {MAX_FILE_SIZE_BYTES / (1024 * 1024):.0f} MB per file")
    print("=" * 70)

    # Ask for confirmation (unless --yes flag is set)
    if not args.yes:
        response = input("\nDo you want to proceed? (yes/no): ").strip().lower()
        if response not in ["yes", "y"]:
            logger.info("Collection cancelled by user")
            print("Collection cancelled.")
            return
    else:
        logger.info("Auto-proceeding with --yes flag")

    print("\nStarting collection...\n")

    # Process manufacturers and collect requests
    # Don't pass limit if etld1 is specified
    limit = None if args.etld1 else args.limit
    results = await process_manufacturers_and_collect_requests(
        query_filter=query_filter,
        output_dir=output_dir,
        limit=limit,
        max_files=args.max_files,
    )

    logger.info(f"Collection results: {results}")


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
