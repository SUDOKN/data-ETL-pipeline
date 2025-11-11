import asyncio
import logging
import csv
from pathlib import Path
from datetime import datetime
from typing import Dict, Set

from core.dependencies.load_core_env import load_core_env
from scraper_app.dependencies.load_scraper_env import load_scraper_env
from open_ai_key_app.dependencies.load_open_ai_app_env import load_open_ai_app_env
from data_etl_app.dependencies.load_data_etl_env import load_data_etl_env

# Load environment variables
load_core_env()
load_scraper_env()
load_data_etl_env()
load_open_ai_app_env()

from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.services.gpt_batch_request_service import (
    find_gpt_batch_requests_by_custom_ids,
)
from core.services.deferred_manufacturer_service import (
    get_bin_field_embedded_gpt_request_ids,
    get_basic_field_embedded_gpt_request_id,
    get_keyword_field_embedded_gpt_request_ids,
)
from core.utils.mongo_client import init_db
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CategoryTracker:
    """Track mfg_etld1 values for different categories"""

    def __init__(self):
        # Binary fields
        self.is_manufacturer_has_missing_requests: Set[str] = set()
        self.is_manufacturer_has_pending_responses: Set[str] = set()
        self.is_contract_manufacturer_has_missing_requests: Set[str] = set()
        self.is_contract_manufacturer_has_pending_responses: Set[str] = set()
        self.is_product_manufacturer_has_missing_requests: Set[str] = set()
        self.is_product_manufacturer_has_pending_responses: Set[str] = set()

        # Basic fields
        self.addresses_has_missing_requests: Set[str] = set()
        self.addresses_has_pending_responses: Set[str] = set()
        self.business_desc_has_missing_requests: Set[str] = set()
        self.business_desc_has_pending_responses: Set[str] = set()

        # Keyword fields
        self.products_has_missing_requests: Set[str] = set()
        self.products_has_pending_responses: Set[str] = set()

        # Concept fields - certificates
        self.certificates_has_missing_search_requests: Set[str] = set()
        self.certificates_has_pending_search_responses: Set[str] = set()
        self.certificates_has_null_mapping_requests: Set[str] = set()
        self.certificates_has_missing_mapping_requests: Set[str] = set()
        self.certificates_has_pending_mapping_responses: Set[str] = set()

        # Concept fields - industries
        self.industries_has_missing_search_requests: Set[str] = set()
        self.industries_has_pending_search_responses: Set[str] = set()
        self.industries_has_null_mapping_requests: Set[str] = set()
        self.industries_has_missing_mapping_requests: Set[str] = set()
        self.industries_has_pending_mapping_responses: Set[str] = set()

        # Concept fields - process_caps
        self.process_caps_has_missing_search_requests: Set[str] = set()
        self.process_caps_has_pending_search_responses: Set[str] = set()
        self.process_caps_has_null_mapping_requests: Set[str] = set()
        self.process_caps_has_missing_mapping_requests: Set[str] = set()
        self.process_caps_has_pending_mapping_responses: Set[str] = set()

        # Concept fields - material_caps
        self.material_caps_has_missing_search_requests: Set[str] = set()
        self.material_caps_has_pending_search_responses: Set[str] = set()
        self.material_caps_has_null_mapping_requests: Set[str] = set()
        self.material_caps_has_missing_mapping_requests: Set[str] = set()
        self.material_caps_has_pending_mapping_responses: Set[str] = set()

        # Counters per field type
        self.field_counters: Dict[str, int] = {
            "is_manufacturer": 0,
            "is_contract_manufacturer": 0,
            "is_product_manufacturer": 0,
            "addresses": 0,
            "business_desc": 0,
            "products": 0,
            "certificates": 0,
            "industries": 0,
            "process_caps": 0,
            "material_caps": 0,
        }


async def process_binary_field(
    df_mfg: DeferredManufacturer,
    field_name: str,
    tracker: CategoryTracker,
):
    """Process a binary classification field"""
    field_data = getattr(df_mfg, field_name)
    if not field_data:
        return

    tracker.field_counters[field_name] += 1

    # Get embedded request IDs
    request_ids = get_bin_field_embedded_gpt_request_ids(field_data)

    # Lookup existing requests
    request_map = await find_gpt_batch_requests_by_custom_ids(list(request_ids))

    # Check for missing requests
    missing_ids = request_ids - set(request_map.keys())
    if missing_ids:
        getattr(tracker, f"{field_name}_has_missing_requests").add(df_mfg.mfg_etld1)
        return

    # Check for pending responses
    for batch_request in request_map.values():
        if batch_request.response_blob is None:
            getattr(tracker, f"{field_name}_has_pending_responses").add(
                df_mfg.mfg_etld1
            )
            return


async def process_basic_field(
    df_mfg: DeferredManufacturer,
    field_name: str,
    tracker: CategoryTracker,
):
    """Process a basic extraction field"""
    field_data = getattr(df_mfg, field_name)
    if not field_data:
        return

    tracker.field_counters[field_name] += 1

    # Get embedded request ID
    request_id = get_basic_field_embedded_gpt_request_id(field_data)

    # Lookup existing request
    request_map = await find_gpt_batch_requests_by_custom_ids([request_id])

    # Check for missing request
    if request_id not in request_map:
        getattr(tracker, f"{field_name}_has_missing_requests").add(df_mfg.mfg_etld1)
        return

    # Check for pending response
    batch_request = request_map[request_id]
    if batch_request.response_blob is None:
        getattr(tracker, f"{field_name}_has_pending_responses").add(df_mfg.mfg_etld1)


async def process_keyword_field(
    df_mfg: DeferredManufacturer,
    field_name: str,
    tracker: CategoryTracker,
):
    """Process a keyword extraction field"""
    field_data = getattr(df_mfg, field_name)
    if not field_data:
        return

    tracker.field_counters[field_name] += 1

    # Get embedded request IDs
    request_ids = get_keyword_field_embedded_gpt_request_ids(field_data)

    # Lookup existing requests
    request_map = await find_gpt_batch_requests_by_custom_ids(list(request_ids))

    # Check for missing requests
    missing_ids = request_ids - set(request_map.keys())
    if missing_ids:
        getattr(tracker, f"{field_name}_has_missing_requests").add(df_mfg.mfg_etld1)
        return

    # Check for pending responses
    for batch_request in request_map.values():
        if batch_request.response_blob is None:
            getattr(tracker, f"{field_name}_has_pending_responses").add(
                df_mfg.mfg_etld1
            )
            return


async def process_concept_field(
    df_mfg: DeferredManufacturer,
    field_name: str,
    tracker: CategoryTracker,
):
    """Process a concept extraction field"""
    field_data = getattr(df_mfg, field_name)
    if not field_data:
        return

    tracker.field_counters[field_name] += 1

    # Extract search request IDs from bundles
    search_request_ids: Set[GPTBatchRequestCustomID] = set()
    for bundle in field_data.chunk_request_bundle_map.values():
        search_request_ids.add(bundle.llm_search_request_id)

    # Process search requests
    if search_request_ids:
        search_request_map = await find_gpt_batch_requests_by_custom_ids(
            list(search_request_ids)
        )

        # Check for missing search requests
        missing_search_ids = search_request_ids - set(search_request_map.keys())
        if missing_search_ids:
            getattr(tracker, f"{field_name}_has_missing_search_requests").add(
                df_mfg.mfg_etld1
            )
            return

        # Check for pending search responses
        for batch_request in search_request_map.values():
            if batch_request.response_blob is None:
                getattr(tracker, f"{field_name}_has_pending_search_responses").add(
                    df_mfg.mfg_etld1
                )
                return

    # Process mapping request
    if field_data.llm_mapping_request_id is None:
        getattr(tracker, f"{field_name}_has_null_mapping_requests").add(
            df_mfg.mfg_etld1
        )
        return

    # Lookup mapping request
    mapping_request_map = await find_gpt_batch_requests_by_custom_ids(
        [field_data.llm_mapping_request_id]
    )

    # Check for missing mapping request
    if field_data.llm_mapping_request_id not in mapping_request_map:
        getattr(tracker, f"{field_name}_has_missing_mapping_requests").add(
            df_mfg.mfg_etld1
        )
        return

    # Check for pending mapping response
    batch_request = mapping_request_map[field_data.llm_mapping_request_id]
    if batch_request.response_blob is None:
        getattr(tracker, f"{field_name}_has_pending_mapping_responses").add(
            df_mfg.mfg_etld1
        )


def print_progress(tracker: CategoryTracker, processed_count: int):
    """Print progress every 1000 documents"""
    logger.info(f"\n{'='*80}")
    logger.info(f"Progress: {processed_count} deferred manufacturers processed")
    logger.info(f"{'='*80}")

    # Binary fields
    logger.info("\nBinary Classification Fields:")
    for field in [
        "is_manufacturer",
        "is_contract_manufacturer",
        "is_product_manufacturer",
    ]:
        logger.info(f"  {field}:")
        logger.info(f"    Total processed: {tracker.field_counters[field]}")
        logger.info(
            f"    Missing requests: {len(getattr(tracker, f'{field}_has_missing_requests'))}"
        )
        logger.info(
            f"    Pending responses: {len(getattr(tracker, f'{field}_has_pending_responses'))}"
        )

    # Basic fields
    logger.info("\nBasic Extraction Fields:")
    for field in ["addresses", "business_desc"]:
        logger.info(f"  {field}:")
        logger.info(f"    Total processed: {tracker.field_counters[field]}")
        logger.info(
            f"    Missing requests: {len(getattr(tracker, f'{field}_has_missing_requests'))}"
        )
        logger.info(
            f"    Pending responses: {len(getattr(tracker, f'{field}_has_pending_responses'))}"
        )

    # Keyword fields
    logger.info("\nKeyword Extraction Fields:")
    logger.info(f"  products:")
    logger.info(f"    Total processed: {tracker.field_counters['products']}")
    logger.info(f"    Missing requests: {len(tracker.products_has_missing_requests)}")
    logger.info(f"    Pending responses: {len(tracker.products_has_pending_responses)}")

    # Concept fields
    logger.info("\nConcept Extraction Fields:")
    for field in ["certificates", "industries", "process_caps", "material_caps"]:
        logger.info(f"  {field}:")
        logger.info(f"    Total processed: {tracker.field_counters[field]}")
        logger.info(
            f"    Missing search requests: {len(getattr(tracker, f'{field}_has_missing_search_requests'))}"
        )
        logger.info(
            f"    Pending search responses: {len(getattr(tracker, f'{field}_has_pending_search_responses'))}"
        )
        logger.info(
            f"    Null mapping requests: {len(getattr(tracker, f'{field}_has_null_mapping_requests'))}"
        )
        logger.info(
            f"    Missing mapping requests: {len(getattr(tracker, f'{field}_has_missing_mapping_requests'))}"
        )
        logger.info(
            f"    Pending mapping responses: {len(getattr(tracker, f'{field}_has_pending_mapping_responses'))}"
        )

    logger.info(f"{'='*80}\n")


def save_to_csv(category_name: str, mfg_etld1_set: Set[str], output_dir: Path):
    """Save a set of mfg_etld1 values to a CSV file"""
    csv_path = output_dir / f"{category_name}.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["mfg_etld1"])
        for mfg_etld1 in sorted(mfg_etld1_set):
            writer.writerow([mfg_etld1])
    logger.info(f"Saved {len(mfg_etld1_set)} entries to {csv_path}")


async def analyze_deferred_manufacturers():
    """Main analysis function"""
    logger.info("Starting analysis of DeferredManufacturer documents...")

    # Create output directory with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(f"deferred_mfg_analysis_{timestamp}")
    output_dir.mkdir(exist_ok=True)
    logger.info(f"Output directory: {output_dir}")

    tracker = CategoryTracker()
    processed_count = 0

    # Query deferred manufacturers with < 200,000 tokens
    logger.info("Querying deferred manufacturers with < 200,000 tokens...")
    cursor = DeferredManufacturer.find(
        {"scraped_text_file_num_tokens": {"$lt": 200_000}}
    )

    async for df_mfg in cursor:
        processed_count += 1

        # Process binary fields
        await process_binary_field(df_mfg, "is_manufacturer", tracker)
        await process_binary_field(df_mfg, "is_contract_manufacturer", tracker)
        await process_binary_field(df_mfg, "is_product_manufacturer", tracker)

        # Process basic fields
        await process_basic_field(df_mfg, "addresses", tracker)
        await process_basic_field(df_mfg, "business_desc", tracker)

        # Process keyword fields
        await process_keyword_field(df_mfg, "products", tracker)

        # Process concept fields
        await process_concept_field(df_mfg, "certificates", tracker)
        await process_concept_field(df_mfg, "industries", tracker)
        await process_concept_field(df_mfg, "process_caps", tracker)
        await process_concept_field(df_mfg, "material_caps", tracker)

        # Print progress every 1000 documents
        if processed_count % 1000 == 0:
            print_progress(tracker, processed_count)

    # Print final progress
    logger.info("\nFinal Results:")
    print_progress(tracker, processed_count)

    # Save all categories to CSV files
    logger.info("\nSaving results to CSV files...")

    # Binary fields
    save_to_csv(
        "is_manufacturer_has_missing_requests",
        tracker.is_manufacturer_has_missing_requests,
        output_dir,
    )
    save_to_csv(
        "is_manufacturer_has_pending_responses",
        tracker.is_manufacturer_has_pending_responses,
        output_dir,
    )
    save_to_csv(
        "is_contract_manufacturer_has_missing_requests",
        tracker.is_contract_manufacturer_has_missing_requests,
        output_dir,
    )
    save_to_csv(
        "is_contract_manufacturer_has_pending_responses",
        tracker.is_contract_manufacturer_has_pending_responses,
        output_dir,
    )
    save_to_csv(
        "is_product_manufacturer_has_missing_requests",
        tracker.is_product_manufacturer_has_missing_requests,
        output_dir,
    )
    save_to_csv(
        "is_product_manufacturer_has_pending_responses",
        tracker.is_product_manufacturer_has_pending_responses,
        output_dir,
    )

    # Basic fields
    save_to_csv(
        "addresses_has_missing_requests",
        tracker.addresses_has_missing_requests,
        output_dir,
    )
    save_to_csv(
        "addresses_has_pending_responses",
        tracker.addresses_has_pending_responses,
        output_dir,
    )
    save_to_csv(
        "business_desc_has_missing_requests",
        tracker.business_desc_has_missing_requests,
        output_dir,
    )
    save_to_csv(
        "business_desc_has_pending_responses",
        tracker.business_desc_has_pending_responses,
        output_dir,
    )

    # Keyword fields
    save_to_csv(
        "products_has_missing_requests",
        tracker.products_has_missing_requests,
        output_dir,
    )
    save_to_csv(
        "products_has_pending_responses",
        tracker.products_has_pending_responses,
        output_dir,
    )

    # Concept fields - certificates
    save_to_csv(
        "certificates_has_missing_search_requests",
        tracker.certificates_has_missing_search_requests,
        output_dir,
    )
    save_to_csv(
        "certificates_has_pending_search_responses",
        tracker.certificates_has_pending_search_responses,
        output_dir,
    )
    save_to_csv(
        "certificates_has_null_mapping_requests",
        tracker.certificates_has_null_mapping_requests,
        output_dir,
    )
    save_to_csv(
        "certificates_has_missing_mapping_requests",
        tracker.certificates_has_missing_mapping_requests,
        output_dir,
    )
    save_to_csv(
        "certificates_has_pending_mapping_responses",
        tracker.certificates_has_pending_mapping_responses,
        output_dir,
    )

    # Concept fields - industries
    save_to_csv(
        "industries_has_missing_search_requests",
        tracker.industries_has_missing_search_requests,
        output_dir,
    )
    save_to_csv(
        "industries_has_pending_search_responses",
        tracker.industries_has_pending_search_responses,
        output_dir,
    )
    save_to_csv(
        "industries_has_null_mapping_requests",
        tracker.industries_has_null_mapping_requests,
        output_dir,
    )
    save_to_csv(
        "industries_has_missing_mapping_requests",
        tracker.industries_has_missing_mapping_requests,
        output_dir,
    )
    save_to_csv(
        "industries_has_pending_mapping_responses",
        tracker.industries_has_pending_mapping_responses,
        output_dir,
    )

    # Concept fields - process_caps
    save_to_csv(
        "process_caps_has_missing_search_requests",
        tracker.process_caps_has_missing_search_requests,
        output_dir,
    )
    save_to_csv(
        "process_caps_has_pending_search_responses",
        tracker.process_caps_has_pending_search_responses,
        output_dir,
    )
    save_to_csv(
        "process_caps_has_null_mapping_requests",
        tracker.process_caps_has_null_mapping_requests,
        output_dir,
    )
    save_to_csv(
        "process_caps_has_missing_mapping_requests",
        tracker.process_caps_has_missing_mapping_requests,
        output_dir,
    )
    save_to_csv(
        "process_caps_has_pending_mapping_responses",
        tracker.process_caps_has_pending_mapping_responses,
        output_dir,
    )

    # Concept fields - material_caps
    save_to_csv(
        "material_caps_has_missing_search_requests",
        tracker.material_caps_has_missing_search_requests,
        output_dir,
    )
    save_to_csv(
        "material_caps_has_pending_search_responses",
        tracker.material_caps_has_pending_search_responses,
        output_dir,
    )
    save_to_csv(
        "material_caps_has_null_mapping_requests",
        tracker.material_caps_has_null_mapping_requests,
        output_dir,
    )
    save_to_csv(
        "material_caps_has_missing_mapping_requests",
        tracker.material_caps_has_missing_mapping_requests,
        output_dir,
    )
    save_to_csv(
        "material_caps_has_pending_mapping_responses",
        tracker.material_caps_has_pending_mapping_responses,
        output_dir,
    )

    logger.info(f"\n✅ Analysis complete! Results saved to {output_dir}/")
    logger.info(f"Total deferred manufacturers processed: {processed_count}")


async def main():
    await init_db()
    logger.info("Database initialized.")
    await analyze_deferred_manufacturers()


if __name__ == "__main__":
    asyncio.run(main())


"""
Last output:
================================================================================
INFO:__main__:Progress: 22291 deferred manufacturers processed
INFO:__main__:================================================================================
INFO:__main__:
Binary Classification Fields:
INFO:__main__:  is_manufacturer:
INFO:__main__:    Total processed: 2567
INFO:__main__:    Missing requests: 148
INFO:__main__:    Pending responses: 2416
INFO:__main__:  is_contract_manufacturer:
INFO:__main__:    Total processed: 12872
INFO:__main__:    Missing requests: 669
INFO:__main__:    Pending responses: 12197
INFO:__main__:  is_product_manufacturer:
INFO:__main__:    Total processed: 12857
INFO:__main__:    Missing requests: 647
INFO:__main__:    Pending responses: 12197
INFO:__main__:
Basic Extraction Fields:
INFO:__main__:  addresses:
INFO:__main__:    Total processed: 13396
INFO:__main__:    Missing requests: 243
INFO:__main__:    Pending responses: 12712
INFO:__main__:  business_desc:
INFO:__main__:    Total processed: 12900
INFO:__main__:    Missing requests: 238
INFO:__main__:    Pending responses: 12249
INFO:__main__:
Keyword Extraction Fields:
INFO:__main__:  products:
INFO:__main__:    Total processed: 13339
INFO:__main__:    Missing requests: 91
INFO:__main__:    Pending responses: 12793
INFO:__main__:
Concept Extraction Fields:
INFO:__main__:  certificates:
INFO:__main__:    Total processed: 20647
INFO:__main__:    Missing search requests: 518
INFO:__main__:    Pending search responses: 12903
INFO:__main__:    Null mapping requests: 456
INFO:__main__:    Missing mapping requests: 0
INFO:__main__:    Pending mapping responses: 6766
INFO:__main__:  industries:
INFO:__main__:    Total processed: 21997
INFO:__main__:    Missing search requests: 934
INFO:__main__:    Pending search responses: 12977
INFO:__main__:    Null mapping requests: 460
INFO:__main__:    Missing mapping requests: 0
INFO:__main__:    Pending mapping responses: 7390
INFO:__main__:  process_caps:
INFO:__main__:    Total processed: 22065
INFO:__main__:    Missing search requests: 296
INFO:__main__:    Pending search responses: 13007
INFO:__main__:    Null mapping requests: 457
INFO:__main__:    Missing mapping requests: 0
INFO:__main__:    Pending mapping responses: 7633
INFO:__main__:  material_caps:
INFO:__main__:    Total processed: 22142
INFO:__main__:    Missing search requests: 914
INFO:__main__:    Pending search responses: 12931
INFO:__main__:    Null mapping requests: 459
INFO:__main__:    Missing mapping requests: 0
INFO:__main__:    Pending mapping responses: 7315
INFO:__main__:================================================================================

INFO:__main__:
Saving results to CSV files...
INFO:__main__:Saved 148 entries to deferred_mfg_analysis_20251109_000114/is_manufacturer_has_missing_requests.csv
INFO:__main__:Saved 2416 entries to deferred_mfg_analysis_20251109_000114/is_manufacturer_has_pending_responses.csv
INFO:__main__:Saved 669 entries to deferred_mfg_analysis_20251109_000114/is_contract_manufacturer_has_missing_requests.csv
INFO:__main__:Saved 12197 entries to deferred_mfg_analysis_20251109_000114/is_contract_manufacturer_has_pending_responses.csv
INFO:__main__:Saved 647 entries to deferred_mfg_analysis_20251109_000114/is_product_manufacturer_has_missing_requests.csv
INFO:__main__:Saved 12197 entries to deferred_mfg_analysis_20251109_000114/is_product_manufacturer_has_pending_responses.csv
INFO:__main__:Saved 243 entries to deferred_mfg_analysis_20251109_000114/addresses_has_missing_requests.csv
INFO:__main__:Saved 12712 entries to deferred_mfg_analysis_20251109_000114/addresses_has_pending_responses.csv
INFO:__main__:Saved 238 entries to deferred_mfg_analysis_20251109_000114/business_desc_has_missing_requests.csv
INFO:__main__:Saved 12249 entries to deferred_mfg_analysis_20251109_000114/business_desc_has_pending_responses.csv
INFO:__main__:Saved 91 entries to deferred_mfg_analysis_20251109_000114/products_has_missing_requests.csv
INFO:__main__:Saved 12793 entries to deferred_mfg_analysis_20251109_000114/products_has_pending_responses.csv
INFO:__main__:Saved 518 entries to deferred_mfg_analysis_20251109_000114/certificates_has_missing_search_requests.csv
INFO:__main__:Saved 12903 entries to deferred_mfg_analysis_20251109_000114/certificates_has_pending_search_responses.csv
INFO:__main__:Saved 456 entries to deferred_mfg_analysis_20251109_000114/certificates_has_null_mapping_requests.csv
INFO:__main__:Saved 0 entries to deferred_mfg_analysis_20251109_000114/certificates_has_missing_mapping_requests.csv
INFO:__main__:Saved 6766 entries to deferred_mfg_analysis_20251109_000114/certificates_has_pending_mapping_responses.csv
INFO:__main__:Saved 934 entries to deferred_mfg_analysis_20251109_000114/industries_has_missing_search_requests.csv
INFO:__main__:Saved 12977 entries to deferred_mfg_analysis_20251109_000114/industries_has_pending_search_responses.csv
INFO:__main__:Saved 460 entries to deferred_mfg_analysis_20251109_000114/industries_has_null_mapping_requests.csv
INFO:__main__:Saved 0 entries to deferred_mfg_analysis_20251109_000114/industries_has_missing_mapping_requests.csv
INFO:__main__:Saved 7390 entries to deferred_mfg_analysis_20251109_000114/industries_has_pending_mapping_responses.csv
INFO:__main__:Saved 296 entries to deferred_mfg_analysis_20251109_000114/process_caps_has_missing_search_requests.csv
INFO:__main__:Saved 13007 entries to deferred_mfg_analysis_20251109_000114/process_caps_has_pending_search_responses.csv
INFO:__main__:Saved 457 entries to deferred_mfg_analysis_20251109_000114/process_caps_has_null_mapping_requests.csv
INFO:__main__:Saved 0 entries to deferred_mfg_analysis_20251109_000114/process_caps_has_missing_mapping_requests.csv
INFO:__main__:Saved 7633 entries to deferred_mfg_analysis_20251109_000114/process_caps_has_pending_mapping_responses.csv
INFO:__main__:Saved 914 entries to deferred_mfg_analysis_20251109_000114/material_caps_has_missing_search_requests.csv
INFO:__main__:Saved 12931 entries to deferred_mfg_analysis_20251109_000114/material_caps_has_pending_search_responses.csv
INFO:__main__:Saved 459 entries to deferred_mfg_analysis_20251109_000114/material_caps_has_null_mapping_requests.csv
INFO:__main__:Saved 0 entries to deferred_mfg_analysis_20251109_000114/material_caps_has_missing_mapping_requests.csv
INFO:__main__:Saved 7315 entries to deferred_mfg_analysis_20251109_000114/material_caps_has_pending_mapping_responses.csv
INFO:__main__:
✅ Analysis complete! Results saved to deferred_mfg_analysis_20251109_000114/
INFO:__main__:Total deferred manufacturers processed: 22291
"""
