import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional


from core.models.base_files import CSVFile
from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.services.gpt_batch_request_service import (
    find_gpt_batch_requests_by_custom_ids,
    is_batch_request_pending,
)
from core.services.deferred_manufacturer_service import (
    get_embedded_gpt_request_ids,
)
from core.utils.batch_jsonl_file_writer import (
    BatchRequestJSONLFileWriter,
    MaxFilesReachedException,
)
from core.utils.time_util import get_timestamp_str

logger = logging.getLogger(__name__)


@dataclass
class BatchFileGenerationResult:
    batch_request_jsonl_file_writer: BatchRequestJSONLFileWriter
    df_mfgs_with_orphan_custom_ids_file: CSVFile

    @property
    def final_summary(self) -> dict:
        return {
            **self.batch_request_jsonl_file_writer.result_summary,
            "df_mfgs_with_orphan_custom_ids_file": {
                "file_path": str(self.df_mfgs_with_orphan_custom_ids_file.full_path),
                "total_mfgs": self.df_mfgs_with_orphan_custom_ids_file.total_rows
                - 1,  # exclude header
            },
        }


async def iterate_df_manufacturers_and_write_batch_files(
    timestamp: datetime,
    query_filter: dict,
    output_dir: Path,
    max_requests_per_file: int,
    max_tokens_per_file: int,
    max_file_size_in_bytes: int,
    max_files: Optional[int],
    max_manufacturers: Optional[int] = None,
) -> BatchFileGenerationResult:
    # result = BatchFileGenerationResult(
    #     files=[], df_mfgs_with_orphan_custom_ids=CSVFile(output_dir=output_dir, )
    # )

    batch_request_jsonl_file_writer = BatchRequestJSONLFileWriter(
        output_dir=output_dir,
        run_timestamp=timestamp,
        max_files=max_files,
        max_requests_per_file=max_requests_per_file,
        max_tokens_per_file=max_tokens_per_file,
        max_file_size_in_bytes=max_file_size_in_bytes,
    )
    df_mfgs_with_orphan_custom_ids_file = CSVFile(
        output_dir=batch_request_jsonl_file_writer.output_dir,
        prefix="df_mfgs_with_orphan_custom_ids",
        timestamp_str=get_timestamp_str(timestamp),
        headers=["mfg_etld1", "orphan_custom_ids_count"],
    )

    try:
        logger.info("Starting iteration over DeferredManufacturers")
        count = 0
        async with (
            DeferredManufacturer.get_pymongo_collection()
            .find(query_filter)
            .sort("updated_at", -1)  # -1 = descending
            .sort("scraped_text_file_num_tokens", 1)  # 1 = ascending
        ) as cursor:
            async for df_mfg_doc in cursor:
                # logger.debug(
                #     f"Processing DeferredManufacturer {df_mfg_doc['mfg_etld1']}"
                # )
                deferred_mfg = DeferredManufacturer(**df_mfg_doc)
                custom_ids = get_embedded_gpt_request_ids(deferred_mfg)
                # logger.debug(
                #     f"Found {len(custom_ids):,} embedded GPTBatchRequest IDs in DeferredManufacturer {deferred_mfg.mfg_etld1}"
                # )
                all_requests = await find_gpt_batch_requests_by_custom_ids(
                    list(custom_ids)
                )

                found_custom_ids: set[str] = set(all_requests.keys())
                missing_custom_ids = list(custom_ids - found_custom_ids)
                if missing_custom_ids:
                    # logger.warning(
                    #     f"DeferredManufacturer {deferred_mfg.mfg_etld1} has "
                    #     f"{len(missing_custom_ids):,} orphan GPTBatchRequests"
                    # )
                    df_mfgs_with_orphan_custom_ids_file.add_csv_row(
                        [deferred_mfg.mfg_etld1, str(len(missing_custom_ids))]
                    )

                pending_request_blobs = [
                    req.request
                    for req in all_requests.values()
                    if is_batch_request_pending(req)
                ]
                batch_request_jsonl_file_writer.write_item_request_blobs(
                    item_id=deferred_mfg.mfg_etld1, request_blobs=pending_request_blobs
                )

                if pending_request_blobs:
                    count += 1
                if max_manufacturers and count >= max_manufacturers:
                    logger.info(
                        f"Reached limit of {max_manufacturers:,} DeferredManufacturers to process; stopping."
                    )
                    break
    except MaxFilesReachedException as e:
        logger.info(f"Stopping: {e}")
    except Exception as e:
        logger.error(f"Error during batch file generation: {e}")
        raise e
    finally:
        batch_request_jsonl_file_writer.current_file.close_pointer()  # all other prev files closed by writer
        df_mfgs_with_orphan_custom_ids_file.close_pointer()
        return BatchFileGenerationResult(
            batch_request_jsonl_file_writer=batch_request_jsonl_file_writer,
            df_mfgs_with_orphan_custom_ids_file=df_mfgs_with_orphan_custom_ids_file,
        )
