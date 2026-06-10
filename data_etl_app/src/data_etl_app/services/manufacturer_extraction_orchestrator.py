from datetime import datetime
import logging
import time

from core.models.binary_classification_result import (
    BaseClassificationDecision,
)
from core.models.db.manufacturer import Manufacturer
from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.db.extraction_error import ExtractionError
from data_etl_app.models.pipeline_nodes.classification.binary_reconcile_node import (
    BinaryClassificationTypeEnum,
)
from litellm_proxy_app.models.llm_model import LLM_Model
from open_ai_key_app.models.gpt_model_params import GPTModelParams
from scraper_app.models.scraped_text_file import ScrapedTextFile

from core.services.manufacturer_service import (
    update_manufacturer,
)
from core.services.deferred_manufacturer_service import (
    delete_deferred_manufacturer_if_empty,
    get_deferred_manufacturer_by_etld1_scraped_file_version,
)
from core.services.gpt_batch_request_writes import (
    bulk_delete_gpt_batch_requests_by_mfg_etld1_and_field,
)
from data_etl_app.services.knowledge.ontology_service import OntologyService
from data_etl_app.services.knowledge.prompt_service import PromptService
from data_etl_app.services.extraction_pipeline_factory import ExtractionPipelineFactory
from data_etl_app.services.ground_truth.binary_ground_truth_service import (
    get_binary_ground_truth,
)

from data_etl_app.utils.find_email_addresses import get_validated_emails_from_text_async

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


class ManufacturerExtractionOrchestrator:
    """
    Orchestrates the intake of manufacturers into the deferred extraction pipeline.
    Determines what's incomplete and creates appropriate batch requests.
    """

    def __init__(
        self,
        prompt_service: PromptService,
        ontology_service: OntologyService,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
    ):
        self.is_manufacturer_pipeline = (
            ExtractionPipelineFactory.create_binary_classification_pipeline(
                binary_field_type=BinaryClassificationTypeEnum.is_manufacturer,
                prompt=prompt_service.is_manufacturer_prompt,
            )
        )
        self.pipelines = ExtractionPipelineFactory.create_pipelines(
            prompt_service=prompt_service,
            ontology_service=ontology_service,
        )
        self.llm_model = llm_model
        self.model_params = model_params

    async def process_manufacturer(
        self,
        timestamp: datetime,
        mfg: Manufacturer,
        scraped_text_file: ScrapedTextFile,
        eager: bool,
    ) -> None:
        """
        Main entry point: process a manufacturer and create/update deferred extraction.

        Args:
            timestamp (datetime): Current timestamp.
            mfg (Manufacturer): Manufacturer to process.
            scraped_text_file (ScrapedTextFile): Scraped text file associated with the manufacturer.
            eager (bool): Whether to process eagerly or not.
        """

        overall_start = time.perf_counter()
        field_timings: dict[str, float] = {}

        # Create or update the deferred manufacturer
        deferred_mfg = await self._get_or_create_deferred(timestamp, mfg)

        logger.debug(
            f"[{mfg.etld1}] Starting extraction pipeline with {len(self.pipelines)} field types"
        )

        if not mfg.is_manufacturer:
            try:
                logger.info(f"Finding out if company {mfg.etld1} is a manufacturer.")
                _t0 = time.perf_counter()
                await self.is_manufacturer_pipeline.execute(
                    mfg=mfg,
                    deferred_mfg=deferred_mfg,
                    scraped_text_file=scraped_text_file,
                    timestamp=timestamp,
                    pipeline_context={},
                    llm_model=self.llm_model,
                    model_params=self.model_params,
                    eager=eager,
                )
                field_timings["is_manufacturer"] = time.perf_counter() - _t0
            except Exception as e:
                logger.error(f"{mfg.etld1}.is_manufacturer errored:{e}")
                await ExtractionError.insert_one(
                    ExtractionError(
                        created_at=timestamp,
                        error=str(e),
                        field="is_manufacturer",
                        mfg_etld1=mfg.etld1,
                    )
                )
                return  # if is_manufacturer check fails, skip further processing

        if not mfg.email_addresses:
            try:
                logger.info(f"Extracting email addresses for {mfg.etld1}")
                _t0 = time.perf_counter()
                mfg.email_addresses = await get_validated_emails_from_text_async(
                    mfg.etld1, scraped_text_file.text
                )
                field_timings["email_addresses"] = time.perf_counter() - _t0
                await update_manufacturer(
                    updated_at=timestamp,
                    manufacturer=mfg,
                )
            except Exception as e:
                logger.error(f"{mfg.name}.email_addresses errored:{e}")
                await ExtractionError.insert_one(
                    ExtractionError(
                        created_at=timestamp,
                        error=str(e),
                        field="email_addresses",
                        mfg_etld1=mfg.etld1,
                    )
                )

        assert (
            mfg.is_manufacturer is not None
        ), "mfg.is_manufacturer should have been set by this point"

        is_manufacturer_gt = await get_binary_ground_truth(
            mfg,
            mfg.is_manufacturer.metadata.prompt_version_id,
            BinaryClassificationTypeEnum.is_manufacturer,
        )

        # assert is_manufacturer_gt is not None, "is_manufacturer_gt should not be None"

        final_decision: BaseClassificationDecision = (
            is_manufacturer_gt.final_decision
            if is_manufacturer_gt and is_manufacturer_gt.final_decision
            else mfg.is_manufacturer.result
        )

        if not final_decision.answer:
            logger.info(
                f"Would have skipped further extraction for {mfg.etld1} as it is not a manufacturer, "
                f"but continuing for testing or gt purposes."
            )
            # return

        for field_type, pipeline in self.pipelines.items():
            logger.debug(
                f"[{mfg.etld1}] Processing extraction pipeline for field '{field_type.name}', stats: {mfg.scraped_text_file_num_tokens} tokens"
            )
            if not bool(getattr(mfg, field_type.name)):
                logger.info(
                    f"mfg=[{mfg.etld1}] ❌ Missing data for field '{field_type.name}'. Processing pipeline..."
                )
                _t0 = time.perf_counter()
                await pipeline.execute(
                    mfg=mfg,
                    deferred_mfg=deferred_mfg,
                    scraped_text_file=scraped_text_file,
                    timestamp=timestamp,
                    pipeline_context={},
                    eager=eager,
                    llm_model=self.llm_model,
                    model_params=self.model_params,
                )
                field_timings[field_type.name] = time.perf_counter() - _t0
            else:
                logger.info(
                    f"mfg=[{mfg.etld1}] ✓ Already has data for field '{field_type.name}'. Setting deferred.{field_type.name} to None..."
                )
                setattr(deferred_mfg, field_type.name, None)
                await deferred_mfg.save()
                await bulk_delete_gpt_batch_requests_by_mfg_etld1_and_field(
                    mfg_etld1=mfg.etld1,
                    field_type=field_type,
                )

        overall_elapsed = time.perf_counter() - overall_start
        timing_summary = ", ".join(
            f"{field}={elapsed:.2f}s" for field, elapsed in field_timings.items()
        )
        logger.info(
            f"[{mfg.etld1}] Completed extraction pipeline for all fields | "
            f"total={overall_elapsed:.2f}s | per-field: [{timing_summary}]"
        )
        if deferred_mfg.id:
            # only make a DB call if deferred_mfg exists in DB
            # which maybe because _get_or_create_deferred returned an existing instance
            # or it returned a new one, but it remained untouched (no fields set) by the pipelines
            await delete_deferred_manufacturer_if_empty(deferred_mfg)

    async def _get_or_create_deferred(
        self, timestamp: datetime, mfg: Manufacturer
    ) -> DeferredManufacturer:
        """Get existing DeferredManufacturer or create new one"""
        deferred_manufacturer = (
            await get_deferred_manufacturer_by_etld1_scraped_file_version(
                mfg_etld1=mfg.etld1,
                scraped_text_file_version_id=mfg.scraped_text_file_version_id,
            )
        )

        if deferred_manufacturer:
            return deferred_manufacturer

        if not deferred_manufacturer:
            deferred_manufacturer = DeferredManufacturer(
                created_at=timestamp,
                etld1=mfg.etld1,
                scraped_text_file_num_tokens=mfg.scraped_text_file_num_tokens,
                scraped_text_file_version_id=mfg.scraped_text_file_version_id,
                is_manufacturer=None,
                is_contract_manufacturer=None,
                is_product_manufacturer=None,
                addresses=None,
                business_desc=None,
                products=None,
                certificates=None,
                industries=None,
                process_caps=None,
                material_caps=None,
            )
            # await deferred_manufacturer.insert()

        return deferred_manufacturer
