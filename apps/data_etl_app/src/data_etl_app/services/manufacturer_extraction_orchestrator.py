from __future__ import annotations

from datetime import datetime
import logging
import time

from core.models.extraction_results.binary_classification_result import (
    BaseClassificationDecision,
)
from data_etl_app.db_models.manufacturer import Manufacturer
from data_etl_app.db_models.deferred_manufacturer import (
    DeferredManufacturer,
)
from llm_providers.db_models.extraction_error import (
    ExtractionError,
)
from data_etl_app.models.types_and_enums import (
    BasicFieldTypeEnum,
    BinaryClassificationTypeEnum,
)
from llm_providers.models.llm_model import LLM_Model
from llm_providers.models.open_ai.gpt_model_params import (
    GPTModelParams,
)
from data_etl_app.models.s3.scraped_mfg_file import ScrapedMfgFile

from data_etl_app.services.manufacturer_service import (
    update_manufacturer,
)
from data_etl_app.services.deferred_manufacturer_service import (
    delete_deferred_manufacturer_if_empty,
    get_deferred_manufacturer_by_etld1_scraped_file_version,
)
from data_etl_app.services.gpt_batch_request_service import (
    bulk_delete_gpt_batch_requests_by_mfg_etld1_and_field,
)
from core.models.pipeline_nodes import PipelineContext, StageToggles
from core.models.chunking_strat import ChunkingStrategy
from core.models.field_types import ExtractionFieldType
from core.models.ontology import Ontology
from data_etl_app.services.prompt_service import PromptService
from data_etl_app.services.extraction_pipeline_factory import (
    ExtractionPipelineFactory,
)
from data_etl_app.services.ground_truth.binary_ground_truth_service import (
    get_binary_ground_truth,
)

from data_etl_app.utils.find_email_addresses import (
    get_validated_emails_from_text_async,
)

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
        ontology: Ontology,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        metadata_init_at: datetime,
        stage_toggles: StageToggles | None = None,
        chunk_strategy_overrides: (
            dict[ExtractionFieldType, ChunkingStrategy] | None
        ) = None,
    ):
        # Which (field, stage) pairs this orchestrator is allowed to run. None
        # means the whole chain, which is what every production path passes;
        # a partial set is a testing tool for iterating on a stage without
        # paying for the ones after it.
        self.stage_toggles = stage_toggles or StageToggles()
        if self.stage_toggles.any_disabled():
            logger.warning(
                f"⚠️  Stage-gated run: {self.stage_toggles!r}. Fields whose chain "
                f"stops early will NOT have a final result written."
            )
        self.is_manufacturer_pipeline = (
            ExtractionPipelineFactory.create_binary_classification_pipeline(
                binary_field_type=BinaryClassificationTypeEnum.is_manufacturer,
                prompt=prompt_service.is_manufacturer_prompt,
                llm_model=llm_model,
                model_params=model_params,
                ontology=ontology,
                created_at=metadata_init_at,
            )
        )
        self.business_desc_pipeline = (
            ExtractionPipelineFactory.create_business_desc_pipeline(
                prompt=prompt_service.find_business_desc_prompt,
                llm_model=llm_model,
                model_params=model_params,
                ontology=ontology,
                created_at=metadata_init_at,
            )
        )
        self.pipelines = ExtractionPipelineFactory.create_pipelines(
            prompt_service=prompt_service,
            ontology=ontology,
            llm_model=llm_model,
            model_params=model_params,
            created_at=metadata_init_at,
            chunk_strategy_overrides=chunk_strategy_overrides,
        )
        self.ontology = ontology
        self.prompt_service = prompt_service
        self.llm_model = llm_model
        self.model_params = model_params

    async def process_manufacturer(
        self,
        timestamp: datetime,
        mfg: Manufacturer,
        scraped_text_file: ScrapedMfgFile,
        eager: bool,
    ) -> None:
        """
        Main entry point: process a manufacturer and create/update deferred extraction.

        Args:
            timestamp (datetime): Current timestamp.
            mfg (Manufacturer): Manufacturer to process.
            scraped_text_file (ScrapedMfgFile): Scraped text file associated with the manufacturer.
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
                    subject=mfg,
                    deferred_subject=deferred_mfg,
                    scraped_text_file=scraped_text_file,
                    timestamp=timestamp,
                    pipeline_context=PipelineContext(
                        # Prerequisite pipeline: a blanket toggle is not aimed
                        # at it, only a toggle naming this field is.
                        stage_toggles=self.stage_toggles.explicit_only(
                            BinaryClassificationTypeEnum.is_manufacturer
                        )
                    ),
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
                        subject_unique_id=mfg.etld1,
                    )
                )
                return  # if is_manufacturer check fails, skip further processing

        if not mfg.business_desc:
            try:
                logger.info(f"Extracting business description for {mfg.etld1}")
                _t0 = time.perf_counter()
                await self.business_desc_pipeline.execute(
                    subject=mfg,
                    deferred_subject=deferred_mfg,
                    scraped_text_file=scraped_text_file,
                    timestamp=timestamp,
                    pipeline_context=PipelineContext(
                        # Prerequisite pipeline — see is_manufacturer above.
                        stage_toggles=self.stage_toggles.explicit_only(
                            BasicFieldTypeEnum.business_desc
                        )
                    ),
                    eager=eager,
                )
                field_timings["business_desc"] = time.perf_counter() - _t0
            except Exception as e:
                logger.error(f"{mfg.etld1}.business_desc errored:{e}")
                await ExtractionError.insert_one(
                    ExtractionError(
                        created_at=timestamp,
                        error=str(e),
                        field="business_desc",
                        subject_unique_id=mfg.etld1,
                    )
                )
                return  # if business_desc extraction fails, skip further processing

        if (
            not mfg.business_desc
            or not mfg.business_desc.result
            or not mfg.business_desc.result.name
        ):
            raise ValueError(
                f"Manufacturer {mfg.etld1} is missing business description result or name after extraction. "
                f"business_desc: {mfg.business_desc}"
            )

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
                        subject_unique_id=mfg.etld1,
                    )
                )

        assert (
            mfg.is_manufacturer is not None
        ), "mfg.is_manufacturer should have been set by this point"

        is_manufacturer_gt = await get_binary_ground_truth(
            mfg,
            mfg.is_manufacturer.metadata.single_stage.prompt_version_id,
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
                    subject=mfg,
                    deferred_subject=deferred_mfg,
                    scraped_text_file=scraped_text_file,
                    timestamp=timestamp,
                    pipeline_context=PipelineContext(
                        subject_name=mfg.business_desc.result.name,
                        stage_toggles=self.stage_toggles,
                    ),
                    eager=eager,
                )
                field_timings[field_type.name] = time.perf_counter() - _t0
            else:
                logger.info(
                    f"mfg=[{mfg.etld1}] ✓ Already has data for field '{field_type.name}'. Setting deferred.{field_type.name} to None..."
                )
                setattr(deferred_mfg, field_type.name, None)
                await deferred_mfg.save()
                # await bulk_delete_gpt_batch_requests_by_mfg_etld1_and_field(
                #     subject_unique_id=mfg.etld1,
                #     field_type=field_type,
                # ) TODO: uncomment

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
                contract_products=None,
                equipments=None,
                conformity_attestations=None,
                industries=None,
                process_caps=None,
                material_caps=None,
            )
            # await deferred_manufacturer.insert()

        return deferred_manufacturer
