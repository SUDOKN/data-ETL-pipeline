import asyncio
import logging
import litellm
from typing import Dict, Optional

from core.models.prompt import Prompt
from litellm_proxy_app.models.llm_model import LLM_Model

from data_etl_app.utils.prompt_s3_util import download_prompt, get_prompt_filename

logger = logging.getLogger(__name__)


PROMPT_NAMES = [
    "find_business_desc",
    "is_manufacturer",
    "is_product_manufacturer",
    "is_contract_manufacturer",
    "extract_any_address",
    # keywords
    "extract_any_product",
    # concepts
    "extract_any_certificate",
    "extract_any_industry",
    "extract_any_material_cap",
    "extract_any_process_cap",
    "certificate_evidence",
    "industry_evidence",
    "material_cap_evidence",
    "process_cap_evidence",
    "unknown_to_known_certificate",
    "unknown_to_known_industry",
    "unknown_to_known_material_cap",
    "unknown_to_known_process_cap",
]


class PromptService:
    _instance: "PromptService | None" = None
    _lock = asyncio.Lock()
    _initialized = False

    def __init__(self):
        self._prompt_cache: Dict[str, Prompt] = {}

    @classmethod
    async def get_instance(cls, llm_model: LLM_Model) -> "PromptService":
        """Get the singleton instance with lazy initialization."""
        if cls._instance is None:
            async with cls._lock:
                if cls._instance is None:
                    logger.info("Creating new PromptService singleton instance")
                    cls._instance = cls()

        # Initialize data if not already done
        if not cls._initialized:
            async with cls._lock:
                if not cls._initialized:
                    logger.info("Initializing PromptService data")
                    await cls._instance._init_data(llm_model)
                    cls._initialized = True

        return cls._instance

    async def _init_data(self, llm_model: LLM_Model) -> None:
        """Initialize prompt cache by downloading from S3."""
        self.llm_model = llm_model
        self._prompt_cache = {}
        try:
            for prompt_name in PROMPT_NAMES:
                self._prompt_cache[prompt_name] = await self._download_prompt(
                    prompt_name, self.llm_model, None
                )
                logger.info(
                    f"Downloaded {prompt_name} prompt with {(self._prompt_cache[prompt_name]).num_tokens}"
                )
            logger.info("PromptService initialized and prompts loaded")
        except Exception as e:
            logger.error(f"Failed to initialize prompt service: {e}")
            raise

    async def _download_prompt(
        self, prompt_name: str, llm_model: LLM_Model, version_id: Optional[str]
    ) -> Prompt:
        prompt_file_name = get_prompt_filename(prompt_name)
        prompt_content, actual_version_id = await download_prompt(
            prompt_file_name, version_id
        )
        if version_id and actual_version_id != version_id:
            raise ValueError(
                f"Requested version ID {version_id} but got {actual_version_id} for {prompt_name}"
            )
        return Prompt(
            s3_version_id=actual_version_id,
            name=prompt_name,
            text=prompt_content,
            num_tokens=litellm.token_counter(
                model=llm_model.model_name, text=prompt_content
            ),
        )

    async def refresh(self) -> None:
        """Reload prompt data from S3."""
        logger.info("Refreshing prompt data")
        async with self._lock:
            logger.info("Lock acquired, starting prompt refresh")
            await self._init_data(self.llm_model)

    def _get_prompt(self, prompt_name: str) -> Prompt:
        """Helper method to get prompt from cache with validation."""
        if not self._initialized:
            raise RuntimeError(
                f"PromptService not initialized. Call get_instance() first."
            )

        if prompt_name not in self._prompt_cache:
            raise ValueError(f"{prompt_name} prompt not found in cache")

        return self._prompt_cache[prompt_name]

    @property
    def find_business_desc_prompt(self) -> Prompt:
        return self._get_prompt("find_business_desc")

    @property
    def is_manufacturer_prompt(self) -> Prompt:
        return self._get_prompt("is_manufacturer")

    @property
    def is_product_manufacturer_prompt(self) -> Prompt:
        return self._get_prompt("is_product_manufacturer")

    @property
    def is_contract_manufacturer_prompt(self) -> Prompt:
        return self._get_prompt("is_contract_manufacturer")

    @property
    def extract_any_address_prompt(self) -> Prompt:
        return self._get_prompt("extract_any_address")

    @property
    def extract_any_product_prompt(self) -> Prompt:
        return self._get_prompt("extract_any_product")

    @property
    def extract_any_certificate_prompt(self) -> Prompt:
        return self._get_prompt("extract_any_certificate")

    @property
    def extract_any_industry_prompt(self) -> Prompt:
        return self._get_prompt("extract_any_industry")

    @property
    def extract_any_material_cap_prompt(self) -> Prompt:
        return self._get_prompt("extract_any_material_cap")

    @property
    def extract_any_process_cap_prompt(self) -> Prompt:
        return self._get_prompt("extract_any_process_cap")

    @property
    def certificate_evidence_prompt(self) -> Prompt:
        return self._get_prompt("certificate_evidence")

    @property
    def industry_evidence_prompt(self) -> Prompt:
        return self._get_prompt("industry_evidence")

    @property
    def material_cap_evidence_prompt(self) -> Prompt:
        return self._get_prompt("material_cap_evidence")

    @property
    def process_cap_evidence_prompt(self) -> Prompt:
        return self._get_prompt("process_cap_evidence")

    @property
    def unknown_to_known_certificate_prompt(self) -> Prompt:
        return self._get_prompt("unknown_to_known_certificate")

    @property
    def unknown_to_known_industry_prompt(self) -> Prompt:
        return self._get_prompt("unknown_to_known_industry")

    @property
    def unknown_to_known_material_cap_prompt(self) -> Prompt:
        return self._get_prompt("unknown_to_known_material_cap")

    @property
    def unknown_to_known_process_cap_prompt(self) -> Prompt:
        return self._get_prompt("unknown_to_known_process_cap")


# Factory function for getting the service instance
async def get_prompt_service(llm_model: LLM_Model) -> PromptService:
    """Factory function to get the PromptService instance."""
    return await PromptService.get_instance(llm_model=llm_model)
