import asyncio
import logging
import litellm
from typing import Dict, Optional

from core.models.prompt import Prompt
from core.models.llm_model import LLM_Model
from data_etl_app.utils.prompt_s3_util import download_prompt, get_prompt_filename
from data_etl_app.utils.prompt_local_util import read_local_prompt

logger = logging.getLogger(__name__)


STAGED_PROMPT_FILE_PATHS = {
    # phrase search
    "certificate_phrase_search": "1_phrase_search/certificate_phrase_search.txt",
    "industry_phrase_search": "1_phrase_search/industry_phrase_search.txt",
    "material_cap_phrase_search": "1_phrase_search/material_cap_phrase_search.txt",
    "process_cap_phrase_search": "1_phrase_search/process_cap_phrase_search.txt",
    "product_phrase_search": "1_phrase_search/product_phrase_search.txt",
    # recursive phrase search
    "certificate_phrase_recursive_search": "2_phrase_recursive_search/certificate_phrase_recursive_search.txt",
    "industry_phrase_recursive_search": "2_phrase_recursive_search/industry_phrase_recursive_search.txt",
    "material_cap_phrase_recursive_search": "2_phrase_recursive_search/material_cap_phrase_recursive_search.txt",
    "process_cap_phrase_recursive_search": "2_phrase_recursive_search/process_cap_phrase_recursive_search.txt",
    "product_phrase_recursive_search": "2_phrase_recursive_search/product_phrase_recursive_search.txt",
    # phrase relationship
    "certificate_phrase_relationship": "3_phrase_relationship/certificate_phrase_relationship.txt",
    "industry_phrase_relationship": "3_phrase_relationship/industry_phrase_relationship.txt",
    "material_cap_phrase_relationship": "3_phrase_relationship/material_cap_phrase_relationship.txt",
    "process_cap_phrase_relationship": "3_phrase_relationship/process_cap_phrase_relationship.txt",
    "product_phrase_relationship": "3_phrase_relationship/product_phrase_relationship.txt",
    # relationship screening
    "certificate_phrase_relationship_screening": "4_phrase_relationship_screening/certificate_phrase_relationship_screening.txt",
    "industry_phrase_relationship_screening": "4_phrase_relationship_screening/industry_phrase_relationship_screening.txt",
    "material_cap_phrase_relationship_screening": "4_phrase_relationship_screening/material_cap_phrase_relationship_screening.txt",
    "process_cap_phrase_relationship_screening": "4_phrase_relationship_screening/process_cap_phrase_relationship_screening.txt",
    "product_phrase_relationship_screening": "4_phrase_relationship_screening/product_phrase_relationship_screening.txt",
    # freehand grounding
    "product_phrase_freehand_grounding": "5_freehand_grounding/product_phrase_freehand_grounding.txt",
    # initial grounding
    "certificate_phrase_initial_grounding": "5_initial_grounding/certificate_phrase_initial_grounding.txt",
    "industry_phrase_initial_grounding": "5_initial_grounding/industry_phrase_initial_grounding.txt",
    "material_cap_phrase_initial_grounding": "5_initial_grounding/material_cap_phrase_initial_grounding.txt",
    "process_cap_phrase_initial_grounding": "5_initial_grounding/process_cap_phrase_initial_grounding.txt",
    # recursive grounding
    "certificate_phrase_recursive_grounding": "6_recursive_grounding/certificate_phrase_recursive_grounding.txt",
    "industry_phrase_recursive_grounding": "6_recursive_grounding/industry_phrase_recursive_grounding.txt",
    "material_cap_phrase_recursive_grounding": "6_recursive_grounding/material_cap_phrase_recursive_grounding.txt",
    "process_cap_phrase_recursive_grounding": "6_recursive_grounding/process_cap_phrase_recursive_grounding.txt",
}


PROMPT_NAMES = [
    "find_business_desc",
    "is_manufacturer",
    "is_product_manufacturer",
    "is_contract_manufacturer",
    "extract_any_address",
    *STAGED_PROMPT_FILE_PATHS.keys(),
]


PROMPT_ALIASES = {
    "extract_any_product": "product_phrase_search",
    "extract_any_certificate": "certificate_phrase_search",
    "extract_any_industry": "industry_phrase_search",
    "extract_any_material_cap": "material_cap_phrase_search",
    "extract_any_process_cap": "process_cap_phrase_search",
    "certificate_phrase_second_search": "certificate_phrase_recursive_search",
    "industry_phrase_second_search": "industry_phrase_recursive_search",
    "material_cap_phrase_second_search": "material_cap_phrase_recursive_search",
    "process_cap_phrase_second_search": "process_cap_phrase_recursive_search",
    "unknown_to_known_certificate": "certificate_phrase_initial_grounding",
    "unknown_to_known_industry": "industry_phrase_initial_grounding",
    "unknown_to_known_material_cap": "material_cap_phrase_initial_grounding",
    "unknown_to_known_process_cap": "process_cap_phrase_initial_grounding",
}


class PromptService:
    _instance: "PromptService | None" = None
    _lock = asyncio.Lock()
    _initialized = False

    def __init__(self):
        self._prompt_cache: Dict[str, Prompt] = {}
        self._use_local: bool = False

    @classmethod
    async def get_instance(
        cls, llm_model: LLM_Model, use_local: bool = False
    ) -> "PromptService":
        """Get the singleton instance with lazy initialization.

        :param llm_model: The LLM model used for token counting.
        :param use_local: If True, load prompts from the local prompts directory
            (LOCAL_PROMPTS_DIR) instead of downloading them from S3.
        """
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
                    await cls._instance._init_data(llm_model, use_local)
                    cls._initialized = True

        return cls._instance

    async def _init_data(self, llm_model: LLM_Model, use_local: bool = False) -> None:
        """Initialize prompt cache from local files or by downloading from S3."""
        self.llm_model = llm_model
        self._use_local = use_local
        self._prompt_cache = {}
        source = "local" if use_local else "S3"
        try:
            for prompt_name in PROMPT_NAMES:
                if use_local:
                    self._prompt_cache[prompt_name] = self._load_local_prompt(
                        prompt_name, self.llm_model
                    )
                else:
                    self._prompt_cache[prompt_name] = await self._download_prompt(
                        prompt_name, self.llm_model, None
                    )
                logger.info(
                    f"Loaded {prompt_name} prompt from {source} with {(self._prompt_cache[prompt_name]).num_tokens} tokens"
                )
            logger.info(f"PromptService initialized and prompts loaded from {source}")
        except Exception as e:
            logger.error(f"Failed to initialize prompt service: {e}")
            raise

    async def _download_prompt(
        self, prompt_name: str, llm_model: LLM_Model, version_id: Optional[str]
    ) -> Prompt:
        prompt_file_name = self._get_prompt_file_path(prompt_name)
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
            num_tokens=litellm.token_counter(model=llm_model.name, text=prompt_content),
        )

    def _load_local_prompt(self, prompt_name: str, llm_model: LLM_Model) -> Prompt:
        """Load a prompt from the local prompts directory (LOCAL_PROMPTS_DIR)."""
        prompt_file_name = self._get_prompt_file_path(prompt_name)
        prompt_content = read_local_prompt(prompt_file_name)
        return Prompt(
            s3_version_id="local",
            name=prompt_name,
            text=prompt_content,
            num_tokens=litellm.token_counter(model=llm_model.name, text=prompt_content),
        )

    def _get_prompt_file_path(self, prompt_name: str) -> str:
        return STAGED_PROMPT_FILE_PATHS.get(
            prompt_name, get_prompt_filename(prompt_name)
        )

    async def refresh(self) -> None:
        """Reload prompt data from the current source (local or S3)."""
        logger.info("Refreshing prompt data")
        async with self._lock:
            logger.info("Lock acquired, starting prompt refresh")
            await self._init_data(self.llm_model, self._use_local)

    def _get_prompt(self, prompt_name: str) -> Prompt:
        """Helper method to get prompt from cache with validation."""
        prompt_name = PROMPT_ALIASES.get(prompt_name, prompt_name)

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
        return self._get_prompt("product_phrase_search")

    @property
    def product_phrase_search_prompt(self) -> Prompt:
        return self._get_prompt("product_phrase_search")

    @property
    def product_phrase_recursive_search_prompt(self) -> Prompt:
        return self._get_prompt("product_phrase_recursive_search")

    @property
    def product_phrase_relationship_prompt(self) -> Prompt:
        return self._get_prompt("product_phrase_relationship")

    @property
    def product_phrase_relationship_screening_prompt(self) -> Prompt:
        return self._get_prompt("product_phrase_relationship_screening")

    @property
    def product_phrase_freehand_grounding_prompt(self) -> Prompt:
        return self._get_prompt("product_phrase_freehand_grounding")

    @property
    def extract_any_certificate_prompt(self) -> Prompt:
        return self._get_prompt("certificate_phrase_search")

    @property
    def certificate_phrase_search_prompt(self) -> Prompt:
        return self._get_prompt("certificate_phrase_search")

    @property
    def extract_any_industry_prompt(self) -> Prompt:
        return self._get_prompt("industry_phrase_search")

    @property
    def industry_phrase_search_prompt(self) -> Prompt:
        return self._get_prompt("industry_phrase_search")

    @property
    def extract_any_material_cap_prompt(self) -> Prompt:
        return self._get_prompt("material_cap_phrase_search")

    @property
    def material_cap_phrase_search_prompt(self) -> Prompt:
        return self._get_prompt("material_cap_phrase_search")

    @property
    def extract_any_process_cap_prompt(self) -> Prompt:
        return self._get_prompt("process_cap_phrase_search")

    @property
    def process_cap_phrase_search_prompt(self) -> Prompt:
        return self._get_prompt("process_cap_phrase_search")

    @property
    def certificate_phrase_relationship_prompt(self) -> Prompt:
        return self._get_prompt("certificate_phrase_relationship")

    @property
    def industry_phrase_relationship_prompt(self) -> Prompt:
        return self._get_prompt("industry_phrase_relationship")

    @property
    def material_cap_phrase_relationship_prompt(self) -> Prompt:
        return self._get_prompt("material_cap_phrase_relationship")

    @property
    def process_cap_phrase_relationship_prompt(self) -> Prompt:
        return self._get_prompt("process_cap_phrase_relationship")

    @property
    def certificate_phrase_second_search_prompt(self) -> Prompt:
        return self._get_prompt("certificate_phrase_recursive_search")

    @property
    def certificate_phrase_recursive_search_prompt(self) -> Prompt:
        return self._get_prompt("certificate_phrase_recursive_search")

    @property
    def industry_phrase_second_search_prompt(self) -> Prompt:
        return self._get_prompt("industry_phrase_recursive_search")

    @property
    def industry_phrase_recursive_search_prompt(self) -> Prompt:
        return self._get_prompt("industry_phrase_recursive_search")

    @property
    def material_cap_phrase_second_search_prompt(self) -> Prompt:
        return self._get_prompt("material_cap_phrase_recursive_search")

    @property
    def material_cap_phrase_recursive_search_prompt(self) -> Prompt:
        return self._get_prompt("material_cap_phrase_recursive_search")

    @property
    def process_cap_phrase_second_search_prompt(self) -> Prompt:
        return self._get_prompt("process_cap_phrase_recursive_search")

    @property
    def process_cap_phrase_recursive_search_prompt(self) -> Prompt:
        return self._get_prompt("process_cap_phrase_recursive_search")

    @property
    def certificate_phrase_relationship_screening_prompt(self) -> Prompt:
        return self._get_prompt("certificate_phrase_relationship_screening")

    @property
    def industry_phrase_relationship_screening_prompt(self) -> Prompt:
        return self._get_prompt("industry_phrase_relationship_screening")

    @property
    def material_cap_phrase_relationship_screening_prompt(self) -> Prompt:
        return self._get_prompt("material_cap_phrase_relationship_screening")

    @property
    def process_cap_phrase_relationship_screening_prompt(self) -> Prompt:
        return self._get_prompt("process_cap_phrase_relationship_screening")

    @property
    def unknown_to_known_certificate_prompt(self) -> Prompt:
        return self._get_prompt("certificate_phrase_initial_grounding")

    @property
    def certificate_phrase_initial_grounding_prompt(self) -> Prompt:
        return self._get_prompt("certificate_phrase_initial_grounding")

    @property
    def unknown_to_known_industry_prompt(self) -> Prompt:
        return self._get_prompt("industry_phrase_initial_grounding")

    @property
    def industry_phrase_initial_grounding_prompt(self) -> Prompt:
        return self._get_prompt("industry_phrase_initial_grounding")

    @property
    def unknown_to_known_material_cap_prompt(self) -> Prompt:
        return self._get_prompt("material_cap_phrase_initial_grounding")

    @property
    def material_cap_phrase_initial_grounding_prompt(self) -> Prompt:
        return self._get_prompt("material_cap_phrase_initial_grounding")

    @property
    def unknown_to_known_process_cap_prompt(self) -> Prompt:
        return self._get_prompt("process_cap_phrase_initial_grounding")

    @property
    def process_cap_phrase_initial_grounding_prompt(self) -> Prompt:
        return self._get_prompt("process_cap_phrase_initial_grounding")

    @property
    def certificate_phrase_recursive_grounding_prompt(self) -> Prompt:
        return self._get_prompt("certificate_phrase_recursive_grounding")

    @property
    def industry_phrase_recursive_grounding_prompt(self) -> Prompt:
        return self._get_prompt("industry_phrase_recursive_grounding")

    @property
    def material_cap_phrase_recursive_grounding_prompt(self) -> Prompt:
        return self._get_prompt("material_cap_phrase_recursive_grounding")

    @property
    def process_cap_phrase_recursive_grounding_prompt(self) -> Prompt:
        return self._get_prompt("process_cap_phrase_recursive_grounding")


# Factory function for getting the service instance
async def get_prompt_service(
    llm_model: LLM_Model, use_local: bool = False
) -> PromptService:
    """Factory function to get the PromptService instance."""
    return await PromptService.get_instance(llm_model=llm_model, use_local=use_local)
