import logging
import threading
from typing import Dict, Optional

from open_ai_key_app.utils.ask_gpt_util import num_tokens_from_string
from shared.models.prompt import Prompt
from data_etl_app.utils.prompt_s3_util import download_prompt

logger = logging.getLogger(__name__)


PROMPT_NAMES = [
    "find_business_name",
    "is_manufacturer",
    "is_product_manufacturer",
    "is_contract_manufacturer",
    "extract_any_product",
    "extract_any_certificate",
    "extract_any_industry",
    "extract_any_material_cap",
    "extract_any_process_cap",
    "unknown_to_known_certificate",
    "unknown_to_known_industry",
    "unknown_to_known_material_cap",
    "unknown_to_known_process_cap",
]


class PromptService:
    _instance: "PromptService | None" = None
    _lock = (
        threading.Lock()
    )  # not strictly necessary, but good practice for thread safety, read in notes
    _prompt_cache: Dict[str, Prompt]

    def __new__(cls) -> "PromptService":
        # this gets called before __init__, when anyone calls PromptService()
        # it ensures that only one instance of the service is created
        # every next time, it will return the same instance
        logger.info("PromptService instance is None, acquiring lock for creation")
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    logger.info("Creating new PromptService singleton instance")
                    cls._instance = super().__new__(cls)
                    cls._instance._init_data()
                else:
                    logger.info(
                        "Another thread created the PromptService instance while waiting for lock"
                    )
        else:
            logger.debug("Returning existing PromptService singleton instance")
        return cls._instance

    def _init_data(self) -> None:
        self._prompt_cache = {}
        for attr in PROMPT_NAMES:
            if hasattr(self, attr):
                delattr(self, attr)
            self._prompt_cache[attr] = self._download_prompt(attr, None)
        logger.info("PromptService initialized and prompts loaded")

    def _download_prompt(
        self, prompt_filename: str, version_id: Optional[str]
    ) -> Prompt:
        prompt_content, actual_version_id = download_prompt(
            f"{prompt_filename}.txt", version_id
        )
        if version_id and actual_version_id != version_id:
            raise ValueError(
                f"Requested version ID {version_id} but got {actual_version_id} for {prompt_filename}"
            )
        return Prompt(
            s3_version_id=actual_version_id,
            name=prompt_filename,
            text=prompt_content,
            num_tokens=num_tokens_from_string(prompt_content),
        )

    def refresh(self) -> None:
        """Reload ontology data from S3 and clear all cached properties."""
        logger.info("Refreshing prompt data - acquiring lock")
        with self._lock:
            logger.info("Lock acquired, starting prompt refresh")
            self._init_data()

    @property
    def find_business_name_prompt(self) -> Prompt:
        if "find_business_name" not in self._prompt_cache:
            raise ValueError("find_business_name prompt not found in cache")

        return self._prompt_cache["find_business_name"]

    @property
    def is_manufacturer_prompt(self) -> Prompt:
        if "is_manufacturer" not in self._prompt_cache:
            raise ValueError("is_manufacturer prompt not found in cache")

        return self._prompt_cache["is_manufacturer"]

    @property
    def is_product_manufacturer_prompt(self) -> Prompt:
        if "is_product_manufacturer" not in self._prompt_cache:
            raise ValueError("is_product_manufacturer prompt not found in cache")

        return self._prompt_cache["is_product_manufacturer"]

    @property
    def is_contract_manufacturer_prompt(self) -> Prompt:
        if "is_contract_manufacturer" not in self._prompt_cache:
            raise ValueError("is_contract_manufacturer prompt not found in cache")

        return self._prompt_cache["is_contract_manufacturer"]

    @property
    def extract_any_product_prompt(self) -> Prompt:
        if "extract_any_product" not in self._prompt_cache:
            raise ValueError("extract_any_product prompt not found in cache")

        return self._prompt_cache["extract_any_product"]

    @property
    def extract_any_certificate_prompt(self) -> Prompt:
        if "extract_any_certificate" not in self._prompt_cache:
            raise ValueError("extract_any_certificate prompt not found in cache")

        return self._prompt_cache["extract_any_certificate"]

    @property
    def extract_any_industry_prompt(self) -> Prompt:
        if "extract_any_industry" not in self._prompt_cache:
            raise ValueError("extract_any_industry prompt not found in cache")

        return self._prompt_cache["extract_any_industry"]

    @property
    def extract_any_material_cap_prompt(self) -> Prompt:
        if "extract_any_material_cap" not in self._prompt_cache:
            raise ValueError("extract_any_material_cap prompt not found in cache")

        return self._prompt_cache["extract_any_material_cap"]

    @property
    def extract_any_process_cap_prompt(self) -> Prompt:
        if "extract_any_process_cap" not in self._prompt_cache:
            raise ValueError("extract_any_process_cap prompt not found in cache")

        return self._prompt_cache["extract_any_process_cap"]

    @property
    def unknown_to_known_certificate_prompt(self) -> Prompt:
        if "unknown_to_known_certificate" not in self._prompt_cache:
            raise ValueError("unknown_to_known_certificate prompt not found in cache")

        return self._prompt_cache["unknown_to_known_certificate"]

    @property
    def unknown_to_known_industry_prompt(self) -> Prompt:
        if "unknown_to_known_industry" not in self._prompt_cache:
            raise ValueError("unknown_to_known_industry prompt not found in cache")

        return self._prompt_cache["unknown_to_known_industry"]

    @property
    def unknown_to_known_material_cap_prompt(self) -> Prompt:
        if "unknown_to_known_material_cap" not in self._prompt_cache:
            raise ValueError("unknown_to_known_material_cap prompt not found in cache")

        return self._prompt_cache["unknown_to_known_material_cap"]

    @property
    def unknown_to_known_process_cap_prompt(self) -> Prompt:
        if "unknown_to_known_process_cap" not in self._prompt_cache:
            raise ValueError("unknown_to_known_process_cap prompt not found in cache")

        return self._prompt_cache["unknown_to_known_process_cap"]


prompt_service = PromptService()
