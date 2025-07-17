import os
import logging
import threading
from typing import Dict

logger = logging.getLogger(__name__)

PROMPT_BASE_PATH = "./data_etl_app/src/data_etl_app/knowledge/prompts"
logger.debug(f"Current working directory inside prompt_service: {os.getcwd()}")


class PromptService:
    _instance: "PromptService | None" = None
    _lock = (
        threading.Lock()
    )  # not strictly necessary, but good practice for thread safety, read in notes
    _cache: Dict[str, str]

    def __new__(cls) -> "PromptService":
        # this gets called before __init__, when anyone calls PromptService()
        # it ensures that only one instance of the service is created
        # every next time, it will return the same instance
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._init_data()
        return cls._instance

    def _init_data(self) -> None:
        self._cache = {}
        for attr in [
            "_is_manufacturer_prompt",
            "_is_product_manufacturer_prompt",
            "_is_contract_manufacturer_prompt",
            "_extract_industry_prompt",
            "_unknown_to_known_industry_prompt",
            "_extract_material_prompt",
            "_unknown_to_known_material_prompt",
            "_extract_process_prompt",
            "_unknown_to_known_process_prompt",
            "_extract_certificate_prompt",
            "_unknown_to_known_certificate_prompt",
        ]:
            if hasattr(self, attr):
                delattr(self, attr)

    def refresh(self) -> None:
        """Reload ontology data from S3 and clear all cached properties."""
        with self._lock:
            self._init_data()

    @property
    def is_manufacturer_prompt(self) -> str:
        if "_is_manufacturer_prompt" not in self._cache:
            with open(f"{PROMPT_BASE_PATH}/is_manufacturer.txt", "r") as file:
                is_manufacturer_prompt = file.read()
            self._cache["_is_manufacturer_prompt"] = is_manufacturer_prompt

        return self._cache["_is_manufacturer_prompt"]

    @property
    def is_product_manufacturer_prompt(self) -> str:
        if "_is_product_manufacturer_prompt" not in self._cache:
            with open(f"{PROMPT_BASE_PATH}/is_product_manufacturer.txt", "r") as file:
                is_product_manufacturer_prompt = file.read()
            self._cache["_is_product_manufacturer_prompt"] = (
                is_product_manufacturer_prompt
            )

        return self._cache["_is_product_manufacturer_prompt"]

    @property
    def is_contract_manufacturer_prompt(self) -> str:
        if "_is_contract_manufacturer_prompt" not in self._cache:
            with open(f"{PROMPT_BASE_PATH}/is_contract_manufacturer.txt", "r") as file:
                is_contract_manufacturer_prompt = file.read()
            self._cache["_is_contract_manufacturer_prompt"] = (
                is_contract_manufacturer_prompt
            )

        return self._cache["_is_contract_manufacturer_prompt"]

    @property
    def extract_industry_prompt(self) -> str:
        if "_extract_industry_prompt" not in self._cache:
            with open(f"{PROMPT_BASE_PATH}/extract_any_industry.txt", "r") as file:
                extract_any_industry_prompt = file.read()
            self._cache["_extract_industry_prompt"] = extract_any_industry_prompt

        return self._cache["_extract_industry_prompt"]

    @property
    def unknown_to_known_industry_prompt(self) -> str:
        if "_unknown_to_known_industry_prompt" not in self._cache:
            with open(f"{PROMPT_BASE_PATH}/unknown_to_known_industry.txt", "r") as file:
                unknown_to_known_industry_prompt = file.read()
            self._cache["_unknown_to_known_industry_prompt"] = (
                unknown_to_known_industry_prompt
            )

        return self._cache["_unknown_to_known_industry_prompt"]

    @property
    def extract_material_prompt(self) -> str:
        if "_extract_material_prompt" not in self._cache:
            with open(f"{PROMPT_BASE_PATH}/extract_any_material_cap.txt", "r") as file:
                extract_any_material_prompt = file.read()
            self._cache["_extract_material_prompt"] = extract_any_material_prompt

        return self._cache["_extract_material_prompt"]

    @property
    def unknown_to_known_material_prompt(self) -> str:
        if "_unknown_to_known_material_prompt" not in self._cache:
            with open(
                f"{PROMPT_BASE_PATH}/unknown_to_known_material_cap.txt", "r"
            ) as file:
                unknown_to_known_material_prompt = file.read()
            self._cache["_unknown_to_known_material_prompt"] = (
                unknown_to_known_material_prompt
            )

        return self._cache["_unknown_to_known_material_prompt"]

    @property
    def extract_process_prompt(self) -> str:
        if "_extract_process_prompt" not in self._cache:
            with open(f"{PROMPT_BASE_PATH}/extract_any_process_cap.txt", "r") as file:
                extract_any_process_prompt = file.read()
            self._cache["_extract_process_prompt"] = extract_any_process_prompt

        return self._cache["_extract_process_prompt"]

    @property
    def unknown_to_known_process_prompt(self) -> str:
        if "_unknown_to_known_process_prompt" not in self._cache:
            with open(
                f"{PROMPT_BASE_PATH}/unknown_to_known_process_cap.txt", "r"
            ) as file:
                unknown_to_known_process_prompt = file.read()
            self._cache["_unknown_to_known_process_prompt"] = (
                unknown_to_known_process_prompt
            )

        return self._cache["_unknown_to_known_process_prompt"]

    @property
    def extract_certificate_prompt(self) -> str:
        if "_extract_certificate_prompt" not in self._cache:
            with open(f"{PROMPT_BASE_PATH}/extract_any_certificate.txt", "r") as file:
                extract_any_certificate_prompt = file.read()
            self._cache["_extract_certificate_prompt"] = extract_any_certificate_prompt

        return self._cache["_extract_certificate_prompt"]

    @property
    def unknown_to_known_certificate_prompt(self) -> str:
        if "_unknown_to_known_certificate_prompt" not in self._cache:
            with open(
                f"{PROMPT_BASE_PATH}/unknown_to_known_certificate.txt", "r"
            ) as file:
                unknown_to_known_certificate_prompt = file.read()
            self._cache["_unknown_to_known_certificate_prompt"] = (
                unknown_to_known_certificate_prompt
            )

        return self._cache["_unknown_to_known_certificate_prompt"]


prompt_service = PromptService()
