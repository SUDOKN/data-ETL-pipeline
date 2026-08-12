import asyncio
import hashlib
import logging
import litellm
from typing import TYPE_CHECKING, Dict, Optional

from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.models.llm_model import LLM_Model
from infra.utils.aws.s3.prompt_s3_util import (
    PromptObject,
    download_prompt,
)

if TYPE_CHECKING:
    # Type-only, so that the assembly module — and with it every rule catalog —
    # still loads no earlier than the first call that needs a pin.
    from data_etl_app.services.prompt_assembly_service import PromptPin

logger = logging.getLogger(__name__)


STAGED_PROMPT_FILE_PATHS = {
    # phrase search
    "certificate_phrase_search": "multi_stage/1_phrase_search/certificate_phrase_search.txt",
    "industry_phrase_search": "multi_stage/1_phrase_search/industry_phrase_search.txt",
    "material_cap_phrase_search": "multi_stage/1_phrase_search/material_cap_phrase_search.txt",
    "process_cap_phrase_search": "multi_stage/1_phrase_search/process_cap_phrase_search.txt",
    "product_phrase_search": "multi_stage/1_phrase_search/product_phrase_search.txt",
    "equipment_phrase_search": "multi_stage/1_phrase_search/equipment_phrase_search.txt",
    # recursive phrase search
    "certificate_phrase_recursive_search": "multi_stage/2_phrase_recursive_search/certificate_phrase_recursive_search.txt",
    "industry_phrase_recursive_search": "multi_stage/2_phrase_recursive_search/industry_phrase_recursive_search.txt",
    "material_cap_phrase_recursive_search": "multi_stage/2_phrase_recursive_search/material_cap_phrase_recursive_search.txt",
    "process_cap_phrase_recursive_search": "multi_stage/2_phrase_recursive_search/process_cap_phrase_recursive_search.txt",
    "product_phrase_recursive_search": "multi_stage/2_phrase_recursive_search/product_phrase_recursive_search.txt",
    "equipment_phrase_recursive_search": "multi_stage/2_phrase_recursive_search/equipment_phrase_recursive_search.txt",
    # phrase relationship
    "certificate_phrase_relationship": "multi_stage/3_phrase_relationship/certificate_phrase_relationship.txt",
    "industry_phrase_relationship": "multi_stage/3_phrase_relationship/industry_phrase_relationship.txt",
    "material_cap_phrase_relationship": "multi_stage/3_phrase_relationship/material_cap_phrase_relationship.txt",
    "process_cap_phrase_relationship": "multi_stage/3_phrase_relationship/process_cap_phrase_relationship.txt",
    "product_phrase_relationship": "multi_stage/3_phrase_relationship/product_phrase_relationship.txt",
    "equipment_phrase_relationship": "multi_stage/3_phrase_relationship/equipment_phrase_relationship.txt",
    # relationship screening
    "certificate_phrase_relationship_screening": "multi_stage/4_phrase_relationship_screening/certificate_phrase_relationship_screening.txt",
    "industry_phrase_relationship_screening": "multi_stage/4_phrase_relationship_screening/industry_phrase_relationship_screening.txt",
    "material_cap_phrase_relationship_screening": "multi_stage/4_phrase_relationship_screening/material_cap_phrase_relationship_screening.txt",
    "process_cap_phrase_relationship_screening": "multi_stage/4_phrase_relationship_screening/process_cap_phrase_relationship_screening.txt",
    "product_phrase_screening_pure_product": "multi_stage/4_phrase_relationship_screening/product_phrase_screening_pure_product.txt",
    "product_phrase_screening_contract": "multi_stage/4_phrase_relationship_screening/product_phrase_screening_contract.txt",
    "equipment_phrase_relationship_screening": "multi_stage/4_phrase_relationship_screening/equipment_phrase_relationship_screening.txt",
    # freehand grounding
    # Split per manufacturing arrangement: the shared prompt asserted a
    # pure-product relationship that was false for contract work.
    "product_phrase_freehand_grounding_pure_product": "multi_stage/5_freehand_grounding/product_phrase_freehand_grounding_pure_product.txt",
    "product_phrase_freehand_grounding_contract": "multi_stage/5_freehand_grounding/product_phrase_freehand_grounding_contract.txt",
    "equipment_phrase_freehand_grounding": "multi_stage/5_freehand_grounding/equipment_phrase_freehand_grounding.txt",
    # initial grounding
    "certificate_phrase_initial_grounding": "multi_stage/5_initial_grounding/certificate_phrase_initial_grounding.txt",
    "industry_phrase_initial_grounding": "multi_stage/5_initial_grounding/industry_phrase_initial_grounding.txt",
    "material_cap_phrase_initial_grounding": "multi_stage/5_initial_grounding/material_cap_phrase_initial_grounding.txt",
    "process_cap_phrase_initial_grounding": "multi_stage/5_initial_grounding/process_cap_phrase_initial_grounding.txt",
    # recursive grounding
    "certificate_phrase_recursive_grounding": "multi_stage/6_recursive_grounding/certificate_phrase_recursive_grounding.txt",
    "industry_phrase_recursive_grounding": "multi_stage/6_recursive_grounding/industry_phrase_recursive_grounding.txt",
    "material_cap_phrase_recursive_grounding": "multi_stage/6_recursive_grounding/material_cap_phrase_recursive_grounding.txt",
    "process_cap_phrase_recursive_grounding": "multi_stage/6_recursive_grounding/process_cap_phrase_recursive_grounding.txt",
}


SINGLE_STAGE_PROMPT_FILE_PATHS = {
    "find_business_desc": "single_stage/find_business_desc.txt",
    "is_manufacturer": "single_stage/is_manufacturer.txt",
    "is_product_manufacturer": "single_stage/is_product_manufacturer.txt",
    "is_contract_manufacturer": "single_stage/is_contract_manufacturer.txt",
    "extract_any_address": "single_stage/extract_any_address.txt",
}


PROMPT_NAMES = [
    "find_business_desc",
    "is_manufacturer",
    "is_product_manufacturer",
    "is_contract_manufacturer",
    "extract_any_address",
    *STAGED_PROMPT_FILE_PATHS.keys(),
]


class PromptProvenanceError(Exception):
    """A prompt read from S3 is not the one its rule catalog describes.

    Raised at init, before any LLM spend: the catalog in the deployed code is what
    ``core`` validates ``applied_rules`` against, so a prompt rendered from a
    different catalog would have every rule report checked against the wrong rule
    set — silently, and in a way that corrupts the persisted results rather than
    failing them.
    """


def _prompt_pins() -> Dict[str, "PromptPin"]:
    """``prompt_name -> PromptPin`` for every prompt with a published record.

    Pinning is what makes a run reproducible: without it the service fetches
    whatever is currently latest, so a prompt could change under a resumed
    extraction. That applies to the hand-written prompts (search, recursive search,
    relationship, single-stage) exactly as it does to the catalog-derived ones —
    they went unpinned until 2026-08-11 only because nothing published them, and an
    edit to a relationship prompt sat on one machine while every run read the
    superseded S3 copy.

    A catalog present but unpublished falls back to latest and is still
    provenance-checked against whatever comes back. A static prompt missing from
    the pin file falls back to latest with nothing to check it against, which is
    what the warning below is for.
    """
    # Imported here rather than at module scope so a catalog problem cannot stop
    # the module from importing.
    from data_etl_app.services.prompt_assembly_service import load_prompt_pins

    pins = load_prompt_pins()
    unpinned = [
        name
        for name in PROMPT_NAMES
        if name not in pins or pins[name].s3_version_id is None
    ]
    if unpinned:
        logger.warning(
            "%d prompt(s) have no published S3 version and will be fetched as "
            "'latest', so runs using them are not reproducible: %s. "
            "Run `assemble_prompts.py adopt` (or `publish`) to pin them.",
            len(unpinned),
            ", ".join(sorted(unpinned)),
        )
    return pins


def _verify_provenance(
    prompt_name: str, obj: PromptObject, pin: "PromptPin"
) -> None:
    """Check that the bytes in hand are the ones this prompt's record published.

    Four separate failures, because no one of them implies the others: a catalog
    version can be bumped without changing the rendered text, and the text can
    change without the version being bumped (a skeleton edit does exactly that).

    A hand-written prompt carries no catalog version, so for it the digest is the
    whole check rather than one of two. That is not a weaker guarantee about the
    bytes — a digest match is a digest match — only a narrower one: it proves the
    object is what was published, and says nothing about which rule set the
    deployed code will validate against, because a static prompt has no rules.
    """
    expects_catalog = pin.catalog_version is not None

    if obj.rendered_sha256 is None or (expects_catalog and obj.catalog_version is None):
        raise PromptProvenanceError(
            f"{prompt_name}: S3 object {obj.version_id} carries no provenance "
            f"stamp, so it cannot be shown to match what was published. "
            f"Re-publish it with "
            f"`assemble_prompts.py publish --force --only {prompt_name}`."
        )

    if expects_catalog and obj.catalog_version != pin.catalog_version:
        raise PromptProvenanceError(
            f"{prompt_name}: S3 object {obj.version_id} was rendered from catalog "
            f"{obj.catalog_version}, but the deployed catalog is "
            f"{pin.catalog_version}. Publish the catalog, or deploy the code "
            f"matching the published prompt."
        )

    if not expects_catalog and obj.catalog_version is not None:
        raise PromptProvenanceError(
            f"{prompt_name}: S3 object {obj.version_id} was rendered from catalog "
            f"{obj.catalog_version}, but this prompt is pinned as hand-written. "
            f"The prompt gained a catalog after it was pinned: delete its entry "
            f"from the static pin file so the catalog's own record is what a run "
            f"reads."
        )

    if pin.rendered_sha256 and obj.rendered_sha256 != pin.rendered_sha256:
        recorded_by = "the catalog" if expects_catalog else "the static pin file"
        raise PromptProvenanceError(
            f"{prompt_name}: S3 object {obj.version_id} is stamped with digest "
            f"{obj.rendered_sha256[:12]} but {recorded_by} recorded "
            f"{pin.rendered_sha256[:12]} at publish time. The prompt was "
            f"re-uploaded out of band."
        )

    actual = hashlib.sha256(obj.text.encode("utf-8")).hexdigest()
    if actual != obj.rendered_sha256:
        raise PromptProvenanceError(
            f"{prompt_name}: S3 object {obj.version_id} does not hash to its own "
            f"stamp (got {actual[:12]}, stamped {obj.rendered_sha256[:12]}). The "
            f"object body was modified after it was stamped."
        )


class PromptService:
    # One cached instance per model name so that token counts remain correct when
    # callers switch between LLM models.
    _instances: dict[str, "PromptService"] = {}
    _lock = asyncio.Lock()

    def __init__(self):
        self._prompt_cache: Dict[str, Prompt] = {}
        self.llm_model: LLM_Model
        self._initialized = False

    @classmethod
    async def get_instance(cls, llm_model: LLM_Model) -> "PromptService":
        """Get the cached instance for the requested LLM model.

        :param llm_model: The LLM model used for token counting.
        """
        key = llm_model.name
        if key not in cls._instances:
            async with cls._lock:
                if key not in cls._instances:
                    logger.info(
                        f"Creating new PromptService instance for model={llm_model.name}"
                    )
                    service = cls()
                    await service._init_data(llm_model)
                    cls._instances[key] = service

        return cls._instances[key]

    async def _init_data(self, llm_model: LLM_Model) -> None:
        """Initialize the prompt cache by downloading from S3.

        S3 is the only source. Local development points PROMPT_BUCKET at its own
        bucket rather than taking a separate code path, so the pinning and
        provenance checks below are exercised everywhere they are relied on.
        """
        self.llm_model = llm_model
        self._prompt_cache = {}
        self._initialized = False
        pins = _prompt_pins()
        try:
            for prompt_name in PROMPT_NAMES:
                self._prompt_cache[prompt_name] = await self._download_prompt(
                    prompt_name,
                    self.llm_model,
                    pins.get(prompt_name),
                )
                logger.info(
                    f"Loaded {prompt_name} prompt from S3 with {(self._prompt_cache[prompt_name]).num_tokens} tokens"
                )
            logger.info("PromptService initialized and prompts loaded from S3")
            self._initialized = True
        except Exception as e:
            logger.error(f"Failed to initialize prompt service: {e}")
            raise

    async def _download_prompt(
        self, prompt_name: str, llm_model: LLM_Model, pin: Optional["PromptPin"]
    ) -> Prompt:

        prompt_file_name = self._get_prompt_file_path(prompt_name)
        if pin is not None and pin.s3_key != prompt_file_name:
            raise PromptProvenanceError(
                f"{prompt_name}: published to {pin.s3_key} but this service reads "
                f"{prompt_file_name}. The publish-side key and the runtime map have "
                f"drifted, which leaves one of them pointing at a prompt nobody "
                f"maintains."
            )

        version_id = pin.s3_version_id if pin else None
        obj = await download_prompt(prompt_file_name, version_id)
        if version_id and obj.version_id != version_id:
            raise ValueError(
                f"Requested version ID {version_id} but got {obj.version_id} for {prompt_name}"
            )
        # A prompt with no pin has never been published and falls back to latest,
        # so there is nothing recorded to check the bytes against.
        if pin is not None:
            _verify_provenance(prompt_name, obj, pin)
        return Prompt(
            s3_version_id=obj.version_id,
            name=prompt_name,
            text=obj.text,
            num_tokens=litellm.token_counter(model=llm_model.name, text=obj.text),
            catalog_version=obj.catalog_version,
        )

    def _get_prompt_file_path(self, prompt_name: str) -> str:
        return (
            STAGED_PROMPT_FILE_PATHS.get(prompt_name)
            or SINGLE_STAGE_PROMPT_FILE_PATHS.get(prompt_name)
            or f"{prompt_name}.txt"
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
                "PromptService not initialized. Call get_instance() first."
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
    def product_phrase_search_prompt(self) -> Prompt:
        return self._get_prompt("product_phrase_search")

    @property
    def product_phrase_recursive_search_prompt(self) -> Prompt:
        return self._get_prompt("product_phrase_recursive_search")

    @property
    def product_phrase_relationship_prompt(self) -> Prompt:
        return self._get_prompt("product_phrase_relationship")

    @property
    def product_phrase_screening_pure_product_prompt(self) -> Prompt:
        return self._get_prompt("product_phrase_screening_pure_product")

    @property
    def product_phrase_screening_contract_prompt(self) -> Prompt:
        return self._get_prompt("product_phrase_screening_contract")

    @property
    def product_phrase_freehand_grounding_pure_product_prompt(self) -> Prompt:
        return self._get_prompt("product_phrase_freehand_grounding_pure_product")

    @property
    def product_phrase_freehand_grounding_contract_prompt(self) -> Prompt:
        return self._get_prompt("product_phrase_freehand_grounding_contract")

    @property
    def equipment_phrase_search_prompt(self) -> Prompt:
        return self._get_prompt("equipment_phrase_search")

    @property
    def equipment_phrase_recursive_search_prompt(self) -> Prompt:
        return self._get_prompt("equipment_phrase_recursive_search")

    @property
    def equipment_phrase_relationship_prompt(self) -> Prompt:
        return self._get_prompt("equipment_phrase_relationship")

    @property
    def equipment_phrase_relationship_screening_prompt(self) -> Prompt:
        return self._get_prompt("equipment_phrase_relationship_screening")

    @property
    def equipment_phrase_freehand_grounding_prompt(self) -> Prompt:
        return self._get_prompt("equipment_phrase_freehand_grounding")

    @property
    def certificate_phrase_search_prompt(self) -> Prompt:
        return self._get_prompt("certificate_phrase_search")

    @property
    def industry_phrase_search_prompt(self) -> Prompt:
        return self._get_prompt("industry_phrase_search")

    @property
    def material_cap_phrase_search_prompt(self) -> Prompt:
        return self._get_prompt("material_cap_phrase_search")

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
    def certificate_phrase_recursive_search_prompt(self) -> Prompt:
        return self._get_prompt("certificate_phrase_recursive_search")

    @property
    def industry_phrase_recursive_search_prompt(self) -> Prompt:
        return self._get_prompt("industry_phrase_recursive_search")

    @property
    def material_cap_phrase_recursive_search_prompt(self) -> Prompt:
        return self._get_prompt("material_cap_phrase_recursive_search")

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
    def certificate_phrase_initial_grounding_prompt(self) -> Prompt:
        return self._get_prompt("certificate_phrase_initial_grounding")

    @property
    def industry_phrase_initial_grounding_prompt(self) -> Prompt:
        return self._get_prompt("industry_phrase_initial_grounding")

    @property
    def material_cap_phrase_initial_grounding_prompt(self) -> Prompt:
        return self._get_prompt("material_cap_phrase_initial_grounding")

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
async def get_prompt_service(llm_model: LLM_Model) -> PromptService:
    """Factory function to get the PromptService instance."""
    return await PromptService.get_instance(llm_model=llm_model)
