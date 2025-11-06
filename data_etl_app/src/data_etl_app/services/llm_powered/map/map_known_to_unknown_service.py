import json
import logging
from data_etl_app.services.knowledge.ontology_service import get_ontology_service
from typing_extensions import TypedDict
from typing import Optional

from core.models.field_types import MfgETLDType

from data_etl_app.models.skos_concept import Concept, ConceptJSONEncoder
from data_etl_app.models.types_and_enums import ConceptTypeEnum

from open_ai_key_app.utils.token_util import num_tokens_from_string
from open_ai_key_app.utils.ask_gpt_util import (
    ask_gpt_async,
)
from open_ai_key_app.models.gpt_model import (
    GPT_4o_mini,
    DefaultModelParameters,
)

logger = logging.getLogger(__name__)


class LLMMappingResult(TypedDict):
    known_to_unknowns: dict[Concept, set[str]]
    unmapped_unknowns: set[str]


async def map_known_concepts_with_found_keywords(
    concept_type: ConceptTypeEnum,
    mfg_etld1: str,
    known_concepts: set[Concept],  # DO NOT MUTATE
    unmatched_keywords: set[str],
    prompt_text: str,
    # gpt_model: GPTModel,
    # model_params: ModelParameters,
) -> LLMMappingResult:

    context = json.dumps(
        {"unknowns": list(unmatched_keywords), "knowns": list(known_concepts)},
        cls=ConceptJSONEncoder,
    )

    logger.debug(f"\nmapping unknown_to_known")
    logger.debug(f"context {num_tokens_from_string(context)}:{context}")

    gpt_response = await ask_gpt_async(
        context, prompt_text, GPT_4o_mini, DefaultModelParameters
    )

    raw_gpt_mapping = parse_llm_concept_mapping_result(gpt_response=gpt_response)
    return get_mapped_known_concepts_and_unmapped_keywords(
        mfg_etld1=mfg_etld1,
        known_concepts=known_concepts,
        keywords_to_map=unmatched_keywords,
        raw_gpt_mapping=raw_gpt_mapping,
        concept_type=concept_type,
    )


def parse_llm_concept_mapping_result(gpt_response: Optional[str]) -> dict[str, str]:
    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError(
            "parse_llm_concept_mapping_result: Empty or invalid response from GPT"
        )

    try:
        gpt_response = gpt_response.replace("```", "").replace("json", "")
        raw_gpt_mapping: dict[str, str] = json.loads(
            gpt_response
        )  # from unknown --> known
        logger.debug(f"raw_gpt_mapping:{json.dumps(raw_gpt_mapping, indent=2)}")
    except:
        raise ValueError(
            f"parse_llm_concept_mapping_result: Invalid response from GPT:{gpt_response}"
        )

    if not isinstance(raw_gpt_mapping, dict):
        raise ValueError(
            "parse_llm_concept_mapping_result: Expected raw_gpt_mapping to be a dictionary"
        )

    logger.debug(f"raw_gpt_mapping:{raw_gpt_mapping}")

    return raw_gpt_mapping


async def get_mapped_known_concepts_and_unmapped_keywords_in_chunk_by_concept_type(
    mfg_etld1: MfgETLDType,
    unmatched_keywords_in_chunk: set[str],
    raw_gpt_mapping: dict[str, str],
    concept_type: ConceptTypeEnum,
) -> LLMMappingResult:
    ontology_service = await get_ontology_service()
    if concept_type == ConceptTypeEnum.material_caps:
        _, known_concepts = ontology_service.material_caps
    elif concept_type == ConceptTypeEnum.process_caps:
        _, known_concepts = ontology_service.process_caps
    elif concept_type == ConceptTypeEnum.industries:
        _, known_concepts = ontology_service.industries
    elif concept_type == ConceptTypeEnum.certificates:
        _, known_concepts = ontology_service.certificates
    else:
        raise ValueError(f"Unsupported concept_type:{concept_type}")

    return get_mapped_known_concepts_and_unmapped_keywords_in_chunk(
        mfg_etld1=mfg_etld1,
        known_concepts=known_concepts,
        unmatched_keywords_in_chunk=unmatched_keywords_in_chunk,
        raw_gpt_mapping=raw_gpt_mapping,
        concept_type=concept_type,
    )


async def get_mapped_known_concepts_and_unmapped_keywords_by_concept_type(
    mfg_etld1: MfgETLDType,
    unmatched_keywords: set[str],
    raw_gpt_mapping: dict[str, str],
    concept_type: ConceptTypeEnum,
) -> LLMMappingResult:
    ontology_service = await get_ontology_service()
    if concept_type == ConceptTypeEnum.material_caps:
        _, known_concepts = ontology_service.material_caps
    elif concept_type == ConceptTypeEnum.process_caps:
        _, known_concepts = ontology_service.process_caps
    elif concept_type == ConceptTypeEnum.industries:
        _, known_concepts = ontology_service.industries
    elif concept_type == ConceptTypeEnum.certificates:
        _, known_concepts = ontology_service.certificates
    else:
        raise ValueError(f"Unsupported concept_type:{concept_type}")

    return get_mapped_known_concepts_and_unmapped_keywords(
        mfg_etld1=mfg_etld1,
        known_concepts=known_concepts,
        keywords_to_map=unmatched_keywords,
        raw_gpt_mapping=raw_gpt_mapping,
        concept_type=concept_type,
    )


def get_mapped_known_concepts_and_unmapped_keywords_in_chunk(
    mfg_etld1: MfgETLDType,
    known_concepts: set[Concept],
    unmatched_keywords_in_chunk: set[str],
    raw_gpt_mapping: dict[str, str],
    concept_type: ConceptTypeEnum,
) -> LLMMappingResult:
    return get_mapped_known_concepts_and_unmapped_keywords(
        mfg_etld1=mfg_etld1,
        known_concepts=known_concepts,
        keywords_to_map=unmatched_keywords_in_chunk,
        raw_gpt_mapping=raw_gpt_mapping,
        concept_type=concept_type,
        keywords_to_map_are_per_chunk=True,
    )


def get_mapped_known_concepts_and_unmapped_keywords(
    mfg_etld1: MfgETLDType,
    known_concepts: set[Concept],
    keywords_to_map: set[str],
    raw_gpt_mapping: dict[str, str],
    concept_type: ConceptTypeEnum,
    keywords_to_map_are_per_chunk: bool = False,
) -> LLMMappingResult:
    match_label_map: dict[str, Concept] = {
        label: k for k in known_concepts for label in k.matchLabels
    }
    known_to_unknowns: dict[Concept, set[str]] = {}

    # mapped_unknown "biotech" -> ["pharmaceutical"] mapped_knowns
    # mu ~ mapped_unknown, mk ~ mapped_knowns
    for mu, mk_label in raw_gpt_mapping.items():
        if mu not in keywords_to_map:
            # mapped_unknown was hallucinated, in which case we will skip this map
            # and raise a warning but only if we are not processing per-chunk
            if not keywords_to_map_are_per_chunk:
                logger.warning(
                    f"WARNING: {mfg_etld1}:{concept_type.name} mapped_unknown:{mu} was not in the original unknowns list"
                )
        else:
            if mk_label:  # mk must not be null/None
                matched_known_concept = match_label_map[mk_label]
                if not matched_known_concept:
                    logger.warning(
                        f"WARNING: {mfg_etld1}:{concept_type.name} mapped_known:{mk_label} was not in the original knowns list"
                    )
                else:
                    known_to_unknowns[matched_known_concept] = known_to_unknowns.get(
                        matched_known_concept, set()
                    )
                    known_to_unknowns[matched_known_concept].add(mu)

    # Derive unmapped_unknowns
    # mus ~ mapped_unknowns
    unmapped_unknowns = keywords_to_map.copy()
    for mk, mus in known_to_unknowns.items():
        logger.debug(f"removing mapped_known:{mk}")
        logger.debug(f"removing mapped_unknowns:{mus}")
        unmapped_unknowns -= (
            mus  # NOTE: unknowns may contain hallucinations, but subtraction is safe
        )

    # pack everything and send back
    final_bundle: LLMMappingResult = {
        "known_to_unknowns": known_to_unknowns,  # actively used
        "unmapped_unknowns": unmapped_unknowns,
    }

    return final_bundle
