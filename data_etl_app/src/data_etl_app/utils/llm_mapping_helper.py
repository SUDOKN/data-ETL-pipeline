import json
import logging
from datetime import datetime
from typing import TypedDict

from core.models.field_types import LLMMappingResult
from core.models.prompt import Prompt
from core.models.db.gpt_batch_request import GPTBatchRequest
from data_etl_app.models.types_and_enums import ConceptTypeEnum
from data_etl_app.models.skos_concept import Concept, ConceptJSONEncoder
from open_ai_key_app.models.gpt_model import No_model
from open_ai_key_app.models.llm_model import LLM_Model
from open_ai_key_app.models.gpt_model_params import GPTModelParams

from core.services.gpt_batch_request_service import create_base_gpt_batch_request

logger = logging.getLogger(__name__)


class KnownToUnknownMap(TypedDict):
    known_to_unknowns: dict[Concept, set[str]]
    unmapped_unknowns: set[str]


def get_matched_concepts_and_unmatched_keywords(
    known_concepts: set[Concept], confirmed_keywords_w_evidence: dict[str, str]
) -> tuple[set[Concept], dict[str, str]]:
    confirmed_keywords: set[str] = set(confirmed_keywords_w_evidence.keys())
    unmatched_keywords: dict[str, str] = confirmed_keywords_w_evidence.copy()
    matched_concepts: set[Concept] = set()

    for confirmed_keyword in confirmed_keywords:
        for concept in known_concepts:
            if confirmed_keyword in concept.matchLabels:
                matched_concepts.add(concept)
                unmatched_keywords.pop(confirmed_keyword, None)

    return matched_concepts, unmatched_keywords


def create_deferred_mapping_gpt_request(
    deferred_at: datetime,
    etld1: str,
    llm_mapping_request_id: str,
    known_concepts: set[Concept],  # DO NOT MUTATE
    unmatched_keywords_w_evidence: dict[str, str],
    mapping_prompt: Prompt,
    eager: bool,
    gpt_model: LLM_Model,
    model_params: GPTModelParams,
) -> GPTBatchRequest:
    logger.info(
        f"create_deferred_mapping_gpt_request: Generating GPTBatchRequest for {llm_mapping_request_id}"
    )
    context = json.dumps(
        {
            "candidates": unmatched_keywords_w_evidence,
            "knowns": list(known_concepts),
        },
        cls=ConceptJSONEncoder,
    )
    gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        etld1=etld1,
        custom_id=llm_mapping_request_id,
        context=context,
        prompt=mapping_prompt,
        gpt_model=gpt_model,
        model_params=model_params,
        batch_id="Eager" if eager else None,
    )
    return gpt_batch_request


def get_mapped_known_concepts_and_unmapped_keywords(
    mfg_etld1: str,
    concept_type: ConceptTypeEnum,
    known_concepts: set[Concept],
    unmatched_keywords_w_evidence: dict[str, str],
    raw_gpt_mapping: LLMMappingResult,
) -> KnownToUnknownMap:
    keywords_to_map = set(unmatched_keywords_w_evidence.keys())
    match_label_map: dict[str, Concept] = {
        label: k for k in known_concepts for label in k.matchLabels
    }
    known_to_unknowns: dict[Concept, set[str]] = {}

    # mapped_unknown "biotech" -> ["pharmaceutical"] mapped_knowns
    # mu ~ mapped_unknown, mk ~ mapped_knowns
    for mu, mk_labels in raw_gpt_mapping.items():
        if mu not in keywords_to_map:
            # mapped_unknown was hallucinated, in which case we will skip this map
            # and raise a warning
            logger.warning(
                f"WARNING: {mfg_etld1}:{concept_type.name} mapped_unknown:{mu} was not in the original unknowns list"
            )
        else:
            if mk_labels:  # mk must not be null/None
                for mk_label in mk_labels:
                    matched_known_concept = match_label_map.get(mk_label)
                    if not matched_known_concept:
                        logger.debug(
                            f"WARNING: {mfg_etld1}:{concept_type.name} mapped_known:{mk_label} was not in the original knowns list"
                        )
                    else:
                        known_to_unknowns.setdefault(matched_known_concept, set()).add(
                            mu
                        )

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
    final_bundle: KnownToUnknownMap = {
        "known_to_unknowns": known_to_unknowns,  # actively used
        "unmapped_unknowns": unmapped_unknowns,
    }

    return final_bundle
