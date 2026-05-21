import asyncio
import json
import logging
from datetime import datetime
from typing import Optional, TypedDict

from core.models.prompt import Prompt
from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.deferred_concept_extraction import (
    DeferredConceptExtractionRequests,
)
from core.models.gpt_batch_request_blob import (
    GPTBatchRequestBlob,
    GPTBatchRequestBlobBody,
)
from core.models.gpt_batch_response_blob import (
    GPTBatchResponseBlob,
    GPTBatchResponseBody,
    GPTResponseBlobBody,
    GPTBatchResponseBlobUsage,
    GPTBatchResponseBlobChoice,
    GPTBatchResponseBlobChoiceMessage,
)
from data_etl_app.models.pipeline_nodes import ConceptEvidenceNode
from data_etl_app.models.types_and_enums import ConceptTypeEnum
from data_etl_app.models.skos_concept import Concept, ConceptJSONEncoder
from open_ai_key_app.models.gpt_model import (
    LLM_Model,
    GPT_4o_mini,
    ModelParameters,
    DefaultModelParameters,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

from core.services.gpt_batch_request_service import create_base_gpt_batch_request
from data_etl_app.services.extraction.deferred_concept_mapping_service import (
    create_deferred_mapping_gpt_request,
)

logger = logging.getLogger(__name__)


class KnownToUnknownMap(TypedDict):
    known_to_unknowns: dict[Concept, set[str]]
    unmapped_unknowns: set[str]


def parse_llm_mapping_result(gpt_response: Optional[str]) -> dict[str, str]:
    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError("parse_llm_mapping_result: Empty or invalid response from GPT")

    try:
        gpt_response = gpt_response.replace("```", "").replace("json", "")
        raw_gpt_mapping: dict[str, str] = json.loads(
            gpt_response
        )  # from unknown --> known
        logger.debug(f"raw_gpt_mapping:{json.dumps(raw_gpt_mapping, indent=2)}")
    except:
        raise ValueError(
            f"parse_llm_mapping_result: Invalid response from GPT:{gpt_response}"
        )

    if not isinstance(raw_gpt_mapping, dict):
        raise ValueError(
            "parse_llm_mapping_result: Expected raw_gpt_mapping to be a dictionary"
        )

    logger.debug(f"raw_gpt_mapping:{raw_gpt_mapping}")

    return raw_gpt_mapping


async def create_missing_mapping_requests(
    deferred_at: datetime,
    mfg_etld1: str,
    concept_type: ConceptTypeEnum,  # used for logging and debugging
    extraction_requests: DeferredConceptExtractionRequests,
    missing_mapping_req_ids: set[GPTBatchRequestCustomID],
    known_concepts: set[Concept],  # DO NOT MUTATE
    mapping_prompt: Prompt,
    upstream_completed_batch_req_map: dict[GPTBatchRequestCustomID, GPTBatchRequest],
    llm_model: LLM_Model,
    model_params: ModelParameters = DefaultModelParameters,
    BATCH_SIZE=100,
) -> list[GPTBatchRequest]:
    logger.info(
        f"create_missing_concept_mapping_requests: Generating GPTBatchRequests for {mfg_etld1}:{concept_type}"
    )

    # create chunk_items only for missing batch requests
    batch_requests: list[GPTBatchRequest] = []
    chunk_items = list(
        {
            chunk_bounds: bundle
            for chunk_bounds, bundle in extraction_requests.request_map.items()
            if bundle.llm_mapping_request_id in missing_mapping_req_ids
        }.items()
    )
    llm_evidence_gpt_request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest] = (
        upstream_completed_batch_req_map
    )

    # Process chunks in batches to yield control periodically
    for i in range(0, len(chunk_items), BATCH_SIZE):
        batch = chunk_items[i : i + BATCH_SIZE]

        # Process current batch
        for chunk_bounds, extraction_bundle in batch:
            llm_evidence_results = await ConceptEvidenceNode.parse_batch_request_result(
                mfg_etld1=mfg_etld1,
                field_type=concept_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=extraction_bundle,
                completed_request_map=llm_evidence_gpt_request_map,
                deferred_at=deferred_at,
            )
            confirmed_keywords_w_evidence = {
                kw: evidence
                for kw, evidence in llm_evidence_results.items()
                if evidence
            }

            (
                _matched_concepts,  # _matched_concepts would be added later by reconcile node
                unmatched_keywords,
            ) = get_matched_concepts_and_unmatched_keywords(
                known_concepts, confirmed_keywords_w_evidence
            )

            llm_mapping_request_id = extraction_bundle.llm_mapping_request_id
            if not llm_mapping_request_id:
                raise ValueError(
                    f"concept_evidence_node.get_batch_request_result: llm_mapping_request_id is None for chunk bounds {chunk_bounds} in {mfg_etld1}:{concept_type}"
                )

            if not unmatched_keywords:
                # add a dummy response blob with empty dict
                logger.info(
                    f"All concepts matched via brute search for {mfg_etld1}:{concept_type} or none were found in the first place, creating dummy mapping request"
                )
                dummy_batch_request = _create_dummy_completed_mapping_batch_request(
                    deferred_at=deferred_at,
                    llm_mapping_request_id=llm_mapping_request_id,
                )
                new_batch_request = dummy_batch_request
            else:
                mapping_batch_request = create_deferred_mapping_gpt_request(
                    deferred_at=deferred_at,
                    llm_mapping_request_id=llm_mapping_request_id,
                    known_concepts=known_concepts,  # TODO: RAG known concepts instead of passing full set
                    unmatched_keywords=unmatched_keywords,
                    mapping_prompt=mapping_prompt,
                    gpt_model=llm_model,
                    model_params=model_params,
                )
                new_batch_request = mapping_batch_request

            batch_requests.append(new_batch_request)

        # Yield control to event loop after each batch
        await asyncio.sleep(0)

        if (i + BATCH_SIZE) % 500 == 0:
            logger.info(
                f"Created {min(i + BATCH_SIZE, len(chunk_items))}/{len(chunk_items)} "
                f"gpt request for {mfg_etld1}:{concept_type}"
            )

    return batch_requests


def _create_dummy_completed_mapping_batch_request(
    deferred_at: datetime, llm_mapping_request_id: GPTBatchRequestCustomID
) -> GPTBatchRequest:
    if llm_mapping_request_id is None:
        raise ValueError(
            "_create_dummy_completed_mapping_batch_request: llm_mapping_request_id is None"
        )

    return GPTBatchRequest(
        created_at=deferred_at,
        updated_at=deferred_at,
        num_batches_paired_with=0,
        request=GPTBatchRequestBlob(
            custom_id=llm_mapping_request_id,
            body=GPTBatchRequestBlobBody(
                model="basic_logic",
                messages=[
                    {
                        "role": "system",
                        "content": "No mapping needed - all concepts matched via brute search or none were found in the first place.",
                    }
                ],
                input_tokens=1,
                max_tokens=1,
            ),
        ),
        batch_id="empty_unmapped_unknowns",
        response_blob=GPTBatchResponseBlob(
            batch_id="empty_unmapped_unknowns",
            request_custom_id="no-request",
            response=GPTBatchResponseBody(
                status_code=200,
                body=GPTResponseBlobBody(
                    created=deferred_at,
                    choices=[
                        GPTBatchResponseBlobChoice(
                            index=0,
                            message=GPTBatchResponseBlobChoiceMessage(
                                role="assistant", content="```json\n{}\n```"
                            ),
                        )
                    ],
                    usage=GPTBatchResponseBlobUsage(
                        prompt_tokens=1,
                        completion_tokens=1,
                        total_tokens=2,
                    ),
                ),
            ),
        ),
    )


def create_deferred_mapping_gpt_request(
    deferred_at: datetime,
    llm_mapping_request_id: str,
    known_concepts: set[Concept],  # DO NOT MUTATE
    unmatched_keywords: set[str],
    mapping_prompt: Prompt,
    gpt_model: LLM_Model = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> GPTBatchRequest:
    logger.info(
        f"create_deferred_mapping_gpt_request: Generating GPTBatchRequest for {llm_mapping_request_id}"
    )
    context = json.dumps(
        {"unknowns": list(unmatched_keywords), "knowns": list(known_concepts)},
        cls=ConceptJSONEncoder,
    )
    gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        custom_id=llm_mapping_request_id,
        context=context,
        prompt=mapping_prompt,
        gpt_model=gpt_model,
        model_params=model_params,
    )
    return gpt_batch_request


def get_matched_concepts_and_unmatched_keywords(
    known_concepts: set[Concept], confirmed_keywords_w_evidence: dict[str, str]
) -> tuple[set[Concept], set[str]]:
    confirmed_keywords: set[str] = set(confirmed_keywords_w_evidence.keys())
    unmatched_keywords: set[str] = confirmed_keywords.copy()
    matched_concepts: set[Concept] = set()

    for kc in known_concepts:
        common = kc.matchLabels & confirmed_keywords
        if common:
            matched_concepts.add(kc)
            unmatched_keywords -= common
    return matched_concepts, unmatched_keywords


def get_mapped_known_concepts_and_unmapped_keywords(
    mfg_etld1: str,
    concept_type: ConceptTypeEnum,
    known_concepts: set[Concept],
    keywords_to_map: set[str],
    raw_gpt_mapping: dict[str, str],
) -> KnownToUnknownMap:
    match_label_map: dict[str, Concept] = {
        label: k for k in known_concepts for label in k.matchLabels
    }
    known_to_unknowns: dict[Concept, set[str]] = {}

    # mapped_unknown "biotech" -> ["pharmaceutical"] mapped_knowns
    # mu ~ mapped_unknown, mk ~ mapped_knowns
    for mu, mk_label in raw_gpt_mapping.items():
        if mu not in keywords_to_map:
            # mapped_unknown was hallucinated, in which case we will skip this map
            # and raise a warning
            logger.warning(
                f"WARNING: {mfg_etld1}:{concept_type.name} mapped_unknown:{mu} was not in the original unknowns list"
            )
        else:
            if mk_label:  # mk must not be null/None
                matched_known_concept = match_label_map.get(mk_label)
                if not matched_known_concept:
                    logger.debug(
                        f"WARNING: {mfg_etld1}:{concept_type.name} mapped_known:{mk_label} was not in the original knowns list"
                    )
                else:
                    known_to_unknowns.setdefault(matched_known_concept, set()).add(mu)

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
