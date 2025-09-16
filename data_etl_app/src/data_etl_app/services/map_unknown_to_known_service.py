import json
import logging
from typing_extensions import TypedDict

from data_etl_app.models.skos_concept import Concept, ConceptJSONEncoder
from open_ai_key_app.utils.ask_gpt_util import (
    ask_gpt_async,
    num_tokens_from_string,
)
from open_ai_key_app.models.gpt_model import (
    GPT_4o_mini,
    DefaultModelParameters,
)

logger = logging.getLogger(__name__)


class MapKnownToUnknownResult(TypedDict):
    # raw_mapping: dict[str, list[str]]
    known_to_unknowns: dict[Concept, list[str]]
    unmapped_knowns: set[Concept]
    unmapped_unknowns: set[str]


async def mapKnownToUnknown(
    concept_type: str,
    mfg_etld1: str,
    known_concepts: list[Concept],  # DO NOT MUTATE
    unmapped_unknowns: set[str],
    prompt: str,
    # gpt_model: GPTModel,
    # model_params: ModelParameters,
) -> MapKnownToUnknownResult:

    context = json.dumps(
        {"unknowns": list(unmapped_unknowns), "knowns": known_concepts},
        cls=ConceptJSONEncoder,
    )

    logger.debug(f"\nmapping unknown_to_known")
    logger.debug(f"context {num_tokens_from_string(context)}:{context}")

    gpt_response = await ask_gpt_async(
        context, prompt, GPT_4o_mini, DefaultModelParameters
    )
    # gpt_response = await ask_gpt_async(context, prompt, pool, gpt_model, model_params)

    logger.debug(f"gptresponse:{gpt_response}")

    if not gpt_response:
        logger.error(f"gptresponse:{gpt_response}")
        raise ValueError(
            f"{mfg_etld1}:{concept_type} unknown_to_known: Empty response from GPT"
        )

    gpt_response = gpt_response.replace("```", "").replace("json", "")

    # NOTE: mapping can be {1 unknowns:M knowns} or {M unknowns:1 knowns}
    mapping: dict[str, str] = json.loads(gpt_response)  # from unknown --> known
    logger.debug(f"mapping:{json.dumps(mapping, indent=2)}")

    known_concept_labels = [label for k in known_concepts for label in k.matchLabels]
    unmapped_knowns = set(known_concepts)  # starts as the full set of passed knowns
    known_to_unknowns: dict[Concept, list[str]] = {k: [] for k in known_concepts}

    # mapped_unknown "biotech" -> ["pharmaceutical"] mapped_knowns
    # mu: mapped_unknowns, mk: mapped_knowns
    for mu, mk in mapping.items():
        if mu not in unmapped_unknowns:
            # case 2: mapped_unknown was either hallucinated, in which case we will still check if mapped_knowns are valid, so just raise a warning
            logger.warning(
                f"WARNING: {mfg_etld1}:{concept_type} mapped_unknown:{mu} was not in the original unknowns list"
            )
        else:
            if mk:  # mk must not be null/None
                if mk not in known_concept_labels:
                    logger.warning(
                        f"WARNING: {mfg_etld1}:{concept_type} mapped_known:{mk} was not in the original knowns list"
                    )
                else:
                    known_concept = next(  # comparing with k.matchLabels instead of just k.label in case llm didn't provide the primary label
                        (k for k in known_concepts if mk in k.matchLabels), None
                    )
                    if not known_concept:
                        raise ValueError(
                            f"{mfg_etld1}:{concept_type} mapped_known:{mk} was not found in known_concepts"
                        )
                    known_to_unknowns[known_concept].append(mu)

    # Derive unmapped_knowns and unmapped_unknowns
    for kc, unknowns in known_to_unknowns.items():
        if (
            unknowns
        ):  # meaning there was at least one mapping from unknown to known by llm
            unmapped_knowns.remove(kc)
            logger.debug(f"removing mapped_knowns:{kc}")
            logger.debug(f"removing mapped_unknowns:{unknowns}")
            unmapped_unknowns -= set(
                unknowns
            )  # NOTE: unknowns may contain hallucinations, but subtraction is safe
        # else: known was not mapped and stays in both known_to_unknowns and unmapped_knowns

    # pack everything and send back
    final_bundle: MapKnownToUnknownResult = {
        # "raw_mapping": mapping,
        "known_to_unknowns": {
            k: v for k, v in known_to_unknowns.items() if v
        },  # actively used
        "unmapped_knowns": unmapped_knowns,
        "unmapped_unknowns": unmapped_unknowns,
    }

    return final_bundle
