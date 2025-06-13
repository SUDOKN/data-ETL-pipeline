import json
from typing_extensions import TypedDict

from data_etl_app.models.skos_concept import Concept, ConceptJSONEncoder
from open_ai_key_app.utils.ask_gpt import (
    ask_gpt_async,
    num_tokens_from_string,
)
from open_ai_key_app.models.gpt_model import (
    GPT_4o_mini,
    DefaultModelParameters,
)


class MapKnownToUnknownResult(TypedDict):
    # raw_mapping: dict[str, list[str]]
    known_to_unknowns: dict[Concept, list[str]]
    unmapped_knowns: set[Concept]
    unmapped_unknowns: set[str]


async def mapKnownToUnknown(
    concept_type: str,
    manufacturer_url: str,
    known_concepts: list[Concept],
    unknowns: list[str],
    prompt: str,
    # gpt_model: GPTModel,
    # model_params: ModelParameters,
    debug: bool = False,
) -> MapKnownToUnknownResult:

    context = json.dumps(
        {"unknowns": unknowns, "knowns": known_concepts}, cls=ConceptJSONEncoder
    )
    if debug:
        print(f"\nmapping unknown_to_known")
        print(f"context {num_tokens_from_string(context)}:{context}")

    gpt_response = await ask_gpt_async(
        context, prompt, GPT_4o_mini, DefaultModelParameters
    )
    # gpt_response = await ask_gpt_async(context, prompt, pool, gpt_model, model_params)

    if debug:
        print(f"gptresponse:{gpt_response}")

    if not gpt_response:
        print(f"gptresponse:{gpt_response}")
        raise ValueError(
            f"{manufacturer_url}:{concept_type} unknown_to_known: Empty response from GPT"
        )

    gpt_response = gpt_response.replace("```", "").replace("json", "")

    # NOTE: mapping can be {1 unknowns:M knowns} or {M unknowns:1 knowns}
    mapping: dict[str, str] = json.loads(gpt_response)  # from unknown --> known
    if debug:
        print(f"mapping:{json.dumps(mapping, indent=2)}")

    known_concept_labels = [label for k in known_concepts for label in k.matchLabels]
    unmapped_knowns = set(known_concepts)  # starts as the full set of passed knowns
    unmapped_unknowns = set(unknowns)
    known_to_unknowns: dict[Concept, list[str]] = {k: [] for k in known_concepts}

    # mapped_unknown "biotech" -> ["pharmaceutical"] mapped_knowns
    for mu, mk in mapping.items():
        if mu not in unmapped_unknowns:
            # case 2: mapped_unknown was either hallucinated, in which case we will still check if mapped_knowns are valid, so just raise a warning
            print(
                f"WARNING: {manufacturer_url}:{concept_type} mapped_unknown:{mu} was not in the original unknowns list"
            )
        else:
            if mk:  # mk must not be null/None
                if mk not in known_concept_labels:
                    print(
                        f"WARNING: {manufacturer_url}:{concept_type} mapped_known:{mk} was not in the original knowns list"
                    )
                else:
                    known_concept = next(
                        (k for k in known_concepts if mk in k.matchLabels), None
                    )
                    if not known_concept:
                        raise ValueError(
                            f"{manufacturer_url}:{concept_type} mapped_known:{mk} was not found in known_concepts"
                        )
                    known_to_unknowns[known_concept].append(mu)

    # Derive unmapped_knowns and unmapped_unknowns
    for kc, unknowns in known_to_unknowns.items():
        if (
            unknowns
        ):  # meaning there was at least one mapping from unknown to known by llm
            unmapped_knowns.remove(kc)
            if debug:
                print(f"removing mapped_knowns:{kc}")
                print(f"removing mapped_unknowns:{unknowns}")
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
