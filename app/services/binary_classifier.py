import asyncio
import json
from typing import TypedDict

from utils.chunk_util import ChunkingStrat, get_chunks, get_chunks_with_overlap
from utils.multi_key_gpt import (
    num_tokens_from_string,
    ask_gpt_async,
    GPTModel,
    GPT_4o_mini,
    ModelParameters,
    DefaultModelParameters,
)
from utils.key_util import pool


class BinaryClassifierResult(TypedDict):
    name: str
    answer: bool
    explanation: str


async def binary_classifier(
    keyword_label: str,
    manufacturer_url: str,
    text: str,
    binary_prompt: str,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
    debug: bool = False,
) -> BinaryClassifierResult:
    text_tokens = num_tokens_from_string(text)
    if debug:
        print(f"text_tokens: {text_tokens}")

    gpt_response = await ask_gpt_async(
        text, binary_prompt, pool, gpt_model, model_params
    )

    if not gpt_response:
        print(f"Invalid gpt_response:{gpt_response}")
        raise ValueError(
            f"{manufacturer_url}:{keyword_label} llm_results: Empty or invalid response from GPT"
        )

    if debug:
        print(f"classification gpt response:\n{gpt_response}")

    try:
        gpt_response = gpt_response.replace("```", "").replace("json", "")
        classification_result: BinaryClassifierResult = json.loads(gpt_response)
    except:
        raise ValueError(
            f"{manufacturer_url}:{keyword_label} binary_classifier: non-json result from GPT:{gpt_response}"
        )

    return classification_result


async def binary_classifier2(
    text: str,
    binary_prompt: str,
    summarization_prompt: str,
    chunk_strategy: ChunkingStrat,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
    max_text_token_limit: int = 100000,  # beyond this text would be chunked and summarized before final binary decision
    debug: bool = False,
) -> BinaryClassifierResult:

    text_tokens = num_tokens_from_string(text)
    if text_tokens > max_text_token_limit:
        print(
            f"text_tokens:{text_tokens} > max_text_token_limit:{max_text_token_limit} => chunk and summarize"
        )

        # find the number of chunks after which each chunk becomes less than max_text_token_limit
        # in most cases, 2 chunks would be enough.
        n = 2
        while (
            text_tokens // n + chunk_strategy.overlap * text_tokens
        ) > max_text_token_limit:
            n += 1

        max_tokens_per_chunk = int(
            text_tokens / n + chunk_strategy.overlap * text_tokens
        )

        if chunk_strategy.overlap == 0:
            chunks = get_chunks(text, max_tokens_per_chunk)
        else:
            chunks = get_chunks_with_overlap(
                text, max_tokens_per_chunk, chunk_strategy.overlap
            )

        # reassign text to a combined summary of its chunks (each summary will contain evidence and counter evidence)
        summaries = await asyncio.gather(
            *[
                asyncio.create_task(
                    summarize(
                        chunk, summarization_prompt, gpt_model, model_params, debug
                    )
                )
                for chunk in chunks
            ]
        )
        if debug:
            print(f"final summaries:\n{summaries}")
        text = "\n".join(summaries)

    gpt_response = await ask_gpt_async(
        text, binary_prompt, pool, gpt_model, model_params
    )

    if not gpt_response:
        print(f"Invalid gpt_response:{gpt_response}")
        raise ValueError("llm_results: Empty or invalid response from GPT")

    if debug:
        print(f"classification gpt response:\n{gpt_response}")

    try:
        classification_result: BinaryClassifierResult = json.loads(gpt_response)
    except:
        raise ValueError(f"binary_classifier: non-json result from GPT:{gpt_response}")

    return classification_result


async def summarize(
    text: str,
    summarization_prompt: str,
    gpt_model: GPTModel,
    model_params: ModelParameters,
    debug: bool = False,
) -> str:
    text_tokens = num_tokens_from_string(text)
    if debug:
        print(f"text_tokens summarize: {text_tokens}")

    # gpt_response = await ask_gpt_async(
    #     text, summarization_prompt, pool, safe_completion_tokens=int(0.3 * text_tokens)
    # )
    gpt_response = await ask_gpt_async(
        text, summarization_prompt, pool, gpt_model, model_params
    )

    if not gpt_response:
        print(f"Invalid gpt_response:{gpt_response}")
        raise ValueError("llm_results: Empty or invalid response from GPT")

    if debug:
        print(f"summary:\n{gpt_response}")

    return gpt_response
