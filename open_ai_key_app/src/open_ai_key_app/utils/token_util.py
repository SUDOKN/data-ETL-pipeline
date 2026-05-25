import tiktoken
from open_ai_key_app.models.gpt_model import GPT_5_2, GPT_4o_mini, MODEL_ENCODING_MAP
from open_ai_key_app.models.llm_model import LLM_Model


# --- Token Estimation ---
def num_tokens_from_string(string: str, gpt_model: LLM_Model) -> int:
    encoding_name = MODEL_ENCODING_MAP.get(gpt_model.model_name, "o200k_base")
    # print(f"using encoding_name: {encoding_name} for model: {gpt_model.model_name}")
    encoding = tiktoken.get_encoding(encoding_name)
    # print(f"found encoding: {encoding}")
    return len(encoding.encode(string))


if __name__ == "__main__":
    test_strings = ["\n", " ", "  ", "\t"]
    for test_string in test_strings:
        print(
            f"Number of tokens in '{test_string}': {num_tokens_from_string(test_string, GPT_5_2)}"
        )
