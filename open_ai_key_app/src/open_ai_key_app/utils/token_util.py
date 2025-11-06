import tiktoken
from open_ai_key_app.models.gpt_model import GPTModel, GPT_4o_mini


# --- Token Estimation ---
def num_tokens_from_string(string: str, gpt_model: GPTModel = GPT_4o_mini) -> int:
    encoding = tiktoken.encoding_for_model(gpt_model.model_name)
    return len(encoding.encode(string))


if __name__ == "__main__":
    test_strings = ["\n", " ", "  ", "\t"]
    for test_string in test_strings:
        print(
            f"Number of tokens in '{test_string}': {num_tokens_from_string(test_string)}"
        )
