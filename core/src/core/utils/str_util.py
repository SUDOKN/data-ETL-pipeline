import json
import logging
import re

logger = logging.getLogger(__name__)


def make_json_array_parse_safe(response: str) -> str:
    """
    Makes a string safe for JSON parsing by handling common formatting issues.

    Handles cases like:
    - Unescaped quotes inside array strings
    - Code block markers (```)
    - Extra whitespace

    Args:
        response: The raw response string to clean

    Returns:
        A JSON-parsable string

    Example:
        Input: '["Item with "quotes" inside"]'
        Output: '["Item with \\"quotes\\" inside"]'
    """
    if not response:
        return response

    # Remove code block markers and json labels
    cleaned = response.replace("```", "").replace("json", "").strip()

    # Try to parse as-is first
    try:
        json.loads(cleaned)
        return cleaned
    except json.JSONDecodeError:
        pass

    # Handle unescaped quotes inside JSON array strings
    try:
        if cleaned.strip().startswith("[") and cleaned.strip().endswith("]"):
            # Parse character by character, tracking state
            result = []
            i = 0

            while i < len(cleaned):
                char = cleaned[i]

                # Start of array or whitespace/comma - pass through
                if char in ["[", "]", ",", " ", "\n", "\t", "\r"]:
                    result.append(char)
                    i += 1
                    continue

                # Start of a string
                if char == '"':
                    # Find the end of this string, handling the unescaped quotes
                    string_start = i
                    i += 1
                    string_content = []

                    while i < len(cleaned):
                        ch = cleaned[i]

                        # Already escaped quote - preserve it
                        if (
                            ch == "\\"
                            and i + 1 < len(cleaned)
                            and cleaned[i + 1] == '"'
                        ):
                            string_content.append('\\"')
                            i += 2
                            continue

                        # Other escape sequences - preserve them
                        if ch == "\\" and i + 1 < len(cleaned):
                            string_content.append(ch)
                            string_content.append(cleaned[i + 1])
                            i += 2
                            continue

                        # Unescaped quote - check if it's the end of the string
                        if ch == '"':
                            # Look ahead to see if this is really the end
                            # The string ends if followed by: comma, ], or whitespace then comma/]
                            next_non_space = i + 1
                            while next_non_space < len(cleaned) and cleaned[
                                next_non_space
                            ] in [" ", "\n", "\t", "\r"]:
                                next_non_space += 1

                            if next_non_space >= len(cleaned) or cleaned[
                                next_non_space
                            ] in [",", "]"]:
                                # This is the end quote
                                result.append('"')
                                result.extend(string_content)
                                result.append('"')
                                i += 1
                                break
                            else:
                                # This is an internal quote - escape it
                                string_content.append('\\"')
                                i += 1
                                continue

                        # Regular character
                        string_content.append(ch)
                        i += 1
                    else:
                        # Reached end without closing quote - just append what we have
                        result.append('"')
                        result.extend(string_content)
                else:
                    # Non-string, non-structural character (shouldn't happen in clean JSON)
                    result.append(char)
                    i += 1

            fixed = "".join(result)

            # Validate it parses
            try:
                json.loads(fixed)
                return fixed
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to fix JSON with quote escaping approach: {e}")

    except Exception as e:
        logger.warning(f"Error while attempting to fix JSON quotes: {e}")

    # If all else fails, return the cleaned version
    return cleaned
