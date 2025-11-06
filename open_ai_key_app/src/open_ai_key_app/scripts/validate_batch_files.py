"""
Validate batch JSONL files to ensure they meet OpenAI Batch API requirements.

This script checks:
1. Each line is valid JSON
2. Required fields are present (custom_id, method, url, body)
3. File format is correct
"""

import json
import sys
from pathlib import Path


def validate_jsonl_file(file_path: Path) -> tuple[bool, list[str]]:
    """
    Validate a JSONL file for OpenAI Batch API.

    Args:
        file_path: Path to the JSONL file

    Returns:
        Tuple of (is_valid, list of errors)
    """
    errors = []

    if not file_path.exists():
        return False, [f"File does not exist: {file_path}"]

    if not file_path.suffix == ".jsonl":
        errors.append(f"File extension is '{file_path.suffix}', must be '.jsonl'")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        if not lines:
            errors.append("File is empty")
            return False, errors

        for line_num, line in enumerate(lines, 1):
            line = line.strip()
            if not line:
                errors.append(f"Line {line_num}: Empty line (should be removed)")
                continue

            # Try to parse JSON
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                errors.append(f"Line {line_num}: Invalid JSON - {e}")
                continue

            # Check required fields for Batch API
            required_fields = ["custom_id", "method", "url", "body"]
            missing_fields = [field for field in required_fields if field not in obj]

            if missing_fields:
                errors.append(
                    f"Line {line_num}: Missing required fields: {', '.join(missing_fields)}"
                )

            # Validate field values
            if "method" in obj and obj["method"] != "POST":
                errors.append(
                    f"Line {line_num}: method must be 'POST', got '{obj['method']}'"
                )

            if "url" in obj and not obj["url"].startswith("/v1/"):
                errors.append(
                    f"Line {line_num}: url must start with '/v1/', got '{obj['url']}'"
                )

            # Check if body is present and is a dict
            if "body" in obj and not isinstance(obj["body"], dict):
                errors.append(f"Line {line_num}: 'body' must be an object/dict")

        # Check for duplicate custom_ids
        custom_ids = []
        for line in lines:
            if line.strip():
                try:
                    obj = json.loads(line.strip())
                    if "custom_id" in obj:
                        custom_ids.append(obj["custom_id"])
                except:
                    pass

        duplicates = [cid for cid in set(custom_ids) if custom_ids.count(cid) > 1]
        if duplicates:
            errors.append(
                f"Duplicate custom_ids found: {', '.join(duplicates[:5])}"
                + (f" and {len(duplicates) - 5} more" if len(duplicates) > 5 else "")
            )

    except Exception as e:
        errors.append(f"Error reading file: {e}")
        return False, errors

    return len(errors) == 0, errors


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python validate_batch_files.py <file_or_directory>")
        print()
        print("Examples:")
        print("  python validate_batch_files.py batch_file.jsonl")
        print("  python validate_batch_files.py ./batch_requests_output")
        sys.exit(1)

    path = Path(sys.argv[1])

    if not path.exists():
        print(f"Error: Path does not exist: {path}")
        sys.exit(1)

    # Collect files to validate
    files_to_validate = []
    if path.is_file():
        files_to_validate.append(path)
    elif path.is_dir():
        # Find all .jsonl files recursively
        files_to_validate = list(path.rglob("*.jsonl"))
        if not files_to_validate:
            print(f"No .jsonl files found in {path}")
            sys.exit(1)

    print("=" * 70)
    print(f"VALIDATING {len(files_to_validate)} FILE(S)")
    print("=" * 70)
    print()

    all_valid = True

    for file_path in sorted(files_to_validate):
        print(f"📄 {file_path.name}")
        print(f"   Path: {file_path}")

        is_valid, errors = validate_jsonl_file(file_path)

        if is_valid:
            # Count lines
            with open(file_path, "r", encoding="utf-8") as f:
                num_lines = sum(1 for line in f if line.strip())
            print(f"   ✅ VALID ({num_lines} requests)")
        else:
            all_valid = False
            print(f"   ❌ INVALID ({len(errors)} error(s))")
            for error in errors[:10]:  # Show first 10 errors
                print(f"      - {error}")
            if len(errors) > 10:
                print(f"      ... and {len(errors) - 10} more errors")

        print()

    print("=" * 70)
    if all_valid:
        print("✅ ALL FILES VALID")
        sys.exit(0)
    else:
        print("❌ SOME FILES HAVE ERRORS")
        sys.exit(1)


if __name__ == "__main__":
    main()
