#!/usr/bin/env python3
"""
Script to update custom_id fields in batch output JSONL files.
Replaces 'materials' -> 'material_caps' and 'processes' -> 'process_caps'
within the custom_id field of each JSON object.
"""

import argparse
import json
from pathlib import Path


def update_custom_id(custom_id: str) -> str:
    """
    Replace 'materials' with 'material_caps' and 'processes' with 'process_caps'
    in the custom_id string.

    Args:
        custom_id: The original custom_id string

    Returns:
        Updated custom_id string with replacements applied
    """
    updated = custom_id.replace("materials", "material_caps")
    updated = updated.replace("processes", "process_caps")
    return updated


def process_jsonl_file(file_path: Path) -> tuple[int, int]:
    """
    Process a single JSONL file, updating custom_id fields in place.

    Args:
        file_path: Path to the JSONL file to process

    Returns:
        Tuple of (total_lines, modified_lines)
    """
    lines = []
    total_lines = 0
    modified_lines = 0

    # Read all lines and process them
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            total_lines += 1
            line = line.strip()
            if not line:
                lines.append(line)
                continue

            try:
                data = json.loads(line)

                # Check if custom_id exists and needs updating
                if "custom_id" in data:
                    original_custom_id = data["custom_id"]
                    updated_custom_id = update_custom_id(original_custom_id)

                    if original_custom_id != updated_custom_id:
                        data["custom_id"] = updated_custom_id
                        modified_lines += 1

                # Convert back to JSON and store
                lines.append(json.dumps(data, ensure_ascii=False))

            except json.JSONDecodeError as e:
                print(
                    f"Warning: Failed to parse JSON in {file_path.name}, line {total_lines}: {e}"
                )
                lines.append(line)

    # Write back to file if any modifications were made
    if modified_lines > 0:
        with open(file_path, "w", encoding="utf-8") as f:
            for line in lines:
                f.write(line + "\n")

    return total_lines, modified_lines


def main():
    """Main function to process all JSONL files in the batch_results_output directory."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Update custom_id fields in batch output JSONL files"
    )
    parser.add_argument(
        "-f",
        "--file",
        type=str,
        help="Specific filename to process (e.g., batch_68ef571855ec8190842f9755f5b3d896_output.jsonl)",
    )
    parser.add_argument(
        "-d",
        "--directory",
        type=str,
        help="Custom directory path containing JSONL files (default: ./batch_results_output)",
    )

    args = parser.parse_args()

    # Get the directory containing batch result files
    script_dir = Path(__file__).parent

    if args.directory:
        batch_dir = Path(args.directory)
    else:
        batch_dir = script_dir / "batch_results_output"

    if not batch_dir.exists():
        print(f"Error: Directory not found: {batch_dir}")
        return

    # Determine which files to process
    if args.file:
        # Process specific file
        file_path = batch_dir / args.file
        if not file_path.exists():
            print(f"Error: File not found: {file_path}")
            return
        if not file_path.suffix == ".jsonl":
            print(f"Error: File must have .jsonl extension: {file_path}")
            return
        jsonl_files = [file_path]
        print(f"Processing single file: {args.file}")
    else:
        # Find all JSONL files
        jsonl_files = list(batch_dir.glob("*.jsonl"))
        if not jsonl_files:
            print(f"No .jsonl files found in {batch_dir}")
            return
        print(f"Found {len(jsonl_files)} JSONL files to process")

    print("-" * 80)

    total_files_modified = 0
    total_lines_processed = 0
    total_lines_modified = 0

    # Process each file
    for file_path in sorted(jsonl_files):
        lines, modified = process_jsonl_file(file_path)
        total_lines_processed += lines
        total_lines_modified += modified

        if modified > 0:
            total_files_modified += 1
            print(f"✓ {file_path.name}: {modified}/{lines} lines modified")
        else:
            print(f"  {file_path.name}: No changes needed")

    print("-" * 80)
    print(f"Summary:")
    print(f"  Total files processed: {len(jsonl_files)}")
    print(f"  Files modified: {total_files_modified}")
    print(f"  Total lines processed: {total_lines_processed}")
    print(f"  Total lines modified: {total_lines_modified}")


if __name__ == "__main__":
    main()
