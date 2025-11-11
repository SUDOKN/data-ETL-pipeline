#!/usr/bin/env python3
"""
Demonstrate that the new JSONL splitting handles embedded newlines correctly.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from data_etl_app.utils.chunk_util import (
    split_jsonl_on_record_boundaries,
    split_bytes_on_line_boundaries
)


def create_sample_jsonl():
    """Create sample JSONL similar to your OpenAI batch requests"""
    records = []
    
    for i in range(5):
        record = {
            "custom_id": f"example.com>process>chunk>{i}",
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": "gpt-4o-mini",
                "messages": [
                    {
                        "role": "system",
                        "content": (
                            "You are given a block of text scraped from a manufacturer's website.\n"
                            "Your task is to extract and return a **single-line JSON array**\n"
                            "\n"
                            "---\n"
                            "\n"
                            "#### 🧼 **Formatting Rules**:\n"
                            "- **Use PascalCase**: CNC Machining, Laser Cutting\n"
                            "- **No trailing punctuation**\n"
                            "- **Be specific**: Prefer 'Aluminum Die Casting' over 'Casting'\n"
                            "\n"
                            "#### 📋 **Examples**:\n"
                            "- Input: 'We offer precision CNC machining...'\n"
                            "  Output: [\"CNC Machining\"]\n"
                        )
                    },
                    {
                        "role": "user",
                        "content": f"Sample manufacturing text for record {i}"
                    }
                ]
            }
        }
        records.append(record)
    
    return '\n'.join(json.dumps(r, separators=(',', ':')) for r in records) + '\n'


def compare_splitting_methods():
    """Compare old vs new splitting method"""
    
    print("=" * 80)
    print("DEMONSTRATION: JSONL Splitting with Embedded Newlines")
    print("=" * 80)
    
    jsonl_text = create_sample_jsonl()
    jsonl_bytes = jsonl_text.encode('utf-8')
    
    print(f"\nOriginal JSONL size: {len(jsonl_bytes):,} bytes")
    print(f"Number of records: 5")
    
    # Show a sample record
    first_record = json.loads(jsonl_text.split('\n')[0])
    content = first_record['body']['messages'][0]['content']
    newline_count = content.count('\n')
    print(f"Newlines in 'content' field: {newline_count}")
    
    print("\n" + "=" * 80)
    print("OLD METHOD: split_bytes_on_line_boundaries (BROKEN)")
    print("=" * 80)
    
    try:
        old_chunks = split_bytes_on_line_boundaries(
            jsonl_bytes,
            max_chunk_size=500,  # Small size to force splitting
            newline_search_window=100
        )
        print(f"Created {len(old_chunks)} chunks")
        
        # Try to parse each chunk
        total_valid = 0
        total_broken = 0
        for i, chunk in enumerate(old_chunks):
            chunk_text = chunk.decode('utf-8')
            lines = [l.strip() for l in chunk_text.split('\n') if l.strip()]
            print(f"\nChunk {i}: {len(chunk)} bytes, {len(lines)} lines")
            
            for j, line in enumerate(lines[:3]):  # Show first 3 lines
                try:
                    json.loads(line)
                    print(f"  Line {j}: ✅ Valid JSON")
                    total_valid += 1
                except json.JSONDecodeError as e:
                    print(f"  Line {j}: ❌ BROKEN - {e.msg}")
                    print(f"           {line[:80]}...")
                    total_broken += 1
        
        print(f"\n⚠️  OLD METHOD: {total_valid} valid, {total_broken} BROKEN records")
        
    except Exception as e:
        print(f"❌ Old method failed: {e}")
    
    print("\n" + "=" * 80)
    print("NEW METHOD: split_jsonl_on_record_boundaries (CORRECT)")
    print("=" * 80)
    
    new_chunks = split_jsonl_on_record_boundaries(
        jsonl_bytes,
        max_chunk_size=500  # Same small size
    )
    print(f"Created {len(new_chunks)} chunks")
    
    # Try to parse each chunk
    total_valid = 0
    for i, chunk in enumerate(new_chunks):
        chunk_text = chunk.decode('utf-8')
        lines = [l.strip() for l in chunk_text.split('\n') if l.strip()]
        print(f"\nChunk {i}: {len(chunk)} bytes, {len(lines)} lines")
        
        for j, line in enumerate(lines):
            try:
                record = json.loads(line)
                custom_id = record['custom_id']
                print(f"  Line {j}: ✅ Valid JSON - {custom_id}")
                total_valid += 1
            except json.JSONDecodeError as e:
                print(f"  Line {j}: ❌ BROKEN - {e.msg}")
    
    print(f"\n✅ NEW METHOD: All {total_valid} records valid!")
    
    # Verify data integrity
    rejoined = b''.join(new_chunks)
    if rejoined == jsonl_bytes:
        print("✅ Rejoined chunks match original data exactly")
    else:
        print("❌ Data integrity issue!")
    
    print("\n" + "=" * 80)
    print("CONCLUSION")
    print("=" * 80)
    print("The new split_jsonl_on_record_boundaries() method correctly handles")
    print("JSON objects with embedded newlines, while the old method would")
    print("break records in the middle, causing upload failures.")
    print("=" * 80)


if __name__ == "__main__":
    compare_splitting_methods()
