#!/usr/bin/env python3
"""
Verify that JSONL splitting correctly handles files with embedded newlines.
This script tests the actual batch file to ensure no records are broken.
"""
import json
import sys
from pathlib import Path

# Add project to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from data_etl_app.utils.chunk_util import split_jsonl_on_record_boundaries


def verify_jsonl_file(file_path: Path, max_chunk_size: int = 50 * 1024 * 1024):
    """Verify that a JSONL file can be split without breaking records"""
    
    print(f"Reading file: {file_path}")
    with open(file_path, "rb") as f:
        data = f.read()
    
    file_size = len(data)
    print(f"File size: {file_size:,} bytes ({file_size / (1024**2):.2f} MB)")
    
    # Count original records
    original_records = []
    for line in data.decode('utf-8').split('\n'):
        if line.strip():
            try:
                original_records.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"⚠️  Invalid JSON in original file: {e}")
                print(f"Line: {line[:100]}...")
                return False
    
    print(f"Original records: {len(original_records)}")
    
    # Split into chunks
    print(f"\nSplitting with max_chunk_size={max_chunk_size:,} bytes...")
    chunks = split_jsonl_on_record_boundaries(data, max_chunk_size)
    print(f"Created {len(chunks)} chunk(s)")
    
    # Verify each chunk
    total_records = 0
    for i, chunk in enumerate(chunks):
        chunk_size = len(chunk)
        print(f"\nChunk {i}: {chunk_size:,} bytes ({chunk_size / (1024**2):.2f} MB)")
        
        # Count records in this chunk
        chunk_records = 0
        for line in chunk.decode('utf-8').split('\n'):
            if line.strip():
                try:
                    json.loads(line)
                    chunk_records += 1
                except json.JSONDecodeError as e:
                    print(f"❌ Invalid JSON in chunk {i}: {e}")
                    print(f"Line: {line[:100]}...")
                    return False
        
        print(f"  Records: {chunk_records}")
        total_records += chunk_records
    
    # Verify totals match
    print(f"\n{'='*60}")
    print(f"Total records recovered: {total_records}")
    print(f"Original records: {len(original_records)}")
    
    if total_records == len(original_records):
        print("✅ SUCCESS! All records preserved correctly.")
        
        # Verify rejoined data matches original
        rejoined = b''.join(chunks)
        if rejoined == data:
            print("✅ Rejoined chunks match original data exactly.")
        else:
            print("⚠️  Warning: Rejoined data differs from original")
            print(f"   Original size: {len(data):,}")
            print(f"   Rejoined size: {len(rejoined):,}")
        
        return True
    else:
        print(f"❌ FAILED! Record count mismatch!")
        return False


if __name__ == "__main__":
    # Test with the actual batch file
    batch_file = Path(__file__).parent.parent.parent / "batch_data" / "20251108_184556" / "20251108_184556_batch_requests_0000.jsonl"
    
    if not batch_file.exists():
        print(f"File not found: {batch_file}")
        print("Please provide path to a JSONL batch file as argument")
        sys.exit(1)
    
    success = verify_jsonl_file(batch_file)
    sys.exit(0 if success else 1)
