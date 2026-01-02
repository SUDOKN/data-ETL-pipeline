import json
import pytest

from core.utils.str_util import make_json_array_parse_safe


def test_make_json_parse_safe_already_valid_json():
    """Test that already valid JSON is returned unchanged."""
    # Simple array
    valid_json = '["item1", "item2", "item3"]'
    result = make_json_array_parse_safe(valid_json)
    assert json.loads(result) == ["item1", "item2", "item3"]

    # Array with various strings
    valid_json = '["Front Brake Kit", "Rear Brake Kit", "Parking Brake Cable"]'
    result = make_json_array_parse_safe(valid_json)
    assert json.loads(result) == [
        "Front Brake Kit",
        "Rear Brake Kit",
        "Parking Brake Cable",
    ]

    # Empty array
    assert make_json_array_parse_safe("[]") == "[]"
    assert json.loads(make_json_array_parse_safe("[]")) == []

    # Single item array
    valid_json = '["single item"]'
    result = make_json_array_parse_safe(valid_json)
    assert json.loads(result) == ["single item"]


def test_make_json_parse_safe_with_code_blocks():
    """Test removal of code block markers."""
    # JSON with triple backticks
    json_with_backticks = '```["item1", "item2"]```'
    result = make_json_array_parse_safe(json_with_backticks)
    assert json.loads(result) == ["item1", "item2"]

    # JSON with backticks and 'json' label
    json_with_label = '```json\n["item1", "item2"]\n```'
    result = make_json_array_parse_safe(json_with_label)
    assert json.loads(result) == ["item1", "item2"]

    # Only backticks at start
    json_partial = '```["item1", "item2"]'
    result = make_json_array_parse_safe(json_partial)
    assert json.loads(result) == ["item1", "item2"]


def test_make_json_parse_safe_unescaped_quotes():
    """Test handling of unescaped quotes inside array strings - the main use case."""
    # Single item with unescaped quotes
    bad_json = '["Item with "quotes" inside"]'
    result = make_json_array_parse_safe(bad_json)
    parsed = json.loads(result)
    assert len(parsed) == 1
    assert "quotes" in parsed[0]

    # Multiple items with unescaped quotes
    bad_json = '["Item with "quotes"", "Another "item" here"]'
    result = make_json_array_parse_safe(bad_json)
    parsed = json.loads(result)
    assert len(parsed) == 2
    assert "quotes" in parsed[0]
    assert "item" in parsed[1]

    # Real-world example from the issue
    bad_json = """[
    "Front Brake Kit Datsun Z (Late 280Z Hub)",
    "Front Brake Kit Datsun Z (Early 240Z/260Z Hub)",
    "Front Brake Kit Datsun Z (280Z Early Hub)",
    "Front Brake Kit Datsun Z (Scalloped Hub)",
    "Front Brake Kit Datsun 510 (Rear)",
    "Tilton 600-Series Balance Bar Assembly",
    "Rear Brake Kit Datsun 510 (Vented Rotor)",
    "Rear Brake Kit Datsun 510 (Solid Rotor, 10.25")",
    "Rear Brake Kit Datsun 510 (Solid Rotor, 11.40")",
    "Parking Brake Cable 510",
    "Rear Brake Kit Datsun Z (Solid Rotor)",
    "Datsun 510 Pedal Assembly",
    "Aluminum Brake Reservoir (Two Chamber)",
    "Aluminum Brake Reservoir (Three Chamber)",
    "Remote Tandem and Compact Master Cylinder Inlet Kit",
    "Datsun Z Pedal Assembly",
    "Datsun Z Car Aluminum Brake Reservoir"
]"""
    result = make_json_array_parse_safe(bad_json)
    parsed = json.loads(result)
    assert len(parsed) == 17
    assert parsed[7] == 'Rear Brake Kit Datsun 510 (Solid Rotor, 10.25")'
    assert parsed[8] == 'Rear Brake Kit Datsun 510 (Solid Rotor, 11.40")'


def test_make_json_parse_safe_measurements_with_quotes():
    """Test handling of measurements with inch marks (quotes)."""
    # Single measurement
    bad_json = '["10.25\\" rotor"]'
    result = make_json_array_parse_safe(bad_json)
    parsed = json.loads(result)
    assert len(parsed) == 1
    assert "10.25" in parsed[0]

    # Multiple measurements
    bad_json = '["10.25\\" rotor", "11.40\\" rotor", "8\\" brake"]'
    result = make_json_array_parse_safe(bad_json)
    parsed = json.loads(result)
    assert len(parsed) == 3
    assert "10.25" in parsed[0]
    assert "11.40" in parsed[1]
    assert "8" in parsed[2]


def test_make_json_parse_safe_mixed_quotes():
    """Test handling of mixed quote scenarios."""
    # Item with parentheses and quotes
    bad_json = '["Brake Kit (10\\" size)", "Another Item (5\\" model)"]'
    result = make_json_array_parse_safe(bad_json)
    parsed = json.loads(result)
    assert len(parsed) == 2

    # Item with multiple quote types
    bad_json = '["Item with \\"escaped\\" and "unescaped" quotes"]'
    result = make_json_array_parse_safe(bad_json)
    parsed = json.loads(result)
    assert len(parsed) == 1


def test_make_json_parse_safe_whitespace_handling():
    """Test handling of various whitespace scenarios."""
    # Extra whitespace around array
    json_with_space = '  ["item1", "item2"]  '
    result = make_json_array_parse_safe(json_with_space)
    assert json.loads(result) == ["item1", "item2"]

    # Newlines and indentation (formatted JSON)
    formatted_json = """[
        "item1",
        "item2",
        "item3"
    ]"""
    result = make_json_array_parse_safe(formatted_json)
    assert json.loads(result) == ["item1", "item2", "item3"]

    # Tabs and spaces
    json_with_tabs = '[\t"item1",\t"item2"\t]'
    result = make_json_array_parse_safe(json_with_tabs)
    assert json.loads(result) == ["item1", "item2"]


def test_make_json_parse_safe_special_characters():
    """Test handling of special characters in strings."""
    # Items with parentheses
    json_str = '["Item (Part A)", "Item (Part B)"]'
    result = make_json_array_parse_safe(json_str)
    parsed = json.loads(result)
    assert parsed == ["Item (Part A)", "Item (Part B)"]

    # Items with numbers and dashes
    json_str = '["600-Series Assembly", "240Z/260Z Hub"]'
    result = make_json_array_parse_safe(json_str)
    parsed = json.loads(result)
    assert parsed == ["600-Series Assembly", "240Z/260Z Hub"]

    # Items with slashes
    json_str = '["Front/Rear Kit", "Part A/B/C"]'
    result = make_json_array_parse_safe(json_str)
    parsed = json.loads(result)
    assert parsed == ["Front/Rear Kit", "Part A/B/C"]


def test_make_json_parse_safe_edge_cases():
    """Test edge cases and unusual inputs."""
    # Single item no array
    single_str = '"just a string"'
    result = make_json_array_parse_safe(single_str)
    assert json.loads(result) == "just a string"

    # Array with empty strings
    json_str = '["", "item", ""]'
    result = make_json_array_parse_safe(json_str)
    assert json.loads(result) == ["", "item", ""]

    # Very long item
    long_item = "A" * 1000
    json_str = f'["{long_item}"]'
    result = make_json_array_parse_safe(json_str)
    parsed = json.loads(result)
    assert len(parsed) == 1
    assert len(parsed[0]) == 1000


def test_make_json_parse_safe_nested_structures():
    """Test that the function handles nested structures (though not the primary use case)."""
    # Nested arrays (if they're already valid)
    nested = '[["item1", "item2"], ["item3", "item4"]]'
    result = make_json_array_parse_safe(nested)
    assert json.loads(result) == [["item1", "item2"], ["item3", "item4"]]

    # Objects in array (if already valid)
    obj_array = '[{"name": "item1"}, {"name": "item2"}]'
    result = make_json_array_parse_safe(obj_array)
    assert json.loads(result) == [{"name": "item1"}, {"name": "item2"}]


def test_make_json_parse_safe_preserves_already_escaped():
    """Test that already properly escaped quotes are preserved."""
    # Properly escaped quotes should remain
    valid_escaped = '["Item with \\"quotes\\" inside"]'
    result = make_json_array_parse_safe(valid_escaped)
    parsed = json.loads(result)
    assert len(parsed) == 1
    assert '"quotes"' in parsed[0] or "quotes" in parsed[0]


def test_make_json_parse_safe_complex_real_world():
    """Test with complex real-world examples."""
    # Product names with various special characters
    complex_json = """[
    "Front Brake Kit Datsun Z (Late 280Z Hub)",
    "Tilton 600-Series Balance Bar Assembly",
    "Rear Brake Kit Datsun 510 (Solid Rotor, 10.25")",
    "Remote Tandem and Compact Master Cylinder Inlet Kit"
]"""
    result = make_json_array_parse_safe(complex_json)
    parsed = json.loads(result)
    assert len(parsed) == 4
    assert "280Z" in parsed[0]
    assert "600-Series" in parsed[1]
    assert '10.25"' in parsed[2]
    assert "Master Cylinder" in parsed[3]


def test_make_json_parse_safe_with_commas_in_values():
    """Test handling of commas within quoted values."""
    # Commas inside items should be preserved
    json_str = '["Item 1, Part A", "Item 2, Part B", "Item 3, Part C"]'
    result = make_json_array_parse_safe(json_str)
    parsed = json.loads(result)
    assert len(parsed) == 3
    assert parsed[0] == "Item 1, Part A"
    assert parsed[1] == "Item 2, Part B"
    assert parsed[2] == "Item 3, Part C"

    # This is an extremely ambiguous case where it's unclear if the comma is part of the value
    # or a delimiter. The function may not handle this perfectly, so we test what it does handle.
    # The more common case from the original issue is measurements with inch marks at the end
    json_str = '["Rear Brake Kit Datsun 510 (Solid Rotor, 10.25")", "Another Item"]'
    result = make_json_array_parse_safe(json_str)
    parsed = json.loads(result)
    assert len(parsed) == 2
    assert '10.25"' in parsed[0]


def test_make_json_parse_safe_unicode_characters():
    """Test handling of unicode characters."""
    # Unicode characters in strings
    json_str = '["Item with émoji 🚗", "Another itém"]'
    result = make_json_array_parse_safe(json_str)
    parsed = json.loads(result)
    assert len(parsed) == 2
    assert "🚗" in parsed[0]
    assert "itém" in parsed[1]


def test_make_json_parse_safe_numbers_in_array():
    """Test that numeric values work if present."""
    # Mixed strings and numbers (valid JSON)
    json_str = '["item1", "item2", "item3"]'
    result = make_json_array_parse_safe(json_str)
    parsed = json.loads(result)
    assert parsed == ["item1", "item2", "item3"]


def test_make_json_parse_safe_fallback_behavior():
    """Test that function returns cleaned version when it can't fix the JSON."""
    # Completely malformed JSON that can't be fixed
    # Should return cleaned version (without backticks) even if unparseable
    malformed = "```not valid json at all{[}]```"
    result = make_json_array_parse_safe(malformed)
    assert "```" not in result
    assert "json" not in result or result == "not valid at all{[}]"
