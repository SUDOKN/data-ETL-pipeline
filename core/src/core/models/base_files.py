from dataclasses import field
from functools import cached_property
from io import TextIOWrapper
from pathlib import Path


class FileOnDisk:
    name: str = field(init=False)
    full_path: Path = field(init=False)

    def __init__(
        self, output_dir: Path, prefix: str, timestamp_str: str, extension: str
    ):
        self.name = FileOnDisk.get_complete_file_name(timestamp_str, prefix, extension)
        self.full_path = FileOnDisk.get_complete_file_path(output_dir, self.name)

    @cached_property
    def pointer(self) -> TextIOWrapper:
        return self.full_path.open("w", encoding="utf-8")

    def close_pointer(self):
        if hasattr(self, "pointer"):
            self.pointer.close()

    def __post_init__(self):
        self.full_path.touch(exist_ok=True)

    def __str__(self) -> str:
        return str(self.full_path)

    def delete_file(self):
        self.close_pointer()
        if self.full_path.exists():
            self.full_path.unlink()

    @staticmethod
    def get_complete_file_name(timestamp_str: str, prefix: str, extension: str) -> str:
        return f"{timestamp_str}_{prefix}.{extension}"

    @staticmethod
    def get_complete_file_path(output_dir: Path, file_name: str) -> Path:
        return output_dir / file_name


class CSVFile(FileOnDisk):
    total_rows: int = 0

    def __init__(
        self, output_dir: Path, prefix: str, timestamp_str: str, headers: list[str]
    ):
        super().__init__(
            output_dir=output_dir,
            prefix=f"{prefix}",
            timestamp_str=timestamp_str,
            extension="csv",
        )
        self.add_csv_row(headers)

    def add_csv_row(self, row: list[str]):
        """Add a row to the CSV file."""
        line = ",".join(row) + "\n"
        self.pointer.write(line)
        self.total_rows += 1


class JSONLFile(FileOnDisk):
    def __init__(
        self, output_dir: Path, common_prefix: str, file_index: int, timestamp_str: str
    ):
        super().__init__(
            output_dir=output_dir,
            prefix=f"{common_prefix}_{file_index:04d}",
            timestamp_str=timestamp_str,
            extension="jsonl",
        )

    @staticmethod
    def get_json_line_size_in_bytes(json_line: str) -> int:
        # Size includes the JSON string + newline character
        return len(json_line.encode("utf-8")) + 1  # +1 for newline character
