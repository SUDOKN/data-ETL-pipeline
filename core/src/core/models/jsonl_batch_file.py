from dataclasses import dataclass
from core.models.base_files import JSONLFile


@dataclass
class JSONLBatchFileSummary:
    unique_items: int
    unique_lines: int
    request_count: int
    total_tokens: int

    # to dict
    def to_dict(self) -> dict:
        return {
            "unique_items": self.unique_items,
            "unique_lines": self.unique_lines,
            "request_count": self.request_count,
            "total_tokens": self.total_tokens,
        }


class JSONLBatchFile(JSONLFile):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.unique_item_ids: set[str] = set()
        self.unique_line_ids: set[str] = set()
        self.total_requests: int = 0
        self.total_tokens: int = 0
        self.size_in_bytes: int = 0

    def can_batch_file_fit_item(
        self,
        item_tokens: int,
        item_request_count: int,
        item_size_in_bytes: int,
        max_requests_per_file: int,
        max_tokens_per_file: int,
        max_file_size_in_bytes: int,
    ) -> bool:
        """
        Check if adding an item would exceed any constraints.

        1 request = 1 line in JSONL file
        """
        if (
            self.total_requests + item_request_count > max_requests_per_file
            or self.total_tokens + item_tokens > max_tokens_per_file
            or (self.size_in_bytes + item_size_in_bytes) > max_file_size_in_bytes
        ):
            return False
        return True

    def add_json_line(
        self,
        item_id: str,  # may repeat for multiple lines
        line_id: str,  # unique per line
        json_line: str,
        tokens: int,
    ):
        """Update the batch file stats after adding an item."""
        self.unique_item_ids.add(item_id)
        self.unique_line_ids.add(line_id)
        self.pointer.write(json_line + "\n")
        self.total_tokens += tokens
        self.total_requests += 1
        self.size_in_bytes += JSONLBatchFile.get_json_line_size_in_bytes(json_line)

    def close_pointer(self):
        if hasattr(self, "pointer"):
            self.pointer.close()

    def get_summary(self) -> JSONLBatchFileSummary:
        return JSONLBatchFileSummary(
            unique_items=len(self.unique_item_ids),
            unique_lines=len(self.unique_line_ids),
            request_count=self.total_requests,
            total_tokens=self.total_tokens,
        )
