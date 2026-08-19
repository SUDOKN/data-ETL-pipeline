"""The provenance, pricing and timing an extraction dump carries, partial or full.

A dump is what an evaluation reads. Without a header naming the prompts, models,
ontology and text that produced it, two dumps a day apart are indistinguishable
and any verdict drawn by comparing them is unfounded. Full dumps carried no
header at all until 2026-08-13 — only the stage-gated partial path had one.

Since 2026-08-14 each request also carries the token usage and timestamps off
its stored response, and the header carries the rollups — so the cost and
duration of a run are facts in the file, not a trip to Mongo. Per-request MODEL
latency exists only for eagerly dispatched requests; the Batch API does not
report it, so batch requests carry timestamps and turnaround instead.
"""

import json
from datetime import datetime, timezone
from enum import Enum

from core.models.deferred_extraction.deferred_concept_extraction import (
    IterativeTaggingRequest,
)
from core.utils.extraction_dump_util import (
    build_chunk_requests,
    build_run_provenance,
    write_extraction_dump,
)


class _Field(str, Enum):
    conformity_attestations = "conformity_attestations"
    is_contract_manufacturer = "is_contract_manufacturer"


class _ScrapedText:
    s3_version_id = "text-version-1"
    num_tokens = 23757
    last_modified_on = datetime(2026, 8, 13, 19, 0, 0)


class _Metadata:
    @staticmethod
    def model_dump(mode: str = "python") -> dict[str, object]:
        return {
            "ontology_version_id": "ontology-v1",
            "llm_phrase_search": {
                "llm_model": {"name": "gpt-4o-mini"},
                "prompt_version_id": "PROMPT-VERSION-1",
                "catalog_version": None,
            },
        }


class _Usage:
    def __init__(self, prompt_tokens: int, completion_tokens: int) -> None:
        self.prompt_tokens = prompt_tokens
        self.completion_tokens = completion_tokens
        self.total_tokens = prompt_tokens + completion_tokens


class _ChatCompletion:
    def __init__(self, model: str, usage: _Usage, created: datetime) -> None:
        self.model = model
        self.usage = usage
        self.created = created


class _Response:
    def __init__(
        self,
        model: str,
        prompt_tokens: int,
        completion_tokens: int,
        created: datetime,
        client_latency_ms=None,
        openai_processing_ms=None,
    ) -> None:
        self.chat_completion_result = _ChatCompletion(
            model, _Usage(prompt_tokens, completion_tokens), created
        )
        self.client_latency_ms = client_latency_ms
        self.openai_processing_ms = openai_processing_ms


def _utc(hour: int, minute: int, second: int) -> datetime:
    return datetime(2026, 8, 14, hour, minute, second, tzinfo=timezone.utc)


class _RequestDoc:
    def __init__(
        self,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        model: str = "gpt-4o-mini-2024-07-18",
        created_at: datetime | None = None,
        completed_at: datetime | None = None,
        client_latency_ms=None,
        openai_processing_ms=None,
    ) -> None:
        self.created_at = created_at
        self.response = _Response(
            model,
            prompt_tokens,
            completion_tokens,
            completed_at or _utc(23, 59, 59),
            client_latency_ms=client_latency_ms,
            openai_processing_ms=openai_processing_ms,
        )


class _ConceptBundle:
    brute = {"steel"}
    search_sub_bounds = ["0-100"]
    llm_phrase_search_req_ids = ["req>search>chunk>0-100"]
    # sub-window bounds -> that sub-window's rounds
    llm_phrase_recursive_search_req_ids = {"0-100": ["req>recursive>0"]}
    llm_phrase_relationship_req_ids = ["req>rel>0", "req>rel>1"]
    llm_phrase_relationship_screening_req_ids: list[str] = []
    llm_phrase_initial_grounding_req_ids = ["req>ig>0"]
    llm_phrase_recursive_tagging_reqs = {
        1: {
            IterativeTaggingRequest(
                parent_descend_req_id=None,
                descend_req_id="req>descend>l[1]>Machining",
                name="Machining",
                level=1,
            )
        },
        2: {
            IterativeTaggingRequest(
                parent_descend_req_id="req>descend>l[1]>Machining",
                descend_req_id="req>descend>l[2]>Milling",
                name="Milling",
                level=2,
            )
        },
    }


class _SingleStageBundle:
    """The single-stage bundle's one request field predates the ``_req_id``
    naming convention, so it is matched by name, not suffix."""

    llm_request_id = "req>single>chunk>0-100"


# created_at values are deliberately NAIVE (as Mongo returns them) while
# completion timestamps are aware UTC — the entry builder must normalize both
# before differencing, or the arithmetic raises.
_COMPLETED = {
    "req>search>chunk>0-100": _RequestDoc(
        4210, 512, created_at=datetime(2026, 8, 14, 19, 0, 0), completed_at=_utc(19, 4, 34)
    ),
    "req>recursive>0": _RequestDoc(
        900, 120, created_at=datetime(2026, 8, 14, 19, 5, 0), completed_at=_utc(19, 6, 0)
    ),
    "req>rel>0": _RequestDoc(
        1000, 200, created_at=datetime(2026, 8, 14, 19, 5, 0), completed_at=_utc(19, 7, 0)
    ),
    "req>rel>1": _RequestDoc(
        1100, 300, created_at=datetime(2026, 8, 14, 19, 5, 0), completed_at=_utc(19, 8, 30)
    ),
    "req>ig>0": _RequestDoc(
        800, 90, created_at=datetime(2026, 8, 14, 19, 9, 0), completed_at=_utc(19, 10, 0)
    ),
    "req>descend>l[1]>Machining": _RequestDoc(
        500, 40, created_at=datetime(2026, 8, 14, 19, 10, 30), completed_at=_utc(19, 11, 0)
    ),
    "req>descend>l[2]>Milling": _RequestDoc(
        480, 44, created_at=datetime(2026, 8, 14, 19, 11, 30), completed_at=_utc(19, 12, 0)
    ),
    "req>single>chunk>0-100": _RequestDoc(
        30000, 60, created_at=datetime(2026, 8, 14, 19, 16, 33), completed_at=_utc(19, 21, 7)
    ),
}


def _ids(entries: list[dict[str, object]]) -> list[str]:
    return [entry["custom_id"] for entry in entries]


def test_requests_are_reported_per_stage_with_usage_and_timing():
    requests = build_chunk_requests(_ConceptBundle(), _COMPLETED)

    # A single-request stage still reports a list, so a reader never has to
    # branch on the shape.
    assert requests["llm_phrase_search"] == [
        {
            "custom_id": "req>search>chunk>0-100",
            "input_tokens": 4210,
            "output_tokens": 512,
            "created_at": "2026-08-14T19:00:00+00:00",
            "completed_at": "2026-08-14T19:04:34+00:00",
            "turnaround_seconds": 274,
        }
    ]
    assert _ids(requests["llm_phrase_relationship"]) == ["req>rel>0", "req>rel>1"]
    assert requests["llm_phrase_relationship"][1]["output_tokens"] == 300
    assert requests["llm_phrase_relationship"][1]["turnaround_seconds"] == 210
    assert _ids(requests["llm_phrase_initial_grounding"]) == ["req>ig>0"]


def test_a_stage_with_no_requests_is_absent_not_empty():
    """Same convention the rows use: absent means it did not run. An empty list
    would read as "it ran and produced nothing"."""
    requests = build_chunk_requests(_ConceptBundle(), _COMPLETED)

    assert "llm_phrase_relationship_screening" not in requests


def test_the_descent_tree_is_reported_by_level():
    requests = build_chunk_requests(_ConceptBundle(), _COMPLETED)

    tree = requests["llm_phrase_recursive_tagging"]
    assert {level: _ids(entries) for level, entries in tree.items()} == {
        1: ["req>descend>l[1]>Machining"],
        2: ["req>descend>l[2]>Milling"],
    }
    assert tree[2][0]["input_tokens"] == 480


def test_non_request_bundle_fields_are_not_mistaken_for_requests():
    """``brute`` is a set of matched labels and ``search_sub_bounds`` is chunk
    geometry — neither is batch requests."""
    requests = build_chunk_requests(_ConceptBundle(), _COMPLETED)
    assert "brute" not in requests
    assert "search_sub_bounds" not in requests
    assert not any("sub_bounds" in stage for stage in requests)


def test_the_single_stage_request_field_is_recognized():
    """``llm_request_id`` does not follow the suffix convention; losing it made
    every single-stage dump claim its chunk answered no requests at all."""
    requests = build_chunk_requests(_SingleStageBundle(), _COMPLETED)

    assert requests == {
        "single_stage_extraction": [
            {
                "custom_id": "req>single>chunk>0-100",
                "input_tokens": 30000,
                "output_tokens": 60,
                "created_at": "2026-08-14T19:16:33+00:00",
                "completed_at": "2026-08-14T19:21:07+00:00",
                "turnaround_seconds": 274,
            }
        ]
    }


def test_latency_fields_appear_only_on_eagerly_dispatched_requests():
    """The Batch API reports no per-request model latency, so batch entries
    must not grow fields to say nothing was measured."""
    completed = {
        "req>search>chunk>0-100": _RequestDoc(
            4210,
            512,
            created_at=datetime(2026, 8, 14, 19, 0, 0),
            completed_at=_utc(19, 0, 12),
            client_latency_ms=11840,
            openai_processing_ms=11212,
        ),
        "req>recursive>0": _RequestDoc(
            900, 120, created_at=datetime(2026, 8, 14, 19, 5, 0), completed_at=_utc(19, 6, 0)
        ),
    }
    requests = build_chunk_requests(_ConceptBundle(), completed)

    eager_entry = requests["llm_phrase_search"][0]
    assert eager_entry["client_latency_ms"] == 11840
    assert eager_entry["openai_processing_ms"] == 11212
    batch_entry = requests["llm_phrase_recursive_search"]["0-100"][0]
    assert "client_latency_ms" not in batch_entry
    assert "openai_processing_ms" not in batch_entry


def test_a_synthetic_response_prices_as_null_not_as_its_fake_tokens():
    """Dummy responses store 1/1 usage, but no API call happened — counting
    them would claim spend that never occurred; their fabricated completion
    time must not enter any span either."""
    completed = {
        "req>search>chunk>0-100": _RequestDoc(1, 1, model="no_model"),
    }
    requests = build_chunk_requests(_ConceptBundle(), completed)

    entry = requests["llm_phrase_search"][0]
    assert entry["input_tokens"] is None
    assert entry["output_tokens"] is None
    assert entry["note"] == "synthetic_response"
    assert "completed_at" not in entry


def test_a_request_the_completed_map_cannot_answer_for_is_marked():
    """Null tokens must be distinguishable from zero AND from a dummy — a
    missing lookup is a defect worth seeing, not a cost of zero."""
    requests = build_chunk_requests(_ConceptBundle(), {})

    entry = requests["llm_phrase_search"][0]
    assert entry["input_tokens"] is None
    assert entry["note"] == "usage_unavailable"


def test_full_run_provenance_claims_no_stage_gating():
    provenance = build_run_provenance(
        metadata=_Metadata(),
        scraped_text_file=_ScrapedText(),
        partial=False,
    )

    assert provenance["partial"] is False
    # The stage keys belong to a stopped run; on a full dump their presence
    # would imply something was switched off.
    assert "stopped_at" not in provenance
    assert "stages_disabled" not in provenance
    assert provenance["scraped_text"]["s3_version_id"] == "text-version-1"
    assert provenance["scraped_text"]["num_tokens"] == 23757
    assert provenance["extraction_metadata"]["ontology_version_id"] == "ontology-v1"


def test_partial_run_provenance_adds_the_stage_picture():
    provenance = build_run_provenance(
        metadata=_Metadata(),
        scraped_text_file=_ScrapedText(),
        partial=True,
        stopped_at="screening",
        stages_run=["phrase_search", "relationship"],
        stages_disabled=["reconcile", "screening"],
    )

    assert provenance["stopped_at"] == "screening"
    assert provenance["stages_run"] == ["phrase_search", "relationship"]
    assert "note" in provenance
    # ...on top of everything a full dump carries, not instead of it.
    assert (
        provenance["extraction_metadata"]["llm_phrase_search"]["prompt_version_id"]
        == "PROMPT-VERSION-1"
    )


def _capture_written(monkeypatch) -> dict[str, str]:
    written: dict[str, str] = {}
    monkeypatch.setattr("pathlib.Path.mkdir", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "pathlib.Path.write_text",
        lambda self, text, encoding=None: written.update(
            {"path": str(self), "text": text}
        ),
    )
    return written


def test_written_dump_puts_provenance_ahead_of_the_chunks(monkeypatch):
    written = _capture_written(monkeypatch)

    write_extraction_dump(
        subject_unique_id="steelcraft.com",
        field_type=_Field.conformity_attestations,
        timestamp=datetime(2026, 8, 13, 19, 33, 13),
        chunked_contents={"0-100": {"rows": [{"phrase": "SDI Certified"}]}},
        chunked_request_map={"0-100": _ConceptBundle()},
        completed_requests=_COMPLETED,
        run_provenance=build_run_provenance(
            metadata=_Metadata(),
            scraped_text_file=_ScrapedText(),
            partial=False,
        ),
    )

    payload = json.loads(written["text"])
    # Order matters: a reader has to meet the provenance before the first row.
    assert list(payload) == [
        "subject_unique_id",
        "field_type",
        "timestamp",
        "run",
        "chunks",
    ]
    chunk = payload["chunks"]["0-100"]
    assert chunk["rows"] == [{"phrase": "SDI Certified"}]
    assert _ids(chunk["requests"]["llm_phrase_search"]) == ["req>search>chunk>0-100"]
    assert payload["run"]["extraction_metadata"]["ontology_version_id"] == "ontology-v1"
    assert written["path"].split("/")[-3] == "extraction_dumps"


def test_the_header_rollups_sum_every_chunk_and_sit_before_the_metadata(
    monkeypatch,
):
    """The rollups are the numbers a reader wants first; burying them after
    ~220 lines of dumped metadata would defeat them."""
    written = _capture_written(monkeypatch)

    write_extraction_dump(
        subject_unique_id="steelcraft.com",
        field_type=_Field.conformity_attestations,
        timestamp=datetime(2026, 8, 13, 19, 33, 13),
        chunked_contents={"0-100": {"rows": []}},
        chunked_request_map={"0-100": _ConceptBundle()},
        completed_requests=_COMPLETED,
        run_provenance=build_run_provenance(
            metadata=_Metadata(),
            scraped_text_file=_ScrapedText(),
            partial=False,
        ),
    )

    run = json.loads(written["text"])["run"]
    assert list(run) == [
        "partial",
        "scraped_text",
        "token_usage",
        "time_span",
        "extraction_metadata",
    ]
    usage = run["token_usage"]
    # Every priced request in _COMPLETED except the single-stage one, which
    # this bundle does not reference.
    assert usage["input_tokens"] == 4210 + 900 + 1000 + 1100 + 800 + 500 + 480
    assert usage["output_tokens"] == 512 + 120 + 200 + 300 + 90 + 40 + 44
    assert usage["by_stage"]["llm_phrase_search"] == {
        "input_tokens": 4210,
        "output_tokens": 512,
    }
    # The descent tree's levels are flattened into one stage total.
    assert usage["by_stage"]["llm_phrase_recursive_tagging"] == {
        "input_tokens": 980,
        "output_tokens": 84,
    }


def test_time_spans_at_run_stage_and_chunk_granularity(monkeypatch):
    """Wall-clock spans: earliest request creation to latest completion, with
    the bounds kept so a suspicious duration is checkable inside the file."""
    written = _capture_written(monkeypatch)

    write_extraction_dump(
        subject_unique_id="steelcraft.com",
        field_type=_Field.conformity_attestations,
        timestamp=datetime(2026, 8, 13, 19, 33, 13),
        chunked_contents={"0-100": {"rows": []}},
        chunked_request_map={"0-100": _ConceptBundle()},
        completed_requests=_COMPLETED,
        run_provenance=build_run_provenance(
            metadata=_Metadata(),
            scraped_text_file=_ScrapedText(),
            partial=False,
        ),
    )

    payload = json.loads(written["text"])
    run_span = payload["run"]["time_span"]
    assert run_span["started_at"] == "2026-08-14T19:00:00+00:00"
    assert run_span["ended_at"] == "2026-08-14T19:12:00+00:00"
    assert run_span["seconds"] == 720
    # Stage spans are real waits: a stage's requests only exist once its
    # predecessor parsed.
    assert run_span["by_stage"]["llm_phrase_recursive_tagging"] == {
        "started_at": "2026-08-14T19:10:30+00:00",
        "ended_at": "2026-08-14T19:12:00+00:00",
        "seconds": 90,
    }
    # One chunk here, so its span is the run's minus the per-stage breakdown.
    assert payload["chunks"]["0-100"]["time_span"] == {
        "started_at": "2026-08-14T19:00:00+00:00",
        "ended_at": "2026-08-14T19:12:00+00:00",
        "seconds": 720,
    }


def test_unpriceable_requests_are_left_out_of_the_rollups(monkeypatch):
    """A synthetic or unanswerable request contributes nothing — not zero rows
    of fake spend, and not a fabricated instant inside a span."""
    written = _capture_written(monkeypatch)

    write_extraction_dump(
        subject_unique_id="steelcraft.com",
        field_type=_Field.conformity_attestations,
        timestamp=datetime(2026, 8, 13, 19, 33, 13),
        chunked_contents={"0-100": {"rows": []}},
        chunked_request_map={"0-100": _ConceptBundle()},
        completed_requests={
            "req>search>chunk>0-100": _RequestDoc(
                4210,
                512,
                created_at=datetime(2026, 8, 14, 19, 0, 0),
                completed_at=_utc(19, 4, 34),
            ),
            "req>rel>0": _RequestDoc(1, 1, model="no_model"),
        },
        run_provenance=build_run_provenance(
            metadata=_Metadata(),
            scraped_text_file=_ScrapedText(),
            partial=False,
        ),
    )

    run = json.loads(written["text"])["run"]
    usage = run["token_usage"]
    assert usage["input_tokens"] == 4210
    assert usage["output_tokens"] == 512
    assert "llm_phrase_relationship" not in usage["by_stage"]
    # The span is bounded by the one real request alone.
    assert run["time_span"]["seconds"] == 274
    assert list(run["time_span"]["by_stage"]) == ["llm_phrase_search"]


def test_a_single_stage_chunk_carries_its_result(monkeypatch):
    """Single-stage fields have no phrases; the chunk's content is the parsed
    result. Until 2026-08-14 these dumps were header-only shells."""
    written = _capture_written(monkeypatch)

    write_extraction_dump(
        subject_unique_id="steelcraft.com",
        field_type=_Field.is_contract_manufacturer,
        timestamp=datetime(2026, 8, 14, 19, 16, 33),
        chunked_contents={
            "0-100": {"result": {"answer": True, "confidence": 4}}
        },
        chunked_request_map={"0-100": _SingleStageBundle()},
        completed_requests=_COMPLETED,
        run_provenance=build_run_provenance(
            metadata=_Metadata(),
            scraped_text_file=_ScrapedText(),
            partial=False,
        ),
    )

    payload = json.loads(written["text"])
    chunk = payload["chunks"]["0-100"]
    assert chunk["result"] == {"answer": True, "confidence": 4}
    assert "rows" not in chunk
    assert _ids(chunk["requests"]["single_stage_extraction"]) == [
        "req>single>chunk>0-100"
    ]
    assert payload["run"]["token_usage"]["by_stage"]["single_stage_extraction"] == {
        "input_tokens": 30000,
        "output_tokens": 60,
    }
    assert chunk["time_span"]["seconds"] == 274


def test_a_chunk_with_no_bundle_still_writes_its_rows(monkeypatch):
    """Diagnostics must not vanish because the request map disagrees with the
    rows — the rows are the thing being read. With nothing to time, no span
    key appears anywhere: absent reads as "not measurable", zero would read
    as "instant"."""
    written = _capture_written(monkeypatch)

    write_extraction_dump(
        subject_unique_id="steelcraft.com",
        field_type=_Field.conformity_attestations,
        timestamp=datetime(2026, 8, 13, 19, 33, 13),
        chunked_contents={"999-1000": {"rows": [{"phrase": "orphan"}]}},
        chunked_request_map={},
        completed_requests={},
        run_provenance=build_run_provenance(
            metadata=_Metadata(),
            scraped_text_file=_ScrapedText(),
            partial=False,
        ),
    )

    payload = json.loads(written["text"])
    chunk = payload["chunks"]["999-1000"]
    assert chunk["requests"] == {}
    assert chunk["rows"] == [{"phrase": "orphan"}]
    assert "time_span" not in chunk
    assert "time_span" not in payload["run"]
