"""The recursive base's eager loop dispatches exactly the requests that have no
answer yet — no more, no less.

NO MORE: a node may create pre-answered requests — the mention stage's single
dummy for a window with nothing to locate (built for NO_MODEL, response filled
in) — and until 2026-08-23 those were sent too: run 20260823T034518 lost
steelcraft's seven phrase fields when dispatch_gpt_batch_request's model guard
met one.

NO LESS: until 2026-08-24 the loop dispatched only the requests it had just
CREATED, so a stored row with no response — the state record_response_parse_error
writes on purpose so the next pass re-asks — was never re-dispatched. Because
get_missing_req_ids only asks whether a request DOCUMENT exists, the loop broke,
are_all_requests_complete stayed False, and execute returned without calling
next_node: no exception, no dump, a silently vanished field. Run
20260824T012721 lost 36% of its records that way.
"""

from datetime import datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest

import core.models.pipeline_nodes.base.base_llm_recursive_extraction_node as recursive_module
from core.models.pipeline_nodes.base.base_llm_recursive_extraction_node import (
    MAX_UNPRODUCTIVE_PASSES,
    BaseLLMRecursiveExtractionNode,
)
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.pipeline_nodes.base.pipeline_stage import PipelineStage

T0 = datetime(2026, 8, 23, 4, 0, 0)


class _Field:
    name = "products"

    def __hash__(self) -> int:
        return hash(self.name)


def _request(custom_id: str, answered: bool) -> Any:
    return SimpleNamespace(
        request=SimpleNamespace(custom_id=custom_id),
        response=SimpleNamespace(result="{}") if answered else None,
    )


class _Node(BaseLLMRecursiveExtractionNode):
    stage = PipelineStage.mention_collection

    def __init__(self) -> None:
        super().__init__(field_type=cast(Any, _Field()), next_node=cast(Any, None))
        self.dispatched: list[str] = []
        self._missing_calls = 0
        # Per-test knobs; a test reassigns these before calling execute.
        self.embedded_ids: set[str] = {"dummy", "real"}
        self.created: list[tuple[str, bool]] = [("dummy", True), ("real", False)]
        self.missing_on_first_pass: set[str] = {"dummy", "real"}
        self.complete: bool = True

    async def embed_request_ids(self, *args, **kwargs) -> None: ...

    def get_embedded_request_ids(self, *args, **kwargs):
        return set(self.embedded_ids)

    def get_request_custom_id(self, *args, **kwargs) -> str:
        return "id"

    async def get_result(self, *args, **kwargs):
        return None

    async def get_missing_req_ids(self, *args, **kwargs):  # type: ignore[override]
        self._missing_calls += 1
        return set(self.missing_on_first_pass) if self._missing_calls == 1 else set()

    async def create_batch_requests(self, *args, **kwargs):  # type: ignore[override]
        return [_request(cid, answered=a) for cid, a in self.created]

    async def dispatch_batch_request(self, gpt_batch_request, metadata):  # type: ignore[override]
        self.dispatched.append(gpt_batch_request.request.custom_id)
        return SimpleNamespace(request_custom_id=gpt_batch_request.request.custom_id)

    async def are_all_requests_complete(self, *args, **kwargs):  # type: ignore[override]
        return self.complete

    async def get_incomplete_req_ids(self, *args, **kwargs):  # type: ignore[override]
        return set()

    async def get_completed_request_map(self, *args, **kwargs):  # type: ignore[override]
        return {}


class _Store:
    """The rows the DB holds, and the two queries the loop makes of them."""

    def __init__(self, prestored: dict[str, bool] | None = None) -> None:
        # custom_id -> answered
        self.rows: dict[str, bool] = dict(prestored or {})
        self.upserted: list[list[str]] = []
        self.recorded: list[list[str]] = []
        self.recording_works = True

    async def upsert(self, batch_requests, subject_unique_id):
        self.upserted.append([r.request.custom_id for r in batch_requests])
        for r in batch_requests:
            self.rows.setdefault(r.request.custom_id, r.response is not None)

    async def record(self, batch_requests, response_blobs, timestamp):
        assert len(batch_requests) == len(response_blobs)
        self.recorded.append([r.request.custom_id for r in batch_requests])
        if self.recording_works:
            for r in batch_requests:
                self.rows[r.request.custom_id] = True
            return len(batch_requests), 0
        return 0, len(batch_requests)

    async def find_incomplete(self, subject_unique_id, custom_ids):
        assert custom_ids, "the real query rejects an empty id list"
        return {
            cid: _request(cid, answered=False)
            for cid in custom_ids
            if cid in self.rows and not self.rows[cid]
        }


def _install(monkeypatch, store: _Store) -> None:
    monkeypatch.setattr(
        recursive_module,
        "bulk_upsert_gpt_batch_requests_with_only_req_bodies",
        store.upsert,
    )
    monkeypatch.setattr(recursive_module, "bulk_record_gpt_batch_responses", store.record)
    monkeypatch.setattr(
        recursive_module,
        "find_incomplete_gpt_batch_requests_by_custom_ids",
        store.find_incomplete,
    )


async def _run(node: _Node) -> None:
    async def save() -> None: ...

    deferred = SimpleNamespace(
        subject_unique_id="acme.example",
        products=SimpleNamespace(metadata=None, chunked_request_map={"0:10": object()}),
        save=save,
    )
    await node.execute(
        subject=cast(Any, SimpleNamespace(subject_unique_id="acme.example")),
        deferred_subject=cast(Any, deferred),
        scraped_text_file=cast(Any, SimpleNamespace(text="")),
        timestamp=T0,
        pipeline_context=PipelineContext(),
        eager=True,
    )


@pytest.mark.asyncio
async def test_pre_answered_requests_are_created_and_recorded_but_never_dispatched(monkeypatch):
    store = _Store()
    _install(monkeypatch, store)
    node = _Node()
    await _run(node)
    assert store.upserted == [["dummy", "real"]]  # the dummy is stored, answered
    assert node.dispatched == ["real"]  # and never sent
    assert store.recorded == [["real"]]


@pytest.mark.asyncio
async def test_a_stored_request_with_no_response_is_re_dispatched(monkeypatch):
    """The Defect B regression: nothing is MISSING (the row exists) but it is
    unanswered, so the loop must still re-ask it."""
    store = _Store(prestored={"dummy": True, "real": False})
    _install(monkeypatch, store)
    node = _Node()
    node.missing_on_first_pass = set()  # every document already exists
    await _run(node)
    assert store.upserted == []  # nothing to create
    assert node.dispatched == ["real"]  # but the unanswered row is re-asked
    assert store.recorded == [["real"]]


@pytest.mark.asyncio
async def test_nothing_outstanding_dispatches_nothing(monkeypatch):
    store = _Store(prestored={"dummy": True, "real": True})
    _install(monkeypatch, store)
    node = _Node()
    node.missing_on_first_pass = set()
    await _run(node)
    assert node.dispatched == [] and store.upserted == [] and store.recorded == []


@pytest.mark.asyncio
async def test_finishing_incomplete_raises_instead_of_returning_silently(monkeypatch):
    """The loop can only end with work outstanding through an inconsistency
    between the two queries — and that must be loud, not a vanished field."""
    store = _Store(prestored={"dummy": True, "real": True})
    _install(monkeypatch, store)
    node = _Node()
    node.missing_on_first_pass = set()
    node.complete = False  # ... yet are_all_requests_complete disagrees
    with pytest.raises(ValueError, match="still unanswered"):
        await _run(node)


@pytest.mark.asyncio
async def test_a_response_that_never_records_is_bounded(monkeypatch):
    """A dispatch whose response never records would otherwise loop forever now
    that convergence is over unanswered requests rather than absent ones."""
    store = _Store(prestored={"dummy": True, "real": False})
    store.recording_works = False
    _install(monkeypatch, store)
    node = _Node()
    node.missing_on_first_pass = set()
    with pytest.raises(ValueError, match="made no progress"):
        await _run(node)
    assert node.dispatched == ["real"] * MAX_UNPRODUCTIVE_PASSES


# --- the parse-error retry budget (2026-08-25) -------------------------------
#
# Making the re-dispatch REACHABLE (above) did not make it REACHED: the parse
# failure that arms it is raised from inside embed_request_ids, at the TOP of
# the loop, so the exception escaped before the dispatch at the bottom could
# spend one of the RESPONSE_PARSE_ERROR_CAP re-asks that
# record_response_parse_error_capped's docstring promises. Run 20260824T190359
# surfaced a grounding parse failure on attempt 1 of 4 and lost both subjects
# — nine completed fields of steelcraft.com to a single record.


def _raises_on_first_passes(node: _Node, error: Exception, times: int = 1) -> None:
    """Make embed_request_ids fail its first ``times`` passes, as a recorded
    parse error does: the row it nulled is already unanswered in the store."""
    node.embed_calls = 0  # type: ignore[attr-defined]

    async def embed(*args, **kwargs) -> None:
        node.embed_calls += 1  # type: ignore[attr-defined]
        if node.embed_calls <= times:  # type: ignore[attr-defined]
            raise error

    node.embed_request_ids = embed  # type: ignore[assignment,method-assign]


@pytest.mark.asyncio
async def test_a_held_parse_error_lets_the_request_be_re_dispatched(monkeypatch):
    """The pass finishes, the nulled row is found unanswered, and it is re-asked
    — which is the whole point of nulling it."""
    store = _Store(prestored={"stored": False})
    _install(monkeypatch, store)
    node = _Node()
    node.embedded_ids = {"stored"}
    node.missing_on_first_pass = set()
    _raises_on_first_passes(node, ValueError("option 'Testing' is not a vocabulary label"))

    await _run(node)

    assert node.dispatched == ["stored"]
    assert node.embed_calls == 2  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_an_error_that_leaves_nothing_to_re_ask_is_re_raised(monkeypatch):
    """Holding is for errors the retry can act on. One that armed nothing is a
    genuine bug — a re-defer, a malformed request — and must surface unchanged
    rather than be spun on until the unproductive bound trips."""
    store = _Store()
    _install(monkeypatch, store)
    node = _Node()
    node.embedded_ids = set()
    node.missing_on_first_pass = set()
    _raises_on_first_passes(node, ValueError("the upstream state changed"))

    with pytest.raises(ValueError, match="the upstream state changed"):
        await _run(node)
    assert node.dispatched == []


@pytest.mark.asyncio
async def test_repeated_parse_failure_is_never_held(monkeypatch):
    """It is raised INSTEAD of recording, so it arms no re-dispatch and means
    the budget is spent. Held, it would cycle a response that cannot parse."""
    store = _Store(prestored={"stored": False})
    _install(monkeypatch, store)
    node = _Node()
    node.embedded_ids = {"stored"}
    node.missing_on_first_pass = set()
    _raises_on_first_passes(
        node, recursive_module.RepeatedParseFailure("out of re-dispatches")
    )

    with pytest.raises(recursive_module.RepeatedParseFailure):
        await _run(node)
    assert node.dispatched == []


@pytest.mark.asyncio
async def test_a_parse_error_that_repeats_is_bounded(monkeypatch):
    """Two bounds stop a spin; this is the one that fires when the response
    keeps failing to parse without the cap having been reached."""
    store = _Store(prestored={"stored": False})
    store.recording_works = False
    _install(monkeypatch, store)
    node = _Node()
    node.embedded_ids = {"stored"}
    node.missing_on_first_pass = set()
    _raises_on_first_passes(node, ValueError("not a vocabulary label"), times=99)

    with pytest.raises(ValueError, match="made no progress"):
        await _run(node)
    assert node.dispatched == ["stored"] * MAX_UNPRODUCTIVE_PASSES
