"""The synthesis block of a partial dump (Phase 3.2): one row per record with
the group's key, forms, focal form, snippet count, the synthesis, and the
own-name lint; the summary counts; the unsynthesized / unknown / retried ids.
(The per-snippet context quotes and their audit rode here from the
location-stage merge, 2026-09-03, until 2026-09-05.)"""

from core.models.extraction_schemas.synthesis import SynthesisAnswer, SynthesisRecordInput
from core.services.pipeline_nodes.multi_stage.llm_phrase_synthesis_node_service import (
    ChunkAnswer,
    ChunkSynthesisResult,
)
from core.utils.aggregation_fold import WindowInput, fold_document
from core.utils.synthesis_dump_util import (
    STATUS_NOT_SYNTHESIZED,
    STATUS_SYNTHESIZED,
    build_synthesis_dump,
)

SEP = "#" * 50
WINDOW = (
    f"{SEP}\nhttps://acme.example/materials\n\n"
    "We stock Aluminum and Brass. Sample Lead Time: 2 weeks.\n"
    "aluminum alloys ship daily.\n"
)


def _result() -> ChunkSynthesisResult:
    fold = fold_document([WindowInput(WINDOW, ["Aluminum", "Brass", "Lead Time", "ghost"])])
    records: list[SynthesisRecordInput] = fold.synthesis_records()
    ids = [r.record_id for r in records]
    answer = ChunkAnswer(
        sent_ids=ids,
        syntheses={
            ids[0]: SynthesisAnswer(synthesis="Acme Example stocks aluminum, the site says."),
            ids[2]: SynthesisAnswer(synthesis="lead time is quoted."),
        },
        unknown_answer_ids=["gnope"],
        retried_record_ids=[ids[1], ids[2]],
    )
    return ChunkSynthesisResult(
        fold=fold,
        records=records,
        wire_text=WINDOW,
        answer=answer,
        group_request_count=1,
        retry_request_count=1,
    )


def test_dump_rows_carry_group_context_and_the_synthesis_or_its_absence():
    dump = build_synthesis_dump(_result(), subject_name="Acme Example")
    rows = dump["records"]
    assert [r["focal_form"] for r in rows] == ["Aluminum", "Brass", "Lead Time"]
    assert rows[0]["forms"] == ["Aluminum", "aluminum"] and rows[0]["key"] == "aluminum"
    assert rows[0]["snippets"] == 2 and rows[0]["mention_count"] == 2
    assert rows[0]["status"] == STATUS_SYNTHESIZED and rows[0]["own_name_hits_in_synthesis"] == 1
    assert rows[1]["status"] == STATUS_NOT_SYNTHESIZED and rows[1]["synthesis"] is None
    assert rows[1]["retried"] is True and rows[2]["retried"] is True and rows[0]["retried"] is False
    summary = dump["summary"]
    assert summary["records"] == 3 and summary["snippets"] == 4 and summary["synthesized"] == 2
    assert summary["not_synthesized"] == [rows[1]["group_id"]]
    assert summary["unknown_answer_ids"] == ["gnope"]
    assert summary["retried"] == [rows[1]["group_id"], rows[2]["group_id"]]
    assert summary["group_requests"] == 1 and summary["retry_requests"] == 1
    assert summary["own_name_hits_in_syntheses"] == 1
    assert summary["synthesis_chars"] == sum(len(r["synthesis"]) for r in rows if r["synthesis"])
    # the empty 'ghost' bundle never reaches synthesis, so it has no row
    assert "ghost" not in {r["key"] for r in rows}
    # nothing per-snippet rides the row any more (2026-09-05)
    assert "snippet_contexts" not in rows[0] and "context_check" not in dump
