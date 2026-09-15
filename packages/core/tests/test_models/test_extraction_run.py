"""The per-run extraction record (2026-08-27).

The subject document is ``save()``d over on every run, so it can only answer
"what does this manufacturer look like now". ``extraction_runs`` answers "what
did run X do" — the question the aggregation fold exists to make answerable,
since two extractions of one site fold differently.

The fold is written to BOTH sinks by decision: reading one manufacturer must
not need a second lookup, and comparing two runs must be possible. These pin
that the two copies are the same object and that the run document is keyed so
a re-entrant reconcile updates rather than duplicates.
"""

from datetime import datetime, timezone
from typing import Optional, TypedDict

import pytest

from core.db_models.extraction_run import ExtractionRun
from core.models.extraction_schemas.run_provenance import (
    RunProvenance,
    StoredPageExclusion,
    build_run_provenance_record,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    InitialGroundingStats,
)
from core.models.extraction_schemas.stored_fold import build_stored_fold
from core.models.extraction_results.concept_extraction_results import (
    BatchedInitialGroundingNodeMetadata,
    ConceptsFound,
)
from core.models.extraction_results.extraction_node_metadata import (
    BatchedScreeningNodeMetadata,
    ExtractionNodeMetadata,
    RecursiveSearchNodeMetadata,
)
from core.models.extraction_results.keyword_extraction_results import (
    BatchedFreehandGroundingNodeMetadata,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    ConceptExtractionMetadata,
    ConceptExtractionResults,
    ConceptExtractionStats,
    KeywordExtractionMetadata,
    KeywordExtractionResults,
    KeywordExtractionStats,
)
from core.models.chunking_strat import CONFORMITY_ATTESTATION_CHUNKING_STRAT
from core.utils.aggregation_fold import WindowInput, fold_document
from core.utils.floor_scan import DroppedPage, PageExclusion
from llm_providers.models.llm_model import GPT_4o_mini, LLM_Model
from llm_providers.models.open_ai.gpt_model_params import GPTModelParams

TEXT = (
    f"{'#' * 50}\nhttps://acme.example/materials\n\n"
    "We stock Aluminum and Brass.\n"
)


def _stored_fold():
    result = fold_document(
        [
            WindowInput(
                text=TEXT,
                sent_forms=["Aluminum", "Brass"],
                window_id=f"0:{len(TEXT)}",
            )
        ]
    )
    return build_stored_fold(result, text_version_id="s3v1")


@pytest.fixture(autouse=True, scope="module")
def offline_extraction_run_settings():
    """Instantiating a Beanie Document needs only its ``_document_settings``
    (the synchronous half of ``init_beanie``) — the same offline pattern the
    other document tests use."""
    from beanie.odm.settings.document import DocumentSettings

    settings_class = getattr(ExtractionRun, "Settings")  # noqa: B009 — pyright-safe access
    settings_vars = {
        a: getattr(settings_class, a)
        for a in dir(settings_class)
        if not a.startswith("__")
    }
    ExtractionRun._document_settings = DocumentSettings(**settings_vars)


class _ScrapedFile:
    s3_version_id = "s3v1"
    num_tokens = 42
    last_modified_on = datetime(2026, 5, 16, tzinfo=timezone.utc)


_WHEN = datetime(2026, 8, 27, tzinfo=timezone.utc)

class _StageKwargs(TypedDict):
    """The per-stage metadata every node shares. A TypedDict rather than a bare
    ``dict`` so ``**_STAGE`` splats with known types — a plain dict literal
    infers one union for every value and makes each splat six type errors."""

    llm_model: LLM_Model
    model_params: GPTModelParams
    prompt_name: str
    prompt_version_id: str
    catalog_version: Optional[str]
    created_at: datetime


_STAGE: _StageKwargs = {
    "llm_model": GPT_4o_mini,
    "model_params": GPTModelParams.with_defaults(),
    "prompt_name": "material_cap_phrase_search",
    "prompt_version_id": "pv1",
    "catalog_version": None,
    "created_at": _WHEN,
}


def _provenance():
    return build_run_provenance_record(
        run_timestamp=_WHEN,
        scraped_text_file=_ScrapedFile(),
        page_exclusion=None,
    )


def _concept_results() -> ConceptExtractionResults:
    return ConceptExtractionResults(
        metadata=ConceptExtractionMetadata(
            created_at=_WHEN,
            chunk_strat=CONFORMITY_ATTESTATION_CHUNKING_STRAT,
            ontology_version_id="ontology-v1",
            llm_phrase_search=ExtractionNodeMetadata(**_STAGE),
            llm_phrase_recursive_search=RecursiveSearchNodeMetadata(
                **_STAGE, max_rounds=0
            ),
            llm_phrase_relationship_screening=BatchedScreeningNodeMetadata(
                **_STAGE, max_pairs_per_request=25
            ),
            llm_phrase_initial_grounding=BatchedInitialGroundingNodeMetadata(
                **_STAGE, max_pairs_per_request=25
            ),
            llm_phrase_recursive_grounding=ExtractionNodeMetadata(**_STAGE),
        ),
        run_provenance=_provenance(),
        results=ConceptsFound(in_vocab={"Metal"}, out_of_vocab=set()),
        chunked_extraction_stats={
            "0:1000": ConceptExtractionStats(
                results=ConceptsFound(in_vocab={"Metal"}, out_of_vocab=set()),
                brute_search=set(),
                aggregation_fold=_stored_fold(),
                llm_phrase_search={0: set(), 1: {"aluminum"}},
                llm_phrase_screening={1: {}},
                llm_phrase_initial_grounding=InitialGroundingStats(
                    in_vocab={1: {}}, out_of_vocab={1: {}}
                ),
                llm_phrase_recursive_grounding={},
            )
        },
    )


def _keyword_results() -> KeywordExtractionResults:
    return KeywordExtractionResults(
        metadata=KeywordExtractionMetadata(
            created_at=_WHEN,
            chunk_strat=CONFORMITY_ATTESTATION_CHUNKING_STRAT,
            ontology_version_id="ontology-v1",
            llm_phrase_search=ExtractionNodeMetadata(**_STAGE),
            llm_phrase_recursive_search=RecursiveSearchNodeMetadata(
                **_STAGE, max_rounds=0
            ),
            llm_phrase_relationship_screening=BatchedScreeningNodeMetadata(
                **_STAGE, max_pairs_per_request=25
            ),
            llm_phrase_freehand_grounding=BatchedFreehandGroundingNodeMetadata(
                **_STAGE, max_pairs_per_request=25
            ),
        ),
        run_provenance=_provenance(),
        results=ConceptsFound(in_vocab=set(), out_of_vocab={"cnc machining centers"}),
        chunked_extraction_stats={
            "0:1000": KeywordExtractionStats(
                results=ConceptsFound(
                    in_vocab=set(), out_of_vocab={"cnc machining centers"}
                ),
                aggregation_fold=_stored_fold(),
                llm_phrase_search={0: set(), 1: {"cnc machines"}},
                llm_phrase_screening={1: {}},
                llm_phrase_freehand_grounding={1: {}},
            )
        },
    )


def _run(family, results) -> ExtractionRun:
    return ExtractionRun(
        subject_unique_id="acme.example",
        field_name="material_caps" if family == "concept" else "equipments",
        field_family=family,
        run_timestamp=_WHEN,
        run_provenance=_provenance(),
        results=results,
    )


def test_the_run_document_carries_the_whole_result():
    """The record, not a summary of it: the final concepts, the per-chunk
    stats, and the fold inside them."""
    reloaded = ExtractionRun.model_validate_json(_run("concept", _concept_results()).model_dump_json())
    assert isinstance(reloaded.results, ConceptExtractionResults)
    assert reloaded.results.results.in_vocab == {"Metal"}
    fold = reloaded.results.chunked_extraction_stats["0:1000"].aggregation_fold
    assert fold.resolve(TEXT) == _stored_fold().resolve(TEXT)


def test_the_fold_is_stored_once_not_beside_the_results():
    """`folds` was dropped when results moved in: it would have been the same
    object twice in one document."""
    assert "folds" not in ExtractionRun.model_fields
    dumped = _run("concept", _concept_results()).model_dump()
    assert set(dumped) == {
        "id",
        "subject_unique_id",
        "field_name",
        "field_family",
        "run_timestamp",
        "run_provenance",
        "results",
    }


def test_a_keyword_run_round_trips_as_a_keyword_result():
    reloaded = ExtractionRun.model_validate_json(_run("keyword", _keyword_results()).model_dump_json())
    assert isinstance(reloaded.results, KeywordExtractionResults)
    assert reloaded.results.results.out_of_vocab == {"cnc machining centers"}


def test_the_family_tag_is_held_to_the_results_it_carries():
    """The two results models have no discriminator of their own; the tag is
    what stops a stored union resolving by whatever happens to be required."""
    with pytest.raises(ValueError, match="expects ConceptExtractionResults"):
        _run("concept", _keyword_results())
    with pytest.raises(ValueError, match="expects KeywordExtractionResults"):
        _run("keyword", _concept_results())


def test_a_stored_run_names_the_run_that_produced_the_cached_copy():
    """The invariant that makes 'the subject is a cache' checkable: the copy on
    the subject carries the same run timestamp as its record."""
    run = _run("concept", _concept_results())
    assert run.results.run_provenance.run_timestamp == run.run_timestamp


def test_the_run_document_is_keyed_by_subject_field_and_run():
    """The upsert key, and the unique index seeded for it."""
    fields = ExtractionRun.model_fields
    for key in ("subject_unique_id", "field_name", "run_timestamp"):
        assert fields[key].is_required() is True


def test_provenance_records_what_the_run_read_rather_than_what_s3_holds():
    exclusion = PageExclusion(
        version="1",
        text="kept",
        dropped=(DroppedPage(url="https://acme.example/privacy", start=10, end=30),),
        chars_before=44,
    )
    provenance = build_run_provenance_record(
        run_timestamp=datetime(2026, 8, 27, tzinfo=timezone.utc),
        scraped_text_file=_ScrapedFile(),
        page_exclusion=exclusion,
    )
    assert provenance.scraped_text_version_id == "s3v1"
    assert provenance.scraped_text_num_tokens == 42
    assert provenance.page_exclusion is not None
    assert provenance.page_exclusion.chars_before == 44
    assert provenance.page_exclusion.chars_removed == 20
    assert [p.url for p in provenance.page_exclusion.pages] == [
        "https://acme.example/privacy"
    ]


def test_a_single_stage_run_carries_no_exclusion():
    """The single-stage pipelines read the full text; None means no trimming
    ran, which must stay distinguishable from 'trimmed nothing'."""
    provenance = build_run_provenance_record(
        run_timestamp=datetime(2026, 8, 27, tzinfo=timezone.utc),
        scraped_text_file=_ScrapedFile(),
        page_exclusion=None,
    )
    assert provenance.page_exclusion is None


def test_provenance_round_trips_through_json():
    provenance = RunProvenance(
        run_timestamp=datetime(2026, 8, 27, tzinfo=timezone.utc),
        scraped_text_version_id="s3v1",
        scraped_text_num_tokens=42,
        scraped_text_last_modified_on=datetime(2026, 5, 16, tzinfo=timezone.utc),
        page_exclusion=StoredPageExclusion(
            version="1", chars_before=10, chars_removed=0, chars_after=10
        ),
    )
    reloaded = RunProvenance.model_validate_json(provenance.model_dump_json())
    assert reloaded == provenance


# --- the document the writer sends to Mongo (2026-09-14) ---------------------
#
# Found on the first full-tail run after the writer landed: ``model_dump`` is
# not a BSON encoder. It keeps ``set[str]`` as sets (BSON refuses them, so every
# keyword field's history failed the write) and it cannot dump the descent
# result at all (a set of models becomes a set of dicts — unhashable), which
# sank every concept field's run. These pin the encoder the writer uses now.


def _concept_results_with_a_descent_node() -> ConceptExtractionResults:
    from core.models.extraction_schemas.iterative_tagging import (
        IterativelyTaggedPhraseGroup,
    )

    results = _concept_results()
    results.chunked_extraction_stats["0:1000"].llm_phrase_recursive_grounding = {
        4: {
            IterativelyTaggedPhraseGroup(
                parent_group_id="Painting",
                group_id="Wet Painting",
                direct_phrases_to_og_tag_w_rules={},
                iterative_phrases_to_og_tag_w_rules={},
            )
        }
    }
    return results


def test_a_concept_run_with_a_descent_node_encodes_for_bson():
    import bson

    from core.services.extraction_run_service import encode_extraction_run

    run = _run("concept", _concept_results_with_a_descent_node())
    # The shape that sank run 20260915T020646: pydantic cannot dump a set of
    # models in python mode.
    with pytest.raises(TypeError):
        run.model_dump(mode="python", exclude={"id", "revision_id"})

    document = encode_extraction_run(run)
    bson.BSON.encode(document)  # what update_one needs; raises on a set
    assert "_id" not in document and "id" not in document and "revision_id" not in document
    assert document["run_timestamp"] == _WHEN  # the upsert key stays a datetime
    assert document["results"]["results"]["in_vocab"] == ["Metal"]
    descent = document["results"]["chunked_extraction_stats"]["0:1000"]["llm_phrase_recursive_grounding"]
    assert descent == {
        "4": [
            {
                "parent_group_id": "Painting",
                "group_id": "Wet Painting",
                "stop_reason": None,
                "declined_records": {},
                "direct_phrases_to_og_tag_w_rules": {},
                "iterative_phrases_to_og_tag_w_rules": {},
            }
        ]
    }


def test_a_keyword_run_encodes_its_empty_sets_for_bson():
    import bson

    from core.services.extraction_run_service import encode_extraction_run

    document = encode_extraction_run(_run("keyword", _keyword_results()))
    bson.BSON.encode(document)
    assert document["results"]["results"]["in_vocab"] == []
    assert document["results"]["results"]["out_of_vocab"] == ["cnc machining centers"]
    assert document["field_family"] == "keyword"


def test_the_encoded_document_reads_back_as_the_same_run():
    """Not just writable: what Mongo would hand back validates to the run that
    was written — int keys stringified by BSON come back as ints, lists come
    back as sets, the descent node keeps its identity."""
    from core.services.extraction_run_service import encode_extraction_run

    for family, results in (
        ("concept", _concept_results_with_a_descent_node()),
        ("keyword", _keyword_results()),
    ):
        run = _run(family, results)
        reloaded = ExtractionRun.model_validate(encode_extraction_run(run))
        assert reloaded.results == run.results
        assert reloaded.run_provenance == run.run_provenance
        assert (reloaded.subject_unique_id, reloaded.field_name, reloaded.run_timestamp) == (
            run.subject_unique_id, run.field_name, run.run_timestamp
        )
