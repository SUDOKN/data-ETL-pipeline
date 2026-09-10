"""Per-stage node metadata: the run identity of each stage of the phrase
pipeline — its model, prompt pin, and whatever else changes its output.

This file was called ``llm_phrase_extraction_results`` until 2026-08-27, which
it had stopped being some time earlier: the results and stats classes moved out
at the phase-2.9 flip and only the node-metadata ones stayed. The name now says
what is here, and the accurate one went to the module that holds the results.

Node metadata is fixed BEFORE a stage runs, which is why the upstream-content
digest is not here: it rides request-id construction (the ``|ud=`` segment), and
that knows the upstream results.
"""

from __future__ import annotations

from pydantic import BaseModel
from datetime import datetime
from typing import Optional


from core.field_types import (
    OntologyVersionIDType,
)
from infra.field_types import (
    S3FileVersionIDType,
)
from llm_providers.models.llm_model import LLM_Model
from core.models.chunking_strat import ChunkingStrategy
from llm_providers.models.open_ai.gpt_model_params import (
    GPTModelParams,
)

class BaseExtractionMetadata(BaseModel):
    created_at: datetime
    chunk_strat: ChunkingStrategy
    ontology_version_id: OntologyVersionIDType


class ExtractionNodeMetadata(BaseModel):
    llm_model: LLM_Model
    model_params: GPTModelParams
    prompt_name: str
    prompt_version_id: S3FileVersionIDType
    # Which rule catalog produced this prompt. Stored alongside the S3 version so
    # an applied_rule record can be joined back to the rule text that asked for
    # it, even after the catalog has since been edited. None for prompts that
    # have no catalog (search, relationship, single-stage).
    catalog_version: Optional[str] = None
    created_at: datetime

    def to_custom_id_segment(self) -> str:
        """Model, non-default params, and the pinned prompt version — e.g.
        ``gpt-4.1|temperature=0.0|pv=<s3-version-id>``. The prompt version makes
        the prompt part of request identity: without it, a re-run after a prompt
        edit finds the old requests complete and replays responses the old
        prompt produced.

        Everything that decides what a request *contains* belongs in this segment,
        for that same reason. Batched stages append their group size (``|gs=50``)
        on top: it decides which phrases share a request, so the same group index
        at a different cap is a different question with a different answer."""
        return (
            f"{self.model_params.to_custom_id_segment(self.llm_model.name)}"
            f"|pv={self.prompt_version_id}"
        )


class RecursiveSearchNodeMetadata(ExtractionNodeMetadata):
    # Hard cap on the number of recursive search rounds (beyond the first search).
    # Recursion also stops early when a round yields no new phrases.
    max_rounds: int


class BatchedRelationshipNodeMetadata(ExtractionNodeMetadata):
    # Hard cap on the number of candidate phrases sent to the LLM in a single
    # relationship request. The full candidate list for a chunk is split into
    # ceil(num_phrases / max_phrases_per_request) groups, each described
    # independently — against the same full chunk text — and merged back into one
    # flat result. Unit is phrases, not pairs: relationship is what *produces* the
    # phrase→description pairs the screening stage then batches.
    max_phrases_per_request: int

    def to_custom_id_segment(self) -> str:
        return f"{super().to_custom_id_segment()}|gs={self.max_phrases_per_request}"


class BatchedSynthesisNodeMetadata(ExtractionNodeMetadata):
    # v3 synthesis (PIPELINE_V3_PLAN.md D15 as amended 2026-08-22, D16; Phase
    # 3.2; the location-stage merge, 2026-09-03: the request also carries the
    # chunk's text, and the answer carries per-entry context quotes). The unit
    # is ENTRIES (a record's distinct snippets): a chunk's records are packed
    # in bundle order into requests of at most `max_entries_per_request`
    # entries — SOFT cutoff: a record is never split, and a record larger than
    # the cap travels alone. Part of request identity. The old `|loc=` A/B
    # segment died with the location stage — every merged-stage id differs
    # from every pre-merge id anyway (the wire content changed).
    max_entries_per_request: int

    def to_custom_id_segment(self) -> str:
        return f"{super().to_custom_id_segment()}|gs={self.max_entries_per_request}"


class AggregationFoldMetadata(BaseModel):
    """Run identity of the pure-code aggregation fold (v3 substep 2.3,
    ``core.utils.aggregation_fold``). The fold issues no request, so it has no
    model or prompt; what it carries is exactly what changes its output: the
    normalizer version (D10 — the grouping rule set plus the pinned lemmatizer
    dictionary) and the per-field verb-fold dial (L2, process/material only).
    Recorded so a dump names the grouping that produced it, and so a resumed
    subject cannot silently regroup under a bumped normalizer
    (``PrefillNode.raise_if_metadata_is_stale``). Editing fold RULES without
    bumping the version leaves this unchanged on purpose: that is the cheap
    iteration loop the design promises."""

    normalizer_version: str
    verb_fold: bool
    # D21 (2026-08-27): whether a group that is nothing but a coordination of
    # sibling groups is skipped by synthesis. Request identity, not a lint —
    # it decides which records exist at all, and synthesis custom_ids digest
    # the records. Defaulted so documents persisted before the dial loads.
    collapse_compounds: bool = False
    # The collector's snippet clip dial (user knob, 2026-08-22; moved here from
    # the retired mention-collection metadata, 2026-09-03): 0 = the
    # sentence-within-line clip every run so far used; r > 0 = the occurrence's
    # sentence unit(s) plus r units of context each side, within its page
    # (``core.utils.aggregation_fold``, B). The snippet hash IS the mention id,
    # and snippets ride the synthesis wire, so the knob is request identity
    # through the synthesis `|ud=` digest. Defaulted so documents persisted
    # before the move load.
    snippet_radius: int = 0


class BatchedScreeningNodeMetadata(ExtractionNodeMetadata):
    # Hard cap on the number of phrase-relationship pairs sent to the LLM in a
    # single screening request. The full set of pairs for a chunk is split into
    # ceil(num_pairs / max_pairs_per_request) groups, each screened independently
    # and merged back into one flat result.
    max_pairs_per_request: int

    def to_custom_id_segment(self) -> str:
        return f"{super().to_custom_id_segment()}|gs={self.max_pairs_per_request}"
