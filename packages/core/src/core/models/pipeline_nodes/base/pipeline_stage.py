"""Stage identity, and the per-run toggles that decide which stages execute.

Every node carries a :class:`PipelineStage` as a class attribute, and every run
carries a :class:`StageToggles` on the pipeline context. A node whose stage is
disabled returns without doing any work AND without calling its successor: the
first disabled stage is where the run stops.

Hard stop rather than pass-through, because a middle stage cannot be made
transparent by doing nothing. Nodes read their upstream out of the pipeline
context — screening reads the relationship map, grounding reads screening's —
so a stage that skips itself and hands an empty map to its successor does not
"leave the pipeline alone", it silently produces a run whose later stages all
saw nothing. The one existing pass-through (``max_rounds=0`` on recursive
search) is only safe because ``get_relationship_candidates`` was written to
tolerate an empty recursive map; nothing else downstream has that property.

What a stopped run leaves behind is a partial dump — see
``core.services.pipeline_nodes.partial_run_dump``.

Toggles are keyed by (field, stage) so one field's stages can be isolated while
the other eleven pipelines run untouched:

    toggles = StageToggles()
    toggles.stop_after(PipelineStage.relationship, fields=[ConceptTypeEnum.industries])

Nothing is disabled by default, so a context built without toggles runs the
whole chain exactly as before.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Iterable, Optional

from core.models.field_types import ExtractionFieldType


class PipelineStage(StrEnum):
    """One phase of a field's extraction chain."""

    prefill = "prefill"
    phrase_search = "phrase_search"
    recursive_search = "recursive_search"
    relationship = "relationship"
    screening = "screening"
    initial_grounding = "initial_grounding"
    freehand_grounding = "freehand_grounding"
    iterative_grounding = "iterative_grounding"
    single_stage_extraction = "single_stage_extraction"
    reconcile = "reconcile"


# Rank used only by ``stop_after``; the chain itself is what actually orders
# execution. Concept grounding (initial -> iterative) and keyword grounding
# (freehand) are alternatives, never both in one chain, so they share the
# grounding tier: ``stop_after(screening)`` stops before either.
_STAGE_RANK: dict[PipelineStage, int] = {
    PipelineStage.prefill: 0,
    PipelineStage.single_stage_extraction: 1,
    PipelineStage.phrase_search: 1,
    PipelineStage.recursive_search: 2,
    PipelineStage.relationship: 3,
    PipelineStage.screening: 4,
    PipelineStage.initial_grounding: 5,
    PipelineStage.freehand_grounding: 5,
    PipelineStage.iterative_grounding: 6,
    PipelineStage.reconcile: 7,
}

# Prefill builds the chunked request map every later stage indexes into, so
# there is nothing coherent to run if it is skipped. Not toggleable.
_ALWAYS_ON: frozenset[PipelineStage] = frozenset({PipelineStage.prefill})

_ALL_FIELDS = "*"


class StageToggles:
    """Which (field, stage) pairs are allowed to run in this pipeline run.

    Stores what is DISABLED, so the empty instance means "run everything" and a
    field the caller never mentioned is never accidentally switched off.
    """

    def __init__(self) -> None:
        self._disabled: dict[str, set[PipelineStage]] = {}

    @staticmethod
    def _field_keys(
        fields: Optional[Iterable[ExtractionFieldType]],
    ) -> list[str]:
        if fields is None:
            return [_ALL_FIELDS]
        return [field.name for field in fields]

    def disable(
        self,
        *stages: PipelineStage,
        fields: Optional[Iterable[ExtractionFieldType]] = None,
    ) -> "StageToggles":
        """Switch off *stages*, for *fields* or for every field when omitted."""
        for stage in stages:
            if stage in _ALWAYS_ON:
                raise ValueError(
                    f"{stage} cannot be disabled: it builds the chunked request map "
                    f"that every later stage indexes into."
                )
        for key in self._field_keys(fields):
            self._disabled.setdefault(key, set()).update(stages)
        return self

    def stop_after(
        self,
        stage: PipelineStage,
        *,
        fields: Optional[Iterable[ExtractionFieldType]] = None,
    ) -> "StageToggles":
        """Disable every stage ranked after *stage*, so the run stops there.

        Reconcile is ranked last and so is included: stopping after any LLM
        stage means no final result is written to the subject, which is the
        point — a partial run must not leave a half-computed answer behind that
        the orchestrator would then read as "this field is done".
        """
        cutoff = _STAGE_RANK[stage]
        self.disable(
            *(
                later
                for later, rank in _STAGE_RANK.items()
                if rank > cutoff and later not in _ALWAYS_ON
            ),
            fields=fields,
        )
        return self

    def explicit_only(self, field_type: ExtractionFieldType) -> "StageToggles":
        """These toggles minus the blanket ones, for a PREREQUISITE pipeline.

        ``is_manufacturer`` and ``business_desc`` run ahead of the field
        pipelines and the orchestrator refuses to continue without their
        results, so an unqualified ``stop_after(relationship)`` — which disables
        reconcile for every field — would take those two down with it and fail
        the run before the stage under test was ever reached. A blanket toggle
        is aimed at the fields being tested; switching off a prerequisite has to
        be asked for by name.
        """
        scoped = StageToggles()
        named = self._disabled.get(field_type.name)
        if named:
            scoped._disabled[field_type.name] = set(named)
        return scoped

    def is_enabled(
        self, field_type: ExtractionFieldType, stage: PipelineStage
    ) -> bool:
        if stage in _ALWAYS_ON:
            return True
        if stage in self._disabled.get(_ALL_FIELDS, ()):
            return False
        return stage not in self._disabled.get(field_type.name, ())

    def disabled_stages(self, field_type: ExtractionFieldType) -> set[PipelineStage]:
        """Every stage switched off for *field_type*, global toggles included."""
        return set(self._disabled.get(_ALL_FIELDS, set())) | set(
            self._disabled.get(field_type.name, set())
        )

    def any_disabled(self) -> bool:
        return any(stages for stages in self._disabled.values())

    def __repr__(self) -> str:
        if not self.any_disabled():
            return "StageToggles(all stages enabled)"
        parts = [
            f"{key}: {sorted(stage.value for stage in stages)}"
            for key, stages in sorted(self._disabled.items())
            if stages
        ]
        return f"StageToggles(disabled={{{', '.join(parts)}}})"
