"""The aggregation fold as it is PERSISTED (2026-08-27, user decision).

Why the fold is stored at all. It is intermediary data that no stored result
can reproduce: which mention landed in which group is a function of the
grouping rules (``normalizer_version``, ``verb_fold``, ``collapse_compounds``)
and of the exact text, so recomputing it after a rules change silently answers
a DIFFERENT question while claiming to describe the old run. Everything
downstream keys off group ids, and until now a shipped run recorded per-group
mention COUNTS and not one mention — the stage that decides what every later
stage reads was the one stage nothing could show after the fact.

Why OFFSETS and not text. The stored shape keeps positions, not passages: the
snippet, the form, and the two content ids are exact slices and hashes of an
IMMUTABLE input, so storing them would be storing a derivation. Resolution is
a pure slice with no rule replay at all::

    S3 object @ text_version_id
      → delete the run provenance's dropped-page spans   (deterministic)
      → post-exclusion text
      → text[mention.start : mention.end]                (the form)
      → text[mention.snippet_start : mention.snippet_end]  (the snippet)

That the exclusion is replayed from STORED SPANS rather than by re-running the
exclusion rule is what makes this independent of the rule version — see
``core.models.extraction_schemas.run_provenance``.

Offsets are DOCUMENT-ABSOLUTE here, where ``aggregation_fold.FoldedMention``
carries window-local ones. A reader resolving a snippet should not have to
know that windows exist, let alone reconstruct the sub-window split; the
conversion adds each window's base offset once, at write time, and
``window_base_offset`` proves the base it used against the window's own text
length rather than trusting the label it parsed.

Three fields are kept as text despite being "derivable", each for a stated
reason:

* ``sent_form`` — recovering it means replaying the two-tier floor scan, not
  slicing. It is also the only link from a mention back to the search phrase
  that found it.
* ``page`` — the one derived field whose derivation needs the page-marker scan
  rules rather than a slice or a hash.
* ``location`` / ``location_source`` — the context line above the mention's
  snippet: since 2026-09-05 the fold's own, derived in code
  (``aggregation_fold`` docstring D; source ``"code"``, or ``"none"`` with
  ``DEFAULT_LOCATION`` where the page block has no heading or table header
  above the snippet). Kept as text because deriving it needs the window's
  line geometry, not a slice. Documents written between the location-stage
  merge (2026-09-03) and that date hold the synthesis model's per-snippet
  quote under source ``"llm"``; pre-merge documents hold the retired location
  stage's prose. All load unchanged.

Dropped as pure slices or hashes: ``form``, ``snippet``, ``record_id``,
``mention_id``, ``is_discovered_casing``. Dropped as bulk: the whole
``WindowCollection`` — its ``text`` is the window verbatim (storing it would
put a copy of the site text in the document for every field), its ``items``
re-store every snippet a second time, and its ``scan`` is every hit of every
form.
"""

from __future__ import annotations

from typing import Optional, Sequence

from pydantic import BaseModel, Field, model_validator

from core.utils.aggregation_fold import (
    DEFAULT_LOCATION,
    LOCATION_SOURCE_CODE,
    LOCATION_SOURCE_NONE,
    FoldResult,
    FoldedMention,
    MentionBundle,
    WindowFold,
)
from core.utils.record_id_util import mention_id_for_snippet, record_id_for_phrase


class OffsetsOutOfRange(ValueError):
    """A stored offset does not resolve against the text it was given.

    Always a bug rather than drift: the offsets were written from the very
    text they are being resolved against, so a mismatch means the wrong text
    version, an un-replayed page exclusion, or a conversion that added the
    wrong window base.
    """


class WindowBoundsMismatch(ValueError):
    """A window's id does not describe the window's own text.

    ``WindowInput.window_id`` is documented as a free label, so the conversion
    refuses to read a base offset out of it on faith: it parses the label and
    checks the span it names against ``len(window.collection.text)``. A
    mismatch means the label is not the sub-window bounds and the absolute
    offsets it would produce are fiction.
    """


def window_base_offset(window: WindowFold) -> int:
    """The document-absolute offset of *window*'s first character.

    Read out of ``window_id`` (``"start:end"`` — the sub-window bounds the
    mention stage slices with) and then PROVEN: the span the label names must
    be exactly as long as the window's text. See ``WindowBoundsMismatch``.
    """
    label = window.window_id
    if label is None:
        raise WindowBoundsMismatch(
            f"window {window.window_index} has no window_id, so it has no "
            f"document-absolute base offset"
        )
    try:
        start_text, end_text = label.split(":")
        start, end = int(start_text), int(end_text)
    except ValueError as parse_error:
        raise WindowBoundsMismatch(
            f"window {window.window_index}: window_id {label!r} is not "
            f"'start:end' bounds"
        ) from parse_error
    expected = len(window.collection.text)
    if end - start != expected:
        raise WindowBoundsMismatch(
            f"window {window.window_index}: window_id {label!r} names a span of "
            f"{end - start} characters but the window's text is {expected}"
        )
    return start


class StoredMention(BaseModel):
    """One occurrence of one form, as positions into the document.

    ``[start, end)`` is the occurrence and ``[snippet_start, snippet_end)`` the
    passage holding it, both DOCUMENT-ABSOLUTE into the post-exclusion text.
    ``window_index`` is kept because it is half the locked order — not because
    the offsets need it. ``location`` is the context line above the snippet
    (see the module docstring)."""

    window_index: int
    start: int
    end: int
    snippet_start: int
    snippet_end: int
    page: Optional[str]
    sent_form: str
    location: str
    location_source: str

    @model_validator(mode="after")
    def check_spans(self) -> "StoredMention":
        if self.start >= self.end:
            raise ValueError(
                f"mention span [{self.start}, {self.end}) is empty or inverted"
            )
        if self.snippet_start > self.start or self.snippet_end < self.end:
            raise ValueError(
                f"snippet span [{self.snippet_start}, {self.snippet_end}) does not "
                f"contain the occurrence [{self.start}, {self.end})"
            )
        return self

    def form_in(self, text: str) -> str:
        """The occurrence's own text — what ``FoldedMention.form`` held."""
        return self._slice(text, self.start, self.end, "form")

    def snippet_in(self, text: str) -> str:
        """The clipped passage — what ``FoldedMention.snippet`` held."""
        return self._slice(text, self.snippet_start, self.snippet_end, "snippet")

    def record_id_in(self, text: str) -> str:
        return record_id_for_phrase(self.form_in(text))

    def mention_id_in(self, text: str) -> str:
        return mention_id_for_snippet(self.snippet_in(text))

    def is_discovered_casing_in(self, text: str) -> bool:
        return self.form_in(text) != self.sent_form

    def resolve(self, text: str, *, window_base: int) -> FoldedMention:
        """The full in-memory mention this row was written from. ``location``
        comes back as the fold computed it (None where the row holds the
        default), so a fold and its stored twin resolve equal.

        *window_base* puts the offsets back where ``FoldedMention`` expects
        them — WINDOW-LOCAL — which is why this takes it rather than guessing:
        a stored row alone cannot know its window's base. Callers normally go
        through ``StoredFold.resolve``, which supplies it.
        """
        snippet = self.snippet_in(text)
        form = self.form_in(text)
        return FoldedMention(
            window_index=self.window_index,
            start=self.start - window_base,
            end=self.end - window_base,
            form=form,
            record_id=record_id_for_phrase(form),
            page=self.page,
            snippet=snippet,
            snippet_start=self.snippet_start - window_base,
            mention_id=mention_id_for_snippet(snippet),
            sent_form=self.sent_form,
            location=(
                None if self.location_source == LOCATION_SOURCE_NONE else self.location
            ),
        )

    def _slice(self, text: str, start: int, end: int, what: str) -> str:
        if start < 0 or end > len(text):
            raise OffsetsOutOfRange(
                f"{what} span [{start}, {end}) falls outside a text of "
                f"{len(text)} characters"
            )
        return text[start:end]


class StoredBundle(BaseModel):
    """One group: what it is, and every mention that landed in it.

    ``forms``, ``key`` and ``collapsed_into`` are the grouping DECISION — the
    part no later recomputation can be trusted to reproduce — so they are
    stored verbatim rather than re-derived from the mentions.
    """

    group_id: str
    key: str
    forms: list[str]
    collapsed_into: list[str] = Field(default_factory=list)
    mentions: list[StoredMention] = Field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return not self.mentions

    @property
    def is_collapsed(self) -> bool:
        return bool(self.collapsed_into)


class StoredWindowFold(BaseModel):
    """One window's report: what the scan reached.

    Counts are not stored where a list is: ``sent_forms`` and
    ``zero_hit_forms`` are what a reader needs to tell a search false positive
    from a scan miss, and neither is recoverable once the run's window text is
    gone. The four Location-stage coverage lists at the bottom are retired
    (2026-09-03, the location-stage merge) — kept so stored pre-merge
    documents load, written empty since.
    """

    window_index: int
    window_id: Optional[str]
    base_offset: int
    text_length: int
    sent_forms: list[str] = Field(default_factory=list)
    zero_hit_forms: list[str] = Field(default_factory=list)
    short_forms: list[str] = Field(default_factory=list)
    discovered_casings: dict[str, list[str]] = Field(default_factory=dict)
    excluded_pages: list[str] = Field(default_factory=list)
    described: list[str] = Field(default_factory=list)
    not_described: list[str] = Field(default_factory=list)
    retried: list[str] = Field(default_factory=list)
    unknown_answer_ids: list[str] = Field(default_factory=list)


class StoredFold(BaseModel):
    """One chunk's fold, persisted.

    The rule identity is repeated here rather than only in
    ``AggregationFoldMetadata`` so the block is self-describing when it is read
    apart from its metadata — which is the whole point of also writing it to
    ``ExtractionRun``. ``text_version_id`` is what the offsets resolve against
    and is not optional: offsets without it are numbers without a document.
    """

    text_version_id: str
    normalizer_version: str
    verb_fold: bool
    snippet_radius: int
    collapse_compounds: bool
    bundles: list[StoredBundle] = Field(default_factory=list)
    windows: list[StoredWindowFold] = Field(default_factory=list)

    @property
    def mention_count(self) -> int:
        return sum(len(bundle.mentions) for bundle in self.bundles)

    def bundle(self, group_id: str) -> Optional[StoredBundle]:
        for stored in self.bundles:
            if stored.group_id == group_id:
                return stored
        return None

    def base_offset_of(self, window_index: int) -> int:
        for window in self.windows:
            if window.window_index == window_index:
                return window.base_offset
        raise KeyError(f"no stored window {window_index}")

    def resolve(self, text: str) -> list[MentionBundle]:
        """Rebuild the in-memory bundles from *text*.

        The inverse of ``from_fold_result``, and the reason the offsets-only
        shape is safe to store: given the same post-exclusion text, this
        returns bundles equal to the ones the run folded, ids and all.
        """
        rebuilt: list[MentionBundle] = []
        for stored in self.bundles:
            mentions = [
                mention.resolve(
                    text, window_base=self.base_offset_of(mention.window_index)
                )
                for mention in stored.mentions
            ]
            rebuilt.append(
                MentionBundle(
                    group_id=stored.group_id,
                    key=stored.key,
                    forms=tuple(stored.forms),
                    mentions=tuple(mentions),
                    collapsed_into=tuple(stored.collapsed_into),
                )
            )
        return rebuilt


def _stored_mention(mention: FoldedMention, base: int) -> StoredMention:
    return StoredMention(
        window_index=mention.window_index,
        start=base + mention.start,
        end=base + mention.end,
        snippet_start=base + mention.snippet_start,
        snippet_end=base + mention.snippet_start + len(mention.snippet),
        page=mention.page,
        sent_form=mention.sent_form,
        location=DEFAULT_LOCATION if mention.location is None else mention.location,
        location_source=(
            LOCATION_SOURCE_NONE if mention.location is None else LOCATION_SOURCE_CODE
        ),
    )


def _stored_window(window: WindowFold, base: int) -> StoredWindowFold:
    collection = window.collection
    return StoredWindowFold(
        window_index=window.window_index,
        window_id=window.window_id,
        base_offset=base,
        text_length=len(collection.text),
        sent_forms=list(collection.sent_forms),
        zero_hit_forms=list(collection.zero_hit_forms),
        short_forms=list(collection.scan.short_forms),
        discovered_casings={
            form: list(casings)
            for form, casings in collection.discovered_casings.items()
        },
        excluded_pages=list(collection.excluded_pages),
    )


def build_stored_fold(result: FoldResult, *, text_version_id: str) -> StoredFold:
    """The persisted twin of *result*, with document-absolute offsets. Each
    mention's ``location`` is the fold's own (code-derived); a mention the
    fold could not locate stores ``DEFAULT_LOCATION``.

    Kept as a conversion rather than by making the fold's own dataclasses
    pydantic: the fold is the pipeline's one hot pure-code loop and its
    ``frozen=True, order=True`` mentions carry the locked order that every
    synthesis digest rests on. A twin also lets the stored shape drop what the
    in-memory one needs (the window text, the scan) without arguing about it.
    """
    bases = {w.window_index: window_base_offset(w) for w in result.windows}
    missing = {
        m.window_index
        for bundle in result.bundles
        for m in bundle.mentions
        if m.window_index not in bases
    }
    if missing:
        raise WindowBoundsMismatch(
            f"mentions reference window index(es) {sorted(missing)} that the fold "
            f"has no window for"
        )
    return StoredFold(
        text_version_id=text_version_id,
        normalizer_version=result.normalizer_version,
        verb_fold=result.verb_fold,
        snippet_radius=result.snippet_radius,
        collapse_compounds=result.collapse_compounds,
        bundles=[
            StoredBundle(
                group_id=bundle.group_id,
                key=bundle.key,
                forms=list(bundle.forms),
                collapsed_into=list(bundle.collapsed_into),
                mentions=[
                    _stored_mention(m, bases[m.window_index]) for m in bundle.mentions
                ],
            )
            for bundle in result.bundles
        ],
        windows=[_stored_window(w, bases[w.window_index]) for w in result.windows],
    )


def window_texts_of(stored: StoredFold, text: str) -> Sequence[str]:
    """Each window's slice of *text*, by the stored base and length.

    A cheap integrity check for a reader holding a candidate text: if these do
    not come back the expected lengths, the text is the wrong one.
    """
    return [
        text[window.base_offset : window.base_offset + window.text_length]
        for window in stored.windows
    ]
