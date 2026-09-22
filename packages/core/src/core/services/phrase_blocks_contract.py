"""The phrase blocks a request carries, and the holding of the response to them.

Every request that asks the model to answer for a supplied set of phrases
(relationship, screening, initial / recursive / freehand grounding) carries the
phrases as fenced JSON in its user message. The strict response schema cannot pin
that set -- it is per-request, the schema is per-catalog -- so nothing structural
stops a response from answering for four phrases out of sixty and validating
cleanly (observed 2026-08-12: one freehand response closed its array five entries
in and silently dropped 55 screened phrases from a chunk). That is what the parse
side here exists to catch: it reads the phrases back out of the request's own
context and diffs the response's keys against them. The request document IS the
channel, so the set validated against is the set sent, by construction, and no
parse signature has to thread it through.

TWO BLOCKS, NEVER ONE
---------------------
The four stages that carry relationship summaries render the phrase set TWICE:
``<<<PHRASES`` as a bare array, then ``<<<PHRASES_WITH_SUMMARIES`` as the
phrase -> summary map. Both come from the same dict, so the array is exactly
``list(summaries)`` and the repetition is mechanically redundant. It is
deliberate and must not be collapsed: shown only the map, the model loses track
of what the phrase itself is and starts confusing the phrase with its summary --
answering under summary text, or folding summary wording into the phrase.
Naming the phrases alone first fixes the referent before the summaries can blur
it. ``render_phrase_blocks`` emits both from one argument precisely so that the
two can never drift apart; it was two independent calls per stage until
2026-08-12, with nothing but convention keeping them in step.

Relationship is the exception: it runs before summaries exist, so it renders the
array alone via ``render_phrases_block``.

WHY FENCED
----------
It was a bare ``extracted phrases:`` marker line until 2026-08-12, and
relationship is the stage whose context embeds the raw scraped chunk -- BEFORE
the block. Since the reader takes the first match, a scraped page containing
those two lines would have won over the real one and the response would have been
validated against the website's text. A fence cannot be produced by accident, and
two fences raise rather than let either win.

The tokens are named for the MODEL, not for this module. They read as what they
hold. ``SENT_PHRASES`` was precise about the invariant enforced below and meant
nothing to the model reading it; internal precision lives in the Python names
here instead, where "sent" is exact and never reaches the wire.

Dummy requests carry the records block, empty. An absent block is therefore always a
malformed request rather than a request that asked nothing -- until 2026-08-12
dummies omitted the phrases block, so ``sent_phrases_from_user_message`` returned
None for them and validation was skipped rather than passing trivially.

RECONCILIATION
--------------
Repair-first, because models echo phrases imperfectly rather than maliciously:
casing and whitespace drift, a phrase comes back truncated, or the summary comes
back where the phrase should be. A response key that normalizes onto exactly one
sent phrase -- equal, contained in it, or containing it -- is rewritten back to
the sent phrase. What cannot be repaired is judged: an extra key answers a
question nobody asked (dropped, warned); a missing sent phrase is an unanswered
question, and whether that fails the response or merely thins it is the calling
stage's policy, not this module's.

The summaries block had a matching reader until 2026-08-11, so that quoted
evidence could be validated against the summary byte-for-byte -- possibly days
later in the batch path. With the evidence field gone nothing needs the summaries
at parse time and the reader went with it. If quote checking ever comes back, the
reader is what to restore; the fencing is still here and still unambiguous.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Iterable, Literal, Optional, TypeVar

from core.models.extraction_schemas.relationship import LLMPhraseRelationshipResults

logger = logging.getLogger(__name__)

V = TypeVar("V")

PHRASES_OPEN = "<<<PHRASES"
PHRASES_CLOSE = "PHRASES>>>"
SUMMARIES_OPEN = "<<<PHRASES_WITH_SUMMARIES"
SUMMARIES_CLOSE = "PHRASES_WITH_SUMMARIES>>>"
# Recursive search is the one stage whose phrase list is an EXCLUSION set rather
# than the question, so it gets its own fence instead of reusing the phrases one:
# a `<<<PHRASES` block means "answer about exactly these", and `_PHRASES_RE` is
# what enforces that downstream. Handing recursive search the same token would
# name the opposite instruction with the reader's word for it. Both tokens here
# begin `<<<ALREADY_`, so neither fence line can match `_PHRASES_RE`, which
# anchors at `^[ \t]*` -- the close token being a suffix of `PHRASES>>>` is
# harmless for that reason, and only for that reason.
ALREADY_EXTRACTED_OPEN = "<<<ALREADY_EXTRACTED_PHRASES"
ALREADY_EXTRACTED_CLOSE = "ALREADY_EXTRACTED_PHRASES>>>"

# The summaries block is per-request and never cached, so its whitespace is paid
# for on every call -- indent=2 would be about 6% of a flat map of strings. The
# newline separator keeps one phrase per line, which costs nothing over
# collapsing to one line and keeps a long map readable.
_SUMMARY_SEPARATORS = (",\n", ":")

# Leading horizontal whitespace is tolerated on both fence lines because the
# anchor is otherwise an invisible failure: screening built its context as
# `...\n\n ` + the marker, and that one space put the line off column 0, so the
# pattern found nothing and the stage read as "asked nothing" rather than
# "misrendered". `[ \t]` and not `\s`, which would eat the newline.
#
# `PHRASES_OPEN` is a prefix of `SUMMARIES_OPEN`, so this pattern would find the
# summaries fence too were it not for the `\n` immediately after the token: the
# summaries line continues `_WITH_SUMMARIES` where this needs a line break. The
# close tokens are disjoint for the same reason (`>` vs `_`). Keep that `\n`
# adjacent to the token -- padding it with `[ \t]*` is harmless today but stops
# being the thing that separates the two fences.
_PHRASES_RE = re.compile(
    rf"^[ \t]*{re.escape(PHRASES_OPEN)}\n(\[[^\n]*\])\n[ \t]*{re.escape(PHRASES_CLOSE)}$",
    re.MULTILINE,
)


def render_phrases_block(phrases: Iterable[str]) -> str:
    """The block naming what the model is being asked about, phrases alone."""
    # ensure_ascii=False so the model is shown the phrase, never a backslash-u
    # escape of it. Under the default the model is handed the escaped spelling
    # of "Paladin(TM) PW Series", answers in the escaping style it was shown,
    # and miscopies the hex digits -- returning escapes that decode to control
    # characters, or that carry the wrong digit count entirely. Observed
    # 2026-08-12 on steelcraft.com: all 17 phrases containing TM or (R) came
    # back mangled that way, which reconciliation downstream cannot reliably
    # undo. 17 of 17 -- deterministic, not a flake. The exact corruptions are
    # pinned in the tests, which assert on rendered text for this reason: a
    # round trip through json.loads cannot see it, because escapes decode.
    # Keep the array on one line: the reader takes exactly the fence's middle.
    payload = json.dumps(list(phrases), ensure_ascii=False)
    return f"{PHRASES_OPEN}\n{payload}\n{PHRASES_CLOSE}"


def render_already_extracted_block(phrases: Iterable[str]) -> str:
    """The exclusion set shown to recursive search, as a fenced JSON block.

    Sorted, because the caller holds a `set` and its iteration order is not
    stable across processes: unsorted, the same chunk renders a different
    prompt on every run, which defeats seeded/zero-temperature comparison of
    two prompt versions and silently perturbs any cache keyed on the text.
    `ensure_ascii=False` and the one-line array for the same reasons as
    ``render_phrases_block``.
    """
    payload = json.dumps(sorted(phrases), ensure_ascii=False)
    return f"{ALREADY_EXTRACTED_OPEN}\n{payload}\n{ALREADY_EXTRACTED_CLOSE}"


def render_summaries_block(summaries: LLMPhraseRelationshipResults) -> str:
    """The phrase -> summary map as a fenced JSON block."""
    # ensure_ascii=False for the same reason as the phrases block it travels
    # with: the keys here are the phrases the model echoes back. The summaries
    # are also quoted back in explanations, so this keeps that text as the
    # source wrote it.
    payload = json.dumps(summaries, separators=_SUMMARY_SEPARATORS, ensure_ascii=False)
    return f"{SUMMARIES_OPEN}\n{payload}\n{SUMMARIES_CLOSE}"


def render_phrase_blocks(summaries: LLMPhraseRelationshipResults) -> str:
    """Both blocks, phrases first, from the one map they both describe.

    One argument on purpose -- see the module docstring. The bare list is what
    stops the model conflating a phrase with its summary, and taking both
    renders from a single source is what stops the list and the map disagreeing
    about which phrases were sent.
    """
    return f"{render_phrases_block(summaries)}\n\n{render_summaries_block(summaries)}"


def sent_phrases_from_user_message(user_message: str) -> Optional[list[str]]:
    """The phrases this request asked about, or None when it carried no block.

    Every request we build carries one, dummies included, so None means a
    malformed or foreign request and is the signal to skip validation. A
    present-but-corrupt block raises: we wrote it, so it cannot honestly fail to
    parse.
    """
    payloads = _PHRASES_RE.findall(user_message)
    if not payloads:
        return None
    if len(payloads) > 1:
        # We write exactly one. More than one means something else in the
        # context produced a fence, and picking either would validate the
        # response against a set we did not send.
        raise ValueError(
            f"user message carries {len(payloads)} phrases blocks; "
            "exactly one is written per request"
        )
    return json.loads(payloads[0])


class MissingResponsePhrases(ValueError):
    """A response left sent phrases unanswered and the stage treats that as a
    failed response (recorded and re-dispatched by the retry loop)."""


def _normalize(phrase: str) -> str:
    return phrase.strip().casefold()


@dataclass
class PhraseReconciliation:
    """The facts of holding a response's keys to the sent phrases.

    ``result`` carries the response values under the SENT phrase strings, in
    response order; ``repaired`` maps each rewritten response key to the sent
    phrase it was matched with; ``extra`` keys had no unique home; ``missing``
    sent phrases got no answer.
    """

    result: dict = field(default_factory=dict)
    repaired: dict[str, str] = field(default_factory=dict)
    extra: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)


def reconcile_response_phrases(
    sent_phrases: Iterable[str],
    response_by_phrase: dict[str, V],
) -> PhraseReconciliation:
    """Match response keys to sent phrases; pure -- policy stays with callers.

    Two passes over the response in its own order: exact normalized match
    first, then unique containment (either direction) among what neither pass
    has claimed. A sent phrase, once claimed, is off the market, so two
    response keys can never repair onto the same phrase -- the second becomes
    extra.
    """
    unclaimed: dict[str, str] = {}
    sent_order: list[str] = []
    for sent in sent_phrases:
        sent_order.append(sent)
        unclaimed.setdefault(_normalize(sent), sent)

    reconciliation = PhraseReconciliation()
    claimed: dict[str, str] = {}  # response key -> sent phrase
    leftovers: list[str] = []

    for received in response_by_phrase:
        sent = unclaimed.pop(_normalize(received), None)
        if sent is None:
            leftovers.append(received)
        else:
            claimed[received] = sent

    for received in leftovers:
        received_norm = _normalize(received)
        if received_norm:
            candidates = [
                norm
                for norm in unclaimed
                if norm in received_norm or received_norm in norm
            ]
        else:
            candidates = []
        if len(candidates) == 1:
            claimed[received] = unclaimed.pop(candidates[0])
        else:
            reconciliation.extra.append(received)

    for received, value in response_by_phrase.items():
        sent = claimed.get(received)
        if sent is None:
            continue
        reconciliation.result[sent] = value
        if received != sent:
            reconciliation.repaired[received] = sent

    claimed_sent = set(claimed.values())
    reconciliation.missing = [s for s in sent_order if s not in claimed_sent]
    return reconciliation


def hold_response_to_sent_phrases(
    *,
    user_message: str,
    response_by_phrase: dict[str, V],
    where: str,
    on_missing: Literal["raise", "drop"],
    repairs: Optional[dict[str, str]] = None,
) -> dict[str, V]:
    """Validate a parsed phrase-keyed response against its own request.

    Returns the response re-keyed to the sent phrases. Repairs and dropped
    extras are warned; missing phrases raise ``MissingResponsePhrases`` for
    stages whose retry loop should get another attempt (initial and freehand
    grounding), or warn-and-thin for stages that cannot re-dispatch safely
    (descents, whose embed path wipes incomplete requests' error history).

    ``repairs`` is an optional sink: pass a dict and each repair is recorded as
    ``sent phrase -> what the model actually answered under``. A warning is a
    fine alert but a poor record, and the phrase trails join every stage on the
    phrase string -- so a row that silently shows the sent phrase is joinable but
    not auditable. Callers that dump trails pass a sink; the rest need not care.
    Keyed by the SENT phrase because that is the identity every other stage and
    the trail row already use.
    """
    sent_phrases = sent_phrases_from_user_message(user_message)
    if sent_phrases is None:
        # No block means a foreign or malformed request, and skipping the
        # hold is the documented choice. Skipping it SILENTLY is not: every
        # request we build carries a block, dummies included, so a fence the
        # renderer stopped emitting would switch this guard off across every
        # stage with nothing in the log to show for it. This line firing means
        # the contract is broken, not the response (added 2026-08-27).
        logger.warning(
            f"{where}: request carries no sent-phrases block; the phrase hold is "
            f"SKIPPED and {len(response_by_phrase)} response key(s) pass through "
            f"unvalidated"
        )
        return response_by_phrase

    reconciliation = reconcile_response_phrases(sent_phrases, response_by_phrase)

    for received, sent in reconciliation.repaired.items():
        logger.warning(
            f"{where}: repaired response phrase {received!r} -> sent phrase {sent!r}"
        )
        if repairs is not None:
            repairs[sent] = received
    if reconciliation.extra:
        logger.warning(
            f"{where}: dropping {len(reconciliation.extra)} response phrase(s) "
            f"that were never sent: {reconciliation.extra}"
        )
    if reconciliation.missing:
        message = (
            f"{where}: response answered {len(reconciliation.result)} of "
            f"{len(sent_phrases)} sent phrases; no grounding came back for "
            f"{reconciliation.missing}"
        )
        if on_missing == "raise":
            raise MissingResponsePhrases(message)
        logger.warning(message)

    return reconciliation.result


# ---------------------------------------------------------------------------
# v2 (pipeline v2, PIPELINE_V2_PLAN.md): record blocks
# ---------------------------------------------------------------------------
#
# Downstream of the relationship stage, v2 requests carry RECORDS instead of
# phrase→summary pairs: the id list as a bare array, then the id → record map.
# Two blocks for the same reason as the phrase pair above — naming the keys
# alone first fixes the referent — and rendered from one argument so the two
# can never drift; the reader raises when they do anyway, because a drifted
# pair means the request was not built by ``render_record_blocks``.
#
# Holding is EXACT — no reconciler, no normalization. Record ids are
# machine-minted ASCII, so a response key that is not exactly a sent id is
# corruption, not echo drift; matching it "helpfully" onto a near id would be
# the silent-neighbour failure the content-derived id exists to rule out.
#
# The records payload is MULTI-LINE (one record per top-level entry, via the
# same separators the summaries block uses) and read back by a lazy fenced
# match rather than a one-line anchor. That is safe against forgery for the
# same reason the payload cannot break the fence: every newline inside a
# record's own text is JSON-escaped, so the physical lines between the fences
# are exactly the ones json.dumps wrote, and none of them can begin with a
# fence token.

RECORDS_OPEN = "<<<RECORDS"
RECORDS_CLOSE = "RECORDS>>>"

# Lazy across lines: the payload spans one line per top-level record. The
# separate id-list block that once preceded this one was dropped on
# 2026-09-22 (user experiment): with records named by position in words,
# the records block alone names what was asked, and the readers below take
# the sent ids from its keys (or its entries' ``record_id``).
_RECORDS_RE = re.compile(
    rf"^[ \t]*{re.escape(RECORDS_OPEN)}\n(.*?)\n[ \t]*{re.escape(RECORDS_CLOSE)}$",
    re.MULTILINE | re.DOTALL,
)


def render_records_block(records: dict) -> str:
    """The id → record map as a fenced JSON block, one top-level entry per line.

    ``ensure_ascii=False`` for the record CONTENT — mention forms and accounts
    carry the site's own text, and showing the model escapes of it is the
    corruption chain the phrases block already closed.
    """
    payload = json.dumps(records, separators=_SUMMARY_SEPARATORS, ensure_ascii=False)
    return f"{RECORDS_OPEN}\n{payload}\n{RECORDS_CLOSE}"


def render_record_blocks(records: dict) -> str:
    """The records block — the one block a record-keyed request carries."""
    return render_records_block(records)


def _one_fenced_payload(
    pattern: re.Pattern[str], user_message: str, what: str
) -> Optional[str]:
    payloads = pattern.findall(user_message)
    if not payloads:
        return None
    if len(payloads) > 1:
        raise ValueError(
            f"user message carries {len(payloads)} {what} blocks; "
            "exactly one is written per request"
        )
    return payloads[0]


def sent_record_ids_from_user_message(user_message: str) -> Optional[list[str]]:
    """The record ids this request asked about, in the order the records block
    lists them, or None when it carried no block."""
    sent = sent_records_from_user_message(user_message)
    if sent is None:
        return None
    if isinstance(sent, dict):
        return list(sent)
    return [entry["record_id"] for entry in sent]


def sent_records_from_user_message(user_message: str) -> Optional[dict | list]:
    """The full records payload this request carried, or None without one: the
    id → record MAP v2 stages render, or the ARRAY of ``{record_id, ...}``
    entries the v3 synthesis stage renders (``render_synthesis_record_blocks``).

    This is what candidate-axis holds read: what a screening request asked
    about each record (its candidates) is part of the request document itself.
    """
    payload = _one_fenced_payload(_RECORDS_RE, user_message, "records")
    return None if payload is None else json.loads(payload)


def _record_ids_of(sent_records: dict | list) -> set[str]:
    """The ids a records block names, whichever shape it has: the keys of a v2
    map, or each entry's ``record_id`` in a v3 array. A malformed array entry
    (no ``record_id``) raises, because we wrote it."""
    if isinstance(sent_records, dict):
        return set(sent_records)
    return {entry["record_id"] for entry in sent_records}


class MissingResponseRecords(ValueError):
    """A response left sent records unanswered (or answered under ids that were
    never sent) and the stage treats that as a failed response."""


def hold_response_to_sent_record_ids(
    *,
    user_message: str,
    response_by_record_id: dict[str, V],
    where: str,
    on_missing: Literal["raise", "drop"],
) -> dict[str, V]:
    """Validate a parsed record-keyed response against its own request, exactly.

    Returns the response in sent order. An unknown response id RAISES regardless
    of ``on_missing`` — for phrases an extra key is echo drift and gets dropped,
    but an id nobody sent is a fabricated answer and dropping it would hide the
    fabrication. Missing ids raise or thin per the caller's policy, mirroring
    the phrase hold.
    """
    sent_ids = sent_record_ids_from_user_message(user_message)
    if sent_ids is None:
        # No block means a foreign or malformed request, and skipping the
        # hold is the documented choice. Skipping it SILENTLY is not: every
        # request we build carries a block, dummies included, so a fence the
        # renderer stopped emitting would switch this guard off across every
        # stage with nothing in the log to show for it. This line firing means
        # the contract is broken, not the response (added 2026-08-27).
        logger.warning(
            f"{where}: request carries no records block; the record hold is "
            f"SKIPPED and {len(response_by_record_id)} response id(s) pass through "
            f"unvalidated"
        )
        return response_by_record_id

    unknown = [rid for rid in response_by_record_id if rid not in set(sent_ids)]
    if unknown:
        raise MissingResponseRecords(
            f"{where}: response carries record id(s) that were never sent: "
            f"{unknown}"
        )

    missing = [rid for rid in sent_ids if rid not in response_by_record_id]
    if missing:
        message = (
            f"{where}: response answered {len(response_by_record_id)} of "
            f"{len(sent_ids)} sent records; nothing came back for {missing}"
        )
        if on_missing == "raise":
            raise MissingResponseRecords(message)
        logger.warning(message)

    return {
        rid: response_by_record_id[rid]
        for rid in sent_ids
        if rid in response_by_record_id
    }


# ---------------------------------------------------------------------------
# v3 (pipeline v3, PIPELINE_V3_PLAN.md): synthesis record blocks
# ---------------------------------------------------------------------------
#
# Synthesis (D15; sole survivor of this section since the 2026-09-03
# location-stage merge retired the mention-location wire and its fenced
# MENTION_IDS / MENTIONS blocks) is answered per record and holds with the
# record-id hold above; its records block is an ARRAY of {record_id, snippets}
# rather than the v2 map — user decision — so `_record_ids_of` reads ids off
# either shape.

def render_records_array_block(records: list[dict]) -> str:
    """The records payload as a fenced JSON ARRAY, one top-level entry per line.

    Same separators and ``ensure_ascii=False`` as the map renderer, for the same
    reasons: one record per physical line keeps a long payload readable, and the
    model is shown the site's text as written, never an escape of it.
    """
    payload = json.dumps(records, separators=_SUMMARY_SEPARATORS, ensure_ascii=False)
    return f"{RECORDS_OPEN}\n{payload}\n{RECORDS_CLOSE}"


def render_synthesis_record_blocks(records: list[dict]) -> str:
    """The synthesis records block (the id list that once preceded it was
    dropped 2026-09-22; the array's ``record_id`` entries name what was asked).

    ``records`` is a list of ``{"record_id": ..., "snippets": [...]}`` dicts (the
    ``model_dump()`` of ``SynthesisRecordInput``). Raises on a duplicate id —
    two records under one id would fuse at the hold.
    """
    ids = [record["record_id"] for record in records]
    if len(set(ids)) != len(ids):
        dupes = sorted({rid for rid in ids if ids.count(rid) > 1})
        raise ValueError(f"duplicate record_id(s) in synthesis request: {dupes}")
    return render_records_array_block(records)
