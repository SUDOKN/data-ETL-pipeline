"""The phrase→summary payload a request carries, written into the context.

Four stages show the model a set of phrases with their relationship summaries. This
renders that map into the user message as a fenced JSON block.

It was a codec until 2026-08-11: a matching ``extract_phrase_summaries_block``
read the map back at parse time, because validating the response's quoted evidence
against the summary meant having the summary again — possibly days later in the
batch path. Reading it out of the request's own context, rather than off a parallel
field, made the text validated against BE the text sent, by construction. With the
evidence field gone nothing needs the summaries at parse time, so the reader went
with it. If quote checking ever comes back, the reader is what to restore; the
block's fencing is still here and still unambiguous.

Every stage that carries summaries renders the block, INCLUDING when it is empty,
so its presence never has to be interpreted.
"""

from __future__ import annotations

import json

from core.models.extraction_schemas.relationship import LLMPhraseRelationshipResults

# Fenced rather than label-delimited so the block is findable without depending on
# the prose around it. Reword the surrounding context freely; just keep the block.
_BLOCK_OPEN = "<<<PHRASE_SUMMARIES"
_BLOCK_CLOSE = "PHRASE_SUMMARIES>>>"

# The block is per-request and never cached, so its whitespace is paid for on
# every call — indent=2 is about 6% of the block for a flat map of strings. The
# newline separator keeps one phrase per line, which costs nothing over
# collapsing to a single line and keeps a long map readable.
_SEPARATORS = (",\n", ":")


def render_phrase_summaries_block(
    summaries: LLMPhraseRelationshipResults,
) -> str:
    """Render the phrase→summary map as a fenced JSON block for the context."""
    payload = json.dumps(summaries, separators=_SEPARATORS)
    return f"{_BLOCK_OPEN}\n{payload}\n{_BLOCK_CLOSE}"
