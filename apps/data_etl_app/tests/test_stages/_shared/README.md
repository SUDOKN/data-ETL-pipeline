# Shared evaluation primitives

Utilities that every stage evaluation needs and that are dangerous to
re-implement per stage. Pure stdlib; import from any stage folder:

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # -> test_stages/
from _shared.text_matching import normalize_spaces, occurs_in, forms_overlap
```

## `text_matching.py` — read its docstring before matching ANY model output against scraped text

Four traps in this corpus make a naive matcher return confident wrong
answers with no error raised: non-breaking spaces mid-phrase (843 in
steelcraft alone), whitespace runs inside phrases, typographic hyphens, and
short-form substrings ("tight" credited for `TIG`). The first three manufacture
FALSE ALARMS — correct output judged fabricated or missing; the fourth
manufactures FALSE CREDIT. The search harness hit the first one for real: it
produced a RED verdict on correct output before the cause was found.

**Added 2026-08-27 — typographic hyphens (affects every stage, so re-read if
your instrument predates this).** `normalize_spaces` now also folds U+2010,
U+2011, U+2012 and U+00AD to an ASCII hyphen, one character for one character,
so the offset guarantee is unchanged. Found while seeding the expanded corpus:
med-tekinc prints its ONLY capability as `heat‑treating` with a NON-BREAKING
hyphen on two lines and an ordinary hyphen on three others, and agstech carries
SOFT hyphens mid-word (`X­ray`, `high­resolution`) that render as nothing at all.
Measured across all 20 corpus texts: 5 non-breaking, 3 soft. EN and EM dashes are
deliberately NOT folded — 1,046 of 1,094 en dashes are whitespace-adjacent
separators, so folding them would credit `steel-and` for `steel—and`, which is
false credit rather than a rescued match.

Owned by whoever needs to change it; if you extend it, add the case to
`tests/test_shared_text_matching.py` beside it so the next stage inherits the
protection rather than the bug.
