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

Three traps in this corpus make a naive matcher return confident wrong
answers with no error raised: non-breaking spaces mid-phrase (843 in
steelcraft alone), whitespace runs inside phrases, and short-form substrings
("tight" credited for `TIG`). The first two manufacture FALSE ALARMS —
correct output judged fabricated or missing; the third manufactures FALSE
CREDIT. The search harness hit the first one for real: it produced a RED
verdict on correct output before the cause was found.

Owned by whoever needs to change it; if you extend it, add the case to
`tests/test_shared_text_matching.py` beside it so the next stage inherits the
protection rather than the bug.
