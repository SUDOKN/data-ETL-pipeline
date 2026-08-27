# Stage evaluations

One permanent, evolving evaluation instrument per pipeline stage, each in its
own folder (`search/`, `grounding/`, `synthesis/`, `mention_collection/`, …).
These are NOT ordinary pytest suites: each stage folder is self-describing —
it holds a runbook/playbook the assistant follows on demand ("run the <stage>
eval"), deterministic metric code, an evolving expectation/eval set, and an
append-only results history. Any pytest files inside a stage folder guard that
harness's own code, not the pipeline.

Stage folders are owned independently (they may be built in parallel
sessions); read a stage's own README before changing anything in it.

## `_shared/` — read before matching model output against scraped text

`_shared/text_matching.py` holds the primitives every stage instrument needs
and that are dangerous to re-implement: this corpus sets three traps that make
a naive matcher return confident WRONG numbers with no error raised —
non-breaking spaces mid-phrase (843 in steelcraft alone), whitespace runs
inside phrases, and short-form substrings ("tight" credited for `TIG`, the
same defect that made the pipeline's brute search score `Lead` at 52 hits and
0 real ones). The first two manufacture false alarms (correct output judged
fabricated or missing); the third manufactures false credit.

This is not hypothetical: the search harness produced a RED verdict on correct
output before the cause was found. If your stage compares any model string to
site text, import these rather than writing your own:

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # -> test_stages/
from _shared.text_matching import normalize_spaces, occurs_in, forms_overlap
```

## What is tracked in git, and what is not (2026-08-27, user decision)

**Tracked — the instrument:** each stage's eval/expectation set, `config/`,
`checks/` code, `tests/`, and its docs. Anyone cloning gets a working
instrument and can re-run it.

**Not tracked — the run data:** `history/**`, `results/**`,
`evidence_snapshots/**` and any `raw/` directory. It is golden-corpus scale
and partly verbatim scraped site text, which does not belong on GitHub.

**Two deliberate exceptions**, both KB-scale prose or numbers with no site text:
each stage's cross-run trend file (`history/metrics.jsonl`, `ledger.json`,
`LEDGER.md`, `metrics_scoreboard.csv`) and the per-run written analysis
(`history/runs/*/*.md`). Without the first, a fresh clone cannot compare a new
run to any earlier one; without the second, the conclusions a run was made to
produce would live only on one machine.

**Consequence to know:** per-run judgments, scorecards and pulled payloads
exist only on the machine that produced them. They can be regenerated (the
mechanical pass is a minute; a judged census is many agent runs), but they are
not recoverable from a clone. If a census is worth keeping, archive it
deliberately somewhere outside the repo.
