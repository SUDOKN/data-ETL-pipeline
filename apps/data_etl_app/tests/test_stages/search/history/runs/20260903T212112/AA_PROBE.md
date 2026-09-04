# A/A reproducibility probe — run 20260903T212112 (union-pass corpus/prompts)

100-window sample, two fresh sends each, same day, production params
(gpt-4.1, temp 0, seed 12345). Cost $1.51 (full 1,344-window probe priced
$29.30 dry-run; sample sent instead). Raw: `raw/aa_probe_results.json`.

- **Identical phrase sets (fresh pair): 73/100 = 73.0%** — old floor 28.4%.
- Fresh vs stored production response: 80/100 = 80.0% (temporal drift included).
- **Per-form Jaccard, non-empty pairs: 0.897** (81 pairs) — old floor 0.769.
- 19/100 windows deterministically empty on both sends.

Reading rule this baseline inherits: per-window comparisons are readable down
to ~27% churn (was ~72%); any judged difference smaller than the double-judge
disagreement rate is still unreadable. Quote these floors beside every number
from this run.
