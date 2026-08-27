# Search A/A reproducibility probe — 2026-08-26 (first measurement ever)

Method: the 96 stored first-search request bodies of run 20260825T194457
(byte-identical payloads, original nonce, gpt-4.1, temperature 0, seed 12345)
were each sent twice fresh through the production LiteLLM proxy; the two
returned phrase sets were diffed per window, and each was also diffed against
the stored production response. Cost ≈ $1.94. Raw results (with full
responses): raw/aa_probe_results.json (gitignored — carries site text).

## Numbers

- **Whole-window set identity: 27/95 = 28.4%** (fresh pair, same minute);
  25/96 = 26.0% vs the stored response (temporal drift included — barely
  different, so drift over days adds almost nothing beyond same-day churn).
- **Per-form overlap: mean Jaccard 0.769 exact / 0.791 casefold; median 0.786.**
  Mean |size difference| between the two sends: **3.49 phrases per window**.
- Per field (mean Jaccard, fresh pair): conformity_attestations **0.937** ·
  material_caps 0.831 · industries 0.751 · process_caps 0.732 · products
  0.683 · equipments **0.678**.

## What it means for every future search measurement

Temperature 0 plus a fixed seed does NOT pin this stage — same as the
grounding lesson, but measured on search's own output for the first time.
Roughly **a fifth to a quarter of a window's forms churn between identical
sends**, worst exactly where quality is worst (products, equipments).

- A per-window comparison below ~25% effect size is unreadable.
- Field-level aggregates (recall over confirmed entries, boundary
  composition) average some churn out, but any A/B on search prompts must
  either use paired windows with effects well above the per-field floor
  above, or aggregate across many windows/subjects.
- Whole-window set identity is a STRICT bar — do not quote 28.4% as "the
  reproducibility of search" without also quoting Jaccard 0.77, which is the
  fairer per-form number.
- Comparability note: grounding's measured floor (77.7%/90.1%) is per-GROUP
  set identity; search's window-set identity (28.4%) is not directly
  comparable to it — windows hold ~14 forms, groups far fewer candidates.

Re-run the probe (checks/aa_probe.py) after any search prompt, model, or
chunking change; append the new floor here and in config/common.yaml.
