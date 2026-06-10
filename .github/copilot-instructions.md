# Copilot Instructions

## Environment

- This repo uses a Python virtual environment located at the repo root: `.venv/`.
- **Always activate it before running any Python code, tests, scripts, or `pip`/`pytest` commands:**

  ```bash
  source .venv/bin/activate
  ```

- The repo is a multi-package monorepo. Each app has its own `pyproject.toml`
  (e.g. `core/`, `data_etl_app/`, `scraper_app/`, `open_ai_key_app/`,
  `litellm_proxy_app/`). Packages import each other (e.g. `core` imports from
  `data_etl_app` and `open_ai_key_app`), so the shared `.venv` has them all
  installed.
- Run tests with `pytest` from inside the relevant app directory **after**
  activating `.venv`, e.g.:

  ```bash
  source .venv/bin/activate
  cd data_etl_app && python -m pytest -q
  ```
