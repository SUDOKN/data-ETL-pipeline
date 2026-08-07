# Copilot Instructions

## Environment

- This repo uses a Python virtual environment located at the repo root: `.venv/`.
- **Always activate it before running any Python code, tests, scripts, or `pip`/`pytest` commands:**

  ```bash
  source .venv/bin/activate
  ```

- The repo is a multi-package monorepo. Each package/app has its own `pyproject.toml`
  and is installed **editable** into the shared `.venv`:
  - `packages/` — `pure_utils`, `core`, `infra`, `llm_providers`, `scraper`
  - `apps/` — `data_etl_app`, `litellm_proxy_app`
- Dependency rule: `core` may import from any package; **no package may import from `core`**.
  `pure_utils` is a leaf — it must not depend on any other package in this repo.
- Test/lint tooling lives in each package's `[project.optional-dependencies].dev` extra,
  not in runtime `dependencies`. Install with `pip install -e "packages/core[dev]"`.

## Tests

- Pytest is configured once at the repo root in `pytest.ini` (`rootdir` = repo root).
  There are no per-package pytest sections — don't add any.
- Tests live in `<package>/tests/`, never under `src/`. Test directories have **no**
  `__init__.py`; the root config uses `--import-mode=importlib`.
- A test belongs to the package that owns the code under test. If you're testing
  `infra.*`, the test goes in `packages/infra/tests/`, not in whichever package you
  happened to be working in.
- Tests needing live external services (AWS, Google Maps, real Chrome, network) must be
  marked `@pytest.mark.integration`. The root config deselects them by default.

  ```bash
  source .venv/bin/activate
  python -m pytest                  # whole monorepo, integration deselected
  python -m pytest -m integration   # only the live-service tests
  python -m pytest packages/infra   # one package
  ```

- `--strict-markers` is on, so any new marker must be declared in `pytest.ini`.

## Imports

Always use the installed distribution name. Never use the `packages.<pkg>.src.<pkg>` or
`apps.<app>.src.<app>` spelling — both resolve, which creates two module objects for the
same file and silently breaks Beanie `Document` class identity.

```python
from core.field_types import SubjectUniqueIDType    # yes
from packages.core.src.core.field_types import ...  # no
```

## Environment variables

- One `.env` at the repo root (gitignored). `pure_utils.env_util.load_env` finds it by
  walking up for `.git`; `SUDOKN_ENV_FILE` overrides.
- **Entrypoints only** (app, bot, script, `conftest.py`) call `load_env(<BUNDLE>)` exactly
  once, before doing work. Bundles live in `data_etl_app/dependencies/env.py`.
- **Library code must never read env at import time.** Use `require_env(...)` /
  `optional_env(...)` inside the function that needs the value. Importing any module with an
  empty environment must not raise — don't regress this.
- Each package declares what it needs in its own `required_env.py`, grouped by concern
  (`infra.required_env.MONGO`, `S3_PROMPTS`, ...). Apps compose the groups they use.

## MongoDB / Beanie

- `infra.utils.db_clients.mongo_client.init_db(document_models, uri=None, ...)` is generic —
  document models are injected, never imported by `infra`.
- Each package exports `DOCUMENT_MODELS` from its `db_models/__init__.py`; the app sums them.
- App code calls `data_etl_app.dependencies.db.init_app_db()`, which returns the
  `AsyncMongoClient` — the caller owns closing it.
- `init_beanie` binds collection state onto the `Document` classes process-wide, so
  re-initializing against a different database raises. One database per process.
