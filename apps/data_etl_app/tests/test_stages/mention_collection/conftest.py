"""Make this stage's `checks/` importable WITHOUT claiming the name `checks`.

Every stage instrument keeps its code in a directory called `checks/`, which is
the convention and worth keeping. But `sys.modules` has exactly one slot per
top-level name: the moment one stage's folder is on `sys.path` and something
imports `checks`, every OTHER stage's `checks` resolves to it for the rest of
the process. Running the whole `test_stages` tree in one pytest session is
enough to trigger it, and the symptom is a sibling's tests failing to collect
with `cannot import name '<their module>' from 'checks'` — pointing at a path
inside a different stage.

That is exactly what this instrument did to the synthesis harness on
2026-08-27, before this file existed.

So the package is registered here under the unique name `mention_checks`,
bound to this folder's `checks/` directory. Nothing is ever written to
`sys.modules["checks"]` from this stage, so import order stops mattering and no
sibling can be shadowed. The tests import `from mention_checks import ...`;
the CLI (`python checks/run_eval.py`) is unaffected, because it puts the
`checks/` directory itself on the path and imports bare module names.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

EVAL_ROOT = Path(__file__).resolve().parent
TEST_STAGES_ROOT = EVAL_ROOT.parent

# `_shared` is a single package shared by every stage, so putting test_stages on
# the path is safe — there is only one of it, and no stage defines a rival.
if str(TEST_STAGES_ROOT) not in sys.path:
    sys.path.insert(0, str(TEST_STAGES_ROOT))

PACKAGE_NAME = "mention_checks"

if PACKAGE_NAME not in sys.modules:
    _checks_dir = EVAL_ROOT / "checks"
    _spec = importlib.util.spec_from_file_location(
        PACKAGE_NAME,
        _checks_dir / "__init__.py",
        submodule_search_locations=[str(_checks_dir)],
    )
    if _spec is None or _spec.loader is None:  # pragma: no cover
        raise ImportError(f"cannot load {PACKAGE_NAME} from {_checks_dir}")
    _module = importlib.util.module_from_spec(_spec)
    # Registered BEFORE exec so the package's own `from . import paths` resolves.
    sys.modules[PACKAGE_NAME] = _module
    _spec.loader.exec_module(_module)
