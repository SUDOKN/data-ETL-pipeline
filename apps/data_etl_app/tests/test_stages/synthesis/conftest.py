"""Make the stage folder importable so `from checks import ...` resolves
under pytest's importlib mode and for `python checks/run_eval.py`."""

import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))
