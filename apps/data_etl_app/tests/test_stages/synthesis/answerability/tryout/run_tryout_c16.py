"""Fourth pass-2 tryout (2026-09-12, option B): the C16 list rule ("an item in a list takes the dealing its
introducing sentence gives it") on top of the three validated edits, run on requests that CARRY the target
defect — three real run-A synthesis requests of tanfel process_caps chunk 0:81109 (17 major own-listing
hedges on gpt-4.1; pulled from Mongo to pass2/req_tanfel_g{13,10,15}_user.txt) — plus the sales-rep request
(synthesis_shape_tryout_20260909/1/A_user.txt) as the party control. Arms: old = run A's static
(runA_system.txt), three = run A + the three validated edits (pass2/three_system.txt), c16 = three + the list
rule (pass2/c16_system.txt). Deterministic: temperature 0, seed 12345. Usage:
run_tryout_c16.py [mini|4.1] [arm ...]  → pass2/c16_out_<req>_<arm>[_41].json"""
import asyncio, os, pathlib, sys

ROOT = next(p for p in pathlib.Path(__file__).resolve().parents if (p / ".git").exists())  # repo root
for line in (ROOT / ".env").read_text().splitlines():
    if "=" in line and not line.startswith("#"):
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

from llm_providers.models.llm_model import GPT_4_1, GPT_4_1_mini  # noqa: E402
from llm_providers.models.llm_model_params import LLMModelParams  # noqa: E402
from llm_providers.utils.ask_llm_util import ask_gpt  # noqa: E402

HERE = pathlib.Path(__file__).resolve().parent
P2 = HERE / "pass2"
WHICH = sys.argv[1] if len(sys.argv) > 1 else "mini"
MODEL = GPT_4_1 if WHICH == "4.1" else GPT_4_1_mini
TAG = "_41" if WHICH == "4.1" else ""
ARMS = tuple(sys.argv[2:]) or ("old", "three", "c16")
SYSTEMS = {"old": HERE / "runA_system.txt", "three": P2 / "three_system.txt", "c16": P2 / "c16_system.txt", "c16b": P2 / "c16b_system.txt"}
REQUESTS = {
    "g13": P2 / "req_tanfel_g13_user.txt",
    "g10": P2 / "req_tanfel_g10_user.txt",
    "g15": P2 / "req_tanfel_g15_user.txt",
    "rep": ROOT / "docs_local/synthesis_shape_tryout_20260909/1/A_user.txt",
}
PARAMS = LLMModelParams(temperature=0.0, top_p=1.0, presence_penalty=0.0, frequency_penalty=0.0, seed=12345, max_completion_tokens=10000)


async def one(req: str, arm: str) -> None:
    text = await ask_gpt(context=REQUESTS[req].read_text(), prompt=SYSTEMS[arm].read_text(), gpt_model=MODEL, model_params=PARAMS)
    (P2 / f"c16_out_{req}_{arm}{TAG}.json").write_text(text or "")
    print(req, arm, WHICH, len(text or ""), file=sys.stderr)


async def main() -> None:
    await asyncio.gather(*[one(r, a) for r in REQUESTS for a in ARMS])


asyncio.run(main())
