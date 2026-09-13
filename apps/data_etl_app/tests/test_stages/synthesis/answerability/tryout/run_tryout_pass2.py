"""Pass-2 shared-block tryout (2026-09-12): the same three real requests through the same WEAK model under
the RUN A static (runA_system.txt, published 2026-09-11 22:30) and the pass-2 static (the five next-pass
sentences: list item takes its introducing sentence's dealing; no closing restatement of the dealing; no wire
vocabulary; frame named only as shown; first person is the manufacturer's). Outputs under pass2/.
Derived from run_tryout.py, which stays as run A ran it.

Run A shared-block tryout: the three real synthesis requests of the 2026-09-09 tryout
(docs_local/synthesis_shape_tryout_20260909/{1,2,3}/A_user.txt) through a WEAK model under
the OLD static (D_system.txt = the static published 2026-09-11 00:33) and the NEW static
(run A shared block). Deterministic: temperature 0, seed 12345, both arms identical."""
import asyncio, json, os, pathlib, sys

ROOT = next(p for p in pathlib.Path(__file__).resolve().parents if (p / ".git").exists())  # repo root
for line in (ROOT / ".env").read_text().splitlines():
    if "=" in line and not line.startswith("#"):
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

from llm_providers.models.llm_model import GPT_4_1_mini  # noqa: E402
from llm_providers.models.llm_model_params import LLMModelParams  # noqa: E402
from llm_providers.utils.ask_llm_util import ask_gpt  # noqa: E402

TRY = ROOT / "docs_local/synthesis_shape_tryout_20260909"
NEW = (ROOT / "apps/data_etl_app/src/data_etl_app/knowledge/prompts/final_texts/static/multi_stage/4_phrase_synthesis/product_phrase_synthesis.txt").read_text()
OUT_DIR = pathlib.Path(__file__).resolve().parent
OUT = OUT_DIR / "pass2"
OUT.mkdir(exist_ok=True)
OLD = (OUT_DIR / "runA_system.txt").read_text()  # run A static = the old arm of this pass
SUFFIX = sys.argv[1] if len(sys.argv) > 1 else ""
ARMS = ("new",) if SUFFIX else ("old", "new")
PARAMS = LLMModelParams(temperature=0.0, top_p=1.0, presence_penalty=0.0, frequency_penalty=0.0, seed=12345, max_completion_tokens=10000)


async def one(chunk: str, arm: str, system: str) -> None:
    user = (TRY / chunk / "A_user.txt").read_text()
    text = await ask_gpt(context=user, prompt=system, gpt_model=GPT_4_1_mini, model_params=PARAMS)
    (OUT / f"out_{chunk}_{arm}{SUFFIX}.json").write_text(text or "")
    print(chunk, arm, len(text or ""), file=sys.stderr)


async def main() -> None:
    await asyncio.gather(*[one(c, arm, s) for c in ("1", "2", "3") for arm, s in (("old", OLD), ("new", NEW)) if arm in ARMS])


asyncio.run(main())
