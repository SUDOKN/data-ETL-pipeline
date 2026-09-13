"""Repeat the published static on the Mathews production request N times with the production call shape, to see
whether the tryout path ever produces the laundering the production runs produced (2026-09-12)."""
import asyncio, json, os, pathlib, re, sys
ROOT = next(p for p in pathlib.Path(__file__).resolve().parents if (p / ".git").exists())  # repo root
for line in (ROOT / ".env").read_text().splitlines():
    if "=" in line and not line.startswith("#"):
        k, v = line.split("=", 1); os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
from llm_providers.models.llm_model import GPT_4_1  # noqa: E402
from llm_providers.models.open_ai.gpt_model_params import GPTModelParams  # noqa: E402
from llm_providers.utils.ask_llm_util import ask_gpt  # noqa: E402
from core.models.extraction_schemas.synthesis import SYNTHESIS_RESPONSE_SCHEMA  # noqa: E402
P2 = pathlib.Path(__file__).resolve().parent / "pass2"
ARM = sys.argv[2] if len(sys.argv) > 2 else "c16"
SYSTEM = (P2 / f"{ARM}_system.txt").read_text(); USER = (P2 / "req_mathewsco_g2_user.txt").read_text()
PARAMS = GPTModelParams(temperature=0, top_p=1, presence_penalty=0, frequency_penalty=0, seed=12345, max_completion_tokens=10_000, response_format={"type": "json_object"}).with_response_format(SYNTHESIS_RESPONSE_SCHEMA)
N = int(sys.argv[1]) if len(sys.argv) > 1 else 3

async def one(i: int) -> str:
    t = await ask_gpt(context=USER, prompt=SYSTEM, gpt_model=GPT_4_1, model_params=PARAMS) or ""
    (P2 / f"repeat_mw2_{ARM}_{i}.json").write_text(t); return t

async def main() -> None:
    outs = await asyncio.gather(*[one(i) for i in range(N)])
    for i, t in enumerate(outs):
        recs = json.loads(re.sub(r"^```(?:json)?\s*|\s*```$", "", t.strip()))["syntheses"]
        hedged = sum(bool(re.search(r"capacity|not specif|represent|unstated|principal|another company|third[- ]party", r["synthesis"], re.I)) for r in recs)
        print(f"repeat {i}: n={len(recs)} paragraphs with any hedge/party marker={hedged} without={len(recs)-hedged}", file=sys.stderr)

asyncio.run(main())
