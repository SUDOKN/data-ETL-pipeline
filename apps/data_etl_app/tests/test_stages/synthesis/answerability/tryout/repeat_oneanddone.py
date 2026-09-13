"""Replay ONE production synthesis request N times per arm, production call shape, and count how many of
its records each repeat answers — the one-and-done probe (2026-09-13). Run X1 (20260913T170246, the label
wire) had 27 of 1,598 first-pass requests answer a single record and close the array (run 191548: 5), and the
chunk retries hit the same mode, so 211 records went unsynthesized. Arms: `new` = the label static on disk
(md5 5f7ba520…) + the label schema; `old` = the published C16 static (pass2/c16_system.txt) + the old
{record_id, synthesis} schema. Usage: repeat_oneanddone.py <N> <custom_id substring> [arms...]"""
import asyncio, json, os, pathlib, re, sys
ROOT = next(p for p in pathlib.Path(__file__).resolve().parents if (p / ".git").exists())  # repo root
for line in (ROOT / ".env").read_text().splitlines():
    if "=" in line and not line.startswith("#"):
        k, v = line.split("=", 1); os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
from pydantic import BaseModel, ConfigDict  # noqa: E402
from llm_providers.models.llm_model import GPT_4_1  # noqa: E402
from llm_providers.models.open_ai.gpt_model_params import GPTModelParams  # noqa: E402
from llm_providers.utils.ask_llm_util import ask_gpt  # noqa: E402
from core.models.extraction_schemas.synthesis import SYNTHESIS_RESPONSE_SCHEMA  # noqa: E402
from core.models.extraction_schemas.response_format_util import build_gpt_response_format  # noqa: E402
sys.path.insert(0, str(ROOT / "apps/data_etl_app/tests/test_stages/synthesis"))
from checks import pull  # noqa: E402

HERE = pathlib.Path(__file__).resolve().parent
N = int(sys.argv[1]); NEEDLE = sys.argv[2]; ARMS = tuple(sys.argv[3:]) or ("new", "old")
STATIC_DIR = ROOT / "apps/data_etl_app/src/data_etl_app/knowledge/prompts/final_texts/static/multi_stage/4_phrase_synthesis"
NEW_SYSTEM = (STATIC_DIR / "process_cap_phrase_synthesis.txt").read_text()
OLD_SYSTEM = (HERE / "pass2" / "c16_system.txt").read_text()


class _OldRec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    record_id: str
    synthesis: str


class _OldResp(BaseModel):
    model_config = ConfigDict(extra="forbid")
    syntheses: list[_OldRec]


OLD_SCHEMA = build_gpt_response_format(_OldResp, name="phrase_synthesis")
BASE = GPTModelParams(temperature=0, top_p=1, presence_penalty=0, frequency_penalty=0, seed=12345, max_completion_tokens=10_000, response_format={"type": "json_object"})
ARM = {"new": (NEW_SYSTEM, BASE.with_response_format(SYNTHESIS_RESPONSE_SCHEMA)), "old": (OLD_SYSTEM, BASE.with_response_format(OLD_SCHEMA))}

doc = next(d for d in pull._mongo_collection().find({"request.custom_id": {"$regex": re.escape(NEEDLE)}}))
view = pull._doc_view(doc)
USER = view["user_message"]; sent = len(pull.parse_wire_records(USER))
print(f"request {view['custom_id']}\nsent records {sent}; stored answer records {len(pull._answer_texts(doc))}; stored system == new static: {view['system_message'] == NEW_SYSTEM}", file=sys.stderr)


async def one(arm: str, i: int) -> int:
    system, params = ARM[arm]
    text = await ask_gpt(context=USER, prompt=system, gpt_model=GPT_4_1, model_params=params) or ""
    (HERE / "pass2" / f"oneanddone_{arm}_{i}.json").write_text(text)
    try:
        return len(json.loads(text)["syntheses"])
    except Exception:
        return -1


async def main() -> None:
    for arm in ARMS:
        counts = await asyncio.gather(*[one(arm, i) for i in range(N)])
        print(f"arm {arm}: records answered per repeat {list(counts)} of {sent} sent", file=sys.stderr)


asyncio.run(main())
