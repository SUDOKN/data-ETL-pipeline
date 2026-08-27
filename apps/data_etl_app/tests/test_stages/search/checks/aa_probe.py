"""Search-stage A/A reproducibility probe — the never-measured noise floor.

Grounding's measured floor is 78–90% identical on byte-identical payloads at
temperature 0 with a fixed seed; search's own floor has NEVER been measured
(latent-flaw ledger E3). Without it, small judged deltas on search output are
unreadable.

Method: take stored production search request BODIES (byte-identical payloads,
original nonce included), send each body twice fresh through the same LiteLLM
proxy the pipeline uses, and diff the two phrase sets per window — plus each
against the stored production response (temporal drift, a bonus axis).
Nothing is written to Mongo; the replay cache is untouched.

Spend-guarded: --dry-run (default) prices the probe and sends nothing;
--send actually spends. Cost at gpt-4.1 ≈ $2 per full two-subject probe.

Usage:
  .venv/bin/python aa_probe.py --run 20260825T194457            # price it
  .venv/bin/python aa_probe.py --run 20260825T194457 --send     # run it
  optional: --fields products equipments   --limit 20
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from loading import SEARCH_ID_RE  # noqa: E402
from paths import REPO_ROOT, run_dir  # noqa: E402
from pull import mongo_uri, subjects_of_run  # noqa: E402

PRICE_IN_PER_M = 2.00  # gpt-4.1
PRICE_OUT_PER_M = 8.00


def load_env() -> None:
    for line in (REPO_ROOT / ".env").read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            import os

            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def fetch_bodies(run_id: str, fields: list[str] | None, limit: int | None):
    from pymongo import MongoClient

    subjects = subjects_of_run(run_id)
    coll = MongoClient(mongo_uri(), serverSelectionTimeoutMS=20000)
    coll = coll.get_default_database()["gpt_batch_requests"]
    docs = []
    for doc in coll.find(
        {
            "request.custom_id": {"$regex": ">llm_search>chunk>"},
            "subject_unique_id": {"$in": subjects},
        }
    ):
        request = doc.get("request", {}) or {}
        cid = request.get("custom_id") or ""
        match = SEARCH_ID_RE.match(cid)
        if not match:
            continue
        if fields and match.group("field") not in fields:
            continue
        body = request.get("body") or {}
        stored = None
        response = doc.get("response")
        if isinstance(response, dict):
            choices = (response.get("chat_completion_result") or {}).get("choices") or []
            if choices:
                stored = (choices[0].get("message") or {}).get("content")
        usage = {}
        if isinstance(response, dict):
            usage = (response.get("chat_completion_result") or {}).get("usage") or {}
        docs.append({"custom_id": cid, "body": body, "stored_content": stored,
                     "usage": usage,
                     "subject": match.group("subject"), "field": match.group("field")})
    docs.sort(key=lambda d: d["custom_id"])
    return docs[:limit] if limit else docs


def phrases_of(content: str | None) -> frozenset[str] | None:
    if not isinstance(content, str):
        return None
    try:
        payload = json.loads(content)
    except ValueError:
        return None
    plist = payload.get("phrases") if isinstance(payload, dict) else None
    if isinstance(plist, list) and all(isinstance(p, str) for p in plist):
        return frozenset(plist)
    return None


async def send_body(client, body: dict) -> str | None:
    kwargs = {
        key: body[key]
        for key in (
            "model", "messages", "max_completion_tokens", "temperature",
            "top_p", "presence_penalty", "frequency_penalty", "seed",
            "response_format",
        )
        if key in body and body[key] is not None
    }
    response = await client.chat.completions.create(**kwargs)
    return response.choices[0].message.content


async def probe(docs, out_path: Path) -> None:
    import os

    from openai import AsyncOpenAI

    client = AsyncOpenAI(
        base_url=os.environ["LITELLM_PROXY_URL"],
        api_key=os.environ["LITELLM_VIRTUAL_KEY"],
    )
    semaphore = asyncio.Semaphore(8)

    async def one(doc):
        async with semaphore:
            first = await send_body(client, doc["body"])
            second = await send_body(client, doc["body"])
        set_a, set_b = phrases_of(first), phrases_of(second)
        set_stored = phrases_of(doc["stored_content"])
        return {
            "custom_id": doc["custom_id"],
            "subject": doc["subject"],
            "field": doc["field"],
            "identical_fresh_pair": (set_a == set_b) if set_a is not None and set_b is not None else None,
            "identical_to_stored": (set_a == set_stored) if set_a is not None and set_stored is not None else None,
            "sizes": [len(s) if s is not None else None for s in (set_stored, set_a, set_b)],
            "fresh_only": sorted((set_a or frozenset()) ^ (set_b or frozenset()))[:20],
            "vs_stored_diff": sorted((set_a or frozenset()) ^ (set_stored or frozenset()))[:20],
            "responses": {"a": first, "b": second},
        }

    results = await asyncio.gather(*(one(d) for d in docs))
    rows = [{k: v for k, v in r.items() if k != "responses"} for r in results]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(
        {"generated_at": datetime.now(timezone.utc).isoformat(), "results": results},
        ensure_ascii=False, indent=2))

    paired = [r for r in rows if r["identical_fresh_pair"] is not None]
    stored_paired = [r for r in rows if r["identical_to_stored"] is not None]
    print(f"windows probed: {len(rows)}")
    if paired:
        same = sum(r["identical_fresh_pair"] for r in paired)
        print(f"A/A identical phrase sets (fresh pair, same day): {same}/{len(paired)}"
              f" = {same / len(paired):.1%}")
    if stored_paired:
        same = sum(r["identical_to_stored"] for r in stored_paired)
        print(f"fresh vs stored production response: {same}/{len(stored_paired)}"
              f" = {same / len(stored_paired):.1%} (temporal drift included)")
    by_field: dict[tuple[str, str], list] = {}
    for r in paired:
        by_field.setdefault((r["subject"], r["field"]), []).append(r["identical_fresh_pair"])
    for (subject, field_name), vals in sorted(by_field.items()):
        print(f"  {subject:26} {field_name:26} {sum(vals)}/{len(vals)}")
    print(f"full results (with raw responses): {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", required=True, help="run id whose stored search requests to probe")
    parser.add_argument("--fields", nargs="*", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--send", action="store_true", help="actually spend; default is a dry-run pricing")
    args = parser.parse_args()

    load_env()
    docs = fetch_bodies(args.run, args.fields, args.limit)
    if not docs:
        print("no stored search requests found", file=sys.stderr)
        raise SystemExit(1)
    tokens_in = sum((d["usage"].get("prompt_tokens") or 0) for d in docs)
    tokens_out = sum((d["usage"].get("completion_tokens") or 0) for d in docs)
    est = 2 * (tokens_in / 1e6 * PRICE_IN_PER_M + tokens_out / 1e6 * PRICE_OUT_PER_M)
    print(f"{len(docs)} stored search windows; two fresh sends each ≈ "
          f"{2 * tokens_in:,} in / {2 * tokens_out:,} out tokens ≈ ${est:.2f}")
    if not args.send:
        print("dry-run only — pass --send to spend.")
        return
    out_path = run_dir(args.run) / "raw" / "aa_probe_results.json"
    asyncio.run(probe(docs, out_path))


if __name__ == "__main__":
    main()
