"""Pull one run's search-stage request documents from Mongo.

Writes history/runs/<run_id>/raw/search_requests.json — one object per stored
request: custom_id, user_message (the exact wire text the window read),
response content, finish_reason, usage, created_at. The raw file is
GITIGNORED: it embeds the scraped site text.

Usage: .venv/bin/python pull.py --run 20260825T194457
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from paths import DUMP_ROOT, REPO_ROOT, run_dir  # noqa: E402


def mongo_uri() -> str:
    env = REPO_ROOT / ".env"
    for line in env.read_text().splitlines():
        if line.strip().startswith("MONGO_DB_URI="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise RuntimeError("MONGO_DB_URI not found in .env")


def subjects_of_run(run_id: str) -> list[str]:
    subjects = set()
    for dump_path in (DUMP_ROOT / run_id).glob("*.json"):
        payload = json.loads(dump_path.read_text())
        sid = payload.get("subject_unique_id")
        if sid:
            subjects.add(sid)
    return sorted(subjects)


def pull(run_id: str) -> Path:
    from pymongo import MongoClient  # local import: offline callers never need it

    subjects = subjects_of_run(run_id)
    if not subjects:
        raise RuntimeError(f"no dumps found for run {run_id}")
    coll = MongoClient(mongo_uri(), serverSelectionTimeoutMS=20000)
    coll = coll.get_default_database()["gpt_batch_requests"]
    query = {
        "request.custom_id": {"$regex": ">llm_search>chunk>|>llm_recursive_search>round>"},
        "subject_unique_id": {"$in": subjects},
    }
    out = []
    for doc in coll.find(query):
        request = doc.get("request", {}) or {}
        body = request.get("body", {}) or {}
        messages = body.get("messages") or []
        user_message = next(
            (m.get("content") for m in messages if m.get("role") == "user"), None
        )
        response = doc.get("response")
        content = None
        finish_reason = None
        usage = None
        if isinstance(response, dict):
            result = response.get("chat_completion_result") or {}
            choices = result.get("choices") or []
            if choices:
                content = (choices[0].get("message") or {}).get("content")
                finish_reason = choices[0].get("finish_reason")
            usage = result.get("usage")
        out.append(
            {
                "custom_id": request.get("custom_id"),
                "subject": doc.get("subject_unique_id"),
                "created_at": str(doc.get("created_at")),
                "batch_id": doc.get("batch_id"),
                "user_message": user_message,
                "content": content,
                "finish_reason": finish_reason,
                "usage": usage,
            }
        )
    raw_dir = run_dir(run_id) / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    gitignore = raw_dir / ".gitignore"
    if not gitignore.exists():
        gitignore.write_text("# holds scraped site text — never commit\n*\n!.gitignore\n")
    out_path = raw_dir / "search_requests.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False))
    print(f"pulled {len(out)} request docs for {len(subjects)} subjects -> {out_path}")
    return out_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", required=True)
    pull(parser.parse_args().run)
