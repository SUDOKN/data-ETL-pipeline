"""Snapshot a run's mention-stage wire payloads from Mongo.

    python checks/pull.py --run <run_id>

WHY THIS EXISTS. The dump records what the fold produced; it does not record the
WINDOW TEXT the scan actually ran over. Without that text the instrument can
check a run against itself, but it cannot re-derive the answer independently —
and the mechanical half of this stage is deterministic, so an independent
re-derivation is available and is the strongest check the instrument can make.

WHICH TEXT IS AUTHORITATIVE. Three copies of a subject's text exist and they are
NOT interchangeable:

1. `test_stages/sample_scraped_texts/<domain>.txt` — the pinned snapshot golden
   labels are anchored to. Quote-anchored, so it survives the differences below.
2. The S3 object after `normalize_scraped_text`, minus dropped legal pages —
   what the pipeline actually chunked. Offsets in the dump resolve against THIS.
3. The wire text of each request — (2) plus a nonce line and, on a window that
   opens mid-page, a continued-page header. NOT offset-comparable.

Verified offsets must be checked against (2) or the wire windows, never against
(1): the local snapshot differed from the pipeline's text by 44 characters on
one measured subject, which is enough to make every offset check fail
spuriously.

Read-only. Nothing here writes to Mongo.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Optional

if __package__ in (None, ""):  # pragma: no cover - script invocation
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import loading  # type: ignore[import-not-found]
    import paths  # type: ignore[import-not-found]
else:
    from . import loading, paths

COLLECTION = "gpt_batch_requests"

# The two fenced blocks a mention-location request carries.
_MENTION_IDS_RE = re.compile(r"<<<MENTION_IDS\n(.*?)\nMENTION_IDS>>>", re.DOTALL)
_MENTIONS_RE = re.compile(r"<<<MENTIONS\n(.*?)\nMENTIONS>>>", re.DOTALL)


def mongo_uri() -> str:
    """Read `MONGO_DB_URI` out of the repo's .env.

    Parsed by line prefix rather than imported from `infra`: that package's
    client is async and drags the pipeline's dependency chain into what should
    be a read-only script.
    """
    env_path = paths.REPO_ROOT / ".env"
    if not env_path.is_file():
        raise FileNotFoundError(f"no .env at {env_path}")
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith("MONGO_DB_URI="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise KeyError("MONGO_DB_URI not found in .env")


def subjects_of_run(run_id: str, dumps_root: Optional[Path] = None) -> list[str]:
    root = (dumps_root or paths.DUMP_ROOT) / run_id
    subjects: set[str] = set()
    for path in root.glob("*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        subject = payload.get("subject_unique_id")
        if isinstance(subject, str):
            subjects.add(subject)
    return sorted(subjects)


def wanted_custom_ids(run_id: str, dumps_root: Optional[Path] = None) -> list[str]:
    """Exactly the mention-stage request ids this run's dumps name.

    Querying by id set rather than by a regex over every request keeps the pull
    to the run in hand: a regex would also drag in every earlier run's requests
    that happen to still be in the collection.
    """
    ids: set[str] = set()
    for run in loading.load_run(run_id, dumps_root):
        for request in run.requests:
            custom_id = request.get("custom_id")
            if isinstance(custom_id, str) and paths.parse_custom_id(custom_id):
                ids.add(custom_id)
    return sorted(ids)


def _wire_view(doc: dict[str, Any]) -> dict[str, Any]:
    request = doc.get("request") or {}
    body = request.get("body") or {}
    messages = body.get("messages") or []
    user_message = ""
    system_message = ""
    for message in messages:
        role = message.get("role")
        if role == "user":
            user_message = str(message.get("content", ""))
        elif role == "system":
            system_message = str(message.get("content", ""))

    response = doc.get("response") or {}
    result = response.get("chat_completion_result") or {}
    choices = result.get("choices") or []
    answer = ""
    finish_reason = None
    if choices:
        message = choices[0].get("message") or {}
        answer = str(message.get("content", ""))
        finish_reason = choices[0].get("finish_reason")

    ids_block = _MENTION_IDS_RE.search(user_message)
    items_block = _MENTIONS_RE.search(user_message)
    return {
        "custom_id": request.get("custom_id"),
        "system_message": system_message,
        "user_message": user_message,
        "mention_ids": ids_block.group(1).splitlines() if ids_block else [],
        "mentions_block": items_block.group(1) if items_block else None,
        "answer": answer,
        "finish_reason": finish_reason,
        "usage": result.get("usage"),
    }


def pull(run_id: str, dumps_root: Optional[Path] = None) -> Path:
    try:
        from pymongo import MongoClient  # noqa: PLC0415
    except ImportError as error:  # pragma: no cover
        raise RuntimeError("pymongo is not installed in this environment") from error

    ids = wanted_custom_ids(run_id, dumps_root)
    out_dir = paths.run_dir(run_id) / "raw"
    out_dir.mkdir(parents=True, exist_ok=True)
    # Belt and braces beside the repo-wide rule: pulled bodies hold the verbatim
    # scraped site text and must never reach GitHub.
    (out_dir / ".gitignore").write_text(
        "# holds scraped site text — never commit\n*\n!.gitignore\n", encoding="utf-8"
    )

    client = MongoClient(mongo_uri())
    try:
        collection = client.get_default_database()[COLLECTION]
        documents = [
            _wire_view(doc)
            for doc in collection.find({"request.custom_id": {"$in": ids}})
        ]
    finally:
        client.close()

    out_path = out_dir / "mention_requests.json"
    out_path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "subjects": subjects_of_run(run_id, dumps_root),
                "requested": len(ids),
                "found": len(documents),
                "requests": documents,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    print(
        f"pulled {len(documents)} of {len(ids)} mention requests for run {run_id} "
        f"-> {out_path}"
    )
    if len(documents) < len(ids):
        # Expected whenever a run replayed answers stored under an earlier run
        # and that run's requests were scope-deleted since. Say so rather than
        # letting a partial snapshot look complete.
        print(
            f"NOTE: {len(ids) - len(documents)} requests are not in Mongo — "
            "typically replayed answers whose originals were scope-deleted."
        )
    return out_path


def load_snapshot(run_id: str) -> Optional[dict[str, Any]]:
    path = paths.run_dir(run_id) / "raw" / "mention_requests.json"
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", dest="run_id", required=True)
    parser.add_argument("--dumps-root", type=Path, default=None)
    args = parser.parse_args(argv)
    pull(args.run_id, args.dumps_root)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
