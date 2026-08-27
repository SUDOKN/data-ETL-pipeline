"""Snapshot the synthesis wire evidence for one run out of Mongo.

A full-run dump keeps only ``{focal_form, synthesis}`` per record — the
entries (snippet + location) each paragraph was written from live in the
stored batch requests, which a later scoped delete erases. This tool pins
them to disk per run, so judgments stay reproducible:

  evidence_snapshots/<run_id>/synthesis_requests.json  — raw docs (holds site
      text; gitignored)
  evidence_snapshots/<run_id>/evidence_index.json      — per record:
      focal_form, entries, request custom_id, ud digest, evidence sha256
      (gitignored; regenerable from the raw file)

Run:  .venv/bin/python checks/run_eval.py --run <run_id> --pull
"""

from __future__ import annotations

import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any, Optional

if __package__ in (None, ""):  # runnable as a script
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from checks import loading  # type: ignore[no-redef]
else:
    from . import loading

SNAPSHOT_ROOT = loading.EVAL_ROOT / "evidence_snapshots"

_RECORDS_RE = re.compile(r"<<<RECORDS\n(.*?)\nRECORDS>>>", re.DOTALL)


def _mongo_collection():
    from pymongo import MongoClient  # local import: only the snapshot needs it

    env_path = loading.REPO_ROOT / ".env"
    uri = None
    for line in env_path.read_text().splitlines():
        if line.strip().startswith("MONGO_DB_URI="):
            uri = line.split("=", 1)[1].strip().strip('"').strip("'")
            break
    if not uri:
        raise RuntimeError(f"MONGO_DB_URI not found in {env_path}")
    client = MongoClient(uri, serverSelectionTimeoutMS=20000)
    return client.get_default_database()["gpt_batch_requests"]


def _doc_view(doc: dict[str, Any]) -> dict[str, Any]:
    request = doc.get("request") or {}
    body = request.get("body") or {}
    messages = body.get("messages") or []
    user = next((m.get("content") for m in messages if m.get("role") == "user"), None)
    system = next(
        (m.get("content") for m in messages if m.get("role") == "system"), None
    )
    response = doc.get("response")
    content = None
    usage = None
    if isinstance(response, dict):
        result = response.get("chat_completion_result") or response.get("body") or response
        choices = result.get("choices") or []
        if choices:
            content = choices[0].get("message", {}).get("content")
        usage = result.get("usage")
    return {
        "custom_id": request.get("custom_id"),
        "subject": doc.get("subject_unique_id"),
        "created_at": str(doc.get("created_at")),
        "system_message": system,
        "user_message": user,
        "content": content,
        "usage": usage,
    }


def parse_wire_records(user_message: str) -> list[dict[str, Any]]:
    """The RECORDS array as sent: [{record_id, focal_form, entries:[...]}]."""
    match = _RECORDS_RE.search(user_message or "")
    if not match:
        return []
    return json.loads(match.group(1))


def evidence_digest(entries: list[dict[str, Any]]) -> str:
    payload = json.dumps(entries, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _address_of(custom_id: str) -> tuple[str, str, str]:
    """(subject_safe, field, chunk_bounds) off a synthesis custom id."""
    head = custom_id.split(">")
    subject = re.sub(r"[^A-Za-z0-9]", "_", head[0])
    field_name = head[1]
    chunk_bounds = head[head.index("chunk") + 1]
    return subject, field_name, chunk_bounds


def build_snapshot(run_id: str) -> dict[str, Any]:
    """Pull exactly the run's synthesis requests and index the evidence."""
    files = loading.dump_files(run_id)
    wanted: set[str] = set()
    for (subject, field_name), path in files.items():
        if field_name == loading.SHARED_DUPLICATE:
            continue  # its requests read ">products>" — same ids as products
        dump = loading.load_dump(path)
        for entry in loading.synthesis_requests(dump):
            if entry.get("note") == "synthetic_response":
                continue
            custom_id = entry.get("custom_id")
            if custom_id:
                wanted.add(custom_id)
    if not wanted:
        raise RuntimeError(f"run {run_id}: no synthesis requests found in its dumps")

    collection = _mongo_collection()
    docs = [
        _doc_view(doc)
        for doc in collection.find({"request.custom_id": {"$in": sorted(wanted)}})
    ]
    found = {d["custom_id"] for d in docs}
    missing = sorted(wanted - found)

    index: dict[str, Any] = {}
    for doc in docs:
        custom_id = doc["custom_id"]
        subject, field_name, chunk_bounds = _address_of(custom_id)
        for record in parse_wire_records(doc["user_message"] or ""):
            entries = record.get("entries") or []
            index_key = "|".join([subject, field_name, chunk_bounds, record["record_id"]])
            index[index_key] = {
                "focal_form": record.get("focal_form"),
                "entries": entries,
                "request_custom_id": custom_id,
                "ud": loading.ud_of_custom_id(custom_id),
                "pv": loading.pv_of_custom_id(custom_id),
                "evidence_sha256": evidence_digest(entries),
            }

    out_dir = SNAPSHOT_ROOT / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "synthesis_requests.json").write_text(
        json.dumps(docs, ensure_ascii=False), encoding="utf-8"
    )
    (out_dir / "evidence_index.json").write_text(
        json.dumps(index, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    return {
        "run_id": run_id,
        "requests_wanted": len(wanted),
        "requests_found": len(found),
        "requests_missing": missing,
        "records_indexed": len(index),
        "out_dir": str(out_dir),
    }


def load_evidence_index(run_id: str) -> Optional[dict[str, Any]]:
    path = SNAPSHOT_ROOT / run_id / "evidence_index.json"
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def evidence_for(
    index: dict[str, Any], record: "loading.SynthRecord"
) -> Optional[dict[str, Any]]:
    """Evidence for a record; contract_products reads products' entries."""
    field_name = (
        "products" if record.field == loading.SHARED_DUPLICATE else record.field
    )
    return index.get(
        "|".join([record.subject, field_name, record.chunk_bounds, record.group_id])
    )


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else loading.latest_run()
    if not target:
        raise SystemExit("no runs on disk and no run id given")
    summary = build_snapshot(target)
    print(json.dumps(summary, indent=2))
