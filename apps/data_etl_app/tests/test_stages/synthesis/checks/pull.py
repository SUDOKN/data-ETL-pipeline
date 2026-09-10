"""Snapshot the synthesis wire evidence for one run out of Mongo.

A dump keeps only ``{focal_form, synthesis}`` (plus counters) per record — the
snippets each paragraph was written from, and the chunk text the model read
them in, live in the stored batch requests, which a later scoped delete
erases. This tool pins them to disk per run, so judgments stay reproducible:

  evidence_snapshots/<run_id>/synthesis_requests.json — every request, with
      the chunk text replaced by a reference (records + answer; gitignored)
  evidence_snapshots/<run_id>/chunk_text/<subject>__<bounds>.md — each
      chunk's wire text exactly once (site text; gitignored)
  evidence_snapshots/<run_id>/system_prompts/<pv>.txt — the static, once per pv
  evidence_snapshots/<run_id>/evidence_index.json — per record: focal_form,
      snippets, the request whose answer was accepted (the RETRY request when
      the record was retried), ud, pv, evidence sha256, chunk-text reference
  evidence_snapshots/<run_id>/subject_names.json — the manufacturer name each
      subject's prompts carried (what the model was told)

Wire shape since 2026-09-05 (location-stage merge, flattened): the user
message is a nonce line, ``text scraped from a manufacturer's website:`` +
the CHUNK TEXT, then ``the name of the manufacturer in question: <name>``,
then ``<<<RECORD_IDS …>>>`` and ``<<<RECORDS [{record_id, focal_form,
snippets: [str]}] RECORDS>>>``. No per-snippet location travels any more;
location is code on the fold (loading.fold_locations).

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
_TEXT_OPEN = "text scraped from a manufacturer's website:\n"
_NAME_MARKER = "the name of the manufacturer in question:"
_IDS_OPEN = "<<<RECORD_IDS"
_RETRY_RE = re.compile(r">retry>(\d+)>")


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
        "parse_errors": doc.get("response_parse_errors"),
    }


def split_user_message(user_message: str) -> dict[str, Any]:
    """{chunk_text, subject_name, tail} off a synthesis user message.

    ``tail`` starts at the manufacturer-name line (the records follow it) and
    is what the raw snapshot keeps; chunk text is stored once per chunk.
    """
    text = user_message or ""
    ids_at = text.find(_IDS_OPEN)
    head = text if ids_at < 0 else text[:ids_at]
    name_at = head.rfind(_NAME_MARKER)
    if name_at < 0:
        return {"chunk_text": None, "subject_name": None, "tail": text}
    line_end = text.find("\n", name_at)
    name_end = line_end if line_end >= 0 else len(text)
    subject_name = text[name_at + len(_NAME_MARKER) : name_end].strip()
    text_at = head.find(_TEXT_OPEN)
    chunk_text = (
        head[text_at + len(_TEXT_OPEN) : name_at].rstrip("\n") if text_at >= 0 else None
    )
    return {"chunk_text": chunk_text, "subject_name": subject_name, "tail": text[name_at:]}


def parse_wire_records(user_message: str) -> list[dict[str, Any]]:
    """The RECORDS array as sent: [{record_id, focal_form, snippets:[str]}]."""
    match = _RECORDS_RE.search(user_message or "")
    if not match:
        return []
    return json.loads(match.group(1))


def evidence_digest(snippets: list[Any]) -> str:
    payload = json.dumps(snippets, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def retry_index(custom_id: str) -> int:
    """0 for a group request, N for its ``>retry>N>`` re-ask."""
    match = _RETRY_RE.search(custom_id or "")
    return int(match.group(1)) if match else 0


def _address_of(custom_id: str) -> tuple[str, str, str]:
    """(subject_safe, field, chunk_bounds) off a synthesis custom id, with the
    subject spelled exactly as the dump filename spells it."""
    head = custom_id.split(">")
    subject = loading.safe_subject(head[0])
    field_name = head[1]
    chunk_bounds = head[head.index("chunk") + 1]
    return subject, field_name, chunk_bounds


def _chunk_file_name(subject: str, chunk_bounds: str) -> str:
    return f"{subject}__{chunk_bounds.replace(':', '-')}.md"


def build_snapshot(run_id: str) -> dict[str, Any]:
    """Pull exactly the run's synthesis requests (group + retry) and index the
    evidence. A retried record is indexed against its RETRY request — the
    co-pack whose answer the pipeline accepted — with the group request kept
    as ``superseded_request_custom_ids``."""
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

    out_dir = SNAPSHOT_ROOT / run_id
    chunk_dir = out_dir / "chunk_text"
    prompt_dir = out_dir / "system_prompts"
    chunk_dir.mkdir(parents=True, exist_ok=True)
    prompt_dir.mkdir(parents=True, exist_ok=True)

    collection = _mongo_collection()
    docs: list[dict[str, Any]] = []
    chunk_hashes: dict[tuple[str, str], str] = {}
    chunk_mismatches: list[dict[str, Any]] = []
    subject_names: dict[str, str] = {}
    name_conflicts: list[dict[str, Any]] = []
    prompts_written: set[str] = set()
    for raw in collection.find({"request.custom_id": {"$in": sorted(wanted)}}):
        view = _doc_view(raw)
        custom_id = view["custom_id"]
        subject, field_name, chunk_bounds = _address_of(custom_id)
        parts = split_user_message(view["user_message"] or "")
        chunk_ref = None
        chunk_sha = None
        if parts["chunk_text"] is not None:
            chunk_sha = _sha256(parts["chunk_text"])
            key = (subject, chunk_bounds)
            known = chunk_hashes.get(key)
            if known is None:
                chunk_hashes[key] = chunk_sha
                (chunk_dir / _chunk_file_name(subject, chunk_bounds)).write_text(
                    parts["chunk_text"], encoding="utf-8"
                )
            elif known != chunk_sha:
                chunk_mismatches.append(
                    {"custom_id": custom_id, "expected": known, "got": chunk_sha}
                )
            chunk_ref = f"chunk_text/{_chunk_file_name(subject, chunk_bounds)}"
        if parts["subject_name"]:
            previous = subject_names.setdefault(subject, parts["subject_name"])
            if previous != parts["subject_name"]:
                name_conflicts.append(
                    {"subject": subject, "seen": [previous, parts["subject_name"]]}
                )
        pv = loading.pv_of_custom_id(custom_id)
        if pv and pv not in prompts_written and view["system_message"]:
            (prompt_dir / f"{pv}.txt").write_text(view["system_message"], encoding="utf-8")
            prompts_written.add(pv)
        docs.append(
            {
                "custom_id": custom_id,
                "subject": view["subject"],
                "created_at": view["created_at"],
                "retry_index": retry_index(custom_id),
                "system_prompt": f"system_prompts/{pv}.txt" if pv else None,
                "chunk_text": chunk_ref,
                "chunk_text_sha256": chunk_sha,
                "subject_name": parts["subject_name"],
                "user_message_tail": parts["tail"],
                "content": view["content"],
                "usage": view["usage"],
                "parse_errors": view["parse_errors"],
            }
        )
    found = {d["custom_id"] for d in docs}
    missing = sorted(wanted - found)

    # Which request's answer did the pipeline ACCEPT for each record? Verified
    # by content, not assumed: the dump's paragraph is matched against every
    # candidate answer (group request + retries). A retried record usually
    # carries the retry's text, but not always (run 20260905T213127: 101 of
    # 1,593 kept the group answer) — so the accepted request is the one whose
    # answer equals the paragraph; when none does, the highest retry is taken
    # and the record is flagged ``answer_mismatch``.
    paragraphs = _dump_paragraphs(files)
    answers = {custom_id: _answer_texts(doc) for custom_id, doc in ((d["custom_id"], d) for d in docs)}
    candidates: dict[str, list[tuple[dict[str, Any], dict[str, Any]]]] = {}
    for doc in sorted(docs, key=lambda d: (d["retry_index"], d["custom_id"])):
        subject, field_name, chunk_bounds = _address_of(doc["custom_id"])
        for record in parse_wire_records(doc["user_message_tail"] or ""):
            index_key = "|".join([subject, field_name, chunk_bounds, record["record_id"]])
            candidates.setdefault(index_key, []).append((doc, record))
    index: dict[str, Any] = {}
    matched_by_text = 0
    mismatched: list[str] = []
    for index_key, options in candidates.items():
        paragraph = paragraphs.get(index_key)
        record_id = index_key.rsplit("|", 1)[1]
        chosen = None
        if paragraph is not None:
            for doc, record in reversed(options):  # latest retry first
                if answers[doc["custom_id"]].get(record_id) == paragraph:
                    chosen = (doc, record)
                    matched_by_text += 1
                    break
        if chosen is None:
            chosen = options[-1]
            if paragraph is not None:
                mismatched.append(index_key)
        doc, record = chosen
        snippets = list(record.get("snippets") or [])
        index[index_key] = {
            "focal_form": record.get("focal_form"),
            "snippets": snippets,
            "request_custom_id": doc["custom_id"],
            "retry_index": doc["retry_index"],
            "superseded_request_custom_ids": [
                d["custom_id"] for d, _ in options if d["custom_id"] != doc["custom_id"]
            ],
            "accepted_by_text_match": paragraph is not None and index_key not in mismatched,
            "ud": loading.ud_of_custom_id(doc["custom_id"]),
            "pv": loading.pv_of_custom_id(doc["custom_id"]),
            "evidence_sha256": evidence_digest(snippets),
            "chunk_text": doc["chunk_text"],
            "chunk_text_sha256": doc["chunk_text_sha256"],
        }

    (out_dir / "synthesis_requests.json").write_text(
        json.dumps(docs, ensure_ascii=False), encoding="utf-8"
    )
    (out_dir / "evidence_index.json").write_text(
        json.dumps(index, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    (out_dir / "subject_names.json").write_text(
        json.dumps(subject_names, ensure_ascii=False, indent=1, sort_keys=True),
        encoding="utf-8",
    )
    return {
        "run_id": run_id,
        "requests_wanted": len(wanted),
        "requests_found": len(found),
        "requests_missing": missing,
        "retry_requests": sum(1 for d in docs if d["retry_index"]),
        "records_indexed": len(index),
        "records_from_retries": sum(1 for v in index.values() if v["retry_index"]),
        "records_accepted_by_text_match": matched_by_text,
        "records_answer_mismatch": mismatched,
        "records_without_dump_paragraph": sum(
            1 for k in index if paragraphs.get(k) is None
        ),
        "chunk_texts_written": len(chunk_hashes),
        "chunk_text_mismatches": chunk_mismatches,
        "subject_names": subject_names,
        "subject_name_conflicts": name_conflicts,
        "system_prompts_written": sorted(prompts_written),
        "out_dir": str(out_dir),
    }


def _dump_paragraphs(files: dict[tuple[str, str], Path]) -> dict[str, str]:
    """index_key -> the synthesis paragraph the dump holds (synthesized only)."""
    out: dict[str, str] = {}
    for (subject, field_name), path in files.items():
        if field_name == loading.SHARED_DUPLICATE:
            continue
        for record in loading.iter_records(subject, field_name, loading.load_dump(path)):
            if record.synthesis:
                out["|".join([subject, field_name, record.chunk_bounds, record.group_id])] = (
                    record.synthesis
                )
    return out


def _answer_texts(doc: dict[str, Any]) -> dict[str, str]:
    """record_id -> synthesis text in one request's answer ({} when the
    answer is missing or unparseable; a duplicated id keeps its last text)."""
    try:
        payload = json.loads(doc.get("content") or "")
    except (TypeError, ValueError):
        return {}
    out: dict[str, str] = {}
    for row in payload.get("syntheses") or []:
        if isinstance(row, dict) and isinstance(row.get("synthesis"), str):
            out[str(row.get("record_id"))] = row["synthesis"]
    return out


def load_evidence_index(run_id: str) -> Optional[dict[str, Any]]:
    path = SNAPSHOT_ROOT / run_id / "evidence_index.json"
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def load_subject_names(run_id: str) -> dict[str, str]:
    path = SNAPSHOT_ROOT / run_id / "subject_names.json"
    if not path.is_file():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def chunk_text_path(run_id: str, reference: Optional[str]) -> Optional[Path]:
    """Absolute path of a snapshot's chunk-text reference (``chunk_text/…``)."""
    if not reference:
        return None
    return SNAPSHOT_ROOT / run_id / reference


def evidence_for(
    index: dict[str, Any], record: "loading.SynthRecord"
) -> Optional[dict[str, Any]]:
    """Evidence for a record; contract_products reads products' snippets."""
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
