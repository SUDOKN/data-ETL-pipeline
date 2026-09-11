"""Pack judge slices into agent jobs and write one instruction file per job.

  .venv/bin/python checks/judge_jobs.py --run <run_id> [--per-job 190] [--model claude-sonnet]

Reads ``history/runs/<run>/pending/judged/SLICES.json`` (from
``judged_population.py``; falls back to ``pending/SLICES.json`` from
``slices.py``) and ``history/runs/<run>/JUDGE_PROMPT.md`` (the filled copy of
``JUDGE_PROMPT_TEMPLATE.md``), packs consecutive slices into jobs of at most
``--per-job`` records (a slice larger than that rides alone), and writes
``history/runs/<run>/pending/judged/jobs/J<nn>.md`` + ``MANIFEST.json``. Each
job file lists its ITEMS (order path, start, end, out file, judge string) and
then the common instructions, so an agent prompt need only say "read J<nn>.md
and follow it". Verdict files land in ``history/runs/<run>/verdicts/``.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from checks import scorecard  # type: ignore[no-redef]
else:
    from . import scorecard


def build(run_id: str, per_job: int, model: str, scratch_hint: str) -> dict[str, Any]:
    run_dir = scorecard.history_dir(run_id)
    slices_path = run_dir / "pending" / "judged" / "SLICES.json"
    if not slices_path.is_file():
        slices_path = run_dir / "pending" / "SLICES.json"
    slices = json.loads(slices_path.read_text(encoding="utf-8"))
    slices.sort(key=lambda s: (s["subject"], s["field"], s["part"]))
    jobs: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    count = 0
    for s in slices:
        if current and count + s["records"] > per_job:
            jobs.append(current)
            current, count = [], 0
        current.append(s)
        count += s["records"]
    if current:
        jobs.append(current)

    prompt = (run_dir / "JUDGE_PROMPT.md").read_text(encoding="utf-8")
    body = prompt.split("\n---\n", 1)[1] if "\n---\n" in prompt else prompt
    taxonomy = str((scorecard.history_dir(run_id).parents[2] / "TAXONOMY.md").resolve())
    common = (
        body.replace("{TAXONOMY}", taxonomy)
        .replace("{MODEL}", model)
        .replace("`{ORDER}`", "the ITEM's `order` path")
        .replace("`records[{START}:{END}]`\n   (0-based, half-open; {N} records)", "the ITEM's `records[start:end]` (0-based, half-open)")
        .replace("`## {FIELD}` addendum defines J6 for your field", "`## <field>` addendum of each ITEM's field defines J6 for it")
        .replace(f"`agent:{model} {{FIELD}} {{SUBJECT}} part{{PART}}`", "the ITEM's `judge` string exactly")
        .replace("**Write** to `{OUT}`", "**Write** each ITEM's rows to its `out` file")
        .replace("`<scratchpad>/judge_{SUBJECT}_{FIELD}_part{PART}/`", f"`{scratch_hint}/judge_<job id>/`")
        .replace("`{SUBJECT}/{FIELD} part{PART}: <rows\nwritten> rows,", "per ITEM `<subject>/<field> part<part>: <rows written> rows,")
    )
    common = re.sub(r"\{RUN_ID\}", run_id, common)
    verdicts = run_dir / "verdicts"
    verdicts.mkdir(exist_ok=True)
    job_dir = run_dir / "pending" / "judged" / "jobs"
    job_dir.mkdir(parents=True, exist_ok=True)
    manifest = []
    for j, items in enumerate(jobs, start=1):
        jid = f"J{j:02d}"
        lines = [
            f"# Judge job {jid} — synthesis eval, run {run_id}\n",
            "You have several ITEMS (slices of work orders). Judge EVERY record of every item; write each item's rows to its own `out` file. Common instructions follow the item list.\n",
            "## ITEMS\n",
        ]
        for it in items:
            out = verdicts / f"{it['subject']}__{it['field']}__part{it['part']}.jsonl"
            lines.append(json.dumps({
                "subject": it["subject"], "field": it["field"], "part": it["part"],
                "order": str(Path(it["path"]).resolve()), "start": it["start"], "end": it["end"],
                "records": it["records"], "out": str(out),
                "judge": f"agent:{model} {it['field']} {it['subject']} part{it['part']}",
            }))
        lines.append("\n## Common instructions\n")
        lines.append(common)
        (job_dir / f"{jid}.md").write_text("\n".join(lines), encoding="utf-8")
        manifest.append({"job": jid, "records": sum(i["records"] for i in items), "items": len(items), "file": str(job_dir / f"{jid}.md")})
    (job_dir / "MANIFEST.json").write_text(json.dumps(manifest, indent=1), encoding="utf-8")
    leftovers = sorted({m.group(0) for f in job_dir.glob("J*.md") for m in re.finditer(r"\{[A-Z_]+\}", f.read_text(encoding="utf-8"))})
    return {"jobs": len(manifest), "records": sum(m["records"] for m in manifest), "max": max(m["records"] for m in manifest), "min": min(m["records"] for m in manifest), "unfilled_placeholders": leftovers, "dir": str(job_dir)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True)
    parser.add_argument("--per-job", type=int, default=190)
    parser.add_argument("--model", default="claude-sonnet")
    parser.add_argument("--scratch", default="<scratchpad>", help="scratch directory hint written into the job files")
    args = parser.parse_args()
    print(json.dumps(build(args.run, args.per_job, args.model, args.scratch), indent=1))


if __name__ == "__main__":
    main()
