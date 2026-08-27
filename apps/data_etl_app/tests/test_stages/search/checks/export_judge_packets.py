"""Export one judging packet per (subject, field) for the judged census.

A judge agent must see exactly what the search stage saw — the window's wire
text — and exactly what it returned, with nothing else in the way. Packets are
written under history/runs/<run>/raw/judge_packets/ because they embed the
scraped SITE TEXT and must stay gitignored; the judgments the agents write
back are small and DO get committed.

Usage: .venv/bin/python export_judge_packets.py --run 20260825T194457
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from expectations import load_expectations  # noqa: E402
from loading import load_run  # noqa: E402
from paths import run_dir, subject_slug  # noqa: E402

CODE_TABLES = {
    "products": "V valid product designation · B borderline (component, option, family, third-party part offered) · P process/service/finish · M material · G generic noun · C client's product/application · S spec/attribute/equipment/facility/feature · D document/literature title · U UI or fragment junk",
    "equipments": "V machine/tool designation · B borderline (generic-but-machine category) · D product/good made or sold · P process name · T software/tooling/fixture ambiguous · S spec/facility · G generic · U junk",
    "process_caps": "V performed operation on the work (inspection/testing/measurement ARE operations) · B borderline operation · L certification-lab/test-house procedure · I installation/field-site work · Z business/legal/HR activity · M material · E equipment · G generic · U junk",
    "material_caps": "V material/alloy/grade designation · B borderline (material family) · P process implying no substance · N part/component name · C consumable · G generic · U junk",
    "industries": "V industry/sector served or targeted · B borderline (customer segment, application domain) · O the subject's OWN activity presented as a sector · W word-association · G generic · U junk",
    "conformity_attestations": "V standard/certification/regulatory designation · B borderline (named program, audit scheme, code body) · D document title with no standard id · Z non-conformity certificate sense · G generic · U junk",
}


def export(run_id: str, only_subject: str | None = None) -> Path:
    raw = run_dir(run_id) / "raw"
    out_dir = raw / "judge_packets"
    out_dir.mkdir(parents=True, exist_ok=True)
    pulled = raw / "search_requests.json"
    field_runs = load_run(run_id, pulled_file=pulled if pulled.is_file() else None)
    written = 0
    for fr in field_runs:
        # Re-exporting while judge agents are mid-read would race their file
        # handles, so a targeted re-export is the safe way to refresh one
        # subject's packets (e.g. after its expectation set is verified).
        if only_subject and fr.subject != only_subject:
            continue
        windows = [w for w in fr.windows if w.round_index is None and w.phrases is not None]
        if not windows:
            continue
        exp = load_expectations(fr.subject, fr.field)
        lines = [
            f"# Judge packet — {fr.subject} / {fr.field} — run {run_id}",
            "",
            "## The task",
            "",
            f"Code EVERY returned form below with the {fr.field} code table:",
            "",
            f"    {CODE_TABLES.get(fr.field, 'see TAXONOMY.md')}",
            "",
            "Judge by the PLAIN MEANING of the field, reading the window text —",
            "not by any prompt, and not by what you think the pipeline intended.",
            "The stage is RECALL-FIRST: a thing named in the text is in-field even",
            "when it belongs to a client, supplier, lab or parent company — record",
            "that as an `actor` flag, never as a lower code.",
            "",
            "Also list, per window, any in-field entity the text NAMES that no",
            "returned form covers (a MISS). Quote it verbatim.",
            "",
        ]
        if exp:
            confirmed = [e for e in exp.entries if e.get("status") == "confirmed"]
            if confirmed:
                lines += [
                    f"Known must-find entities for this subject/field ({len(confirmed)} confirmed);",
                    "you may find more, and finding more is the point:",
                    "",
                ]
                lines += [f"  - {e['name']}" for e in confirmed[:60]]
                lines += [""]
        for w in windows:
            lines += [
                "---",
                "",
                f"## Window {w.sub_bounds} (chunk {w.chunk_bounds})",
                "",
                f"### Forms returned by search ({len(w.phrases or [])})",
                "",
            ]
            lines += [f"{i + 1}. {form!r}" for i, form in enumerate(w.phrases or [])]
            lines += ["", "### The window text the stage read", "", "```", (w.wire_text or "").rstrip(), "```", ""]
        path = out_dir / f"{subject_slug(fr.subject)}__{fr.field}.md"
        path.write_text("\n".join(lines), encoding="utf-8")
        written += 1
    print(f"wrote {written} judge packets -> {out_dir}")
    return out_dir


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", required=True)
    parser.add_argument("--subject", default=None, help="export only this subject's packets")
    args = parser.parse_args()
    export(args.run, args.subject)
