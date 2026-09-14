"""Run-B tryout (2026-09-14): the six FIELD SECTIONS ("## What to settle about the dealing", requirements doc
§10, revised against the C16 base) tried on real production requests of run 20260912T191548 before the user
publishes. One or two requests per field, chosen from the run's verified verdicts as the requests that carry
the field's target classes (own-listing under-claims, laundering, parent voice, case-study titles, the
customer-equipment feature list C26, the nested-list C27), plus the sales-rep coin (mathewsco process_caps).

Arms: c16 = the published static (pass2/c16_system.txt, md5 18d3e093…); b = the same static with that
FIELD's section inserted (pass2/b_<field>_system.txt — six files, one per field, written from the approved
drafts). Every arm runs N repeats (default 5) in the PRODUCTION call shape (gpt-4.1, GPTModelParams as the
notebook builds them, the strict wire schema, temperature 0, seed 12345) and is read as a MODE FRACTION,
never as one sample (design doc §23).

Requests are rebuilt from the evidence snapshot (no Mongo): the snapshot keeps each chunk's text once and
every request's tail (name line + fenced record blocks), and the node's user message is exactly
    "text scraped from a manufacturer's website:\n" + chunk_text + "\n\n" + tail
behind the per-request nonce line (llm_phrase_synthesis_node_service.render_synthesis_context,
gpt_batch_request_service.NONCE_LABEL). `--check` proves it against a request that WAS pulled from Mongo.

Usage (from anywhere):
    run_tryout_b.py --check                      # rebuild tanfel g15 and diff it against pass2/req_tanfel_g15_user.txt
    run_tryout_b.py --write                      # write pass2/reqb_<tag>_user.txt for every target request
    run_tryout_b.py [--n 5] [--arms c16 b] [--reqs tag ...]   # run; outputs pass2/b_out_<tag>_<arm>_<k>.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import sys
import uuid

ROOT = next(p for p in pathlib.Path(__file__).resolve().parents if (p / ".git").exists())
for line in (ROOT / ".env").read_text().splitlines():
    if "=" in line and not line.startswith("#"):
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

HERE = pathlib.Path(__file__).resolve().parent
P2 = HERE / "pass2"
HARNESS = HERE.parents[1]  # tests/test_stages/synthesis
SNAPSHOT = HARNESS / "evidence_snapshots" / "20260912T191548"

NONCE_LABEL = "request nonce (ignore): "
TEXT_OPEN = "text scraped from a manufacturer's website:\n"

# tag -> (field, custom-id prefix up to the model segment). Chosen 2026-09-14 from run 191548's verified
# verdicts (majors / fails per request) and CANDIDATE_DIMENSIONS C21–C27.
TARGETS: dict[str, tuple[str, str]] = {
    # products: own-listing under-claims on a private-label catalog (C16's class, must NOT reopen); case-study titles
    "prod_ag36": ("products", "agstech.net>products>llm_phrase_synthesis>chunk>95453:181033>group>36>"),
    "prod_al15": ("products", "alecmfg.com>products>llm_phrase_synthesis>chunk>0:94646>group>15>"),
    # equipments: instrument catalog rows ("offers for sale" supplied); a machine list with laundering + under-claims
    "eq_ag8": ("equipments", "agstech.net>equipments>llm_phrase_synthesis>chunk>0:95453>group>8>"),
    "eq_fz8": ("equipments", "fzemanufacturing.com>equipments>llm_phrase_synthesis>chunk>95976:185041>group>8>"),
    # industries: the representative's line card (served vs made); "Healthcare institutions" under-claim
    "ind_mw1": ("industries", "mathewsco.com>industries>llm_phrase_synthesis>chunk>0:43039>group>1>"),
    "ind_sc4": ("industries", "steelcraft.com>industries>llm_phrase_synthesis>chunk>0:91845>group>4>"),
    # conformity: the parent-policy voice (TD-P3); standards under-claims on a materials page; the nested-list C27
    "conf_td0": ("conformity_attestations", "taylordunn.com>conformity_attestations>llm_phrase_synthesis>chunk>0:37787>group>0>"),
    "conf_hw9": ("conformity_attestations", "howcogroup.com>conformity_attestations>llm_phrase_synthesis>chunk>0:73328>group>9>"),
    "conf_sc4": ("conformity_attestations", "steelcraft.com>conformity_attestations>llm_phrase_synthesis>chunk>0:91845>group>4>"),
    # material_caps: the "materials available for the processes we offer" hedge (18 J2 minors); a parts/materials mix
    "mat_al6": ("material_caps", "alecmfg.com>material_caps>llm_phrase_synthesis>chunk>0:94646>group>6>"),
    "mat_dm14": ("material_caps", "decimal.net>material_caps>llm_phrase_synthesis>chunk>0:89556>group>14>"),
    # process_caps: the sales-rep coin (C21/§23); own-listing hedge on capability indexes (C22); customer-equipment features (C26)
    "proc_mw2": ("process_caps", "mathewsco.com>process_caps>llm_phrase_synthesis>chunk>0:43039>group>2>"),
    "proc_tf15": ("process_caps", "tanfel.com>process_caps>llm_phrase_synthesis>chunk>0:81109>group>15>"),
    "proc_ag19": ("process_caps", "agstech.net>process_caps>llm_phrase_synthesis>chunk>95453:181033>group>19>"),
}


def _snapshot_requests() -> list[dict]:
    return json.loads((SNAPSHOT / "synthesis_requests.json").read_text(encoding="utf-8"))


def rebuild_user_message(prefix: str, *, nonce: str | None = None) -> str:
    """The user message of the first-pass request whose custom id starts with ``prefix``."""
    hits = [r for r in _snapshot_requests() if r["custom_id"].startswith(prefix) and str(r.get("retry_index", "0")) == "0"]
    if len(hits) != 1:
        raise SystemExit(f"{prefix}: {len(hits)} first-pass requests in the snapshot (want 1)")
    req = hits[0]
    # The snapshot stores chunk text rstripped of its newlines (pull.split_user_message); on the wire the
    # window text ends with one newline (every Mongo-pulled request in pass2/ shows "\n\n\n" before the name
    # line), so it goes back before the builder's own "\n\n".
    chunk = (SNAPSHOT / req["chunk_text"]).read_text(encoding="utf-8").rstrip("\n") + "\n"
    body = f"{TEXT_OPEN}{chunk}\n\n{req['user_message_tail']}"
    return f"{NONCE_LABEL}{nonce or uuid.uuid4().hex}\n\n{body}"


def check() -> None:
    """Rebuild tanfel process_caps 0:81109 g15 and compare with the Mongo-pulled request byte for byte
    (the nonce line excepted)."""
    pulled = (P2 / "req_tanfel_g15_user.txt").read_text(encoding="utf-8")
    nonce = pulled.split("\n", 1)[0][len(NONCE_LABEL) :]
    rebuilt = rebuild_user_message(TARGETS["proc_tf15"][1], nonce=nonce)
    if rebuilt == pulled:
        print("check OK: rebuilt request is byte-identical to the Mongo-pulled one", file=sys.stderr)
        return
    a, b = rebuilt.splitlines(), pulled.splitlines()
    print(f"check FAILED: {len(a)} vs {len(b)} lines", file=sys.stderr)
    for i, (x, y) in enumerate(zip(a, b)):
        if x != y:
            print(f"  first difference at line {i}:\n    rebuilt: {x[:160]!r}\n    pulled:  {y[:160]!r}", file=sys.stderr)
            break
    raise SystemExit(1)


def write_requests(tags: list[str]) -> None:
    for tag in tags:
        path = P2 / f"reqb_{tag}_user.txt"
        path.write_text(rebuild_user_message(TARGETS[tag][1]), encoding="utf-8")
        print(f"wrote {path.name} ({path.stat().st_size:,} bytes)", file=sys.stderr)


def system_text(arm: str, field: str) -> str:
    path = P2 / "c16_system.txt" if arm == "c16" else P2 / f"b_{field}_system.txt"
    return path.read_text(encoding="utf-8")


async def run(tags: list[str], arms: list[str], n: int, concurrency: int) -> None:
    from llm_providers.models.llm_model import GPT_4_1  # noqa: E402
    from llm_providers.models.open_ai.gpt_model_params import GPTModelParams  # noqa: E402
    from core.models.extraction_schemas.synthesis import SYNTHESIS_RESPONSE_SCHEMA  # noqa: E402
    from llm_providers.utils.ask_llm_util import ask_gpt  # noqa: E402

    # The PRODUCTION call shape (run_tryout_p3.py "4.1s"): the notebook's GPTModelParams + the strict wire schema
    # the synthesis node attaches. Every call carries a fresh nonce, as production does.
    params = GPTModelParams(
        temperature=0, top_p=1, presence_penalty=0, frequency_penalty=0, seed=12345,
        max_completion_tokens=10_000, response_format={"type": "json_object"},
    ).with_response_format(SYNTHESIS_RESPONSE_SCHEMA)
    gate = asyncio.Semaphore(concurrency)

    async def one(tag: str, arm: str, k: int) -> None:
        out = P2 / f"b_out_{tag}_{arm}_{k}.json"
        if out.exists() and out.stat().st_size > 0:
            print(f"skip {out.name} (exists)", file=sys.stderr)
            return
        field, prefix = TARGETS[tag]
        context = rebuild_user_message(prefix)
        async with gate:
            text = await ask_gpt(context=context, prompt=system_text(arm, field), gpt_model=GPT_4_1, model_params=params)
        out.write_text(text or "", encoding="utf-8")
        print(f"{tag} {arm} #{k}: {len(text or ''):,} chars", file=sys.stderr)

    await asyncio.gather(*[one(t, a, k) for t in tags for a in arms for k in range(n)])


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--n", type=int, default=5)
    ap.add_argument("--arms", nargs="+", default=["c16", "b"])
    ap.add_argument("--reqs", nargs="+", default=list(TARGETS))
    ap.add_argument("--concurrency", type=int, default=8)
    args = ap.parse_args()
    unknown = [t for t in args.reqs if t not in TARGETS]
    if unknown:
        raise SystemExit(f"unknown request tags: {unknown}; known: {list(TARGETS)}")
    if args.check:
        check()
        return
    if args.write:
        write_requests(args.reqs)
        return
    missing = [P2 / f"b_{TARGETS[t][0]}_system.txt" for t in args.reqs if "b" in args.arms]
    missing = sorted({m for m in missing if not m.exists()})
    if missing:
        raise SystemExit("arm b needs the per-field system texts first: " + ", ".join(m.name for m in missing))
    asyncio.run(run(args.reqs, args.arms, args.n, args.concurrency))


if __name__ == "__main__":
    main()
