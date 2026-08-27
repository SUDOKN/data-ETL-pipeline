# The SUDOKN Extraction Pipeline, Explained From Scratch

*Written 2026-08-24, against the code on branch `new-ground-truth-v2` (commit `29d2167`).*

This document explains what this whole system does, how it does it, every stage
it goes through, and — the part that is usually undocumented — **every place
where the logic splits in two**, including the splits that quietly rejoin the
main road so that nothing downstream ever notices.

I am assuming you know how to program but know nothing about this project. Every
term gets defined the first time it appears. Nothing is abbreviated without being
spelled out first.

---

## Table of contents

1. [What problem is this thing solving?](#1-what-problem-is-this-thing-solving)
2. [The vocabulary you need](#2-the-vocabulary-you-need)
3. [The four big phases](#3-the-four-big-phases)
4. [The twelve fields, in three families](#4-the-twelve-fields-in-three-families)
5. [How one field's pipeline is physically built](#5-how-one-fields-pipeline-is-physically-built)
6. [The universal life cycle of a single stage](#6-the-universal-life-cycle-of-a-single-stage)
7. [Stage by stage, in order](#7-stage-by-stage-in-order)
   - [7.0 Prefill](#70-prefill--cutting-the-text-up)
   - [7.1 Phrase search](#71-phrase-search--what-does-this-page-even-mention)
   - [7.2 Recursive search](#72-recursive-search--currently-switched-off)
   - [7.3 Mention collection](#73-mention-collection--where-does-each-phrase-actually-sit)
   - [7.3b The aggregation fold](#73b-the-aggregation-fold--the-invisible-pure-code-stage)
   - [7.4 Synthesis](#74-synthesis--one-paragraph-per-thing)
   - [7.5 Grounding](#75-grounding--turning-a-description-into-candidate-labels)
   - [7.6 Screening](#76-screening--does-the-manufacturer-actually-do-this)
   - [7.7 Descent](#77-descent--how-specific-can-we-get-concepts-only)
   - [7.8 Reconcile](#78-reconcile--write-the-answer-and-dump-the-evidence)
8. [The complete fork catalogue](#8-the-complete-fork-catalogue)
9. [Forks that rejoin silently](#9-forks-that-rejoin-silently)
10. [Failure modes and weird scenarios](#10-failure-modes-and-weird-scenarios)
11. [What is switched on right now](#11-what-is-switched-on-right-now)
12. [How you actually run it](#12-how-you-actually-run-it)

---

## 1. What problem is this thing solving?

There is a database of American manufacturing companies. For each company, we
want structured, machine-readable facts:

- What products do they make?
- What products do they make *for other people* (contract manufacturing)?
- What machines do they own?
- What industries do they serve?
- What certifications do they hold?
- What manufacturing processes can they perform?
- What materials can they work with?
- Where are they? What is their business description? Are they even a
  manufacturer at all?

The only raw material we have is **their website**. Nothing else. So the whole
system is: *scrape a company's website, read it with a large language model, and
turn what it says into rows and triples that a database can answer questions
about.*

The hard part is not "call the model." The hard part is that a website says
things like:

> "Our Falcon SZ Series frames are available in 16-gauge galvannealed steel."

...and you have to decide, without hallucinating:

- Is `Falcon SZ Series` a product this company makes, or a brand they resell?
- Is `galvannealed steel` a material *capability* (they can work it) or just a
  word on a spec sheet?
- Does `16-gauge` mean anything for our purposes? (No.)
- If the ontology — the fixed, curated vocabulary of allowed answers — has a
  concept called `Steel` and one called `Galvanized Steel`, which one is right?

Every stage in this pipeline exists because one of those judgments went wrong at
some point and needed to be split into a smaller, more checkable question.

---

## 2. The vocabulary you need

Read this section once. Everything after it uses these words.

**etld1** — "effective top-level domain plus one". It is the company's identity
in this system. For `https://www.steelcraft.com/products/frames`, the etld1 is
`steelcraft.com`. One company = one etld1.

**Scraped text file** — the entire website flattened into one giant text file
and stored in Amazon S3 (a file store). Each page inside it is written as:

```
##########################################
https://steelcraft.com/products/frames

Frames

Our Falcon SZ Series frames are available in...
```

That is: a separator line of `#` characters, then the page's URL alone on a
line, then a blank line, then the page content. This layout matters more than
you would think — several stages detect page boundaries by looking for exactly
those two line shapes.

**Ontology** — a curated tree of allowed answers, stored as RDF (a graph data
format) in S3. For example, under `process_caps` there might be `Machining` →
`Milling` → `CNC Milling`. Each node is a **Concept**, and each concept has a
name plus **altLabels** (alternative spellings that mean the same thing).
Together the name and altLabels are called **matchLabels**.

**In-vocabulary vs out-of-vocabulary (OOV)** — "in-vocabulary" means the answer
exists in the ontology. "Out-of-vocabulary" means the model found something real
that the ontology has no word for. We keep OOV answers separately, because they
are the list of things the ontology is missing.

**Field** — one column of the answer. `products`, `industries`,
`material_caps`, `is_manufacturer`, etc. There are twelve of them plus
`email_addresses`.

**Pipeline / chain** — for each field, an ordered chain of objects called
**nodes**. Node 1 does its work, then calls node 2, which calls node 3, and so
on. This is the classic "chain of responsibility" pattern. The chain is built
once by `ExtractionPipelineFactory` and reused for every company.

**Stage** — the name of one link in that chain. Defined by the `PipelineStage`
enum: `prefill`, `phrase_search`, `recursive_search`, `mention_collection`,
`synthesis`, `initial_grounding`, `oov_grounding`, `freehand_grounding`,
`screening`, `iterative_grounding`, `single_stage_extraction`, `reconcile`.
(There is also a retired `relationship` stage, kept only so old stored data can
still be read.)

**Chunk** — the scraped text is too big for one model call, so it is cut into
pieces. Currently: at most 2 chunks of at most 20,000 tokens each. A **token**
is roughly three-quarters of an English word — it is the unit the model counts
in and bills in.

**Sub-window** — each 20,000-token chunk is further cut into 4 smaller windows
of ~5,000 tokens for the stages that need fine-grained reading. The setting is
called `search_divisor` and it is currently 4.

**Bounds** — a chunk or window is identified by a string like `"0:81234"`, which
means "characters 0 through 81,234 of the scraped text". Everything downstream
can recover the exact text with `subject_text[start:end]`, so the geometry never
has to be stored twice.

**GPTBatchRequest** — one row in MongoDB representing one question to the model.
It holds the request body, the response (once it exists), and a **custom_id**.

**custom_id** — the unique name of a request, and the single most important
design idea in this codebase. It looks like:

```
steelcraft.com>products>llm_search>chunk>0:81234>sub>0:20455>pn=product_phrase_search|pv=Xk9...|m=gpt-4.1|t=0|mct=4000
```

It encodes: which company, which field, which stage, which chunk, which
sub-window, which prompt name, which **prompt version** (`pv=`), which model,
and which model settings. Because a request is looked up purely by custom_id:

- If you re-run the same company with the same prompts, every request is found
  already answered in MongoDB and **the model is never called again**. Re-runs
  are nearly free.
- If you change one word of a prompt, the prompt version changes, so every
  custom_id downstream of it changes, so nothing matches, so those stages are
  genuinely re-asked. You cannot accidentally mix old answers with a new prompt.
- Two different pipelines that mint the *same* custom_id automatically share the
  answer. (This is used deliberately — see the `products` / `contract_products`
  trick in §9.)

**`|ud=` (upstream digest)** — for the later stages, the custom_id also contains
a hash of the exact payload being sent. So if the content changes even slightly,
the id changes, and a stale old answer can never be silently reused.

**Deferred manufacturer** — a scratchpad document in MongoDB, one per company,
that holds the in-progress state of every field: which request ids have been
minted, which chunks exist, and the run's **metadata** (prompt versions, model,
chunking settings). When a field finishes, the final answer is written onto the
real `Manufacturer` document. "Deferred" because in production the model is
called through OpenAI's *batch* API, which takes hours, so the work has to be
resumable across process restarts.

**Eager vs deferred (batch)** — two execution modes.
- **Eager** (`eager=True`): send every request immediately over the normal API,
  wait for answers, keep going. This is what the notebook and the queue worker
  use. One company runs start to finish in one process.
- **Deferred** (`eager=False`): write the request rows, stop. A separate process
  gathers them into a JSONL file, ships it to OpenAI's batch endpoint (50%
  cheaper, up to 24h), and later records the answers. The pipeline is re-run
  from the top and now gets one stage further. Repeat until done.

**Prompt** — the instruction text sent to the model, stored in S3 and pinned by
version. Some prompts are hand-written ("static"); others are **rendered** from
a **rule catalog**.

**Rule catalog** — a JSON file listing the numbered rules a stage must apply
(e.g. `SCR-2`, `SCR-G3`), which is both (a) rendered into the prompt text and
(b) turned into the JSON schema the model must answer in, so that the model has
to report *which rules it applied* to reach its answer. This is what makes the
output auditable by a human annotator later.

**Hold** — a validation step where a response is checked against exactly the
set of items the request asked about. If a request asked about 60 items and the
response answers for 4, the hold catches it. Different stages have different
policies about what to do next (warn, drop, or fail).

---

## 3. The four big phases

Zoomed all the way out, a company goes through four phases. Only phase 2 is what
people mean when they say "the pipeline", and it is where 95% of this document
lives.

### Phase A — Scraping

`packages/scraper` drives a pool of headless Chrome browsers (via Selenium).
Starting from the company's homepage, it does a breadth-first crawl up to 5
levels deep, up to 5 browsers at once, with a timeout. It:

- skips file extensions that aren't web pages (`.pdf`, `.zip`, `.exe`, ...),
- blocks social media domains so it doesn't wander onto Facebook,
- tries to click cookie-consent banners so the real content loads,
- de-duplicates repeated boilerplate content across pages,
- concatenates everything into the one big text file described above,
- uploads it to S3 and records the **version id** on the `Manufacturer`.

The version id matters: it means "this answer was extracted from *this exact
snapshot* of the website". If the site is re-scraped, the old answers are no
longer claimed to describe the new text.

### Phase B — Extraction

The subject of this document. Turns the text file into structured results per
field. Runs either from a queue worker bot (`new_extract_queue_bot.py`) or, for
development, from a Jupyter notebook (`mfg_extraction_test.ipynb`).

### Phase C — Ground truth / human audit

A separate layer where a human with a manufacturing background reads the run's
output and marks each LLM-written text field `agree`, `agree_but` (correct but
incomplete — the recall flag), or `disagree` with a replacement. This is stored
as an audit trail and is intended to feed both fine-tuning and automated prompt
optimization later. It also feeds back into Phase B in one place today: the
`is_manufacturer` decision prefers the human ground truth over the model's
answer when one exists.

### Phase D — Publishing to the knowledge graph

`ttl_generator_service.py` turns a finished `Manufacturer` into RDF triples
using standard industrial ontologies (BFO, IOF Core, IOF SupplyChain, plus the
project's own SUDOKN namespace) and pushes them into a graph database with a
SPARQL update that first deletes everything under that company's URI prefix and
then inserts the new triples. Delete-then-insert, so re-publishing is idempotent.

---

## 4. The twelve fields, in three families

The fields are not all processed the same way. There are three shapes.

### Family 1 — Single-stage fields (simple)

`addresses`, `business_desc`, `is_manufacturer`, `is_product_manufacturer`,
`is_contract_manufacturer`.

Chain: **prefill → single_stage_extraction → reconcile**. One model call over
(almost) the entire website at once — the chunking strategy for these is a single
chunk of `120,000 tokens − prompt size − 10,000`. Ask the question, parse the
JSON, write the answer. That's it.

The three `is_*` fields are **binary classifications**: the answer is a yes/no
plus an explanation plus the list of catalog rules that were applied.

### Family 2 — Keyword fields (open vocabulary)

`products`, `contract_products`, `equipments`.

There is no fixed list of allowed answers. If a company makes "hydraulic
manifold blocks", the answer is the string `hydraulic manifold blocks`. The
model **mints** the label itself.

Chain: **prefill → phrase_search → recursive_search → mention_collection →
synthesis → freehand_grounding → screening → reconcile**.

`products` and `contract_products` are the *same question asked two ways*:
"products this company makes and sells as its own" versus "products this company
manufactures for other companies under contract". They share their first four
stages entirely (see §9).

### Family 3 — Concept fields (closed vocabulary)

`conformity_attestations` (certifications like `ISO 9001`), `industries`,
`process_caps` (manufacturing processes), `material_caps` (materials).

Answers must come from the ontology tree. There is also an escape hatch for
things the ontology is missing.

Chain: **prefill → phrase_search → recursive_search → mention_collection →
synthesis → initial_grounding → oov_grounding → screening → iterative_grounding
→ reconcile**.

That is the longest chain: ten stages, up to eight of which call the model.

---

## 5. How one field's pipeline is physically built

`ExtractionPipelineFactory.create_pipelines()` returns a dictionary mapping each
field to the head of its chain. The chain is built as nested constructor calls,
which reads a bit like Russian dolls:

```python
ConceptExtractionPrefillNode(
    field_type=ConceptTypeEnum.industries,
    chunk_strategy=wide,
    ontology=ontology,
    # ... one metadata object per downstream stage ...
    next_node=ConceptPhraseSearchNode(
        next_node=ConceptRecursiveSearchNode(
            next_node=ConceptMentionCollectionNode(
                next_node=ConceptSynthesisNode(
                    next_node=ConceptInitialGroundingNode(
                        next_node=ConceptOovGroundingNode(
                            next_node=ConceptRelationshipScreeningNode(
                                next_node=ConceptIterativeGroundingNode(
                                    next_node=ConceptReconcileNode(...)))))))))
```

Two things to notice.

**The prefill node is handed every stage's metadata, not just its own.** That is
because prefill writes the run's identity — prompt versions, model, chunking
settings, knob values — onto the deferred document *once*, at the very start.
Every stage afterwards stamps its custom_ids from that stored copy. So a run's
identity is frozen at the moment the field is first deferred.

**The nodes are stateless with respect to the company.** One
`ManufacturerExtractionOrchestrator` builds these chains once and then runs many
companies through them one after another. The company arrives as an argument.
This is deliberate: it guarantees every company in a sweep is measured against
byte-identical pipelines.

### The one shared object: `PipelineContext`

As each stage finishes, it publishes its completed request map into a shared
object:

```python
pipeline_context[type(self)] = completed_request_map
```

Downstream stages read their upstream out of it: `pipeline_context[ConceptSynthesisNode]`.
The context also carries three things set up front by the orchestrator:

- `subject_name` — the company's real name, from the business description stage.
  Used so that later prompts can say "the manufacturer in question is Steelcraft"
  and so masking/lint checks can spot when the model leaks the name where it
  shouldn't.
- `subject_text` — the scraped text (after page trimming). Needed because some
  stages scan the text mechanically at the moment they mint request ids.
- `stage_toggles` — which stages this run is allowed to execute.

It also records `stages_completed` in order, which is what lets a deliberately
stopped run dump what it has so far.

---

## 6. The universal life cycle of a single stage

Before walking the stages, understand the loop that *every* model-calling stage
runs. It lives in `BaseLLMExtractionNode.execute()` and it is the same eight
steps every time.

```
1. Am I switched off for this run?        → if yes: STOP the whole chain here.
2. Mint my request ids (embed_request_ids) and save them on the deferred doc.
3. Which of my ids have no request row in MongoDB?      (get_missing_req_ids)
4. Build request bodies for exactly those, and upsert them.
5. If eager: find every embedded id with no answer yet, fire them all
   concurrently, record the responses.
6. Are all my ids answered now?
      NO  → return quietly. (Batch mode. The next run picks up here.)
      YES → continue.
7. Fetch the completed map; validate my own responses (parse them now, so a bad
   answer fails against MY request, not against whoever reads it later).
8. Publish the map to the pipeline context, and call next_node.execute().
```

### Why step 7 exists

Historically, a stage's answer was first parsed by whichever *later* stage
consumed it. On 2026-08-11 that meant the screening stage logged "COMPLETE" at
one timestamp, and 65 milliseconds later the grounding stage crashed on a
malformed screening response. The failure surfaced against the node that *read*
the data instead of the node that *paid for* it — one phase after the request
that could have been re-asked was already declared done. `validate_own_responses`
moves the check back to where the request was made.

### The recursive variant

Some stages need more than one round-trip: recursive search asks again, mention
collection has a retry pass, the grounding stages and screening have retry
passes, and descent walks a tree level by level. Those nodes extend
`BaseLLMRecursiveExtractionNode`, which in eager mode replaces steps 2–6 with a
loop:

```
while True:
    embed_request_ids()                 # may add new ids this pass, or not
    create + upsert anything missing
    find every embedded id that is still unanswered
    if nothing missing and nothing unanswered: break
    dispatch them all, record responses
```

Two safety devices sit on that loop:

- **`MAX_UNPRODUCTIVE_PASSES = 3`.** If three passes in a row create no new
  requests *and* fail to shrink the unanswered set, it raises instead of
  spinning forever.
- **It raises instead of returning silently** if it exits with work still
  outstanding. That `else` branch did not exist until 2026-08-24, and its
  absence caused a run where three fields — 773 records, 36% of the run —
  simply vanished with no exception, no log, and no dump, while the sweep
  reported success.

The bug underneath that: the loop used to dispatch only requests it had *just
created*. But `record_response_parse_error` deliberately nulls a request's
response so that the next pass re-asks it. Such a row is not "missing" — the
document exists — so the loop never re-sent it. Which means the parse-error
retry had **never worked for any recursive stage** since it was written. It now
converges over *unanswered* requests, not merely absent ones.

### Two conventions every batched stage shares

**The single dummy.** If a unit of work (a window, a chunk) has nothing to ask
about, the stage still creates exactly one request — but pre-answered, built
against a fake model called `NO_MODEL` with its response already filled in
(`{"mentions": []}`, `{"syntheses": []}`, `{"screenings": []}`). This keeps the
"every unit has at least one request" invariant true, so "no requests" always
means "something is wrong" rather than "nothing to do". These rows are already
complete, so the dispatcher never tries to send them — a fact that itself broke
once, in August 2026, when a dummy got sent and tripped the model guard.

**Fenced blocks.** Requests that ask about a supplied set of items carry that set
twice in the user message, inside fences:

```
<<<MENTION_IDS
["m4k9x2m", "mpq81zd", ...]
MENTION_IDS>>>

<<<MENTIONS
[{"mention_id": "m4k9x2m", "mention": "Our Falcon SZ Series frames..."}, ...]
MENTIONS>>>
```

Two reasons. First, the parse side reads the sent set back out of the request
document itself, so "what we validate against" is "what we sent", by
construction — no threading required. Second, the fence exists because one stage
used to embed the raw scraped page *above* a bare `extracted phrases:` marker
line; a website containing that phrase would have won the match and the response
would have been validated against the website's own text. A fence cannot occur
by accident, and two fences raise rather than letting either win.

Why the same list twice (bare array, then the array of objects)? Because when
shown only the map, the model loses track of what the item itself *is* and starts
answering under the description instead of under the item. Naming the items alone
first fixes the referent. This repetition is deliberate and documented as
"never collapse this".

---

## 7. Stage by stage, in order

I will follow a concept field (`industries`) because it has the longest chain,
and note where keyword fields differ.

Running example: a fictional company `acme-frames.com` whose website contains,
among 60 pages:

> "**Frames**
> Our Falcon SZ Series frames are available in 16-gauge galvannealed steel.
> Acme has been ISO 9001:2015 certified since 2004 and serves the healthcare and
> education sectors.
> [Privacy Policy] [Terms of Use]"

---

### 7.0 Prefill — cutting the text up

**Class:** `ConceptExtractionPrefillNode` / `KeywordExtractionPrefillNode` /
`SingleStageExtractionPrefillNode`
**Calls the model:** no. Pure setup.
**Can be switched off:** no. It is in `_ALWAYS_ON` — everything downstream
indexes into the map it builds, so skipping it leaves nothing coherent to run.

Prefill does four things.

**(a) Drop excluded pages.** Before anything is measured or cut, pages whose URL
path says they are legal boilerplate — `privacy`, `cookie`, `terms`, `legal`,
`disclaimer`, `gdpr`, `imprint` — are removed from the text entirely.

Why: on a real run, Steelcraft's privacy policy alone was 35,000 characters,
introduced 103 phrases that appear on no other page, and cost roughly 80,000
prompt tokens. Worse, since chunks are also aligned to page starts, a legal page
could eat an entire window. Measured effect of removing them: real-content
coverage went from 64% back up to 80% of the token budget.

This runs on *every* execution, fresh or resumed, because all the stored chunk
bounds are offsets into the *trimmed* text. The trimming rule carries a version
string (`page_exclusion_version`) that is part of the run's metadata — so
changing the rule counts as metadata drift and refuses to resume an old run
rather than silently re-interpreting its offsets.

> **Fork:** `drop_excluded_pages` is a per-field setting. It is **on** for the
> six phrase fields and **off** for the single-stage fields — because a company's
> street address legitimately sits on an imprint page.

**(b) Cut into chunks.** With the current `wide` strategy: soft limit 20,000
tokens, at most 2 chunks, **0% overlap**, and each chunk must *start* at a page
boundary.

> **Fork:** overlap used to be 15%. It was set to 0 on 2026-08-22 because in the
> v3 design overlap only duplicated work: search is window-local, mention
> collection and folding are per-chunk, synthesis is per-group, and reconcile
> merges across chunks by key anyway. Measured cost of keeping it: 297 duplicate
> occurrences and 87 groups synthesized twice.

> **Fork:** page alignment (`align_to_page_headers`) is on for phrase fields, off
> for single-stage. Before it, every one of 39 measured windows started
> mid-page, which meant the stage that describes *where* a passage sits often
> couldn't see which page it was on. Aligning costs about 8% more windows.
>
> **Sub-fork:** a single page longer than the token limit still has to split. Its
> continuation windows start mid-page, and `wire_window_text` prepends an
> explicit "continued from <URL>" header so the model still knows where it is.

**(c) Derive sub-windows.** Each 20,000-token chunk is divided by
`search_divisor` (currently 4) into ~5,000-token windows.

> **Fork:** if `search_divisor == 1`, the sub-window list is just `[chunk_bounds]`
> — identical to having no sub-windows at all. The rest of the code doesn't
> branch; it just sees a list of one.

> **Fork:** the **trailing remainder merge**. Line-respecting division sometimes
> leaves a final sliver. If the last window is shorter than 20% of the one before
> it, it is merged backwards into its predecessor. Measured motive: two slivers
> (386 and 775 characters) cost 12 search requests and 10 dummy mention requests
> and produced 0–2 phrases.

**(d) Brute search.** For concept fields only, prefill also runs a plain
case-insensitive, whole-word regex scan of each chunk against every matchLabel
in the ontology. If the text literally contains the string `Aerospace`, that
label is recorded as a "brute" hit. This is a free floor — a safety net under the
model. It also records, per sub-window, the *exact casing* each brute label
appears in, because the later mention stage works on exact strings.

**(e) The resume fork — the big one.**

```python
if not bool(getattr(deferred_subject, self.field_type.name)):
    # brand new: chunk it, write metadata, build an empty request map
else:
    # already deferred: check the stored metadata against today's
    self.raise_if_metadata_is_stale(...)
```

`raise_if_metadata_is_stale` compares the *entire* stored metadata tree against
what is configured right now, field by field, excluding only `created_at`. **Any**
difference raises `StaleExtractionMetadataError` and refuses to run.

Why so blunt? Because extraction always runs on the *latest* resources — the
prompt bytes come from whatever `PromptService` pinned at startup, the vocabulary
from the latest ontology — while custom_ids are stamped from the metadata
*stored* on the deferred field. If those drift apart, the pipeline builds a
request from today's prompt and files it under yesterday's version id, which
makes the record assert a provenance it does not have. An earlier version of this
guard checked only a few "important" fields, and a republished prompt sailed
straight through it: a resumed company silently replayed answers the *old* prompt
had produced, and an A/B test measured nothing.

The remedy the error message gives you: set `deferred.<field>` to null for that
company and re-run. The stored batch requests do **not** need deleting — the ones
whose custom_id still matches today's metadata are reused, so only what actually
changed gets re-sent.

---

### 7.1 Phrase search — "what does this page even mention?"

**Stage:** `phrase_search` · **Calls the model:** yes, once per sub-window.

The model is shown one ~5,000-token window and asked to return a flat JSON array
of phrases relevant to this field, copied verbatim from the text.

```json
{"phrases": ["healthcare", "education", "hospital casework"]}
```

That's it. No reasoning, no judgment about whether the company actually serves
those industries. Just: *what does this window mention*. Deliberately broad —
precision is somebody else's job further down.

**Fork — the output token cap.** The search stages override the pipeline's
`max_completion_tokens` (20,000) with their own **4,000**. Reason: across a real
run the largest first-search answer was 657 output tokens and the mean was 229.
That 20,000-token headroom is exactly what a repetition loop fills — one
recursive round spent all 20,000 tokens repeating a single phrase 2,453 times and
truncated mid-string. Capping at 4,000 bounds the damage.

**Fork — truncation salvage.** If the response fails JSON validation, the parser
checks whether it is specifically an object whose first key is `phrases` opening
an array, and if so walks the array recovering every *complete* string element,
stopping at the one cut mid-string. That runaway response still held 48 distinct
real phrases before it started repeating itself, and discarding the whole answer
would have thrown them away. Anything that isn't that exact shape still raises —
a response that closed its array failed for some other reason, and one holding
non-strings isn't this schema's answer at all.

**Fork — repetition reporting.** `set()` would absorb a repetition loop
silently, so before de-duplicating, the parser checks: 50+ phrases and fewer than
half of them unique → log a warning naming the most-repeated string and its
count.

**Fork — parse failure.** Any unrecoverable parse failure calls
`record_response_parse_error_capped`, which writes an error record holding the
*full* raw response (the exception carries it as an attribute rather than in its
message, because a degenerate response is 124 KB and the message ends up in
tracebacks), nulls the request's response and batch_id so the next pass re-asks,
and raises `RepeatedParseFailure` once the same request has failed
`RESPONSE_PARSE_ERROR_CAP` (3) times.

**The chunk-level view.** Downstream, nobody cares about sub-windows. One
function — `parse_batch_request_result` — returns the *union* of a chunk's
sub-window phrases. That is what keeps the sub-window split invisible below the
search stages.

---

### 7.2 Recursive search — currently switched off

**Stage:** `recursive_search` · **Calls the model:** in principle yes, in
practice **zero times**.

The design: re-search each window while telling the model "you already found
these — find *new* ones", repeating until a round yields nothing new or the round
cap is hit.

`DEFAULT_RECURSIVE_SEARCH_MAX_ROUNDS = 0` since 2026-08-22, for both concept and
keyword families. The measurement that killed it (run `20260822T061410`): only
65% of the phrases it returned were verbatim — 22% occurred *nowhere* in the
window they were supposedly read from. It ran on empty lists, it dumped page
menus, and in its actual intended role (completing partial forms) it carried
about 10% of the occurrences the first search already had.

**This is the cleanest example of a "fork that rejoins silently".** With
`max_rounds=0`, the node embeds no request ids, creates nothing, dispatches
nothing, publishes an **empty** completed map to the pipeline context, and calls
its successor. Everything downstream reads that empty map and unions it with the
first search's results — `X ∪ {} = X`. Nothing else in the chain knows or cares.

The stage is not deleted, because it can be turned back on by passing
`max_recursive_search_rounds=N` without touching any other code.

---

### 7.3 Mention collection — "where does each phrase actually sit?"

**Stage:** `mention_collection` · **Calls the model:** yes, but for a much
smaller job than the name suggests.

This stage used to ask the model to *find* every occurrence of every phrase.
That was measured twice and found to be a lossy Ctrl+F: the code that consumed
its answers already threw away any reported occurrence whose snippet didn't
contain the phrase verbatim, so the model's *accepted* output was a strict subset
of what a mechanical scan finds — minus a further 17–19% it satisficed away,
deterministically skipping repeated listing-title lines for common words.

So the job was split:

- **Code** finds the occurrences.
- **The model** is asked only one thing: for each distinct passage, *where does
  this sit in the page?* — in its own words. That is the one part that genuinely
  requires seeing the container.

An answer looks like:

```json
{"mentions": [
  {"mention_id": "m4k9x2m",
   "location": "in a product-description sentence under the 'Frames' heading, in the site's own copy"}
]}
```

#### What code does before the model is asked

**Pool the forms chunk-wide, then filter per window.**

> **Fork (settled 2026-08-22).** Search harvests phrases per *sub-window*. But a
> phrase search listed for window 1 and not for window 2 was measured to leave
> ~1,100 occurrences uncollected per run. Since a regex scan is free, the
> **chunk's** phrases are pooled — every sub-window's search results, plus any
> recursive-search results, plus the exact casings of brute-search labels — and
> then each sub-window is handed the pooled phrases that actually *occur* in it.
> Search stays answerable for per-window recall as a metric; pooling just stops
> one window's omission from costing the whole chunk.

**Scan with word boundaries, always.** The matcher (`floor_scan`) places a
boundary guard at an edge of the phrase only when that edge *is* a word
character. So `6061-T6` matches in "6061-T6 aluminum" but not in "16061-T6";
`CNC` matches in "CNC/Manual"; `C++` needs no right guard; and `Lead` matches
"Lead Time" and "Lead-free" but never "Leader". That last one is a real measured
failure: plain substring matching for `Lead` produced 52 hits, 0 of them real.

**Two tiers.** Tier 1 is exact-case. Tier 2 is case-insensitive, and its extra
hits are "casing rescue" — spellings search never emitted but that are really
there. **Fork:** forms of 3 characters or fewer stay case-sensitive even in tier
2, because `Al`, `SS`, and `ms` in lowercase are English noise.

**The scan domain excludes page furniture.** Separator lines and bare-URL header
lines are blanked out — but blanked *with spaces*, length-preserving, so every
offset the scan reports is still an offset into the original text. Excluded
(legal) pages are blanked the same way.

**Longest-span containment.** If `Lead` and `Lead Time` both hit inside "Sample
Lead Time", only the longest one owns the span. A hit strictly inside a longer
hit of *any* phrase is not an owner.

**Clip to a snippet.** Each occurrence's snippet is the sentence containing it
*within its line*, or the whole line if that line has no sentence punctuation
(menus, headings, list entries). Median snippet: 112 characters; max 623 — versus
138 / 1,973 for the old LLM collector.

> **Fork — `snippet_radius`.** A knob. At `0` (the default and the one every run
> so far has used) you get exactly the clip above. Above 0, the snippet widens to
> include *r* sentence units on each side, where blank lines are skipped and a
> page-boundary line is a hard stop that is never crossed. Since the snippet's
> hash *is* the mention id, this knob is part of the request identity — changing
> it re-asks everything.

**Distinct snippets only.** A line repeated fifty times becomes **one** wire
item. On one measured run, 4,907 occurrences collapsed to 2,880 items.

#### Then the model is asked

Items are split into groups of at most `max_mentions_per_request` (**50**), each
group becoming one request against the same window text. Measured on 2026-08-22:
median 23 distinct snippets per window, p90 81, max 125 — so most windows fit in
a single request.

> **Fork — the dummy.** A window with zero items still gets exactly one request,
> pre-answered with `{"mentions": []}`.

> **Fork — the hold is warn-only in both directions.** An id the model answered
> that was never sent is dropped with a warning. A sent id with no answer is
> simply absent, and the fold gives that mention `"(location not described)"`.
> **Nothing raises.** The reasoning: the model can no longer *lose* a mention —
> the mention is a fact of the text, established by code — it can only fail to
> colour one. A raising hold would replay a temperature-0 mis-echo to death.

> **Fork — the two-pass under-answer retry.** Once every group request for a
> window is answered, the node **assesses** the window: which sent ids did no
> answer describe? Those are stored on the deferred document, and if any exist,
> **one** retry request set is embedded for exactly those items, with `>retry>1>`
> in the custom_id. A third pass finds nothing left to add and the loop exits.
> Nothing is ever retried twice. Measured motive: on one run, 1 of 106 first-pass
> answers described 0 of 47 items — the model had echoed the request's nonce
> string as an id.

---

### 7.3b The aggregation fold — the invisible pure-code stage

**This is not a `PipelineStage`.** It issues no requests, has no toggle, and does
not appear in any enum. It is pure, deterministic code that runs at the moment
the mention stage's results are parsed. It is also, arguably, the conceptual
centre of the whole v3 design, so it gets its own section.

Its job: take a chunk's raw occurrences plus the model's location descriptions,
and produce **groups** — the things that everything downstream actually reasons
about.

**Grouping is a dictionary keyed by a normalized form.** No LLM, no union-find.
`normalize()` is the grouper, and it is versioned (`NORMALIZER_VERSION`).

The normalizer has three layers:

- **L0 (always on)** — casefold; strip `™ ® ©`; hyphens, dashes and slashes
  become spaces; `&` becomes `and`; edge punctuation stripped; whitespace
  collapsed.
- **L1 (always on)** — per-token *noun* lemma from a pinned dictionary
  (`lemminflect==0.2.3`), using **only** the dictionary, never its rule-based
  guesser. Measured reason: the guesser is suffix-stripping in disguise —
  `continuous`→`continuou`, `abs`→`ab`, `as`→`a`. Words the dictionary doesn't
  know get one narrowly-guarded fallback: 5+ letters, alphabetic, ends in `s` but
  not `ss`/`us`/`is`/`ous` → drop the `s`. That fallback is what folds domain
  plurals like `stampings`, `counterbores`, `helicoils`, and over 4,157 real
  phrases it merged nothing it shouldn't have.
- **L2 (a per-field dial)** — verb/participle fold, so `CNC milled` and
  `CNC milling` land together. **Fork:** on for `material_caps` and `process_caps`
  only, off everywhere else, where a verb fold only uglifies noun modifiers
  (`mounting bracket` → `mount bracket`).

> **Fork — the code-token guard.** A token is never lemmatized if it contains a
> digit (`6061-T6`, `ISO9001`), has an internal capital (`AccuGrips` — a
> camel-cased name's casing *is* its identity, so `AccuGrip` and `AccuGrips` stay
> separate by design), or is capitalized and unknown to the dictionary (`Texas`,
> `Paladin`).

The whole normalizer is tuned around one asymmetry: **a wrong merge lands
upstream of every check, while an under-merge costs one redundant request and
heals at reconcile.** So every layer errs toward under-merging.

**What comes out.** For each group:

- `group_id = hash(normalized key)` — an opaque, random-looking id. Because it's
  opaque, the model never sees a candidate-looking label as the grouping key. And
  because it's content-derived rather than positional, the same group carries the
  same id in every chunk, which makes cross-chunk joining free. (Positional ids
  like `R1`, `R2` were rejected precisely because they reassign silently when
  something upstream changes, turning a stale replay into silent
  *misattribution*.)
- `focal_form` — the group's most frequent member form by mention count, ties
  broken by earliest occurrence. Chosen **in code**, deterministically. This is
  what synthesis is told the record is "about".
- The group's mentions, in a **locked order** (window index, then character
  offset), so the downstream payload and its hash depend on nothing but the text.
- The group's **entries** for synthesis: its *distinct* snippets in locked order,
  each with the location of its first occurrence.

> **Fork — `include_location`.** An A/B arm. `True` (every run so far) gives
> synthesis entries as `{location, snippet}`. `False` gives the snippet alone.
> It's encoded in the custom_id as `|loc=1` or `|loc=0`, so both arms can coexist
> in one database. **This A/B has never been run.**

> **Fork — empty bundles are kept.** A phrase search found but that has zero
> occurrences in its window — a search false positive, or one swallowed by
> longest-span containment — becomes a group with status `no_mentions`. It is
> skipped by synthesis and goes nowhere downstream, but it stays visible in the
> dump. The reasoning: the honest not-found branch must read as "asked, found
> nothing", never as silence.

> **Fork — id collision.** If two genuinely different strings hash to the same
> 7-character base-36 id, it raises `RecordIdCollisionError`. This is
> deterministic, so retrying is pointless; the remedy is to lengthen the id,
> which is a wire-format change that re-dispatches everything. At 7 characters
> the odds of any collision within a 300-phrase chunk are under one in a million.
> It has happened once in practice, and it killed a field mid-run.

---

### 7.4 Synthesis — one paragraph per thing

**Stage:** `synthesis` · **Calls the model:** yes, once per group-batch per chunk.

The fold has produced groups. Synthesis writes **one description per group**,
using that group's evidence.

A request carries: the manufacturer's name at the top (the prompt then tells the
model to mask it in the output), and a fenced block of records, each being:

```json
{"record_id": "gn8gmdzc",
 "focal_form": "Falcon SZ Series frames",
 "entries": [
   {"location": "in a product-description sentence under the 'Frames' heading",
    "snippet": "Our Falcon SZ Series frames are available in 16-gauge galvannealed steel."},
   {"location": "in the main navigation menu",
    "snippet": "Falcon SZ Series"}
 ]}
```

And the model answers one description per record id.

> **Fork — packing.** Records are packed in bundle order into requests of at most
> `max_entries_per_request` (**50**) entries. It is a **soft** cutoff: a record is
> never split across requests, and a record bigger than the cap travels alone.

> **Fork — the dummy.** A chunk with no records gets one pre-answered request.

> **Fork — the hold, which is strict on one axis and lenient on the other.** An
> id the model answered that was never sent is **dropped** with a warning (a
> mis-echo, never a real record). A sent id with no answer is **missing**. There
> is no path by which a fabricated record reaches downstream, because results are
> read *by sent id*.

> **Fork — the same two-pass retry as the mention stage.** Assess the chunk once
> its group requests are complete; embed one retry set for exactly the
> unsynthesized ids; whatever is still missing after that is reported as
> `not_synthesized` in the dump and goes nowhere. A missing record never becomes
> a crash.

#### The `GroupRecord` — the single most important object downstream

After synthesis, everything downstream stops looking at raw text. It sees only:

```python
GroupRecords = dict[str, GroupRecord]   # group_id -> record
class GroupRecord:
    focal_form: str
    synthesis: str
```

Two fields. No member forms, no entries. The synthesis **is** the evidence
digest. This was a deliberate decision on 2026-08-24. And because `group_id` is
already an opaque hash, the "don't show the model a candidate-looking key"
discipline comes for free with no masking step.

There is exactly **one** function that produces this — `get_chunk_group_records`
— and every downstream consumer calls it: freehand grounding, in-vocab
grounding, OOV grounding, screening, descent, and both reconcile nodes. That is
on purpose: a consumer can never disagree with the stage it consumes about what a
group contains.

That function recomputes the fold from the text plus the stored mention answers
(the fold is deterministic; nothing but request ids is persisted), holds the
syntheses, and returns **only synthesized groups**. Groups with status
`no_mentions` or `not_synthesized` stay visible in the dump and go nowhere.

#### A known, accepted defect worth knowing about

Two groups whose evidence is thin and nearly identical sometimes get **the same
synthesis string**. The canonical case: an `FE Series Double-Egress Frames`
record and a `DE Series` record, each resting on a single bare navigation-menu
entry, in the chunk where neither has prose evidence. In the chunk where they
have 3 and 2 prose entries each, they are described correctly.

This was investigated thoroughly and the fix was **measured and killed**: the
"thin twin" signature (one evidence entry each, byte-identical locations) fires
837–1,557 times per run, giving 1–3% precision; narrowing it by confusable names
catches 1 of 14 cases. So the decision (2026-08-24) was: **accept it, and add a
tripwire.** The synthesis dump now counts identical-synthesis pairs within a
request, as a diagnostic counter, never as a retry trigger. Separately, the
`focal_form_lint` catches the genuinely harmful subclass — where a record's own
name is absent from its own synthesis — and screening sits downstream as a second
filter. Census: 12–23 such collapses per full run, most of them
accurate-but-undifferentiated rather than wrong.

---

### 7.5 Grounding — turning a description into candidate labels

"Grounding" means: given a record, what **labels** does it correspond to? This is
where the concept and keyword families finally diverge — but note that all three
grounding variants share **one service file**, because they differ only in what
rides beside the record and whether the emitted labels are held to a vocabulary.
Everything else — the request shape, the record-keyed response, the rule
validation walk, the exact id hold, the structural declination — is shared, and
sharing it is what keeps the three contracts from drifting apart.

All three grounding stages are **enumeration only**. They emit *candidates*. They
do **not** decide whether the manufacturer actually does the thing. That is
screening's job, downstream. This split is deliberate: the older design asked one
stage to both identify and attribute, and it did both worse.

#### 7.5a Initial grounding (concept fields) — the in-vocabulary pass

The request carries each group record plus an **outline of the entire ontology**
for this field. The model picks whichever existing concept labels the record
corresponds to, and reports which catalog rules it applied.

> **Fork — membership is enforced at the decoder.** The in-vocab pass has no
> escape hatch, so a label that isn't a vocabulary label is *corruption by
> definition* and fails the response. This closed a real hole where invented
> labels were leaking through and being persisted as fake "the ontology is
> missing this" gaps.
>
> **Sub-fork:** *casing drift alone* is repaired to the vocabulary's own spelling
> and warned about, not failed. The prompt says copy verbatim, so a re-cased copy
> is echo drift, not a different claim.

> **Fork — structural declination.** A record from which nothing qualifies
> answers with an empty array *plus an explanation*. That is a real answer, not
> an error, and it is preserved as `{"declined": "<why>"}` in the dump. There used
> to be a magic sentinel string ("None of the above") for this; it leaked into the
> out-of-vocabulary results and was retired.

> **Fork — the retry pass**, same shape as synthesis's: assess, embed one retry
> set for unanswered records, warn on what's still missing. Batch size: 30
> records per request.

#### 7.5b OOV grounding (concept fields) — the discovery pass

Runs **serially after** in-vocab grounding, and only for concept fields.

Each record rides with **what the in-vocab pass already found from it**, and the
model is asked what *both* the ontology and pass 1 are missing. The labels it
emits are minted freely — that's the entire point.

> **Fork — the whole pass is optional, and that is run *config*, not a toggle.**
> `oov_grounding_enabled=False` means the metadata's OOV entry is `None`. The node
> then embeds **zero** requests, publishes an **empty** completed map, and hands
> the chain straight to screening. This is a distinct run identity (it changes the
> stored metadata, so a run with it off can never be confused with a run with it
> on), and it is deliberately *not* a `StageToggle`, because toggles are hard-stops
> and this must be a pass-through.

> **Fork — an OOV mint that restates a vocabulary label.** If the model mints
> `CNC machining` and the ontology already has `CNC Machining`, that is folded
> onto the canonical spelling *before* screening (so one candidate gets one
> verdict under the canonical name), or re-routed at reconcile. It is **never**
> raised as an error and never persisted as a fake ontology gap. Re-routing a
> minted label that happens to match the vocabulary is reconcile's classification
> job, not a parse defect.

#### 7.5c Freehand grounding (keyword fields)

The keyword families' enumeration pass. **No options at all** — the model mints
every candidate itself, because there is no vocabulary to hold it to. Same
request shape, same retry pass, same hold. Batch size 30.

---

### 7.6 Screening — "does the manufacturer actually do this?"

**Stage:** `screening` · **Calls the model:** yes.

This is the precision gate. Candidates are **supplied** — grounding enumerated
them — so this stage never identifies anything. It judges, per candidate, the
manufacturer's *relationship* to it, using only the record's focal form and
synthesis. **Never the chunk text.** That is a deliberate design decision: the
stage's whole job is attribution, and giving it raw page text lets it start
re-identifying things instead.

Example: for the record `Falcon SZ Series frames` with candidate
`Hollow Metal Frames`, screening asks whether the manufacturer *makes* it, or
merely *mentions* it (a competitor's product, a link to a supplier, a
specification, a page about an industry they read about).

The rules are numbered and come from a rule catalog. The model must say which
rules it applied. A typical chain: `SCR-1` establishes that an entity of the
right kind was identified, `SCR-2` that it is substantively the thing,
`SCR-3` that the manufacturer's relationship to it is the required one, and a set
of guards (`SCR-G1`, `SCR-G2`, ...) that report only when violated.

> **Fork — two hold axes, both exact.** The record-id axis is held by the shared
> record hold. The candidate axis is held against the request's *own* records
> payload. Both are **exact**: ids and candidates are supplied strings, so a
> response key that differs is corruption, and "helpfully" matching it onto a
> neighbour is exactly the silent misattribution the whole record design exists to
> rule out.

> **Fork — fails closed.** If candidates exist for a record but no screening
> verdict does, the record's dump status becomes `screening_dropped`, distinct
> from `screened_out`. The two-axis hold should make this unreachable; until it is
> proven so, it must not be allowed to masquerade as "screened out".

> **Fork — the retry pass**, again. Batch size here is 25 pairs per request.

#### The precedence trap in the rule catalogs (worth understanding)

Every combinator in a catalog resolves **first-match-wins**. So two rules that
could both answer a case do not split it — the earlier one takes it and the later
one is *starved*. Overlap is therefore always a precedence bug, and the cost is
always paid by the later rule.

- An `all` section **masks**: document order is a dependency chain. The first
  condition that can absorb a case absorbs it, the chain terminates, and every
  rule behind it is recorded `not_triggered` — its notes never execute.
- An `ordered` section **starves**: only the chosen branch is reported, so a
  branch never *reached* is indistinguishable from one that never *applies*.
- An `any` section (guards) is cheap but blind: two guards firing costs nothing
  and gives an annotator two reasons, but a guard reports only on violation, so
  its silence is unobservable.

The measured consequence, from a 2026-08-18 audit: rule `SCR-3b` said in as many
words that supplying a sector's members counts as serving the sector — and had
**never once been read**, because `SCR-2` was phrased as an existential over
`SCR-3`'s predicate and settled those cases first. 20 of 20 industries screen-outs
failed at `SCR-2`; `SCR-3` was reached zero times.

The corollary, which is unintuitive: **a note inherits its parent rule's
precedence.** A carve-out written under rule N is unreachable for any case rule
N−1 already resolved. So placement is not a topical decision — a carve-out belongs
on the *earliest* rule that can plausibly resolve the case, not the rule it is
most obviously about.

---

### 7.7 Descent — how specific can we get? (concepts only)

**Stage:** `iterative_grounding` · **Calls the model:** yes, once per level.

Only concept fields have this, because only they have a *tree*.

Screening said the record is legitimately associated with `Machining`. But the
ontology has `Machining → Milling → CNC Milling`. Can we go deeper?

The seed is **every in-vocabulary candidate that passed screening**, grouped
concept-major. Then, level by level: show the model the parent concept's
children as options, plus the records that evidence the parent, and ask which
child (if any) applies. Repeat until nothing descends further.

> **Fork — stopping is structural, not quality-based.** A round stops because the
> concept is a leaf, or the maximum depth was reached, or the model answered an
> **empty options array with an explanation** (the structural declination again).
> A parent whose records all decline simply produces no children. There is no
> "confidence" threshold anywhere.

> **Fork — `false_child`.** The model can name a concept that is real but is
> **not** a child of the parent it was asked under. That event is *recorded on the
> node* rather than asserted as a finding. It's evidence about the model, not
> evidence about the company.

> **Fork — minted siblings are allowed.** Rule `RGR-M3` lets the model propose a
> sibling type of its own. Those become out-of-vocabulary results at reconcile.

> **Fork — only the deepest tag survives.** `get_deepest_concepts_and_oov`
> discards the immediate parent. If `CNC Milling` was reached, `Milling` and
> `Machining` are dropped — the deepest node implies its ancestors.

> **Fork — per-level de-duplication.** Node identity is `(parent, name)`, so a
> concept reached directly *and* named by a parent's response would produce two
> descents of one concept, where downstream asserts one per level. Ordinary
> in-vocabulary nodes therefore merge by concept name, **first-in wins** — because
> an earlier pass's node may already carry a dispatched request, so the existing
> node must survive. Stopped nodes and out-of-vocabulary nodes keep per-parent
> identity, since each parent's verdict is its own record.

> **Fork — survivors are not re-screened.** The descent runs *after* screening
> and its results are trusted. Since the descent only refines an already-approved
> parent, re-screening would ask a question already answered.

---

### 7.8 Reconcile — write the answer and dump the evidence

**Stage:** `reconcile` · **Calls the model:** no.

Reconcile is pure aggregation. It re-derives everything one last time from the
stored request maps and produces three outputs.

**(1) The final result on the `Manufacturer` document.** For concept fields:

```python
ConceptExtractionResultsV2(
    metadata=<the run identity>,
    results=ConceptsFound(in_vocab={...}, out_of_vocab={...}),
    chunked_extraction_stats={ "0:81234": <per-chunk stats>, ... },
)
```

> **Fork — routing OOV back in-vocabulary.** An out-of-vocabulary candidate that
> resolves through the ontology's label map becomes an in-vocabulary result. Only
> genuinely unmatched labels stay out-of-vocabulary.

> **Fork — case de-duplication across chunks.** Chunks propose out-of-vocabulary
> labels independently, so the union carries case variants of one label
> (`galvannealed steel` / `Galvannealed Steel`). The union is
> case-insensitively de-duplicated. Per-chunk stats keep the raw variants,
> deliberately, so nothing is lost for analysis.

**(2) The extraction dump** — one JSON file per (company, field) per run, written
to `packages/logs/extraction_dumps/<timestamp>/<etld1>__<field>.json`. This is
the artifact you actually read when analyzing a run. One row per group, with a
`status` field taking one of:

| status | meaning |
|---|---|
| `no_mentions` | the group had zero occurrences — the honest not-found branch |
| `not_synthesized` | synthesis never wrote a description, even after the retry |
| `no_candidates` | every grounding pass declined this record, and said why |
| `screened_out` | grounding proposed candidates; screening rejected all of them |
| `grounded` | at least one candidate passed screening |
| `screening_dropped` | candidates exist but no verdict does — defensive, should be unreachable |

> **The second-witness check.** `status` is deliberately redundant with the other
> fields in the row. `_record_status_from_fields` re-derives it from the row alone
> and logs an error on disagreement. So a future bug that makes the fields lie is
> caught by the dump instead of shipping.

> **Fork — `search_round` provenance.** Round 0 is reserved for brute-search
> survivors. A phrase that matched nothing gets `search_round: null, provenance:
> "unmatched"` rather than a fake round 0.

> **Fork — a grounding pass that never ran has its key OMITTED** from the row,
> rather than written as empty. That is what keeps "never asked" distinguishable
> from "asked, found nothing" when you read the file six weeks later.

**(3) The wipe-down** — which currently does nothing. `ReconcileNode.wipe_down`
is called, but its entire body is commented out. In its intended form it would
delete the completed batch requests and null the deferred field. Today the
deferred document and every request row survive, which is why re-runs replay
from MongoDB for free. Worth knowing that the cleanup exists in name only.

---

## 8. The complete fork catalogue

### 8.0 Before any field runs — the orchestrator's own forks

`ManufacturerExtractionOrchestrator.process_manufacturer()` runs a strict
pre-flight sequence before it touches the twelve field pipelines, and it is full
of branches with very different consequences.

```
get-or-create the deferred document
  │
  ├─ mfg.is_manufacturer already set? ──── yes ──► skip
  │        no ──► run the is_manufacturer pipeline
  │                 └─ raises? ──► log ExtractionError, RETURN. Nothing else runs.
  │
  ├─ mfg.business_desc already set? ────── yes ──► skip
  │        no ──► run the business_desc pipeline
  │                 └─ raises? ──► log ExtractionError, RETURN. Nothing else runs.
  │
  ├─ business_desc has no .result.name? ──► raise ValueError. Hard stop.
  │
  ├─ mfg.email_addresses already set? ──── yes ──► skip
  │        no ──► regex-scan the text (NO model call)
  │                 └─ raises? ──► log ExtractionError and CONTINUE ANYWAY
  │
  ├─ look up the human ground truth for is_manufacturer
  │        found?  ──► use the human's verdict
  │        absent? ──► use the model's verdict
  │
  ├─ verdict is "not a manufacturer"? ──► log it and CONTINUE ANYWAY
  │                                        (the `return` is commented out,
  │                                         "for testing or gt purposes")
  │
  └─ for each of the 12 field pipelines:
         field already has data? ──► set deferred.<field> = None, save, skip
         otherwise               ──► run the pipeline
```

Three of these deserve emphasis.

**The prerequisite abort is asymmetric.** `is_manufacturer` and `business_desc`
failing kills the whole company, because everything after them needs the
company's real name. `email_addresses` failing is logged and shrugged off,
because nothing depends on it.

**"Not a manufacturer" no longer stops anything.** The `return` that would skip a
non-manufacturer is commented out. Right now every company is fully extracted
regardless. This is intentional for ground-truth collection, but it means the
`is_manufacturer` gate currently costs money without saving any.

**The twelve field pipelines are NOT individually wrapped in try/except.** If
`industries` raises, `process_caps` and `material_caps` never run for that
company. The only protection is one level up, in the notebook's sweep loop, which
catches per *company* so one bad company doesn't lose the whole sweep.

**Prerequisite pipelines get `explicit_only` toggles.** If you run with
`StageToggles().stop_after(PipelineStage.synthesis)` — a blanket toggle that
disables reconcile for every field — that would take `is_manufacturer` and
`business_desc` down with it, and the run would fail before your stage under test
was ever reached. So prerequisite pipelines receive only the toggles that name
them *by name*. Switching off a prerequisite has to be asked for explicitly.

### 8.1 Configuration forks (decided before the run)

| Fork | Values | Where | Consequence |
|---|---|---|---|
| `eager` | True / False | orchestrator arg | Synchronous run vs. deferred batch resumption |
| `oov_grounding_enabled` | True / False | `create_pipelines` | Concept OOV pass runs or is a pass-through; **changes run identity** |
| `max_recursive_search_rounds` | 0 / N | factory | Recursive search is a no-op or actually iterates. **Currently 0** |
| `search_divisor` | 1 / 4 | chunking strategy | Whole chunks vs 5k sub-windows for search + mention |
| `drop_excluded_pages` | on / off | chunking strategy | Legal pages removed before chunking. On for phrase fields |
| `align_to_page_headers` | on / off | chunking strategy | Chunks start at page boundaries. On for phrase fields |
| `overlap` | 0 / 0.15 | chunking strategy | **Currently 0** |
| `verb_fold` | on / off | per field | On only for `material_caps` + `process_caps` |
| `snippet_radius` | 0 / r | run knob | Snippet width. **Currently 0** — and it is part of the mention id |
| `include_location` | True / False | run knob | The unrun synthesis A/B. **Currently True** |
| `max_mentions_per_request` | 50 | run knob | Mention request packing |
| `max_entries_per_request` | 50 | run knob | Synthesis packing (soft — never splits a record) |
| `max_pairs_per_request` | 30 / 30 / 30 / 25 | run knob | initial / OOV / freehand grounding, then screening |
| `chunk_strategy_overrides` | per field | orchestrator arg | Experiment knob; leaves source defaults untouched |
| `StageToggles` | per (field, stage) | orchestrator arg | Hard-stops the chain at the first disabled stage |

### 8.2 Runtime forks inside a stage

| Fork | Branches | Rejoins? |
|---|---|---|
| Field brand new vs resumed | chunk + write metadata / staleness check | yes, at the same next node |
| Metadata drift on resume | raise `StaleExtractionMetadataError` | **no — hard stop** |
| Missing request ids | create bodies / skip creation | yes |
| All requests answered? | proceed to next node / return quietly | eventually (batch mode) |
| Response parses? | continue / record error, null response, re-ask (cap 3) | yes, after retry |
| Search response truncated | salvage complete elements / raise | yes |
| Search response repetitive | warn / silent | yes |
| Window has 0 mentions | one pre-answered dummy / real requests | yes |
| Chunk has 0 records | one pre-answered dummy / real requests | yes |
| Model under-answers | assess + one retry set / no retry | yes |
| Still missing after retry | default the value, warn | yes — never a crash |
| Model answers an id nobody sent | drop + warn (grounding/screening/mention/synthesis) | yes |
| Model answers one record twice | **raise** | no |
| In-vocab label not in the vocabulary | **raise** (corruption) | no |
| In-vocab label with wrong casing | repair to canonical + warn | yes |
| OOV mint that restates a vocab label | fold onto canonical before screening | yes |
| Grounding finds nothing | structural declination + explanation | yes — recorded, not an error |
| Screening rejects everything | `screened_out` | yes |
| Candidates exist, verdict missing | `screening_dropped`, fails closed | yes |
| Descent names a non-child | record `false_child` on the node | yes — not asserted as a finding |
| Descent mints a sibling | becomes an OOV result | yes |
| Descent reaches a leaf / max depth / empty options | stop descending | yes |
| Group id collision | **raise** `RecordIdCollisionError` | no |
| Convergence loop makes no progress 3× | **raise** | no |
| Loop exits with work outstanding | **raise** (since 2026-08-24) | no |
| Stage disabled by toggle | write a partial dump, stop the chain | **no — hard stop by design** |

---

## 9. Forks that rejoin silently

These are the ones you specifically asked about: the flow splits, but everything
downstream is written so that it cannot tell which branch was taken. Each one is
a place where you could stare at a stage's output and never know a decision was
made.

**1. `products` and `contract_products` share four stages.**

This is the biggest one. They are two separate top-level pipelines, with separate
storage, separate final results, and separate screening prompts. But
`ContractProductPhraseSearchNode`, `ContractProductRecursiveSearchNode`,
`ContractProductMentionCollectionNode` and `ContractProductSynthesisNode` all
override `get_request_custom_id` to **deliberately ignore the field type they
were constructed with** and compute the id as if the field were `products`:

```python
@staticmethod
def get_request_custom_id(subject_unique_id, field_type, chunk_bounds, sub_bounds, metadata):
    # Deliberately ignore the passed field_type and use the shared "products"
    # identity so this phase's custom_id matches the pure-product branch's.
    return KeywordPhraseSearchNode.get_request_custom_id(
        ..., field_type=KeywordTypeEnum.products, ...)
```

Since requests are looked up purely by custom_id, whichever pipeline runs first
creates and pays for those requests; the second finds them already complete and
**never calls the model**. Four of the seven stages — including the two most
token-hungry ones — are paid for once instead of twice.

From `contract_products`' point of view, its search / mention / synthesis stages
"ran" — the logs say so, the pipeline context has the maps — and there is no
signal anywhere that the answers were computed under a different field's name.
Only from freehand grounding onward do the two branches genuinely diverge.

**2. Recursive search with `max_rounds=0`.** Embeds nothing, publishes an empty
map, calls the successor. Downstream unions that empty set into the first
search's results and is none the wiser. The only reason this is safe is that
`get_relationship_candidates` and its successors were *written* to tolerate an
empty recursive map — a property that, as the `pipeline_stage` docstring warns,
**nothing else downstream has**. That is exactly why disabled stages are hard
stops rather than pass-throughs: a middle stage cannot be made transparent by
doing nothing, because its successor reads *its* map and would silently produce a
run whose later stages all saw an empty world.

**3. OOV grounding switched off.** Same shape. Zero requests, empty map, straight
to screening. It is expressed as run config rather than a toggle *precisely
because* it needs this pass-through behaviour, which toggles refuse to give.

**4. Sub-windows disappear below the search stages.** Search runs per 5,000-token
window, but `parse_batch_request_result` returns the union across a chunk's
windows. Everything from mention collection down thinks in chunks. If you set
`search_divisor` from 4 to 1, no downstream code changes behaviour at all.

**5. The trailing-remainder merge.** A sliver window is folded into its
predecessor at prefill. Downstream just sees a slightly longer last window.

**6. The `no_mentions` branch.** A phrase that search found but that occurs
nowhere becomes a group with zero mentions. It is *kept*, given a status, and
excluded from synthesis and everything after. Downstream stages never see it —
but the dump does, so the not-found case is visible to a human even though it is
invisible to the pipeline.

**7. Undescribed mentions get a default.** A snippet the location model didn't
answer for keeps its mention under `"(location not described)"`. Nothing raises,
nothing is lost, and the synthesis entry simply carries that placeholder. The
mention itself is a fact of the text, so the model's silence cannot delete it.

**8. Casing rescue.** The mechanical scan finds casings of a phrase that search
never emitted, and they become collected forms in their own right, sitting in
their family's group. This is the mechanical replacement for what recursive
search used to supply — and since recursive search is now off, it is the *only*
supplier. Nothing downstream distinguishes a searched form from a rescued one.

**9. Chunk-wide form pooling.** A phrase found in window 1 is scanned for in
windows 2, 3 and 4 as well. Downstream reads a window's `sent_forms` list off the
bundle and has no way to tell which of them that particular window's search
actually returned.

**10. Excluded pages are removed twice, differently.** They are trimmed out of
the text before chunking (offsets shift), *and* the scan blanks them
length-preservingly, *and* `wire_window_text` replaces them with a marker on the
wire. Three mechanisms, one URL rule, and no stage downstream has to know about
any of it.

**11. Dummy requests are indistinguishable from real ones in the request map.**
A pre-answered dummy is a complete `GPTBatchRequest` like any other. The
completed map that reaches the pipeline context contains both.

**12. A repaired casing in grounding.** The response said `iso 9001`; the stored
result says `ISO 9001`. A warning is logged, and the result carries no trace of
the repair.

**13. Human ground truth silently overrides the model.** In the orchestrator,
`final_decision` is the human's verdict if a `BinaryGroundTruth` exists and
otherwise the model's. The rest of the function cannot tell which it got.

**14. Batch mode's quiet return.** In deferred mode, a stage with unanswered
requests simply returns without calling its successor and without raising. That
is the entire deferred design — the run is expected to be re-entered later — but
it looks identical, from the outside, to a run that finished. The recursive nodes
used to share this behaviour by accident, and that accident is what silently lost
36% of a run before it was fixed on 2026-08-24 by making them raise instead.

---

## 10. Failure modes and weird scenarios

Things that have actually gone wrong, and what the code now does about them.

**A stage's bad response used to blame the wrong stage.** Fixed by
`validate_own_responses` — see §6.

**A model repeating one phrase 2,453 times until it hit 20,000 tokens.** Fixed
three ways: a 4,000-token cap on search stages, truncation salvage, and a
repetition warning. Notably this was *not* model-specific — it happened on
`gpt-4.1-mini` and on `gpt-4.1`.

**A parse-error retry that never worked.** For every recursive stage, since the
day it was written. Fixed 2026-08-24. It has **no live verification** yet — the
evidence is five unit tests and the next genuine mid-run failure.

**Three fields vanishing with no exception, no log, and no dump, while the sweep
reported success.** Two causes: a duplicate group id raising inside a synthesis
parse (killing the field *and* its shared `contract_products` sibling), and the
recursive loop's silent return. The second was fixed; the first was deliberately
left alone, because with the loop fixed it buys three re-dispatches before the
cap trips.

The operational lesson recorded from that incident: **a missing dump is not
always a crash.** Always check the dump *count* against the previous run's.

**A republished prompt sailing through the staleness guard.** Fixed by comparing
the entire metadata tree rather than a chosen subset. The general rule that came
out of it: *a prompt-version change orphans every stale row, so it also hides
every resume bug.* Resume behaviour is only observable across two runs at the
**same** prompt state.

**Non-ASCII phrases coming back mangled.** Showing the model `\uXXXX` escapes
made it echo them back corrupted — 17 of 17 non-ASCII phrases. Fixed with
`ensure_ascii=False` when serializing.

**Substring matching for short words.** `Lead` matched `Leader`, `leading`: 52
hits, 0 real. Fixed by the word-boundary rule.

**Publishing prompts is the user's job, never the tooling's.** And there is a
sharp edge: `publish` ships **every** static prompt whose text differs from its
recorded pin. So you must never leave two prompt changes pending at once, or
publishing one ships the other and your A/B measures two variables.

**Prompt/pin drift.** `assemble_prompts.py check` fails if a rendered prompt was
hand-edited, or if either kind of prompt was edited but never published. The
unpublished case bit the project on 2026-08-11 — edits never left the machine
while the pipeline read the superseded S3 copy as "latest" — and stayed
half-closed until 2026-08-23, when catalog prompts were finally compared against
their record too.

**Analysis traps recorded from real runs**, worth repeating because they are easy
to fall into again:

- Pair dump records by *(subject, field, chunk_bounds, group_id)*. 242 group ids
  appear in **both** chunks of a field, so a key without chunk bounds silently
  drops 114 records — which hid an entire class of regression.
- When sweeping for invented entity names, count the **location** text as
  evidence, not just the snippet. Ignoring locations produced 9 and 19 false hits
  in one analysis.
- Deleting stored batch requests before a run is neither free nor necessary.
  Search and the single-stage fields replay from MongoDB every run — worth $1.44
  on a two-subject run.

---

## 11. What is switched on right now

As of 2026-08-24, branch `new-ground-truth-v2`, commit `29d2167`:

| Thing | State |
|---|---|
| Chunking | 20,000 tokens, max 2 chunks, 0% overlap, `search_divisor=4` |
| Page alignment + legal-page trimming | **on** for all six phrase fields |
| Recursive search | **off** (`max_rounds=0`) for every field |
| v2 relationship stage | **retired** — no longer built by the factory; classes kept only to read old stored runs |
| Mention collection | code collects, model locates; retry pass built |
| Synthesis | on, `include_location=True`, `max_entries=50`, retry pass built |
| Downstream tail | re-keyed onto `{focal_form, synthesis}` group records |
| Retry passes on grounding + screening | built, **never fired live** |
| The `loc=1` vs `loc=0` A/B | **never run** — every run so far is the same arm |
| Seven screening prompts | rendered but **UNPUBLISHED**; `check` fails on exactly those seven |
| Screening | not exercised by any run so far — the last runs used `StageToggles().stop_after(PipelineStage.synthesis)`. That line is currently **commented out** in the notebook, so as the notebook stands the full chain would run |
| Everything in v3 phase 3.3 | unit-tested (1,274 tests green) but **never run live** |
| `wipe_down` | body commented out — nothing is cleaned up after reconcile |

The next step recorded in `PIPELINE_V3_PLAN.md` is: publish the seven screening
prompts, enable screening, and drive the **first end-to-end run** through the
notebook. That run would be the first ever to exercise v3's grounding → screening
→ reconcile tail.

---

## 12. How you actually run it

### In production

`new_scrape_queue_bot.py` pulls a company off an SQS queue, scrapes it, uploads
the text to S3, and pushes the etld1 onto the extraction queue.
`new_extract_queue_bot.py` pulls from there and calls
`orchestrator.process_manufacturer(..., eager=True)`.

Two guards before extraction: text shorter than 50 tokens or longer than 125,000
tokens is tagged `INVALID_ITEM` and skipped.

### For development

`apps/data_etl_app/src/data_etl_app/scripts/mfg_extraction_test.ipynb`.

The shape of it:

1. Load environment, initialize AWS clients and MongoDB.
2. Build `PromptService` (pins prompt versions) and load the latest ontology.
3. Pick a model (`GPT_4_1` at the moment) and model params (temperature 0,
   `seed=12345`, `max_completion_tokens=20_000`).
4. List the companies to sweep in `MFG_ETLD1S`.
5. **One `RUN_TIMESTAMP` for the whole sweep**, so every company's dump lands in
   a single `logs/extraction_dumps/<ts>/` folder. One folder is then one arm of
   an A/B across N subjects, instead of N unrelated runs to collate by hand.
   Custom ids are prefixed by company, so companies cannot collide inside it.
6. Optionally set `stage_toggles = StageToggles().stop_after(PipelineStage.synthesis)`
   to stop early and pay only for the stages you are iterating on.
7. Build **one** orchestrator (it holds no per-company state) and loop, catching
   exceptions per company so one blow-up doesn't cost the rest of the sweep.

`prepare_manufacturer()` resets each company first: find or create the subject,
reset its LLM-extracted fields, drop the deferred document. Without those resets
a re-run measures what the previous run left behind instead of the prompts under
test.

### Reading the results

- **Dumps:** `packages/logs/extraction_dumps/<timestamp>/<etld1>__<field>.json`
  — one row per group, with the provenance header, per-request token usage and
  timings, and every stage's verdict joined on `group_id`.
- **Final results:** on the `Manufacturer` document in MongoDB.
- **Evidence write-ups:** `pipeline_v3_evidence/<date>_run_<id>_<topic>/README.md`
  — the analysis of each run, with its A/B numbers.
- **The plan and journal:** `PIPELINE_V3_PLAN.md`, whose `### RESUME HERE` block
  at the top is always the live state.

---

## Appendix — the chains at a glance

```
SINGLE-STAGE   (addresses, business_desc, is_manufacturer,
                is_product_manufacturer, is_contract_manufacturer)

   prefill ──► single_stage_extraction ──► reconcile


KEYWORD        (products, contract_products, equipments)

   prefill ──► phrase_search ──► recursive_search ──► mention_collection
                                    (off: 0 rounds)      + aggregation fold
                                                              │
                              ┌───────────────────────────────┘
                              ▼
                          synthesis ──► freehand_grounding ──► screening ──► reconcile


CONCEPT        (conformity_attestations, industries, process_caps, material_caps)

   prefill ──► phrase_search ──► recursive_search ──► mention_collection
                                    (off: 0 rounds)      + aggregation fold
                                                              │
                              ┌───────────────────────────────┘
                              ▼
                          synthesis ──► initial_grounding ──► oov_grounding ──► screening
                                         (in-vocabulary)      (optional;         │
                                                               empty when off)   │
                              ┌────────────────────────────────────────────────  ┘
                              ▼
                     iterative_grounding ──► reconcile
                          (descent)
```

Shared between `products` and `contract_products`: `phrase_search`,
`recursive_search`, `mention_collection`, `synthesis` — same custom ids, one set
of model calls. Separate: `freehand_grounding`, `screening`, `reconcile`.
