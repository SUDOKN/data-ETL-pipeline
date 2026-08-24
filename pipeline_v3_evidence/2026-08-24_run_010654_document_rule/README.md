# Eighth v3 run analyzed: 20260824T010654 — the document-listing rule

The fix for WRONG 2 of the previous run: a sentence added to *What the manufacturer does with the entity*
telling synthesis that **a document is not the thing it is about**. Same two subjects, `loc=1`, 50 entries
per request, `gpt-4.1`, temperature 0, seed 12345.

**Clean A/B.** All 95 synthesis requests carry an identical `ud=` digest and an identical id set across
`20260824T002404` and this run — only `pv=` differs (six new prompt version ids). Every difference below
is the prompt and nothing else.

Scripts: `document_rule_ab.py` / `document_rule_ab_output.txt`, run from the repo root.

## Verdict in one line
**The rule landed and the feared overreach did not happen.** On the 77 records whose only evidence is a
document title, the ambiguous *"Steelcraft provides this resource"* goes **23 → 0** and an explicitly
document-scoped claim goes **1 → 45**; **zero** conformity attestations were newly withheld; cost, length
and delivery are all flat. One real defect surfaced, and it was in the lint, not the pipeline.

## RIGHT

1. **The unscoped claim is gone: 23 → 0.** No record now says the manufacturer "provides this resource"
   about a phrase whose only evidence is a document title.
2. **The scoped claim replaced it: 1 → 45 of 77 (58%).** The stage now writes *"This shows that Steelcraft
   offers a document titled INPACT Door Systems Brochure"* where it wrote *"indicating that Steelcraft
   provides this brochure as a resource"*. The claim is pinned to the document, which is what the rule
   asked for.

   | focal form | before | after |
   |---|---|---|
   | `Falcon SZ Series` | "indicating that Steelcraft **provides this resource**" | "This shows that Steelcraft **offers a document titled** Falcon SZ Series" |
   | `Steelcraft Portfolio` | "showing that Steelcraft **provides this portfolio**" | "This shows that Steelcraft **offers a document** …" |
   | `2 Day Rapid Data Sheet` | "directing users to the data sheet" | "Steelcraft offers a document titled '2 Day Rapid Data Sheet' … **The entries show no** …" |

3. **The overreach did NOT materialise — 0 attestations newly withheld.** 165 conformity-attestation
   records; exactly 1 withholds a dealing in **both** runs, and reading it confirms the call is correct in
   both: `Steel Door Institute FEMA UL Intertek Florida Building Code`, whose evidence is a line in a list
   of **external links** under 'Allegion & Industry Links' — not a certificate. Before: *"does not describe
   any specific dealing or relationship."* After: *"does not show any further dealing with them beyond
   providing the reference."* Same verdict, better reasoning. **The rule can stay on all six statics.**
4. **Everything else is flat.** 2,133/2,133 synthesized, 0 not-synthesized, 0 retries, 0 unknown ids.
   Cost $2.42 → $2.43 (input +2.0% for the added sentence, output −0.4%). Mean synthesis length unchanged
   at 365 chars.

## WRONG

1. **The focal-form lint mishandled possessives — my defect, found and fixed in this pass.**
   The dump reported `focal_form_absent_records` 13 → 3, but those two numbers were produced by *different
   versions of the lint*, so they are not comparable. Recomputed with one version, the run flagged **3**
   rows, all `products`, all the same cause: a focal form like `Steelcraft's Express Stock program`
   normalizes to `steelcraft s express stock program`, and the bare `s` left by the apostrophe appears in
   no synthesis. It only became visible now because the prose moved from *"Steelcraft's Express Stock
   program"* (which satisfied the verbatim fast path) to *"Steelcraft operates the Express Stock program"*.

   Fixed by stripping `'s` before normalizing. Re-measured across every synthesis run on record:

   | run | flagged |
   |---|---|
   | 20260823T195031 | 1 |
   | 20260823T200044 | 1 — **the real FE→DE swap** |
   | 20260824T002404 | 1 |
   | 20260824T010654 | **0** |

## Open

- **32 of the 77 document-listing records (42%) still carry neither phrasing.** They were never the
  unscoped-claim cases, so nothing regressed — but the rule's reach on that remainder is unmeasured.
  Worth a look only if document titles turn out to survive screening in 3.3.
- The root cause is upstream and untouched: document titles are being extracted as `products` at all,
  consistent with the 37% products precision measured on 2026-08-22.
