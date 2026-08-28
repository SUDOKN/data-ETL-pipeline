# Boundary — what this instrument hands to its neighbours

The mention stage sits between search and synthesis, so several numbers are
visible here but answerable elsewhere. This file names them, so nothing falls
between two instruments and nothing is judged twice.

Written for the owners of `search/`, `synthesis/` and `grounding/`, who work in
parallel sessions. Shared code in the tree is the durable channel between us.

## Handed UP to the search eval — computed here, judged there

This instrument takes the sent forms as **given** and judges only what the stage
does with them. These are exported on every scorecard and gate nothing here:

| metric | why it is search's | where it appears |
|---|---|---|
| `zero_hit_forms` | a form with no occurrence is a search false positive; since the 2026-08-27 D8 reversal containment no longer swallows anything, so this is the ONLY remaining cause | window rows |
| `empty_groups` (`status: no_mentions`) | same fact at group level. The fold's own docstring now says an empty bundle is "only a search false positive" | `metrics.empty_groups` |
| junk-form composition (document titles, client products, wrong-field forms) | admission is search's decision; this stage must handle junk correctly either way | not computed here |
| form-level precision / recall | search's J1/J2 | not computed here |

**The one thing worth acting on:** empty groups changed meaning on 2026-08-27.
Before that date the overwhelming cause was longest-span containment (measured:
all 11 empty groups in one sampled field). After it, containment cannot cause
one. A trend line through that date is comparing two different quantities.

## Handed DOWN to synthesis — this stage's output is its input

| what | why it matters there |
|---|---|
| `max_entries_in_a_group`, `groups_over_50_entries` | synthesis packs at most 50 entries per request; a group past that is split across the packing boundary that produces cross-record evidence bleed (their candidate C3). The D8 reversal pushes groups over that line — `door` went 60 → 157 entries |
| the **snippet extent** judgment (S1) | synthesis's candidate C1 (fragment completion) blames upstream snippet truncation and points at this stage's clipping radius. S1 is that measurement |
| the **location correctness** judgment (S2∧S3∧S4) | synthesis judges are told to count the location as evidence ("a heading like 'More from Allegion' is provenance"). If S4 is bad, synthesis's party-attribution numbers rest on bad provenance |
| the **inheritance** judgment (I1) | a diluting inherited occurrence is evidence synthesis will faithfully aggregate into a wrong paragraph |

**Sequencing note:** a mention or location prompt edit re-digests every
synthesis request and voids synthesis's verdict cache. The user has said
redoing synthesis is acceptable, so findings are not held back — but the report
must name that cost when it recommends a prompt change.

## Handed DOWN to grounding

Grounding judges are likewise told to count the location text as evidence when a
party name appears only there. Same dependency as synthesis: S4 is upstream of
their wrong-actor numbers.

## ⚠ Action owed to the sibling harnesses — the `collapsed` status

`bbfc42b` (2026-08-27) added a third bundle status, `collapsed` (D21): a group
whose surface form is only a coordination of siblings that already hold all its
mentions keeps its row, gains `collapsed_into`, and is skipped downstream.

**No sibling harness knows it exists.**

- `grounding/checks/run_eval.py` — `KNOWN_STATUSES` is
  `{"grounded", "no_candidates", "screened_out", "no_mentions", "not_synthesized"}`.
  Every collapsed row will raise a spurious `status_accounting` finding on the
  first post-`bbfc42b` run.
- `synthesis/checks/mechanical.py` — INV-4 asserts a `no_mentions` row carries no
  synthesis. A `collapsed` row is also skipped by synthesis and wants the same
  assertion; today it is unchecked.

Both are one-line changes in their own folders, which this instrument does not
edit. Measured on run 194457: 16 of 3,309 groups collapse.

## ⚠ Owed to `_shared/text_matching` — the short-form case rule costs a real hit

Found while seeding the negative corpus (2026-08-27), on howcogroup.com:

> Our established supply chain partners provide FCAW, **TiG**, MMA & Sub Arc
> welding capabilities

`TIG` scores **zero** there. Forms of three characters or fewer are matched
case-sensitively — by `_shared/text_matching.occurs_in` and, independently, by
the pipeline's own `floor_scan` — so `TIG` does not match `TiG`.

The rule is right far more often than it is wrong: it is what stops "tight"
counting as TIG and "absolute" as ABS, and dropping it would reintroduce the
`Lead`-at-52-hits class. But it is not free, and the cost had never been
measured. This case is deliberately **not** labelled `must_not_occur`, because
that would enshrine a matcher artifact as ground truth.

Worth considering by whoever owns the shared matcher: a case-*insensitive*
match on a short form whose casing is neither all-upper nor all-lower (`TiG`,
`PhD`) is almost certainly the real token rather than a substring accident,
since the substring traps (`them` → HEM, `competitive` → PET) are lowercase runs
inside ordinary words. Not proposed as a change here — this instrument only
reports it.

## What this instrument will never claim

- Whether a form should have been extracted (search).
- Whether a synthesis paragraph is faithful (synthesis).
- Whether a tag is grounded correctly (grounding).
- Whether the ontology has the right labels (nobody's eval yet).
