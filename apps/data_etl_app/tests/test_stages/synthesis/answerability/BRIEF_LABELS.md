# Addendum to BRIEF.md — grading the LABELS (runs on the label wire, from 20260913T170246 on)

Since 2026-09-13 every synthesis answer carries four LABELS the model decided BEFORE writing the paragraph
(synthesis design doc §33): `doer` — who the snippets give the doing to: "the manufacturer" / "another party" /
"nobody" / "not shown"; `doer_name` — the other party as the text names it ("unnamed" for a bare they/their);
`capacity` — the manufacturer's own dealing in the capacity the snippets fix, one of ten generic values
("makes, performs, or provides it as its own"; "works on it to another party's order or specification"; "lists,
carries, represents, resells, or distributes what another party makes or does"; "services, tests, inspects, or
installs it"; "uses it as an input, tool, material, or machine"; "supplies into or serves it"; "holds, is certified
to, or claims to meet it"; "arranges for another party to perform it"; "unstated"; "none"); `dealing_words` — the
snippets' own words for the dealing. Each sample row carries them under `labels`.

Grade the paragraph on the eight axes EXACTLY as BRIEF.md says (paragraph first, then the snippets). THEN grade
the labels, against the SNIPPETS, and add one object to the row:

```json
"labels": {"C": {"mark": "carried|contradicted|na", "quote": "", "note": ""},
           "D": {"mark": "carried|contradicted|na", "quote": "", "note": ""},
           "consistent": true}
```

- `labels.C` = the `doer` (and `doer_name`) against the snippets. `carried`: the snippets give the doing to that
  party (the manufacturer's own first person or name → "the manufacturer"; a they/their/named other company →
  "another party" with the right name; a definition/explainer/how-to/party-less sentence → "nobody"; the entity only
  named or listed → "not shown"). `contradicted`: the snippets give it to someone else (the classic case: another
  party's "They offer…" labelled "the manufacturer"; the manufacturer's own "we manufacture" labelled "nobody").
  `na`: the snippets do not settle who does it.
- `labels.D` = the `capacity` against the snippets. `carried`: the snippets fix that capacity, or they show a
  dealing without fixing it and the label is "unstated", or they show no dealing of the manufacturer's and the
  label is "none". `contradicted`: the snippets fix a different capacity, or fix none and the label supplies one
  (a bare line on a line card labelled "makes … as its own"), or show a plain own dealing and the label says
  "none"/"unstated". Apply the frame ruling of BRIEF.md's axis D: the manufacturer's own product/service/
  materials/capabilities page fixes "as its own" for a bare item; a line card, a represented maker's page, a
  document library or an explainer does not.
- `labels.consistent` = whether the PARAGRAPH says the same thing the labels say (same party, same capacity);
  `false` where they disagree (e.g. label "another party" but the paragraph writes the dealing as the
  manufacturer's own, or label "unstated" but the paragraph asserts "makes"). Quote the paragraph words in
  `labels.D.note` or `labels.C.note` when `false`.
- `quote` = the deciding snippet words for `contradicted`; empty otherwise. One-sentence notes.

Output file, judge string and reply format are BRIEF.md's, with the run id in the judge string:
`"judge": "agent:claude-opus <field> run170246"`. Add to your reply: counts of labels.C and labels.D carried /
contradicted / na, the count of `consistent: false`, and the three most common ways a label was wrong, each with
one content_key.
