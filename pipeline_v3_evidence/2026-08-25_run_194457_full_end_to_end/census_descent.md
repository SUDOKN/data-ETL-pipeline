# Census — recursive descent (iterative grounding), run 20260825T194457

**Method: every hop read and classified by hand. No regex was used for any judgment.**
Enumerated with a script from `lvl_by_lvl_itps` across all 14 dumps: 600 concept-node
instances, of which **128 are unique descent hops** (`origin=recursive_grounding`,
parent set, deduplicated across chunk twins). All 128 were read individually —
parent→child, the RGR-E1 evidence explanation, the RGR-M1/M2 match explanation, and
the record's synthesis. The other 465 node instances are seed tags from
initial/freehand grounding (no descent decision) and 5 dual-origin nodes.

## Headline

| code | meaning | count | share |
|---|---|---:|---:|
| D | direct — the child concept is named or unambiguously described in the synthesis | 73 | 57% |
| N | normalized — child is the nearest vocabulary form of what the record says | 1 | 1% |
| B | **bridge** — the hop needs an inference step the synthesis does not state | 35 | 27% |
| X | **axis error** — the child is on the wrong axis for the field | 18 | 14% |
| P | **party error** — the process belongs to another actor | 1 | 1% |
| | **defective (B+X+P)** | **54** | **42%** |

Per field:

| dump | hops | D+N | B | X | P | defective |
|---|---:|---:|---:|---:|---:|---:|
| alecmfg process_caps | 62 | 45 | 17 | 0 | 0 | 27% |
| alecmfg + steelcraft industries | 31 | 8 | 5 | 18 | 0 | **74%** |
| alecmfg + steelcraft material_caps | 18 | 13 | 5 | 0 | 0 | 28% |
| steelcraft process_caps | 17 | 8 | 8 | 0 | 1 | **53%** |

## THE STRUCTURAL FINDING: descent is the only LLM stage with nothing downstream of it

`PipelineStage` ranks screening at 8 and `iterative_grounding` at 9
(`pipeline_stage.py`, "recursive descent runs post-screening"). Measured
consequence across the run: **134 of 135 descent-produced tags are absent from
their row's `screening` map — 99.3% unscreened.** The screen vets the SEED
concept and then descent walks below it unchecked, so the pipeline's most
specific (and therefore most falsifiable) claims are the only ones nothing
vets. Every defect below reached the output unopposed.

Compounding it: descent also inflates. 436 rows carry a trail; they emit 600
concept instances (mean 1.38, max 8). One anodizing mention on `gxlo64tk`
becomes five tags — Surface Finishing → Coating → Chemical Coating → Anodizing
→ Decorative Anodizing; `ga39any9` yields Conventional + CNC + Five Axis +
Precision + Specialized Machining from one record.

## Named bridge families (counted, not sampled)

1. **`Painting → Wet Painting`, "liquid" inserted — 7 of 7 hops** (steelcraft
   process). The record says "clear coat baked on" / "baked-on rust inhibiting
   primer" / "custom colors"; every explanation supplies the word *liquid* to
   satisfy Wet Painting's definition. The vocabulary offers only Wet vs Powder
   below Painting and the text never says which, so the hop cannot be made
   honestly — it is a **vocabulary-shape-forced bridge**, not a model whim.
2. **`Anodizing → Decorative Anodizing`, "aesthetic" inserted — 9 hops, 5 of
   them bridges** (alecmfg). The records state functional purposes: "corrosion
   resistance and thermal stability" (g3dg00wo, gn3tvhkq), "clear anodized
   (Type II) with **masked grounding pads**" (gxlo64tk — an electrical
   requirement), "natural anodizing … Ra ≤ 1.6 μm to support thermal coating"
   (gybnb00a). Same forced-choice shape: Decorative appears to be the only
   reachable anodizing leaf.
3. **difficulty ⇒ `Specialized Machining` — 5 hops** (g2wuddk9, g8vw1ocy,
   g9c11j0e, ga39any9, gna4icy6). Pattern verbatim: "Machining oxygen-free
   copper … is a specialized process due to the material's properties." Hard
   material is treated as proof of a distinct capability class.
4. **own activity ⇒ industry served — 18 hops, 100% of that subtree** (alecmfg
   industries). Every `Industrial Machinery and Equipment → Machine Tools /
   Manufacturing Equipment` hop reasons from Alec's OWN processes: "'We're more
   than a machine shop' … machine shops are characterized by the use of machine
   tools" (gvsg9rsy); focal forms include `CNC Machining`, `prototyping`,
   `3D Printing`, `production`, `machine shop`, and one from an **author bio**
   (gwui1coh). This is the IGR-M2 leak descending one level deeper.
   Note `gjjkomsh`: its true industry, `Industrial Automation`, was DROPPED as
   non-vocabulary — and descent then filled the gap with the subject's own
   process. A vocabulary hole and an axis leak compounding.
5. **facility type ⇒ equipment sector — 5 hops** (steelcraft industries): every
   `Healthcare and Medical Devices → Healthcare Equipment` hop argues that
   doors used in hospitals are "support products used in care-delivery
   environments". Doors sold to hospitals do not put the maker in the
   healthcare-equipment sector.
6. **comparison material treated as used — 3 hops** (steelcraft grmn3z4h,
   `Polymer → Plastic → Thermoplastic → PVC`). The site says its FT separator is
   "more durable … compared to **traditional vinyl separators**" — vinyl is what
   Steelcraft REPLACED. Screening had already failed the sibling `Plastic` tag
   on this record; descent, running after the screen, walked the same anti-
   evidence down to `PVC` anyway. This is the clearest single proof that the
   post-screening position of descent is load-bearing.
7. **finish spec ⇒ performed process — 2 hops** (g04y5dft `#2B smooth rolled
   mill finish` → Surface Preparation, which is the steel MILL's process, coded
   P; gxfzf496 `#4 Brushed Satin` → Mechanical Polishing).
8. **standard compliance ⇒ process — 1 hop** (g4rno1vt): "ANSI/SDI A250.10
   (**Prime Paint**)" is a compliance claim; descent converted it into an
   observed Wet Painting operation.

## Unexpected behaviours the taxonomy did not anticipate

- **Idiom read as evidence** (gsqiy4oj): "Alec Model uses '**cutting-edge**
  machines' for this purpose" is cited as evidence for `Precision Metal
  Cutting`. A marketing idiom entered a capability claim.
- **A technically wrong equivalence stated confidently** (gxensd0m): "'Snap-fit'
  assembly refers to joining components by pressing … exactly matches the
  definition of Press Fitting." Snap-fit (elastic feature engagement) and press
  fit (interference) are different joining methods.
- **Self-declared uncertainty that still grounds** (gtaig6fy): "While the
  specific material is not named, the context of medical device manufacturing …"
  → `Precision Metal Cutting`. RGR-E1 records its own evidence gap and satisfies
  anyway.
- **Probabilistic class assignment** (gj6o4dlw): "it is a class that is **most
  often** non-ferrous" → `Non-Ferrous Alloy`.

## What descent gets right

The sound 57% is genuinely good and worth protecting: acronym and designation
resolution is excellent (`SLS`→Selective Laser Sintering, `SLA`→Stereolithography,
`4140`→Low Alloy Steel, `EN AW-6082 T6`→Wrought Aluminum Alloy, `Ti6Al4V`→Ti-6Al-4V,
`A60 galvannealed`→Galvanized Steel), and multi-level machining chains on explicit
text (`CNC plasma and mill` → Conventional Machining → CNC Machining → Multipoint
Cutting → Milling → CNC Milling) are correct at every level. alecmfg material_caps
descent is 8/9 sound. The failure is concentrated where the vocabulary forces a
choice the text does not support, and where the field's axis is already leaking
upstream.

## Fixes this census supports

1. **Screen the descendants** — either move `iterative_grounding` above
   `screening`, or run a second screening pass over descent output. 99.3%
   unscreened is not a defensible terminal state for the stage that emits the
   most specific claims. (Cheapest variant: screen only hops whose RGR-E1 does
   not quote the record.)
2. **Add a "stop at the parent" outcome.** Families 1 and 2 exist because the
   ontology's leaves force a distinction the source text never makes. Descent
   needs an explicit, rewarded option to remain at `Painting` / `Anodizing`
   when no evidence discriminates the children.
3. **RGR-E1 must quote.** Require the child concept's discriminating feature to
   appear in the record; ban the inserted-property pattern ("liquid",
   "aesthetic", "specialized because the material is hard"). The 4 unexpected
   behaviours above all fail a quote test.
4. **Do not descend a tag the screen failed** (grmn3z4h), and do not descend on
   the subject's own activity in `industries` — fixing IGR-M2 upstream removes
   18 of the 54 defects by itself.
