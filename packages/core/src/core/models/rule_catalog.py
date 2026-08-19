from __future__ import annotations

from datetime import datetime
from typing import Iterator, Literal, Optional

from pydantic import BaseModel, ConfigDict, model_validator

from core.models.extraction_schemas.grounding import SENTINEL_GROUNDING_LABELS

# Pipeline stages that report applied rules. A catalog's ``stage`` is one of
# these, and ``(stage, field_type)`` identifies exactly one prompt.
STAGE_RELATIONSHIP_SCREENING = "phrase_relationship_screening"
STAGE_INITIAL_GROUNDING = "phrase_initial_grounding"
STAGE_RECURSIVE_GROUNDING = "phrase_recursive_grounding"
STAGE_FREEHAND_GROUNDING = "phrase_freehand_grounding"
# Whole-text yes/no questions (is_manufacturer etc.). Structurally these are
# screening applied to the site instead of to a phrase — one candidate entity
# judged through a chained conjunction — so they share the screening kinds
# (condition + guard) and derive the verdict the same way. ``field_type`` here is
# the classification name itself, which keeps (stage, field_type) one-to-one with
# a prompt.
STAGE_BINARY_CLASSIFICATION = "binary_classification"

# A rule's kind fixes both whether it is reported and when. Keeping the mapping
# here rather than in each catalog file means a catalog cannot declare a rule that
# the parser would then refuse to accept.
RuleKind = Literal["condition", "guard", "preference", "quality", "format", "note"]
ReportWhen = Literal["always", "on_violation", "when_chosen", "never"]
Combinator = Literal["all", "any", "ordered", "note"]

REPORT_WHEN_BY_KIND: dict[str, ReportWhen] = {
    "condition": "always",
    "quality": "always",
    "format": "always",
    "guard": "on_violation",
    "preference": "when_chosen",
    "note": "never",
}

# Guidance folded into a parent rule. Rendered as a sub-bullet, never reported,
# and therefore carries no outcome vocabulary.
NOTE_KIND = "note"

# There is deliberately no "exception" kind. A carve-out from a condition is just a
# named region of that condition's failure, which is what a guard already is, and
# expressing it as a nested rule with inverted polarity put a member into an
# all-must-hold section whose "satisfied" meant reject. Screening now states the
# carve-out as its own positive condition instead (SCR-3: the relationship is the
# manufacturer's), so every member of a conjunction reads the same way.

# Rules reported only on one outcome can only ever carry that outcome — the parser
# rejects the rest (see ``validate_applied_rules``). A catalog that declares more
# puts values in the rendered prompt that are guaranteed parse failures on arrival,
# so the vocabulary is pinned to what is actually reachable.
ONLY_REACHABLE_OUTCOME_BY_REPORT_WHEN: dict[str, str] = {
    "on_violation": "violated",
    "when_chosen": "chosen",
}


class RuleNode(BaseModel):
    """One rule in a catalog. ``id`` is stable and position-independent, so display
    numbering can be reordered without invalidating stored ``applied_rule`` records."""

    model_config = ConfigDict(extra="forbid")

    id: str
    kind: RuleKind
    reportable: bool
    report_when: ReportWhen
    text: str
    combinator: Optional[Combinator] = None
    children: list["RuleNode"] = []

    @model_validator(mode="after")
    def check_reporting_matches_kind(self) -> "RuleNode":
        expected_reportable = self.kind != NOTE_KIND
        if self.reportable != expected_reportable:
            raise ValueError(
                f"rule {self.id}: kind={self.kind!r} implies reportable="
                f"{expected_reportable}, got {self.reportable}"
            )

        expected_report_when = REPORT_WHEN_BY_KIND[self.kind]
        if self.report_when != expected_report_when:
            raise ValueError(
                f"rule {self.id}: kind={self.kind!r} implies report_when="
                f"{expected_report_when!r}, got {self.report_when!r}"
            )
        return self

    def walk(self) -> Iterator["RuleNode"]:
        """Depth-first over this rule and its children."""
        yield self
        for child in self.children:
            yield from child.walk()


class RuleSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    section_id: str
    heading: str
    combinator: Combinator
    rules: list[RuleNode]


class EntityRelationships(BaseModel):
    """The relationship the manufacturer must be shown to have with the entity,
    in the three grammatical forms the rule text needs."""

    model_config = ConfigDict(extra="forbid")

    base: str
    third_person: str
    gerund: str


class PublishedRecord(BaseModel):
    """Where the rendered prompt currently lives in S3. Written back by the
    assemble-prompts CLI on publish, and excluded from the render input so that
    recording it can never change what renders next time."""

    model_config = ConfigDict(extra="forbid")

    s3_version_id: Optional[str] = None
    rendered_sha256: Optional[str] = None
    uploaded_at: Optional[datetime] = None


class RuleCatalog(BaseModel):
    model_config = ConfigDict(extra="forbid")

    catalog_version: str
    prompt_name: str
    stage: str
    field_types: list[str]
    entity_noun: str
    entity_relationships: EntityRelationships
    outcome_vocab: dict[str, list[str]]
    sections: list[RuleSection]
    published: PublishedRecord

    # What an explanation is asked to cite — rendered into the report block and
    # into the example's explanation slots. The multi-stage prompts judge a phrase
    # against its relationship summary; binary classification judges the scraped
    # text itself, and an instruction to cite a relationship summary that the
    # request does not carry would teach the model to invent one.
    evidence_source: str = "the phrase and its relationship summary"

    # What the matching rules compare an identified entity AGAINST — the option
    # side of the comparison. It exists because the two option-list stages are not
    # handed the same payload: initial grounding receives an outline of labels and
    # nothing else, while recursive grounding receives each child concept with its
    # SKOS definition attached (ConceptJSONEncoder). Naming a definition the request
    # never carried is worse than naming nothing: asked to consult a definition it
    # does not have, the model confabulates one, and it will confabulate whichever
    # definition justifies the match it already favoured. Wording that follows the
    # payload keeps the rule honest, and flipping this one line is what a stage
    # needs when its payload gains definitions.
    option_evidence: str = "what the option names"

    # The reserved label this prompt's escape-hatch branch tells the model to
    # return, for the catalogs that have one — recursive grounding and freehand.
    # Initial grounding has no escape hatch and leaves this unset.
    #
    # It exists so the literal in the rule text can be checked against the constant
    # the parser matches on. Those are two copies of one string in two files, and a
    # prompt that says "None of these" while the code looks for "None of the above"
    # fails silently: the sentinel stops being recognised and flows into the results
    # as if it were a discovered label.
    sentinel_tag: Optional[str] = None

    @model_validator(mode="after")
    def check_ids_and_vocab(self) -> "RuleCatalog":
        seen: set[str] = set()
        kinds: set[str] = set()
        for rule in self.walk_rules():
            if rule.id in seen:
                raise ValueError(f"{self.prompt_name}: duplicate rule id {rule.id!r}")
            seen.add(rule.id)
            kinds.add(rule.kind)

        reportable_kinds = kinds - {NOTE_KIND}
        missing = reportable_kinds - set(self.outcome_vocab)
        if missing:
            raise ValueError(
                f"{self.prompt_name}: rules use kinds {sorted(missing)} with no "
                f"outcome_vocab entry"
            )

        unused = set(self.outcome_vocab) - reportable_kinds
        if unused:
            raise ValueError(
                f"{self.prompt_name}: outcome_vocab declares {sorted(unused)} but no "
                f"rule uses those kinds"
            )

        for kind in sorted(reportable_kinds):
            forced = ONLY_REACHABLE_OUTCOME_BY_REPORT_WHEN.get(
                REPORT_WHEN_BY_KIND[kind]
            )
            if forced is not None and self.outcome_vocab[kind] != [forced]:
                raise ValueError(
                    f"{self.prompt_name}: {kind!r} rules are reported only when "
                    f"{forced!r}, so {forced!r} is the only outcome they can carry, "
                    f"but outcome_vocab declares {self.outcome_vocab[kind]}. The "
                    f"extra values would be rendered into the prompt as allowed and "
                    f"then rejected at parse time."
                )
        return self

    @model_validator(mode="after")
    def check_sentinel_tag_is_spelled_in_a_rule(self) -> "RuleCatalog":
        if self.sentinel_tag is None:
            return self

        if self.sentinel_tag not in SENTINEL_GROUNDING_LABELS:
            raise ValueError(
                f"{self.prompt_name}: sentinel_tag {self.sentinel_tag!r} is not a "
                f"reserved grounding label. The parser only recognises "
                f"{sorted(SENTINEL_GROUNDING_LABELS)}, so anything else would reach "
                f"the results as a discovered label."
            )

        # Exactly one PREFERENCE rule must spell it, because that is the branch that
        # tells the model to return it and there is only ever one escape hatch. Other
        # kinds may mention it freely — a formatting rule exempting the sentinel from
        # its own layout requirement is the case that proved this.
        #
        # Verbatim and case-sensitive: is_sentinel_grounding_label casefolds, but the
        # prompt should still show the model the exact spelling rather than lean on
        # that.
        branches = [
            rule.id
            for rule in self.walk_rules()
            if rule.kind == "preference" and self.sentinel_tag in rule.text
        ]
        if len(branches) != 1:
            raise ValueError(
                f"{self.prompt_name}: sentinel_tag {self.sentinel_tag!r} must be "
                f"spelled verbatim in exactly one preference rule — the branch that "
                f"instructs the model to return it — so the prompt and the parser "
                f"agree on it; found it in {branches or 'no preference rules'}"
            )
        return self

    @model_validator(mode="after")
    def check_option_evidence_is_declared_where_it_is_used(self) -> "RuleCatalog":
        """A catalog whose rule text spells {{option_evidence}} must state its value.

        Unlike evidence_source — consumed by the assembler when it builds the report
        block, and never visible in a rule — this token appears in the rules
        themselves. Leaving it on the model default renders correctly and still fails
        the reader: opening the catalog shows a placeholder with no answer anywhere in
        the file, which is exactly how the initial grounding catalogs looked on
        2026-08-18. The default stays as a floor for catalogs that never mention it.
        """
        token = "{{option_evidence}}"
        users = [rule.id for rule in self.walk_rules() if token in rule.text]
        if users and "option_evidence" not in self.model_fields_set:
            raise ValueError(
                f"{self.prompt_name}: {users} spell {token}, so this catalog must "
                f"declare `option_evidence` rather than inherit the default "
                f"({self.option_evidence!r}). What the matching rules compare "
                f"against is a property of the payload this stage is sent, and a "
                f"reader of the catalog has to be able to see which one it gets."
            )
        return self

    def walk_rules(self) -> Iterator[RuleNode]:
        """Depth-first over every rule in every section, children included."""
        for section in self.sections:
            for rule in section.rules:
                yield from rule.walk()

    def rules_by_id(self) -> dict[str, RuleNode]:
        return {rule.id: rule for rule in self.walk_rules()}

    def always_reported_rule_ids(self) -> set[str]:
        """Rules a response MUST carry regardless of outcome. Guards appear only
        when violated and preferences only when chosen, so neither is required."""
        return {
            rule.id for rule in self.walk_rules() if rule.report_when == "always"
        }

    def valid_outcomes(self, rule_id: str) -> set[str]:
        rule = self.rules_by_id().get(rule_id)
        if rule is None:
            raise KeyError(f"{self.prompt_name}: unknown rule id {rule_id!r}")
        return set(self.outcome_vocab.get(rule.kind, []))


RuleNode.model_rebuild()
