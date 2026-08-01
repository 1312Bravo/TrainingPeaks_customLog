from __future__ import annotations
from dataclasses import dataclass, field
from .enums import CardType, TrainingLevel
from .references import CardReference

# ----------------------------------------------------------
# Shared Card Fields
# ----------------------------------------------------------
# BaseTrainingCard holds the coaching fields that every card type needs for
# preview, detail view, filtering, and recommendation/navigation logic.

@dataclass(slots=True)
class BaseTrainingCard:
    id: str
    slug: str
    title: str
    card_type: CardType
    suitable_levels: list[TrainingLevel]
    summary: str
    purpose: str
    detailed_description: str = ""
    tags: list[str] = field(default_factory=list)
    goal_race_context: list[str] = field(default_factory=list)
    when_to_choose: list[str] = field(default_factory=list)
    when_not_to_choose: list[str] = field(default_factory=list)
    expected_adaptations: list[str] = field(default_factory=list)
    training_characteristics: list[str] = field(default_factory=list)
    terrain_demands: list[str] = field(default_factory=list)
    common_mistakes: list[str] = field(default_factory=list)
    warning_signs: list[str] = field(default_factory=list)
    progression_rules: list[str] = field(default_factory=list)
    regression_rules: list[str] = field(default_factory=list)
    references: list[CardReference] = field(default_factory=list)

    # Keep validation light: only reject cards that cannot be identified,
    # displayed, or understood at a basic level.
    def __post_init__(self) -> None:
        if not self.id.strip():
            raise ValueError("Training card id cannot be empty.")
        if not self.slug.strip():
            raise ValueError("Training card slug cannot be empty.")
        if not self.title.strip():
            raise ValueError("Training card title cannot be empty.")
        if not self.suitable_levels:
            raise ValueError("Training card suitable_levels cannot be empty.")
        if not self.summary.strip():
            raise ValueError("Training card summary cannot be empty.")
        if not self.purpose.strip():
            raise ValueError("Training card purpose cannot be empty.")

