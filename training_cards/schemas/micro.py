from __future__ import annotations
from dataclasses import dataclass, field
from .base import BaseTrainingCard
from .enums import CardType

# ----------------------------------------------------------
# Micro Week Card
# ----------------------------------------------------------
# Micro cards describe reusable week structures inside mezzo blocks.

@dataclass(slots=True)
class MicroCard(BaseTrainingCard):
    recommended_duration_days: str = ""
    week_structure: list[str] = field(default_factory=list)
    key_sessions: list[str] = field(default_factory=list)
    load_pattern: str = ""
    placement_guidance: list[str] = field(default_factory=list)
    recovery_requirements: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        BaseTrainingCard.__post_init__(self)
        if self.card_type != CardType.MICRO:
            raise ValueError("MicroCard card_type must be CardType.MICRO.")
