from __future__ import annotations
from dataclasses import dataclass, field
from .base import BaseTrainingCard
from .enums import CardType

# ----------------------------------------------------------
# Macro Phase Card
# ----------------------------------------------------------
# Macro cards describe larger training phases or goal periods.

@dataclass(slots=True)
class MacroCard(BaseTrainingCard):
    recommended_duration_weeks: str = ""
    timing_guidance: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        BaseTrainingCard.__post_init__(self)
        if self.card_type != CardType.MACRO:
            raise ValueError("MacroCard card_type must be CardType.MACRO.")
