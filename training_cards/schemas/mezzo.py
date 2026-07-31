from __future__ import annotations
from dataclasses import dataclass, field
from .base import BaseTrainingCard
from .enums import CardType

# ----------------------------------------------------------
# Mezzo Block Card
# ----------------------------------------------------------
# Mezzo cards describe focused blocks inside a macro phase.

@dataclass(slots=True)
class MezzoCard(BaseTrainingCard):
    recommended_duration_weeks: str = ""
    placement_guidance: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        BaseTrainingCard.__post_init__(self)
        if self.card_type != CardType.MEZZO:
            raise ValueError("MezzoCard card_type must be CardType.MEZZO.")
