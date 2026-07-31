from __future__ import annotations
from dataclasses import dataclass, field
from .base import BaseTrainingCard
from .enums import CardType

@dataclass(slots=True)
class SessionCard(BaseTrainingCard):
    session_family: str = ""
    typical_duration: str = ""
    intensity_guidance: list[str] = field(default_factory=list)
    execution_notes: list[str] = field(default_factory=list)
    recovery_requirements: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.card_type != CardType.SESSION:
            raise ValueError("SessionCard card_type must be CardType.SESSION.")
