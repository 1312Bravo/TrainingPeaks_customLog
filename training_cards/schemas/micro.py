from __future__ import annotations
from dataclasses import dataclass, field
from .base import BaseTrainingCard
from .enums import CardType

@dataclass(slots=True)
class MicroCard(BaseTrainingCard):
    recommended_duration_days: str = ""
    week_structure: list[str] = field(default_factory=list)
    key_sessions: list[str] = field(default_factory=list)
    load_pattern: str = ""
    parent_macro_options: list[str] = field(default_factory=list)
    parent_mezzo_options: list[str] = field(default_factory=list)
    placement_guidance: list[str] = field(default_factory=list)
    recovery_requirements: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.card_type != CardType.MICRO:
            raise ValueError("MicroCard card_type must be CardType.MICRO.")
