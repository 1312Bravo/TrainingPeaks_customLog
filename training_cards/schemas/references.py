from __future__ import annotations
from dataclasses import dataclass, field
from .enums import CardRelationship

@dataclass(slots=True)
class CardReference:
    card_id: str
    relationship: CardRelationship
    tags: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.card_id.strip():
            raise ValueError("Card reference card_id cannot be empty.")

