from __future__ import annotations
from dataclasses import dataclass, field
from .enums import CardRelationship

# ----------------------------------------------------------
# Card Graph References
# ----------------------------------------------------------
# Card files stay grouped by type, not nested by plan hierarchy. References
# connect them into parent/child, sequencing, alternative, and support paths.

@dataclass(slots=True)
class CardReference:
    card_id: str
    relationship: CardRelationship
    tags: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.card_id.strip():
            raise ValueError("Card reference card_id cannot be empty.")
