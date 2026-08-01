from __future__ import annotations
from dataclasses import asdict
from typing import Any

from training_cards.schemas import (
    BaseTrainingCard,
    CardReference,
    CardRelationship,
    CardType,
    MacroCard,
    MezzoCard,
    MicroCard,
    SessionCard,
    SessionPart,
    TrainingLevel,
)

CARD_CLASS_BY_TYPE = {
    CardType.MACRO: MacroCard,
    CardType.MEZZO: MezzoCard,
    CardType.MICRO: MicroCard,
    CardType.SESSION: SessionCard,
}

# Convert a validated card object into plain JSON-safe data.
def card_to_dict(card: BaseTrainingCard) -> dict[str, Any]:
    data = asdict(card)

    data["card_type"] = str(card.card_type)
    data["suitable_levels"] = [str(level) for level in card.suitable_levels]
    data["references"] = [
        {
            "card_id": reference.card_id,
            "relationship": str(reference.relationship),
            "tags": reference.tags,
        }
        for reference in card.references
    ]
    if isinstance(card, SessionCard):
        data["workout_parts"] = [
            {
                "name": part.name,
                "duration": part.duration,
                "rpe": part.rpe,
                "instructions": part.instructions,
                "terrain_notes": part.terrain_notes,
            }
            for part in card.workout_parts
        ]

    return data

# Convert JSON data back into the correct card dataclass.
def card_from_dict(data: dict[str, Any]) -> BaseTrainingCard:
    card_data = dict(data)
    card_type = CardType(card_data["card_type"])

    card_data.setdefault("slug", card_data["id"].replace("_", "-"))
    card_data["card_type"] = card_type
    card_data["suitable_levels"] = [TrainingLevel(level) for level in card_data["suitable_levels"]]
    card_data["references"] = [
        CardReference(
            card_id = reference["card_id"],
            relationship = CardRelationship(reference["relationship"]),
            tags = reference.get("tags", []),
        )
        for reference in card_data.get("references", [])
    ]
    if card_type == CardType.SESSION:
        card_data["workout_parts"] = [
            SessionPart(
                name = part["name"],
                duration = part["duration"],
                rpe = part["rpe"],
                instructions = part.get("instructions", ""),
                terrain_notes = part.get("terrain_notes", ""),
            )
            for part in card_data.get("workout_parts", [])
        ]

    card_class = CARD_CLASS_BY_TYPE[card_type]

    return card_class(**card_data)

