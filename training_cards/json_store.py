from __future__ import annotations
import json
from pathlib import Path

from training_cards.serialization import card_from_dict, card_to_dict
from training_cards.schemas import BaseTrainingCard

CARD_TYPE_FOLDER = {
    "macro": "macro",
    "mezzo": "mezzo",
    "micro": "micro",
    "session": "session",
}

# Write cards as one JSON file per card, grouped by planning level.
# This is used for local cache/export now and can support cloud upload later.

def export_cards_to_json(cards: list[BaseTrainingCard], output_dir: Path) -> None:
    output_dir.mkdir(parents = True, exist_ok = True)

    for card in cards:
        type_dir = output_dir / CARD_TYPE_FOLDER[str(card.card_type)]
        type_dir.mkdir(parents = True, exist_ok = True)
        output_path = type_dir / f"{card.id}.json"
        output_path.write_text(
            json.dumps(card_to_dict(card), indent = 2, ensure_ascii = False) + "\n",
            encoding = "utf-8",
        )

# Load all JSON card files under macro/mezzo/micro/session folders and
# validate them by rebuilding the dataclass objects.

def load_cards_from_json(input_dir: Path) -> list[BaseTrainingCard]:
    cards = []

    for path in sorted(input_dir.glob("*/*.json")):
        data = json.loads(path.read_text(encoding = "utf-8"))
        cards.append(card_from_dict(data))

    return cards
