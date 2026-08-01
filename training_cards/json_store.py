from __future__ import annotations
import json
from pathlib import Path
from typing import Any

from training_cards.serialization import card_from_dict, card_to_dict
from training_cards.schemas import BaseTrainingCard

LIBRARY_ID = "running_training_cards"
SCHEMA_VERSION = "1.0.0"
LIBRARY_VERSION = "0.1.0"
LAST_UPDATED = "2026-08-01"
CARDS_ROOT = "cards"

CARD_TYPE_FOLDER = {
    "macro": "macro",
    "mezzo": "mezzo",
    "micro": "micro",
    "session": "session",
}

# Build the table of contents for the cloud/local JSON card library.
def build_manifest(cards: list[BaseTrainingCard]) -> dict[str, Any]:
    card_items = [
        {
            "id": card.id,
            "slug": card.slug,
            "card_type": str(card.card_type),
            "title": card.title,
        }
        for card in sorted(cards, key = lambda card: card.id)
    ]

    return {
        "library_id": LIBRARY_ID,
        "schema_version": SCHEMA_VERSION,
        "library_version": LIBRARY_VERSION,
        "updated_at": LAST_UPDATED,
        "cards_root": CARDS_ROOT,
        "card_count": len(card_items),
        "cards": card_items,
    }

# Write cards as one JSON file per card, grouped by planning level.
# This is used for local cache/export now and can support cloud upload later.
def export_cards_to_json(cards: list[BaseTrainingCard], output_dir: Path) -> None:
    output_dir.mkdir(parents = True, exist_ok = True)

    for card in cards:
        type_dir = output_dir / CARD_TYPE_FOLDER[str(card.card_type)]
        type_dir.mkdir(parents = True, exist_ok = True)
        output_path = type_dir / f"{card.slug}.json"
        output_path.write_text(
            json.dumps(card_to_dict(card), indent = 2, ensure_ascii = False) + "\n",
            encoding = "utf-8",
        )

# Write a complete local copy of the cloud-style library: manifest plus cards.
def export_card_library_to_json(cards: list[BaseTrainingCard], output_dir: Path) -> None:
    output_dir.mkdir(parents = True, exist_ok = True)
    manifest_path = output_dir / "manifest.json"
    cards_dir = output_dir / CARDS_ROOT

    manifest_path.write_text(
        json.dumps(build_manifest(cards), indent = 2, ensure_ascii = False) + "\n",
        encoding = "utf-8",
    )
    export_cards_to_json(cards, cards_dir)

# Load JSON card files under macro/mezzo/micro/session folders and validate
# them by rebuilding the dataclass objects.
def load_cards_from_json(input_dir: Path) -> list[BaseTrainingCard]:
    cards = []

    for path in sorted(input_dir.glob("*/*.json")):
        data = json.loads(path.read_text(encoding = "utf-8"))
        cards.append(card_from_dict(data))

    return cards

