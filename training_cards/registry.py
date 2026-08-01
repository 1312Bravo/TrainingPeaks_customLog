from __future__ import annotations

from training_cards.cloud_config import GOOGLE_DRIVE_LIBRARY
from training_cards.json_store import load_card_library_from_json
from training_cards.schemas import BaseTrainingCard, CardType

# ----------------------------------------------------------
# Active Card Registry
# ----------------------------------------------------------
# Cloud JSON is the source of truth. The registry loads the downloaded local
# cache, which should be refreshed from Google Drive when card content changes.


# Load and validate the active JSON card library.
def load_active_cards() -> list[BaseTrainingCard]:
    try:
        return load_card_library_from_json(GOOGLE_DRIVE_LIBRARY.local_cache_dir)
    except FileNotFoundError as error:
        raise RuntimeError(
            "Training card JSON cache is missing. Run: py -m training_cards.scripts.download_cloud_library"
        ) from error


ALL_CARDS = load_active_cards()
CARD_BY_ID = {card.id: card for card in ALL_CARDS}


# Return a single card by stable ID.
def get_card(card_id: str) -> BaseTrainingCard:
    try:
        return CARD_BY_ID[card_id]
    except KeyError as error:
        raise KeyError(f"Unknown training card id: {card_id}") from error


# Return cards from one planning level, such as macro or session.
def get_cards_by_type(card_type: CardType) -> list[BaseTrainingCard]:
    return [card for card in ALL_CARDS if card.card_type == card_type]


# Return cards that include a free-text tag.
def get_cards_by_tag(tag: str) -> list[BaseTrainingCard]:
    return [card for card in ALL_CARDS if tag in card.tags]


# Follow all structured references from one card to the actual card objects.
def get_referenced_cards(card: BaseTrainingCard) -> list[BaseTrainingCard]:
    return [CARD_BY_ID[reference.card_id] for reference in card.references]
