from training_cards.cards.macro import MACRO_CARDS
from training_cards.cards.mezzo import MEZZO_CARDS
from training_cards.cards.micro import MICRO_CARDS
from training_cards.cards.session import SESSION_CARDS
from training_cards.schemas import BaseTrainingCard, CardType

# ----------------------------------------------------------
# In-Memory Card Registry
# ----------------------------------------------------------
# For now the registry is built from Python seed cards. Later, this can switch
# to JSON/cloud loading while keeping the same public helpers.

ALL_CARDS = [
    *MACRO_CARDS,
    *MEZZO_CARDS,
    *MICRO_CARDS,
    *SESSION_CARDS,
]

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
