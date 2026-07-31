from training_cards.cards.macro import MACRO_CARDS
from training_cards.cards.mezzo import MEZZO_CARDS
from training_cards.cards.micro import MICRO_CARDS
from training_cards.cards.session import SESSION_CARDS
from training_cards.schemas import BaseTrainingCard, CardType

ALL_CARDS = [
    *MACRO_CARDS,
    *MEZZO_CARDS,
    *MICRO_CARDS,
    *SESSION_CARDS,
]

CARD_BY_ID = {card.id: card for card in ALL_CARDS}

def get_card(card_id: str) -> BaseTrainingCard:
    return CARD_BY_ID[card_id]

def get_cards_by_type(card_type: CardType) -> list[BaseTrainingCard]:
    return [card for card in ALL_CARDS if card.card_type == card_type]

def get_cards_by_tag(tag: str) -> list[BaseTrainingCard]:
    return [card for card in ALL_CARDS if tag in card.tags]

def get_referenced_cards(card: BaseTrainingCard) -> list[BaseTrainingCard]:
    return [CARD_BY_ID[reference.card_id] for reference in card.references]

