from training_cards.cards.macro import MACRO_CARDS
from training_cards.cards.mezzo import MEZZO_CARDS
from training_cards.cards.micro import MICRO_CARDS
from training_cards.cards.session import SESSION_CARDS

# Keep Python seed cards available for backup/export while cloud JSON is active.
ALL_SEED_CARDS = [
    *MACRO_CARDS,
    *MEZZO_CARDS,
    *MICRO_CARDS,
    *SESSION_CARDS,
]
