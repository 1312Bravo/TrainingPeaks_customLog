from enum import StrEnum

class CardType(StrEnum):
    MACRO = "macro"
    MEZZO = "mezzo"
    MICRO = "micro"

class TrainingLevel(StrEnum):
    ALL = "all"
    BEGINNER = "beginner"
    INTERMEDIATE = "intermediate"
    ADVANCED = "advanced"
    ELITE = "elite"
