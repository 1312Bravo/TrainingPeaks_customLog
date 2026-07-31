from enum import StrEnum

class CardType(StrEnum):
    MACRO = "macro"
    MEZZO = "mezzo"
    MICRO = "micro"
    SESSION = "session"

class CardRelationship(StrEnum):
    PARENT = "parent"
    CHILD = "child"
    PREVIOUS = "previous"
    NEXT = "next"
    ALTERNATIVE = "alternative"
    SUPPORT = "support"

class TrainingLevel(StrEnum):
    ALL = "all"
    BEGINNER = "beginner"
    INTERMEDIATE = "intermediate"
    ADVANCED = "advanced"
    ELITE = "elite"
