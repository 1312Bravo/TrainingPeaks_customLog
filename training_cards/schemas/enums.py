from enum import StrEnum

# ----------------------------------------------------------
# Controlled Vocabulary
# ----------------------------------------------------------
# These enums keep card data consistent when cards are filtered, exported,
# loaded from JSON, or shown in the Training Platform app.

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
