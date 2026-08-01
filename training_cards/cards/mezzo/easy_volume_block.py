from training_cards.schemas import CardRelationship, CardReference, CardType, MezzoCard, TrainingLevel

easy_volume_block = MezzoCard(
    id = "mezzo_001",
    slug = "easy-volume-block",
    title = "Easy Volume Block",
    card_type = CardType.MEZZO,
    suitable_levels = [TrainingLevel.ALL],
    summary = "A low-intensity block that increases training frequency or volume without adding much workout stress.",
    purpose = "Build repeatable aerobic load and basic durability while keeping recovery predictable.",
    detailed_description = (
        "This block is the simplest way to grow training capacity: run easily, repeat often, and avoid turning volume into hidden intensity. "
        "It fits well after a return phase or early in base development. "
        "For trail runners, easy volume can include gentle vertical gain and relaxed hiking, but technical descents and long steep climbs should not dominate the block."
    ),
    tags = ["easy", "volume", "aerobic", "durability"],
    goal_race_context = [
        "Useful for most race goals when consistency or weekly volume is a limiter.",
        "Can support road, trail, mountain, and mixed-surface running.",
    ],
    when_to_choose = [
        "When the athlete needs more easy running tolerance.",
        "After inconsistent training or before harder developmental work.",
        "When fatigue is low enough to add frequency or duration.",
    ],
    when_not_to_choose = [
        "Do not use if easy running is already causing persistent fatigue.",
        "Do not increase volume aggressively after injury or illness.",
    ],
    expected_adaptations = [
        "Improved aerobic consistency.",
        "Better tolerance for weekly training frequency.",
        "Basic tissue durability for later blocks.",
    ],
    training_characteristics = [
        "Mostly conversational easy running.",
        "Small increases in frequency or duration.",
        "Short relaxed strides may be included if recovery is stable.",
    ],
    terrain_demands = [
        "Prefer terrain that keeps effort easy.",
        "Trail runners can use gentle trails, but should avoid making every easy run muscularly demanding.",
    ],
    common_mistakes = [
        "Running easy days too fast.",
        "Increasing distance, vertical gain, and technical load at the same time.",
        "Skipping recovery because the sessions look easy on paper.",
    ],
    warning_signs = [
        "Easy runs feel progressively harder.",
        "Soreness changes stride or lasts into the next key session.",
    ],
    progression_rules = [
        "Increase only one load variable at a time.",
        "Hold volume steady before moving into a harder block.",
    ],
    regression_rules = [
        "Reduce run duration before cutting frequency if routine is the goal.",
        "Use flatter routes if trail load is driving fatigue.",
    ],
    references = [
        CardReference(card_id = "macro_001", relationship = CardRelationship.PARENT, tags = ["starter_block"]),
        CardReference(card_id = "macro_002", relationship = CardRelationship.PARENT, tags = ["core_block"]),
        CardReference(card_id = "mezzo_002", relationship = CardRelationship.NEXT, tags = ["natural_sequence"]),
        CardReference(card_id = "micro_003", relationship = CardRelationship.CHILD, tags = ["core_week"]),
        CardReference(card_id = "micro_001", relationship = CardRelationship.CHILD, tags = ["deload"]),
    ],
    recommended_duration_weeks = "2-6",
    placement_guidance = [
        "Place early in a macro phase or after recovery when the athlete needs routine and capacity.",
        "Use before intensity blocks if easy volume is not yet stable.",
    ],
)

