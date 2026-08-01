from training_cards.schemas import CardRelationship, CardReference, CardType, MezzoCard, TrainingLevel

race_practice_block = MezzoCard(
    id = "mezzo_007",
    slug = "race-practice-block",
    title = "Race Practice Block",
    card_type = CardType.MEZZO,
    suitable_levels = [TrainingLevel.ALL],
    summary = "A specific preparation block that rehearses race execution without racing every workout.",
    purpose = "Practice pacing, fueling, equipment, terrain, and mental execution for the goal event.",
    detailed_description = (
        "Race practice turns fitness into execution. "
        "The block should include enough specificity to reveal problems before race day, but not so much that every week becomes a full simulation. "
        "For trail runners, this often means practicing effort-based pacing, climbs, descents, hiking transitions, gear choices, fueling under movement, and technical confidence."
    ),
    tags = ["race_practice", "specificity", "execution", "pacing"],
    goal_race_context = [
        "Useful before goal races once general fitness is ready.",
        "Specificity should scale with race importance, distance, terrain, and athlete experience.",
    ],
    when_to_choose = [
        "When the athlete needs execution practice before a goal event.",
        "When pacing, fueling, terrain handling, or confidence are likely limiters.",
    ],
    when_not_to_choose = [
        "Do not use before the athlete has enough base to absorb specific sessions.",
        "Do not use repeated maximal simulations as normal training.",
    ],
    expected_adaptations = [
        "Improved race pacing and decision-making.",
        "Better confidence with goal-specific demands.",
        "Earlier identification of fueling, gear, or terrain problems.",
    ],
    training_characteristics = [
        "Specific long runs, controlled tune-up efforts, or segments at expected race effort.",
        "Fueling and gear practice built into key sessions.",
        "Recovery planned around specific stress.",
    ],
    terrain_demands = [
        "Match the most important race demands broadly, not every course detail.",
        "Trail runners should include representative climbing, descending, surface, and pacing variability.",
    ],
    common_mistakes = [
        "Doing too many race-like efforts.",
        "Testing new gear or nutrition too late.",
        "Mistaking specificity for maximum difficulty.",
    ],
    warning_signs = [
        "Specific sessions leave heavy fatigue for several days.",
        "Race anxiety drives extra training instead of better execution.",
    ],
    progression_rules = [
        "Progress from controlled practice to more specific rehearsal.",
        "Keep the final specific sessions confidence-building rather than destructive.",
    ],
    regression_rules = [
        "Reduce specificity if recovery or confidence worsens.",
        "Simplify terrain or shorten sessions when execution quality drops.",
    ],
    references = [
        CardReference(card_id = "macro_004", relationship = CardRelationship.PARENT, tags = ["core_block"]),
        CardReference(card_id = "macro_005", relationship = CardRelationship.NEXT, tags = ["natural_sequence"]),
        CardReference(card_id = "mezzo_008", relationship = CardRelationship.SUPPORT, tags = ["execution"]),
        CardReference(card_id = "micro_008", relationship = CardRelationship.CHILD, tags = ["core_week"]),
    ],
    recommended_duration_weeks = "2-6",
    placement_guidance = [
        "Use in the final specific preparation period before taper.",
        "Keep enough recovery margin so the athlete arrives fresh, not rehearsed into fatigue.",
    ],
)

