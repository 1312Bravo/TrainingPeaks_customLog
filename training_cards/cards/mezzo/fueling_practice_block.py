from training_cards.schemas import CardRelationship, CardReference, CardType, MezzoCard, TrainingLevel

fueling_practice_block = MezzoCard(
    id = "mezzo_008",
    slug = "fueling-practice-block",
    title = "Fueling Practice Block",
    card_type = CardType.MEZZO,
    suitable_levels = [TrainingLevel.ALL],
    summary = "A support block that practices carbohydrate, fluid, and equipment habits during relevant runs.",
    purpose = "Make fueling and hydration reliable before long or race-specific efforts expose problems.",
    detailed_description = (
        "Fueling practice is not only for very long races. "
        "Any runner whose goal involves sustained effort can benefit from learning what intake is tolerated at the relevant intensity and duration. "
        "For trail runners, fueling may need to work during climbs, descents, hiking, heat, cold, altitude, or technical terrain when eating and drinking are less convenient."
    ),
    tags = ["fueling", "hydration", "execution", "support"],
    goal_race_context = [
        "Useful when race duration or conditions make energy and fluid intake important.",
        "Especially useful before long endurance and race-practice blocks.",
    ],
    when_to_choose = [
        "When the athlete has GI uncertainty, inconsistent fueling, or long-race goals.",
        "When specific sessions are long enough to practice intake meaningfully.",
    ],
    when_not_to_choose = [
        "Do not make every run a fueling experiment.",
        "Do not force aggressive intake changes during illness or active GI distress.",
    ],
    expected_adaptations = [
        "Improved confidence with carbohydrate and fluid intake.",
        "Better tolerance of race-like fueling routines.",
        "Reduced risk of underfueling key long sessions.",
    ],
    training_characteristics = [
        "Practice intake during long, steady, or race-specific sessions.",
        "Test one major variable at a time.",
        "Record what worked, what failed, and under what conditions.",
    ],
    terrain_demands = [
        "Road runners may practice at race rhythm and aid-station timing.",
        "Trail runners should practice carrying, opening, eating, and drinking on variable terrain.",
    ],
    common_mistakes = [
        "Trying new products only on race day.",
        "Changing too many fueling variables at once.",
        "Practicing only in easy conditions when the race will be more complex.",
    ],
    warning_signs = [
        "Repeated GI distress despite conservative changes.",
        "Underfueling long sessions because eating feels inconvenient.",
    ],
    progression_rules = [
        "Move from simple intake practice to more race-like timing and conditions.",
        "Increase intake gradually when tolerance is uncertain.",
    ],
    regression_rules = [
        "Simplify products, timing, or dose if GI symptoms appear.",
        "Separate fueling practice from very hard sessions until tolerance improves.",
    ],
    references = [
        CardReference(card_id = "macro_004", relationship = CardRelationship.PARENT, tags = ["support_block"]),
        CardReference(card_id = "mezzo_006", relationship = CardRelationship.SUPPORT, tags = ["long_efforts"]),
        CardReference(card_id = "mezzo_007", relationship = CardRelationship.SUPPORT, tags = ["race_execution"]),
        CardReference(card_id = "micro_006", relationship = CardRelationship.CHILD, tags = ["practice_context"]),
        CardReference(card_id = "session_003", relationship = CardRelationship.CHILD, tags = ["practice_session"]),
    ],
    recommended_duration_weeks = "2-8",
    placement_guidance = [
        "Layer into long endurance or race-practice work rather than isolating it from relevant sessions.",
        "Begin early enough that adjustments can be tested before the goal event.",
    ],
)

