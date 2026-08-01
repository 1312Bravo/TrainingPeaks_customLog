from training_cards.schemas import CardRelationship, CardReference, CardType, MezzoCard, TrainingLevel

long_endurance_block = MezzoCard(
    id = "mezzo_006",
    slug = "long-endurance-block",
    title = "Long Endurance Block",
    card_type = CardType.MEZZO,
    suitable_levels = [TrainingLevel.ALL],
    summary = "A block focused on extending duration, fueling practice, and late-session durability.",
    purpose = "Prepare the athlete to handle longer efforts with stable energy, pacing, and mechanics.",
    detailed_description = (
        "Long endurance is about extending useful endurance, not simply collecting the biggest single run. "
        "The block should teach the athlete to pace, fuel, and recover from longer outings. "
        "For trail runners, duration and vertical gain may matter more than distance; hiking, poles, gear, and downhill fatigue can be trained as part of the long effort when relevant."
    ),
    tags = ["long_endurance", "time_on_feet", "fueling", "durability"],
    goal_race_context = [
        "Useful when race duration is a major limiter.",
        "Especially relevant for longer events, hilly courses, or athletes who fade late.",
    ],
    when_to_choose = [
        "When basic endurance is stable and longer efforts are the next limiter.",
        "When fueling, pacing, or late-run durability need practice.",
    ],
    when_not_to_choose = [
        "Do not use if long runs already require excessive recovery.",
        "Do not use during acute injury risk or high fatigue.",
    ],
    expected_adaptations = [
        "Improved long-duration stamina.",
        "Better fueling tolerance and pacing control.",
        "Improved muscular and mental durability late in runs.",
    ],
    training_characteristics = [
        "Long runs, extended easy-to-steady outings, or back-to-back structures.",
        "Fueling practice is part of the training, not an optional extra.",
        "Most other running supports recovery and consistency.",
    ],
    terrain_demands = [
        "Road runners may focus on duration and pace control.",
        "Trail runners may use vertical gain, hiking, descents, and varied surfaces to match goal demands.",
    ],
    common_mistakes = [
        "Making every long run a race simulation.",
        "Neglecting fueling until the longest sessions.",
        "Adding downhill load faster than tissue adaptation.",
    ],
    warning_signs = [
        "Long-run recovery takes most of the week.",
        "GI issues repeatedly prevent adequate fueling.",
        "Persistent downhill soreness or joint pain.",
    ],
    progression_rules = [
        "Progress duration, specificity, or back-to-back structure gradually.",
        "Practice fueling before the longest or most specific runs.",
    ],
    regression_rules = [
        "Shorten the long run or split load across two days if recovery is poor.",
        "Reduce vertical gain or downhill exposure if mechanical stress dominates.",
    ],
    references = [
        CardReference(card_id = "macro_002", relationship = CardRelationship.PARENT, tags = ["if_endurance_limiter"]),
        CardReference(card_id = "macro_004", relationship = CardRelationship.PARENT, tags = ["specificity"]),
        CardReference(card_id = "mezzo_008", relationship = CardRelationship.SUPPORT, tags = ["execution"]),
        CardReference(card_id = "micro_006", relationship = CardRelationship.CHILD, tags = ["core_week"]),
        CardReference(card_id = "micro_007", relationship = CardRelationship.CHILD, tags = ["advanced_option"]),
    ],
    recommended_duration_weeks = "3-8",
    placement_guidance = [
        "Use after endurance development when the goal requires longer sustained work.",
        "Place far enough from race day that fatigue can be absorbed before taper.",
    ],
)

