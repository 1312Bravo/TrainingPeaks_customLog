from training_cards.schemas import CardRelationship, CardReference, CardType, MezzoCard, TrainingLevel

endurance_development_block = MezzoCard(
    id = "mezzo_002",
    slug = "endurance-development-block",
    title = "Endurance Development Block",
    card_type = CardType.MEZZO,
    suitable_levels = [TrainingLevel.ALL],
    summary = "A steady aerobic block that develops endurance through consistent volume and controlled long-run progression.",
    purpose = "Improve the ability to sustain aerobic running for longer without excessive fatigue.",
    detailed_description = (
        "This block develops endurance after basic consistency is already in place. "
        "The main stress is not high intensity, but accumulated aerobic work and a gradually more substantial long run. "
        "For trail runners, the long run may be measured by time and vertical gain rather than distance, with hiking used as a normal endurance skill when terrain demands it."
    ),
    tags = ["endurance", "aerobic", "long_run", "base"],
    goal_race_context = [
        "Useful for any goal where sustained aerobic capacity matters.",
        "Especially relevant as race duration increases.",
    ],
    when_to_choose = [
        "When easy volume is stable and the athlete can recover from moderate long runs.",
        "When endurance is the primary limiter rather than speed.",
    ],
    when_not_to_choose = [
        "Do not use if the athlete cannot recover from the current long run.",
        "Do not use as a substitute for race-specific work very close to a key event.",
    ],
    expected_adaptations = [
        "Improved aerobic stamina.",
        "Better long-run tolerance.",
        "Greater confidence sustaining low-to-moderate effort.",
    ],
    training_characteristics = [
        "Mostly easy running with one progressively longer endurance session.",
        "Occasional steady segments can be used if they remain controlled.",
        "Recovery weeks protect adaptation.",
    ],
    terrain_demands = [
        "Terrain should match the intended endurance stress.",
        "Trail runners can include rolling terrain and moderate climbs, but downhill load should progress gradually.",
    ],
    common_mistakes = [
        "Making long runs too hard.",
        "Progressing long-run duration every week without consolidation.",
        "Ignoring fueling on longer outings.",
    ],
    warning_signs = [
        "Long-run fatigue compromises several following days.",
        "Heart rate or effort drifts unusually on easy runs.",
    ],
    progression_rules = [
        "Extend long-run duration only when recovery is predictable.",
        "Use cutback weeks after two to three building weeks.",
    ],
    regression_rules = [
        "Shorten the long run or reduce terrain difficulty if recovery slips.",
        "Return to easy volume when endurance sessions feel too costly.",
    ],
    references = [
        CardReference(card_id = "macro_002", relationship = CardRelationship.PARENT, tags = ["core_block"]),
        CardReference(card_id = "macro_003", relationship = CardRelationship.PARENT, tags = ["support_block"]),
        CardReference(card_id = "mezzo_001", relationship = CardRelationship.PREVIOUS, tags = ["common_sequence"]),
        CardReference(card_id = "mezzo_006", relationship = CardRelationship.NEXT, tags = ["if_goal_demands"]),
        CardReference(card_id = "micro_006", relationship = CardRelationship.CHILD, tags = ["core_week"]),
    ],
    recommended_duration_weeks = "3-8",
    placement_guidance = [
        "Use after easy volume is stable.",
        "Place before more specific long-endurance or race-practice work.",
    ],
)

