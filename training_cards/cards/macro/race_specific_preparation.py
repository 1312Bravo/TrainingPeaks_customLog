from training_cards.schemas import CardRelationship, CardReference, CardType, MacroCard, TrainingLevel

race_specific_preparation = MacroCard(
    id = "macro_004",
    slug = "race-specific-preparation",
    title = "Race-Specific Preparation",
    card_type = CardType.MACRO,
    suitable_levels = [TrainingLevel.ALL],
    summary = "A phase that shapes fitness toward the specific demands of the athlete's goal event.",
    purpose = "Convert general fitness into race-ready execution by matching duration, intensity, terrain, fueling, and pacing demands.",
    detailed_description = (
        "Race-specific preparation is where training starts to look more like the goal without copying the race every week. "
        "The athlete practices the demands that matter most: pacing, long efforts, fueling, surface, elevation pattern, heat, technicality, or sustained intensity. "
        "For trail and mountain runners, this may mean vertical gain, hiking transitions, downhill durability, uneven pacing, and gear or nutrition rehearsal. "
        "The phase should sharpen readiness while avoiding the trap of doing race simulations so often that the athlete arrives tired."
    ),
    tags = ["race_specific", "preparation", "pacing", "fueling"],
    goal_race_context = [
        "Useful for any goal race once general fitness is developed.",
        "Specific demands should be relative to the race duration, surface, elevation, and athlete ability.",
    ],
    when_to_choose = [
        "When a goal event is close enough that specificity matters.",
        "When base and build phases have created enough capacity to handle race-like sessions.",
        "When pacing, fueling, terrain, or execution are likely performance limiters.",
    ],
    when_not_to_choose = [
        "Do not use too early if basic aerobic capacity and durability are still weak.",
        "Do not use if repeated race simulations are causing excessive fatigue.",
    ],
    expected_adaptations = [
        "Better race-specific endurance and pacing control.",
        "Improved confidence with goal-event demands.",
        "Improved fueling, gear, and execution habits.",
    ],
    training_characteristics = [
        "Key sessions resemble important race demands without fully replicating the race too often.",
        "Long runs and workouts become more specific to expected effort and duration.",
        "Recovery is protected so specific sessions are absorbed.",
    ],
    terrain_demands = [
        "Road goals may prioritize surface, pace rhythm, and fueling at speed.",
        "Trail goals may prioritize vertical gain, descents, hiking efficiency, technical confidence, and time-on-feet.",
        "Course specificity should increase gradually and remain proportional to recovery capacity.",
    ],
    common_mistakes = [
        "Doing race simulations too frequently.",
        "Ignoring fueling practice until race day.",
        "Overfitting training to one course detail while missing the bigger demand.",
    ],
    warning_signs = [
        "Key sessions require unusually long recovery.",
        "Race-specific terrain creates persistent soreness.",
        "Motivation drops because training feels like racing every week.",
    ],
    progression_rules = [
        "Increase specificity before increasing total stress.",
        "Practice race fueling and pacing in controlled sessions first.",
        "Move to peak and taper when the main race demands have been rehearsed.",
    ],
    regression_rules = [
        "Shorten specific sessions if execution quality drops.",
        "Reduce downhill, heat, or technical exposure if mechanical stress is too high.",
    ],
    references = [
        CardReference(card_id = "macro_003", relationship = CardRelationship.PREVIOUS, tags = ["common_sequence"]),
        CardReference(card_id = "macro_005", relationship = CardRelationship.NEXT, tags = ["natural_sequence"]),
        CardReference(card_id = "mezzo_007", relationship = CardRelationship.CHILD, tags = ["specificity"]),
        CardReference(card_id = "mezzo_006", relationship = CardRelationship.CHILD, tags = ["endurance"]),
        CardReference(card_id = "mezzo_008", relationship = CardRelationship.CHILD, tags = ["execution"]),
    ],
    recommended_duration_weeks = "4-10",
    timing_guidance = [
        "Usually follows base and build work.",
        "The closer the goal race, the more specific but less excessive the training should become.",
    ],
)

