from training_cards.schemas import CardRelationship, CardReference, CardType, SessionCard, SessionPart, TrainingLevel

aerobic_power_intervals = SessionCard(
    id = "session_008",
    slug = "aerobic-power-intervals",
    title = "Aerobic Power Intervals",
    card_type = CardType.SESSION,
    suitable_levels = [TrainingLevel.INTERMEDIATE, TrainingLevel.ADVANCED, TrainingLevel.ELITE],
    summary = "Short to moderate high-intensity intervals that target high-end aerobic capacity.",
    purpose = "Improve aerobic power while protecting mechanics and recovery.",
    detailed_description = "Aerobic power intervals are powerful but expensive. They require enough recovery to keep quality high. For trail runners, uphill intervals can reduce impact, but very steep or technical routes may change the session into strength or skill work instead of aerobic power.",
    tags = ["aerobic_power", "vo2max", "intervals"],
    goal_race_context = ["Useful when high-end aerobic capacity, climbing power, or surging ability matters."],
    when_to_choose = ["When the athlete is fresh and base load is stable.", "During an aerobic power block."],
    when_not_to_choose = ["Do not use during high fatigue or injury return.", "Do not use as all-out sprinting."],
    expected_adaptations = ["Improved high-end aerobic capacity.", "Better tolerance of hard efforts."],
    training_characteristics = ["High-intensity intervals.", "Generous recovery.", "Quality over volume."],
    terrain_demands = ["Use safe terrain.", "Trail runners may use moderate climbs with consistent footing."],
    common_mistakes = ["Too hard too early.", "Too many repetitions.", "Poor terrain choice."],
    warning_signs = ["Quality drops sharply.", "Mechanics become strained."],
    progression_rules = ["Improve consistency before increasing reps."],
    regression_rules = ["Reduce reps or switch to threshold work if fatigue is high."],
    references = [
        CardReference(card_id = "micro_005", relationship = CardRelationship.PARENT, tags = ["key_session"]),
        CardReference(card_id = "mezzo_005", relationship = CardRelationship.PARENT, tags = ["core_session"]),
    ],
    session_family = "aerobic_power",
    typical_duration = "10-30 minutes total quality work",
    workout_parts = [
        SessionPart(name = "Warm-Up", duration = "15-25 min", rpe = "2-4", instructions = "Build gradually and include short pickups before the first hard rep."),
        SessionPart(name = "Aerobic Power Repeats", duration = "1-5 min each, 10-30 min total", rpe = "8-9", instructions = "Run hard but controlled; quality should stay high."),
        SessionPart(name = "Full Easy Recoveries", duration = "Equal time or longer as needed", rpe = "1-3", instructions = "Recover enough to repeat good mechanics."),
        SessionPart(name = "Cool Down", duration = "10-15 min", rpe = "2-3", instructions = "Jog easily."),
    ],
    intensity_guidance = ["High intensity but controlled.", "Recover enough to maintain quality."],
    execution_notes = ["Avoid turning every rep into a maximal effort."],
    recovery_requirements = ["Usually requires easy running afterward."],
)

