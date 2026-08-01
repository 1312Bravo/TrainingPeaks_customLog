from training_cards.schemas import CardRelationship, CardReference, CardType, SessionCard, SessionPart, TrainingLevel

threshold_intervals = SessionCard(
    id = "session_007",
    slug = "threshold-intervals",
    title = "Threshold Intervals",
    card_type = CardType.SESSION,
    suitable_levels = [TrainingLevel.INTERMEDIATE, TrainingLevel.ADVANCED, TrainingLevel.ELITE],
    summary = "Repeated controlled hard intervals with short recoveries to develop sustainable effort.",
    purpose = "Build threshold capacity with better control than one long hard effort.",
    detailed_description = "Threshold intervals let the athlete accumulate quality work while preserving control. They are not supposed to feel like maximal intervals. For trail runners, they can be done by effort on climbs or smoother terrain when pace is unreliable.",
    tags = ["threshold", "intervals", "controlled_intensity"],
    goal_race_context = ["Useful across many race distances where strong sustainable effort matters."],
    when_to_choose = ["During threshold development.", "When tempo runs are too hard to pace well."],
    when_not_to_choose = ["Do not use when easy training feels stale.", "Do not turn recoveries into hard running."],
    expected_adaptations = ["Improved sustainable output.", "Better lactate-related control.", "Pacing discipline."],
    training_characteristics = ["Repeated intervals.", "Short easy recoveries.", "Controlled hard effort."],
    terrain_demands = ["Use terrain that allows stable effort.", "Climbs can work well for trail runners."],
    common_mistakes = ["Running faster than threshold.", "Adding too much total work."],
    warning_signs = ["Intervals fade badly.", "Effort feels maximal early."],
    progression_rules = ["Add total controlled time gradually."],
    regression_rules = ["Reduce reps, shorten intervals, or extend recovery."],
    references = [
        CardReference(card_id = "micro_005", relationship = CardRelationship.PARENT, tags = ["key_session"]),
        CardReference(card_id = "mezzo_004", relationship = CardRelationship.PARENT, tags = ["core_session"]),
    ],
    session_family = "threshold",
    typical_duration = "15-50 minutes total quality work",
    workout_parts = [
        SessionPart(name = "Warm-Up", duration = "15-25 min", rpe = "2-4", instructions = "Run easy and include short relaxed pickups if needed."),
        SessionPart(name = "Threshold Repeats", duration = "3-12 min each, 15-50 min total", rpe = "6-7", instructions = "Run controlled hard repeats that stay repeatable, not maximal.", terrain_notes = "On trails or climbs, use effort instead of pace."),
        SessionPart(name = "Easy Recoveries", duration = "1-3 min between repeats", rpe = "2-3", instructions = "Recover enough to keep the next repeat controlled."),
        SessionPart(name = "Cool Down", duration = "10-15 min", rpe = "2-3", instructions = "Jog easily."),
    ],
    intensity_guidance = ["Controlled hard.", "Repeatable, not maximal."],
    execution_notes = ["Stop before form and effort control collapse."],
    recovery_requirements = ["Easy day before or after for most athletes."],
)

