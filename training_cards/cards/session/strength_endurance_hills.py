from training_cards.schemas import CardRelationship, CardReference, CardType, SessionCard, SessionPart, TrainingLevel

strength_endurance_hills = SessionCard(
    id = "session_010",
    slug = "strength-endurance-hills",
    title = "Strength Endurance Hills",
    card_type = CardType.SESSION,
    suitable_levels = [TrainingLevel.INTERMEDIATE, TrainingLevel.ADVANCED, TrainingLevel.ELITE],
    summary = "Controlled uphill efforts that build sustained force and fatigue resistance.",
    purpose = "Improve strength endurance for sustained running under muscular demand.",
    detailed_description = "This session sits between pure endurance and high-intensity hill power. Effort should be strong and sustainable, with form staying controlled. For trail runners, it transfers well to climbs and hiking transitions, but the descent between efforts should not create unnecessary damage.",
    tags = ["strength_endurance", "hills", "muscular_endurance"],
    goal_race_context = ["Useful when hills, strength endurance, or late-race form are limiters."],
    when_to_choose = ["During strength-endurance blocks.", "When the athlete has stable base training."],
    when_not_to_choose = ["Do not use during tendon or muscle irritation."],
    expected_adaptations = ["Improved sustained force.", "Better climbing durability.", "Reduced form breakdown."],
    training_characteristics = ["Longer uphill repeats or sustained hill segments.", "Controlled strong effort.", "Easy support volume."],
    terrain_demands = ["Use a manageable hill.", "Avoid technical descent fatigue between reps."],
    common_mistakes = ["Grinding too hard.", "Choosing a hill that changes the workout goal."],
    warning_signs = ["Mechanics deteriorate.", "Localized tendon pain appears."],
    progression_rules = ["Increase total uphill time gradually."],
    regression_rules = ["Reduce grade, reps, or duration."],
    references = [
        CardReference(card_id = "micro_004", relationship = CardRelationship.PARENT, tags = ["key_session"]),
        CardReference(card_id = "mezzo_003", relationship = CardRelationship.PARENT, tags = ["core_session"]),
    ],
    session_family = "strength_endurance",
    typical_duration = "10-40 minutes total uphill work",
    workout_parts = [
        SessionPart(name = "Warm-Up", duration = "15-25 min", rpe = "2-4", instructions = "Run easy before starting uphill work."),
        SessionPart(name = "Uphill Strength Endurance", duration = "2-10 min each, 10-40 min total", rpe = "6-8", instructions = "Hold strong sustainable effort with stable posture and rhythm.", terrain_notes = "Grade should create muscular demand without forcing poor mechanics."),
        SessionPart(name = "Easy Down/Flat Recovery", duration = "2-5 min between reps", rpe = "1-3", instructions = "Recover easily and avoid hard descending."),
        SessionPart(name = "Cool Down", duration = "10-15 min", rpe = "2-3", instructions = "Jog easily."),
    ],
    intensity_guidance = ["Strong but controlled.", "Below all-out effort."],
    execution_notes = ["Keep posture and rhythm stable."],
    recovery_requirements = ["Easy running afterward; watch calf and quad soreness."],
)

