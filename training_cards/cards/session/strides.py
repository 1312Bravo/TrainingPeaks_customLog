from training_cards.schemas import CardRelationship, CardReference, CardType, SessionCard, SessionPart, TrainingLevel

strides = SessionCard(
    id = "session_011",
    slug = "strides",
    title = "Strides",
    card_type = CardType.SESSION,
    suitable_levels = [TrainingLevel.ALL],
    summary = "Short relaxed fast efforts used to maintain rhythm, coordination, and leg speed.",
    purpose = "Add neuromuscular sharpness without creating a full workout load.",
    detailed_description = "Strides are small touches of faster running. They should feel smooth, relaxed, and technically clean. Trail runners can do them on safe flat ground or gentle grass, not on risky technical terrain.",
    tags = ["strides", "speed", "coordination"],
    goal_race_context = ["Useful in base, maintenance, taper, and many easy weeks."],
    when_to_choose = ["After easy runs when the athlete is fresh enough.", "During taper to maintain rhythm."],
    when_not_to_choose = ["Do not use when sprinting would irritate pain or soreness."],
    expected_adaptations = ["Better coordination.", "Maintained leg speed.", "Improved running rhythm."],
    training_characteristics = ["Very short fast relaxed efforts.", "Full easy recovery.", "Low volume."],
    terrain_demands = ["Safe, smooth surface preferred."],
    common_mistakes = ["Sprinting all-out.", "Adding too many repetitions."],
    warning_signs = ["Tightness or strain during fast running."],
    progression_rules = ["Add repetitions gradually if tolerated."],
    regression_rules = ["Skip strides when sore or fatigued."],
    references = [
        CardReference(card_id = "micro_002", relationship = CardRelationship.PARENT, tags = ["optional"]),
        CardReference(card_id = "micro_009", relationship = CardRelationship.PARENT, tags = ["rhythm"]),
    ],
    session_family = "neuromuscular",
    typical_duration = "4-10 short efforts",
    workout_parts = [
        SessionPart(name = "Easy Run Before Strides", duration = "20-60 min", rpe = "3-4", instructions = "Run easy before adding strides."),
        SessionPart(name = "Strides", duration = "10-25 sec each", rpe = "7-8", instructions = "Run fast but relaxed with smooth mechanics."),
        SessionPart(name = "Full Easy Recovery", duration = "45-120 sec between strides", rpe = "1-2", instructions = "Walk or jog until fully ready."),
        SessionPart(name = "Easy Finish", duration = "5-10 min", rpe = "2-3", instructions = "Finish relaxed."),
    ],
    intensity_guidance = ["Fast but relaxed.", "Not maximal sprinting."],
    execution_notes = ["Stop while the movement still feels smooth."],
    recovery_requirements = ["Low recovery cost if kept short."],
)

