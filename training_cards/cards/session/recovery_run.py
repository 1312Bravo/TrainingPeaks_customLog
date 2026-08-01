from training_cards.schemas import CardRelationship, CardReference, CardType, SessionCard, SessionPart, TrainingLevel

recovery_run = SessionCard(
    id = "session_002",
    slug = "recovery-run",
    title = "Recovery Run",
    card_type = CardType.SESSION,
    suitable_levels = [TrainingLevel.ALL],
    summary = "A very easy short run used to promote movement without adding meaningful training stress.",
    purpose = "Support recovery and routine while keeping load very low.",
    detailed_description = "A recovery run should feel almost too easy. If it does not help the athlete feel better, walking, rest, or cross-training may be better. Trail runners should keep terrain smooth and avoid downhill pounding during recovery.",
    tags = ["recovery", "easy", "low_load"],
    goal_race_context = ["Useful after harder sessions, races, or long runs."],
    when_to_choose = ["When light movement supports recovery.", "During recovery weeks."],
    when_not_to_choose = ["Do not use if running worsens soreness or pain."],
    expected_adaptations = ["Improved recovery routine.", "Reduced stiffness for some athletes."],
    training_characteristics = ["Very short.", "Very easy.", "No workout intent."],
    terrain_demands = ["Flat or forgiving terrain preferred."],
    common_mistakes = ["Running too far.", "Turning recovery into aerobic maintenance."],
    warning_signs = ["Feeling worse after the run.", "Pain increases."],
    progression_rules = ["Progress only when recovery runs consistently feel restorative."],
    regression_rules = ["Use walking or rest instead."],
    references = [CardReference(card_id = "micro_001", relationship = CardRelationship.PARENT, tags = ["core_session"])],
    session_family = "recovery",
    typical_duration = "10-45 minutes",
    workout_parts = [
        SessionPart(name = "Very Easy Run", duration = "10-35 min", rpe = "1-3", instructions = "Keep the whole run restorative; stop early if it does not feel helpful.", terrain_notes = "Choose flat or forgiving terrain."),
        SessionPart(name = "Optional Mobility", duration = "5-10 min", rpe = "1-2", instructions = "Light mobility only, no strength fatigue."),
    ],
    intensity_guidance = ["Very easy.", "Below normal easy-run effort."],
    execution_notes = ["Stop before the session becomes training stress."],
    recovery_requirements = ["Should require little to no recovery."],
)

