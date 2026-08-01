from training_cards.schemas import CardRelationship, CardReference, CardType, SessionCard, SessionPart, TrainingLevel

easy_run = SessionCard(
    id = "session_001",
    slug = "easy-run",
    title = "Easy Run",
    card_type = CardType.SESSION,
    suitable_levels = [TrainingLevel.ALL],
    summary = "A comfortable aerobic run used to build volume, recovery, and consistency.",
    purpose = "Accumulate low-intensity aerobic work without adding major fatigue.",
    detailed_description = "Easy running is the backbone session because it supports consistency and aerobic development. The effort should remain conversational. For trail runners, pace should be secondary to effort because terrain, climbing, footing, and weather can change the cost.",
    tags = ["easy", "aerobic", "volume"],
    goal_race_context = ["Useful in nearly every training phase."],
    when_to_choose = ["For aerobic volume.", "Between harder sessions.", "During return, base, recovery, or taper weeks."],
    when_not_to_choose = ["Do not use as a hidden workout by drifting too hard."],
    expected_adaptations = ["Aerobic durability.", "Routine and recovery support."],
    training_characteristics = ["Conversational effort.", "Controlled duration.", "Low mechanical and metabolic stress."],
    terrain_demands = ["Choose terrain that allows easy effort.", "Trail runners should reduce pace expectations on climbs or technical ground."],
    common_mistakes = ["Running too fast.", "Choosing terrain that makes the session no longer easy."],
    warning_signs = ["Easy effort feels strained.", "Soreness changes stride."],
    progression_rules = ["Add duration gradually.", "Keep effort easy before increasing terrain stress."],
    regression_rules = ["Shorten or flatten the run if fatigue is high."],
    references = [
        CardReference(card_id = "micro_002", relationship = CardRelationship.PARENT, tags = ["core_session"]),
        CardReference(card_id = "micro_003", relationship = CardRelationship.PARENT, tags = ["core_session"]),
    ],
    session_family = "easy",
    typical_duration = "20-90 minutes",
    workout_parts = [
        SessionPart(name = "Start Easy", duration = "5-15 min", rpe = "2-3", instructions = "Begin relaxed and let effort settle naturally."),
        SessionPart(name = "Easy Aerobic Running", duration = "15-70 min", rpe = "3-4", instructions = "Keep breathing conversational and avoid drifting into steady-hard effort.", terrain_notes = "On trails, hike or slow down on climbs to keep effort easy."),
        SessionPart(name = "Finish Relaxed", duration = "5 min", rpe = "2-3", instructions = "Finish controlled, not depleted."),
    ],
    intensity_guidance = ["Conversational effort.", "Usually low intensity."],
    execution_notes = ["Finish feeling able to repeat the session tomorrow."],
    recovery_requirements = ["Low recovery cost when properly easy."],
)

