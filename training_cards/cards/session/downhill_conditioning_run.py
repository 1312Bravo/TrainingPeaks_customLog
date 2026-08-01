from training_cards.schemas import CardRelationship, CardReference, CardType, SessionCard, SessionPart, TrainingLevel

downhill_conditioning_run = SessionCard(
    id = "session_014",
    slug = "downhill-conditioning-run",
    title = "Downhill Conditioning Run",
    card_type = CardType.SESSION,
    suitable_levels = [TrainingLevel.INTERMEDIATE, TrainingLevel.ADVANCED, TrainingLevel.ELITE],
    summary = "A controlled session that gradually prepares the legs for downhill and eccentric load.",
    purpose = "Build downhill durability and confidence without excessive muscle damage.",
    detailed_description = "Downhill running creates a different mechanical stress than flat or uphill running. This session should be introduced gradually, with technique and recovery prioritized. For trail runners, it can improve descending confidence and late-race quad durability, but too much too soon can compromise several days of training.",
    tags = ["downhill", "eccentric", "durability", "trail_specific"],
    goal_race_context = ["Useful for hilly or mountain goals with meaningful descending."],
    when_to_choose = ["When downhill durability is a limiter.", "When the athlete is healthy and recovery is stable."],
    when_not_to_choose = ["Do not use with knee pain, quad soreness, or poor recovery.", "Do not use close to race day unless already well adapted."],
    expected_adaptations = ["Improved eccentric tolerance.", "Better downhill confidence.", "Reduced late-descent breakdown."],
    training_characteristics = ["Controlled downhill exposure.", "Technique focus.", "Limited volume at first."],
    terrain_demands = ["Use safe descents.", "Technicality should progress slowly."],
    common_mistakes = ["Too much downhill volume.", "Descending hard while fatigued.", "Choosing risky terrain."],
    warning_signs = ["Quad soreness lasts several days.", "Knee pain or braking mechanics appear."],
    progression_rules = ["Progress exposure gradually.", "Separate hard downhill load from other major stressors."],
    regression_rules = ["Reduce descent length, grade, or technicality."],
    references = [
        CardReference(card_id = "micro_004", relationship = CardRelationship.PARENT, tags = ["trail_option"]),
        CardReference(card_id = "micro_008", relationship = CardRelationship.PARENT, tags = ["specificity"]),
    ],
    session_family = "trail_specific",
    typical_duration = "Short controlled downhill segments within an easy run",
    workout_parts = [
        SessionPart(name = "Warm-Up", duration = "15-25 min", rpe = "2-4", instructions = "Run easy before descending work."),
        SessionPart(name = "Controlled Downhill Exposure", duration = "30 sec to 5 min segments", rpe = "3-6", instructions = "Descend smoothly with relaxed control; stop before braking or coordination worsens.", terrain_notes = "Use safe descents and progress technicality slowly."),
        SessionPart(name = "Easy Recovery", duration = "2-5 min between segments", rpe = "1-3", instructions = "Recover on flat, uphill, or very easy terrain."),
        SessionPart(name = "Cool Down", duration = "10-15 min", rpe = "2-3", instructions = "Finish easy and monitor soreness afterward."),
    ],
    intensity_guidance = ["Controlled, smooth, not reckless.", "Effort may feel easy while mechanical load is high."],
    execution_notes = ["Stop before coordination breaks down."],
    recovery_requirements = ["Plan easy running afterward and monitor soreness."],
)

