from training_cards.schemas import CardRelationship, CardReference, CardType, SessionCard, SessionPart, TrainingLevel

race_simulation_run = SessionCard(
    id = "session_012",
    slug = "race-simulation-run",
    title = "Race Simulation Run",
    card_type = CardType.SESSION,
    suitable_levels = [TrainingLevel.INTERMEDIATE, TrainingLevel.ADVANCED, TrainingLevel.ELITE],
    summary = "A controlled rehearsal of important race demands.",
    purpose = "Practice execution before race day without creating race-level damage.",
    detailed_description = "Race simulation should be specific enough to teach something and conservative enough to recover from. It can include goal effort, fueling, gear, terrain, or pacing rehearsal. For trail runners, it may include climbs, descents, hiking, poles, pack setup, or technical pacing, but should not try to recreate every course detail.",
    tags = ["race_simulation", "specificity", "execution"],
    goal_race_context = ["Useful before important goal events."],
    when_to_choose = ["During race practice weeks.", "When execution questions need testing."],
    when_not_to_choose = ["Do not use too close to race day if it creates heavy fatigue.", "Do not use as a weekly race."],
    expected_adaptations = ["Better race execution.", "Improved confidence.", "Practical feedback on fueling and pacing."],
    training_characteristics = ["Specific but controlled.", "Usually longer or more complex than normal quality sessions."],
    terrain_demands = ["Match key demands broadly.", "Avoid unnecessary risk or excessive downhill damage."],
    common_mistakes = ["Overdoing the simulation.", "Testing too many variables."],
    warning_signs = ["Session requires excessive recovery.", "Specific terrain causes new pain."],
    progression_rules = ["Progress specificity before total stress."],
    regression_rules = ["Shorten or simplify the rehearsal."],
    references = [
        CardReference(card_id = "micro_008", relationship = CardRelationship.PARENT, tags = ["key_session"]),
        CardReference(card_id = "mezzo_007", relationship = CardRelationship.PARENT, tags = ["core_session"]),
    ],
    session_family = "race_practice",
    typical_duration = "Varies by goal and athlete level",
    workout_parts = [
        SessionPart(name = "Warm-Up", duration = "10-30 min", rpe = "2-4", instructions = "Start controlled and prepare equipment, fueling, and pacing cues."),
        SessionPart(name = "Race-Specific Rehearsal", duration = "20 min to several hours", rpe = "5-8", instructions = "Practice the most important goal-race demands without racing the whole session.", terrain_notes = "For trail goals, include representative climbs, descents, hiking, gear, or technical pacing as needed."),
        SessionPart(name = "Fueling And Execution Practice", duration = "Throughout main set", rpe = "N/A", instructions = "Use planned intake, gear, and pacing decisions."),
        SessionPart(name = "Cool Down", duration = "10-20 min", rpe = "2-3", instructions = "Finish controlled and assess what needs adjustment."),
    ],
    intensity_guidance = ["Race-relevant but controlled.", "Avoid all-out effort unless explicitly planned."],
    execution_notes = ["Practice fueling, pacing, and equipment intentionally."],
    recovery_requirements = ["Plan recovery as if it were a major key session."],
)

