from training_cards.schemas import CardRelationship, CardReference, CardType, SessionCard, SessionPart, TrainingLevel

long_run = SessionCard(
    id = "session_003",
    slug = "long-run",
    title = "Long Run",
    card_type = CardType.SESSION,
    suitable_levels = [TrainingLevel.ALL],
    summary = "A longer aerobic session that develops endurance, pacing, fueling, and durability.",
    purpose = "Extend useful endurance while practicing controlled effort over time.",
    detailed_description = "The long run should match the athlete's current capacity and goal context. It is not automatically better because it is longer. For trail runners, time-on-feet, vertical gain, hiking, and downhill load may be more useful than distance alone.",
    tags = ["long_run", "endurance", "fueling"],
    goal_race_context = ["Important for events where duration and fatigue resistance matter."],
    when_to_choose = ["During endurance and race-specific phases.", "When the athlete can recover from the planned duration."],
    when_not_to_choose = ["Do not extend if the previous long run was not absorbed."],
    expected_adaptations = ["Endurance.", "Fueling practice.", "Late-session durability."],
    training_characteristics = ["Longer duration.", "Usually easy to steady.", "Fueling practice when relevant."],
    terrain_demands = ["Match terrain to the intended stimulus.", "Trail runners may use vertical gain and hiking as part of the session."],
    common_mistakes = ["Racing the long run.", "Underfueling.", "Adding too much downhill stress too soon."],
    warning_signs = ["Recovery takes several days.", "Mechanics degrade late."],
    progression_rules = ["Increase duration, specificity, or terrain stress gradually."],
    regression_rules = ["Shorten or simplify terrain if recovery suffers."],
    references = [
        CardReference(card_id = "micro_006", relationship = CardRelationship.PARENT, tags = ["core_session"]),
        CardReference(card_id = "micro_003", relationship = CardRelationship.PARENT, tags = ["option"]),
    ],
    session_family = "endurance",
    typical_duration = "60 minutes to several hours, depending on athlete and goal",
    workout_parts = [
        SessionPart(name = "Warm-Up Into Rhythm", duration = "10-20 min", rpe = "2-3", instructions = "Start easier than expected and settle into the day."),
        SessionPart(name = "Main Endurance", duration = "45 min to several hours", rpe = "3-5", instructions = "Hold sustainable effort and practice fueling when duration makes it relevant.", terrain_notes = "For trail goals, use time, vertical gain, hiking, and descent load as better guides than distance alone."),
        SessionPart(name = "Controlled Finish", duration = "5-15 min", rpe = "2-4", instructions = "Finish with stable mechanics; avoid forcing a fast ending unless prescribed."),
    ],
    intensity_guidance = ["Mostly easy to steady.", "Avoid frequent race-level effort unless it is a planned simulation."],
    execution_notes = ["Practice fueling when duration makes it relevant.", "Use effort rather than pace on variable terrain."],
    recovery_requirements = ["Plan easier running afterward."],
)

