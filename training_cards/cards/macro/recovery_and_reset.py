from training_cards.schemas import CardRelationship, CardReference, CardType, MacroCard, TrainingLevel

recovery_and_reset = MacroCard(
    id = "macro_006",
    slug = "recovery-and-reset",
    title = "Recovery And Reset",
    card_type = CardType.MACRO,
    suitable_levels = [TrainingLevel.ALL],
    summary = "A low-pressure phase for absorbing training or racing, restoring health, and preparing for the next goal cycle.",
    purpose = "Recover physically and mentally before choosing the next developmental direction.",
    detailed_description = (
        "Recovery and reset protects long-term progress by giving the athlete space to absorb work rather than immediately chasing the next block. "
        "It can follow a race, a demanding training phase, or a period of high life stress. "
        "Running can remain present, but it should feel optional, easy, and restorative until normal motivation and recovery return. "
        "For trail runners, this phase should be especially cautious after races with long descents, technical terrain, or high muscle damage."
    ),
    tags = ["recovery", "reset", "transition", "low_pressure"],
    goal_race_context = [
        "Useful after goal races, failed build-ups, high fatigue periods, or season transitions.",
        "Duration depends on race stress, emotional load, and residual soreness.",
    ],
    when_to_choose = [
        "After a demanding race or training block.",
        "When motivation, sleep, soreness, or mood suggest accumulated fatigue.",
        "When the next goal is not yet selected.",
    ],
    when_not_to_choose = [
        "Do not use as avoidance if the athlete is healthy and simply needs structured base training.",
        "Do not replace medical evaluation when pain or illness symptoms persist.",
    ],
    expected_adaptations = [
        "Restored freshness and motivation.",
        "Reduced residual soreness and fatigue.",
        "Clearer readiness for the next training direction.",
    ],
    training_characteristics = [
        "Easy running, optional cross-training, walking, mobility, and light strength.",
        "No forced workouts until recovery markers normalize.",
        "Gradual return to routine before structured progression.",
    ],
    terrain_demands = [
        "Choose low-risk terrain.",
        "Limit steep downhill running after mechanically demanding races.",
        "Use trails for enjoyment only if they do not add meaningful fatigue.",
    ],
    common_mistakes = [
        "Starting the next build too soon.",
        "Using recovery weeks as hidden workout weeks.",
        "Ignoring emotional fatigue after a major goal.",
    ],
    warning_signs = [
        "Persistent soreness, poor sleep, low mood, or unusual irritability.",
        "Pain that does not improve with reduced training.",
        "Loss of appetite or repeated poor recovery from easy sessions.",
    ],
    progression_rules = [
        "Return to consistency when easy movement feels normal again.",
        "Choose the next macro only after recovery and motivation are stable.",
    ],
    regression_rules = [
        "Reduce to walking, mobility, or rest if easy running does not feel restorative.",
        "Seek appropriate professional support for persistent pain, illness, or concerning fatigue.",
    ],
    references = [
        CardReference(card_id = "macro_005", relationship = CardRelationship.PREVIOUS, tags = ["post_race"]),
        CardReference(card_id = "macro_001", relationship = CardRelationship.NEXT, tags = ["restart"]),
        CardReference(card_id = "macro_002", relationship = CardRelationship.NEXT, tags = ["if_ready"]),
        CardReference(card_id = "mezzo_009", relationship = CardRelationship.CHILD, tags = ["core_block"]),
    ],
    recommended_duration_weeks = "1-6",
    timing_guidance = [
        "Use immediately after major races or very demanding phases.",
        "Longer and more damaging races usually require more conservative recovery.",
    ],
)

