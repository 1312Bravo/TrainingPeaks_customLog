from training_cards.schemas import CardRelationship, CardReference, CardType, MezzoCard, TrainingLevel

recovery_block = MezzoCard(
    id = "mezzo_009",
    slug = "recovery-block",
    title = "Recovery Block",
    card_type = CardType.MEZZO,
    suitable_levels = [TrainingLevel.ALL],
    summary = "A low-load block that restores readiness after accumulated fatigue, racing, or demanding training.",
    purpose = "Reduce fatigue and re-establish stable readiness before the next training emphasis.",
    detailed_description = (
        "A recovery block is not wasted time; it is where hard training becomes usable adaptation. "
        "The athlete should reduce load enough that sleep, mood, soreness, and easy effort begin to normalize. "
        "For trail runners, reducing downhill and technical load is often more important than reducing distance alone because mechanical stress can linger after hard terrain."
    ),
    tags = ["recovery", "deload", "fatigue_management"],
    goal_race_context = [
        "Useful after races, hard blocks, travel, or accumulated stress.",
        "Can appear inside any macro phase when fatigue needs management.",
    ],
    when_to_choose = [
        "When fatigue is rising faster than fitness.",
        "After two to four harder weeks, depending on athlete level and load.",
        "After mechanically demanding race or terrain exposure.",
    ],
    when_not_to_choose = [
        "Do not use as complete rest by default if light movement helps recovery.",
        "Do not ignore persistent pain or illness symptoms.",
    ],
    expected_adaptations = [
        "Reduced fatigue and soreness.",
        "Improved readiness for the next block.",
        "Better long-term consistency.",
    ],
    training_characteristics = [
        "Lower volume and reduced key-session demand.",
        "Easy running, mobility, light strength, or cross-training as appropriate.",
        "Return to normal load only when recovery markers improve.",
    ],
    terrain_demands = [
        "Choose forgiving terrain.",
        "Limit steep descents and technical routes if musculoskeletal fatigue is present.",
    ],
    common_mistakes = [
        "Keeping intensity high because volume is reduced.",
        "Treating recovery as a punishment instead of a planned adaptation tool.",
        "Returning to hard training before soreness and motivation normalize.",
    ],
    warning_signs = [
        "Fatigue does not improve after several easier days.",
        "Pain persists or worsens with easy running.",
        "Mood, sleep, or appetite remain disrupted.",
    ],
    progression_rules = [
        "Progress back to normal training when easy running feels normal and soreness is low.",
        "Resume with one key stressor first, not everything at once.",
    ],
    regression_rules = [
        "Reduce to rest, walking, or cross-training if easy running is not restorative.",
        "Seek professional help for persistent pain, illness, or concerning fatigue.",
    ],
    references = [
        CardReference(card_id = "macro_006", relationship = CardRelationship.PARENT, tags = ["core_block"]),
        CardReference(card_id = "macro_001", relationship = CardRelationship.PARENT, tags = ["restart"]),
        CardReference(card_id = "macro_005", relationship = CardRelationship.PARENT, tags = ["fatigue_management"]),
        CardReference(card_id = "micro_001", relationship = CardRelationship.CHILD, tags = ["core_week"]),
        CardReference(card_id = "mezzo_001", relationship = CardRelationship.NEXT, tags = ["if_ready"]),
    ],
    recommended_duration_weeks = "1-4",
    placement_guidance = [
        "Use after demanding blocks or whenever fatigue signals require it.",
        "Can be planned or inserted reactively.",
    ],
)

