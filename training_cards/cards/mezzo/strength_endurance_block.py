from training_cards.schemas import CardRelationship, CardReference, CardType, MezzoCard, TrainingLevel

strength_endurance_block = MezzoCard(
    id = "mezzo_003",
    slug = "strength-endurance-block",
    title = "Strength Endurance Block",
    card_type = CardType.MEZZO,
    suitable_levels = [TrainingLevel.INTERMEDIATE, TrainingLevel.ADVANCED, TrainingLevel.ELITE],
    summary = "A block that improves the ability to maintain form and force production under fatigue.",
    purpose = "Build running-specific strength endurance without replacing aerobic development.",
    detailed_description = (
        "This block adds controlled muscular demand through hills, sustained efforts, strength support, or terrain that requires forceful running. "
        "It should feel strong and controlled, not like repeated maximal efforts. "
        "For trail runners, this is one of the most transferable blocks because climbs, descents, uneven footing, and hiking transitions often expose strength-endurance limits."
    ),
    tags = ["strength_endurance", "hills", "durability", "form"],
    goal_race_context = [
        "Useful when the goal requires sustained force, hills, fatigue resistance, or late-race form.",
        "Applicable across distances when muscular durability is a limiter.",
    ],
    when_to_choose = [
        "When aerobic base is stable and the athlete tolerates moderate load.",
        "When climbing, late-race fatigue, or form breakdown are clear limiters.",
    ],
    when_not_to_choose = [
        "Do not use during acute tendon, calf, hamstring, or knee irritation.",
        "Do not combine with a sudden jump in volume or downhill load.",
    ],
    expected_adaptations = [
        "Improved resistance to form breakdown.",
        "Better force production during sustained or hilly running.",
        "Improved running economy support when paired with sensible strength work.",
    ],
    training_characteristics = [
        "Hill efforts, steady strength-focused running, or controlled muscular endurance sessions.",
        "Easy running remains the majority of weekly volume.",
        "Strength training should support the block, not destroy key run quality.",
    ],
    terrain_demands = [
        "Use hills or resistance only when they match the target stress.",
        "Trail runners should progress downhill exposure separately from uphill strength demand.",
    ],
    common_mistakes = [
        "Running hill work too hard too often.",
        "Treating strength endurance as maximal sprinting.",
        "Adding heavy gym work without adjusting run load.",
    ],
    warning_signs = [
        "Calf, Achilles, hamstring, or knee soreness worsening across sessions.",
        "Loss of coordination on climbs or descents.",
    ],
    progression_rules = [
        "Progress duration or repetitions before increasing intensity.",
        "Keep recovery days flat or mechanically easy when needed.",
    ],
    regression_rules = [
        "Reduce hill grade, repetition count, or strength load if mechanics degrade.",
        "Return to endurance development if muscular soreness dominates the week.",
    ],
    references = [
        CardReference(card_id = "macro_003", relationship = CardRelationship.PARENT, tags = ["core_block"]),
        CardReference(card_id = "macro_004", relationship = CardRelationship.PARENT, tags = ["if_goal_demands"]),
        CardReference(card_id = "mezzo_004", relationship = CardRelationship.ALTERNATIVE, tags = ["different_stress"]),
        CardReference(card_id = "micro_004", relationship = CardRelationship.CHILD, tags = ["core_week"]),
        CardReference(card_id = "session_010", relationship = CardRelationship.CHILD, tags = ["key_session"]),
    ],
    recommended_duration_weeks = "3-6",
    placement_guidance = [
        "Use after base development or early in build phase.",
        "Avoid placing the hardest version immediately before taper.",
    ],
)

