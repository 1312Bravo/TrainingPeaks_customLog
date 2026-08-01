from training_cards.schemas import CardRelationship, CardReference, CardType, MacroCard, TrainingLevel

peak_and_taper = MacroCard(
    id = "macro_005",
    slug = "peak-and-taper",
    title = "Peak And Taper",
    card_type = CardType.MACRO,
    suitable_levels = [TrainingLevel.ALL],
    summary = "A final phase that reduces accumulated fatigue while preserving race-specific sharpness.",
    purpose = "Arrive at the goal race fresh, confident, and ready to express fitness.",
    detailed_description = (
        "Peak and taper is not a time to build major new fitness. "
        "The work shifts toward maintaining rhythm, preserving intensity touches, reducing total load, and arriving mentally calm. "
        "For trail runners, the final weeks should keep enough terrain familiarity to feel coordinated while avoiding excessive downhill damage, technical risk, or long fatigue-heavy outings. "
        "The best taper is specific enough to maintain confidence and light enough to reveal fitness."
    ),
    tags = ["taper", "freshness", "race_readiness", "peak"],
    goal_race_context = [
        "Useful before an important race or time trial.",
        "Taper length and load reduction should reflect race duration, athlete fatigue, and training history.",
    ],
    when_to_choose = [
        "When the goal race is close and the main work has already been done.",
        "When the athlete needs freshness more than additional fitness.",
    ],
    when_not_to_choose = [
        "Do not use too early if key preparation is still missing.",
        "Do not use as complete rest unless the athlete is injured, ill, or unusually fatigued.",
    ],
    expected_adaptations = [
        "Reduced accumulated fatigue.",
        "Maintained neuromuscular sharpness and aerobic readiness.",
        "Improved race-day freshness and confidence.",
    ],
    training_characteristics = [
        "Reduced total volume with some short controlled intensity.",
        "Fewer long or mechanically damaging sessions.",
        "Race logistics, fueling, and pacing cues become more important.",
    ],
    terrain_demands = [
        "Use familiar terrain without chasing extra fatigue.",
        "Trail runners should limit hard descents and risky technical sections late in taper.",
        "Keep terrain confidence, not terrain stress, as the goal.",
    ],
    common_mistakes = [
        "Testing fitness too close to race day.",
        "Reducing intensity and volume so much that the athlete feels flat.",
        "Adding last-minute terrain or gear experiments.",
    ],
    warning_signs = [
        "Restlessness leading to extra sessions.",
        "Heavy legs from doing too much late.",
        "New pain or soreness from unnecessary terrain stress.",
    ],
    progression_rules = [
        "Reduce load while keeping a small amount of race-relevant rhythm.",
        "Prioritize sleep, fueling, logistics, and confidence.",
    ],
    regression_rules = [
        "Reduce volume further if fatigue remains high.",
        "Replace workouts with short easy running if soreness or illness appears.",
    ],
    references = [
        CardReference(card_id = "macro_004", relationship = CardRelationship.PREVIOUS, tags = ["common_sequence"]),
        CardReference(card_id = "macro_006", relationship = CardRelationship.NEXT, tags = ["post_race"]),
        CardReference(card_id = "mezzo_009", relationship = CardRelationship.CHILD, tags = ["fatigue_management"]),
        CardReference(card_id = "micro_009", relationship = CardRelationship.CHILD, tags = ["race_week"]),
    ],
    recommended_duration_weeks = "1-3",
    timing_guidance = [
        "Shorter races often need a shorter taper; longer or more damaging races may need a longer reduction.",
        "The final week should protect freshness more than fitness.",
    ],
)

