from training_cards.schemas import CardRelationship, CardReference, CardType, MacroCard, TrainingLevel

base_development = MacroCard(
    id = "macro_002",
    slug = "base-development",
    title = "Base Development",
    card_type = CardType.MACRO,
    suitable_levels = [TrainingLevel.ALL],
    summary = "A foundational phase that builds aerobic capacity, durability, and tolerance for steady training load.",
    purpose = "Develop the aerobic and musculoskeletal foundation needed for later specific or higher-intensity work.",
    detailed_description = (
        "Base development gives the athlete more room to train before intensity becomes the limiting factor. "
        "Most work should remain easy to steady, with progression coming from consistency, frequency, duration, and carefully managed long runs. "
        "For trail runners, this phase can include moderate vertical gain, hiking efficiency, and varied surfaces, but the goal is still sustainable aerobic development rather than hard mountain-specific stress. "
        "A good base phase should leave the athlete more durable, not simply more tired."
    ),
    tags = ["base", "aerobic", "durability", "volume"],
    goal_race_context = [
        "Fits most running goals when aerobic capacity or durability is a limiter.",
        "Especially useful before build or race-specific preparation.",
        "Can be extended for longer races or athletes needing more time-on-feet tolerance.",
    ],
    when_to_choose = [
        "After return-to-consistency work is stable.",
        "Early in a preparation cycle before demanding workouts or race-specific sessions.",
        "When the athlete needs more aerobic volume tolerance.",
    ],
    when_not_to_choose = [
        "Do not use as a high-volume push when recovery is already strained.",
        "Do not use if the goal race is very close and specific preparation is missing.",
    ],
    expected_adaptations = [
        "Improved aerobic efficiency and endurance.",
        "Better tolerance for weekly running volume.",
        "Greater durability for later intensity, long runs, and race-specific demands.",
    ],
    training_characteristics = [
        "High proportion of low-intensity running.",
        "Gradual long-run development.",
        "Light strides, drills, or short controlled efforts can maintain neuromuscular sharpness.",
    ],
    terrain_demands = [
        "Use varied but controllable terrain.",
        "For trail runners, include rolling routes and moderate climbing without turning the phase into a climbing block.",
        "Keep downhill load progressive and recoverable.",
    ],
    common_mistakes = [
        "Turning steady aerobic runs into frequent threshold efforts.",
        "Increasing volume and vertical gain at the same time too aggressively.",
        "Ignoring strength, mobility, and recovery habits.",
    ],
    warning_signs = [
        "Easy pace or effort deteriorates across the week.",
        "Long runs create soreness that affects several following sessions.",
        "Persistent heaviness without clear recovery.",
    ],
    progression_rules = [
        "Increase volume gradually and keep most training comfortable.",
        "Progress long runs before adding frequent intensity.",
        "Move to build phase when the athlete tolerates stable volume and recovers predictably.",
    ],
    regression_rules = [
        "Hold volume steady if fatigue accumulates.",
        "Reduce terrain difficulty before reducing all running if trail stress is the problem.",
    ],
    references = [
        CardReference(card_id = "macro_001", relationship = CardRelationship.PREVIOUS, tags = ["common_sequence"]),
        CardReference(card_id = "macro_003", relationship = CardRelationship.NEXT, tags = ["natural_sequence"]),
        CardReference(card_id = "mezzo_001", relationship = CardRelationship.CHILD, tags = ["core_block"]),
        CardReference(card_id = "mezzo_002", relationship = CardRelationship.CHILD, tags = ["core_block"]),
    ],
    recommended_duration_weeks = "8-16",
    timing_guidance = [
        "Best placed early to middle in a goal preparation cycle.",
        "Longer race goals often benefit from a longer base phase.",
    ],
)

