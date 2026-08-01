from training_cards.schemas import CardRelationship, CardReference, CardType, MezzoCard, TrainingLevel

threshold_development_block = MezzoCard(
    id = "mezzo_004",
    slug = "threshold-development-block",
    title = "Threshold Development Block",
    card_type = CardType.MEZZO,
    suitable_levels = [TrainingLevel.INTERMEDIATE, TrainingLevel.ADVANCED, TrainingLevel.ELITE],
    summary = "A controlled intensity block that improves the ability to sustain comfortably hard running.",
    purpose = "Raise sustainable aerobic output while avoiding excessive high-intensity fatigue.",
    detailed_description = (
        "Threshold work is useful because many running performances depend on sustaining a strong but controlled effort. "
        "The block should emphasize control: efforts are hard enough to create adaptation, but not so hard that they become repeated races. "
        "For trail runners, threshold can be done by effort on climbs, rolling terrain, or smoother surfaces depending on the goal; pace alone is often unreliable on variable terrain."
    ),
    tags = ["threshold", "controlled_intensity", "stamina"],
    goal_race_context = [
        "Useful for runners who need better sustainable effort and speed endurance.",
        "Fits many race distances, with session length adjusted to race duration and athlete level.",
    ],
    when_to_choose = [
        "When easy volume and long-run tolerance are stable.",
        "When the athlete fades at strong but submaximal efforts.",
    ],
    when_not_to_choose = [
        "Do not use when the athlete is already carrying high fatigue.",
        "Do not use if most easy runs are drifting into moderate intensity.",
    ],
    expected_adaptations = [
        "Improved lactate clearance and tolerance around sustainable hard effort.",
        "Better ability to hold pace or effort without early fatigue.",
        "Improved pacing discipline.",
    ],
    training_characteristics = [
        "Cruise intervals, steady tempo segments, or controlled progression efforts.",
        "Intensity should feel strong but repeatable.",
        "Easy days must remain easy.",
    ],
    terrain_demands = [
        "Use flatter routes for precise pacing when needed.",
        "Trail runners can use climbs or rolling trails by effort, but should avoid technical terrain that disrupts intensity control.",
    ],
    common_mistakes = [
        "Running threshold sessions at race-effort intensity too often.",
        "Using pace targets that ignore terrain, heat, or fatigue.",
        "Adding too many moderate days around threshold workouts.",
    ],
    warning_signs = [
        "Threshold pace or effort becomes unsustainable early.",
        "Recovery runs feel stale for several days after workouts.",
    ],
    progression_rules = [
        "Increase total controlled work before increasing intensity.",
        "Keep sessions repeatable across weeks.",
    ],
    regression_rules = [
        "Shorten intervals or add recovery if control is lost.",
        "Return to endurance development if fatigue persists.",
    ],
    references = [
        CardReference(card_id = "macro_003", relationship = CardRelationship.PARENT, tags = ["core_block"]),
        CardReference(card_id = "macro_004", relationship = CardRelationship.PARENT, tags = ["if_race_relevant"]),
        CardReference(card_id = "mezzo_005", relationship = CardRelationship.NEXT, tags = ["possible_progression"]),
        CardReference(card_id = "micro_005", relationship = CardRelationship.CHILD, tags = ["core_week"]),
        CardReference(card_id = "session_007", relationship = CardRelationship.CHILD, tags = ["key_session"]),
    ],
    recommended_duration_weeks = "3-6",
    placement_guidance = [
        "Use during build or before race-specific preparation.",
        "Avoid stacking with another demanding intensity block unless volume is reduced.",
    ],
)

