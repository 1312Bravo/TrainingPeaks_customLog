from training_cards.schemas import CardRelationship, CardReference, CardType, MacroCard, TrainingLevel

build_phase = MacroCard(
    id = "macro_003",
    slug = "build-phase",
    title = "Build Phase",
    card_type = CardType.MACRO,
    suitable_levels = [TrainingLevel.INTERMEDIATE, TrainingLevel.ADVANCED, TrainingLevel.ELITE],
    summary = "A development phase that adds purposeful intensity or strength-endurance work on top of stable aerobic training.",
    purpose = "Improve the athlete's ability to sustain stronger efforts while preserving the aerobic base.",
    detailed_description = (
        "The build phase introduces more demanding work once the athlete can already absorb regular training. "
        "The exact emphasis can vary: threshold control, aerobic power, strength endurance, or long-endurance development. "
        "The key is to add stress deliberately while keeping enough low-intensity running to maintain durability and recovery. "
        "For trail runners, this is where climbing strength, controlled downhill exposure, and terrain-specific fatigue resistance can become more prominent without overwhelming the plan."
    ),
    tags = ["build", "development", "intensity", "strength_endurance"],
    goal_race_context = [
        "Useful after base development when the athlete needs a stronger performance ceiling.",
        "Fits many race distances, with the block emphasis adjusted to the goal.",
    ],
    when_to_choose = [
        "When aerobic volume is stable and the athlete recovers well.",
        "When performance needs more threshold, power, or strength-endurance development.",
        "When there is enough time before race-specific preparation.",
    ],
    when_not_to_choose = [
        "Do not use when easy training is already unstable.",
        "Do not use directly after illness, injury, or a very demanding race block.",
        "Do not stack multiple hard emphases at once.",
    ],
    expected_adaptations = [
        "Improved sustainable speed or power.",
        "Better tolerance for controlled intensity.",
        "Improved fatigue resistance during demanding running.",
    ],
    training_characteristics = [
        "One to three key sessions per week depending on level and load.",
        "Most training remains low intensity, with targeted harder sessions.",
        "Strength support may be included if it does not compromise key runs.",
    ],
    terrain_demands = [
        "Road runners may use flat or rolling routes for control.",
        "Trail runners may use climbs, rolling trails, or moderate technical terrain to match the intended stress.",
        "Downhill exposure should progress gradually because it adds mechanical load.",
    ],
    common_mistakes = [
        "Adding too much intensity while also increasing volume.",
        "Making every run moderately hard.",
        "Using terrain that makes the workout harder than intended.",
    ],
    warning_signs = [
        "Loss of quality across repeated key sessions.",
        "Sleep, mood, or resting fatigue worsening over several days.",
        "Soreness that changes mechanics during workouts.",
    ],
    progression_rules = [
        "Progress only one major stressor at a time: volume, intensity, vertical gain, or technical load.",
        "Keep easy days genuinely easy.",
        "Move to race-specific preparation when general capacities are strong enough for goal-specific work.",
    ],
    regression_rules = [
        "Drop the least important hard session first if fatigue rises.",
        "Replace terrain-specific stress with controlled flat running when mechanical load is excessive.",
    ],
    references = [
        CardReference(card_id = "macro_002", relationship = CardRelationship.PREVIOUS, tags = ["common_sequence"]),
        CardReference(card_id = "macro_004", relationship = CardRelationship.NEXT, tags = ["natural_sequence"]),
        CardReference(card_id = "mezzo_004", relationship = CardRelationship.CHILD, tags = ["intensity"]),
        CardReference(card_id = "mezzo_005", relationship = CardRelationship.CHILD, tags = ["intensity"]),
        CardReference(card_id = "mezzo_003", relationship = CardRelationship.CHILD, tags = ["strength"]),
    ],
    recommended_duration_weeks = "6-12",
    timing_guidance = [
        "Usually follows base development.",
        "Should leave enough time afterward for race-specific preparation and taper.",
    ],
)

