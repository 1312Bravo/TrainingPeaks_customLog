from training_cards.schemas import CardRelationship, CardReference, CardType, MezzoCard, TrainingLevel

aerobic_power_block = MezzoCard(
    id = "mezzo_005",
    slug = "aerobic-power-block",
    title = "Aerobic Power Block",
    card_type = CardType.MEZZO,
    suitable_levels = [TrainingLevel.INTERMEDIATE, TrainingLevel.ADVANCED, TrainingLevel.ELITE],
    summary = "A high-intensity aerobic block that develops the ability to run strongly near the upper aerobic range.",
    purpose = "Improve aerobic power and the ability to tolerate short, demanding intervals with adequate recovery.",
    detailed_description = (
        "Aerobic power work is potent and should be used sparingly. "
        "It can improve performance when the athlete already has enough base to absorb intensity, but it becomes costly when layered on top of excessive volume or poor recovery. "
        "For trail runners, aerobic power can be developed on hills or smoother surfaces; the route should let the athlete produce high effort safely without chaotic footing or excessive downhill damage."
    ),
    tags = ["aerobic_power", "vo2max", "high_intensity"],
    goal_race_context = [
        "Useful when speed, climbing power, or high-end aerobic capacity is a limiter.",
        "Most appropriate when the athlete is not already overloaded by race-specific volume.",
    ],
    when_to_choose = [
        "When base fitness is stable and the athlete tolerates workouts well.",
        "When the goal benefits from stronger high-end aerobic capacity.",
    ],
    when_not_to_choose = [
        "Do not use during poor recovery, injury return, or high life stress.",
        "Do not use as frequent all-out training.",
    ],
    expected_adaptations = [
        "Improved high-end aerobic capacity.",
        "Better tolerance of short hard efforts.",
        "Improved ability to surge, climb strongly, or handle race changes.",
    ],
    training_characteristics = [
        "Short to moderate intervals at high but controlled effort.",
        "Generous recovery to protect quality.",
        "Low-intensity volume supports adaptation around the key sessions.",
    ],
    terrain_demands = [
        "Use safe terrain that supports good mechanics.",
        "Trail runners may use climbs to reduce impact, but should avoid technical routes that make effort inconsistent.",
    ],
    common_mistakes = [
        "Doing intervals too hard and turning the block anaerobic.",
        "Adding too many high-intensity sessions per week.",
        "Ignoring recovery because the sessions are short.",
    ],
    warning_signs = [
        "Sharp drop in interval quality.",
        "Unusual irritability, poor sleep, or heavy legs.",
        "Strained mechanics during fast running.",
    ],
    progression_rules = [
        "Progress by improving quality and consistency before adding volume.",
        "Use recovery weeks after demanding loading.",
    ],
    regression_rules = [
        "Reduce repetitions or extend recoveries if quality drops.",
        "Switch to threshold or endurance work if high intensity is not being absorbed.",
    ],
    references = [
        CardReference(card_id = "macro_003", relationship = CardRelationship.PARENT, tags = ["core_block"]),
        CardReference(card_id = "mezzo_004", relationship = CardRelationship.PREVIOUS, tags = ["possible_sequence"]),
        CardReference(card_id = "micro_005", relationship = CardRelationship.CHILD, tags = ["core_week"]),
        CardReference(card_id = "session_008", relationship = CardRelationship.CHILD, tags = ["key_session"]),
    ],
    recommended_duration_weeks = "2-5",
    placement_guidance = [
        "Use after base is stable and before final taper.",
        "Keep the block short if the athlete is also building long-run or terrain stress.",
    ],
)

