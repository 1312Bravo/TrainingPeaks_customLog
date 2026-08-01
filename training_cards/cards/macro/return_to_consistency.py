from training_cards.schemas import CardRelationship, CardReference, CardType, MacroCard, TrainingLevel

return_to_consistency = MacroCard(
    id = "macro_001",
    slug = "return-to-consistency",
    title = "Return To Consistency",
    card_type = CardType.MACRO,
    suitable_levels = [TrainingLevel.ALL],
    summary = "A conservative phase for rebuilding regular training after interruption, low fitness, illness, injury, or inconsistent load.",
    purpose = "Restore rhythm, basic aerobic tolerance, and tissue readiness before adding demanding volume or intensity.",
    detailed_description = (
        "This macro is for the period when the main goal is not fitness expression, but rebuilding the ability to train consistently. "
        "The emphasis is easy aerobic work, predictable frequency, gentle progression, and enough recovery that the athlete finishes most sessions feeling controlled. "
        "For trail runners, the terrain should be simple at first: modest climbing, limited hard descending, and hiking only as a low-stress aerobic tool. "
        "The phase is successful when training feels repeatable across weeks and the athlete can tolerate normal easy running without accumulating unusual soreness or fatigue."
    ),
    tags = ["return", "consistency", "low_intensity", "durability"],
    goal_race_context = [
        "Useful far from a goal race or after a disrupted training period.",
        "Useful before any base, build, or race-specific phase when consistency is not yet reliable.",
    ],
    when_to_choose = [
        "After illness, injury, travel, burnout, or a long period of inconsistent running.",
        "When easy runs feel harder than expected or recovery from normal training is poor.",
        "When the athlete needs frequency and confidence before chasing volume or intensity.",
    ],
    when_not_to_choose = [
        "Do not use as a shortcut back to hard training if pain, illness symptoms, or unusual fatigue are still present.",
        "Do not use if the athlete is already consistently handling stable load and needs a more developmental phase.",
    ],
    expected_adaptations = [
        "Improved tolerance for regular easy running.",
        "Gradual restoration of aerobic rhythm and basic durability.",
        "Better confidence and routine after an interrupted period.",
    ],
    training_characteristics = [
        "Mostly easy running with controlled frequency before meaningful intensity.",
        "Small weekly load increases and simple session structure.",
        "Optional strides only when easy running is already comfortable.",
    ],
    terrain_demands = [
        "Prefer predictable terrain early.",
        "Use gentle trails or rolling routes if they reduce monotony without adding excessive eccentric load.",
        "Limit steep descents until soreness and fatigue responses are normal.",
    ],
    common_mistakes = [
        "Returning to previous volume too quickly.",
        "Adding workouts before easy running feels repeatable.",
        "Using technical or downhill-heavy routes too early.",
    ],
    warning_signs = [
        "Persistent soreness lasting more than normal recovery.",
        "Easy effort feeling unusually labored for several sessions.",
        "Pain that changes stride or worsens during a run.",
    ],
    progression_rules = [
        "Progress when easy sessions feel controlled and recovery is stable for one to two weeks.",
        "Add duration or frequency before intensity.",
        "Move toward base development when the athlete can train consistently without protective pacing.",
    ],
    regression_rules = [
        "Reduce volume and terrain stress if soreness or fatigue accumulates.",
        "Return to walk-run or cross-training if continuous running is not yet tolerated.",
    ],
    references = [
        CardReference(card_id = "macro_002", relationship = CardRelationship.NEXT, tags = ["natural_sequence"]),
        CardReference(card_id = "mezzo_001", relationship = CardRelationship.CHILD, tags = ["starter_block"]),
        CardReference(card_id = "mezzo_009", relationship = CardRelationship.CHILD, tags = ["low_stress"]),
    ],
    recommended_duration_weeks = "3-8",
    timing_guidance = [
        "Use as long as needed before normal training load is reliable.",
        "Usually belongs before base development, not immediately before a key race.",
    ],
)

