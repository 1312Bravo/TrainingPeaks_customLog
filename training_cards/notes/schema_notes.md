# Training Card Schema Notes

These notes explain the current card-class structure and the reasoning behind it.

## Coach Prompt

Before changing schemas or creating cards, consult:

```text
training_cards/prompts/coach_card_creation_prompt.md
```

The prompt is the coaching standard for this folder. It should guide both the card content and the schema design.

## Class Structure

The schemas use inheritance:

```text
BaseTrainingCard
  MacroCard
  MezzoCard
  MicroCard
```

`BaseTrainingCard` contains fields that every training card should have, such as identity, purpose, race context, terrain demands, progression rules, regression rules, warning signs, and sequencing options.

`MacroCard`, `MezzoCard`, and `MicroCard` inherit all base fields and then add fields that only make sense at their own planning level.

## Card Levels

The current planning levels are:

- Macro card: a training phase, usually several weeks.
- Mezzo card: a focused block inside a macro phase.
- Micro card: a week or session-pattern card.

This structure may change later if real card creation shows that another layer is needed.

## Why Context Fields Matter

Trail-running card placement depends on race distance, expected duration, vertical gain, terrain difficulty, athlete readiness, durability, and time until the goal race.

Because of that, the schema avoids rigid fields like `typical_place_in_season`. Instead, cards use context-aware fields such as:

- `goal_race_context`
- `timing_guidance`
- `placement_guidance`

## Why The Schema Is Kept Lean

Fields should not force repeated writing. The schema currently avoids separate fields for athlete readiness, training stressors, and coach notes because those ideas can usually be expressed through:

- `when_to_choose`
- `when_not_to_choose`
- `training_characteristics`
- `terrain_demands`
- `warning_signs`
- `progression_rules`
- `regression_rules`

The schema also avoids separate goal/focus fields such as `primary_focus`, `phase_goal`, and `block_goal` because those ideas should usually be clear from `summary`, `purpose`, `training_characteristics`, and `expected_adaptations`.

## Current Design Rule

Add fields only when they support a real coaching decision, comparison, recommendation, or future Training Platform display.
