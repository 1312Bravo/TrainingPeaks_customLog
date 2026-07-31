# Training Cards

This folder is the isolated workspace for designing reusable training cards before they are connected to the Training Platform app.

## Working Order

1. Define the coach prompt used to create and review cards.
2. Define or revise the card classes and validation rules.
3. Decide the card folder structure and naming conventions.
4. Build seed macro, mezzo, micro, and session cards.
5. Review the cards for coaching usefulness and consistency.
6. Plug the finished card system into `training_platform/` later.

## Current Direction

- Keep this folder independent from the Streamlit app until the training logic is ready.
- Store card schemas separately from card content.
- Start simple and add structure only when the cards need it.

## Current Library Draft

The current draft contains 38 cards:

- 6 macro cards
- 9 mezzo cards
- 9 micro cards
- 14 session cards

Use `training_cards/registry.py` as the central import point for the full library.

Tags are currently free-text strings. Keep them short and consistent; do not convert them into enums until real app filtering shows that stricter control is needed.

## Code And Content

Schema, registry, serialization, and JSON storage files include comments because they explain how the library works.

Individual card files are intentionally written like structured data. Keep comments in card files rare; the card fields themselves should carry the coaching content.
