from training_cards.cloud_store import load_cached_cloud_library


def main() -> None:
    cards = load_cached_cloud_library()
    print(f"Validated cached training-card library: {len(cards)} cards")


if __name__ == "__main__":
    main()
