from training_cards.cloud_store import export_seed_library_to_cache


def main() -> None:
    cache_dir = export_seed_library_to_cache()
    print(f"Exported training-card seed library to: {cache_dir}")


if __name__ == "__main__":
    main()
