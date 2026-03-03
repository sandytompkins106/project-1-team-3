from assets.extract_locations_bronze import run_locations_bronze


if __name__ == "__main__":
    # Location Extract
    df = run_locations_bronze(country_id=155)
    df.to_csv("data/locations_bronze2.csv", index=False)