import os
import urllib3
import pandas as pd
import logging
from config import load_config
from extract import fetch_pokemon
from load import build_path, save_raw_json, save_csv, load_csv
from transform import (
    normalize_pokemon_data,
    extract_types,
    build_type_mappings,
    validate_data,
    compute_type_averages
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = build_path(BASE_DIR, "data")
OUTPUT_DIR = build_path(DATA_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

def main():
    logging.info("Loading configuration...")
    config = load_config()

    timeout = config.get("timeout", 10)
    pokemon_count = config["pokemon_count"]
    base_url = config["base_url"]
    verify_ssl = bool(config["verify_ssl"])

    if not verify_ssl:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    pokemon_csv = config.get("pokemon_csv", "pokemon.csv")
    type_csv = config.get("type_csv", "type.csv")
    pokemon_types_csv = config.get("pokemon_types_csv", "pokemon_types.csv")
    type_averages_csv = config.get("type_averages_csv", "type_averages.csv")

    pokemon_rows = []
    all_types = set()
    pokemon_type_links = []

    logging.info(f"Starting fetch for {pokemon_count} Pokémon...")
    for p in range(1, pokemon_count + 1):
        logging.info(f"Fetching Pokémon {p}/{pokemon_count}")

        try:
            data = fetch_pokemon(p, base_url, verify_ssl, timeout)            
        except Exception as e:
            logging.error(f"Failed to fetch Pokémon {p}: {e}")
            continue

        if config.get("save_raw", False):
            save_raw_json(data, DATA_DIR, str(p))

        pokemon_row = normalize_pokemon_data(data)
        pokemon_rows.append(pokemon_row)

        type_list = extract_types(data)
        for t in type_list:
            all_types.add(t)
            pokemon_type_links.append((p, t))

    type_rows, pokemon_type_rows = build_type_mappings(all_types, pokemon_type_links)

    validate_data(pokemon_rows, type_rows, pokemon_type_links, pokemon_count)

    save_csv(pokemon_rows, OUTPUT_DIR, pokemon_csv)
    save_csv(type_rows, OUTPUT_DIR, type_csv)     
    save_csv(pokemon_type_rows, OUTPUT_DIR, pokemon_types_csv)

    logging.info("Computing type weight averages...")

    pokemon_df = load_csv(OUTPUT_DIR, pokemon_csv)
    type_df = load_csv(OUTPUT_DIR, type_csv)
    pokemon_types_df = load_csv(OUTPUT_DIR, pokemon_types_csv)

    if pokemon_df is None or type_df is None or pokemon_types_df is None:
        logging.error("Aggregation aborted: one or more CSV files could not be read.")
        return

    type_averages_df = compute_type_averages(pokemon_df, type_df, pokemon_types_df)
    save_csv(type_averages_df, OUTPUT_DIR, type_averages_csv)
    logging.info("Aggregation completed!")
    
    logging.info("Processing complete!")

if __name__ == "__main__":
    main()
