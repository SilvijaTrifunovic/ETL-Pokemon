import logging
import pandas as pd


# ---------------------------------------------------------------------
# NORMALIZATION
# ---------------------------------------------------------------------
def normalize_pokemon_data(pokemon_json: dict) -> dict:
    """Normalize raw Pokemon API JSON into a flat dictionary."""

    logging.debug(f"Normalizing Pokemon ID {pokemon_json.get('id')}")

    return {
        "id": pokemon_json["id"],
        "name": pokemon_json["name"],
        "height": pokemon_json["height"],
        "weight": pokemon_json["weight"],
        "species_name": pokemon_json["species"]["name"]
    }


# ---------------------------------------------------------------------
# TYPE EXTRACTION
# ---------------------------------------------------------------------
def extract_types(pokemon_json: dict) -> list:
    """Extract a list of type names from the Pokemon JSON structure."""
    pokemon_id = pokemon_json.get("id")
    logging.debug(f"Extracting types for Pokemon ID {pokemon_id}")

    result = []
    for t in pokemon_json["types"]:
        result.append(t["type"]["name"])

    logging.debug(f"Types for Pokemon {pokemon_id}: {result}")
    return result


# ---------------------------------------------------------------------
# BUILD TYPE TABLE + MAPPING TABLE
# ---------------------------------------------------------------------
def build_type_mappings(all_types: set, pokemon_type_links: list):
    """
    Build two normalized tables:
        - type table:     [{id, name}, ...]
        - mapping table:  [{pokemon_id, type_id}, ...]

    Also performs duplicate detection inside type_rows and mapping rows.
    """
    logging.info(f"Building type mappings for {len(all_types)} unique types...")

    sorted_types = sorted(all_types)
    logging.debug(f"Sorted types: {sorted_types}")

    # Build type table
    type_rows = []
    for i, type_name in enumerate(sorted_types):
        type_rows.append({"id": i + 1, "name": type_name})

    # Duplicate check for type table
    type_ids = []
    type_names = []

    for row in type_rows:
        type_ids.append(row["id"])
        type_names.append(row["name"])

    if len(type_ids) != len(set(type_ids)):
        raise ValueError("Duplicate type IDs detected in type_rows")

    if len(type_names) != len(set(type_names)):
        raise ValueError("Duplicate type names detected in type_rows")

    # Build lookup map
    type_to_id = {}
    for row in type_rows:
        type_to_id[row["name"]] = row["id"]
    logging.debug(f"Type-to-ID map: {type_to_id}")

    # Build mapping table
    pokemon_type_rows = []
    for pokemon_id, type_name in pokemon_type_links:
        pokemon_type_rows.append({
            "pokemon_id": pokemon_id,
            "type_id": type_to_id[type_name]
        })

    # Duplicate detection in mapping table (pokemon_id, type_id)
    mapping_pairs = []

    for row in pokemon_type_rows:
        pair = (row["pokemon_id"], row["type_id"])
        mapping_pairs.append(pair)

    if len(mapping_pairs) != len(set(mapping_pairs)):
        raise ValueError("Duplicate (pokemon_id, type_id) pairs detected in mapping table")

    logging.info("Finished building type rows and mapping rows.")
    return type_rows, pokemon_type_rows


# ---------------------------------------------------------------------
# VALIDATION 
# ---------------------------------------------------------------------
def validate_data(
    pokemon_rows: list[dict],
    type_rows: list[dict],
    pokemon_type_links: list[tuple[int, str]],
    pokemon_count: int
) -> None:
    """
    Validate integrity of all normalized Pokemon datasets before saving.


    1. Pokemon table validation:
       Ensures at least one Pokemon record exists.

    2. Type table validation:
       Ensures at least one Pokemon type exists.

    3. Pokemon–type mapping validation:
       Verifies that every expected Pokemon ID (1..pokemon_count) 
       appears at least once in pokemon_type_links.
    """
    logging.info("Validating normalized data...")

    # Check empty tables
    if not pokemon_rows:
        raise ValueError("pokemon_rows is empty — no Pokemon data fetched.")

    if not type_rows:
        raise ValueError("type_rows is empty — no Pokemon types extracted.")

    # Check that each Pokemon ID appears at least once in mapping
    seen_ids = set()
    for pair in pokemon_type_links:
        seen_ids.add(pair[0])

    for p in range(1, pokemon_count + 1):
        if p not in seen_ids:
            raise ValueError(f"Pokemon with ID {p} has no types.")

    logging.info("Validation completed successfully.")


# ---------------------------------------------------------------------
# AGGREGATION
# ---------------------------------------------------------------------
def compute_type_averages(pokemon_df: pd.DataFrame,
                          type_df: pd.DataFrame,
                          pokemon_types_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute the average Pokémon weight per type.

    This function merges:
        - pokemon_df (id, weight, ...)
        - type_df (id, name)
        - pokemon_types_df (pokemon_id, type_id)

    Steps:
        1. Merge mapping with Pokemon table to attach weights.
        2. Merge the result with type table to attach type names.
        3. Group by type name and compute mean weight.
        4. Return a DataFrame: type_name, avg_weight.

    Returns:
        pd.DataFrame: Aggregated table of average weights per Pokémon type.
    """
    logging.info("Computing average Pokemon weight per type...")

    # Join pokemon_types with pokemon
    merged1 = pokemon_types_df.merge(
        pokemon_df,
        left_on="pokemon_id",
        right_on="id",
        how="inner"
    )

    # Join with type table
    merged2 = merged1.merge(
        type_df,
        left_on="type_id",
        right_on="id",
        how="inner",
        suffixes=("_pokemon", "_type")
    )

    logging.debug(f"Merged DataFrame shape (for averages): {merged2.shape}")

    # Group by type name and compute average weight
    grouped = (
        merged2.groupby("name_type")["weight"]
               .mean()
               .reset_index()
               .rename(columns={"name_type": "type_name",
                                "weight": "avg_weight"})
    )

    logging.info("Computed averages for %d types:", len(grouped))

    formatted_rows = grouped.to_string(
        index=False,
        justify="left",
        formatters={"avg_weight": lambda x: f"{x:,.2f}"}
    )

    logging.info("\n" + formatted_rows)

    return grouped