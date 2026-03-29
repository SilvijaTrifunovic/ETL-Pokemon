[![Python](https://img.shields.io/badge/Python-3.10+-blue)](https://www.python.org/)   [![Process](https://img.shields.io/badge/Process-ETL-brightgreen)](#etl-architecture)  [![Status](https://img.shields.io/badge/Status-Active-success)](#)  

# Pokemon ETL Pipeline
A modular Python ETL pipeline that extracts Pokémon data from the PokeAPI, transforms it into structured tabular datasets, validates dataset integrity, and produces analytics such as average Pokémon weight per type. This project demonstrates clean Data Engineering architecture, reproducibility, and separation of concerns.

---

## Introduction

This ETL pipeline was designed as a professional-style Data Engineering project. It showcases:

- Extract (API calls with timeout & SSL control)  
- Transform (normalization, type extraction, mapping, validation, aggregation)  
- Load (CSV output, raw JSON preservation)  
- Configuration-driven execution  
- Modular and testable code structure  

The pipeline produces normalized datasets and analytics suitable for downstream analysis or BI workflows.

---

## Configuration (`config.json`)

Example:

```json
{
  "base_url": "https://pokeapi.co/api/v2/pokemon/",
  "verify_ssl": false,
  "pokemon_count": 150,
  "pokemon_csv": "pokemon.csv",
  "type_csv": "type.csv",
  "pokemon_types_csv": "pokemon_types.csv",
  "type_averages_csv": "type_averages.csv",
  "save_raw": false,
  "timeout": 10
}
```

Notes:
- `"base_url"` is required.  
- `"save_raw": true` will save raw API responses into `data/raw/`.  
- `"timeout"` controls the API request timeout in seconds.

---

## Dependencies
- `"requests"`
- `"pandas"` 
- `"urllib3"`


Install dependencies:

```bash
pip install -r requirements.txt
```


---

## Running the Pipeline

Execute:

```bash
python main.py
```

The pipeline will:

1. Fetch Pokémon data  
2. Normalize and extract attributes  
3. Build Pokémon–Type relationships  
4. Validate dataset consistency  
5. Save CSV outputs  
6. Compute average Pokémon weight per type  

All output files are written into `data/output/`.

---

## Output Files (`data/output/`)

The following CSVs are generated:

| File | Description |
|------|-------------|
| `pokemon.csv` | Normalized Pokémon attributes (id, name, height, weight, species) |
| `type.csv` | List of unique Pokémon types (id + name) |
| `pokemon_types.csv` | Many-to-many relationship between Pokémon and Types |
| `type_averages.csv` | Aggregated average Pokémon weight per type |

---

## ETL Architecture

The project follows a clean and modular ETL structure:

```
Extract  →  Transform  →  Load  →  Analyze
```

### extract.py  
- Fetches Pokemon data by ID  
- Uses requests.get with timeout and SSL settings  
- Returns raw JSON for each Pokémon  

### transform.py  
- Normalizes JSON into tabular form  
- Extracts Pokemon types  
- Builds deterministic type mappings  
- Creates Pokemon-to-Type relationships  
- Validates dataset integrity  
- Computes average weight per type  

### load.py  
- Saves CSV outputs  
- Saves optional raw JSON  
- Loads CSV files for the aggregation step  
- Supports list-of-dicts and pandas DataFrame inputs  

### main.py  
- Pipeline orchestrator  
- Loads configuration  
- Runs extract → transform → load workflow  
- Handles fetching, transforming, validation, loading, and aggregation  

### config.py  
- Loads and validates config.json  
- Ensures required fields and correct types  


---

## Data Validation

Validation ensures:

- Pokémon dataset is not empty  
- Type dataset is not empty  
- Every Pokémon from 1..N has at least one associated type  
- Duplicate type IDs/names or duplicate Pokémon–Type pairs are detected during transform phase  

Any validation error stops the pipeline with a clear, descriptive message.

---

## Project Structure

```
pokemon-etl/
│
├── extract.py
├── transform.py
├── load.py
├── main.py
├── config.py
├── config.json
├── requirements.txt
├── README.md
│
└── data/
    ├── raw/        # ignored by Git
    └── output/     # ignored by Git
```

All generated data is ignored via `.gitignore` to keep the repository clean.

---

## Example Logs (Formatted)

During aggregation, the pipeline prints human‑readable formatted analytics:

```
Computed averages for 17 types:

type_name   avg_weight
bug         229.92
dragon      766.00
electric    317.89
...
```

## Summary
This project provides a compact, clean, and functional ETL pipeline for PokeAPI data, demonstrating modular design, validation, logging, and reproducible outputs. 
