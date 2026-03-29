# Pokemon ETL Pipeline
This project implements a modular Python ETL pipeline for extracting, transforming, validating, loading, and analyzing Pokemon data using the official PokeAPI. The solution demonstrates clean ETL design with clear separation of concerns and reproducible outputs.

## Configuration (config.json)
Example:
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

Notes:
- base_url is required.
- Set "save_raw": true to store raw PokeAPI responses.
- timeout controls the API request timeout in seconds.

## Output Files
The pipeline generates the following CSV files in data/output/:
pokemon.csv          Normalized Pokemon data  
type.csv             List of Pokemon types  
pokemon_types.csv    Pokemon-to-Type mapping  
type_averages.csv    Average Pokemon weight per type  

If "save_raw" is enabled, raw API responses are saved to data/raw/.

## Dependencies
requests  
pandas  
urllib3  

Install with:
pip install -r requirements.txt

## Running the Pipeline
1. (Optional) Activate a virtual environment.
Windows:
venv\Scripts\activate

macOS/Linux:
source venv/bin/activate

2. Run the ETL process:
python main.py

## ETL Architecture
extract.py  
- Fetches Pokemon data by ID  
- Uses requests.get with timeout and SSL settings  

transform.py  
- Normalizes JSON into tabular form  
- Extracts Pokemon types  
- Builds deterministic type mappings  
- Creates Pokemon-to-Type relationships  
- Validates dataset integrity  
- Computes average weight per type  

load.py  
- Saves CSV outputs  
- Saves raw JSON  
- Loads CSV files for the aggregation step  
- Supports list-of-dicts and pandas DataFrame inputs  

main.py  
- Loads configuration  
- Orchestrates the ETL flow  
- Handles fetching, transforming, validation, loading, and aggregation  

config.py  
- Loads and validates config.json  
- Ensures required fields and correct types  

## Data Validation
The pipeline ensures:
- Pokemon data was successfully fetched  
- Types were extracted  
- Every Pokemon ID from 1 to N has at least one type  

Duplicate type names, IDs, and Pokemon-type pairs are detected during transformation. Any validation error stops the pipeline with a clear message.

## Summary
This project provides a compact, clean, and functional ETL pipeline for PokeAPI data, demonstrating modular design, validation, logging, and reproducible outputs. 