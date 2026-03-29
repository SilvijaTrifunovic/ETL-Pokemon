import os
import logging
import json
import pandas as pd

def build_path(folder: str, filename: str) -> str:
    return os.path.join(folder, filename)

# Saves raw JSON data to a file inside data_dir/raw/.
def save_raw_json(data, data_dir, name):
    raw_dir = build_path(data_dir, "raw")
    os.makedirs(raw_dir, exist_ok=True)

    file_path = build_path(raw_dir, f"{name}.json")

    try:
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)
        logging.debug(f"Saved raw JSON to {file_path}")
    except Exception as e:
        logging.error(f"Failed to save raw JSON {name}: {e}")

    return file_path


def save_csv(
    data: pd.DataFrame | list[dict], 
    output_dir: str, 
    filename: str
) -> str | None:
    """
    Save data to CSV inside output_dir.

    `data` can be:
        - list of dicts   → will be converted to DataFrame
        - pandas DataFrame → will be written directly
    """
    file_path = build_path(output_dir, filename)

    # ✅ Determine format of the input
    if isinstance(data, pd.DataFrame):
        df = data
    elif isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        logging.error(
            f"save_csv() expected DataFrame or list of dicts, got {type(data)}"
        )
        return None
    
    if df.empty:
        logging.warning(
            f"CSV '{filename}' has 0 rows — writing empty file. "
        )

    try:
        df.to_csv(file_path, index=False)
        logging.info(f"Saved CSV to {file_path}")
    except Exception as exc:
        logging.error(f"Failed to save CSV '{filename}': {exc}")
        return None

    return file_path


# Load a CSV file from output_dir and return a DataFrame or None if failed.
def load_csv(output_dir: str, filename: str) -> pd.DataFrame | None:
    file_path = build_path(output_dir, filename)

    try:
        df = pd.read_csv(file_path)
        logging.info(f"Loaded CSV from {file_path}, shape={df.shape}")        
        return df        
    except Exception as exc:
        logging.error(f"Failed to read CSV at path {file_path}: {exc}")
        return None