import json
import os


def load_config() -> dict:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, "config.json")


    with open(file_path, "r") as f:
        config = json.load(f)    
    
    # Validate required keys
    if "base_url" not in config:
        raise KeyError("Missing required config key: 'base_url'")

    # Extra validation (type checking)
    if not isinstance(config["base_url"], str):
        raise TypeError("base_url must be a string")

    if not isinstance(config["pokemon_count"], int):
        raise TypeError("pokemon_count must be an integer")

    if not isinstance(config["verify_ssl"], bool):
        raise TypeError("verify_ssl must be True/False")

    if not config["base_url"].endswith("/"):
        config["base_url"] += "/"

    config.setdefault("pokemon_count", 150)
    config.setdefault("verify_ssl", True)

    return config