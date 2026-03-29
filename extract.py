import requests

def fetch_pokemon(pokemon_id: int, base_url: str, verify_ssl: bool, timeout: int) -> dict:
    """Fetch a single Pokémon JSON object from the API with network safety (timeout)."""
    url = f"{base_url}{pokemon_id}"

    response = requests.get(url, verify=verify_ssl, timeout=timeout)
    response.raise_for_status()

    return response.json()