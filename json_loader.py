import json
from pathlib import Path

def load_json(file_path: str) -> dict:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Input JSON not found: {file_path}")

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
