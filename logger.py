import json
from datetime import datetime
from pathlib import Path

LOG_FILE = Path("data/logs/processing_logs.json")

def log_event(case_id: str, status: str, message: str):
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

    log_entry = {
        "case_id": case_id,
        "status": status,
        "message": message,
        "timestamp": datetime.utcnow().isoformat()
    }

    # ✅ Handle empty or non-existing log file safely
    if LOG_FILE.exists() and LOG_FILE.stat().st_size > 0:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            try:
                logs = json.load(f)
            except json.JSONDecodeError:
                logs = []
    else:
        logs = []

    logs.append(log_entry)

    with open(LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(logs, f, indent=2)
