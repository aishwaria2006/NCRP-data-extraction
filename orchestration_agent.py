from agents.classification_agent import classify
from agents.legal_mapping_agent import map_legal_sections
from agents.decision_twin_agent import simulate_decision
from utils.logger import log_event

def orchestrate(complaint_json: dict) -> dict:
    summary = complaint_json["summary"]
    case_id = summary["complaint_id"]

    try:
        classification = classify(summary)
        legal = map_legal_sections(classification)
        decision = simulate_decision(summary, classification)

        complaint_json["member2_outputs"] = {
            "classification": classification,
            "legal_mapping": legal,
            "decision_twin": decision
        }

        log_event(case_id, "SUCCESS", "All agents executed successfully")

    except Exception as e:
        log_event(case_id, "FAILED", str(e))
        complaint_json["member2_outputs"] = {"error": str(e)}

    return complaint_json
