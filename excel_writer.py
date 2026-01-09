import pandas as pd
from pathlib import Path

def write_excel(enriched_json: dict, output_path: str):
    summary = enriched_json["summary"]
    agents = enriched_json["member2_outputs"]

    row = {
        "complaint_id": summary["complaint_id"],
        "complainant_name": summary["complainant_name"],
        "district": summary["district"],
        "crime_type": summary["crime_type"],
        "platform": summary["platform"],
        "amount_lost": summary["amount_lost"],
        "status": summary["status"],
        "crime_category": agents["classification"]["crime_category"],
        "risk_level": agents["decision_twin"]["risk_level"],
        "priority_score": agents["decision_twin"]["priority_score"],
        "recommended_action": agents["decision_twin"]["recommended_action"],
        "legal_sections": ", ".join(agents["legal_mapping"]["applicable_laws"])
    }

    df = pd.DataFrame([row])
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_path, index=False)
