import pandas as pd
from pathlib import Path
from agents.classification_agent import classify
from agents.legal_mapping_agent import map_legal_sections
from agents.decision_twin_agent import simulate_decision

SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/1TMHxOjxoNDtgJByhW-n6ZFvMVP1nJJPiH50w0Z9pVdU/export?format=csv"
OUTPUT_DIR = Path("data/output/category_wise")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def main():
    df = pd.read_csv(SHEET_CSV_URL)

    category_buckets = {}

    for _, row in df.iterrows():
        summary = {
            "complaint_id": row["Complaint ID"],
            "crime_type": row["Crime Type"],
            "amount_lost": row["Amount Lost "],
            "date_time": row["Date & Time"]
        }

        classification = classify(summary)

        decision = simulate_decision(summary, classification)

        classification["risk_hint"] = decision.get("risk_level", "LOW")

        legal = map_legal_sections(classification)


        record = row.to_dict()
        record.update({
            "Crime Category": classification["crime_category"],
            "Risk Level": decision["risk_level"],
            "Priority Score": decision["priority_score"],
            "Recommended Action": decision["recommended_action"],
            "Legal Sections": ", ".join(legal["applicable_laws"])
        })

        bucket = classification["crime_category"]
        category_buckets.setdefault(bucket, []).append(record)

    for category, records in category_buckets.items():
        out_df = pd.DataFrame(records)
        out_file = OUTPUT_DIR / f"{category.replace(' ', '_')}.xlsx"
        out_df.to_excel(out_file, index=False)
        print(f"✅ Exported {out_file.name}")

if __name__ == "__main__":
    main()
