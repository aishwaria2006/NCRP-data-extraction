import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List
import logging
import pandas as pd

# ================= LOGGING =================

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# =================================================
# ================= MEMBER 3 ======================
# =================================================

class Member3LegalMappingAgent:
    """
    Member-3:
    - Consumes Member-2 classified outputs
    - Maps legal sections
    - Generates FIR drafts
    - Adds intelligence & action plan
    """

    def __init__(self):
        self.legal_db = self._init_legal_db()
    def normalize_keys(self, row: dict) -> dict:
        return {
            "complaint_id": row.get("Complaint ID"),
            "complainant_name": row.get("Complainant Name"),
            "district": row.get("District"),
            "crime_category": row.get("Crime Category"),
            "crime_type": row.get("Crime Type"),
            "risk_level": row.get("Risk Level"),
            "total_fraud_amount": row.get("Amount Lost "),
            **row
        }

    def _init_legal_db(self):
        return {
            "Financial Cyber Fraud": {
                "IT": ["66C", "66D"],
                "IPC": ["420", "406"]
            },
            "UPI Fraud": {
                "IT": ["66C", "66D"],
                "IPC": ["420"]
            },
            "Phishing": {
                "IT": ["66C", "66D", "43"],
                "IPC": ["419", "420"]
            },
            "Identity Theft": {
                "IT": ["66C"],
                "IPC": ["419"]
            },
            "Social Media Fraud": {
                "IT": ["66D"],
                "IPC": ["420"]
            }
        }

    # ------------ LEGAL MAPPING ------------

    def map_legal_sections(self, complaint: Dict) -> Dict:
        crime = complaint.get("crime_category") or complaint.get(
            "crime_type", "Financial Cyber Fraud"
        )
        risk = complaint.get("risk_level", "MEDIUM")

        sections = self.legal_db.get(
            crime, self.legal_db["Financial Cyber Fraud"]
        )

        return {
            "crime_category": crime,
            "applicable_it_act": sections["IT"],
            "applicable_ipc": sections["IPC"],
            "all_sections": sections["IT"] + sections["IPC"],
            "risk_level": risk,
            "mapped_at": datetime.now().isoformat()
        }

    # ------------ INTELLIGENCE ------------

    def intelligence_score(self, complaint: Dict) -> Dict:
        score = 0
        risk = complaint.get("risk_level", "LOW")
        amount = complaint.get("total_fraud_amount", 0)

        score += {"HIGH": 40, "MEDIUM": 25, "LOW": 10}.get(risk, 10)
        score += 30 if amount > 100000 else 20 if amount > 50000 else 10 if amount > 10000 else 0

        threat = "CRITICAL" if score >= 80 else "HIGH" if score >= 60 else "MEDIUM"

        return {
            "intelligence_score": score,
            "threat_level": threat,
            "suspected_organized_crime": score >= 70
        }

    # ------------ FIR DRAFT ------------

    def generate_fir(self, complaint: Dict, legal: Dict) -> Dict:
        amount = complaint.get("total_fraud_amount", 0)

        fir_text = f"""
FIRST INFORMATION REPORT (FIR)

Complainant Name:
{complaint.get('complainant_name', 'Unknown')}

District:
{complaint.get('district', 'Unknown')}

Nature of Offence:
Cyber offence involving {complaint.get('crime_category')} resulting in loss of ₹{amount:,.2f}

Legal Sections Invoked:
IT Act: {', '.join(legal.get('applicable_it_act', []))}
IPC: {', '.join(legal.get('applicable_ipc', []))}

Prayer:
Kindly register FIR and initiate investigation.

Date:
{datetime.now().strftime('%d-%m-%Y')}
"""

        return {
            "fir_ready": True,
            "fir_text": fir_text.strip(),
            "sections_invoked": legal.get("all_sections", [])
        }

    # ------------ ACTION PLAN ------------

    def action_plan(self, complaint: Dict) -> Dict:
        risk = complaint.get("risk_level", "LOW")

        timeline = [
            "Register FIR",
            "Collect bank transaction records",
            "Preserve digital evidence"
        ]

        if risk == "HIGH":
            timeline.insert(0, "Freeze bank accounts within 6 hours")

        return {
            "officer_level": "DCP Cyber Crime" if risk == "HIGH" else "Inspector",
            "timeline": timeline,
            "evidence_checklist": [
                "Bank statements",
                "Transaction IDs",
                "Screenshots",
                "SMS / Email headers",
                "Platform account metadata"
            ]
        }

    # ------------ FULL PIPELINE ------------

    def process_complaint(self, complaint: Dict) -> Dict:
        legal = self.map_legal_sections(complaint)
        intel = self.intelligence_score(complaint)
        fir = self.generate_fir(complaint, legal)
        plan = self.action_plan(complaint)

        complaint["legal_mapping"] = legal
        complaint["intelligence"] = intel
        complaint["fir_draft"] = fir
        complaint["action_plan"] = plan
        complaint["member3_processed_at"] = datetime.now().isoformat()

        return complaint

# =================================================
# ================= MAIN ==========================
# =================================================

def run_member3_pipeline():
    INPUT_DIR = Path("data/output/category_wise")
    OUTPUT_DIR = Path("data/output/legal_enriched")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    agent = Member3LegalMappingAgent()

    for file in INPUT_DIR.glob("*.xlsx"):
        logger.info(f"📥 Processing Member-2 output: {file.name}")
        df = pd.read_excel(file)


        enriched = []
        for _, row in df.iterrows():
            complaint = agent.normalize_keys(row.to_dict())


            complaint = agent.process_complaint(complaint)
            enriched.append(complaint)

        output_excel = OUTPUT_DIR / f"LEGAL_{file.name}"
        pd.DataFrame(enriched).to_excel(output_excel, index=False)

        output_json = OUTPUT_DIR / f"LEGAL_{file.stem}.json"
        with open(output_json, "w", encoding="utf-8") as f:
            json.dump(enriched, f, indent=2, ensure_ascii=False)

        logger.info(f"✅ Legal enrichment completed for {file.name}")

    logger.info("🎉 MEMBER-3 PIPELINE COMPLETED SUCCESSFULLY")

if __name__ == "__main__":
    run_member3_pipeline()
