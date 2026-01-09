def simulate_decision(summary: dict, classification: dict) -> dict:
    amount = summary.get("amount_lost", 0)
    delay = 0 if summary.get("date_time") else 2

    priority_score = min(100, int((amount / 1000) + (10 * delay)))

    if priority_score >= 80:
        action = "Immediate bank escalation and account freeze"
        risk = "HIGH"
    elif priority_score >= 50:
        action = "Verify transactions and notify bank"
        risk = "MEDIUM"
    else:
        action = "Monitor case"
        risk = "LOW"

    return {
        "priority_score": priority_score,
        "risk_level": risk,
        "recommended_action": action
    }
