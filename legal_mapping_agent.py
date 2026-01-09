def map_legal_sections(classification: dict) -> dict:
    # Base laws applicable to most cyber fraud cases
    laws = ["IT Act 66C", "IT Act 66D"]

    # Safely read risk hint
    risk = classification.get("risk_hint", "LOW")

    # Add stricter sections for high-risk cases
    if risk in ["HIGH", "CRITICAL"]:
        laws.extend([
            "IT Act 66F",
            "IPC 420"
        ])

    return {
        "applicable_laws": laws,
        "severity": risk
    }
