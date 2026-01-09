def classify(complaint):
    crime_type = complaint.get("crime_type", "").lower()

    FINANCIAL = [
        "upi", "atm", "credit card", "loan", "investment",
        "crypto", "ticket"
    ]

    SOCIAL_MEDIA = [
        "social media", "dating", "customer care", "job"
    ]

    IDENTITY = [
        "identity", "sim swap", "otp", "phishing"
    ]

    ECOMMERCE = [
        "online shopping", "e-commerce"
    ]

    if any(key in crime_type for key in FINANCIAL):
        category = "Financial Cyber Fraud"
    elif any(key in crime_type for key in SOCIAL_MEDIA):
        category = "Social Media & Platform Crime"
    elif any(key in crime_type for key in IDENTITY):
        category = "Identity & Credential Crime"
    elif any(key in crime_type for key in ECOMMERCE):
        category = "E-Commerce Fraud"
    else:
        category = "Others"

    return {
        "crime_type": complaint.get("crime_type"),
        "crime_category": category
    }
