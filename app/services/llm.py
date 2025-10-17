def classify_lt(text: str) -> dict:
    t = (text or "").strip().lower()
    if t in {"stop", "ne", "atsisakyti", "nedomina"}:
        return {"intent": "not_interested", "confidence": 0.99}
    if "ne tas" in t or "wrong" in t:
        return {"intent": "wrong_contact", "confidence": 0.9}
    return {"intent": "apply", "confidence": 0.6}
