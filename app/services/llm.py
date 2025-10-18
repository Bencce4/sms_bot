import os
from typing import List, Dict

# openai>=1.0
from openai import OpenAI

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")

client = None
if OPENAI_API_KEY:
    client = OpenAI(api_key=OPENAI_API_KEY)

SYSTEM_LT = """Tu esi lietuviškai bendraujantis draugiškas personalo asistentas, rašantis
paprastai, žmogiškai ir trumpai. Kontekstas: atranka statybų darbams.
Tikslas: išsiaiškinti ar žmogui įdomu, miestas, patirtis (metais), kada gali kalbėti/dirbti.
Stilius: natūralus, be robotikos. Neperkrauti klausimais – vienas klausimas vienu metu.
Jei žmogus aiškiai nenori – sustok mandagiai.
"""

def generate_reply_lt(context: List[Dict], user_text: str) -> str:
    """
    context: list of {"role":"user"/"assistant","content":"..."} past turns (optional)
    user_text: latest inbound text from candidate
    returns: assistant reply in Lithuanian (1–2 sentences)
    """
    if not client:
        # fallback for dev if no key present
        return "Puiku, supratau. Parašykite miestą ir kiek metų patirties turite."

    messages = [{"role":"system","content": SYSTEM_LT}]
    messages += context
    messages.append({"role":"user","content": user_text})

    resp = client.chat.completions.create(
        model=LLM_MODEL,
        messages=messages,
        temperature=0.4,
        max_tokens=120,
    )
    return (resp.choices[0].message.content or "").strip()

def classify_lt(text: str) -> Dict:
    """
    Keep your simple regex classifier for gates, but you can swap to LLM later if needed.
    """
    t = (text or "").lower()
    if any(w in t for w in ["stop","atsisakyti","nedomina","ramyb", "nenoriu"]):
        return {"intent":"not_interested","confidence":0.9}
    if any(w in t for w in ["ne tas numeris","ne aš","neteisingas","ne šis"]):
        return {"intent":"wrong_contact","confidence":0.7}
    if any(w in t for w in ["domina","taip","ok","gerai","skambink","kada","galima"]):
        return {"intent":"apply","confidence":0.6}
    return {"intent":"unknown","confidence":0.4}
