# app/services/llm.py
import os, json, re
from datetime import datetime
from typing import List, Dict, Any
from sqlalchemy import desc

from openai import OpenAI
from app.storage.db import SessionLocal
from app.storage.models import Thread, Message

MODEL = os.getenv("LLM_MODEL", "gpt-5-mini")
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

SYSTEM_LT = (
    "Tu – mandagus, šiltas, trumpai rašantis lietuvių kalbos asistentas, bendraujantis kaip žmogus, "
    "be šablonų. Nenaudok emoji. Vienas tikslas: užmegzti žmogišką pokalbį apie darbą, "
    "užduoti natūralų kitą klausimą, arba mandagiai užbaigti, jei žmogus nebesidomi."
)

FEWSHOTS = [
    # not interested
    {
        "role": "user", "content": "ne, nedomina"
    },
    {
        "role": "assistant",
        "content": '{"intent":"not_interested","reply":"Supratau, daugiau netrukdysiu. Jei persigalvotumėte – parašykite. Gražios dienos!"}'
    },
    # wants info
    {
        "role": "user", "content": "domina, ką reikia daryti?"
    },
    {
        "role": "assistant",
        "content": '{"intent":"questions","reply":"Faina! Kuriame mieste ieškote ir kiek turite patirties (apytiksliai metais)?"}'
    },
    # small-talk/unsure
    {
        "role": "user", "content": "gal šiek tiek"
    },
    {
        "role": "assistant",
        "content": '{"intent":"maybe","reply":"Supratau. Kuris miestas jums patogus ir kokios srities darbo labiau norėtumėte?"}'
    },
]

INTENT_STOP_SET = {"stop","not_interested","unsubscribe","do_not_contact","no"}

def _as_json(text: str) -> Dict[str, Any]:
    """Extract a JSON object from model output, tolerate stray text."""
    m = re.search(r'\{.*\}', text, flags=re.S)
    if not m:
        return {"intent":"questions","reply":"Gal galite parašyti miestą ir kiek turite patirties (metais)?"}
    try:
        data = json.loads(m.group(0))
        if "reply" not in data:  # safety
            data["reply"] = "Gal galite parašyti miestą ir kiek turite patirties (metais)?"
        if "intent" not in data or not data["intent"]:
            data["intent"] = "questions"
        return data
    except Exception:
        return {"intent":"questions","reply":"Gal galite parašyti miestą ir kiek turite patirties (metais)?"}

def _thread_history(phone: str, limit: int = 6) -> List[Dict[str,str]]:
    db = SessionLocal()
    try:
        t = db.query(Thread).filter_by(phone=phone, status="open").first()
        if not t:
            return []
        msgs = (db.query(Message)
                  .filter(Message.thread_id==t.id)
                  .order_by(desc(Message.ts))
                  .limit(limit)
                  .all())
        out = []
        for m in reversed(msgs):
            role = "assistant" if m.dir=="out" else "user"
            out.append({"role": role, "content": m.body})
        return out
    finally:
        db.close()

def classify_and_reply_lt(phone: str, latest_user_text: str) -> Dict[str, Any]:
    """
    Returns: {"intent": "...", "reply": "..."}
    Works with gpt-5-mini (no temperature / max_tokens).
    """
    messages = [{"role":"system","content":SYSTEM_LT}]
    messages += FEWSHOTS
    # short instruction for JSON output
    messages.append({
        "role":"system",
        "content":"Atsakyk **tik** vienu JSON objektu: {\"intent\":\"...\",\"reply\":\"...\"}. Be jokių paaiškinimų."
    })
    messages += _thread_history(phone)
    messages.append({"role":"user","content": latest_user_text.strip()})

    resp = client.chat.completions.create(
        model=MODEL,
        messages=messages,
        # no temperature/max_tokens for gpt-5-mini
    )
    text = resp.choices[0].message.content or ""
    data = _as_json(text)
    # normalize intent
    intent = (data.get("intent") or "").strip().lower()
    if intent in {"ne", "nedomina"}:
        intent = "not_interested"
    data["intent"] = intent
    return data
