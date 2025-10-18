import os, json, re
from typing import List, Dict, Any
from sqlalchemy import desc

from openai import OpenAI
from app.storage.db import SessionLocal
from app.storage.models import Thread, Message

# ✅ Use a real model; default to gpt-4o-mini
MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def _call(messages):
    # Keep it simple; no temperature/max_tokens (some models reject non-defaults)
    return client.chat.completions.create(model=MODEL, messages=messages)

SYSTEM_PROMPT = (
    "Tu – žmogiškai bendraujantis, draugiškas Lietuvos įmonės recruiter'is. "
    "Rašai kaip per SMS: trumpai, aiškiai, be biurokratijos, be šablonų. "
    "Tonas: mandagus, šiltas, šnekamas, truputį iniciatyvus (bet ne įkyrus). "
    "Tikslas: mandagiai išsiaiškinti ar žmogus DOMISI darbu; jei taip – paklausti MIESTO ir PATIRTIES metais; "
    "jei žmogus abejoja – pasiūlyk trumpai papasakoti, kas svarbu (pvz., grafikas, atlygis); "
    "jei žmogus aiškiai nebedomina / nenori – padėkok ir užbaik pokalbį. "
    "Nieko nekurk apie poziciją/įmonę, jei kontekste to nėra. "
    "Atsakymas ≤160 simbolių. Viena žinutė. Be kreipimosi vardu, jei jo pats žmogus neįvardijo."
)

FEWSHOTS = [
    {"user": "Gal būtų darbo?", "assistant": "Puiku! Kuris miestas būtų patogu ir kiek metų patirties turite?"},
    {"user": "Nežinau, gal šiek tiek.", "assistant": "Suprantu. Kas svarbiausia: miestas, grafikas ar atlygis? Jei tinka, parašykite miestą ir patirtį metais."},
    {"user": "Nedomina, ačiū.", "assistant": "Ačiū už žinutę, daugiau neberašysiu. Gražios dienos!"},
]

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

def _build_messages(ctx: dict, text: str):
    msgs = [{"role": "system", "content": SYSTEM_PROMPT}]
    # include last turns so the bot sounds consistent
    if ctx and ctx.get("msisdn"):
        msgs += _thread_history(ctx["msisdn"], limit=6)
    for ex in FEWSHOTS:
        msgs.append({"role": "user", "content": ex["user"]})
        msgs.append({"role": "assistant", "content": ex["assistant"]})
    ctx_str = ""
    if ctx:
        safe = {k: v for k, v in ctx.items() if k in ("msisdn", "campaign", "role") and v}
        if safe:
            ctx_str = f"Kontekstas: {safe}. "
    msgs.append({"role": "user", "content": f"{ctx_str}Žinutė: {text}"})
    return msgs

def generate_reply_lt(ctx: dict, text: str) -> str:
    r = _call(_build_messages(ctx, text))
    return (r.choices[0].message.content or "").strip()

def classify_lt(text: str) -> dict:
    sys = (
        "Klasifikuok lietuvišką SMS į: 'questions', 'not_interested', arba 'other'. "
        "Grąžink JSON: {\"intent\": str, \"confidence\": 0..1}. "
        "Pavyzdžiai:\n"
        "- \"nedomina\", \"ne, ačiū\", \"nenoriu\" -> not_interested\n"
        "- klausimai apie darbą -> questions\n"
        "- kita -> other\n"
        "Atsakyk tik JSON."
    )
    r = _call([
        {"role": "system", "content": sys},
        {"role": "user", "content": text},
    ])
    content = (r.choices[0].message.content or "").strip()
    try:
        obj = json.loads(content)
        intent = (obj.get("intent") or "").lower().strip()
        conf = float(obj.get("confidence") or 0.6)
        if intent not in {"questions", "not_interested", "other"}:
            intent = "other"
        return {"intent": intent, "confidence": conf}
    except Exception:
        tl = text.lower()
        if any(w in tl for w in ["nedomina", "nenoriu", "ačiū, ne", "aciu ne", " ne", "ne."]):
            return {"intent": "not_interested", "confidence": 0.8}
        return {"intent": "other", "confidence": 0.5}
