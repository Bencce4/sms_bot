import os, json, re
from typing import List, Dict
from sqlalchemy import desc
from openai import OpenAI

from app.storage.db import SessionLocal
from app.storage.models import Thread, Message

# ==== Model / client ====
MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ==== System prompt (kept compact; rely on our interceptor for offer-questions) ====
SYSTEM_PROMPT = """You are a friendly, human-sounding SMS recruiter for Valandinis (valandinis.lt).

About Valandinis (do not invent specifics):
– Flexible hourly/shift work in construction & related trades across Lithuania (e.g., electricians, plumbers, concrete, roadworks, helpers; sometimes light production/retail/hospitality).
– Quick start and simple process after a short intro.

Goal
– Build warm rapport and qualify interest.
– Ask exactly ONE short question per message; prefer this order: (1) city/region, (2) specialty/trade, (3) years of experience, (4) availability (schedule or start date).
– If the person asks for details/“what do you offer?”, give ONE brief value sentence, then proceed to the next missing qualifier. Do NOT repeat the value sentence in later replies.

Language
– Detect from the latest user message; reply in LT/RU/EN/UA accordingly; otherwise default to LT.

Style / SMS
– ≤160 chars, natural and polite. Avoid bureaucratic words. Never use “role”; prefer “specialybė / sritis / kuo dirbate?”. Never say “rekruteris”.

Safety
– Don’t invent pay/clients/locations. If asked, say details are shared later by phone and continue qualification.
– Never request sensitive data (ID, card, passwords, exact address, emails, codes).

Output
– Return only the message text (no JSON/markdown/explanations).
"""

# ==== Fewshots disabled ====
FEWSHOTS: List[Dict[str,str]] = []

# ==== Helpers: DB history ====
def _thread_history(phone: str, limit: int = 12) -> List[Dict[str,str]]:
    db = SessionLocal()
    try:
        t = db.query(Thread).filter_by(phone=phone, status="open").first()
        if not t:
            return []
        msgs = (db.query(Message)
                  .filter(Message.thread_id == t.id)
                  .order_by(desc(Message.ts))
                  .limit(limit)
                  .all())
        out = []
        for m in reversed(msgs):
            role = "assistant" if m.dir == "out" else "user"
            out.append({"role": role, "content": m.body})
        return out
    finally:
        db.close()

# ==== History scanners for “what’s already collected” ====
_RX_CITY   = re.compile(r'\b(miest|region)\w*', re.I)
_RX_SPEC   = re.compile(r'\b(specialyb|srit|elektrik|santechn|mūrin|murin|beton|pagalbin|mechanik)\w*', re.I)
_RX_EXP    = re.compile(r'\b(\d+)\s*(m(?:et[au]|\.)?|metai|yr|years?)\b', re.I)
_RX_AVAIL  = re.compile(r'\b(pradėt|pradėsiu|start|nuo|grafik)\w*', re.I)

def _pairwise(history: List[Dict[str,str]]):
    for i in range(len(history)-1):
        yield history[i], history[i+1]

def _has_answered_city(history):
    for a,b in _pairwise(history):
        if a["role"]=="assistant" and _RX_CITY.search(a["content"]):
            if b["role"]=="user" and len(b["content"].strip())>0:
                return True
    return False

def _has_answered_spec(history):
    for a,b in _pairwise(history):
        if a["role"]=="assistant" and (_RX_SPEC.search(a["content"]) or "specialyb" in a["content"].lower()):
            if b["role"]=="user" and len(b["content"].strip())>0:
                return True
    return False

def _has_answered_exp(history):
    # either assistant asked or user volunteered years in any message
    asked = any(a["role"]=="assistant" and _RX_EXP.search(a["content"]) for a in history)
    if asked:
        for a,b in _pairwise(history):
            if a["role"]=="assistant" and _RX_EXP.search(a["content"]):
                if b["role"]=="user" and len(b["content"].strip())>0:
                    return True
    # or any user message looks like experience (“8 m”, “10 years”)
    return any(m["role"]=="user" and _RX_EXP.search(m["content"]) for m in history)

def _has_answered_avail(history):
    for a,b in _pairwise(history):
        if a["role"]=="assistant" and _RX_AVAIL.search(a["content"]):
            if b["role"]=="user" and len(b["content"].strip())>0:
                return True
    return False

def _next_missing_question(history, lang="lt"):
    # Choose next missing field
    if not _has_answered_city(history):
        return "Kuriuose miestuose ar regionuose galite dirbti?"
    if not _has_answered_spec(history):
        return "Kokia jūsų specialybė ar sritis?"
    if not _has_answered_exp(history):
        return "Kiek metų patirties turite šioje srityje?"
    if not _has_answered_avail(history):
        return "Kada galėtumėte pradėti arba koks grafikas būtų tinkamas?"
    # Otherwise close politely
    return "Ačiū! Susisieksime dėl detalių."

# ==== Offer-question interceptor ====
OFFER_TRIGGERS = [
    "ką siūlot", "ka siulot", "ką turite", "ka turite",
    "ką galite pasiūlyti", "o ką siūlote", "what do you offer",
    "tell me more", "more info", "what can you offer"
]

def _is_offer_question(text: str) -> bool:
    tl = (text or "").strip().lower()
    return any(p in tl for p in OFFER_TRIGGERS)

def _value_sentence_once(history: List[Dict[str,str]]) -> str:
    # Avoid repeating value line if it appeared in last few turns
    seen = "lankst" in " ".join(m["content"].lower() for m in history if m["role"]=="assistant")
    return "" if seen else "Lankstūs grafikai, greitas startas, paprasta eiga. "

def _polite(text: str) -> str:
    t = text or ""
    # Never “dirbat” → “dirbate”
    t = re.sub(r'\bdirbat(e)?\b', 'dirbate', t, flags=re.I)
    # Prefer “specialybė/sritis” phrasing
    t = re.sub(r'\bKą\s+dirbate\??', 'Kokia jūsų specialybė ar sritis?', t, flags=re.I)
    # Keep it short
    return t.strip()

def _final_sms(s: str) -> str:
    s = re.sub(r'\s+', ' ', s).strip()
    if len(s) > 160:
        s = s[:157].rstrip() + "…"
    return s

# ==== OpenAI call ====
def _call(messages):
    return client.chat.completions.create(model=MODEL, messages=messages)

def _build_messages(ctx: dict, text: str) -> List[Dict[str,str]]:
    msgs = [{"role": "system", "content": SYSTEM_PROMPT}]
    if ctx and ctx.get("msisdn"):
        msgs += _thread_history(ctx["msisdn"], limit=12)
    for ex in FEWSHOTS:
        msgs.append({"role": "user", "content": ex["user"]})
        msgs.append({"role": "assistant", "content": ex["assistant"]})
    msgs.append({"role": "user", "content": text})
    return msgs

# ==== Main generator with interceptor ====
def generate_reply_lt(ctx: dict, text: str) -> str:
    history = _thread_history(ctx.get("msisdn", ""), limit=12)

    # Intercept “what do you offer?” and make it deterministic (no repeats)
    if _is_offer_question(text):
        value = _value_sentence_once(history)
        question = _polite(_next_missing_question(history))
        return _final_sms((value + question).strip())

    # Otherwise, ask the model
    r = _call(_build_messages(ctx, text))
    reply = (r.choices[0].message.content or "").strip()

    # Polite tone + length
    reply = _polite(reply)
    return _final_sms(reply)

# ==== Classifier (unchanged, with a small fallback for offer-questions) ====
def classify_lt(text: str) -> dict:
    sys = (
        "Klasifikuok lietuvišką SMS į: 'questions', 'not_interested', arba 'other'. "
        "Grąžink JSON: {\"intent\": str, \"confidence\": 0..1}. "
        "Pvz.: 'nedomina' -> not_interested; klausimai apie darbą -> questions; kita -> other. "
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
        tl = (text or "").lower()
        if any(w in tl for w in ["nedomina", "nenoriu", "ne, ačiū", "aciu ne", "ne."]):
            return {"intent": "not_interested", "confidence": 0.8}
        if _is_offer_question(tl):
            return {"intent": "questions", "confidence": 0.8}
        return {"intent": "other", "confidence": 0.5}
