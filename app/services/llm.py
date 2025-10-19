# app/services/llm.py
import os, json, re
from typing import List, Dict, Any, Optional
from sqlalchemy import desc, asc

from openai import OpenAI
from app.storage.db import SessionLocal
from app.storage.models import Thread, Message

MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def _call(messages):
    return client.chat.completions.create(model=MODEL, messages=messages)

SYSTEM_PROMPT = """You are a human-sounding recruiter for Valandinis (valandinis.lt).

About Valandinis (public info; do not invent specifics):
– Flexible hourly/shift work in construction and related trades across Lithuania (e.g., electrical, plumbing, concrete, roadworks, helpers, light production/retail/hospitality as available).
– Quick onboarding and simple process; offers follow after a short intro and agreement.
– Payment is hourly for actual hours worked; better ratings and own tools can improve opportunities.

Goal
– Build warm rapport and learn if the person is interested in working via Valandinis.
– If open, ask exactly ONE thing per message in this order: (1) city/region, (2) trade/specialty, (3) years of experience, (4) preferred schedule or start date, (5) own tools/car, (6) expected hourly range (no promises).
– If hesitant, give a single gentle nudge that fits (flexible shifts, quick start, simple process). Do not repeat nudges.
– If clearly not interested or asks to stop, thank and end.

Use the full conversation above
– Read the ENTIRE chat so far and **do not repeat** questions that the user already answered (e.g., if they told city or years, acknowledge and move on).
– If the user’s answer was unclear, briefly clarify once; otherwise proceed to the next item.

Language
– Detect from the user’s latest message.
– Reply in the same language if it’s Lithuanian, Russian, English or Ukrainian; otherwise reply in Lithuanian.

Style (SMS)
– <= 160 characters. One short message. Natural, friendly, slightly proactive, never bureaucratic, never pushy.
– Use their name only if they shared it.
– Ask exactly ONE question per message.

Accuracy & Safety
– Do not invent pay rates, exact roles or schedules unless provided in context. If asked for specifics, say they’re confirmed after a short intro and keep the chat going.
– Never ask for sensitive data (ID, card, passwords, full address, emails, login codes).
– If they say stop / unsubscribe / not interested, end politely.

Output
– Return only the message text to send (no JSON, no markdown, no explanations)."""

# We keep FEWSHOTS empty to let the model be flexible and human.
FEWSHOTS: List[Dict[str, str]] = []

def _thread_history(phone: str, limit: Optional[int] = None) -> List[Dict[str, str]]:
    """
    Return the FULL thread history for this open phone thread, oldest -> newest.
    If limit is given, cap at that many most-recent messages.
    """
    db = SessionLocal()
    try:
        t = db.query(Thread).filter_by(phone=phone, status="open").first()
        if not t:
            return []
        q = (db.query(Message)
               .filter(Message.thread_id == t.id)
               .order_by(asc(Message.ts)))
        if limit is not None:
            q = q.limit(limit)
        msgs = q.all()
        out: List[Dict[str, str]] = []
        for m in msgs:
            role = "assistant" if m.dir == "out" else "user"
            out.append({"role": role, "content": m.body})
        return out
    finally:
        db.close()

def _build_messages(ctx: dict, text: str):
    msgs = [{"role": "system", "content": SYSTEM_PROMPT}]
    # include the ENTIRE open-thread history
    if ctx and ctx.get("msisdn"):
        msgs += _thread_history(ctx["msisdn"], limit=None)  # full history
    # (Optional few-shots left empty on purpose)
    if FEWSHOTS:
        for ex in FEWSHOTS:
            msgs.append({"role": "user", "content": ex["user"]})
            msgs.append({"role": "assistant", "content": ex["assistant"]})
    # Add the latest incoming user message last
    ctx_str = ""
    if ctx:
        safe = {k: v for k, v in ctx.items() if k in ("msisdn", "campaign", "role") and v}
        if safe:
            ctx_str = f"Context: {safe}. "
    msgs.append({"role": "user", "content": f"{ctx_str}{text}"})
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
