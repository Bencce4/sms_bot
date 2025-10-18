# app/services/llm.py
import os, json, re
from typing import List, Dict, Any
from sqlalchemy import desc

from openai import OpenAI
from app.storage.db import SessionLocal
from app.storage.models import Thread, Message

# Use a real model; default to gpt-4o-mini (overridable via env LLM_MODEL)
MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def _call(messages):
    # Keep it simple; some models reject custom temperature/max_tokens
    return client.chat.completions.create(model=MODEL, messages=messages)

SYSTEM_PROMPT = """You are a human-sounding recruiter for Valandinis (valandinis.lt).

About Valandinis (public info; do not invent specifics):
— Flexible hourly/shift work in construction and related trades across Lithuania
  (e.g., electrical, plumbing, concrete, roadworks, helpers; sometimes light production/retail/hospitality).
— Quick onboarding and simple process; offers follow after a short intro and agreement.
— Payment is hourly for actual hours worked; own tools/car and good track record can improve opportunities.

Primary goal
— Build warm rapport and learn if the person is interested in working via Valandinis.
— If open, progress through this list, asking EXACTLY ONE item per message:
   (1) city/region → (2) specialty → (3) years of experience →
   (4) preferred schedule or start date → (5) own tools/car → (6) expected hourly range (no promises).
— **Infer what’s already answered from the transcript** and **do not repeat**. Acknowledge and move on.
— If hesitant: one gentle nudge (flexible shifts, quick start, simple process). Do not repeat nudges.
— If clearly not interested: thank and end.

Language
— Detect from the user’s latest message.
— Reply in the same language if it’s Lithuanian, Russian, English, or Ukrainian; otherwise reply in Lithuanian.

Style (SMS)
— ≤160 characters. One short message. Natural, warm, human; slightly proactive; never bureaucratic or pushy.
— Use their name only if they shared it.
— Exactly ONE question per message. Avoid repetition; vary wording naturally.
— If the user jokes or writes an unrealistic number (e.g., “1000 years”), acknowledge lightly and ask for a realistic confirmation.

Accuracy & Safety
— Do not invent pay rates, exact roles or schedules unless given. If pressed for specifics, say details are confirmed after a short intro and keep the chat moving.
— Never ask for sensitive data (ID, card, passwords, full address, emails, login codes).
— If they say stop / unsubscribe / not interested, end politely.

Output
— Return only the message text to send (no JSON, no markdown, no explanations)."""

# We intentionally keep few-shots empty to let the model act freely with the rules above.
FEWSHOTS: List[Dict[str, str]] = []

def _thread_history(phone: str, limit: int = 10) -> List[Dict[str,str]]:
    db = SessionLocal()
    try:
        t = db.query(Thread).filter_by(phone=phone, status="open").first()
        if not t:
            return []
        msgs = (
            db.query(Message)
              .filter(Message.thread_id == t.id)
              .order_by(desc(Message.ts))
              .limit(limit)
              .all()
        )
        out: List[Dict[str, str]] = []
        for m in reversed(msgs):
            role = "assistant" if m.dir == "out" else "user"
            out.append({"role": role, "content": m.body})
        return out
    finally:
        db.close()

def _build_messages(ctx: dict, text: str):
    msgs = [{"role": "system", "content": SYSTEM_PROMPT}]

    # Include a compact transcript so the model can infer what's already answered.
    history = []
    if ctx and ctx.get("msisdn"):
        history = _thread_history(ctx["msisdn"], limit=8)
    if history:
        transcript_lines = []
        for m in history:
            who = "User" if m["role"] == "user" else "You"
            transcript_lines.append(f"{who}: {m['content']}")
        msgs.append({
            "role": "system",
            "content": "Transcript (most recent first):\n" + "\n".join(transcript_lines)
        })

    # (few-shots intentionally empty, but keep the hook if you add later)
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
    # Keep classifier simple; return {intent, confidence}
    sys = (
        "Klasifikuok SMS į: 'questions', 'not_interested', arba 'other'. "
        "Grąžink JSON: {\"intent\": str, \"confidence\": 0..1}. "
        "Pvz.: 'nedomina', 'ne, ačiū', 'nenoriu' -> not_interested; klausimai apie darbą -> questions; kita -> other. "
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
