# app/services/llm.py
import os, json, re, hashlib, unicodedata
from typing import List, Dict
from sqlalchemy import desc
from openai import OpenAI

from app.storage.db import SessionLocal
from app.storage.models import Thread, Message

# ==== Model / client ====
MODEL = os.getenv("LLM_REPLY_MODEL", os.getenv("LLM_MODEL", "gpt-4o-mini"))
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ==== System prompt (single source of truth) ====
SYSTEM_PROMPT = """You are a short, human-sounding SMS recruiting assistant for Valandinis (valandinis.lt).

About Valandinis (do not invent specifics)
– Flexible hourly/shift work in construction & related trades across Lithuania
  (electricians, plumbers, bricklayers/mūrininkai, roadworks, helpers; sometimes light production/retail/hospitality).
– Quick start and simple process after a short intro.

Primary goal
– Gauge interest and, if positive, collect only: (1) city/region, (2) specialty/trade, (3) years of experience, (4) availability.

How to respond (must follow)
1) LISTEN FIRST: answer the user’s message with one short STATEMENT (≤80 chars, NEVER a question).
   • If they ask “what do you offer?” (any wording), say exactly once per thread:
     “Siūlome lanksčius grafikus, greitą pradžią ir paprastą procesą įsidarbinant.”
2) Then ask exactly ONE next missing qualifier (from the set above), in this order:
   city/region → specialty/trade → years of experience → availability.
3) ONE SMS total. ≤160 chars. Natural, friendly, encouraging. No bureaucratic words.
4) Never repeat the same sentence/idea previously sent in this thread.
5) Safety: don’t invent pay/clients/locations; if asked, say details are shared later by phone and continue.
   Never request sensitive data (ID, card, passwords, exact address, emails, codes). No legal advice.
6) Language: mirror the user’s latest language (LT/RU/EN/UA). Else default LT.
7) Close when all 4 items are collected: “Perduosiu kolegai – paskambins dėl detalių.”

Output: return ONLY the SMS text. No JSON/markdown/explanations.
"""

# ==== Prompt fingerprint & debug ====
PROMPT_SHA = hashlib.sha256(SYSTEM_PROMPT.encode("utf-8")).hexdigest()[:12]
def prompt_info() -> str:
    return f"PROMPT_SHA={PROMPT_SHA} MODEL={MODEL}"
try:
    print(f"[llm] {prompt_info()}", flush=True)
except Exception:
    pass

# ==== DB history ====
def _thread_history(phone: str, limit: int = 12) -> List[Dict[str, str]]:
    if not phone:
        return []
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
        out = []
        for m in reversed(msgs):
            role = "assistant" if m.dir == "out" else "user"
            out.append({"role": role, "content": m.body or ""})
        return out
    finally:
        db.close()

# ==== Utils ====
def _final_sms(s: str) -> str:
    s = re.sub(r"\s+", " ", (s or "")).strip()
    return (s[:157].rstrip() + "…") if len(s) > 160 else s

def _polish(text: str) -> str:
    # small linguistic cleanups; no tone injection, no acks
    t = text or ""
    # normalize Lithuanian diacritics inconsistencies from model
    t = re.sub(r"\bdirbat(e)?\b", "dirbate", t, flags=re.I)
    # avoid bureaucratic “Ką dirbate?” → preferred phrasing
    t = re.sub(r"\bKą\s+dirbate\??", "Kokia jūsų specialybė ar sritis?", t, flags=re.I)
    return t.strip()

# ==== OpenAI call ====
def _build_messages(ctx: dict, text: str) -> List[Dict[str, str]]:
    msgs = [{"role": "system", "content": SYSTEM_PROMPT}]
    msisdn = (ctx or {}).get("msisdn", "")
    if msisdn:
        msgs += _thread_history(msisdn, limit=12)  # feed both sides for memory/anti-repeat
    msgs.append({"role": "user", "content": text})
    return msgs

def _call(messages):
    # low temp for consistency; we rely on the prompt for style & logic
    return client.chat.completions.create(
        model=MODEL,
        messages=messages,
        temperature=0.2,
    )

# ==== Main generator (no hardcoded routers, no fallbacks) ====
def generate_reply_lt(ctx: dict, text: str) -> str:
    # Debug peek
    t_raw = (text or "").strip()
    t = t_raw.lstrip("\\").lower()
    if t in {"!prompt", "!pf", "##prompt##"}:
        return (f"{PROMPT_SHA} {MODEL}")[:160]

    # Let the LLM semantically understand & respond
    r = _call(_build_messages(ctx, text))
    reply = (r.choices[0].message.content or "").strip()
    return _final_sms(_polish(reply))

# ==== Classifier (kept for API compatibility; semantic-only) ====
def classify_lt(text: str) -> dict:
    sys = (
        "Klasifikuok lietuvišką SMS į: 'questions', 'not_interested', arba 'other'. "
        "Grąžink JSON: {\"intent\": str, \"confidence\": 0..1}. "
        "Pvz.: 'nedomina' -> not_interested; klausimai apie darbą -> questions; kita -> other. "
        "Atsakyk tik JSON."
    )
    r = client.chat.completions.create(
        model=MODEL,
        temperature=0,
        messages=[{"role":"system","content":sys},{"role":"user","content":text}],
    )
    content = (r.choices[0].message.content or "").strip()
    try:
        obj = json.loads(content)
        intent = (obj.get("intent") or "").lower().strip()
        conf = float(obj.get("confidence") or 0.6)
        if intent not in {"questions", "not_interested", "other"}:
            intent = "other"
        return {"intent": intent, "confidence": conf}
    except Exception:
        # semantic-only path: if it fails to parse, call it 'other'
        return {"intent": "other", "confidence": 0.5}
