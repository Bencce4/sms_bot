# app/services/llm.py
import os, json, re, hashlib
from typing import List, Dict, Optional
from sqlalchemy import desc
from openai import OpenAI

from app.storage.db import SessionLocal
from app.storage.models import Thread, Message

# ==== Model / client ====
MODEL = os.getenv("LLM_REPLY_MODEL", os.getenv("LLM_MODEL", "gpt-4o-mini"))
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ==== Constants ====
VALUE_LINE = "Siūlome lanksčius grafikus, greitą pradžią ir paprastą procesą įsidarbinant."
CLOSE_TX   = "Perduosiu kolegai – paskambins dėl detalių."

# ==== System prompt (semantic-first) ====
SYSTEM_PROMPT = f"""You are a short, human-sounding SMS recruiter for Valandinis (valandinis.lt).

About Valandinis (do not invent specifics)
– Flexible hourly/shift work in construction & related trades across Lithuania
  (electricians, plumbers, bricklayers/mūrininkai, roadworks, helpers; sometimes light production/retail/hospitality).
– Quick start and simple process after a short intro.

Primary goal
– Gauge interest and, if positive, collect only: (1) city/region, (2) specialty/trade, (3) years of experience, (4) availability.

How to respond (must follow)
1) LISTEN FIRST: answer their message with one short STATEMENT (≤80 chars, NEVER a question).
   • If they ask “what do you offer?” in any wording, say this **once per thread**: “{VALUE_LINE}”
   • If they sound hesitant (e.g., “gal ir domintų”, “nežinau”, unsure), give a brief reassurance and the same value line **once per thread**, then ask: “Ką manote?” (do not advance to qualifiers yet).
2) Then, once interest is clear, ask exactly ONE next missing qualifier in this order:
   • City/region → ask: “Kur dirbate dabar (miestas/regionas)?”
   • Specialty/trade → ask: “Kokia jūsų specialybė ar sritis?”
   • Years of experience → prefer: “Kiek metų patirties turite kaip <specialty>?” else “Kiek metų patirties turite?”
   • Availability → ask: “Nuo kada galėtumėte pradėti arba koks grafikas tinka?”
3) ONE SMS total. ≤160 chars. Natural, friendly, encouraging. No bureaucratic words.
4) Never repeat the same sentence/idea already sent in this thread (especially do **not** repeat the offer line).
5) Do not assume they are job-hunting; ask where they **work now** (not “where you are looking”).
6) Don’t invent pay/clients/locations. If asked: say details are shared later by phone, then continue qualifying.
7) Never request sensitive data (ID, card, passwords, exact address, emails, codes). No legal advice.
8) Language: mirror the user’s latest language (LT/RU/EN/UA). Else default LT.
9) If asked “ar jūs robotas?” or similar, reply as a human teammate (do not disclose being a bot): “Ne. Ar domintų dirbti per Valandinį?” Then continue on next turn if they say yes.
10) When all four items are collected, end with: “{CLOSE_TX}” and do not ask anything else.

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
def _thread_history(phone: str, limit: int = 14) -> List[Dict[str, str]]:
    if not phone:
        return []
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
            out.append({"role": role, "content": m.body or ""})
        return out
    finally:
        db.close()

# ==== Helpers ====
def _final_sms(s: str) -> str:
    s = re.sub(r"\s+", " ", (s or "")).strip()
    return (s[:157].rstrip() + "…") if len(s) > 160 else s

def _polish(text: str) -> str:
    t = (text or "").strip()
    t = re.sub(r"\bKą\s+dirbate\??", "Kokia jūsų specialybė ar sritis?", t, flags=re.I)
    t = re.sub(r"\bdirbat(e)?\b", "dirbate", t, flags=re.I)
    return t.strip()

def _assistant_has(history: List[Dict[str,str]], needle: str) -> bool:
    n = (needle or "").lower()
    for m in history:
        if m["role"] == "assistant" and n in (m["content"] or "").lower():
            return True
    return False

def _strip_repeated_value_line(msg: str, history: List[Dict[str,str]]) -> str:
    if not msg:
        return msg
    if _assistant_has(history, VALUE_LINE.lower()):
        parts = [s.strip() for s in re.split(r'(?<=[\.\!\?])\s+', msg) if s.strip()]
        parts = [s for s in parts if VALUE_LINE.lower() not in s.lower()]
        return " ".join(parts).strip()
    return msg

# light “memory”: pull specialty from the last user reply after we asked for it
_SPECIALTY_Q_PAT = re.compile(r"koki(a|ą)\s+jūsų\s+specialybė|sritis\??", re.I)
def _last_specialty(history: List[Dict[str,str]]) -> Optional[str]:
    for a, b in zip(history[::-1], history[-2::-1]):  # iterate backwards in pairs
        if a["role"] == "user" and b["role"] == "assistant":
            if _SPECIALTY_Q_PAT.search(b["content"] or "") and (a["content"] or "").strip():
                # take concise specialty phrase (first 4 words max)
                raw = a["content"].strip()
                words = raw.split()
                return " ".join(words[:4])
    return None

# sparing micro-acks (variety)
_ACKS = ["Gerai!", "Šaunu!", "Super!", "Puiku!"]
def _ack(history: List[Dict[str,str]]) -> str:
    # use at most ~1/3 of the time based on assistant-turn count
    n = sum(1 for m in history if m["role"] == "assistant")
    return _ACKS[n % len(_ACKS)] if n % 3 == 1 else ""

# hesitation detector (semantic-ish)
_HESIT_PAT = re.compile(r"\b(gal|nezinau|nežinau|maybe|not\s+sure|pamastysiu|pamatysiu)\b", re.I)

def _is_hesitation(text: str) -> bool:
    return bool(_HESIT_PAT.search(text or ""))

def _call(messages):
    return client.chat.completions.create(
        model=MODEL, messages=messages, temperature=0.2
    )

def _build_messages(ctx: dict, text: str) -> List[Dict[str, str]]:
    msgs = [{"role": "system", "content": SYSTEM_PROMPT}]
    msisdn = (ctx or {}).get("msisdn", "")
    if msisdn:
        msgs += _thread_history(msisdn, limit=14)
    msgs.append({"role": "user", "content": text})
    return msgs

# ==== Main generator ====
def generate_reply_lt(ctx: dict, text: str) -> str:
    # Debug control
    t_raw = (text or "").strip()
    t = t_raw.lstrip("\\").lower()
    if t in {"!prompt", "!pf", "##prompt##"}:
        return (f"{PROMPT_SHA} {MODEL}")[:160]

    history = _thread_history((ctx or {}).get("msisdn",""), limit=14)

    # HARD STOP after handoff
    if _assistant_has(history, CLOSE_TX.lower()):
        return ""  # no further messages

    # If user asks if robot → crisp human answer + interest check
    if re.search(r"\b(robot|bot|dirbtin|ai)\b", t_raw, re.I):
        return _final_sms("Ne. Ar domintų dirbti per Valandinį?")

    # Hesitation → nudge + value line + “Ką manote?” (don’t advance yet)
    if _is_hesitation(t_raw):
        already_said = _assistant_has(history, VALUE_LINE.lower())
        value_part = "" if already_said else VALUE_LINE
        ack = _ack(history)
        parts = [p for p in [ack, "Suprantu.", value_part, "Ką manote?"] if p]
        msg = " ".join(parts).strip()
        return _final_sms(msg)

    # Normal semantic flow via the model
    r = _call(_build_messages(ctx, text))
    reply = (r.choices[0].message.content or "").strip()

    # Never repeat the offer value line
    reply = _strip_repeated_value_line(reply, history)

    # **Personalize** exp question with specialty if present in memory
    spec = _last_specialty(history + [{"role": "user", "content": t_raw}])
    if spec:
        # replace generic exp question with specialized one
        reply = re.sub(
            r"\bKiek\s+metų\s+patirties\s+turite\??",
            f"Kiek metų patirties turite kaip {spec}?",
            reply
        )

    # Light ack injection (sparingly) if reply is a bare question
    if reply.endswith("?"):
        ack = _ack(history)
        if ack:
            reply = f"{ack} {reply}"

    # Final polish and length guard
    reply = _polish(reply).strip()
    if not reply:
        return ""
    return _final_sms(reply)

# ==== Classifier (API-compat; semantic via model) ====
def classify_lt(text: str) -> dict:
    sys = (
        "Klasifikuok lietuvišką SMS į: 'questions', 'not_interested', arba 'other'. "
        "Grąžink JSON: {\"intent\": str, \"confidence\": 0..1}. "
        "Pvz.: 'nedomina' -> not_interested; klausimai apie darbą -> questions; kita -> other. "
        "Atsakyk tik JSON."
    )
    r = client.chat.completions.create(
        model=MODEL, temperature=0,
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
        return {"intent": "other", "confidence": 0.5}
