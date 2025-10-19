import os, json, re, unicodedata, hashlib
from typing import List, Dict, Optional
from sqlalchemy import desc
from openai import OpenAI

from app.storage.db import SessionLocal
from app.storage.models import Thread, Message

# ==== Models / client ====
MODEL = os.getenv("LLM_REPLY_MODEL", os.getenv("LLM_MODEL", "gpt-4o-mini"))
EXTRACT_MODEL = os.getenv("LLM_EXTRACT_MODEL", os.getenv("LLM_MODEL", "gpt-4o-mini"))
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ==== System prompt (concise, rules preserved) ====
SYSTEM_PROMPT = """You are a short, upbeat SMS recruiting assistant for Valandinis (valandinis.lt).

About Valandinis (do not invent specifics)
– Flexible hourly/shift work in construction & related trades across Lithuania
  (electricians, plumbers, bricklayers/mūrininkai, roadworks, helpers; sometimes light production/retail/hospitality).
– Quick start and simple process after a short intro.

Primary goal
– Gauge interest and, if positive, collect only: (1) city/region, (2) specialty/trade, (3) years experience, (4) availability.

Core behavior (must follow)
1) LISTEN FIRST: answer the user’s message with one short STATEMENT (≤80 chars; NEVER a question),
   then ask exactly ONE missing qualifier.
2) ONE question per SMS. Max 160 chars. Friendly, natural, encouraging. No bureaucratic phrasing.
3) Use the value line at most once per thread, and only if they ask what you offer or hesitate:
   “Siūlome lanksčius grafikus, greitą pradžią ir paprastą procesą įsidarbinant.”
4) Do not repeat sentences/ideas already sent in this thread.
5) Don’t invent pay/clients/locations. If asked: say details by phone, then continue.
6) Never request sensitive data (ID, card, passwords, exact address, emails, codes). No legal/immigration advice.
7) Don’t claim to be human. You are an SMS assistant.
8) Language: mirror the latest user message (LT/RU/EN/UA). Else default LT.
9) Close when all info gathered: “Perduosiu kolegai – paskambins dėl detalių.”

Order for questions: city/region → specialty → years experience → availability.

Output: return ONLY the message text (no JSON/markdown).
"""

# Fingerprint (for !prompt)
PROMPT_SHA = hashlib.sha256(SYSTEM_PROMPT.encode("utf-8")).hexdigest()[:12]
def prompt_info() -> str:
    return f"PROMPT_SHA={PROMPT_SHA} MODEL={MODEL}"
try:
    print(f"[llm] {prompt_info()}", flush=True)
except Exception:
    pass

# ==== DB history ====
def _thread_history(phone: str, limit: int = 14) -> List[Dict[str,str]]:
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
                  .limit(limit).all())
        out = []
        for m in reversed(msgs):
            role = "assistant" if m.dir == "out" else "user"
            out.append({"role": role, "content": m.body or ""})
        return out
    finally:
        db.close()

# ==== Basic utils ====
def _normalize(text: str) -> str:
    if not text: return ""
    t = unicodedata.normalize("NFKD", text)
    t = "".join(c for c in t if not unicodedata.combining(c))
    return t.strip()

def _final_sms(s: str) -> str:
    s = re.sub(r'\s+', ' ', (s or "")).strip()
    return (s[:157].rstrip() + "…") if len(s) > 160 else s

def _polite(text: str) -> str:
    t = text or ""
    t = re.sub(r'\bdirbat(e)?\b', 'dirbate', t, flags=re.I)
    t = re.sub(r'\bKą\s+dirbate\??', 'Kokia jūsų specialybė ar sritis?', t, flags=re.I)
    return t.strip()

def _as_statement(s: str) -> str:
    s = (s or "").strip()
    if "?" in s:
        return "Gerai, padėsiu suderinti."
    return s

# ==== Slot planner ====
Q_CITY   = "Kuriame mieste ar regione dirbtumėte?"
Q_SPEC   = "Kokia jūsų specialybė ar sritis?"
Q_EXP    = "Kiek metų patirties turite?"
Q_AVAIL  = "Nuo kada galėtumėte pradėti arba koks grafikas tinka?"
CLOSE_TX = "Perduosiu kolegai – paskambins dėl detalių."

def _next_missing(slots: dict) -> str:
    if not slots.get("city_region"):       return Q_CITY
    if not slots.get("specialty"):         return Q_SPEC
    if slots.get("experience_years") is None: return Q_EXP
    if not slots.get("availability"):      return Q_AVAIL
    return CLOSE_TX

# ==== One-time value line control ====
def _value_sentence_once(history: List[Dict[str,str]]) -> str:
    sent_before = " ".join((m["content"] or "").lower() for m in history if m["role"]=="assistant")
    if any(k in sent_before for k in ["lanks", "greit", "paprast"]):
        return ""  # already said
    return "Siūlome lanksčius grafikus, greitą pradžią ir paprastą procesą įsidarbinant."

# ==== Semantic extraction (no hardcoded city lists) ====
EXTRACT_SYS = """Extract recruiting slots from the transcript.
Return strict JSON with:
{
 "intent": "offer"|"pay"|"remote"|"human"|"abuse"|"not_interested"|"question"|"other",
 "city_region": string|null,
 "specialty": string|null,
 "experience_years": number|null,
 "availability": string|null,
 "user_question": string|null
}
Rules:
- Use all context in the transcript (latest messages first if ambiguous).
- experience_years must be a number if any hint like "7 metus", "7 m.", "7 years".
- If nothing for a field, use null. No extra keys or text.
"""

def _transcript(history: List[Dict[str,str]], last_user: str) -> str:
    # compact transcript with roles
    lines = []
    for m in history[-12:]:
        role = "USER" if m["role"]=="user" else "ASSISTANT"
        lines.append(f"{role}: {m['content']}")
    lines.append(f"USER: {last_user}")
    return "\n".join(lines[-20:])

def _extract_slots(history: List[Dict[str,str]], user_text: str) -> dict:
    tr = _transcript(history, user_text)
    try:
        r = client.chat.completions.create(
            model=EXTRACT_MODEL,
            temperature=0,
            messages=[
                {"role":"system","content":EXTRACT_SYS},
                {"role":"user","content":tr}
            ]
        )
        raw = (r.choices[0].message.content or "").strip()
        obj = json.loads(raw)
        # sanitize types
        slots = {
            "intent": obj.get("intent") or "other",
            "city_region": (obj.get("city_region") or None),
            "specialty": (obj.get("specialty") or None),
            "experience_years": obj.get("experience_years"),
            "availability": (obj.get("availability") or None),
            "user_question": (obj.get("user_question") or None),
        }
        # normalize years: accept strings like "7" or "7.0"
        ey = slots["experience_years"]
        if isinstance(ey, str):
            try:
                ey = float(ey)
            except Exception:
                ey = None
        if isinstance(ey, (int, float)):
            slots["experience_years"] = float(ey)
        else:
            slots["experience_years"] = None
        return slots
    except Exception:
        # very light fallback: try to pull a number-of-years pattern
        yrs = None
        m = re.search(r'\b(\d{1,2})\s*(m(?:et[au]|\.)?|metai|metus|metu|metų|yr|years?)\b', _normalize(user_text).lower())
        if m:
            try: yrs = float(m.group(1))
            except Exception: yrs = None
        return {
            "intent":"other","city_region":None,"specialty":None,
            "experience_years": yrs, "availability": None, "user_question": None
        }

def _merge_known(history: List[Dict[str,str]], cur: dict) -> dict:
    """Ask the extractor over the whole transcript so far; merge with current."""
    try:
        # Build a transcript without the latest user text and extract from it too (persist past info).
        hist_only = _transcript(history[:-1] if history and history[-1]["role"]=="user" else history, "")
        r = client.chat.completions.create(
            model=EXTRACT_MODEL, temperature=0,
            messages=[{"role":"system","content":EXTRACT_SYS},
                      {"role":"user","content":hist_only}]
        )
        obj = json.loads((r.choices[0].message.content or "").strip())
        base = {
            "city_region": obj.get("city_region") or None,
            "specialty": obj.get("specialty") or None,
            "experience_years": obj.get("experience_years") if isinstance(obj.get("experience_years"), (int,float)) else None,
            "availability": obj.get("availability") or None
        }
    except Exception:
        base = {"city_region":None,"specialty":None,"experience_years":None,"availability":None}
    # merge: current overrides base if present
    for k in base:
        v = cur.get(k)
        base[k] = v if (v not in [None, ""]) else base[k]
    return base

# ==== OpenAI helpers ====
def _call(messages, temperature=0.2):
    return client.chat.completions.create(model=MODEL, messages=messages, temperature=temperature)

def _build_messages(ctx: dict, text: str) -> List[Dict[str,str]]:
    msgs = [{"role":"system","content":SYSTEM_PROMPT}]
    msisdn = (ctx or {}).get("msisdn","")
    if msisdn:
        msgs += _thread_history(msisdn, limit=14)
    msgs.append({"role":"user","content":text})
    return msgs

# ==== Compose “answer + one question” ====
def _answer_then_ask(answer_line: Optional[str], next_q: str) -> str:
    if next_q == CLOSE_TX:
        msg = f"{_as_statement(answer_line)} {CLOSE_TX}".strip() if answer_line else CLOSE_TX
        return _final_sms(msg)
    parts = [p for p in [_as_statement(answer_line or ""), next_q] if p]
    return _final_sms(" ".join(parts).strip())

# ==== Main generator ====
def generate_reply_lt(ctx: dict, text: str) -> str:
    # Debug triggers (first)
    t_raw = (text or "").strip()
    t = t_raw.lstrip("\\").lower()
    if t in {"!prompt","!pf","##prompt##"}:
        return f"{PROMPT_SHA} {MODEL}"[:160]
    if t.startswith("!trace ") or t.startswith("\\!trace "):
        # Show extractor's intent quickly
        probe = t_raw.split(" ",1)[1] if " " in t_raw else ""
        slots = _extract_slots([], probe)
        return ("TRACE:" + slots.get("intent","other"))[:160]

    # History and semantic extraction
    history = _thread_history((ctx or {}).get("msisdn",""), limit=14)
    slots_now = _extract_slots(history, t_raw)
    slots_merged = _merge_known(history, slots_now)
    intent = slots_now.get("intent","other")
    next_q = _polite(_next_missing(slots_merged))

    # Abrasive user → soft exit
    if intent == "abuse":
        return _final_sms("Supratau. Jei prireiks darbo galimybių – parašykite. Gražios dienos!")

    # Deterministic intents (statement + one question)
    if intent == "offer":
        ans = _value_sentence_once(history) or "Siūlome lanksčius grafikus, greitą pradžią ir paprastą procesą įsidarbinant."
        return _answer_then_ask(ans, next_q)
    if intent == "pay":
        return _answer_then_ask("Atlygis priklauso nuo vietos ir darbo – suderiname telefonu.", next_q)
    if intent == "remote":
        return _answer_then_ask("Darbai dažniausiai vietoje; nuotolinis retas.", next_q)
    if intent == "human":
        return _answer_then_ask("Čia Valandinis SMS asistentas – padėsiu su pagrindiniais klausimais.", next_q)
    if intent == "not_interested":
        return _final_sms("Ačiū, supratau. Jei pasikeis planai – parašykite.")

    # Generic question → model crafts a SHORT statement answer, then we ask next
    if intent == "question":
        r = _call(_build_messages(ctx, text), temperature=0.2)
        reply_stmt = _as_statement(_polite((r.choices[0].message.content or "").strip()))
        # prevent unsolicited repeating of value line unless it was an offer Q
        if "lanks" in reply_stmt.lower() and intent != "offer":
            reply_stmt = "Gerai, padėsiu suderinti."
        return _answer_then_ask(reply_stmt, next_q)

    # No question → keep progressing with the planner
    # If user volunteered the just-asked slot, we still move to the next required.
    return _answer_then_ask("", next_q)

# ==== Classifier (kept for API compatibility; uses extractor under the hood) ====
def classify_lt(text: str) -> dict:
    try:
        slots = _extract_slots([], text)
        intent = slots.get("intent","other")
        conf = 0.8 if intent in {"offer","pay","remote","human","question","not_interested"} else 0.5
        mapped = intent if intent in {"questions","not_interested","other"} else (
            "questions" if intent in {"offer","pay","remote","human","question"} else
            "not_interested" if intent=="not_interested" else "other"
        )
        return {"intent": mapped, "confidence": conf}
    except Exception:
        tl = _normalize(text).lower()
        if any(w in tl for w in ["nedomina","nenoriu","ne, aciu","ne, ačiū","aciu ne","ne.","ne "]):
            return {"intent":"not_interested","confidence":0.8}
        return {"intent":"other","confidence":0.5}
