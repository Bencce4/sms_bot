import os, json, re
from typing import List, Dict
from difflib import SequenceMatcher
from sqlalchemy import desc
from openai import OpenAI

from app.storage.db import SessionLocal
from app.storage.models import Thread, Message

# ==== Model / client ====
MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ==== System prompt (complete, compact, enforce key rules) ====
SYSTEM_PROMPT = """You are an SMS recruiting assistant for Valandinis (valandinis.lt).

About Valandinis (do not invent specifics)
– Flexible hourly/shift work in construction & related trades across Lithuania
  (electricians, plumbers, bricklayers/mūrininkai, roadworks, helpers; sometimes
  light production/retail/hospitality).
– Quick start and simple process after a short intro.

Primary goal
– Gauge if the person is interested in working with Valandinis.
– If yes, capture 2–3 basics with minimum messages:
  (1) city/region, (2) specialty/trade, (3) years of experience, (4) availability.
– If clearly not interested, acknowledge once and stop. A human will call later if they are interested.

Core behavior (must follow)
1) LISTEN FIRST: if the user asks anything, answer in one short line FIRST,
   then ask exactly ONE qualifier.
2) ONE question per SMS. Max 160 chars. Natural, polite, simple. Avoid
   bureaucratic words. Do not use “rekruteris”. Prefer “specialybė / sritis /
   kuo dirbate?” over “role”.
3) Value sentence (“Lankstūs grafikai, greitas startas, paprasta eiga.”)
   at most ONCE per thread, and only if they ask “what do you offer?” or seem hesitant.
4) Never repeat the same sentence/idea previously sent in this thread.
5) Do not invent pay, clients, or locations. If asked, say details are shared later
   by phone, then continue qualification.
6) Never request sensitive data (ID, card, passwords, exact address, emails, codes).
   No legal/immigration advice. Never claim to be human.
7) Language: detect from the latest user message; reply in LT/RU/EN/UA accordingly;
   otherwise default to LT.
8) Close when enough info is collected: “Ačiū! Kolega paskambins dėl detalių.”

Qualification order (ask only what’s missing)
– city/region → specialty/trade → years of experience → availability.

Output
– Return only the message text. No JSON/markdown/explanations.
"""

# ==== History (DB) ====
def _thread_history(phone: str, limit: int = 12) -> List[Dict[str,str]]:
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

# ==== Regexes & small utilities ====
_RX_CITY   = re.compile(r'\b(miest|region)\w*', re.I)
_RX_CITY_ANS = re.compile(
    r'(vilni\w*|kaun\w*|klaip\w*|šiauli\w*|siauli\w*|panevėž\w*|panevez\w*|alyt\w*|marijamp\w*|kedain\w*|uten\w*|taurag\w*|'
    r'telš\w*|tels\w*|mazeik\w*|jonav\w*|rajon\w*|apskrit\w*|aplink|miest\w*|region\w*)',
    re.I
)
_RX_SPEC   = re.compile(r'\b(specialyb|srit|elektrik|santechn|mūrin|murin|beton|pagalbin|mechanik)\w*', re.I)
_RX_EXP    = re.compile(r'\b(\d+)\s*(m(?:et[au]|\.)?|metai|yr|years?)\b', re.I)
_RX_AVAIL  = re.compile(r'\b(pradėt|pradėsiu|pradesiu|start|nuo|grafik)\w*', re.I)
_RX_QUESTION = re.compile(r'(\?|^ *(kas|ką|kaip|kada|kiek|kur|kodėl|ar|galite|what|when|how|pay|salary|offer)\b)', re.I)
_RX_OFFER_SENT = re.compile(r'(lankst\w* grafik|greit\w* start|paprast\w* eig)', re.I)

FEWSHOTS: List[Dict[str, str]] = []

OFFER_TRIGGERS = [
    "ką siūlot", "ką siulot", "ka siulot", "ką turite", "ka turite",
    "ką galite pasiūlyti", "o ką siūlote",
    "what do you offer", "what can you offer", "more info", "tell me more"
]

def _split_sents(text: str) -> List[str]:
    if not text:
        return []
    return [p.strip() for p in re.split(r'(?<=[\.\!\?])\s+|[;\n]+', text) if p.strip()]

def _sim(a: str, b: str) -> float:
    return SequenceMatcher(None, (a or "").lower(), (b or "").lower()).ratio()

def _assistant_sentences(history: List[Dict[str,str]]) -> List[str]:
    out=[]
    for m in history:
        if m["role"]=="assistant":
            out.extend(_split_sents(m["content"]))
    return out

def _mentions_offer(text: str) -> bool:
    return bool(_RX_OFFER_SENT.search(text or ""))

def _is_offer_question(text: str) -> bool:
    tl = (text or "").strip().lower()
    return any(p in tl for p in OFFER_TRIGGERS)

def _user_asked_question(text: str) -> bool:
    return bool(_RX_QUESTION.search(text or ""))

def _polite(text: str) -> str:
    t = text or ""
    t = re.sub(r'\bdirbat(e)?\b', 'dirbate', t, flags=re.I)
    t = re.sub(r'\bKą\s+dirbate\??', 'Kokia jūsų specialybė ar sritis?', t, flags=re.I)
    return t.strip()

def _final_sms(s: str) -> str:
    s = re.sub(r'\s+', ' ', (s or "")).strip()
    return (s[:157].rstrip() + "…") if len(s) > 160 else s

# ==== “Has answered” (accept volunteered info) ====
def _pairwise(history: List[Dict[str,str]]):
    for i in range(len(history)-1):
        yield history[i], history[i+1]

def _has_answered_city(history):
    if any(m["role"]=="user" and _RX_CITY_ANS.search(m["content"]) for m in history):
        return True
    for a,b in _pairwise(history):
        if a["role"]=="assistant" and _RX_CITY.search(a["content"]):
            if b["role"]=="user" and b["content"].strip():
                return True
    return False

def _has_answered_spec(history):
    if any(m["role"]=="user" and _RX_SPEC.search(m["content"]) for m in history):
        return True
    for a,b in _pairwise(history):
        if a["role"]=="assistant" and (_RX_SPEC.search(a["content"]) or "specialyb" in a["content"].lower()):
            if b["role"]=="user" and b["content"].strip():
                return True
    return False

def _has_answered_exp(history):
    if any(m["role"]=="user" and _RX_EXP.search(m["content"]) for m in history):
        return True
    for a,b in _pairwise(history):
        if a["role"]=="assistant" and _RX_EXP.search(a["content"]):
            if b["role"]=="user" and b["content"].strip():
                return True
    return False

def _has_answered_avail(history):
    if any(m["role"]=="user" and _RX_AVAIL.search(m["content"]) for m in history):
        return True
    for a,b in _pairwise(history):
        if a["role"]=="assistant" and _RX_AVAIL.search(a["content"]):
            if b["role"]=="user" and b["content"].strip():
                return True
    return False

def _next_missing_question(history):
    if not _has_answered_city(history):
        return "Kuriuose miestuose ar regionuose galite dirbti?"
    if not _has_answered_spec(history):
        return "Kokia jūsų specialybė ar sritis?"
    if not _has_answered_exp(history):
        return "Kiek metų patirties turite šioje srityje?"
    if not _has_answered_avail(history):
        return "Kada galėtumėte pradėti arba koks grafikas tinka?"
    return "Ačiū! Kolega paskambins dėl detalių."

def _answer_then_ask(answer_line: str, history):
    q = _polite(_next_missing_question(history))
    if q.startswith("Ačiū!"):
        return _final_sms(answer_line)
    return _final_sms(f"{answer_line} {q}")

def _value_sentence_once(history) -> str:
    seen = "lankst" in " ".join(m["content"].lower() for m in history if m["role"]=="assistant")
    return "" if seen else "Lankstūs grafikai, greitas startas, paprasta eiga."

# ==== OpenAI wiring ====
def _build_messages(ctx: dict, text: str) -> List[Dict[str,str]]:
    msgs = [{"role": "system", "content": SYSTEM_PROMPT}]
    msisdn = (ctx or {}).get("msisdn","")
    if msisdn:
        msgs += _thread_history(msisdn, limit=12)
    # compat: replay fewshots if you ever populate FEWSHOTS
    for ex in FEWSHOTS:
        msgs.append({"role": "user", "content": ex.get("user","")})
        msgs.append({"role": "assistant", "content": ex.get("assistant","")})
    msgs.append({"role":"user","content":text})
    return msgs

def _call(messages):
    return client.chat.completions.create(model=MODEL, messages=messages)

# ==== Main generator ====
def generate_reply_lt(ctx: dict, text: str) -> str:
    history = _thread_history((ctx or {}).get("msisdn",""), limit=12)
    prev_sents = _assistant_sentences(history)
    user_text = text or ""

    # Offer-question: deterministic one-liner + next qualifier; never repeat value later
    if _is_offer_question(user_text):
        answer = _value_sentence_once(history) or "Turime įvairių darbų su pamainomis."
        return _answer_then_ask(answer, history)

    # Model call
    r = _call(_build_messages(ctx, text))
    reply = _polite((r.choices[0].message.content or "").strip())

    # Strip unsolicited offer lines
    sents = [s for s in _split_sents(reply) if not _mentions_offer(s)]

    # Anti-repeat vs prior assistant sentences
    kept = [s for s in sents if all(_sim(s, ps) < 0.78 for ps in prev_sents)]

    # If user asked any question, ensure we answer first (even if model failed)
    if _user_asked_question(user_text):
        answer_line = kept[0] if kept else "Trumpai: turime įvairių darbų su pamainomis."
        if _mentions_offer(answer_line) and not _is_offer_question(user_text):
            answer_line = "Trumpai: turime įvairių darbų su pamainomis."
        return _answer_then_ask(answer_line, history)

    # Fallback to next missing qualifier or first clean sentence
    if not kept:
        return _final_sms(_polite(_next_missing_question(history)))

    # Enforce single-sentence/one-question SMS
    return _final_sms(kept[0])

# ==== Classifier (compatible with your code; keeps simple fallbacks) ====
def classify_lt(text: str) -> dict:
    sys = (
        "Klasifikuok lietuvišką SMS į: 'questions', 'not_interested', arba 'other'. "
        "Grąžink JSON: {\"intent\": str, \"confidence\": 0..1}. "
        "Pvz.: 'nedomina' -> not_interested; klausimai apie darbą -> questions; kita -> other. "
        "Atsakyk tik JSON."
    )
    try:
        r = _call([
            {"role": "system", "content": sys},
            {"role": "user", "content": text},
        ])
        content = (r.choices[0].message.content or "").strip()
        obj = json.loads(content)
        intent = (obj.get("intent") or "").lower().strip()
        conf = float(obj.get("confidence") or 0.6)
        if intent not in {"questions", "not_interested", "other"}:
            intent = "other"
        return {"intent": intent, "confidence": conf}
    except Exception:
        tl = (text or "").lower()
        if any(w in tl for w in ["nedomina", "nenoriu", "ne, ačiū", "aciu ne", "ne.", "ne "]):
            return {"intent": "not_interested", "confidence": 0.8}
        if _is_offer_question(tl) or _user_asked_question(tl):
            return {"intent": "questions", "confidence": 0.8}
        return {"intent": "other", "confidence": 0.5}
