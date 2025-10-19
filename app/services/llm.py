import os, json, re, unicodedata, hashlib
from typing import List, Dict
from difflib import SequenceMatcher
from sqlalchemy import desc
from openai import OpenAI

from app.storage.db import SessionLocal
from app.storage.models import Thread, Message

# ==== Model / client ====
MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")
EMBED_MODEL = os.getenv("EMBED_MODEL", "text-embedding-3-small")
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ==== Fewshots (compat; unused by default) ====
FEWSHOTS: List[Dict[str, str]] = []

# ==== System prompt (rules + warmer tone; statement-first to avoid Q+Q) ====
SYSTEM_PROMPT = """You are a short, upbeat SMS recruiting assistant for Valandinis (valandinis.lt).

About Valandinis (do not invent specifics)
– Flexible hourly/shift work in construction & related trades across Lithuania
  (electricians, plumbers, bricklayers/mūrininkai, roadworks, helpers; sometimes light production/retail/hospitality).
– Quick start and simple process after a short intro.

Primary goal
– See if the person is open to work with Valandinis.
– If yes, collect only the missing basics with minimum messages:
  (1) city/region, (2) specialty/trade, (3) years of experience, (4) availability.
– If clearly not interested, acknowledge once and stop. A human will call later if interested.

Core behavior (must follow)
1) LISTEN FIRST: briefly answer the user’s message FIRST in one short STATEMENT (≤80 chars, never a question),
   then ask exactly ONE qualifier.
2) ONE question per SMS. Max 160 chars. Friendly, natural, encouraging. No bureaucratic phrasing.
   Use light micro-acknowledgements like “Puiku!”, “Super!”, “Skamba gerai!” sparingly.
3) Use the value line (“Siūlome lanksčius grafikus, greitą pradžią ir paprastą procesą įsidarbinant.”) at most ONCE per thread,
   and only if they ask what you offer or hesitate.
4) Never repeat the same sentence/idea already sent in this thread.
5) Do not invent pay, clients, or locations. If asked: say details are shared later by phone, then continue qualifying.
6) Never request sensitive data (ID, card, passwords, exact address, emails, codes). No legal/immigration advice.
7) Never claim to be human. You are an SMS assistant.
8) Language: detect from the latest user message; reply in LT/RU/EN/UA; else default LT.
9) Close once enough info is collected with a brief, friendly line: “Perduosiu kolegai – paskambins dėl detalių.”

Ask only what’s missing, in this order:
– city/region → specialty/trade → years of experience → availability.

Output
– Return only the message text. No JSON/markdown/explanations.
"""

# ==== Prompt fingerprint & debug ====
PROMPT_SHA = hashlib.sha256(SYSTEM_PROMPT.encode("utf-8")).hexdigest()[:12]
def prompt_info() -> str:
    return f"PROMPT_SHA={PROMPT_SHA} MODEL={os.getenv('LLM_REPLY_MODEL', os.getenv('LLM_MODEL','gpt-4o-mini'))}"
try:
    print(f"[llm] {prompt_info()}", flush=True)
except Exception:
    pass

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

# ==== Normalization & tiny utils ====
def _normalize(text: str) -> str:
    if not text: return ""
    t = unicodedata.normalize("NFKD", text)
    t = "".join(c for c in t if not unicodedata.combining(c))
    return t.lower().strip()

def _split_sents(text: str) -> List[str]:
    if not text: return []
    return [p.strip() for p in re.split(r'(?<=[\.\!\?])\s+|[;\n]+', text) if p.strip()]

def _sim(a: str, b: str) -> float:
    return SequenceMatcher(None, (a or "").lower(), (b or "").lower()).ratio()

def _assistant_sentences(history: List[Dict[str,str]]) -> List[str]:
    out=[]
    for m in history:
        if m["role"]=="assistant":
            out.extend(_split_sents(m["content"]))
    return out

def _final_sms(s: str) -> str:
    s = re.sub(r'\s+', ' ', (s or "")).strip()
    return (s[:157].rstrip() + "…") if len(s) > 160 else s

def _polite(text: str) -> str:
    t = text or ""
    t = re.sub(r'\bdirbat(e)?\b', 'dirbate', t, flags=re.I)
    t = re.sub(r'\bKą\s+dirbate\??', 'Kokia jūsų specialybė ar sritis?', t, flags=re.I)
    return t.strip()

# ==== Style: friendly micro-acks and rotating question variants ====
_ACKS = ["Puiku!", "Super!", "Skamba gerai!", "Gerai supratau."]
def _ack(history: List[Dict[str,str]]) -> str:
    n = sum(1 for m in history if m["role"]=="assistant")
    return _ACKS[n % len(_ACKS)] if n % 2 == 1 else ""

_Q_CITY = [
    "Kuriame mieste ar regione dirbtumėte?",
    "Kur jums patogiausia dirbti (miestas/regionas)?",
    "Koks miestas ar regionas tinka darbui?",
]
_Q_SPEC = [
    "Kokia jūsų specialybė ar sritis?",
    "Kuo dirbate statybose?",
    "Kokia sritis jums artimiausia?",
]
_Q_EXP = [
    "Kiek metų patirties turite šioje srityje?",
    "Kiek metų patirties maždaug?",
    "Kokia jūsų patirtis (metais)?",
]
_Q_AVAIL = [
    "Nuo kada galėtumėte pradėti arba koks grafikas tinka?",
    "Kada galėtumėt startuoti arba koks grafikas patogus?",
    "Nuo kada jums patogu pradėti?",
]

def _pick_variant(options: List[str], history: List[Dict[str,str]]) -> str:
    n = sum(1 for m in history if m["role"]=="assistant")
    return options[n % len(options)]

# ==== Regexes (slot detection) ====
_RX_CITY   = re.compile(r'\b(miest|region)\w*', re.I)
_RX_CITY_ANS = re.compile(
    r'(vilni\w*|kaun\w*|klaip\w*|šiauli\w*|siauli\w*|panevėž\w*|panevez\w*|alyt\w*|marijamp\w*|kedain\w*|uten\w*|taurag\w*|'
    r'telš\w*|tels\w*|mazeik\w*|jonav\w*|rajon\w*|apskrit\w*|aplink|miest\w*|region\w*)',
    re.I
)
_RX_SPEC   = re.compile(r'\b(specialyb|srit|elektrik|santechn|mūrin|murin|beton|pagalbin|mechanik)\w*', re.I)
_RX_EXP    = re.compile(r'\b(\d+)\s*(m(?:et[au]|\.)?|metai|yr|years?)\b', re.I)
_RX_AVAIL  = re.compile(r'\b(pradėt|pradėsiu|pradesiu|start|nuo|grafik|rytoj|šiand|siand)\w*', re.I)

# ==== Intent detectors (broad/robust) ====
_OFFER_SENT = re.compile(r'(lankst\w* grafik|greit\w* prad|greit\w* start|paprast\w* proces|paprast\w* eig)', re.I)
_OFFER_PAT  = re.compile(
    r'(\b(k(a|ą)|ko)\s+(galit(e)?\s+)?(siu|siul|pasiul)\w*\b)|'
    r'(\b(what|tell)\b.*\boffer\b)|\boffer\?\b',
    re.I,
)
_PAY_PAT    = re.compile(r'\b(atlygin|alga|mok(a|at)|kiek\s*(mok|pay)|salary|eur|€/h|per\s*val)\b', re.I)
_REMOTE_PAT = re.compile(r'\b(nuotol|remote)\b', re.I)
_HUMAN_PAT  = re.compile(r'\b(robot|bot|žmog|zmog|human)\b', re.I)
_INSULT_PAT = re.compile(r'\b(nx|nax|eik\s*nax|fuck|idiot|deb|loho)\w*', re.I)

def _mentions_offer(text: str) -> bool:
    return bool(_OFFER_SENT.search(text or ""))

def _user_asked_offer(text: str) -> bool:
    return bool(_OFFER_PAT.search(_normalize(text)))

def _user_asked_pay(text: str) -> bool:
    return bool(_PAY_PAT.search(_normalize(text)))

def _user_asked_remote(text: str) -> bool:
    return bool(_REMOTE_PAT.search(_normalize(text)))

def _user_asked_human(text: str) -> bool:
    return bool(_HUMAN_PAT.search(_normalize(text)))

def _user_insult(text: str) -> bool:
    return bool(_INSULT_PAT.search(_normalize(text)))

def _user_any_question(text: str) -> bool:
    t = _normalize(text)
    return ("?" in (text or "")) or _user_asked_offer(t) or _user_asked_pay(t) or _user_asked_remote(t) or _user_asked_human(t)

# ==== Slot tracking (accept volunteered info) ====
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
        return _pick_variant(_Q_CITY, history)
    if not _has_answered_spec(history):
        return _pick_variant(_Q_SPEC, history)
    if not _has_answered_exp(history):
        return _pick_variant(_Q_EXP, history)
    if not _has_answered_avail(history):
        return _pick_variant(_Q_AVAIL, history)
    return "Perduosiu kolegai – paskambins dėl detalių."

# ==== Value line (your exact sentence), show once per thread ====
def _value_sentence_once(history) -> str:
    sent_before = " ".join((m["content"] or "").lower() for m in history if m["role"]=="assistant")
    if any(k in sent_before for k in ["lanks", "greit", "paprast"]):
        return ""
    return "Siūlome lanksčius grafikus, greitą pradžią ir paprastą procesą įsidarbinant."

# ==== Compose: statement-first + exactly one qualifier ====
def _as_statement(s: str) -> str:
    s = (s or "").strip()
    if "?" in s:
        # never send a question as the 'answer'
        return "Gerai, padėsiu suderinti."
    return s

def _answer_then_ask(answer_line: str, history) -> str:
    ack = _ack(history)
    q = _polite(_next_missing_question(history))
    if q.startswith("Perduosiu kolegai"):
        msg = f"{answer_line} {q}".strip() if answer_line else q
        return _final_sms(msg)
    parts = [p for p in [_as_statement(answer_line), ack, q] if p]
    return _final_sms(" ".join(parts))

# ==== OpenAI wiring ====
def _build_messages(ctx: dict, text: str) -> List[Dict[str,str]]:
    msgs = [{"role": "system", "content": SYSTEM_PROMPT}]
    msisdn = (ctx or {}).get("msisdn","")
    if msisdn:
        msgs += _thread_history(msisdn, limit=12)
    for ex in FEWSHOTS:
        msgs.append({"role": "user", "content": ex.get("user","")})
        msgs.append({"role": "assistant", "content": ex.get("assistant","")})
    msgs.append({"role":"user","content":text})
    return msgs

def _call(messages):
    return client.chat.completions.create(model=MODEL, messages=messages, temperature=0.2)

# ==== Main generator ====
def generate_reply_lt(ctx: dict, text: str) -> str:
    # --- debug triggers (FIRST) ---
    t_raw = (text or "").strip()
    t = t_raw.lstrip("\\").lower()
    if t in {"!prompt", "!pf", "##prompt##"}:
        model = os.getenv("LLM_REPLY_MODEL", os.getenv("LLM_MODEL", "gpt-4o-mini"))
        return (f"{PROMPT_SHA} {model}")[:160]
    if t.startswith("!trace ") or t.startswith("\\!trace "):
        try:
            probe = t_raw.split(" ", 1)[1]
        except Exception:
            probe = ""
        hits = []
        try:
            if _user_asked_offer(probe):  hits.append("offer")
            if _user_asked_pay(probe):    hits.append("pay")
            if _user_asked_remote(probe): hits.append("remote")
            if _user_asked_human(probe):  hits.append("human")
            if _user_any_question(probe): hits.append("any_q")
        except Exception:
            pass
        return ("TRACE:" + (",".join(hits) if hits else "none"))[:160]

    history = _thread_history((ctx or {}).get("msisdn",""), limit=12)
    prev_sents = _assistant_sentences(history)
    user_text = text or ""

    # Abuse → one polite exit
    if _user_insult(user_text):
        return _final_sms("Supratau. Jei prireiks darbo galimybių – parašykite. Gražios dienos!")

    # Deterministic branches (statement + one qualifier)
    if _user_asked_offer(user_text):
        ans = _value_sentence_once(history) or "Siūlome lanksčius grafikus, greitą pradžią ir paprastą procesą įsidarbinant."
        return _answer_then_ask(ans, history)

    if _user_asked_pay(user_text):
        return _answer_then_ask("Atlygis priklauso nuo vietos ir darbo – suderiname telefonu.", history)

    if _user_asked_remote(user_text):
        return _answer_then_ask("Darbai dažniausiai vietoje; nuotolinis retas.", history)

    if _user_asked_human(user_text):
        return _answer_then_ask("Čia Valandinis SMS asistentas – padėsiu su pagrindiniais klausimais.", history)

    # If user asked any other question → answer (statement) + one qualifier
    if _user_any_question(user_text):
        r = _call(_build_messages(ctx, text))
        reply = _polite((r.choices[0].message.content or "").strip())
        sents = [s for s in _split_sents(reply) if not _mentions_offer(s)]
        kept = [s for s in sents if all(_sim(s, ps) < 0.76 for ps in prev_sents)]
        answer_line = kept[0] if kept else "Gerai, padėsiu suderinti."
        return _answer_then_ask(answer_line, history)

    # No explicit question → ALWAYS move forward: statement ack + ONE qualifier
    r = _call(_build_messages(ctx, text))
    reply = _polite((r.choices[0].message.content or "").strip())
    sents = [s for s in _split_sents(reply) if not _mentions_offer(s)]
    kept = [s for s in sents if all(_sim(s, ps) < 0.76 for ps in prev_sents)]
    answer_line = kept[0] if kept else ""
    # Always append a qualifier unless already closing
    return _answer_then_ask(answer_line, history)

# ==== Classifier (kept compatible; simple fallbacks) ====
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
        tl = _normalize(text)
        if any(w in tl for w in ["nedomina", "nenoriu", "ne, aciu", "ne, ačiū", "aciu ne", "ne.", "ne "]):
            return {"intent": "not_interested", "confidence": 0.8}
        if _user_asked_offer(tl) or _user_any_question(tl):
            return {"intent": "questions", "confidence": 0.8}
        return {"intent": "other", "confidence": 0.5}
