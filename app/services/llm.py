# app/services/llm.py
import os, json, re
from typing import List, Dict
from sqlalchemy import desc
from openai import OpenAI

from app.storage.db import SessionLocal
from app.storage.models import Thread, Message

# Model + client
MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ---------------- System prompt ----------------
SYSTEM_PROMPT = """You are a friendly, human-sounding SMS recruiter for Valandinis (valandinis.lt).

About Valandinis (public info; don’t invent specifics)
– Lankstus valandinis / pamaininis darbas statybose ir giminingose srityse visoje Lietuvoje (pvz.: mūrininkai, elektrikai, santechnikai, betonas, kelių darbai, pagalbiniai; kartais lengva gamyba/retail/maitinimas).
– Greitas startas ir paprasta eiga po trumpos įžangos.

Goal
– Kurk šiltą pokalbį ir įvertink, ar žmogus domisi darbu per Valandinis.
– Jei atvira(s), klausk tik PO VIENĄ dalyką per žinutę šia tvarka: (1) miestas/regionas → (2) specialybė/sritis („kuo dirbate / kokia jūsų specialybė?“) → (3) patirtis metais → (4) pageidaujamas grafikas ARBA nuo kada gali pradėti.
– Niekada proaktyviai neklausk apie atlygį. Jei žmogus klausia apie atlygį/detalų grafiką/vietą/klientus – trumpai pasakyk, kad detales suteiksime skambučiu, ir grįžk prie kito kvalifikavimo punkto (be leidimo prašymo).

Language
– Atsakyk žmogaus kalba, jei tai LT/EN/RU/UA. Jei kita – vieną kartą paklausk, kuri iš šių kalbų patogiau (LT/EN/RU/UA).

Use history & no repeats
– Perskaityk ankstesnius pranešimus; nenaudok tų pačių klausimų antrą kartą, jei informacija jau aiški (miestas/sritis/patirtis/grafikas).

Tone & style (SMS)
– Natūraliai, mandagiai, be biurokratijos, viena SMS (≤160 sim.), vienas klausimas. Venk „dirbat“, vartok „dirbate“ arba „kokia jūsų specialybė?“.
– Trumpai padėkok, jei tinka („Ačiū už atsakymą.“).

Close
– Kai turi bent: miestas + specialybė + patirtis (arba aiškų startą), užbaik: „Susisieksime dėl detalių. Ačiū.“ (be leidimo klausimo).
– Jei STOP / nedomina – padėkok ir užbaik.

Output
– Grąžink tik SMS tekstą (be JSON/paaiškinimų/markdown).
"""

# --------------- DB history helpers ---------------
def _thread_history(phone: str, limit: int = 20) -> List[Dict[str,str]]:
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

# --------------- Light facts extractor ---------------
_CITY_HINTS = ("viln", "kaun", "klaip", "šiaul", "siaul", "panev", "alyt", "kedain", "kėdain", "gargžd", "gargzd", "marij", "mažeik", "mazeik", "telš", "tels")
_TRADES_HINTS = ("mūrin", "mur", "elektr", "santech", "beton", "mechan", "vairuotoj", "dailid", "pagalbin", "stali", "suvir", "tinku")
def _extract_known(history: List[Dict[str,str]]) -> Dict[str,str]:
    known = {"city": "", "trade": "", "exp": "", "start": ""}
    for m in history:
        if m["role"] != "user":
            continue
        t = m["content"].lower()

        # city/region
        if not known["city"]:
            if any(h in t for h in _CITY_HINTS) or "miest" in t or "region" in t or "raj" in t:
                known["city"] = m["content"].strip()

        # trade/specialty
        if not known["trade"]:
            if any(h in t for h in _TRADES_HINTS) or "specialyb" in t or "sritis" in t or "dirbu" in t:
                known["trade"] = m["content"].strip()

        # experience
        if not known["exp"]:
            mobj = re.search(r"(\d+)\s*(m(et|ė|e|etu|etus|etų)?|yrs|years)", t)
            if mobj:
                known["exp"] = m["content"].strip()
            elif re.search(r"\b(beveik|apie)\s*\d+\b", t):
                known["exp"] = m["content"].strip()
            elif re.search(r"\bpradedant", t):
                known["exp"] = m["content"].strip()

        # start date / availability
        if not known["start"]:
            if re.search(r"(nuo|gal(i|e)čiau|galiu|pradė(si|t)|rytoj|kitą|kita|šiandien|siandien)", t):
                known["start"] = m["content"].strip()
    return known

# --------------- Build messages ---------------
def _build_messages(ctx: dict, text: str):
    msgs = [{"role": "system", "content": SYSTEM_PROMPT}]
    hist = []
    if ctx and ctx.get("msisdn"):
        hist = _thread_history(ctx["msisdn"], limit=20)
        msgs += hist

    # add a compact memory hint so the model avoids repeats
    known = _extract_known(hist)
    known_pairs = [f"{k}={v}" for k, v in known.items() if v]
    if known_pairs:
        msgs.append({"role": "system",
                     "content": "Jau žinoma iš ankstesnių žinučių: " + "; ".join(known_pairs) +
                                ". Neklauskite to dar kartą; pereikite prie kito punkto."})

    # finally the current user message
    msgs.append({"role": "user", "content": text})
    return msgs, hist

# --------------- Offer/wording guards ---------------
_OFFER_PHRASES = tuple(x.lower() for x in (
    "lankstus grafikas, greitas startas",
    "lanksčius grafikus, greitą startą",
    "lanksčius darbo grafikus",
    "lankstų grafiką",
    "greitą startą ir paprast",
    "paprastą įsidarbinimo eigą",
    "paprastą įdarbinimo eigą",
    "pasiūlymų gausa statybose",
))
_DETAIL_TRIGGERS = tuple(x.lower() for x in (
    "ką siūlote","ka siulote","ką galite pasiūlyti","ka galite pasiulyti",
    "ką turite","ka turite","o ką turite","o ka turite",
    "what do you offer","tell me more","more info"
))

def _assistant_said_offer(history: List[Dict[str,str]]) -> bool:
    for m in history:
        if m.get("role") == "assistant":
            low = (m.get("content") or "").lower()
            if any(p in low for p in _OFFER_PHRASES):
                return True
    return False

def _strip_offers(reply: str) -> str:
    if not reply:
        return reply
    r = reply
    for p in _OFFER_PHRASES:
        r = re.sub(rf"[^.?!]*{re.escape(p)}[^.?!]*[.?!]?", " ", r, flags=re.I)
    r = re.sub(r"\s{2,}", " ", r).strip()
    return r

def _polite(text: str) -> str:
    if not text:
        return text
    t = text
    t = re.sub(r"\b[Dd]irbat\b", "dirbate", t)
    t = re.sub(r"\b[Kk](ą|a)\s+dirb\w*\b\??", "Kokia jūsų specialybė ar sritis?", t)
    t = t.replace("Kokį miestą ar regioną galėtumėte nurodyti?", "Kuriame mieste ar regione galite dirbti?")
    t = t.replace("Kuriame mieste dirbate arba norėtumėte dirbti?", "Kuriame mieste ar regione dirbate?")
    # tone down exclamations
    t = t.replace("!", ".")
    # remove self-summaries like "Turiu Klaipėdą ir mūrininko specialybę."
    t = re.sub(r"\b[Tt]uriu\b[^.?!]*[.?!]", "", t)
    return re.sub(r"\s{2,}", " ", t).strip()

def _keep_one_question(reply: str) -> str:
    if not reply:
        return reply
    parts = re.split(r"(?<=[.?!])\s+", reply.strip())
    questions = [p for p in parts if p.endswith("?")]
    if questions:
        return questions[-1]  # only the last question
    return parts[-1]

def _friendly_prefix(history: List[Dict[str,str]], reply: str) -> str:
    if not reply or len(reply) > 140:
        return reply
    if history and history[-1].get("role") == "user":
        low = reply.lower()
        if not low.startswith(("ačiū", "aciu", "dėkui", "dekui")):
            return "Ačiū už atsakymą. " + reply
    return reply

# --------------- OpenAI call + postprocess ---------------
def _call(messages: List[Dict[str,str]]):
    return client.chat.completions.create(model=MODEL, messages=messages)

def _postprocess(history: List[Dict[str,str]], user_text: str, reply: str) -> str:
    if not reply:
        return ""

    # If reply already closes, finalize it
    if "Susisieksime dėl detalių" in reply:
        return "Susisieksime dėl detalių. Ačiū."

    asked_details_now = any(k in (user_text or "").lower() for k in _DETAIL_TRIGGERS)
    offered_before     = _assistant_said_offer(history)

    # Allow value/offer line ONLY once (first time they ask). Otherwise strip all offers.
    if asked_details_now and not offered_before:
        # keep model’s question but kill duplicate offers, then add a single concise value sentence once
        reply = _strip_offers(reply)
        reply = "Turime lanksčius grafikus ir greitą startą. " + reply
    else:
        reply = _strip_offers(reply)

    # Keep only one question
    reply = _keep_one_question(reply)

    # Polite wording
    reply = _polite(reply)

    # Add a short friendly prefix when there’s room
    reply = _friendly_prefix(history, reply)

    # Enforce <=160 chars
    if len(reply) > 160:
        reply = reply[:160].rstrip()
    return reply

# --------------- Public functions ---------------
def _build_and_call(ctx: dict, text: str) -> str:
    messages, hist = _build_messages(ctx, text)
    r = _call(messages)
    raw = (r.choices[0].message.content or "").strip()
    return _postprocess(hist, text or "", raw)

def generate_reply_lt(ctx: dict, text: str) -> str:
    return _build_and_call(ctx, text)

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
        # treat “what do you offer” as 'questions'
        if any(w in tl for w in ["ką siūlot", "ka siulot", "ką turite", "ka turite", "ką galite pasiūlyti", "ka galite pasiulyti", "tell me more", "what do you offer", "more info"]):
            return {"intent": "questions", "confidence": 0.8}
        if any(w in tl for w in ["nedomina", "nenoriu", "ačiū, ne", "aciu ne", "ne.", "ne "]):
            return {"intent": "not_interested", "confidence": 0.8}
        return {"intent": "other", "confidence": 0.5}
