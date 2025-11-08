# app/services/llm.py
import os, json, re, hashlib, time
from typing import List, Dict, Optional
from sqlalchemy import desc
from openai import OpenAI

from app.storage.db import SessionLocal
from app.storage.models import Thread, Message

# ==== Models / client ====
MODEL = os.getenv("LLM_REPLY_MODEL", os.getenv("LLM_MODEL", "gpt-4o-mini"))
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ==== Constants (not templates) ====
VALUE_LINE   = "Siūlome lanksčius grafikus, greitą pradžią ir paprastą procesą įsidarbinant."
CLOSE_TX     = "Perduosiu kolegai – paskambins dėl detalių."
YEAR_MAX     = 70
HUMAN_CLOSE  = "Ačiū už Jūsų laiką — užsirašiau. " + CLOSE_TX
TROLL_CLOSE  = "Palikime čia. Jei rimtai domins darbas, parašykite."
DNC_CLOSE    = "Supratau — daugiau nerašysime. Gražios dienos!"
FUTURE_CLOSE = f"Puiku — užsirašysiu ateičiai. {VALUE_LINE}"

# ==== SYSTEM PROMPT (rules only) ====
SYSTEM_PROMPT = """
You write ONE Lithuanian SMS (≤160 chars) on behalf of Valandinis (valandinis.lt).

Objectives:
1) Be human: answer the person’s last message first (1 short line), with a warm, non-pushy tone.
2) Determine interest in THIS opening (city + trade). Treat genuine job/salary/schedule questions as interest — EXCEPT on the very first reply after the opener, where you must ask an explicit interest check instead of closing.
3) If clearly interested, you may close quickly; otherwise collect years → availability, then close.

Hard rules:
- Lithuanian; 1–2 short sentences; no exclamation spam.
- Identity only in the first bot message of the thread. Never say you are a bot/AI.
- Don’t invent pay/clients/precise locations/contract terms; if asked, say it’s best aligned by phone and follow with one helpful next step.
- City & trade come from the opener; don’t change unless corrected by the person.
- Slot order: years first, then availability. Never ask availability before years.
- If user asks to call, you can close immediately.
- Banned phrase: “Ar aktualu dabar, ar palikti ateičiai?” and any meta like “esu Valandinis.lt”.

Output: ONLY the final SMS text.
""".strip()

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
        if not t: return []
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

# ==== Utilities ====
def _final_sms(s: str) -> str:
    s = re.sub(r"\s+", " ", (s or "")).strip()
    return (s[:157].rstrip() + "…") if len(s) > 160 else s

def _assistant_has(history: List[Dict[str,str]], needle: str) -> bool:
    n = (needle or "").lower()
    for m in history:
        if m["role"] == "assistant" and n in (m["content"] or "").lower():
            return True
    return False

def _assistant_has_phrase(history: List[Dict[str,str]], phrase: str) -> bool:
    p = (phrase or "").lower()
    for m in history:
        if m["role"] == "assistant" and p in (m["content"] or "").lower():
            return True
    return False

def _identity_already_used(history: List[Dict[str,str]]) -> bool:
    return any("valandinis.lt" in (m["content"] or "").lower() for m in history if m["role"]=="assistant")

def _strip_repeated_value_line(msg: str, history: List[Dict[str,str]]) -> str:
    if not msg: return msg
    if _assistant_has(history, VALUE_LINE.lower()):
        parts = [s.strip() for s in re.split(r'(?<=[\.\!\?])\s+', msg) if s.strip()]
        parts = [s for s in parts if VALUE_LINE.lower() not in s.lower()]
        return " ".join(parts).strip()
    return msg

def _num_user_msgs(history: List[Dict[str,str]]) -> int:
    return sum(1 for m in history if m["role"] == "user")

def _is_first_user_reply_after_opener(history: List[Dict[str,str]]) -> bool:
    return _num_user_msgs(history) == 1 and any(m["role"]=="assistant" for m in history)

def _last_assistant_text(history: List[Dict[str,str]]) -> str:
    for m in reversed(history):
        if m["role"] == "assistant":
            return (m.get("content") or "")
    return ""

# ==== Robust "already closed" detection ====
def _norm(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[–—−-]", "-", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"[\.!\s]+$", "", s)
    s = s.replace("ė", "e").replace("ą","a").replace("č","c").replace("ę","e").replace("į","i").replace("š","s").replace("ų","u").replace("ū","u").replace("ž","z")
    return s

CLOSE_PATTERNS = [
    re.compile(r"\bperduos(iu|iu)\s+koleg\w+.*\bpaskambins\b"),
    re.compile(r"\baci(u|u)\s+uz\s+(jusu\s+)?laika.*perduos(iu|iu)\s+koleg\w+"),
    re.compile(r"\bsupratau\s*-\s*daugiau\s+nerasysime\b"),
    re.compile(r"\buzsirasi(si(u|u)|au)\s+ateiciai\b"),
    re.compile(r"\bpalikime\s+cia\b"),
]

POSTCLOSE_ACK_RE = re.compile(r"\b(ačiū|aciu|dėkui|dekui|ir\s+jums|geros\s+dienos|ok|okey|okei|👍|thanks)\b", re.I)

def _assistant_last_was_close(history: List[Dict[str,str]]) -> bool:
    last = _norm(_last_assistant_text(history))
    if not last:
        return False
    if ("perduosiu koleg" in last and "paskambins" in last) or \
       ("daugiau nerasysime" in last) or \
       ("uzsirasi" in last and "ateiciai" in last) or \
       ("palikime cia" in last):
        return True
    return any(rx.search(last) for rx in CLOSE_PATTERNS)

# NEW: determine which close we sent (binds KPI directly)
def _last_close_type(history: List[Dict[str,str]]) -> Optional[str]:
    last = _norm(_last_assistant_text(history))
    if not last:
        return None
    hc = _norm(HUMAN_CLOSE)
    dc = _norm(DNC_CLOSE)
    fc = _norm(FUTURE_CLOSE)
    tx = _norm(CLOSE_TX)
    tc = _norm(TROLL_CLOSE)

    # direct contains
    if hc in last or tx in last or ("perduosiu koleg" in last and "paskambins" in last):
        return "human"   # considered "interested" for KPI
    if fc in last or ("uzsirasi" in last and "ateiciai" in last):
        return "future"  # interested with future_maybe=True
    if dc in last or "daugiau nerasysime" in last:
        return "dnc"     # not interested
    if tc in last or "palikime cia" in last:
        return "troll"   # not interested

    # fallback to regex family
    for rx in CLOSE_PATTERNS:
        if rx.search(last):
            # map by heuristic
            if "daugiau nerasysime" in last:
                return "dnc"
            if "ateiciai" in last:
                return "future"
            if "paskambins" in last:
                return "human"
            return "human"
    return None

# ==== Lightweight extractors ====
_YEARS_PAT  = re.compile(r"\b([0-9]{1,3})\s*(m\.|metai|metu|metus)\b", re.I)
_MONTHS_PAT = re.compile(r"\b([0-9]{1,3})\s*(m[eė]n\.?|m[eė]nes(?:iai|ius|i|į)?)\b", re.I)
_WEEKS_PAT  = re.compile(r"\b([0-9]{1,3})\s*(sav(?:ait[ėe]s?)?)\b", re.I)
_DAYS_PAT   = re.compile(r"\b([0-9]{1,3})\s*(dien(?:a|os)?)\b", re.I)
_AVAIL_PAT  = re.compile(r"\b(nuo\s+[\w\-\.]+|rytoj|šiandien|kit(a|ą)\s+savait(ė|e)|nuo\s+kitos\s+savait(ė|e)|iškart)\b", re.I)

AGE_Q_PAT   = re.compile(r"\b(nuo\s+kiek\s+met(ų|u)|kiek\s+met(ų|u)\s+galima\s+dirbti|priimate\s+nuo\s+kiek|įdarbinate\s+nuo)\b", re.I)
AGE_VAL_PAT = re.compile(r"\b(man\s+)?([0-9]{1,2})\s*m(et(ų|u)|\.)\b", re.I)

SAL_KWS     = re.compile(r"\b(alga|atlyg|įkain|tarif|mok(at|ėt)|eur|€|rate|pay|klient|lokacij|adresas|tiksli\s+vieta)\w*\b", re.I)

OPENER_SPEC_PAT_A = re.compile(r"ieškome\s+([a-ząčęėįšųūž\-]+)", re.I)
OPENER_SPEC_PAT_B = re.compile(r"kuriame\s+reikalingas\s+([a-ząčęėįšųūž\-]+)", re.I)
USER_TRADE_PAT    = re.compile(r"\b(aš|as)?\s*(esu|es[uū])?\s*([a-ząčęėįšųūž\-]+inkas|elektrikas|santechnikas|mūrininkas|dažytojas|apdailininkas|stogdengys|suvirintojas|tinkuotojas|plytelių\s+klojėjas)\b", re.I)

def _parse_experience_years(text: str) -> Optional[int]:
    t = text or ""
    if _MONTHS_PAT.search(t) or _WEEKS_PAT.search(t) or _DAYS_PAT.search(t): return 0
    m = _YEARS_PAT.search(t)
    if not m: return None
    try: yrs = int(m.group(1))
    except Exception: return None
    if yrs < 0 or yrs > YEAR_MAX: return None
    return yrs

def _extract_availability(text: str) -> Optional[str]:
    m = _AVAIL_PAT.search(text or "")
    return m.group(0) if m else None

def _extract_opener_specialty(history: List[Dict[str,str]]) -> Optional[str]:
    for m in history:
        if m["role"] != "assistant": continue
        txt = m.get("content") or ""
        for pat in (OPENER_SPEC_PAT_A, OPENER_SPEC_PAT_B):
            mt = pat.search(txt)
            if mt: return mt.group(1).lower()
    return None

def _extract_user_trade(text: str) -> Optional[str]:
    m = USER_TRADE_PAT.search(text or "")
    if not m: return None
    return (m.group(0) or "").strip().lower()

def _asked_which_slot(msg: str) -> Optional[str]:
    s = (msg or "").lower()
    if "kiek metų patirties" in s: return "years"
    if "nuo kada galėtumėte pradėti" in s or "koks grafikas tinka" in s: return "availability"
    return None

def _inferred_slots(history: List[Dict[str,str]]) -> Dict[str, Optional[str]]:
    slots = {"years": None, "availability": None}
    for i in range(len(history)-2, -1, -1):
        a = history[i]; b = history[i+1] if i+1 < len(history) else None
        if not b: continue
        if a["role"] == "assistant" and b["role"] == "user":
            which = _asked_which_slot(a.get("content",""))
            if not which: continue
            ans = (b.get("content") or "").strip()
            if which == "years":
                if AGE_VAL_PAT.search(ans) and "patirt" not in ans.lower():
                    y = None
                else:
                    y = _parse_experience_years(ans)
                slots["years"] = y
            elif which == "availability":
                slots["availability"] = _extract_availability(ans) or None
    return slots

# Small acknowledgements and question detection
QUESTION_INTENTS = {"project_question","direct_question","salary_question","schedule_question","location_question"}
LT_Q_WORDS = re.compile(r"\b(kas|koks|kokia|kada|kur|kaip|kiek|del ko|dėl ko)\b", re.I)
def _looks_like_question(txt: str) -> bool:
    t = (txt or "").strip()
    return t.endswith("?") or bool(LT_Q_WORDS.search(t))

def _ack_prefix(user_text: str) -> str:
    if len((user_text or "").strip()) <= 3:
        return ""
    if any(w in (user_text or "").lower() for w in ["ačiū","aciu","ok","gerai","supratau"]):
        return "Ačiū. "
    return "Supratau. "

# === Future-interest helpers (to prevent loops and close correctly) ===
def _assistant_future_probe_asked(history: List[Dict[str,str]]) -> bool:
    needles = [
        "ar ateityje norėtumėte bendradarbiauti",
        "ar ateityje norėtumėte susisiekti",
        "ar ateityje domintų",
        "palaikyti ryšį ateičiai",
    ]
    h = " ".join((m["content"] or "").lower() for m in history if m["role"] == "assistant")
    return any(n in h for n in needles)

def _user_future_yes(text: str) -> bool:
    t = (text or "").lower()
    return any(w in t for w in ["taip", "tiktų", "norėčiau", "noreciau", "gal", "galbūt", "galbut", "būtų", "butu", "ateityje"])

# --- Last-assistant question classifier (turn-local) ---
def _last_assistant_question_type(history: List[Dict[str, str]]) -> Optional[str]:
    last = _last_assistant_text(history)
    if not last:
        return None
    s = _norm(last)
    if "ar ateityje noretumete bendradarbiauti" in s or "palaikyti rysi ateiciai" in s:
        return "future_probe"
    if "ar sis pasiulymas jums aktualus" in s or "ar aktualu" in s:
        return "interest_check"
    if "kiek metu patirties" in s:
        return "years_q"
    if "nuo kada galetumete pradeti" in s or "koks grafikas tinka" in s:
        return "availability_q"
    return None

# --- Short, hard yes/no maps (Lithuanian variants) ---
_AFFIRM_RX = re.compile(r"^\s*(taip|jo|ok|tinka|tiktu|gerai|domina|galiu|gal|galbut|nor\w+|butu|būtų)\b", re.I)
_DECLINE_RX = re.compile(r"^\s*(ne|nedomina|neaktualu|nenoriu|nereikia)\b", re.I)

def _is_affirmative(txt: str) -> bool:
    return bool(_AFFIRM_RX.search((txt or "")))

def _is_decline(txt: str) -> bool:
    return bool(_DECLINE_RX.search((txt or "")))

# --- Polisher ---
_BAD_LINES = re.compile(r"(Ar aktualu dabar, ar palikti ateičiai\?)", re.I)
def _polish(reply: str, history: List[Dict[str,str]], opener_spec: Optional[str]) -> str:
    if not reply: return reply
    r = reply.strip()
    if _identity_already_used(history):
        r = re.sub(r"\b(C|c)i?a?\s*Valandinis\.?lt\b|\b[Ee]su\s+Valandinis\.?lt\b", "", r).strip(",. ").strip()
    r = re.sub(r"\b[Ee]su\s+Valandinis\.?lt\b", "Rašau iš Valandinis.lt", r)
    if "specialist" in r.lower():
        r = re.sub(r"[Ss]pecialist\w*", (opener_spec or "statybų srityje"), r)
    r = _BAD_LINES.sub("", r).strip()
    r = re.sub(r"^(Sveiki,?\s+){2,}", "Sveiki, ", r)
    if r.count("?") > 1:
        r = r.split("?")[0] + "?"
    r = _strip_repeated_value_line(r, history)
    if re.search(r"[A-Za-z]{3,}", r) and not re.search(r"[ĄČĘĖĮŠŲŪŽąčęėįšųūž]", r):
        r = "Atsakykite trumpai lietuviškai ir tęsime."
    return r

# ==== Close rules (deterministic user-triggered) ====
# tightened trolling: wider Lithuanian profanity/insults and crude slang
TROLL_RX = re.compile(
    r"\b(šūdas|byb|bybis|pizd|pyzd|nx|nahui|nahuy|debila?s?|idiotas?|lochas?|lauchas?|kvailys|durnius|eik\s+na(ch|x)|fuck|f\*+k|wtf|bitch|asshole)\b",
    re.I
)

CLOSE_RULES = [
    ("dnc", re.compile(
        r"\b(stop|ne?be(trukdyk(it)?|rašyk(it)?|siųsk(it)?|junk(it)?))\b|"
        r"\b(atsisakau|unsubscribe|nebesusisiek(it)?|nenoriu\s+gauti)\b|"
        r"\b(ištrink(it)?\s+mano\s+numerį|netrukdyk(it)?)\b", re.I),
     DNC_CLOSE),
    # removed "decline" auto-close to allow semantic future probe
    ("future", re.compile(
        r"\b(ateityje|vėliau|gal\s*(vėliau|ateityje)|kai\s+bus\s+laisviau|dabar\s+ne,?\s*bet|kol\s+kas\s+ne)\b", re.I),
     FUTURE_CLOSE),
    ("call", re.compile(
        r"\b(skambink(it)?|paskambink(it)?|galite\s+paskambinti|paskambinsiu|susiskambinkim)\b|"
        r"\b(duokit\s+numerį|duok\s+nr|perduok\s+kolegai)\b", re.I),
     "Puiku — perduosiu kolegai, jis jums paskambins."),
    ("accept", re.compile(
        r"\b(taip|jo|ok|tinka|domina|gerai|priimu|galima)\b|"
        r"\b(galiu\s+dirbti|esu\s+laisvas|pradėti\s+galiu|nuo\s+[\w\-\.]+)\b", re.I),
     HUMAN_CLOSE),
    ("wrong", re.compile(
        r"\b(ne\s*tas\s*numeris|neteisingas\s*numeris|čia\s*ne\s*jis|apsirikot)\b|"
        r"\b(nesi(u|) aš\s+\w+?ikas|ne\s*elektrikas|ne\s*ta\s*specialybė)\b", re.I),
     "Atsiprašome už trukdymą — patikslinsime kontaktus. Daugiau nerašysime."),
    ("troll", TROLL_RX, TROLL_CLOSE),
]

def _apply_close_rules(user_text: str) -> Optional[str]:
    t = user_text or ""
    for _name, rx, msg in CLOSE_RULES:
        if rx.search(t):
            return msg
    return None

# Hard-stop detector (mirrors the DNC pattern)
_HARD_STOP_RX = re.compile(
    r"\b(stop|ne?be(trukdyk(it)?|rašyk(it)?|siųsk(it)?|junk(it)?))\b|"
    r"\b(atsisakau|unsubscribe|nebesusisiek(it)?|nenoriu\s+gauti)\b|"
    r"\b(ištrink(it)?\s+mano\s+numerį|netrukdyk(it)?)\b",
    re.I
)
def _is_hard_stop(txt: str) -> bool:
    return bool(_HARD_STOP_RX.search(txt or ""))

# ==== Analyzer (semantics only) ====
ANALYZER_SYS = """
You analyze a short Lithuanian SMS thread about a city+trade opening. Return STRICT JSON:
{
  "job_interest": "yes" | "no" | "unsure" | "unknown",
  "future_interest": "yes" | "no" | "unsure" | "unknown",
  "intent": "identity_question" | "project_question" | "salary_question" | "schedule_question" | "location_question" |
            "direct_question" | "provide_years" | "provide_availability" | "call_request" |
            "accept" | "decline" | "hesitant" | "unrelated" | "other",
  "slots": { "years": null|number, "availability_text": null|string },
  "asked_salary": boolean,
  "phone_only_topics": ["salary"|"clients"|"precise_location"|"contract_terms"|"schedule_details"|"age"...],
  "busy_until": null|string,
  "hesitant": boolean,
  "age_question": boolean,
  "age_value": null|number,
  "trolling": boolean
}

Rules:
- Genuine questions about job/salary/schedule/location imply interest unless there is an explicit decline.
- Months/weeks/days experience → years=0.
- "please call me", "can I call", "paskambinsiu", etc. → intent="call_request".
- Detect obvious trolling (absurd answers) → trolling=true.
Return JSON only.
"""

def analyze(user_text: str, short_history: List[Dict[str,str]]) -> dict:
    msgs = [{"role":"system","content":ANALYZER_SYS}]
    msgs += short_history[-6:] if short_history else []
    msgs.append({"role":"user","content":user_text})
    r = client.chat.completions.create(model=MODEL, temperature=0, messages=msgs, max_tokens=220)
    raw = (r.choices[0].message.content or "").strip()
    try:
        obj = json.loads(raw)
    except Exception:
        obj = {}
    plan = {
        "job_interest": (obj.get("job_interest") or "unknown"),
        "future_interest": (obj.get("future_interest") or "unknown"),
        "intent": (obj.get("intent") or "other"),
        "slots": obj.get("slots") or {"years": None, "availability_text": None},
        "asked_salary": bool(obj.get("asked_salary") or False),
        "phone_only_topics": obj.get("phone_only_topics") or [],
        "busy_until": obj.get("busy_until"),
        "hesitant": bool(obj.get("hesitant") or False),
        "age_question": bool(obj.get("age_question") or False),
        "age_value": obj.get("age_value"),
        "trolling": bool(obj.get("trolling") or False),
    }
    yrs = plan["slots"].get("years")
    if yrs is not None:
        try: yrs = int(yrs)
        except Exception: yrs = None
        if yrs is None or yrs < 0 or yrs > YEAR_MAX: plan["slots"]["years"] = None
        else: plan["slots"]["years"] = yrs
    if plan["age_question"] or plan["age_value"] is not None:
        if "age" not in plan["phone_only_topics"]:
            plan["phone_only_topics"].append("age")
    if plan["job_interest"] in ("unknown","unsure") and (plan["intent"] in QUESTION_INTENTS or _looks_like_question(user_text)):
        plan["job_interest"] = "yes"
    return plan

# ==== Generator ====
GENERATOR_SYS = SYSTEM_PROMPT

def generate_sms(plan: dict, short_history: List[Dict[str,str]]) -> str:
    msgs = [{"role":"system","content":GENERATOR_SYS}]
    msgs.append({"role":"user","content":json.dumps({
        "plan": plan,
        "history_tail": [{"role":m["role"],"content":m["content"]} for m in (short_history[-6:] if short_history else [])]
    }, ensure_ascii=False)})
    r = client.chat.completions.create(model=MODEL, temperature=0.35, messages=msgs, max_tokens=140)
    return (r.choices[0].message.content or "").strip()

# ==== Legacy helpers (kept) ====
def _call(messages):
    return client.chat.completions.create(model=MODEL, messages=messages, temperature=0.2, max_tokens=120)

def _build_messages(history: List[Dict[str,str]], user_text: str) -> List[Dict[str,str]]:
    short_hist = history[-12:] if len(history) > 12 else history
    msgs = [{"role": "system", "content": SYSTEM_PROMPT}]
    msgs += short_hist
    msgs.append({"role": "user", "content": user_text})
    return msgs

# ==== Close logging helpers ====
def _log_closed(msisdn: str) -> None:
    try:
        print(json.dumps({"ev": "thread_closed", "msisdn": msisdn, "ts": int(time.time())}, ensure_ascii=False), flush=True)
    except Exception:
        pass

def _close_and_return(msisdn: str, msg: str, history: List[Dict[str,str]], opener_spec: Optional[str]) -> str:
    out = _final_sms(_polish(msg, history, opener_spec))
    _log_closed(msisdn)
    return out

# ==== Main generator ====
def generate_reply_lt(ctx: dict, text: str) -> str:
    t_raw = (text or "").strip()
    if not t_raw: return ""

    t_lower = t_raw.lstrip("\\").lower()
    if t_lower in {"!prompt", "!pf", "##prompt##"}:
        return (f"{PROMPT_SHA} {MODEL}")[:160]

    msisdn = (ctx or {}).get("msisdn","")
    history = _thread_history(msisdn, limit=14)

    # If we already closed, swallow any post-close pleasantries and never re-open
    if _assistant_last_was_close(history):
        if POSTCLOSE_ACK_RE.search(t_lower):
            return ""
        return ""

    # Pre-LLM deterministic close rules
    close_msg = _apply_close_rules(t_raw)
    if close_msg:
        return _close_and_return(msisdn, close_msg, history, _extract_opener_specialty(history))

    # Robot check → identity (no bot mention)
    if re.search(r"\b(robot|bot|dirbtin|ai)\b", t_raw, re.I):
        ident = "Rašau iš Valandinis.lt." if not _identity_already_used(history) else ""
        return _final_sms(_polish(ident or " ", history, _extract_opener_specialty(history)))

    # Analyze semantics
    plan = analyze(t_raw, history)

    # ---- Turn-local disambiguation: map user's yes/no to the last assistant question ----
    last_q = _last_assistant_question_type(history)
    if last_q == "future_probe":
        if _is_affirmative(t_raw) and plan.get("intent") not in {"accept", "call_request"}:
            plan["future_interest"] = "yes"
            plan["job_interest"] = "no"
        elif _is_decline(t_raw):
            plan["future_interest"] = "no"
            plan["job_interest"] = "no"
    elif last_q == "interest_check":
        if _is_affirmative(t_raw):
            plan["job_interest"] = "yes"
        elif _is_decline(t_raw):
            plan["job_interest"] = "no"

    # Trolling soft-stop
    if plan.get("trolling"):
        return _close_and_return(msisdn, TROLL_CLOSE, history, _extract_opener_specialty(history))

    # Merge slot memory
    slots_hist = _inferred_slots(history)
    have_years_hist = slots_hist.get("years") is not None
    have_avail_hist = slots_hist.get("availability") is not None
    plan["have_years"] = have_years_hist or (plan.get("slots") or {}).get("years") is not None
    plan["have_availability"] = have_avail_hist or bool((plan.get("slots") or {}).get("availability_text"))
    if plan.get("busy_until"):
        plan["have_availability"] = True

    opener_spec = _extract_opener_specialty(history)
    user_trade  = _extract_user_trade(t_raw)
    plan["opener_specialty"]   = opener_spec
    plan["user_trade"]         = user_trade
    plan["specialty_mismatch"] = bool(opener_spec and user_trade and (user_trade != opener_spec))

    # === First-reply guard: answer + interest check, no closing ===
    if (_is_first_user_reply_after_opener(history)
        and (plan.get("intent") in QUESTION_INTENTS or _looks_like_question(t_raw))):
        identity = "" if _identity_already_used(history) else "Rašau iš Valandinis.lt. "
        role_word = opener_spec or "elektriko"
        if plan.get("intent") == "salary_question":
            answer = "Ačiū už klausimą — atlygio klausimą patogiausia suderinti telefonu."
        else:
            answer = f"{_ack_prefix(t_raw)}Tai darbas {role_word} pozicijoje Klaipėdoje; detales suderinsime telefonu."
        nxt = " Ar šis pasiūlymas jums aktualus?"
        msg = (identity + answer + nxt).strip()
        return _final_sms(_polish(msg, history, opener_spec))

    # === Q-FIRST (subsequent turns) ===
    if plan.get("intent") in QUESTION_INTENTS or _looks_like_question(t_raw):
        role_word = opener_spec or "elektriko"
        if plan.get("intent") == "salary_question":
            answer = "Ačiū už klausimą — dėl atlygio patogiausia suderinti telefonu, kolega paskambins."
        else:
            answer = f"{_ack_prefix(t_raw)}Tai darbas {role_word} pozicijoje; konkrečias sąlygas suderinsime telefonu."
        interested_now = plan.get("job_interest") == "yes"
        if interested_now:
            return _close_and_return(msisdn, HUMAN_CLOSE, history, opener_spec)
        else:
            nxt = "Ar šis pasiūlymas jums aktualus?"
            return _final_sms(_polish(answer + " " + nxt, history, opener_spec))

    # === Identity question only ===
    if plan.get("intent") == "identity_question":
        ident = "Rašau iš Valandinis.lt." if not _identity_already_used(history) else ""
        follow = " Ar šis pasiūlymas jums aktualus?"
        return _final_sms(_polish((ident + follow).strip(), history, opener_spec))

    # === Call request ===
    if plan.get("intent") == "call_request":
        return _close_and_return(msisdn, "Puiku — perduosiu kolegai, jis jums paskambins.", history, opener_spec)

    # === Interested: close early with minimal info ===
    if plan.get("job_interest") == "yes":
        # Guard: do NOT human-close if the last question was a future probe (unless explicit accept/call)
        if _last_assistant_question_type(history) == "future_probe" and plan.get("intent") not in {"accept", "call_request"}:
            plan["job_interest"] = "no"
        else:
            if plan["have_years"] and plan["have_availability"]:
                return _close_and_return(msisdn, HUMAN_CLOSE, history, opener_spec)
            if plan["have_years"] or plan["have_availability"]:
                return _close_and_return(msisdn, HUMAN_CLOSE, history, opener_spec)
            if not plan["have_years"]:
                q = f"Kiek metų patirties turite kaip {(opener_spec or 'meistras')}?"
                return _final_sms(_polish(q, history, opener_spec))
            return _final_sms(_polish("Nuo kada galėtumėte pradėti arba koks grafikas tinka?", history, opener_spec))

    # === Decline / future-interest handling (fixed) ===
    if plan.get("job_interest") == "no":
        # 1) Hard stop → immediate DNC
        if _is_hard_stop(t_raw):
            return _close_and_return(msisdn, DNC_CLOSE, history, opener_spec)
        # 2) Explicit future/maybe → future close
        if plan.get("future_interest") == "yes" or _user_future_yes(t_raw):
            return _close_and_return(msisdn, FUTURE_CLOSE, history, opener_spec)
        # 3) Probe once if not asked yet
        if plan.get("future_interest") in ("unknown", "unsure") and not _assistant_future_probe_asked(history):
            q = "Supratau, ačiū. Ar ateityje norėtumėte bendradarbiauti su Valandinis.lt?"
            return _final_sms(_polish(q, history, opener_spec))
        # 4) Otherwise close DNC
        return _close_and_return(msisdn, DNC_CLOSE, history, opener_spec)

    # === Neutral/unsure → normal generation ===
    reply = generate_sms(plan, history)

    # Strip phone-only if not asked
    if not (plan.get("asked_salary") or plan.get("age_question") or plan.get("age_value") is not None) \
       and not any(k in plan.get("phone_only_topics", []) for k in ["salary","age"]):
        sents = [s.strip() for s in re.split(r'(?<=[\.\!\?])\s+', reply) if s.strip()]
        kept = []
        for s in sents:
            if SAL_KWS.search(s) or AGE_Q_PAT.search(s) or AGE_VAL_PAT.search(s):
                continue
            kept.append(s)
        reply = " ".join(kept) if kept else reply

    # Enforce slot order
    if ("kada galėtumėte pradėti" in reply.lower() or "koks grafikas tinka" in reply.lower()):
        if not plan["have_years"]:
            reply = f"Kiek metų patirties turite kaip {(opener_spec or 'meistras')}?"

    return _final_sms(_polish(reply, history, opener_spec))

# ==== Simple classifier (semantic) ====
def classify_lt(text: str) -> dict:
    sys = (
        "You are a STRICT classifier for Lithuanian SMS about jobs. "
        "Return exactly one of: 'questions', 'not_interested', 'other'. "
        "Rules: if the message clearly declines or asks to stop, it's 'not_interested'. "
        "If it asks a genuine question about the job, it's 'questions'. Otherwise 'other'. "
        'Reply ONLY with JSON: {"intent": <string>, "confidence": <0..1 number>}.'
    )
    try:
        r = client.chat.completions.create(
            model=MODEL, temperature=0,
            messages=[{"role":"system","content":sys},{"role":"user","content":(text or "")}],
            max_tokens=60
        )
        content = (r.choices[0].message.content or "").strip()
        try:
            obj = json.loads(content)
        except Exception:
            m = re.search(r"\{.*\}", content, re.S)
            obj = json.loads(m.group(0)) if m else {}
        intent = (obj.get("intent") or "").lower().strip()
        conf = float(obj.get("confidence") or 0.6)
        if intent not in {"questions", "not_interested", "other"}:
            intent = "other"
        return {"intent": intent, "confidence": max(0.0, min(conf, 1.0))}
    except Exception:
        return {"intent": "other", "confidence": 0.5}

# ==== LLM-DRIVEN OPENER ====
OPENER_SYS = """
Write one Lithuanian SMS (≤160 chars) to open a recruiting chat for Valandinis.lt.

Use this structure, fixing endings (city locative, trade nominative in the clause):
"Sveiki! Čia Valandinis.lt — matome, kad {mieste} turime objektą, kuriame reikalingas {specialybė}. Ar šiuo metu dirbate ar atviri naujam objektui? 🙂"

Constraints:
- Exactly one greeting. If the model tries to add another greeting or identity later, remove it.
- No bot/AI mention. No extra details. Output only the final SMS text.
"""

def generate_opener_lt(name: str, city: str, specialty: str) -> str:
    city = (city or "").strip()
    specialty = (specialty or "").strip()

    if not city or not specialty:
        return _final_sms("Sveiki! Čia Valandinis.lt. Ar šiuo metu dirbate ar atviri naujam objektui?")

    user_payload = json.dumps({"vardas": "", "miestas": city, "specialybė": specialty}, ensure_ascii=False)

    try:
        r = client.chat.completions.create(
            model=MODEL,
            temperature=0.2,
            max_tokens=120,
            messages=[{"role": "system", "content": OPENER_SYS},
                      {"role": "user", "content": user_payload}]
        )
        text = (r.choices[0].message.content or "").strip()
        text = re.sub(r"^(Sveiki,?\s+){2,}", "Sveiki! ", text)
        text = text.replace("Sveiki, Sveiki!", "Sveiki!").replace("Sveiki,  ", "Sveiki! ")
        if text.count("?") > 1:
            text = text.split("?")[0] + "?"
        return _final_sms(text)
    except Exception:
        fb = f"Sveiki! Čia Valandinis.lt — matome, kad {city} turime objektą, kuriame reikalingas {specialty}. Ar šiuo metu dirbate ar atviri naujam objektui?"
        return _final_sms(fb)

def project_opener(name: str, city: str, specialty: str) -> str:
    return generate_opener_lt(name, city, specialty)

# ==== Deterministic thread-level outcome (admin) ====
def _is_yes(txt: str) -> bool:
    t = (txt or "").lower()
    return any(w in t for w in ["taip","domina","įdomu","tinka","gerai","ok","esu atviras","atvira"]) and not _is_no(t)

def _is_no(txt: str) -> bool:
    t = (txt or "").lower().strip()
    no_words = ["nedomina","ne, ačiū","ne domina","nebus","nenoriu"]
    return t == "ne" or any(w in t for w in no_words)

def _is_maybe_future(txt: str) -> bool:
    t = (txt or "").lower()
    return any(x in t for x in ["gal ateity","gal vėliau","kai bus laisviau","vėliau gal","ateityje"])

# REWIRED: outcome is derived from the actual close that was sent
def classify_thread_outcome(history: List[Dict[str,str]]) -> Dict[str, object]:
    slots = _inferred_slots(history)
    years = slots.get("years")
    availability = slots.get("availability")

    ctype = _last_close_type(history)
    if ctype == "human":
        return {"outcome": "interested", "future_maybe": False, "years": years, "availability": availability}
    if ctype == "future":
        return {"outcome": "interested", "future_maybe": True, "years": years, "availability": availability}
    if ctype in ("dnc", "troll"):
        return {"outcome": "not_interested", "future_maybe": False, "years": years, "availability": availability}

    # Fallback if no recognized close (legacy behavior)
    last_user = ""
    for m in reversed(history):
        if m.get("role") == "user":
            last_user = (m.get("content") or "").strip()
            break
    future_maybe = _is_maybe_future(last_user)
    if _is_no(last_user):
        return {"outcome": "not_interested", "future_maybe": future_maybe, "years": years, "availability": availability}
    actionable = _looks_like_question(last_user) or _is_yes(last_user) or (years is not None) or (availability is not None)
    if actionable:
        return {"outcome": "interested", "future_maybe": future_maybe, "years": years, "availability": availability}
    return {"outcome": "undecided", "future_maybe": future_maybe, "years": years, "availability": availability}

from typing import Optional as _Optional
def summarize_thread_kpis(msisdn: str, last_user_text: _Optional[str] = None) -> dict:
    history = _thread_history(msisdn, limit=50)
    out = classify_thread_outcome(history)

    interested = (
        "yes" if out.get("outcome") == "interested"
        else "no" if out.get("outcome") == "not_interested"
        else "unsure"
    )
    years = out.get("years") if isinstance(out.get("years"), int) else None

    # Derive future_interest from close type first
    future_interest = "unknown"
    ctype = _last_close_type(history)
    if ctype == "future":
        future_interest = "yes"
    elif ctype in ("dnc", "troll"):
        future_interest = "no"

    # If still unknown, optionally use analyzer hint
    if future_interest == "unknown" and last_user_text:
        try:
            plan = analyze(last_user_text, history)
            fi = (plan or {}).get("future_interest")
            if fi in ("yes","no","unsure","unknown"):
                future_interest = fi
        except Exception:
            pass
    if future_interest == "unknown":
        future_interest = "yes" if out.get("future_maybe") else (
            "no" if out.get("outcome") == "not_interested" else "unknown"
        )

    return {"interested": interested, "future_interest": future_interest, "years": years}
