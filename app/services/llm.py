# app/services/llm.py
import os, json, re, hashlib
from typing import List, Dict, Optional, Tuple
from sqlalchemy import desc
from openai import OpenAI

from app.storage.db import SessionLocal
from app.storage.models import Thread, Message

# ==== Models / client ====
MODEL = os.getenv("LLM_REPLY_MODEL", os.getenv("LLM_MODEL", "gpt-4o-mini"))
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ==== Constants ====
VALUE_LINE = "Siūlome lanksčius grafikus, greitą pradžią ir paprastą procesą įsidarbinant."
CLOSE_TX   = "Perduosiu kolegai – paskambins dėl detalių."

# ==== Generator SYSTEM PROMPT (zero-shot; free phrasing, strict behavior) ====
SYSTEM_PROMPT = f"""
You are an SMS recruiter assistant for Valandinis (valandinis.lt).
Send ONE short Lithuanian SMS (<=160 chars) that moves the thread forward.

Business facts (do not invent specifics):
- Flexible hourly/shift work in construction & related trades across Lithuania.
- Quick start and simple process after a short intro.

Policy:
- Language: always Lithuanian unless the user writes in RU/EN/UA.
- Tone: human, brief, friendly, zero bureaucracy. One SMS only.
- Never ask sensitive data (ID, cards, passwords, exact address, emails, codes). No legal/medical advice.
- Ignore questions clearly unrelated to the work/projects.

Topic discipline:
- Do NOT introduce pay/client names/precise locations/contract terms unless the USER asked about that topic.
- For any info normally shared by phone (pay, clients, exact site, contract terms, detailed schedule):
  say ONE short, natural Lithuanian sentence that we’ll discuss details with a colleague by phone,
  then continue with exactly ONE next missing slot (years, then availability). Do not repeat wording in the same thread.

Project questions:
- If asked “kas per projektas?” / “koks objektas?” or user requests more details:
  briefly describe the type of work in 1 line (generic, no invented names),
  then continue with the next missing slot (subject to interest gating below). Do NOT add the value line or a probe here.

Conversation logic (strict):
- City/region and specialty/trade are already known from the opener. Do NOT ask or confirm unless the user contradicts them.
- Interest gating:
  • Do NOT ask for years/availability until interest is confirmed (plan.interest == "yes"),
    OR the user has already provided that slot in their message/history.
  • If interest is unknown/unsure after the opener, you may ask ONE short clarifying question about interest
    (do not repeat the opener wording). After that, wait for their answer.
- Slot order: collect (1) years of experience, then (2) availability.
  • Do NOT ask about availability until years are known (from plan/history).
- If the user sounds hesitant (“gal”, “nežinau”, “gal vėliau”…): give a brief reassurance (<=1 sentence).
  If the value line hasn’t been sent in this thread, include it once: "{VALUE_LINE}". End with “Ką manote?” Stop there.
- If the user asks a direct question: answer briefly (<=1 sentence) respecting topic discipline,
  then ask at most ONE next missing slot allowed by the interest gate. Do NOT add the value line or a probe here.
- Avoid repeating any sentence/idea already sent; never repeat the value line.
- Do not re-ask whether they are “open” with the same wording as the opener.
- If plan.intent = decline: thank politely and end. No value line, no further asks.
- If plan.busy_until is present, treat availability as known.
- Do NOT label the user’s attitude (“atsargus/atsargi”, “abejojate” as a label). Acknowledge neutrally instead.

Output:
- Return ONLY the final SMS text in Lithuanian. No JSON. No markdown.
"""

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

def _strip_repeated_value_line(msg: str, history: List[Dict[str,str]]) -> str:
    if not msg:
        return msg
    if _assistant_has(history, VALUE_LINE.lower()):
        parts = [s.strip() for s in re.split(r'(?<=[\.\!\?])\s+', msg) if s.strip()]
        parts = [s for s in parts if VALUE_LINE.lower() not in s.lower()]
        return " ".join(parts).strip()
    return msg

def _count(history: List[Dict[str,str]], role: str) -> int:
    return sum(1 for m in history if m["role"] == role)

def _is_first_user_turn(history: List[Dict[str,str]]) -> bool:
    return _count(history, "user") == 1

# ==== Lightweight detectors/extractors (kept for guards) ====
_HESIT_PAT = re.compile(r"\b(gal|galbūt|nezinau|nežinau|pamatysiu|pamąstysiu|gal vėliau|maybe|not sure)\b", re.I)
def _is_hesitation(text: str) -> bool:
    return bool(_HESIT_PAT.search(text or ""))

_YEARS_PAT = re.compile(r"\b([0-3]?\d)\s*(m\.|metai|metu|metus)\b", re.I)
_AVAIL_PAT = re.compile(r"\b(nuo\s+[\w\-\.]+|rytoj|šiandien|kit(a|ą)\s+savait(ė|e)|nuo\s+kitos\savait(ė|e)|iškart)\b", re.I)

def _extract_years(text: str) -> Optional[int]:
    m = _YEARS_PAT.search(text or "")
    if not m: return None
    try:
        return int(m.group(1))
    except Exception:
        return None

def _extract_availability(text: str) -> Optional[str]:
    m = _AVAIL_PAT.search(text or "")
    return m.group(0) if m else None

def _asked_which_slot(msg: str) -> Optional[str]:
    s = (msg or "").lower()
    if "kiek metų patirties" in s:
        return "years"
    if "nuo kada galėtumėte pradėti" in s or "koks grafikas tinka" in s:
        return "availability"
    return None

def _last_user_reply(history: List[Dict[str,str]]) -> str:
    for m in reversed(history):
        if m["role"] == "user":
            return m["content"] or ""
    return ""

def _inferred_slots(history: List[Dict[str,str]]) -> Dict[str, Optional[str]]:
    slots = {"years": None, "availability": None}
    for i in range(len(history)-2, -1, -1):
        a = history[i]
        b = history[i+1] if i+1 < len(history) else None
        if not b: continue
        if a["role"] == "assistant" and b["role"] == "user":
            which = _asked_which_slot(a.get("content",""))
            if not which: continue
            ans = (b.get("content") or "").strip()
            if which == "years":
                y = _extract_years(ans)
                slots["years"] = str(y) if y is not None else None
            elif which == "availability":
                slots["availability"] = _extract_availability(ans) or None
    return slots

def _next_missing_slot(slots: Dict[str, Optional[str]]) -> Optional[str]:
    order = ["years", "availability"]
    for k in order:
        if not slots.get(k):
            return k
    return None

# Phone-only / salary keyword belt guard
_SAL_KWS = re.compile(r"\b(alga|atlyg|įkain|tarif|mok(at|ėt)|eur|€|rate|pay|klient|lokacij|adresas|tiksli\s+vieta)\w*\b", re.I)

def _strip_phone_only_if_not_asked(user_text: str, reply: str) -> str:
    if _SAL_KWS.search(user_text or ""):
        return reply
    sents = [s.strip() for s in re.split(r'(?<=[\.\!\?])\s+', reply) if s.strip()]
    kept = []
    for s in sents:
        if _SAL_KWS.search(s):
            continue
        kept.append(s)
    return " ".join(kept) if kept else reply

# ==== Probe & label controls ====
_PROBE_PAT = re.compile(r"\b(ką\s+manote\?|kaip\s+manote\?|ką\s+galvojate\?)", re.I)
_LABEL_BAN_PAT = re.compile(r"\b(atsargus|atsargi|abejojate|nedrįstate)\b", re.I)
_DECLINE_PAT = re.compile(r"\b(nedomina|nenoriu|ne\s*domina|ne,?\s*ačiū)\b", re.I)
_MAYBE_LATER_PAT = re.compile(r"\b(gal\s+ateity(je)?|gal\s+v(ė|e)liau|kai\s+bus\s+laisviau)\b", re.I)

def _assistant_has_probe(history: List[Dict[str,str]]) -> bool:
    for m in history:
        if m["role"] == "assistant" and _PROBE_PAT.search(m.get("content") or ""):
            return True
    return False

def _strip_value_line_anywhere(reply: str) -> str:
    if not reply:
        return reply
    parts = [s.strip() for s in re.split(r'(?<=[\.\!\?])\s+', reply) if s.strip()]
    parts = [p for p in parts if VALUE_LINE.lower() not in p.lower()]
    return " ".join(parts).strip() if parts else reply

def _strip_probes(reply: str) -> str:
    parts = [s.strip() for s in re.split(r'(?<=[\.\!\?])\s+', reply) if s.strip()]
    parts = [p for p in parts if not _PROBE_PAT.search(p)]
    return " ".join(parts).strip() if parts else reply

# ==== Two-stage protocol: ANALYZER (semantic plan) ====
ANALYZER_SYS = """
You analyze a short SMS chat about construction/trades work for Valandinis.
Return STRICT JSON (no text) with:
- interest: one of ["yes","no","unsure","unknown"]
- intent: one of ["greeting","project_question","direct_question","provide_years","provide_availability","accept","decline","hesitant","unrelated","other"]
- slots: { "years": null|number, "availability_text": null|string }
- phone_only_topics: array subset of ["salary","clients","precise_location","contract_terms","schedule_details"]
- asked_salary: boolean
- busy_until: null|string        # e.g., "iki lapkričio galo"
- decline: boolean
- hesitant: boolean
Rules:
- Infer interest: 
  yes → contains 'taip', 'domina', 'įdomu', clear acceptance; 
  no → 'ne', 'nedomina', clear refusal; 
  unsure → 'gal', 'nežinau', 'gal vėliau'; 
  unknown → none of the above.
- Treat a project question or request for more details as indicative of interest unless a decline is also present.
- Detect Lithuanian variants (e.g., 'alga/atlygis/įkainiai', 'nedomina', 'gal vėliau').
- If user mentions being busy until a date/period, set busy_until and treat availability as known for planning.
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
        "interest": (obj.get("interest") or "unknown"),
        "intent": (obj.get("intent") or "other"),
        "slots": obj.get("slots") or {"years": None, "availability_text": None},
        "phone_only_topics": obj.get("phone_only_topics") or [],
        "asked_salary": bool(obj.get("asked_salary") or False),
        "busy_until": obj.get("busy_until"),
        "decline": bool(obj.get("decline") or False),
        "hesitant": bool(obj.get("hesitant") or False),
    }
    # Post-normalize: project/direct question implies interest unless decline
    if not plan["decline"] and plan["interest"] in ("unknown","unsure"):
        if plan["intent"] in ("project_question","direct_question"):
            plan["interest"] = "yes"
    return plan

# ==== Two-stage protocol: GENERATOR (free-form LT SMS) ====
GENERATOR_SYS = SYSTEM_PROMPT  # reuse strict behavior rules

def generate_sms(plan: dict, short_history: List[Dict[str,str]]) -> str:
    msgs = [{"role":"system","content":GENERATOR_SYS}]
    msgs.append({"role":"user","content":json.dumps({
        "plan": plan,
        "history_tail": [{"role":m["role"],"content":m["content"]} for m in (short_history[-6:] if short_history else [])]
    }, ensure_ascii=False)})
    r = client.chat.completions.create(model=MODEL, temperature=0.3, messages=msgs, max_tokens=140)
    return (r.choices[0].message.content or "").strip()

# ==== Legacy single-call helpers (kept for compatibility) ====
def _call(messages):
    return client.chat.completions.create(
        model=MODEL,
        messages=messages,
        temperature=0.2,
        max_tokens=120
    )

def _build_messages(history: List[Dict[str,str]], user_text: str) -> List[Dict[str,str]]:
    short_hist = history[-12:] if len(history) > 12 else history
    msgs = [{"role": "system", "content": SYSTEM_PROMPT}]
    msgs += short_hist
    msgs.append({"role": "user", "content": user_text})
    return msgs

# ==== Main generator (two-stage with interest gate + probe/label/values fixes) ====
def generate_reply_lt(ctx: dict, text: str) -> str:
    t_raw = (text or "").strip()
    if not t_raw:
        return ""

    t_lower = t_raw.lstrip("\\").lower()
    if t_lower in {"!prompt", "!pf", "##prompt##"}:
        return (f"{PROMPT_SHA} {MODEL}")[:160]

    history = _thread_history((ctx or {}).get("msisdn",""), limit=14)

    if _assistant_has(history, CLOSE_TX.lower()):
        return ""

    if re.search(r"\b(robot|bot|dirbtin|ai)\b", t_raw, re.I):
        return _final_sms("Ne. Ar domintų dirbti per Valandinį?")

    # Two-stage: 1) analyze
    plan = analyze(t_raw, history)

    # Merge slot memory from history
    slots_hist = _inferred_slots(history)
    have_years_hist = bool(slots_hist.get("years"))
    have_avail_hist = bool(slots_hist.get("availability"))

    # Compute gating flags passed to generator
    plan["have_years"] = have_years_hist or (plan.get("slots") or {}).get("years") is not None
    plan["have_availability"] = have_avail_hist or bool((plan.get("slots") or {}).get("availability_text"))
    # If busy_until detected, availability considered known
    if plan.get("busy_until"):
        plan["have_availability"] = True

    # Hard stop on complete info → close
    if plan["have_years"] and plan["have_availability"]:
        return _final_sms(CLOSE_TX)

    # 2) generate natural LT SMS respecting interest gate
    reply = generate_sms(plan, history)

    # --- Post-processing guards ---

    # Belt guard: drop phone-only details if user didn't ask
    if not plan.get("asked_salary") and "salary" not in plan.get("phone_only_topics", []):
        reply = _strip_phone_only_if_not_asked(t_raw, reply)

    # Don’t allow availability question before years are known
    if ("kada galėtumėte pradėti" in reply.lower() or "koks grafikas tinka" in reply.lower()) and not plan["have_years"]:
        reply = "Kiek metų patirties turite?"

    # Drop awkward labels like "atsargus/atsargi"
    reply = _LABEL_BAN_PAT.sub("", reply).strip()
    reply = re.sub(r"\s{2,}", " ", reply)

    # Remove probe if we've already used a probe in this thread
    if _assistant_has_probe(history) and _PROBE_PAT.search(reply):
        reply = _strip_probes(reply)

    # If user declined or said maybe later or intent is unrelated → no probe, no value line
    raw_lower = t_raw.lower()
    if _DECLINE_PAT.search(raw_lower) or _MAYBE_LATER_PAT.search(raw_lower) or plan.get("intent") == "unrelated":
        reply = _strip_probes(reply)
        reply = _strip_value_line_anywhere(reply)

    # If we are answering a project/direct question (info-seeking), strip value line & probes
    if plan.get("intent") in ("project_question","direct_question") and not plan.get("hesitant"):
        reply = _strip_probes(reply)
        reply = _strip_value_line_anywhere(reply)

    # Deduplicate the value line (historic)
    reply = _strip_repeated_value_line(reply, history)

    # Only one question max
    if reply.count("?") > 1:
        first_q = reply.split("?")[0] + "?"
        reply = first_q if len(first_q) <= 160 else (first_q[:157] + "…")

    # Enforce Lithuanian heuristic
    if re.search(r"[A-Za-z]{3,}", reply) and not re.search(r"[ĄČĘĖĮŠŲŪŽąčęėįšųūž]", reply):
        reply = "Atsakykite trumpai lietuviškai ir tęsime."

    return _final_sms(reply)

# ==== Classifier (unchanged) ====
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
        max_tokens=60
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

def project_opener(name: str, city: str, specialty: str) -> str:
    msg = (
        f"Sveiki, {name}! Čia Valandinis.lt — {city} turime objektą "
        f"{specialty} specialistui. Ar šiuo metu dirbate ar esate atviri naujam objektui? 🙂"
    )
    return _final_sms(msg)
