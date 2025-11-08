# app/routers/chat.py
from uuid import uuid4
from datetime import datetime, timezone
import os, re

from sqlalchemy import desc
from fastapi import APIRouter, Request, Form, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session

from app.storage.db import SessionLocal
from app.storage.models import Thread, Message
from app.services.llm import (
    generate_reply_lt,
    project_opener,
    analyze,
    PROMPT_SHA,
    MODEL as LLM_MODEL,
    summarize_thread_kpis,
    classify_thread_outcome,
)
from app.services.storage import load_matches
from app.services.gsheets import ensure_headers, append_rows
from app.services.leadlog import write_lead_row

router = APIRouter()

# ---------- close detection (matches llm.py) ----------
_POSTCLOSE_ACK_RE = re.compile(r"\b(ačiū|aciu|dėkui|dekui|ir\s+jums|geros\s+dienos|ok|okey|okei|thanks|👍)\b", re.I)

def _norm(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[–—−-]", "-", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"[\.!\s]+$", "", s)
    s = s.replace("ė","e").replace("ą","a").replace("č","c").replace("ę","e").replace("į","i").replace("š","s").replace("ų","u").replace("ū","u").replace("ž","z")
    return s

_CLOSE_PATTERNS = [
    re.compile(r"\bperduos(iu|iu)\s+koleg\w+.*\bpaskambins\b"),
    re.compile(r"\baci(u|u)\s+uz\s+(jusu\s+)?laika.*perduos(iu|iu)\s+koleg\w+"),
    re.compile(r"\bsupratau\s*-\s*daugiau\s+nerasysime\b"),
    re.compile(r"\buzsirasi(si(u|u)|au)\s+ateiciai\b"),
    re.compile(r"\bpalikime\s+cia\b"),
]

def _is_close_text(text: str) -> bool:
    last = _norm(text)
    if not last:
        return False
    if ("perduosiu koleg" in last and "paskambins" in last) or \
       ("daugiau nerasysime" in last) or \
       ("uzsirasi" in last and "ateiciai" in last) or \
       ("palikime cia" in last):
        return True
    return any(rx.search(last) for rx in _CLOSE_PATTERNS)

def _assistant_closed_history(db: Session, thread_id: int) -> bool:
    last_out = (
        db.query(Message)
          .filter(Message.thread_id == thread_id, Message.dir == "out")
          .order_by(desc(Message.ts))
          .first()
    )
    return bool(last_out and _is_close_text(last_out.body or ""))

# ---------- gsheets helpers ----------
def _append_outreach_event(
    *,
    event: str,
    thread_id: int,
    phone: str,
    text: str = "",
    userref: str = "admin-chat",
    city: str = "",
    specialty: str = "",
    provider_id: str = "",
    latency_s: int | None = None,
):
    try:
        ensure_headers(
            "OutreachEvents",
            [
                "ts_iso",
                "event",
                "thread_id",
                "phone",
                "city",
                "specialty",
                "text",
                "userref",
                "provider_id",
                "latency_s",
            ],
        )
        append_rows(
            "OutreachEvents",
            [
                [
                    datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    event,
                    thread_id,
                    phone,
                    city,
                    specialty,
                    text,
                    userref,
                    provider_id,
                    (latency_s if latency_s is not None else ""),
                ]
            ],
        )
    except Exception:
        pass


def _log_seed_to_sheets(thread_id: int, phone: str, city: str, spec: str, opener: str, created_by: str):
    # Threads + Messages tabs
    try:
        ensure_headers("Threads", ["ts_iso", "thread_id", "phone", "opener_city", "opener_specialty", "created_by"])
        ensure_headers("Messages", ["ts_iso", "thread_id", "phone", "dir", "text", "model", "prompt_sha"])
        ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
        append_rows("Threads", [[ts, thread_id, phone, city, spec, created_by]])
        append_rows("Messages", [[ts, thread_id, phone, "out", opener, LLM_MODEL, PROMPT_SHA]])
    except Exception:
        pass
    # One-sheet Leads (seeded, not actually sent)
    try:
        llm = analyze(opener, [])
        write_lead_row(
            name="",
            phone=phone,
            city=city or "",
            specialty=spec or "",
            msg_dir="out",
            msg_text=opener,
            sent_ok=False,
            llm=llm,
            note="admin_chat_seed",
            model=LLM_MODEL,
            prompt_sha=PROMPT_SHA,
            thread_id=thread_id,
        )
    except Exception:
        pass


def _log_message_to_sheets(thread_id: int, phone: str, direction: str, text: str):
    # Messages tab
    try:
        ensure_headers("Messages", ["ts_iso", "thread_id", "phone", "dir", "text", "model", "prompt_sha"])
        ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
        model = LLM_MODEL if direction == "out" else ""
        psha = PROMPT_SHA if direction == "out" else ""
        append_rows("Messages", [[ts, thread_id, phone, direction, text, model, psha]])
    except Exception:
        pass
    # One-sheet Leads
    try:
        if direction == "in":
            llm = analyze(text, [])
            write_lead_row(
                name="",
                phone=phone,
                city="",
                specialty="",
                msg_dir="in",
                msg_text=text,
                sent_ok=None,
                llm=llm,
                note="admin_chat_in",
                thread_id=thread_id,
            )
        else:
            # Simulator reply — not actually sent via SMS
            write_lead_row(
                name="",
                phone=phone,
                city="",
                specialty="",
                msg_dir="out",
                msg_text=text,
                sent_ok=False,
                llm=None,
                note="admin_chat_reply",
                model=LLM_MODEL,
                prompt_sha=PROMPT_SHA,
                thread_id=thread_id,
            )
    except Exception:
        pass


# ---------- business helpers ----------
def _seed_from_matches(phone: str) -> dict:
    """Best-guess name/city/specialty for this phone from saved matches."""
    try:
        rows = load_matches()
    except Exception:
        rows = []
    phone = (phone or "").strip()
    for r in rows:
        if str(r.get("Tel. nr", "")).strip() == phone:
            return {
                "name": (r.get("Vardas") or r.get("vardas pavardė") or r.get("Name") or "Sveiki"),
                "city": (r.get("Miestas") or "Lietuvoje"),
                "specialty": (r.get("Specialybė") or "statybos"),
            }
    return {"name": "Sveiki", "city": "Lietuvoje", "specialty": "statybos"}


def _get_or_create_thread(db: Session, phone: str) -> Thread:
    # Reuse latest thread regardless of status; only create if none exists.
    t = (
        db.query(Thread)
          .filter(Thread.phone == phone)
          .order_by(Thread.id.desc())
          .first()
    )
    if t:
        return t
    t = Thread(phone=phone, status="open")
    db.add(t)
    db.commit()
    db.refresh(t)
    return t


def _ensure_opener_if_empty(db: Session, t: Thread, seed: dict, created_by: str):
    # Only seed if thread is open AND has no messages.
    if getattr(t, "status", "open") != "open":
        return
    has_msgs = db.query(Message).filter(Message.thread_id == t.id).first() is not None
    if has_msgs:
        return
    opener = project_opener(
        name=seed.get("name") or "Sveiki",
        city=seed.get("city") or "Lietuvoje",
        specialty=seed.get("specialty") or "statybos",
    )
    db.add(Message(thread_id=t.id, dir="out", body=opener))
    db.commit()
    _log_seed_to_sheets(t.id, t.phone, seed.get("city") or "", seed.get("specialty") or "", opener, created_by)



def _finalize_thread_once(db: Session, *, thread: Thread, phone: str, last_user_text: str = ""):
    try:
        db.refresh(thread)
    except Exception:
        pass

    if getattr(thread, "status", "open") == "closed":
        return

    # 1) Compute & write KPIs WHILE OPEN
    try:
        ensure_headers("ThreadKPIs", ["ts_iso", "thread_id", "phone", "interested", "future_interest", "years"])
        k = summarize_thread_kpis(phone, last_user_text=last_user_text or "")
        append_rows(
            "ThreadKPIs",
            [[
                datetime.now(timezone.utc).isoformat(timespec="seconds"),
                thread.id, phone, k["interested"], k["future_interest"], k["years"],
            ]],
        )
    except Exception:
        pass

    # 2) Now close once
    try:
        thread.status = "closed"
        db.add(thread)
        db.commit()
    except Exception:
        pass



# ---------- routes ----------
@router.get("/admin/chat", response_class=HTMLResponse)
def chat_page(request: Request, phone: str | None = Query(default=None)):
    db = SessionLocal()
    try:
        phone = phone or "test-ui"
        t = _get_or_create_thread(db, phone)
        _ensure_opener_if_empty(db, t, _seed_from_matches(phone), created_by="admin_chat_open")

        msgs = db.query(Message).filter(Message.thread_id == t.id).order_by(Message.ts.asc()).all()

        html = ['<link rel="stylesheet" href="/static/styles.css">']
        html += [
            "<h1>SMS Admin</h1><h2>Testinė pokalbių dėžutė</h2>",
            f"<p>Telefonas: <b>{t.phone}</b>. Čia tik lokali simuliacija (SMS nesiunčiamos).</p>",
            '<div style="margin:8px 0;">',
            '<form method="post" action="/chat/new" style="display:inline;margin-right:8px">',
            '<button type="submit" class="btn">New chat</button>',
            "</form>",
            '<form method="post" action="/chat/reset" style="display:inline">',
            f'<input type="hidden" name="phone" value="{t.phone}"/>',
            '<button type="submit" class="btn btn-secondary">Reset this chat</button>',
            "</form>",
            "</div>",
            '<div class="chatbox">',
        ]
        for m in msgs:
            if m.dir == "in":
                html.append(f'<div class="bubble you"><b>Jūs:</b> {m.body}</div>')
            else:
                html.append(f'<div class="bubble bot"><b>Asistentas:</b> {m.body}</div>')
        html.append("</div>")
        html.append(
            f"""
        <form method="post" action="/chat/send" style="display:flex;gap:8px;margin-top:8px">
          <input type="hidden" name="phone" value="{t.phone}" />
          <input name="text" placeholder="Įrašykite žinutę..." style="flex:1;padding:10px;border-radius:8px;border:1px solid #ccc" />
          <button type="submit" class="btn">Siųsti</button>
        </form>
        """
        )
        return HTMLResponse("\n".join(html))
    finally:
        db.close()


@router.post("/chat/send")
def chat_send(phone: str = Form(...), text: str = Form(...)):
    db = SessionLocal()
    try:
        t = _get_or_create_thread(db, phone)
        user = (text or "").strip()

        if user:
            # store inbound
            db.add(Message(thread_id=t.id, dir="in", body=user))
            db.commit()
            _log_message_to_sheets(t.id, t.phone, "in", user)

            # DNC keywords support in simulator
            raw = os.getenv("DNC_PHRASES", "")
            dnc_list = [p.strip().lower() for p in raw.split(",") if p.strip()]
            if dnc_list:
                norm = user.lower().replace(",", " ").replace(".", " ")
                if any(p in norm for p in dnc_list):
                    _append_outreach_event(event="reply_received", thread_id=t.id, phone=t.phone, text=user)
                    _finalize_thread_once(db, thread=t, phone=t.phone, last_user_text=user)
                    return RedirectResponse(url=f"/admin/chat?phone={t.phone}", status_code=303)

            # latency bookkeeping
            last_out = (
                db.query(Message)
                .filter(Message.thread_id == t.id, Message.dir == "out")
                .order_by(desc(Message.ts))
                .first()
            )
            latency_s = None
            if last_out:
                latency_s = int((datetime.utcnow() - last_out.ts).total_seconds())
            _append_outreach_event(
                event="reply_received",
                thread_id=t.id,
                phone=t.phone,
                text=user,
                latency_s=latency_s,
            )

            # If assistant had already closed, and user sends a polite ack → finalize & stop
            if _assistant_closed_history(db, t.id) and _POSTCLOSE_ACK_RE.search(user.lower()):
                _finalize_thread_once(db, thread=t, phone=t.phone, last_user_text=user)
                return RedirectResponse(url=f"/admin/chat?phone={t.phone}", status_code=303)

            # Terminal outcome? If decline or already-closed history → finalize once and stop
            msgs = db.query(Message).filter(Message.thread_id == t.id).order_by(Message.ts.asc()).all()
            history = [{"role": ("assistant" if m.dir == "out" else "user"), "content": m.body or ""} for m in msgs]
            outc = classify_thread_outcome(history)
            if outc.get("outcome") == "not_interested" or _assistant_closed_history(db, t.id):
                _finalize_thread_once(db, thread=t, phone=t.phone, last_user_text=user)
                return RedirectResponse(url=f"/admin/chat?phone={t.phone}", status_code=303)

        # LLM reply (simulated)
        reply = generate_reply_lt({"msisdn": t.phone}, user) or "Atsiprašau, įvyko klaida. Bandykite dar kartą."
        reply = reply.strip()
        if reply:
            db.add(Message(thread_id=t.id, dir="out", body=reply))
            db.commit()
            _log_message_to_sheets(t.id, t.phone, "out", reply)

            _append_outreach_event(
                event="llm_reply_sent",
                thread_id=t.id,
                phone=t.phone,
                text=reply,
                userref="admin-chat",
                provider_id="",
            )

            # NEW: finalize immediately if assistant reply is a closing line
            try:
                if _is_close_text(reply):
                    _finalize_thread_once(db, thread=t, phone=t.phone, last_user_text=user)
            except Exception:
                pass

        return RedirectResponse(url=f"/admin/chat?phone={t.phone}", status_code=303)
    finally:
        db.close()


@router.post("/chat/new")
def chat_new():
    db = SessionLocal()
    try:
        phone = f"test-ui-{uuid4().hex[:6]}"
        t = _get_or_create_thread(db, phone)
        return RedirectResponse(url=f"/admin/chat?phone={t.phone}", status_code=303)
    finally:
        db.close()


@router.post("/chat/reset")
def chat_reset(phone: str = Form(...)):
    db = SessionLocal()
    try:
        t = _get_or_create_thread(db, phone)

        # finalize once using last inbound text if any (write single KPI row)
        last_in = (
            db.query(Message)
            .filter(Message.thread_id == t.id, Message.dir == "in")
            .order_by(desc(Message.ts))
            .first()
        )
        _finalize_thread_once(db, thread=t, phone=t.phone, last_user_text=(last_in.body if last_in else ""))

        # now wipe and re-open (fresh simulator thread)
        db.query(Message).filter(Message.thread_id == t.id).delete()
        t.status = "open"
        db.commit()

        _ensure_opener_if_empty(db, t, _seed_from_matches(phone), created_by="admin_chat_reset")
        return RedirectResponse(url=f"/admin/chat?phone={t.phone}", status_code=303)
    finally:
        db.close()
