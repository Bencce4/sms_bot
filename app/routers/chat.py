# app/routers/chat.py
from uuid import uuid4

from fastapi import APIRouter, Request, Form, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session

from app.storage.db import SessionLocal
from app.storage.models import Thread, Message
from app.services.llm import generate_reply_lt

router = APIRouter()

def _project_opener(name: str, city: str, specialty: str) -> str:
    return (
        f"Sveiki, {name}! Čia Valandinis.lt — {city} turime objektą, "
        f"kuriam ieškome {specialty}. Ar šiuo metu dirbate ar esate atviri naujam objektui? 🙂"
    )

def _create_thread(db: Session, phone: str, name: str, city: str, specialty: str) -> Thread:
    t = Thread(phone=phone, status="open")
    db.add(t); db.commit(); db.refresh(t)
    opener = _project_opener(name=name, city=city, specialty=specialty)
    db.add(Message(thread_id=t.id, dir="out", body=opener))
    db.commit()
    return t

def _get_thread_by_phone(db: Session, phone: str) -> Thread | None:
    return db.query(Thread).filter_by(phone=phone, status="open").first()

def _get_or_create_test_thread(db: Session, phone: str | None) -> Thread:
    if not phone:
        phone = "test-ui"
    t = _get_thread_by_phone(db, phone)
    if t:
        return t
    # Default demo identity
    return _create_thread(db, phone=phone, name="Vardenis", city="Vilniuje", specialty="elektriko")

@router.get("/admin/chat", response_class=HTMLResponse)
def chat_page(request: Request, phone: str | None = Query(default=None)):
    db = SessionLocal()
    try:
        t = _get_or_create_test_thread(db, phone)
        msgs = (
            db.query(Message)
              .filter(Message.thread_id == t.id)
              .order_by(Message.ts.asc())
              .all()
        )

        html = ['<link rel="stylesheet" href="/static/styles.css">']
        html += [
            "<h1>SMS Admin</h1><h2>Testinė pokalbių dėžutė</h2>",
            "<p>Dialogai nesiunčiami per SMS – tai tik bandymų aplinka.</p>",
            '<div style="margin:8px 0;">',
            '<form method="post" action="/chat/new" style="display:inline;margin-right:8px">',
            '<button type="submit" class="btn">New chat</button>',
            '</form>',
            '<form method="post" action="/chat/reset" style="display:inline">',
            f'<input type="hidden" name="phone" value="{t.phone}"/>',
            '<button type="submit" class="btn btn-secondary">Reset this chat</button>',
            '</form>',
            "</div>",
            '<div class="chatbox">'
        ]
        for m in msgs:
            if m.dir == "in":
                html.append(f'<div class="bubble you"><b>Jūs:</b> {m.body}</div>')
            else:
                html.append(f'<div class="bubble bot"><b>Asistentas:</b> {m.body}</div>')
        html.append("</div>")
        html.append(f"""
        <form method="post" action="/chat/send" style="display:flex;gap:8px;margin-top:8px">
          <input type="hidden" name="phone" value="{t.phone}" />
          <input name="text" placeholder="Įrašykite žinutę..." style="flex:1;padding:10px;border-radius:8px;border:1px solid #ccc" />
          <button type="submit" class="btn">Siųsti</button>
        </form>
        """)
        return HTMLResponse("\n".join(html))
    finally:
        db.close()

@router.post("/chat/send")
def chat_send(phone: str = Form(...), text: str = Form(...)):
    db = SessionLocal()
    try:
        t = _get_or_create_test_thread(db, phone)
        user = (text or "").strip()
        if user:
            db.add(Message(thread_id=t.id, dir="in", body=user)); db.commit()
        reply = generate_reply_lt({"msisdn": t.phone}, user) or "Atsiprašau, įvyko klaida. Bandykite dar kartą."
        if reply.strip():
            db.add(Message(thread_id=t.id, dir="out", body=reply.strip())); db.commit()
        return RedirectResponse(url=f"/admin/chat?phone={t.phone}", status_code=303)
    finally:
        db.close()

@router.post("/chat/new")
def chat_new():
    db = SessionLocal()
    try:
        phone = f"test-ui-{uuid4().hex[:6]}"
        _create_thread(db, phone=phone, name="Vardenis", city="Vilniuje", specialty="elektriko")
        return RedirectResponse(url=f"/admin/chat?phone={phone}", status_code=303)
    finally:
        db.close()

@router.post("/chat/reset")
def chat_reset(phone: str = Form(...)):
    db = SessionLocal()
    try:
        t = _get_or_create_test_thread(db, phone)
        # wipe messages and re-insert opener
        db.query(Message).filter(Message.thread_id == t.id).delete()
        db.commit()
        opener = _project_opener(name="Vardenis", city="Vilniuje", specialty="elektriko")
        db.add(Message(thread_id=t.id, dir="out", body=opener))
        db.commit()
        return RedirectResponse(url=f"/admin/chat?phone={t.phone}", status_code=303)
    finally:
        db.close()
