import os
from fastapi import FastAPI, Request, HTTPException
from datetime import datetime, time as dtime
from app.storage.db import Base, engine, SessionLocal
from app.storage.models import Contact, Thread, Message
from app.providers.base import SmsProvider
from app.services.llm import classify_lt

app = FastAPI(title="sms-bot-dev")
Base.metadata.create_all(bind=engine)

DRY_RUN = os.getenv("DRY_RUN", "1") == "1"
PER_PERSON_MIN_SECONDS = int(os.getenv("PER_PERSON_MIN_SECONDS", "90"))

# replace provider line:
from app.providers.base import SmsProvider
provider = SmsProvider(dry_run=DRY_RUN)

@app.get("/healthz")
def healthz():
    return {"ok": True}

def within_business_hours() -> bool:
    if os.getenv("SKIP_BUSINESS_HOURS") == "1":
        return True
    now = datetime.now().time()
    return dtime(9,0) <= now < dtime(18,0)

@app.post("/send")
async def send(payload: dict):
    # --- guards ---
    if not within_business_hours():
        raise HTTPException(400, "Outside business hours (09:00–18:00)")
    to = payload["to"]; body = payload["body"]; userref = payload.get("userref")

    db = SessionLocal()
    try:
        # DNC guard
        c = db.query(Contact).filter_by(phone=to).first()
        if c and c.dnc:
            raise HTTPException(403, "DNC/STOP on this contact")

        # throttle per person
        from sqlalchemy import desc
        last_out = (
            db.query(Message)
              .join(Thread, Thread.id == Message.thread_id)
              .filter(Thread.phone == to, Message.dir == "out")
              .order_by(desc(Message.ts))
              .first()
        )
        if last_out and (datetime.utcnow() - last_out.ts).total_seconds() < PER_PERSON_MIN_SECONDS:
            raise HTTPException(429, "Per-person throttle")

        # ensure contact/thread
        if not c:
            c = Contact(phone=to); db.add(c)
        t = db.query(Thread).filter_by(phone=to, status="open").first()
        if not t:
            t = Thread(phone=to); db.add(t); db.flush()

        c = db.query(Contact).filter_by(phone=to).first()
        if c and c.dnc:
            raise HTTPException(403, "DNC/STOP on this contact")

        # send (still dry-run in Milestone 0)
        prov_id = await provider.send(to, body, userref=userref)
        m = Message(thread_id=t.id, dir="out", body=body, status="sent",
                    provider_id=prov_id, userref=userref)
        db.add(m)
        db.commit()
        return {"id": prov_id}
    finally:
        db.close()

@app.post("/webhooks/dlr")
async def dlr(req: Request):
    """Simulate a delivery report (you can POST JSON to this locally)."""
    payload = await req.json()
    data = provider.parse_dlr(payload, req.headers)
    db = SessionLocal()
    try:
        m = db.query(Message).filter_by(provider_id=data["provider_id"]).first()
        if not m:
            # if we’ve never stored an outbound with this id (race), just noop
            return {"ok": True}
        m.status = data["status"].lower()
        db.commit()
    finally:
        db.close()
    return {"ok": True}

@app.post("/webhooks/mo")
async def mo(req: Request):
    """
    Simulate inbound SMS (MO). POST e.g. {"msisdn":"3706...","message":"domina"}
    """
    payload = await req.json()
    mo = provider.parse_mo(payload, req.headers)
    text = (mo.get("text") or "").strip()
    text_l = text.lower()

    db = SessionLocal()
    try:
        # ensure contact & thread
        c = db.query(Contact).filter_by(phone=mo["from"]).first()
        if not c:
            c = Contact(phone=mo["from"])
            db.add(c)

        t = db.query(Thread).filter_by(phone=mo["from"], status="open").first()
        if not t:
            t = Thread(phone=mo["from"])
            db.add(t)
            db.flush()

        # store inbound message
        db.add(Message(thread_id=t.id, dir="in", body=text, status="delivered"))

        # STOP / DNC handling
        if text_l in {"stop", "ne", "atsisakyti", "nedomina"}:
            c.dnc = True
            db.commit()
            # confirm to the user (still dry-run in Milestone 0)
            await provider.send(mo["from"], "Sustabdyta. Dėkojame. (Atsisakėte pranešimų)", userref="dnc-confirm")
            print("[DNC] Marked contact as DNC and sent confirm.")
            return {"ok": True, "dnc": True}

        # otherwise: classify & (in milestone 0) just log the would-be reply
        db.commit()
    finally:
        db.close()

    cls = classify_lt(text)
    if cls["intent"] in {"not_interested", "wrong_contact"}:
        print("[AUTO-REPLY] LT: Ačiū, pažymėjau. Daugiau neberašysime.")
    else:
        print("[AUTO-REPLY] LT: Gal galite parašyti miestą ir patirtį (metais)?")

    return {"ok": True, "intent": cls["intent"], "confidence": cls["confidence"]}


@app.post("/send-batch")
async def send_batch(payload: dict):
    """payload: {items: [{to, body, userref?}, ...]}"""
    results = []
    for item in payload.get("items", []):
        try:
            r = await send(item)  # reuse logic
            results.append({"to": item["to"], "ok": True, "id": r["id"]})
        except HTTPException as e:
            results.append({"to": item["to"], "ok": False, "error": e.detail})
    return {"results": results}