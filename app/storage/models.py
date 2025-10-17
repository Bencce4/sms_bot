from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, Text
from sqlalchemy.orm import relationship
from datetime import datetime
from .db import Base

class Contact(Base):
    __tablename__ = "contacts"
    phone = Column(String, primary_key=True)
    dnc = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)

class Thread(Base):
    __tablename__ = "threads"
    id = Column(Integer, primary_key=True, autoincrement=True)
    phone = Column(String, ForeignKey("contacts.phone"))
    status = Column(String, default="open")  # open/closed
    last_user_ts = Column(DateTime)

class Message(Base):
    __tablename__ = "messages"
    id = Column(Integer, primary_key=True, autoincrement=True)
    thread_id = Column(Integer, ForeignKey("threads.id"))
    dir = Column(String)           # in | out
    body = Column(Text)
    ts = Column(DateTime, default=datetime.utcnow)
    status = Column(String)        # queued/sent/delivered/failed
    provider_id = Column(String)   # external id (for later)
    userref = Column(String)       # your idempotency key
