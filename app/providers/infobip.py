import os
import json
import time
import logging
import asyncio
from typing import Optional, Dict, Any, List

import requests

# --------------------------------------------------------------------
# Config (read at instantiation to allow env changes on container restarts)
# --------------------------------------------------------------------
TIMEOUT = (5, 15)  # connect, read
log = logging.getLogger("infobip")



import os, json, logging, time
from typing import Optional, Dict, Any, List, Tuple
import requests

TIMEOUT = (5, 15)  # connect, read

API_BASE = os.getenv("INFOBIP_API_BASE", "").rstrip("/")
API_KEY  = os.getenv("INFOBIP_API_KEY", "")
SENDER   = os.getenv("INFOBIP_SENDER", "")

log = logging.getLogger("infobip")

def is_enabled() -> bool:
    return bool(API_BASE and API_KEY and SENDER)

def _headers() -> Dict[str, str]:
    return {
        "Authorization": API_KEY if API_KEY.startswith("App ") else f"App {API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def send_text(to: str, text: str) -> Dict[str, Any]:
    if not is_enabled():
        return {"ok": False, "provider_id": None, "raw": {"reason": "disabled"}}

    url = f"{API_BASE}/sms/2/text/advanced"
    payload = {
        "messages": [{
            "from": SENDER,
            "destinations": [{"to": str(to)}],
            "text": text
        }]
    }
    r = requests.post(url, headers=_headers(), data=json.dumps(payload), timeout=TIMEOUT)
    try:
        data = r.json()
    except Exception:
        data = {"_raw": r.text}

    ok = r.status_code in (200, 201)
    provider_id = None
    try:
        msgs = data.get("messages") or data.get("results") or []
        if msgs and isinstance(msgs, list):
            provider_id = msgs[0].get("messageId") or msgs[0].get("messageIdString")
    except Exception:
        pass

    if not ok:
        log.error("Infobip send failed %s %s", r.status_code, data)

    return {"ok": ok, "provider_id": provider_id, "raw": data}

def parse_inbound(payload: Dict[str, Any]) -> List[Dict[str, str]]:
    results = payload.get("results") or payload.get("messages") or []
    out = []
    for r in results:
        out.append({
            "from": str(r.get("from") or r.get("sender") or ""),
            "to": str(r.get("to") or r.get("destination") or ""),
            "text": (r.get("message") or r.get("text") or "")[:1000],
            "provider_id": str(r.get("messageId") or r.get("messageIdString") or ""),
        })
    return out

# -----------------
# PULL INBOUND API
# -----------------

def fetch_inbound(limit: int = 100) -> Tuple[List[Dict[str,str]], Dict[str, Any]]:
    """
    Calls GET /messages-api/1/inbound?channel=SMS&limit=N
    Returns (normalized_list, raw_json). Infobip guarantees each item is returned only once.
    """
    if not API_BASE or not API_KEY:
        return ([], {"error": "missing-config"})

    url = f"{API_BASE}/messages-api/1/inbound"
    params = {"channel": "SMS", "limit": min(max(limit, 1), 1000)}
    r = requests.get(url, headers=_headers(), params=params, timeout=TIMEOUT)
    try:
        data = r.json()
    except Exception:
        data = {"_raw": r.text, "_status": r.status_code}

    if r.status_code != 200:
        log.error("Infobip inbound fetch failed %s %s", r.status_code, data)
        return ([], data)

    # Example response: {"results":[{...}], "messageCount": 1, "pendingMessageCount": 0}
    normalized = []
    for it in data.get("results", []):
        normalized.append({
            "from": str(it.get("sender") or it.get("from") or ""),
            "to": str(it.get("destination") or it.get("to") or ""),
            "text": (it.get("message") or it.get("text") or "")[:1000],
            "provider_id": str(it.get("messageId") or ""),
        })
    return (normalized, data)

# --------------------------------------------------------------------
# Provider class
# --------------------------------------------------------------------
class InfobipProvider:
    """
    Minimal async-capable provider for Infobip:
      - send(): async wrapper over requests via thread offload
      - parse_mo(): normalizes both our CLI test payloads and Infobip webhooks
      - is_enabled(): checks presence of base/key/sender
    """

    def __init__(self, dry_run: bool = False):
        self.api_base = (os.getenv("INFOBIP_API_BASE", "") or os.getenv("INFOBIP_BASE", "")).rstrip("/")
        self.api_key = os.getenv("INFOBIP_API_KEY", "")
        self.sender = os.getenv("INFOBIP_SENDER", "")
        self.dry_run = dry_run

    # ---------- capability ----------

    @staticmethod
    def _auth_header(key: str) -> str:
        return key if key.startswith("App ") else f"App {key}"

    # ---------- outbound ----------
    async def send(self, to: str, text: str, userref: Optional[str] = None) -> str:
        """
        Returns a provider id (string). In dry-run or error, returns 'dev-<ts>'.
        """
        dev_id = f"dev-{int(time.time() * 1000)}"

        if self.dry_run or not self.is_enabled():
            log.info("[DRY_RUN SEND] to=%s userref=%s body=%r", to, userref, text)
            return dev_id

        url = f"{self.api_base}/sms/2/text/advanced"
        payload = {
            "messages": [{
                "from": self.sender,
                "destinations": [{"to": str(to)}],
                "text": text
            }]
        }
        headers = {
            "Authorization": self._auth_header(self.api_key),
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        def _post():
            return requests.post(url, headers=headers, data=json.dumps(payload), timeout=TIMEOUT)

        try:
            resp = await asyncio.to_thread(_post)
            try:
                data = resp.json()
            except Exception:
                data = {"_raw": resp.text}

            if resp.status_code not in (200, 201):
                log.error("Infobip send failed status=%s data=%s", resp.status_code, data)
                return dev_id

            # Attempt to extract messageId
            provider_id = None
            msgs = data.get("messages") or data.get("results") or []
            if isinstance(msgs, list) and msgs:
                provider_id = msgs[0].get("messageId") or msgs[0].get("messageIdString")
            return provider_id or dev_id
        except Exception as e:
            log.exception("Infobip send error: %s", e)
            return dev_id

    # ---------- inbound ----------
    def parse_mo(self, payload: Dict[str, Any], headers: Dict[str, Any]) -> Dict[str, str]:
        """
        Normalize inbound into: {"from": "...", "to": "...", "text": "...", "provider_id": "..."}
        Supports:
          - Our CLI test: {"msisdn":"...", "message":"..."}
          - Infobip webhook: {"results":[{"from":"...","to":"...","text":"...","messageId":"..."}]}
          - Also tolerates {"messages":[...]} variant from some docs.
        """
        # CLI simulator path
        if "msisdn" in payload and "message" in payload:
            return {
                "from": str(payload.get("msisdn") or ""),
                "to": str(payload.get("to") or ""),
                "text": (payload.get("message") or "")[:1000],
                "provider_id": "",
            }

        # Infobip webhook path
        results = payload.get("results") or payload.get("messages") or []
        if isinstance(results, list) and results:
            r = results[0]
            return {
                "from": str(r.get("from") or r.get("sender") or ""),
                "to": str(r.get("to") or r.get("destination") or ""),
                "text": (r.get("message") or r.get("text") or "")[:1000],
                "provider_id": str(r.get("messageId") or r.get("messageIdString") or ""),
            }

        # Fallback: best-effort
        return {
            "from": str(payload.get("from") or ""),
            "to": str(payload.get("to") or ""),
            "text": (payload.get("text") or payload.get("message") or "")[:1000],
            "provider_id": str(payload.get("messageId") or ""),
        }

# --------------------------------------------------------------------
# Backward-compatible module-level helpers (used by older code)
# --------------------------------------------------------------------
# We keep one shared instance so env is read once per process.
_PROVIDER_SINGLETON: Optional[InfobipProvider] = None

def _provider() -> InfobipProvider:
    global _PROVIDER_SINGLETON
    if _PROVIDER_SINGLETON is None:
        _PROVIDER_SINGLETON = InfobipProvider(dry_run=os.getenv("DRY_RUN", "1") == "1")
    return _PROVIDER_SINGLETON


def send_text(to: str, text: str) -> Dict[str, Any]:
    """
    Synchronous-ish façade for legacy callers; wraps async send().
    Returns {ok: bool, provider_id: str|None, raw: dict} for compatibility.
    """
    prov = _provider()

    async def _run():
        pid = await prov.send(to, text)
        return pid

    try:
        pid = asyncio.run(_run())
    except RuntimeError:
        # Already in event loop (FastAPI); delegate to thread
        pid = asyncio.get_event_loop().run_until_complete(_run())

    return {"ok": bool(pid), "provider_id": pid, "raw": {}}

def parse_inbound(payload: Dict[str, Any]) -> List[Dict[str, str]]:
    """Legacy shape → list of messages."""
    prov = _provider()
    msg = prov.parse_mo(payload, {})
    return [msg] if msg.get("from") or msg.get("text") else []
