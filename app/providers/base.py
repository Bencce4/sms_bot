import time

class SmsProvider:
    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run

    async def send(self, to: str, body: str, userref: str | None = None) -> str:
        """
        In Milestone 0 we don't hit any external API.
        Return a fake provider message id so we can test the flow.
        """
        fake_id = f"dev-{int(time.time()*1000)}"
        print(f"[DRY_RUN SEND] to={to} userref={userref} body={body!r} -> id={fake_id}")
        return fake_id

    def parse_dlr(self, payload: dict, headers: dict) -> dict:
        # normalize a fake DLR shape
        return {
            "provider_id": str(payload.get("id", "dev-dlr")),
            "msisdn": str(payload.get("msisdn", "")),
            "status": payload.get("status", "DELIVERED").upper(),
            "userref": payload.get("userref")
        }

    def parse_mo(self, payload: dict, headers: dict) -> dict:
        # normalize a fake inbound shape
        return {
            "from": str(payload.get("msisdn") or payload.get("from") or ""),
            "to": str(payload.get("receiver") or payload.get("to") or ""),
            "text": payload.get("message") or payload.get("text") or ""
        }
