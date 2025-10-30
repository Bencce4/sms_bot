import asyncio
from app.util.logger import get_logger

log = get_logger("senders.infobip")

try:
    from app.providers.infobip import InfobipProvider, is_enabled
except Exception:
    InfobipProvider = None
    def is_enabled() -> bool:
        return False

async def send_sms_async(to: str, body: str, userref: str | None = None):
    if InfobipProvider and is_enabled():
        provider = InfobipProvider(dry_run=True)
        return await provider.send(to, body, userref=userref)
    else:
        log.info("DRY RUN send_sms to=%s userref=%s len=%d", to, userref, len(body))
        return None

def send_sms(to: str, body: str, userref: str | None = None):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        asyncio.create_task(send_sms_async(to, body, userref))
        return None
    return asyncio.run(send_sms_async(to, body, userref))
