import logging, json, sys, os

class JsonFormatter(logging.Formatter):
    def format(self, record):
        d = {
            "lvl": record.levelname,
            "msg": record.getMessage(),
            "name": record.name,
        }
        if record.args and isinstance(record.args, dict):
            d.update(record.args)
        return json.dumps(d, ensure_ascii=False)

def get_logger(name="smsbot"):
    log = logging.getLogger(name)
    if not log.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(JsonFormatter())
        log.addHandler(h)
        level = getattr(logging, os.getenv("LOG_LEVEL","INFO").upper(), logging.INFO)
        log.setLevel(level)
    return log
