import sys
print("ENVCHK exe:", sys.executable)
print("ENVCHK sys.path:", sys.path)
try:
    import rapidfuzz, importlib
    print("ENVCHK rapidfuzz:", rapidfuzz.__version__, rapidfuzz.__file__)
except Exception as e:
    print("ENVCHK rapidfuzz import failed:", repr(e))
from app.main import app
