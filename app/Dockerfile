FROM python:3.12-slim
WORKDIR /srv
RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
 && python - <<'PY'
import importlib
for m in ["pandas","openpyxl","jinja2","multipart"]:
    importlib.import_module(m)
print("deps_ok")
PY

COPY . /srv/app
WORKDIR /srv/app
CMD ["uvicorn","main:app","--host","0.0.0.0","--port","8000"]
