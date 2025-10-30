FROM python:3.12-slim

WORKDIR /srv

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl && \
    rm -rf /var/lib/apt/lists/*

# show what requirements the image sees (debug)
COPY requirements.txt /tmp/requirements.txt
RUN echo "---- requirements seen in image ----" && cat /tmp/requirements.txt && echo "------------------------------------"

# force-install heavy deps; then the rest
RUN pip install --no-cache-dir \
      pandas==2.2.3 \
      openpyxl==3.1.5 \
      jinja2==3.1.4 \
      python-multipart==0.0.12 \
 && pip install --no-cache-dir -r /tmp/requirements.txt \
 && python - <<'PY'
import importlib
for m in ["pandas","openpyxl","jinja2","multipart"]:
    importlib.import_module(m)
print("deps_ok")
PY

COPY . /srv/app
WORKDIR /srv/app
