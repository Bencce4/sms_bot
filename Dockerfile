FROM python:3.12-slim
WORKDIR /srv/app
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/srv/app
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
COPY app/requirements.txt /srv/app/requirements.txt
RUN python -m pip install --upgrade pip && pip install -r /srv/app/requirements.txt
# IMPORTANT: copy repo root into /srv/app (NOT into /srv/app/app)
COPY . /srv/app
CMD ["uvicorn","app.main:app","--host","0.0.0.0","--port","8000"]
