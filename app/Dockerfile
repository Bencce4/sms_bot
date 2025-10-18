FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl && rm -rf /var/lib/apt/lists/*

WORKDIR /srv
ENV PYTHONPATH=/srv

# build context is ./app, so this copies your code to /srv/app
COPY . /srv/app

# install deps; use requirements.txt if present
RUN if [ -f /srv/app/requirements.txt ]; then \
      pip install --no-cache-dir -r /srv/app/requirements.txt ; \
    else \
      pip install --no-cache-dir fastapi "uvicorn[standard]" httpx "pydantic>=2" \
        SQLAlchemy psycopg[binary] python-jose[cryptography] openai ; \
    fi

EXPOSE 8000
CMD ["uvicorn","app.main:app","--host","0.0.0.0","--port","8000"]
