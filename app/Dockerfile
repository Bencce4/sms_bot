FROM python:3.12-slim

# System deps (optional but useful)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app

# Install python deps
RUN pip install --no-cache-dir fastapi "uvicorn[standard]" httpx "pydantic>=2" SQLAlchemy psycopg[binary] python-jose[cryptography]

# Expose port inside the container
EXPOSE 8000
CMD ["uvicorn","app.main:app","--host","0.0.0.0","--port","8000"]
