FROM python:3.12-slim

ENV PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /srv/app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
  && rm -rf /var/lib/apt/lists/*

# Install deps strictly from the repo's app/requirements.txt
COPY requirements.txt /srv/app/requirements.txt
RUN python -m pip install --upgrade pip && \
    pip install -r /srv/app/requirements.txt

# Add the app code after deps to leverage layer caching
COPY . /srv/app
