FROM python:3.12-bookworm

RUN : \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        wait-for-it \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /usr/src/app

RUN python -m venv venv

COPY . .

RUN venv/bin/pip install . --no-cache-dir
