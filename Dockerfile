FROM python:3.12-bookworm
ARG GH_PAT

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

COPY ./requirements.txt .

RUN python -m venv venv
RUN GH_TOKEN=${GH_PAT} venv/bin/pip install --no-cache-dir -r requirements.txt

COPY app app
