FROM python:3.12-bookworm

RUN : \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        wait-for-it \
        # TODO: once we have wheels on PyPi for these, we can remove this
        gfortran \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    UV_NO_CACHE=1

WORKDIR /usr/src/app

RUN pip install uv

COPY . .

RUN uv pip install . --no-cache --system
