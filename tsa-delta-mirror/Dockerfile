# syntax=docker/dockerfile:1.7
# Single image for ECS Fargate (default) and, later, Lambda (see README "Lambda later").
# Base pinned by digest (python:3.12-slim multi-arch manifest, pulled 2026-10-23). Re-pin with:
#   docker buildx imagetools inspect python:3.12-slim
ARG BASE=python:3.12-slim@sha256:a0f57064cbdcd6285fa45f51237ab0b35ea8ebaed06a1f5973e89e19dfb86704

FROM ${BASE} AS builder
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /build
COPY requirements.lock.txt ./
RUN python -m venv /opt/venv \
 && /opt/venv/bin/pip install --require-hashes --no-deps -r requirements.lock.txt
COPY pyproject.toml README.md ./
COPY src ./src
RUN /opt/venv/bin/pip install --no-deps .

FROM ${BASE} AS runtime
LABEL org.opencontainers.image.title="tsa-delta-mirror" \
      org.opencontainers.image.description="Delta Sharing -> plain Delta Lake on S3 (no Spark)" \
      org.opencontainers.image.source="https://github.com/sparrowlabsdev/s3-lambda-producer-pipeline"
# Non-root, fixed UID so task-definition `user` and ATO scans agree. /tmp is the only writable path.
RUN groupadd --gid 10001 mirror && useradd --uid 10001 --gid 10001 --create-home --shell /usr/sbin/nologin mirror \
 && apt-get update && apt-get upgrade -y --no-install-recommends \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TMPDIR=/tmp \
    MIRROR_LOG_LEVEL=INFO
USER 10001:10001
WORKDIR /home/mirror
ENTRYPOINT ["python", "-m", "tsa_delta_mirror"]
