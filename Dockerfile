# LLM Bot Traffic Analysis Pipeline
# Multi-provider CDN log ingestion and analysis
# Includes Google Cloud CLI for BigQuery operations

# ── Stage 1: builder ──────────────────────────────────────────────────────────
# Installs all system deps (including gcloud CLI), Python packages, and builds
# the package into an isolated prefix. The runtime stage copies only what it
# needs: site-packages, gcloud SDK, and application scripts.
FROM python:3.12.8-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    apt-transport-https \
    ca-certificates \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
    | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
    && apt-get update && apt-get install -y google-cloud-cli \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy only what pip needs to build and install the package
COPY requirements.txt pyproject.toml ./
COPY src/ ./src/

# Install all Python dependencies into an isolated prefix so only the installed
# files are copied to the runtime stage (no pip, setuptools, wheel, or cache)
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt \
    && pip install --no-cache-dir --prefix=/install ".[gcp,ml]"

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
# Minimal production image: copies gcloud SDK and Python packages from builder.
# Excludes: pip, setuptools, wheel, build caches, raw source tree, test/doc
# files, apt build tooling, and Google Cloud repo/key configuration.
FROM python:3.12.8-slim AS runtime

# Only ca-certificates needed at runtime (SSL for gcloud + GCP API calls)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy Google Cloud SDK from builder (avoids re-running apt in runtime)
COPY --from=builder /usr/lib/google-cloud-sdk /usr/lib/google-cloud-sdk
RUN ln -s /usr/lib/google-cloud-sdk/bin/gcloud /usr/bin/gcloud \
    && ln -s /usr/lib/google-cloud-sdk/bin/gsutil /usr/bin/gsutil \
    && ln -s /usr/lib/google-cloud-sdk/bin/bq /usr/bin/bq

# Copy installed Python packages from builder into the standard system prefix.
# Includes llm_bot_pipeline and all dependencies; pip/setuptools/wheel are
# excluded because they were not installed under /install.
COPY --from=builder /install /usr/local

WORKDIR /app

# Copy only runtime-required application files (no src/, tests/, docs/, etc.)
COPY scripts/ ./scripts/

# Create data directory
RUN mkdir -p /app/data

# Run as non-root user
RUN useradd --create-home --shell /bin/bash appuser && \
    chown -R appuser:appuser /app
USER appuser
RUN mkdir -p /home/appuser/.config/gcloud

# Default command
CMD ["python", "scripts/run_daily_etl.py"]
