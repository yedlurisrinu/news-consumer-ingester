# ── Stage 1: Builder ──────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Always fresh — local packages
COPY requirements_locales.txt .
RUN pip install --no-cache-dir -r requirements_locales.txt

# ── Stage 2: Runtime ──────────────────────────────────────
FROM python:3.12-slim AS runtime

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages \
                    /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy source code and static files
COPY . .

# Set PYTHONPATH so imports resolve cleanly

# Non-root user for security
RUN adduser --disabled-password --gecos "" appuser

# Create folder to host log files
RUN mkdir -p /app/logs && chown -R appuser:appuser /app/logs

USER appuser

# Run with Gunicorn + Uvicorn workers for production
CMD ["uvicorn", "src.main:app"]

