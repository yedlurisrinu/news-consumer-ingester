# ── Stage 1: Builder ──────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl \
    && rm -rf /var/lib/apt/lists/*

# uv binary - pinned version
COPY --from=ghcr.io/astral-sh/uv:0.11.7 /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv

    # Always fresh — local packages
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# ── Stage 2: Runtime ──────────────────────────────────────
FROM python:3.12-slim AS runtime

WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

COPY src/ ./src/

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages \
                    /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy source code and static files
COPY . .

# Set PYTHONPATH so imports resolve cleanly
ENV PYTHONPATH=src
# Non-root user for security
RUN adduser --disabled-password --gecos "" appuser

# Create folder to host log files
RUN mkdir -p /app/logs && chown -R appuser:appuser /app/logs

USER appuser

# Run with Gunicorn + Uvicorn workers for production
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8007", "--log-level", "info"]

