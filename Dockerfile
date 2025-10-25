# Multi-stage Dockerfile for Google Drive integration worker

# Stage 1: Builder
FROM python:3.11-slim AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    git \
    libsnappy-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv for faster dependency installation
RUN pip install --no-cache-dir uv

# Set working directory
WORKDIR /app

# Copy project files
COPY pyproject.toml .
COPY src/ src/
COPY lib/ lib/

# Install dependencies using uv
RUN uv pip install --system -e . --no-cache

# Stage 2: Runtime
FROM python:3.11-slim

# Install runtime dependencies only
RUN apt-get update && apt-get install -y \
    libsnappy1v5 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 gdriveuser

# Set working directory
WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /app /app

# Switch to non-root user
USER gdriveuser

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    WORKER_INTEGRATION_ID=google-drive \
    WORKER_MAX_CONCURRENT_CONNECTIONS=10 \
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
    KAFKA_CONSUMER_GROUP_ID=google-drive-worker \
    LOG_LEVEL=INFO \
    LOG_FORMAT=json

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Entry point
ENTRYPOINT ["python", "-m", "google_drive_worker"]