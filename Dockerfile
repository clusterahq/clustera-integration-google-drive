# Multi-stage build for Google Drive integration worker
#
# The toolkit is cloned during build. For private repos, pass GITHUB_TOKEN as build arg:
#   docker build --build-arg GITHUB_TOKEN=ghp_xxx -t google-drive-worker .

# Stage 1: Build environment
FROM python:3.11-slim AS builder

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast dependency management
RUN pip install --no-cache-dir uv

# Set working directory
WORKDIR /app

# Clone the shared toolkit at a specific commit
# TOOLKIT_VERSION: The exact commit hash to checkout (update when toolkit changes)
# GITHUB_TOKEN: Required for private repo access
ARG GITHUB_TOKEN
ARG TOOLKIT_VERSION="b46bbf6"
RUN set -e && \
    if [ -z "$GITHUB_TOKEN" ]; then \
        echo "ERROR: GITHUB_TOKEN build arg is required for private toolkit repo" && exit 1; \
    fi && \
    echo "=== Cloning toolkit at commit: ${TOOLKIT_VERSION} ===" && \
    git clone --no-checkout https://${GITHUB_TOKEN}@github.com/clusterahq/clustera-integration_helper-toolkit.git lib/clustera-integration_helper-toolkit && \
    cd lib/clustera-integration_helper-toolkit && \
    git checkout ${TOOLKIT_VERSION} && \
    echo "=== Toolkit checkout complete ===" && \
    git log -1 --oneline && \
    echo "=== Verifying ConnectionCredentialsResponse exists ===" && \
    grep -q "class ConnectionCredentialsResponse" clustera_integration_toolkit/control_plane/models/secrets.py && \
    echo "=== Verifying ConnectionResolver exists ===" && \
    grep -q "class ConnectionResolver" clustera_integration_toolkit/control_plane/resolver.py && \
    echo "=== Verification passed ==="

# Copy dependency files
COPY pyproject.toml ./
COPY README.md ./
COPY uv.lock* ./

# Copy source code (needed for editable install)
COPY src ./src

# Install dependencies using uv
# The toolkit's [storage] extra is already specified in pyproject.toml
RUN uv pip install --system -e . --no-cache

# Stage 2: Runtime environment
FROM python:3.11-slim

# Create non-root user
RUN groupadd -r google_drive_worker && useradd -r -g google_drive_worker google_drive_worker

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code
COPY --chown=google_drive_worker:google_drive_worker src/ ./src/
# Copy toolkit from builder stage (needed for editable install)
COPY --from=builder --chown=google_drive_worker:google_drive_worker /app/lib /app/lib

# Set environment variables
ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    # Worker defaults (can be overridden)
    WORKER_INTEGRATION_ID=google-drive \
    WORKER_MAX_CONCURRENT_CONNECTIONS=10 \
    # Kafka defaults
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
    KAFKA_CONSUMER_GROUP_ID=google-drive-worker \
    KAFKA_ENABLE_AUTO_COMMIT=false \
    # Google Drive API defaults
    GOOGLE_DRIVE_MOCK_MODE=false \
    GOOGLE_DRIVE_API_BASE_URL=https://www.googleapis.com/drive/v3 \
    # Storage defaults
    STORAGE_MOCK_MODE=false \
    # Logging defaults
    LOG_LEVEL=INFO

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1

# Switch to non-root user
USER google_drive_worker

# Expose metrics port (if implementing metrics endpoint)
EXPOSE 8080

# Set the entrypoint
ENTRYPOINT ["python", "-m", "google_drive_worker"]

# Labels for container metadata
LABEL \
    org.opencontainers.image.title="Google Drive Integration Worker" \
    org.opencontainers.image.description="Kafka-native worker for Google Drive data integration" \
    org.opencontainers.image.version="0.1.0" \
    org.opencontainers.image.vendor="Clustera" \
    org.opencontainers.image.source="https://github.com/clusterahq/clustera-integration-google-drive" \
    maintainer="platform@clustera.io"