# CLAUDE.md - Google Drive Integration Worker

This file provides guidance to Claude Code when working with the Clustera Google Drive Integration Worker.

## Overview

This worker syncs Google Drive data (files, folders, permissions, revisions, sharing metadata) to the Clustera AI platform. It supports both polling triggers and real-time webhooks for change notifications.

See `README.md` for quick start, architecture diagrams, Kubernetes deployment, and usage examples.

## Architecture

### Worker Type: Hybrid (Polling + Webhooks)

The worker:
- Consumes polling triggers from `integrations-inbound-trigger`
- Consumes webhook notifications from `integrations-inbound-webhook`
- Fetches data from the Google Drive API

### Key Components

| Component | Path | Purpose |
|-----------|------|---------|
| Worker | `src/google_drive_worker/worker.py` | Main orchestration, Kafka consumption |
| Config | `src/google_drive_worker/config.py` | Pydantic settings from environment |
| Trigger Handler | `src/google_drive_worker/handlers/trigger.py` | Handles polling triggers |
| Webhook Handler | `src/google_drive_worker/handlers/webhook.py` | Handles change notifications |
| Google Client | `src/google_drive_worker/client/` | Google Drive API wrapper |
| Normalizer | `src/google_drive_worker/normalizer.py` | Data normalization |
| Schemas | `src/google_drive_worker/schemas/` | Pydantic message schemas |

### Kafka Topics

| Topic | Purpose |
|-------|---------|
| `integrations-inbound-trigger` | Inbound polling triggers (consumed) |
| `integrations-inbound-webhook` | Inbound webhook notifications (consumed) |
| `integrations-incoming-records` | Normalized data output (produced) |
| `integrations-errors` | Error records (produced) |

## Shared Toolkit

This worker uses the **clustera-integration_helper-toolkit** shared library (located at `lib/clustera-integration_helper-toolkit/`). The toolkit provides:

| Module | Purpose |
|--------|---------|
| `clustera_toolkit.kafka.KafkaProducer` | Async Kafka producer with idempotence enabled |
| `clustera_toolkit.kafka.KafkaConsumer` | Async Kafka consumer with manual commit support |
| `clustera_toolkit.storage.S3Client` | S3 upload/download with SHA-256 checksums |
| `clustera_toolkit.message.*` | Message envelope builders, size guards (256KB threshold) |
| `clustera_toolkit.idempotency.IdempotencyCache` | In-memory LRU deduplication cache |
| `clustera_toolkit.secrets.ControlPlaneClient` | M2M token client for fetching OAuth tokens |
| `clustera_toolkit.bootstrap.bootstrap_environment()` | Environment initialization and config loading |

**Usage in pyproject.toml:**
```toml
[project]
dependencies = [
    "clustera-integration-toolkit @ file:///${PROJECT_ROOT}/lib/clustera-integration_helper-toolkit"
]
```

**Important**: Always use toolkit utilities instead of implementing your own Kafka/S3/idempotency logic.

## Critical Patterns

### Kafka Partitioning

**ALWAYS use `integration_connection_id` as partition key:**

```python
await producer.send(
    topic="integrations-incoming-records",
    key=message.integration_connection_id,  # REQUIRED
    value=envelope
)
```

### Idempotency Keys

**Pattern**: `google-drive:{connection_id}:{resource_type}:{resource_id}`

**Examples**:
- `google-drive:conn_abc:file:1BxiMVs0XRA5nFMdKvBd...`
- `google-drive:conn_abc:folder:0B1mvXRzY5nFMdKvBd...`
- `google-drive:conn_abc:permission:perm_123`
- `google-drive:conn_abc:revision:rev_456`

### S3 Offloading

For payloads >256KB (common with file content), use the toolkit's S3Client:

```python
if len(payload_bytes) > 256 * 1024:
    s3_url = await s3_client.upload(payload_bytes, checksum)
    message["payload"] = None
    message["s3_url"] = s3_url
    message["metadata"]["sha256"] = checksum
```

## Google Drive API Specifics

### OAuth Scopes

- `drive.readonly`: Read-only access to files and metadata
- `drive.metadata.readonly`: Read-only access to metadata only
- `drive`: Full access (for write operations)

### Key Endpoints

- `files.list` / `files.get`: File and folder metadata
- `permissions.list`: Sharing permissions
- `revisions.list`: File revision history
- `changes.list`: Incremental change tracking
- `channels.stop`: Stop webhook notifications

### Rate Limits

- **Per-user**: 12,000 queries per minute
- **Per-project**: Varies by quota
- Use exponential backoff for 403/429 responses

## Development Commands

```bash
# Run locally (mock mode)
GOOGLE_DRIVE_MOCK_MODE=true python -m google_drive_worker

# Run tests
pytest

# Lint
ruff check src tests

# Type check
mypy src
```

## Anti-Patterns to Avoid

1. **NO auto-commit**: Always use manual Kafka offset commits
2. **NO composite partition keys**: Use only `integration_connection_id`
3. **NO secrets in logs**: Log opaque references only
4. **NO HTTP between workers**: All communication via Kafka
5. **NO blocking calls**: All I/O must be async
6. **NO custom Kafka/S3 implementations**: Use the shared toolkit

## Related Documentation

- Bridge repo: `../../docs/architecture/overview.md`
- Message schemas: `../../docs/architecture/message-schemas.md`
- Anti-patterns: `../../docs/reference/anti-patterns.md`
- Google Drive API: https://developers.google.com/drive/api/
