# CLAUDE.md - Google Drive Integration Worker

This file provides guidance to Claude Code when working with the Clustera Google Drive Integration Worker.

## Overview

This worker syncs Google Drive data (files, folders, permissions, sharing metadata) to the Clustera AI platform via the Google Drive API. It uses a **stateless action-based architecture** where the worker only implements HOW to retrieve/write data, not WHAT or WHEN.

## Architecture

### Worker Type: JSON-RPC 2.0 / Action-Based (Stateless)

The worker processes requests using JSON-RPC 2.0 message format per the message-dictionary specification:

| JSON-RPC 2.0 Method | Legacy Action | Purpose |
|---------------------|---------------|---------|
| `clustera.integration.content.fetch` | `fetch` | Retrieve Google Drive data |
| `clustera.integration.content.dispatch` | `write` | Upload/modify files, manage permissions |
| `clustera.integration.connection.initialize` | `init` | Validate access, set up webhooks |
| `clustera.integration.connection.teardown` | `teardown` | Stop webhooks, cleanup |
| (webhook passthrough) | `webhook` | Parse Drive API push notifications |
| `clustera.integration.capability.discover` | N/A | Report worker capabilities |

### Key Components

| Component | Path | Purpose |
|-----------|------|---------|
| Worker | `src/google_drive_worker/worker.py` | Main orchestration, Kafka consumption, action routing |
| Config | `src/google_drive_worker/config.py` | Pydantic settings from environment |
| Init Handler | `src/google_drive_worker/handlers/init.py` | Validates access, sets up webhooks |
| Fetch Handler | `src/google_drive_worker/handlers/fetch.py` | Fetches Google Drive resources |
| Write Handler | `src/google_drive_worker/handlers/write.py` | Uploads/modifies files, manages permissions |
| Webhook Handler | `src/google_drive_worker/handlers/webhook.py` | Processes Drive API push notifications |
| Teardown Handler | `src/google_drive_worker/handlers/teardown.py` | Stops webhooks, cleanup |
| Capability Handler | `src/google_drive_worker/handlers/capability.py` | Reports worker capabilities |
| ContentIngestEmitter | `src/google_drive_worker/handlers/content_emitter.py` | Shared module for building content.ingest messages |
| API Client | `src/google_drive_worker/client/google_drive_api.py` | Google Drive API wrapper |
| Schemas | `src/google_drive_worker/schemas/` | Pydantic message schemas |

### Kafka Topics

| Topic | Purpose |
|-------|---------|
| `integrations-worker-google-drive` | Inbound action messages |
| `integrations-incoming-records` | Response envelopes with data |
| `integrations-errors` | Error records |

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

### 1. Kafka Partitioning

**ALWAYS use `integration_connection_id` as partition key:**

```python
await producer.send(
    topic="integrations-incoming-records",
    key=message.integration_connection_id,  # REQUIRED
    value=envelope
)
```

### 2. Idempotency Keys

**Pattern**: `google-drive:{connection_id}:{resource_type}:{resource_id}`

```python
def generate_idempotency_key(
    self,
    connection_id: str,
    resource_type: str,
    resource_id: str,
) -> str:
    return f"google-drive:{connection_id}:{resource_type}:{resource_id}"
```

**Examples**:
- `google-drive:conn_abc:file:1BxiMVs0XRA5nFMdKvBd...`
- `google-drive:conn_abc:folder:0B1mvXRzY5nFMdKvBd...`
- `google-drive:conn_abc:permission:perm_123`
- `google-drive:conn_abc:revision:rev_456`

### 3. Message Format (JSON-RPC 2.0)

**Incoming requests** use `clustera.integration.content.fetch`:

```json
{
  "id": "fetch-001",
  "jsonrpc": "2.0",
  "method": "clustera.integration.content.fetch",
  "params": {
    "header": {
      "to_addresses": [{"topics": ["integrations-worker-google-drive"], "topic_key": "conn_abc123"}],
      "return_addresses": [{"topics": ["integrations-incoming-records"], "topic_key": "conn_abc123"}],
      "message_call_stack": [],
      "parameters": {
        "customer_id": "cust_xyz",
        "integration_provider_name": "google-drive",
        "integration_connection_id": "conn_abc123"
      }
    },
    "resource_type": "files",
    "filters": {"q": "mimeType='application/pdf'"},
    "pagination": {"max_results": 100, "page_token": null}
  }
}
```

**Outgoing responses** for content-bearing resources (files with content) use `clustera.integration.content.ingest`:

```json
{
  "jsonrpc": "2.0",
  "method": "clustera.integration.content.ingest",
  "params": {
    "header": {
      "to_addresses": [{"topics": ["integrations-incoming-records"], "topic_key": "conn_abc123"}],
      "return_addresses": [],
      "message_call_stack": [],
      "parameters": {
        "customer_id": "cust_xyz",
        "integration_provider_name": "google-drive",
        "integration_connection_id": "conn_abc123"
      }
    },
    "content": {
      "type": "file",
      "file_url": "s3://bucket/path/to/file.pdf",
      "mime_type": "application/pdf"
    },
    "metadata": {
      "resource_type": "file",
      "resource_id": "1BxiMVs0XRA5nFMdKvBd...",
      "source_document_url": "https://drive.google.com/file/d/1BxiMVs0XRA5nFMdKvBd.../view",
      "source_document_title": "Report.pdf"
    },
    "nonce": "google-drive:conn_abc123:file:1BxiMVs0XRA5nFMdKvBd..."
  }
}
```

**Outgoing responses** for metadata-only resources (files without content, permissions, changes) use `clustera.integration.incoming`:

```json
{
  "jsonrpc": "2.0",
  "method": "clustera.integration.incoming",
  "params": {
    "header": {
      "to_addresses": [{"topics": ["integrations-incoming-records"], "topic_key": "conn_abc123"}],
      "return_addresses": [],
      "message_call_stack": [],
      "parameters": {
        "customer_id": "cust_xyz",
        "integration_provider_name": "google-drive",
        "integration_connection_id": "conn_abc123"
      }
    },
    "provider": "google-drive",
    "payload": {
      "records": [/* normalized resource records */],
      "pagination": {
        "next_page_token": "...",
        "has_more": true
      }
    },
    "received_at": "2025-01-15T10:30:00.123Z",
    "nonce": "google-drive:conn_abc123:fetch:batch_001",
    "metadata": {
      "event_type": "fetch_response",
      "api_calls_made": 5
    }
  }
}
```

### Output Format by Resource Type

| Resource Type | Output Format | Description |
|--------------|---------------|-------------|
| `files` (with content) | `integration.content.ingest` (per file) | Files exported/downloaded with content |
| `files` (metadata only) | `incoming` | File metadata without content |
| `permissions` | `incoming` | Sharing permissions metadata |
| `changes` | `integration.content.ingest` + `incoming` | New/modified files use `content.ingest`, deletions use `incoming` |
| `about` | `incoming` | Drive quota/user info metadata |

**Note**: Content-bearing fetches require `snowball_id` in the connection config.

### 4. Legacy Action Message Examples (Backward Compatible)

The worker also accepts legacy action-based messages for backward compatibility:

**Fetch action (legacy):**
```json
{
  "message_id": "msg_001",
  "customer_id": "cust_xyz",
  "integration_id": "google-drive",
  "integration_connection_id": "conn_abc",
  "action": "fetch",
  "resource_type": "files",
  "filters": {"q": "mimeType='application/pdf'"},
  "pagination": {"page_token": null, "max_results": 100}
}
```

**Note**: New integrations should use JSON-RPC 2.0 format (see Section 3).

### 5. Error Classification

```python
# Retriable - retry with backoff
class RetriableError(IntegrationError):
    """Rate limits, network errors, 5xx responses"""

# Terminal - skip message
class TerminalError(IntegrationError):
    """Invalid credentials, 4xx errors, malformed data"""
```

### 6. S3 Offloading

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

### Changes Fetch (Incremental Sync)

The worker supports fetching changes via the fetch action:

```json
{
  "action": "fetch",
  "resource_type": "changes",
  "filters": {
    "start_page_token": "12345"
  }
}
```

**Important**: The worker does NOT track page tokens. The orchestrator must:
1. Store the last processed page_token (from `about.get` or previous changes response)
2. Pass it in fetch requests
3. Handle token expiration by triggering full sync

### Webhook Setup (Init Action)

Use the init action to set up push notifications:

```json
{
  "action": "init",
  "config": {
    "setup_webhook": true,
    "webhook_url": "https://webhook.example.com/google-drive"
  }
}
```

Push notifications expire after 24 hours or the configured TTL. The orchestrator should schedule periodic init calls to renew.

### Webhook Content Fetching

**TODO**: Implement webhook content fetching similar to Gmail pattern.

When enabled, the webhook handler should:
1. Validate Drive API signature
2. Extract resource ID from notification
3. Call Drive API to get file metadata/content
4. Emit `clustera.integration.content.ingest` messages (same as backfill)

**Environment Variables (planned):**

| Variable | Default | Description |
|----------|---------|-------------|
| `GOOGLE_DRIVE_WEBHOOK_FETCH_CONTENT` | `true` | Enable content fetching (set `false` for legacy passthrough) |
| `GOOGLE_DRIVE_WEBHOOK_MAX_FILES` | `50` | Max files per webhook (rate limit protection) |

**Error Handling (planned):**
- Invalid token: Emits `INVALID_TOKEN` error with `requires_reinit: true`
- Missing snowball_id: Emits `MISSING_SNOWBALL_ID` error
- Missing access_token: Emits `MISSING_ACCESS_TOKEN` error

### Capability Discovery

The worker supports capability discovery via the capability handler:

```json
{
  "method": "clustera.integration.capability.discover",
  "params": {
    "header": { ... }
  }
}
```

**Response:**
```json
{
  "method": "clustera.integration.capability.discovered",
  "params": {
    "capabilities": {
      "supported_actions": ["fetch", "write", "init", "teardown", "webhook"],
      "resource_types": {
        "files": {
          "actions": ["fetch", "write"],
          "supports_pagination": true,
          "supports_filters": true
        },
        "permissions": {
          "actions": ["fetch", "write"],
          "supports_pagination": false
        },
        "changes": {
          "actions": ["fetch"],
          "supports_pagination": true,
          "incremental_sync": true
        }
      },
      "webhook_support": {
        "enabled": true,
        "renewal_required": true,
        "ttl_hours": 24
      }
    }
  }
}
```

## Development Commands

```bash
# Run locally (mock mode)
GOOGLE_DRIVE_MOCK_MODE=true uv run python -m google_drive_worker

# Run tests
uv run pytest

# Lint
uv run ruff check src tests

# Type check
uv run mypy src
```

## Testing Strategy

### Unit Tests (`tests/unit/`)

- Config validation
- Handler logic (can_handle, process_message)
- Idempotency key generation
- Data transformation

### Integration Tests (`tests/integration/`)

- Kafka producer/consumer flow
- Full message processing pipeline
- Error handling and retry logic

### Mock Mode

Set `GOOGLE_DRIVE_MOCK_MODE=true` to use mock responses instead of real API calls. Mock data should be added to `src/google_drive_worker/client/mock_data.py`.

## Resource Types

| Resource | Drive ID Format | Notes |
|----------|-----------------|-------|
| file | `1BxiMVs0XRA5nFMdKvBd...` | Base64-encoded string |
| folder | `0B1mvXRzY5nFMdKvBd...` | Special MIME type: `application/vnd.google-apps.folder` |
| permission | `perm_123` | Permission-specific ID |
| revision | `rev_456` or timestamp | Revision-specific ID |
| change | N/A | Derived from `pageToken` in API |

## Anti-Patterns to Avoid

1. **NO auto-commit**: Always use manual Kafka offset commits
2. **NO composite partition keys**: Use only `integration_connection_id`
3. **NO secrets in logs**: Log opaque references only
4. **NO HTTP between workers**: All communication via Kafka
5. **NO blocking calls**: All I/O must be async

## Related Documentation

- Bridge repo: `/docs/architecture.md`
- Message schemas: `/docs/04-message-schemas.md`
- Anti-patterns: `/docs/anti-patterns.md`
- Google Drive API docs: https://developers.google.com/drive/api/
