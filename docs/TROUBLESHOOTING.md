# Google Drive Worker Troubleshooting Guide

## Overview

This guide helps diagnose and resolve common issues with the Google Drive integration worker.

## Common Issues and Solutions

### 1. OAuth Authentication Failures

#### Symptom: "Invalid Credentials" Error
```
ERROR: 401 Unauthorized - Invalid Credentials
```

**Possible Causes:**
- Expired access token
- Invalid refresh token
- Revoked permissions

**Solutions:**

1. **Check refresh token validity:**
```python
# Test refresh token
curl -X POST "https://oauth2.googleapis.com/token" \
  -d "client_id=${GOOGLE_CLIENT_ID}" \
  -d "client_secret=${GOOGLE_CLIENT_SECRET}" \
  -d "refresh_token=${REFRESH_TOKEN}" \
  -d "grant_type=refresh_token"
```

2. **Verify environment variables:**
```bash
# Check if tokens are set
echo $GOOGLE_REFRESH_TOKEN | head -c 10
```

3. **Re-authenticate if needed:**
- Follow OAuth setup guide to obtain new refresh token
- Update stored credentials in Control Plane

#### Symptom: "Token has been expired or revoked"
```
ERROR: Token has been expired or revoked
```

**Solution:**
- User needs to reauthorize the application
- Trigger reauthorization flow through Control Plane

### 2. Rate Limiting Issues

#### Symptom: 429 Too Many Requests
```
ERROR: 429 - User Rate Limit Exceeded
```

**Solutions:**

1. **Check current quotas:**
```python
# Monitor request rate
import time
from collections import deque

class RateLimiter:
    def __init__(self, max_requests=10, window=1):
        self.max_requests = max_requests
        self.window = window
        self.requests = deque()

    async def wait_if_needed(self):
        now = time.time()
        # Remove old requests
        while self.requests and self.requests[0] < now - self.window:
            self.requests.popleft()

        if len(self.requests) >= self.max_requests:
            sleep_time = self.window - (now - self.requests[0])
            await asyncio.sleep(sleep_time)

        self.requests.append(now)
```

2. **Implement exponential backoff:**
```python
async def retry_with_backoff(func, max_retries=5):
    for attempt in range(max_retries):
        try:
            return await func()
        except RateLimitError as e:
            if attempt == max_retries - 1:
                raise
            wait = (2 ** attempt) + random.uniform(0, 1)
            logger.warning(f"Rate limited, waiting {wait}s")
            await asyncio.sleep(wait)
```

3. **Reduce request frequency:**
- Increase `GDRIVE_API_RATE_LIMIT` environment variable
- Reduce `WORKER_MAX_CONCURRENT_CONNECTIONS`
- Increase batch sizes where possible

### 3. Kafka Consumer Lag

#### Symptom: High Consumer Lag
```
Consumer group 'google-drive-worker' has lag of 10000 messages
```

**Diagnosis:**
```bash
# Check consumer lag
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group google-drive-worker --describe
```

**Solutions:**

1. **Scale up workers:**
```yaml
# Increase replicas in kubernetes
kubectl scale deployment google-drive-worker --replicas=5
```

2. **Optimize processing:**
```python
# Increase batch processing
KAFKA_MAX_POLL_RECORDS=500  # Process more messages per poll
```

3. **Check for processing bottlenecks:**
```python
# Add performance monitoring
import time

async def process_with_timing(message):
    start = time.time()
    try:
        result = await process_message(message)
        duration = time.time() - start
        logger.info(f"Processed in {duration:.2f}s")
        if duration > 5:
            logger.warning(f"Slow processing: {duration:.2f}s")
        return result
    except Exception as e:
        logger.error(f"Processing failed after {time.time() - start:.2f}s")
        raise
```

### 4. Memory Issues

#### Symptom: Out of Memory Errors
```
ERROR: Worker killed due to OOM
```

**Solutions:**

1. **Monitor memory usage:**
```python
import psutil
import os

def log_memory_usage():
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    logger.info(f"Memory usage: {mem_info.rss / 1024 / 1024:.2f} MB")
```

2. **Implement streaming for large files:**
```python
async def stream_large_file(file_id: str):
    """Stream file content instead of loading into memory."""
    async for chunk in client.stream_file(file_id, chunk_size=1024*1024):
        await process_chunk(chunk)
```

3. **Increase memory limits:**
```yaml
# In Kubernetes deployment
resources:
  limits:
    memory: "2Gi"
  requests:
    memory: "1Gi"
```

### 5. Dead Letter Queue Issues

#### Symptom: Messages in DLQ
```
Messages accumulating in integration.trigger.dlq topic
```

**Investigation:**
```bash
# Inspect DLQ messages
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic integration.trigger.dlq \
  --from-beginning \
  --max-messages 10
```

**Common Causes:**
1. Malformed messages
2. Missing required fields
3. Permanent API failures

**Solutions:**

1. **Analyze DLQ messages:**
```python
async def analyze_dlq_message(message):
    """Diagnose why message failed."""
    try:
        # Try to process
        await process_message(message)
    except ValidationError as e:
        logger.error(f"Validation failed: {e}")
        # Fix: Update message schema
    except ApiError as e:
        logger.error(f"API error: {e}")
        # Fix: Check credentials/permissions
    except Exception as e:
        logger.error(f"Unknown error: {e}")
        # Fix: Debug specific issue
```

2. **Reprocess DLQ messages:**
```python
async def reprocess_dlq():
    """Attempt to reprocess DLQ messages."""
    consumer = create_consumer("integration.trigger.dlq")
    producer = create_producer()

    async for message in consumer:
        try:
            # Fix known issues
            fixed_message = fix_message(message.value)

            # Send back to main topic
            await producer.send(
                "integration.trigger",
                key=message.key,
                value=fixed_message
            )

            await consumer.commit()
        except Exception as e:
            logger.error(f"Cannot fix message: {e}")
```

### 6. S3 Upload Failures

#### Symptom: "Failed to upload to S3"
```
ERROR: Failed to upload payload to S3: Access Denied
```

**Solutions:**

1. **Check S3 credentials:**
```bash
# Test S3 access
aws s3 ls s3://${S3_BUCKET_NAME}/ --profile clustera
```

2. **Verify bucket permissions:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"AWS": "arn:aws:iam::ACCOUNT:role/google-drive-worker"},
      "Action": ["s3:PutObject", "s3:GetObject"],
      "Resource": "arn:aws:s3:::clustera-payloads/*"
    }
  ]
}
```

3. **Check payload size:**
```python
# Ensure payload is within limits
MAX_S3_OBJECT_SIZE = 5 * 1024 * 1024 * 1024  # 5GB

if len(payload) > MAX_S3_OBJECT_SIZE:
    # Use multipart upload
    await s3_client.upload_multipart(payload)
```

## Performance Tuning

### 1. Optimize Kafka Consumer

```bash
# Environment variables for tuning
export KAFKA_MAX_POLL_RECORDS=500              # Fetch more records per poll
export KAFKA_MAX_POLL_INTERVAL_MS=600000       # 10 minutes for processing
export KAFKA_SESSION_TIMEOUT_MS=30000          # 30 seconds heartbeat
export KAFKA_FETCH_MIN_BYTES=1024              # Minimum data to fetch
export KAFKA_FETCH_MAX_WAIT_MS=500            # Max wait for data
```

### 2. Connection Pool Optimization

```python
# Configure connection pools
connector = aiohttp.TCPConnector(
    limit=100,               # Total connection pool limit
    limit_per_host=30,      # Per-host connection limit
    ttl_dns_cache=300,      # DNS cache TTL
    enable_cleanup_closed=True
)
```

### 3. Caching Strategy

```python
# Implement intelligent caching
from functools import lru_cache
from cachetools import TTLCache

# Cache file metadata
metadata_cache = TTLCache(maxsize=10000, ttl=3600)

@lru_cache(maxsize=1000)
async def get_cached_metadata(file_id: str):
    if file_id in metadata_cache:
        return metadata_cache[file_id]

    metadata = await fetch_metadata(file_id)
    metadata_cache[file_id] = metadata
    return metadata
```

## Debugging Tools

### 1. Enable Debug Logging

```bash
# Set log level
export LOG_LEVEL=DEBUG
export GOOGLE_API_DEBUG=true

# Enable specific loggers
export PYTHONASYNCIODEBUG=1
```

### 2. Kafka Message Inspection

```bash
# Inspect specific partition
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic integration.trigger \
  --partition 0 \
  --offset 100 \
  --max-messages 10
```

### 3. Health Check Endpoints

```python
# Add health check endpoint
@app.get("/health")
async def health_check():
    checks = {
        "kafka": await check_kafka_connection(),
        "google_api": await check_google_api(),
        "s3": await check_s3_access(),
        "memory": check_memory_usage()
    }

    status = "healthy" if all(checks.values()) else "unhealthy"
    return {"status": status, "checks": checks}
```

### 4. Metrics Collection

```python
# Prometheus metrics
from prometheus_client import Counter, Histogram, Gauge

messages_processed = Counter('messages_processed_total', 'Total messages processed')
processing_duration = Histogram('processing_duration_seconds', 'Message processing duration')
consumer_lag = Gauge('consumer_lag', 'Current consumer lag')

@processing_duration.time()
async def process_with_metrics(message):
    result = await process_message(message)
    messages_processed.inc()
    return result
```

## Emergency Procedures

### 1. Worker Completely Stuck

```bash
# 1. Check worker status
kubectl get pods -l app=google-drive-worker

# 2. Check logs
kubectl logs -f google-drive-worker-xxx --tail=100

# 3. Restart if needed
kubectl rollout restart deployment/google-drive-worker

# 4. Scale to zero and back
kubectl scale deployment google-drive-worker --replicas=0
kubectl scale deployment google-drive-worker --replicas=3
```

### 2. Kafka Topic Issues

```bash
# Reset consumer group offset
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group google-drive-worker \
  --topic integration.trigger \
  --reset-offsets --to-earliest \
  --execute
```

### 3. Clear Poison Messages

```python
async def skip_poison_message():
    """Skip a problematic message."""
    consumer = create_consumer()

    async for message in consumer:
        try:
            await process_message(message.value)
        except PoisonMessageError:
            logger.error(f"Skipping poison message: {message.offset}")
            # Commit offset to skip
            await consumer.commit()
            continue
```

## Monitoring Checklist

- [ ] Consumer lag < 1000 messages
- [ ] Memory usage < 80% of limit
- [ ] API rate limit usage < 80%
- [ ] DLQ messages < 100
- [ ] Error rate < 1%
- [ ] P95 processing time < 5 seconds
- [ ] All health checks passing

## Contact Support

If issues persist after following this guide:

1. Collect diagnostic information:
```bash
# Generate diagnostic bundle
kubectl exec google-drive-worker-xxx -- python -m diagnostics.collect
```

2. Check recent changes:
```bash
git log --oneline -10
```

3. Contact Platform Team:
- Slack: #integrations-platform
- Email: platform@clustera.ai
- Include: Error logs, diagnostic bundle, reproduction steps