# Google Drive Push Notifications Setup Guide

This guide explains how to configure Google Drive push notifications (webhooks) for real-time file change detection in the Clustera Integrations platform.

## Overview

Google Drive supports push notifications through Cloud Pub/Sub channels. When files change in a monitored Drive, Google sends a webhook notification to your configured endpoint, allowing near real-time data synchronization.

## Architecture

```
Google Drive --> Cloud Pub/Sub --> Webhook Proxy --> Kafka (webhook.raw) --> Google Drive Worker
```

1. **Google Drive**: Detects changes and sends notifications
2. **Cloud Pub/Sub**: Google's messaging service that delivers notifications
3. **Webhook Proxy**: Receives and validates webhook requests
4. **Kafka Topic**: `webhook.raw` stores raw webhook messages
5. **Google Drive Worker**: Processes webhooks and fetches actual file data

## Prerequisites

Before setting up push notifications, ensure you have:

1. **Google Cloud Project** with the following APIs enabled:
   - Google Drive API
   - Google Cloud Pub/Sub API (for push notifications)

2. **Service Account** or OAuth 2.0 credentials with appropriate permissions:
   - `drive.readonly` scope (minimum)
   - `drive.file` or `drive` scope for broader access

3. **Webhook Proxy** deployed and accessible from the internet with:
   - Valid SSL certificate (Google requires HTTPS)
   - Public domain or IP address
   - Proper firewall rules to allow Google's requests

## Setup Process

### Step 1: Enable Required APIs in Google Cloud Console

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Select your project
3. Navigate to "APIs & Services" > "Library"
4. Enable the following APIs:
   - **Google Drive API**
   - **Cloud Pub/Sub API** (required for push notifications)

### Step 2: Configure OAuth 2.0 or Service Account

#### Option A: OAuth 2.0 (Recommended for user-specific monitoring)
```python
# Required scopes for push notifications
SCOPES = [
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/drive.metadata.readonly'
]
```

#### Option B: Service Account (Recommended for domain-wide monitoring)
1. Create a service account in Google Cloud Console
2. Download the JSON key file
3. Grant appropriate Drive permissions to the service account email

### Step 3: Configure Webhook Endpoint

The webhook endpoint must be publicly accessible and use HTTPS:

```yaml
# Example webhook proxy configuration
webhook_proxy:
  endpoint: https://webhooks.your-domain.com/google-drive
  port: 443
  ssl_cert: /path/to/cert.pem
  ssl_key: /path/to/key.pem
```

### Step 4: Create Push Notification Channel

Use the Google Drive API to create a notification channel:

```python
import uuid
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# Initialize the Drive API client
creds = Credentials.from_authorized_user_file('token.json', SCOPES)
service = build('drive', 'v3', credentials=creds)

# Create a unique channel ID
channel_id = str(uuid.uuid4())
channel_token = "your-secret-token"  # Used for webhook validation

# Create the notification channel
channel = {
    'id': channel_id,
    'type': 'web_hook',
    'address': 'https://webhooks.your-domain.com/google-drive',
    'token': channel_token,  # Optional: for additional security
    'expiration': int((datetime.now() + timedelta(hours=24)).timestamp() * 1000)
}

# Set up watch on all changes
response = service.changes().watch(
    pageToken=start_page_token,  # Get this from changes().getStartPageToken()
    body=channel,
    supportsAllDrives=True,  # Include shared drives
    includeItemsFromAllDrives=True
).execute()

print(f"Channel created: {response}")
```

### Step 5: Handle Channel Expiration

Google Drive notification channels expire after a maximum of 24 hours. You must renew them before expiration:

```python
def renew_notification_channel(service, old_channel_id):
    """Renew an expiring notification channel."""

    # Stop the old channel
    try:
        service.channels().stop(body={'id': old_channel_id}).execute()
    except Exception as e:
        print(f"Failed to stop old channel: {e}")

    # Create a new channel
    new_channel = create_notification_channel(service)
    return new_channel
```

## Webhook Message Format

When Google Drive detects changes, it sends a POST request to your webhook endpoint:

### Headers
```
X-Goog-Channel-ID: channel_id_123
X-Goog-Channel-Token: your-secret-token
X-Goog-Resource-ID: resource_id_xyz
X-Goog-Resource-URI: https://www.googleapis.com/drive/v3/changes?pageToken=abc123
X-Goog-Resource-State: change
X-Goog-Message-Number: 1
```

### Body (Example)
```json
{
  "kind": "drive#change",
  "id": "channel_id_123",
  "resourceId": "resource_id_xyz",
  "resourceUri": "https://www.googleapis.com/drive/v3/changes?pageToken=abc123",
  "expiration": "1234567890000"
}
```

**Important**: The webhook notification does NOT contain the actual file data. It only notifies that changes have occurred. The worker must use the `pageToken` from `resourceUri` to fetch the actual changes via the Changes API.

## Webhook Processing Flow

1. **Webhook Reception**: Webhook proxy receives the notification
2. **Validation**: Proxy validates the webhook signature/token
3. **Kafka Production**: Proxy produces to `webhook.raw` topic
4. **Worker Processing**: Google Drive worker:
   - Extracts `pageToken` from `resourceUri`
   - Calls Changes API with the token
   - Fetches actual file data for each change
   - Transforms and produces to `ingestion.data`

## Security Considerations

### 1. Webhook Validation

Always validate incoming webhooks to ensure they're from Google:

```python
def validate_webhook(request):
    """Validate that the webhook is from Google."""

    # Check for required headers
    required_headers = [
        'X-Goog-Channel-ID',
        'X-Goog-Resource-ID',
        'X-Goog-Resource-URI'
    ]

    for header in required_headers:
        if header not in request.headers:
            return False

    # Optionally validate channel token
    expected_token = get_expected_token(request.headers['X-Goog-Channel-ID'])
    if request.headers.get('X-Goog-Channel-Token') != expected_token:
        return False

    return True
```

### 2. SSL/TLS Requirements

Google requires HTTPS endpoints with valid SSL certificates:
- Use certificates from trusted Certificate Authorities
- Ensure TLS 1.2 or higher
- Configure proper cipher suites

### 3. Rate Limiting

Implement rate limiting to prevent webhook flooding:
- Track requests per channel ID
- Implement exponential backoff for API calls
- Set reasonable limits based on expected change frequency

## Monitoring and Troubleshooting

### Common Issues and Solutions

#### 1. Webhook Not Receiving Notifications

**Symptoms**: No webhook calls after channel creation

**Solutions**:
- Verify the endpoint is publicly accessible: `curl https://your-webhook-url`
- Check SSL certificate validity: `openssl s_client -connect your-domain:443`
- Ensure firewall allows Google's IP ranges
- Verify the channel hasn't expired (24-hour limit)

#### 2. 401 Unauthorized Errors

**Symptoms**: Changes API returns 401

**Solutions**:
- Refresh the access token
- Verify OAuth scopes include drive.readonly
- Check token expiration

#### 3. Missing Changes

**Symptoms**: Some file changes not detected

**Solutions**:
- Ensure `supportsAllDrives=True` for shared drives
- Check if files are in trash (excluded by default)
- Verify user has access to the files

#### 4. Channel Expiration

**Symptoms**: Notifications stop after ~24 hours

**Solutions**:
- Implement automatic channel renewal before expiration
- Monitor channel expiration times
- Set up alerts for failed renewals

### Monitoring Metrics

Track these metrics for operational health:

1. **Webhook Reception Rate**: webhooks received per minute
2. **Channel Lifetime**: time until expiration
3. **API Call Latency**: time to fetch changes
4. **Error Rate**: failed webhook processing
5. **Lag Time**: delay between file change and processing

### Debug Logging

Enable detailed logging for troubleshooting:

```python
import structlog

logger = structlog.get_logger()

# Log webhook reception
logger.info(
    "webhook_received",
    channel_id=headers.get('X-Goog-Channel-ID'),
    resource_uri=headers.get('X-Goog-Resource-URI'),
    message_number=headers.get('X-Goog-Message-Number')
)

# Log change processing
logger.info(
    "processing_changes",
    page_token=page_token,
    change_count=len(changes),
    has_more=bool(next_page_token)
)
```

## Best Practices

1. **Channel Management**
   - Create channels per user/connection for isolation
   - Implement automatic renewal 1 hour before expiration
   - Store channel metadata for tracking

2. **Error Handling**
   - Implement exponential backoff for API errors
   - Use dead letter queues for failed messages
   - Set up alerts for critical failures

3. **Performance Optimization**
   - Process only required file types
   - Use field masks to reduce API response size
   - Implement caching for frequently accessed metadata

4. **Testing**
   - Use ngrok for local webhook testing
   - Implement health check endpoints
   - Create test channels with limited scope

## Example Implementation

Here's a complete example of setting up and managing push notifications:

```python
import asyncio
import uuid
from datetime import datetime, timedelta
from typing import Optional

class GoogleDrivePushNotificationManager:
    def __init__(self, drive_service, webhook_url: str):
        self.service = drive_service
        self.webhook_url = webhook_url
        self.channels = {}  # Track active channels

    async def setup_push_notifications(self, connection_id: str) -> dict:
        """Set up push notifications for a connection."""

        # Get starting page token
        start_token = self._get_start_page_token()

        # Create notification channel
        channel = self._create_channel(connection_id)

        # Start watching for changes
        watch_response = self.service.changes().watch(
            pageToken=start_token,
            body=channel,
            supportsAllDrives=True
        ).execute()

        # Store channel info
        self.channels[connection_id] = {
            'channel_id': channel['id'],
            'expiration': channel['expiration'],
            'page_token': start_token
        }

        # Schedule renewal
        asyncio.create_task(
            self._schedule_renewal(connection_id, channel['expiration'])
        )

        return watch_response

    def _create_channel(self, connection_id: str) -> dict:
        """Create a notification channel configuration."""

        channel_id = f"{connection_id}_{uuid.uuid4()}"
        expiration = int((datetime.now() + timedelta(hours=23)).timestamp() * 1000)

        return {
            'id': channel_id,
            'type': 'web_hook',
            'address': self.webhook_url,
            'token': self._generate_token(connection_id),
            'expiration': expiration
        }

    async def _schedule_renewal(self, connection_id: str, expiration: int):
        """Schedule channel renewal before expiration."""

        # Renew 1 hour before expiration
        expire_time = datetime.fromtimestamp(expiration / 1000)
        renew_time = expire_time - timedelta(hours=1)
        sleep_seconds = (renew_time - datetime.now()).total_seconds()

        if sleep_seconds > 0:
            await asyncio.sleep(sleep_seconds)
            await self.renew_channel(connection_id)

    async def renew_channel(self, connection_id: str):
        """Renew an expiring channel."""

        old_channel = self.channels.get(connection_id)
        if not old_channel:
            return

        # Stop old channel
        try:
            self.service.channels().stop(
                body={'id': old_channel['channel_id']}
            ).execute()
        except Exception as e:
            print(f"Failed to stop old channel: {e}")

        # Create new channel
        await self.setup_push_notifications(connection_id)

    def _get_start_page_token(self) -> str:
        """Get the starting page token for changes."""

        response = self.service.changes().getStartPageToken(
            supportsAllDrives=True
        ).execute()

        return response.get('startPageToken')

    def _generate_token(self, connection_id: str) -> str:
        """Generate a secure token for webhook validation."""
        import secrets
        return f"{connection_id}_{secrets.token_urlsafe(32)}"
```

## Additional Resources

- [Google Drive Push Notifications Guide](https://developers.google.com/drive/api/guides/push)
- [Cloud Pub/Sub Documentation](https://cloud.google.com/pubsub/docs)
- [Drive API Changes Reference](https://developers.google.com/drive/api/reference/rest/v3/changes)
- [Webhook Security Best Practices](https://cloud.google.com/pubsub/docs/push#security)

## Support

For issues or questions about push notifications:
1. Check the [Troubleshooting](#monitoring-and-troubleshooting) section
2. Review worker logs for detailed error messages
3. Contact the platform team in #integrations-platform (Slack)