# Google OAuth 2.0 Setup Guide

## Overview

This guide provides step-by-step instructions for setting up OAuth 2.0 authentication for the Google Drive integration.

## Prerequisites

- Google Cloud Platform account
- Project created in Google Cloud Console
- Admin access to configure OAuth consent screen

## Step 1: Create Google Cloud Project

1. Navigate to [Google Cloud Console](https://console.cloud.google.com/)
2. Click on the project selector dropdown
3. Click "New Project"
4. Enter project details:
   - **Project Name**: `Clustera Integrations`
   - **Organization**: Select your organization
   - **Location**: Choose appropriate folder
5. Click "Create"

## Step 2: Enable Google Drive API

1. In the Google Cloud Console, navigate to "APIs & Services" > "Library"
2. Search for "Google Drive API"
3. Click on the Google Drive API result
4. Click "Enable"
5. Wait for the API to be enabled

## Step 3: Configure OAuth Consent Screen

### 3.1 Basic Information

1. Navigate to "APIs & Services" > "OAuth consent screen"
2. Select user type:
   - **Internal**: For G Suite organizations only
   - **External**: For any Google account (requires verification)
3. Click "Create"

### 3.2 App Information

Fill in the required fields:

- **App name**: `Clustera Google Drive Integration`
- **User support email**: `support@clustera.ai`
- **App logo**: Upload your application logo (optional)

### 3.3 App Domain

- **Application home page**: `https://clustera.ai`
- **Application privacy policy**: `https://clustera.ai/privacy`
- **Application terms of service**: `https://clustera.ai/terms`

### 3.4 Authorized Domains

Add your domains:
- `clustera.ai`
- `app.clustera.ai`

### 3.5 Developer Contact Information

Add email addresses for Google to contact about the application.

### 3.6 Scopes

Add the following OAuth scopes:

1. Click "Add or Remove Scopes"
2. Search and select:
   - `https://www.googleapis.com/auth/drive.readonly` - View files in Google Drive
   - `https://www.googleapis.com/auth/drive.metadata.readonly` - View metadata for files

3. Click "Update"
4. Click "Save and Continue"

## Step 4: Create OAuth 2.0 Credentials

### 4.1 Create Credentials

1. Navigate to "APIs & Services" > "Credentials"
2. Click "Create Credentials" > "OAuth client ID"
3. Select application type: **Web application**
4. Configure the OAuth client:
   - **Name**: `Clustera Google Drive Integration`

### 4.2 Configure Redirect URIs

Add authorized redirect URIs:

**Production:**
```
https://app.clustera.ai/oauth/callback
https://app.clustera.ai/integrations/google-drive/callback
```

**Development:**
```
http://localhost:3000/oauth/callback
http://localhost:8000/oauth/callback
```

### 4.3 Save Credentials

1. Click "Create"
2. Copy the following values:
   - **Client ID**: `<your-client-id>.apps.googleusercontent.com`
   - **Client Secret**: `<your-client-secret>`
3. Store these securely in your secrets management system

## Step 5: Obtain Refresh Token

### 5.1 Using OAuth Playground (Development)

1. Navigate to [OAuth 2.0 Playground](https://developers.google.com/oauthplayground/)
2. Click the gear icon (Settings)
3. Check "Use your own OAuth credentials"
4. Enter your Client ID and Client Secret
5. In Step 1, select scopes:
   - `https://www.googleapis.com/auth/drive.readonly`
   - `https://www.googleapis.com/auth/drive.metadata.readonly`
6. Click "Authorize APIs"
7. Sign in with your Google account
8. Grant permissions
9. In Step 2, click "Exchange authorization code for tokens"
10. Copy the refresh token

### 5.2 Programmatic Flow

```python
import asyncio
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import Flow

async def get_refresh_token():
    flow = Flow.from_client_config(
        {
            "web": {
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": ["http://localhost:8080"]
            }
        },
        scopes=[
            "https://www.googleapis.com/auth/drive.readonly",
            "https://www.googleapis.com/auth/drive.metadata.readonly"
        ]
    )

    flow.redirect_uri = "http://localhost:8080"

    # Generate authorization URL
    auth_url, _ = flow.authorization_url(
        access_type='offline',
        include_granted_scopes='true',
        prompt='consent'
    )

    print(f"Visit this URL: {auth_url}")

    # After user authorizes, get the code from redirect
    code = input("Enter the authorization code: ")

    # Exchange code for tokens
    flow.fetch_token(code=code)

    # Get refresh token
    refresh_token = flow.credentials.refresh_token
    print(f"Refresh Token: {refresh_token}")

    return refresh_token
```

## Step 6: Token Storage Best Practices

### 6.1 Never Store in Code

```python
# BAD - Never do this
REFRESH_TOKEN = "1//0gLu8xF..."

# GOOD - Use environment variables or secrets management
import os
refresh_token = os.environ.get("GOOGLE_REFRESH_TOKEN")
```

### 6.2 Use Secrets Management

**AWS Secrets Manager:**
```python
import boto3
import json

def get_refresh_token_from_secrets():
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId='google-drive/refresh-token')
    secrets = json.loads(response['SecretString'])
    return secrets['refresh_token']
```

**Kubernetes Secrets:**
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: google-drive-oauth
type: Opaque
stringData:
  refresh_token: "<base64-encoded-refresh-token>"
```

### 6.3 Token Rotation

Implement automatic token rotation:

```python
async def refresh_access_token(refresh_token: str) -> str:
    """Refresh the access token using the refresh token."""
    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://oauth2.googleapis.com/token",
            data={
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "refresh_token": refresh_token,
                "grant_type": "refresh_token"
            }
        ) as response:
            data = await response.json()
            return data["access_token"]
```

## Step 7: Security Considerations

### 7.1 Scope Limitations

Only request the minimum necessary scopes:
- ✅ `drive.readonly` - Read-only access
- ❌ `drive` - Full access (avoid unless necessary)

### 7.2 Token Security

1. **Encrypt at Rest**: Always encrypt stored tokens
2. **Encrypt in Transit**: Use TLS for all token transmission
3. **Access Control**: Limit who can access tokens
4. **Audit Logging**: Log all token usage
5. **Token Expiry**: Implement token rotation policies

### 7.3 Application Verification

For production External apps:
1. Submit for verification when ready
2. Provide all required documentation
3. Complete security assessment if required
4. Wait for Google's approval

## Step 8: Testing OAuth Flow

### 8.1 Test Authorization

```bash
# Test authorization URL generation
curl "https://accounts.google.com/o/oauth2/v2/auth?client_id=YOUR_CLIENT_ID&redirect_uri=http://localhost:8080&response_type=code&scope=https://www.googleapis.com/auth/drive.readonly&access_type=offline&prompt=consent"
```

### 8.2 Test Token Exchange

```bash
# Exchange authorization code for tokens
curl -X POST "https://oauth2.googleapis.com/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "code=AUTH_CODE&client_id=YOUR_CLIENT_ID&client_secret=YOUR_CLIENT_SECRET&redirect_uri=http://localhost:8080&grant_type=authorization_code"
```

### 8.3 Test Token Refresh

```bash
# Refresh access token
curl -X POST "https://oauth2.googleapis.com/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "client_id=YOUR_CLIENT_ID&client_secret=YOUR_CLIENT_SECRET&refresh_token=YOUR_REFRESH_TOKEN&grant_type=refresh_token"
```

## Step 9: Production Deployment

### 9.1 Environment Configuration

```bash
# Production environment variables
export GOOGLE_CLIENT_ID="your-client-id.apps.googleusercontent.com"
export GOOGLE_CLIENT_SECRET="your-client-secret"
export GOOGLE_REDIRECT_URI="https://app.clustera.ai/oauth/callback"
```

### 9.2 Health Checks

Implement health checks for OAuth:

```python
async def check_oauth_health():
    """Verify OAuth configuration is valid."""
    try:
        # Try to refresh a test token
        token = await refresh_access_token(test_refresh_token)
        return {"status": "healthy", "oauth": "configured"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}
```

## Troubleshooting

### Common Issues

#### 1. Invalid Client Error
```
Error: invalid_client
```
**Solution**: Verify client_id and client_secret are correct

#### 2. Redirect URI Mismatch
```
Error: redirect_uri_mismatch
```
**Solution**: Ensure redirect URI exactly matches configured URI (including trailing slashes)

#### 3. Access Blocked
```
Error: access_blocked
```
**Solution**: App needs verification for external users

#### 4. Invalid Grant
```
Error: invalid_grant
```
**Solution**: Refresh token may be expired or revoked, need to reauthorize

#### 5. Rate Limiting
```
Error: rate_limit_exceeded
```
**Solution**: Implement exponential backoff for token refresh

### Debug Logging

Enable debug logging for OAuth issues:

```python
import logging
logging.getLogger('google.auth').setLevel(logging.DEBUG)
logging.getLogger('google_auth_oauthlib').setLevel(logging.DEBUG)
```

## Resources

- [Google OAuth 2.0 Documentation](https://developers.google.com/identity/protocols/oauth2)
- [OAuth 2.0 Playground](https://developers.google.com/oauthplayground/)
- [Google Cloud Console](https://console.cloud.google.com/)
- [OAuth 2.0 Scopes](https://developers.google.com/identity/protocols/oauth2/scopes)
- [Best Practices](https://developers.google.com/identity/protocols/oauth2/best-practices)