# DCM/Campaign Manager 360 OAuth Setup Guide

## Problem

You need automated DCM data extraction but:
- ✅ You have C360 username/password access
- ❌ 2FA is required for login
- ❌ Service account can't access client accounts
- ✅ A colleague can access data using OAuth without 2FA each time

## Solution: OAuth with Refresh Token

Use OAuth **once** (with 2FA), get a refresh token, then automated forever.

---

## Step 1: Create OAuth Client (One-Time)

1. **Go to Google Cloud Console:**
   - https://console.cloud.google.com/apis/credentials?project=bi-data-391216

2. **Create OAuth 2.0 Client ID:**
   - Click "+ CREATE CREDENTIALS" → "OAuth 2.0 Client ID"
   - Application type: **Desktop app**
   - Name: `DCM Orchestrator`
   - Click "CREATE"

3. **Download credentials:**
   - Click the download icon (⬇️) next to your new client
   - Save as `c:\orchestrator\client_secrets.json`

---

## Step 2: Run OAuth Setup (Requires 2FA - One Time Only)

**Who should run this:** Person with C360 username + 2FA device

**What happens:**
1. Browser opens for Google login
2. User logs in with C360 credentials
3. User enters 2FA code
4. User approves access
5. Refresh token is saved

**Commands:**

```cmd
cd c:\orchestrator

REM Install required package if needed
pip install google-auth-oauthlib

REM Run setup script
python setup_dcm_oauth.py
```

**Expected output:**

```
================================================================================
DCM OAuth Setup - One-Time Configuration
================================================================================

This script will:
1. Open your browser for Google login
2. Ask you to enter your 2FA code
3. Save a refresh token for automated access

⚠️  You must use the C360 account credentials
⚠️  Have your 2FA device ready

Press Enter to continue...

🌐 Opening browser for authentication...

[Browser opens, user logs in + 2FA]

✅ OAuth successful!

📝 Add these to your .env file:

DCM_CLIENT_ID=123456789-abc123.apps.googleusercontent.com
DCM_CLIENT_SECRET=GOCSPX-xyz789...
DCM_REFRESH_TOKEN=1//0abc123xyz...

💾 Credentials also saved to: dcm_credentials.json

================================================================================
✅ Setup Complete!
================================================================================
```

---

## Step 3: Add Credentials to .env

Add the credentials from Step 2 to `c:\orchestrator\.env`:

```bash
# DCM/Campaign Manager 360 OAuth
DCM_CLIENT_ID=123456789-abc123.apps.googleusercontent.com
DCM_CLIENT_SECRET=GOCSPX-xyz789...
DCM_REFRESH_TOKEN=1//0abc123xyz...
```

---

## Step 4: Update DCM Extractor Code

### Option A: Quick Fix (Minimal Code Change)

Update `plugins/dcm_extractor.py` DCMClient.initialize() method:

```python
def initialize(self) -> bool:
    """Initialize the DCM API service"""
    try:
        # Check if using OAuth (refresh token) or service account
        refresh_token = os.getenv('DCM_REFRESH_TOKEN')

        if refresh_token:
            # OAuth authentication (for C360 access)
            from google.oauth2.credentials import Credentials

            logger.info("Using OAuth authentication for DCM API")

            self.credentials = Credentials(
                None,  # No access token yet (will be refreshed)
                refresh_token=refresh_token,
                token_uri='https://oauth2.googleapis.com/token',
                client_id=os.getenv('DCM_CLIENT_ID'),
                client_secret=os.getenv('DCM_CLIENT_SECRET'),
                scopes=DCM_API_SCOPES
            )

        elif self.credentials_path:
            # Service account authentication (legacy)
            logger.info("Using service account authentication for DCM API")

            if not os.path.exists(self.credentials_path):
                logger.error(f"Service account key file not found: {self.credentials_path}")
                return False

            self.credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=DCM_API_SCOPES
            )
        else:
            logger.error("No DCM credentials found (need refresh token or service account)")
            return False

        # Build service
        self.service = build('dfareporting', DCM_API_VERSION, credentials=self.credentials)

        logger.info(f"DCM API {DCM_API_VERSION} initialized successfully")
        return True

    except Exception as e:
        logger.error(f"Failed to initialize DCM API: {e}")
        return False
```

### Option B: Alternative - Credential Helper Function

Add this function to `plugins/dcm_extractor.py`:

```python
def _get_dcm_credentials():
    """
    Get DCM credentials - tries OAuth first, falls back to service account.

    Returns:
        google.auth.credentials.Credentials object
    """
    # Try OAuth first (preferred for C360 access)
    refresh_token = os.getenv('DCM_REFRESH_TOKEN')
    if refresh_token:
        from google.oauth2.credentials import Credentials

        logger.info("Authenticating with OAuth (C360 access)")
        return Credentials(
            None,
            refresh_token=refresh_token,
            token_uri='https://oauth2.googleapis.com/token',
            client_id=os.getenv('DCM_CLIENT_ID'),
            client_secret=os.getenv('DCM_CLIENT_SECRET'),
            scopes=DCM_API_SCOPES
        )

    # Fall back to service account
    sa_path = os.getenv('DCM_SERVICE_ACCOUNT_KEY')
    if sa_path and os.path.exists(sa_path):
        logger.info("Authenticating with service account")
        return service_account.Credentials.from_service_account_file(
            sa_path,
            scopes=DCM_API_SCOPES
        )

    raise ValueError("No DCM credentials configured")
```

Then update `DCMClient.initialize()`:

```python
def initialize(self) -> bool:
    """Initialize the DCM API service"""
    try:
        self.credentials = _get_dcm_credentials()
        self.service = build('dfareporting', DCM_API_VERSION, credentials=self.credentials)
        logger.info(f"DCM API {DCM_API_VERSION} initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize DCM API: {e}")
        return False
```

---

## Step 5: Test the Setup

```cmd
REM Test DCM extraction with OAuth
python orchestrate.py --source dcm --list-sites

REM Should show all C360 accounts without requiring 2FA!
```

---

## How It Works After Setup

```
┌─────────────────┐
│ Orchestrator    │
│                 │
│ 1. Reads        │
│    refresh      │
│    token from   │
│    .env         │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Google OAuth    │
│                 │
│ 2. Exchanges    │
│    refresh      │
│    token for    │
│    access token │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ DCM API         │
│                 │
│ 3. Returns data │
│    for all C360 │
│    accounts     │
└─────────────────┘

NO 2FA REQUIRED! ✅
```

**Key points:**
- Refresh token never expires (unless explicitly revoked)
- Access tokens expire after 1 hour but are auto-renewed
- No manual intervention needed
- Works exactly like your other ETL jobs

---

## Troubleshooting

### "Invalid refresh token"

**Cause:** Token was revoked or expired

**Fix:** Run `setup_dcm_oauth.py` again

### "No refresh token received"

**Cause:** App was previously authorized

**Fix:**
1. Go to: https://myaccount.google.com/permissions
2. Remove "DCM Orchestrator" access
3. Run setup again

### "Access denied" / "Insufficient permissions"

**Cause:** OAuth client doesn't have DCM API enabled

**Fix:**
1. Go to: https://console.cloud.google.com/apis/library/dfareporting.googleapis.com
2. Enable Campaign Manager 360 API
3. Run setup again

### "Wrong account logged in"

**Cause:** Browser has multiple Google accounts

**Fix:**
- Use incognito mode when running setup
- Or log out of all other Google accounts first

---

## Security Best Practices

1. **Keep credentials secure:**
   ```bash
   # .env should NEVER be committed to git
   # Add to .gitignore:
   .env
   client_secrets.json
   dcm_credentials.json
   ```

2. **Rotate refresh tokens periodically:**
   - Run setup every 6-12 months
   - Revoke old tokens at: https://myaccount.google.com/permissions

3. **Use least-privilege scopes:**
   - Current scope: `dfareporting` (read-only for most operations)
   - Don't add unnecessary scopes

4. **Monitor access:**
   - Check Google account activity regularly
   - Review API usage in Cloud Console

---

## FAQ

**Q: Does the refresh token expire?**
A: No, refresh tokens don't expire unless explicitly revoked or the user changes their password.

**Q: Can multiple orchestrators use the same refresh token?**
A: Yes! You can copy the token to dev/prod/backup servers.

**Q: What if the C360 user leaves the company?**
A: You'll need to re-run setup with the new user's credentials.

**Q: Will this work for all 70+ client accounts?**
A: Yes! The C360 user has access to all client accounts, so the refresh token inherits that access.

**Q: How is this different from what my colleague is doing?**
A: They're probably doing the same thing - ask them to share their refresh token or help run the setup.

---

## Next Steps

1. ✅ Create OAuth client (Step 1)
2. ✅ Run setup script with C360 user + 2FA (Step 2)
3. ✅ Add credentials to .env (Step 3)
4. ✅ Update DCM extractor code (Step 4)
5. ✅ Test extraction (Step 5)
6. ✅ Add to cron for automated runs

**After setup:** No more manual intervention - runs like all your other ETL jobs!
