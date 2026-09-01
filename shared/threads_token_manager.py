#!/usr/bin/env python3
"""Threads OAuth Token Manager

Handles the complete Threads token lifecycle:
  1. Initial authorization (browser-based OAuth flow)
  2. Token refresh (extends 60-day expiry)
  3. .env file update (writes new token in-place)

Usage:
  # First time — opens browser for OAuth authorization:
  python shared/threads_token_manager.py --authorize

  # Refresh existing token (run periodically via cron/scheduler):
  python shared/threads_token_manager.py --refresh

  # Check token status (days until expiry):
  python shared/threads_token_manager.py --status

  # Force refresh even if not close to expiry:
  python shared/threads_token_manager.py --refresh --force

Scheduling (recommended: every 50 days):
  # Linux cron:
  0 6 */50 * * cd /path/to/orchestrator && python shared/threads_token_manager.py --refresh

  # Windows Task Scheduler:
  schtasks /create /tn "Threads Token Refresh" /tr "python c:\\orchestrator\\shared\\threads_token_manager.py --refresh" /sc daily /mo 50
"""

import os
import sys
import re
import json
import ssl
import time
import logging
import argparse
import tempfile
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from pathlib import Path
from datetime import datetime, timezone

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Threads OAuth endpoints
THREADS_AUTH_URL = "https://threads.net/oauth/authorize"
THREADS_TOKEN_URL = "https://graph.threads.net/oauth/access_token"
THREADS_EXCHANGE_URL = "https://graph.threads.net/access_token"
THREADS_REFRESH_URL = "https://graph.threads.net/refresh_access_token"
THREADS_API_BASE = "https://graph.threads.net/v1.0"

# Local callback server for OAuth
REDIRECT_HOST = "localhost"
REDIRECT_PORT = 8899
REDIRECT_URI_LOCAL = f"https://{REDIRECT_HOST}:{REDIRECT_PORT}/callback"

# Required scopes for the pipeline
SCOPES = "threads_basic,threads_manage_insights,threads_read_replies"

# Path to .env file
ENV_FILE = PROJECT_ROOT / ".env"

# Token metadata file (tracks expiry date) — single-account legacy
TOKEN_META_FILE = PROJECT_ROOT / ".threads_token_meta.json"

# Multi-account token store
TOKENS_FILE = PROJECT_ROOT / ".threads_tokens.json"


def load_env_vars():
    """Load environment variables from .env file."""
    try:
        from dotenv import load_dotenv
        load_dotenv(ENV_FILE)
    except ImportError:
        # Manual fallback if python-dotenv not available
        if ENV_FILE.exists():
            with open(ENV_FILE, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, _, value = line.partition('=')
                        os.environ.setdefault(key.strip(), value.strip())


def get_app_credentials():
    """Get Threads App ID and Secret."""
    app_id = os.getenv('THREADS_APP_ID')
    app_secret = os.getenv('THREADS_APP_SECRET')

    if not app_id or not app_secret:
        logger.error(
            "THREADS_APP_ID and THREADS_APP_SECRET must be set in .env\n"
            "Find them at: https://developers.facebook.com/apps -> Your App -> Settings -> Basic"
        )
        sys.exit(1)

    return app_id, app_secret


def update_env_file(key: str, value: str):
    """Update or add a key=value pair in the .env file, preserving all other content."""
    if not ENV_FILE.exists():
        with open(ENV_FILE, 'w') as f:
            f.write(f"{key}={value}\n")
        logger.info(f"Created {ENV_FILE} with {key}")
        return

    lines = ENV_FILE.read_text().splitlines(keepends=True)
    pattern = re.compile(rf'^{re.escape(key)}=.*', re.MULTILINE)

    found = False
    new_lines = []
    for line in lines:
        if re.match(rf'^{re.escape(key)}=', line.rstrip()):
            new_lines.append(f"{key}={value}\n")
            found = True
        else:
            new_lines.append(line)

    if not found:
        # Key doesn't exist yet — add it in a Threads section
        # Check if there's already a Threads section
        has_section = any('Threads' in line for line in new_lines if line.strip().startswith('#'))
        if not has_section:
            new_lines.append("\n# Threads API Credentials\n")
        new_lines.append(f"{key}={value}\n")

    ENV_FILE.write_text(''.join(new_lines))
    logger.info(f"Updated {key} in {ENV_FILE}")


def save_token_metadata(token: str, expires_in: int):
    """Save token metadata (creation time, expiry) for status checks."""
    meta = {
        'created_at': datetime.now(timezone.utc).isoformat(),
        'expires_in_seconds': expires_in,
        'expires_at': (datetime.now(timezone.utc).timestamp() + expires_in),
        'token_prefix': token[:20] + '...',
    }
    TOKEN_META_FILE.write_text(json.dumps(meta, indent=2))
    logger.info(f"Token metadata saved to {TOKEN_META_FILE}")


def load_token_store() -> dict:
    """Load the multi-account token store."""
    if TOKENS_FILE.exists():
        try:
            return json.loads(TOKENS_FILE.read_text())
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def save_account_token(username: str, user_id: str, access_token: str, expires_in: int):
    """Save or update a single account's token in the multi-account store."""
    store = load_token_store()
    store[username] = {
        'user_id': str(user_id),
        'access_token': access_token,
        'expires_at': datetime.now(timezone.utc).timestamp() + expires_in,
        'created_at': datetime.now(timezone.utc).isoformat(),
    }
    TOKENS_FILE.write_text(json.dumps(store, indent=2))
    logger.info(f"Token for @{username} saved to {TOKENS_FILE}")


def get_token_status():
    """Check current token status and days until expiry."""
    if not TOKEN_META_FILE.exists():
        return None

    meta = json.loads(TOKEN_META_FILE.read_text())
    expires_at = meta.get('expires_at', 0)
    now = datetime.now(timezone.utc).timestamp()
    remaining_seconds = expires_at - now
    remaining_days = remaining_seconds / 86400

    return {
        'created_at': meta.get('created_at'),
        'expires_at': datetime.fromtimestamp(expires_at, tz=timezone.utc).isoformat(),
        'remaining_days': round(remaining_days, 1),
        'is_expired': remaining_days <= 0,
        'needs_refresh': remaining_days <= 7,
    }


class OAuthCallbackHandler(BaseHTTPRequestHandler):
    """HTTP handler to capture the OAuth callback code."""

    authorization_code = None

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        if parsed.path == '/callback':
            code = params.get('code', [None])[0]
            error = params.get('error', [None])[0]

            if code:
                OAuthCallbackHandler.authorization_code = code
                self.send_response(200)
                self.send_header('Content-Type', 'text/html')
                self.end_headers()
                self.wfile.write(b"""
                <html><body style="font-family: sans-serif; text-align: center; padding: 50px;">
                <h1>Authorization Successful</h1>
                <p>You can close this window and return to the terminal.</p>
                </body></html>
                """)
            elif error:
                self.send_response(400)
                self.send_header('Content-Type', 'text/html')
                self.end_headers()
                error_desc = params.get('error_description', ['Unknown error'])[0]
                self.wfile.write(f"""
                <html><body style="font-family: sans-serif; text-align: center; padding: 50px;">
                <h1>Authorization Failed</h1>
                <p>{error}: {error_desc}</p>
                </body></html>
                """.encode())
            else:
                self.send_response(400)
                self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        # Suppress default HTTP server logs
        pass


def _generate_self_signed_cert(certfile: str, keyfile: str):
    """Generate a self-signed SSL certificate for localhost HTTPS callback."""
    try:
        from cryptography import x509
        from cryptography.x509.oid import NameOID
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        import datetime as dt

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(dt.datetime.now(dt.timezone.utc))
            .not_valid_after(dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1))
            .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False)
            .sign(key, hashes.SHA256())
        )
        with open(keyfile, 'wb') as f:
            f.write(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.TraditionalOpenSSL, serialization.NoEncryption()))
        with open(certfile, 'wb') as f:
            f.write(cert.public_bytes(serialization.Encoding.PEM))
        logger.debug("Self-signed SSL certificate generated for localhost")
    except ImportError:
        # Fallback: use openssl CLI if cryptography package not installed
        import subprocess
        subprocess.run([
            'openssl', 'req', '-x509', '-newkey', 'rsa:2048',
            '-keyout', keyfile, '-out', certfile,
            '-days', '1', '-nodes',
            '-subj', '/CN=localhost'
        ], capture_output=True, check=True)
        logger.debug("Self-signed SSL certificate generated via openssl CLI")


def _get_redirect_uri():
    """Return redirect URI: THREADS_REDIRECT_URI from .env if set, else localhost."""
    uri = os.getenv('THREADS_REDIRECT_URI', '').strip()
    return uri if uri else REDIRECT_URI_LOCAL


def authorize(app_id: str, app_secret: str, code: str = None):
    """Run the full OAuth authorization flow (browser-based or with pasted code).

    1. If --code given: use pasted code with THREADS_REDIRECT_URI (for remote callback).
    2. Else: open browser, start local HTTPS server to catch callback.
    3. Exchange auth code for short-lived token, then long-lived token.
    4. Update .env with the long-lived token and user ID.
    """
    redirect_uri = _get_redirect_uri()

    if not app_id or not app_id.strip():
        logger.error("THREADS_APP_ID is empty. Check your .env file.")
        sys.exit(1)
    logger.info("Using Threads app ID: %s", app_id.strip())

    # Build authorization URL (used for browser open or for --code instructions)
    auth_url = (
        f"{THREADS_AUTH_URL}"
        f"?client_id={app_id}"
        f"&redirect_uri={requests.utils.quote(redirect_uri, safe='')}"
        f"&scope={SCOPES}"
        f"&response_type=code"
    )

    if code:
        # Manual flow: user was redirected to remote URL and pasted the code
        logger.info("Using provided authorization code with redirect_uri=%s", redirect_uri)
        if not redirect_uri or redirect_uri == REDIRECT_URI_LOCAL:
            logger.warning(
                "When using --code, set THREADS_REDIRECT_URI in .env to the same URL "
                "configured in the Meta app (e.g. https://bionews.com/oauth/threads/callback)."
            )
    else:
        logger.info("Opening browser for Threads authorization...")
        logger.info(f"If the browser doesn't open, visit this URL manually:\n  {auth_url}\n")

        if redirect_uri == REDIRECT_URI_LOCAL:
            webbrowser.open(auth_url)
            # Start local HTTPS server to catch the callback (Meta requires HTTPS redirect URIs)
            logger.info(f"Waiting for OAuth callback on {redirect_uri}...")
            server = HTTPServer((REDIRECT_HOST, REDIRECT_PORT), OAuthCallbackHandler)
            server.timeout = 300  # 5 minute timeout

            # Generate a self-signed certificate for localhost HTTPS
            certfile = os.path.join(tempfile.gettempdir(), 'threads_oauth_cert.pem')
            keyfile = os.path.join(tempfile.gettempdir(), 'threads_oauth_key.pem')
            _generate_self_signed_cert(certfile, keyfile)
            ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ssl_ctx.load_cert_chain(certfile, keyfile)
            server.socket = ssl_ctx.wrap_socket(server.socket, server_side=True)

            while OAuthCallbackHandler.authorization_code is None:
                server.handle_request()

            code = OAuthCallbackHandler.authorization_code
            server.server_close()
        else:
            # Remote redirect: print URL and ask user to paste the code from the redirect URL
            webbrowser.open(auth_url)
            logger.info(
                "Redirect URI is remote (%s). After authorizing you will be sent there.",
                redirect_uri,
            )
            logger.info("Copy the 'code' parameter from the URL you are redirected to, then run:")
            logger.info('  python shared/threads_token_manager.py --authorize --code "PASTE_CODE_HERE"')
            sys.exit(0)

    if not code:
        logger.error("No authorization code received")
        sys.exit(1)

    logger.info("Authorization code received. Exchanging for token...")

    # Step 2: Exchange code for short-lived token
    response = requests.post(THREADS_TOKEN_URL, data={
        'client_id': app_id,
        'client_secret': app_secret,
        'grant_type': 'authorization_code',
        'redirect_uri': redirect_uri,
        'code': code,
    })

    if response.status_code != 200:
        logger.error(f"Token exchange failed: {response.status_code} {response.text}")
        sys.exit(1)

    token_data = response.json()
    short_lived_token = token_data.get('access_token')
    user_id = str(token_data.get('user_id', ''))

    if not short_lived_token:
        logger.error(f"No access_token in response: {token_data}")
        sys.exit(1)

    logger.info(f"Short-lived token obtained for user {user_id}")

    # Step 3: Exchange for long-lived token (~60 days)
    response = requests.get(THREADS_EXCHANGE_URL, params={
        'grant_type': 'th_exchange_token',
        'client_secret': app_secret,
        'access_token': short_lived_token,
    })

    if response.status_code != 200:
        logger.error(f"Long-lived token exchange failed: {response.status_code} {response.text}")
        sys.exit(1)

    ll_data = response.json()
    long_lived_token = ll_data.get('access_token')
    expires_in = ll_data.get('expires_in', 5184000)  # Default 60 days

    if not long_lived_token:
        logger.error(f"No access_token in long-lived response: {ll_data}")
        sys.exit(1)

    expires_days = expires_in // 86400
    logger.info(f"Long-lived token obtained. Expires in {expires_days} days.")

    # Verify the token works
    verify = requests.get(f"{THREADS_API_BASE}/me", params={
        'fields': 'id,username',
        'access_token': long_lived_token,
    })
    verified_username = ''
    if verify.status_code == 200:
        profile = verify.json()
        verified_username = profile.get('username', '')
        logger.info(f"Token verified: @{verified_username} (ID: {profile.get('id')})")
        user_id = str(profile.get('id', user_id))
    else:
        logger.warning(f"Token verification returned {verify.status_code}, but proceeding anyway")

    # Save to multi-account token store
    if verified_username:
        save_account_token(verified_username, user_id, long_lived_token, expires_in)

    # Also update .env file for backward compatibility
    update_env_file('THREADS_ACCESS_TOKEN', long_lived_token)
    update_env_file('THREADS_USER_ID', user_id)

    # Save metadata for status tracking
    save_token_metadata(long_lived_token, expires_in)

    logger.info("")
    logger.info("=" * 60)
    logger.info("THREADS TOKEN SETUP COMPLETE")
    logger.info("=" * 60)
    logger.info(f"  User ID:    {user_id}")
    logger.info(f"  Expires in: {expires_days} days")
    logger.info(f"  .env file:  {ENV_FILE}")
    logger.info("")
    logger.info("Next steps:")
    logger.info("  1. Test the pipeline: python orchestrate.py --source threads --test-mode")
    logger.info("  2. Schedule token refresh every 50 days (see --help for cron examples)")
    logger.info("=" * 60)


def _refresh_single_account(username: str, acct_info: dict, force: bool):
    """Refresh a single account's token in the store."""
    access_token = acct_info.get('access_token')
    expires_at = acct_info.get('expires_at', 0)
    now = datetime.now(timezone.utc).timestamp()
    remaining_days = (expires_at - now) / 86400

    if not force and remaining_days > 7:
        logger.info(f"  @{username}: valid for {remaining_days:.0f} days, skipping (use --force)")
        return

    if remaining_days <= 0:
        logger.error(f"  @{username}: EXPIRED. Re-authorize with --authorize or --add-token")
        return

    logger.info(f"  Refreshing @{username} ({remaining_days:.1f} days remaining)...")

    response = requests.get(THREADS_REFRESH_URL, params={
        'grant_type': 'th_refresh_token',
        'access_token': access_token,
    })

    if response.status_code != 200:
        logger.error(f"  @{username}: refresh failed -- {response.status_code} {response.text[:200]}")
        return

    data = response.json()
    new_token = data.get('access_token')
    new_expires_in = data.get('expires_in', 5184000)

    if new_token:
        save_account_token(username, acct_info['user_id'], new_token, new_expires_in)
        logger.info(f"  @{username}: refreshed, expires in {new_expires_in // 86400} days")
    else:
        logger.error(f"  @{username}: no token in refresh response")


def refresh(app_secret: str, force: bool = False, account: str = None, refresh_all: bool = False):
    """Refresh tokens. Supports single account, all accounts, or legacy .env mode."""
    store = load_token_store()

    # Multi-account: refresh all or a specific account
    if store:
        if account:
            if account in store:
                _refresh_single_account(account, store[account], force)
            else:
                logger.error(f"Account @{account} not found. Available: {', '.join(store.keys())}")
                sys.exit(1)
        else:
            # Default: refresh all accounts in the store
            logger.info(f"Refreshing {len(store)} account(s)...")
            for uname, acct_info in store.items():
                _refresh_single_account(uname, acct_info, force)
        return

    # Legacy single-account fallback: refresh from .env
    current_token = os.getenv('THREADS_ACCESS_TOKEN')
    if not current_token:
        logger.error("No tokens found. Run --authorize or --add-token first.")
        sys.exit(1)

    status = get_token_status()
    if status and not force:
        if status['is_expired']:
            logger.error(f"Token expired. Run --authorize to get a new one.")
            sys.exit(1)
        if not status['needs_refresh']:
            logger.info(f"Token valid for {status['remaining_days']} days. Use --force to refresh anyway.")
            return

    logger.info("Refreshing Threads token...")
    response = requests.get(THREADS_REFRESH_URL, params={
        'grant_type': 'th_refresh_token',
        'access_token': current_token,
    })

    if response.status_code != 200:
        logger.error(f"Refresh failed: {response.status_code} {response.text}")
        sys.exit(1)

    data = response.json()
    new_token = data.get('access_token')
    expires_in = data.get('expires_in', 5184000)

    if not new_token:
        logger.error(f"No access_token in refresh response: {data}")
        sys.exit(1)

    update_env_file('THREADS_ACCESS_TOKEN', new_token)
    save_token_metadata(new_token, expires_in)
    logger.info(f"Token refreshed. New expiry: {expires_in // 86400} days.")


def show_status(account: str = None):
    """Display token status for all accounts (or a specific one)."""
    store = load_token_store()

    if store:
        accounts_to_show = {account: store[account]} if account and account in store else store
        logger.info(f"Threads Token Store ({len(store)} account(s)):")
        logger.info("")

        now = datetime.now(timezone.utc).timestamp()
        for username, acct_info in accounts_to_show.items():
            expires_at = acct_info.get('expires_at', 0)
            remaining_days = (expires_at - now) / 86400
            status_str = 'EXPIRED' if remaining_days <= 0 else ('REFRESH SOON' if remaining_days <= 7 else 'OK')
            logger.info(f"  @{username:<30} ID: {acct_info.get('user_id', '?'):<20} "
                        f"Expires: {remaining_days:.0f}d  [{status_str}]")

        logger.info("")
        logger.info("Live API checks:")
        for username, acct_info in accounts_to_show.items():
            token = acct_info.get('access_token', '')
            try:
                resp = requests.get(f"{THREADS_API_BASE}/me", params={
                    'fields': 'id,username',
                    'access_token': token,
                }, timeout=10)
                if resp.status_code == 200:
                    profile = resp.json()
                    logger.info(f"  @{username}: OK -- @{profile.get('username')} (ID: {profile.get('id')})")
                else:
                    logger.warning(f"  @{username}: FAILED -- {resp.status_code}")
            except Exception as e:
                logger.warning(f"  @{username}: ERROR -- {e}")
        return

    # Legacy single-account fallback
    current_token = os.getenv('THREADS_ACCESS_TOKEN')
    if not current_token:
        logger.info("No tokens found. Run --authorize or --add-token to set up.")
        return

    status = get_token_status()
    if status:
        logger.info("Threads Token Status (legacy single-account):")
        logger.info(f"  Remaining: {status['remaining_days']} days")
        logger.info(f"  Expired:   {'YES' if status['is_expired'] else 'No'}")
    else:
        logger.info("No token metadata found.")

    try:
        response = requests.get(f"{THREADS_API_BASE}/me", params={
            'fields': 'id,username', 'access_token': current_token,
        }, timeout=10)
        if response.status_code == 200:
            profile = response.json()
            logger.info(f"  Live check: OK -- @{profile.get('username')}")
        else:
            logger.warning(f"  Live check: FAILED -- {response.status_code}")
    except Exception as e:
        logger.warning(f"  Live check: ERROR -- {e}")


def add_token(token: str):
    """Add a token from the User Token Generator to the multi-account store.

    Verifies the token, attempts to exchange for long-lived, and saves it.
    """
    logger.info("Verifying token...")

    response = requests.get(f"{THREADS_API_BASE}/me", params={
        'fields': 'id,username,name',
        'access_token': token,
    }, timeout=10)

    if response.status_code != 200:
        logger.error(f"Token verification failed: {response.status_code} {response.text}")
        sys.exit(1)

    profile = response.json()
    user_id = str(profile.get('id', ''))
    username = profile.get('username', '')
    name = profile.get('name', '')

    logger.info(f"Token verified: @{username} ({name}, ID: {user_id})")

    # Try to exchange for long-lived token (in case it's short-lived)
    expires_in = 5184000  # 60 days default
    app_secret = os.getenv('THREADS_APP_SECRET')

    if app_secret:
        exchange_resp = requests.get(THREADS_EXCHANGE_URL, params={
            'grant_type': 'th_exchange_token',
            'client_secret': app_secret,
            'access_token': token,
        })

        if exchange_resp.status_code == 200:
            ll_data = exchange_resp.json()
            new_token = ll_data.get('access_token')
            if new_token:
                if new_token != token:
                    logger.info(f"Exchanged for long-lived token ({ll_data.get('expires_in', 5184000) // 86400} days)")
                    token = new_token
                else:
                    logger.info("Token is already long-lived")
                expires_in = ll_data.get('expires_in', 5184000)
        else:
            logger.debug(f"Token exchange returned {exchange_resp.status_code} -- may already be long-lived")

    save_account_token(username, user_id, token, expires_in)

    store = load_token_store()
    logger.info("")
    logger.info(f"Account @{username} added to token store.")
    logger.info(f"Token store now has {len(store)} account(s): {', '.join(f'@{u}' for u in store)}")


def main():
    parser = argparse.ArgumentParser(
        description="Threads OAuth Token Manager — single and multi-account",
        epilog="""
Examples:
  First-time OAuth setup (opens browser):
    python shared/threads_token_manager.py --authorize

  Add token from User Token Generator (multi-account):
    python shared/threads_token_manager.py --add-token "THAA..."

  Refresh all accounts in token store:
    python shared/threads_token_manager.py --refresh

  Refresh a specific account:
    python shared/threads_token_manager.py --refresh --account msnewstoday

  Check status of all accounts:
    python shared/threads_token_manager.py --status

Requires THREADS_APP_ID and THREADS_APP_SECRET in .env.
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--authorize', action='store_true',
                       help='Run initial OAuth authorization (opens browser)')
    group.add_argument('--refresh', action='store_true',
                       help='Refresh token(s) — all accounts in store, or use --account')
    group.add_argument('--status', action='store_true',
                       help='Check token status for all accounts')
    group.add_argument('--add-token', type=str, metavar='TOKEN',
                       help='Add a token from User Token Generator to the multi-account store')

    parser.add_argument('--force', action='store_true',
                        help='Force refresh even if token is not near expiry')
    parser.add_argument('--code', type=str, metavar='CODE',
                        help='With --authorize: paste the code from the redirect URL')
    parser.add_argument('--account', type=str, metavar='USERNAME',
                        help='With --refresh or --status: target a specific account')

    args = parser.parse_args()

    # Load existing .env
    load_env_vars()

    if args.authorize:
        app_id, app_secret = get_app_credentials()
        authorize(app_id, app_secret, code=args.code)

    elif args.refresh:
        _, app_secret = get_app_credentials()
        refresh(app_secret, force=args.force, account=args.account)

    elif args.status:
        show_status(account=args.account)

    elif args.add_token:
        add_token(args.add_token)


if __name__ == '__main__':
    main()
