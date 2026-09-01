#!/usr/bin/env python3
"""
Automate export of archived contacts from Mailchimp web UI using Playwright.

This script logs into Mailchimp and exports archived contacts from all audiences,
then combines them into a single CSV file.

Usage:
    python shared/export_mailchimp_archived.py

Environment variables required:
    MAILCHIMP_EMAIL: Your Mailchimp login email
    MAILCHIMP_PASSWORD: Your Mailchimp login password
    MAILCHIMP_TOTP_SECRET: TOTP secret for 2FA (the secret key from authenticator setup)

Optional:
    MAILCHIMP_DOWNLOAD_DIR: Directory for downloads (default: ./mailchimp_exports)
    MAILCHIMP_SERVER_PREFIX: Server prefix (default: us5)

Getting Your TOTP Secret:
    When you set up 2FA in Mailchimp, the authenticator app shows a QR code.
    The secret is either shown as text below the QR code, or you can extract it
    from the QR code URL (otpauth://totp/...?secret=YOURSECRETHERE&...)

    If you've already set up 2FA, you may need to disable and re-enable it to
    get the secret key again.
"""

import os
import sys
import time
import glob
import csv
from datetime import datetime
from pathlib import Path

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, '.env'))

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
except ImportError:
    print("Playwright not installed. Install with: pip install playwright && playwright install chromium")
    sys.exit(1)

try:
    import pyotp
except ImportError:
    print("pyotp not installed. Install with: pip install pyotp")
    sys.exit(1)


def get_credentials():
    """Get Mailchimp login credentials from environment."""
    email = os.environ.get('MAILCHIMP_EMAIL')
    password = os.environ.get('MAILCHIMP_PASSWORD')
    totp_secret = os.environ.get('MAILCHIMP_TOTP_SECRET')

    missing = []
    if not email:
        missing.append('MAILCHIMP_EMAIL')
    if not password:
        missing.append('MAILCHIMP_PASSWORD')
    if not totp_secret:
        missing.append('MAILCHIMP_TOTP_SECRET')

    if missing:
        print(f"ERROR: Missing required environment variables: {', '.join(missing)}")
        print("\nAdd to your .env file:")
        print("  MAILCHIMP_EMAIL=your-email@example.com")
        print("  MAILCHIMP_PASSWORD=your-password")
        print("  MAILCHIMP_TOTP_SECRET=your-totp-secret-key")
        print("\nTo get your TOTP secret:")
        print("  1. Go to Mailchimp Account Settings > Security")
        print("  2. Disable 2FA (if enabled), then re-enable it")
        print("  3. When shown the QR code, look for 'Can't scan?' or 'Manual entry'")
        print("  4. Copy the secret key (usually 16-32 characters)")
        sys.exit(1)

    return email, password, totp_secret


def get_totp_code(totp_secret: str) -> str:
    """Generate current TOTP code from secret."""
    totp = pyotp.TOTP(totp_secret)
    return totp.now()


def wait_for_download(download_dir: str, timeout: int = 60) -> str:
    """Wait for a new file to appear in download directory."""
    existing_files = set(glob.glob(os.path.join(download_dir, '*.csv')))
    start_time = time.time()

    while time.time() - start_time < timeout:
        current_files = set(glob.glob(os.path.join(download_dir, '*.csv')))
        new_files = current_files - existing_files

        if new_files:
            # Wait a bit for file to finish writing
            time.sleep(2)
            return list(new_files)[0]

        time.sleep(1)

    raise TimeoutError(f"No new CSV file appeared in {download_dir} within {timeout} seconds")


def handle_2fa(page, totp_secret: str) -> bool:
    """Handle 2FA by entering TOTP code."""
    time.sleep(2)

    # Common 2FA form selectors
    tfa_selectors = [
        'input[name="mfa_code"]',
        'input[name="code"]',
        'input[name="otp"]',
        'input[placeholder*="code"]',
        'input[placeholder*="Code"]',
        'input[aria-label*="verification"]',
        'input[aria-label*="code"]',
        '[data-testid="mfa-input"]',
        'input[type="text"]',  # Fallback - often the only text input on 2FA page
    ]

    tfa_input = None
    for selector in tfa_selectors:
        tfa_input = page.query_selector(selector)
        if tfa_input:
            break

    if not tfa_input:
        # Check if 2FA is actually required
        page_text = page.content().lower()
        if 'verification' not in page_text and 'two-factor' not in page_text and 'authenticator' not in page_text:
            return True  # No 2FA needed
        print("  WARNING: 2FA page detected but couldn't find input field")
        return False

    print("  2FA required - entering TOTP code...")
    totp_code = get_totp_code(totp_secret)
    tfa_input.fill(totp_code)

    # Look for submit button
    submit_selectors = [
        'button[type="submit"]',
        'button:has-text("Verify")',
        'button:has-text("Submit")',
        'button:has-text("Continue")',
        'input[type="submit"]',
    ]

    for selector in submit_selectors:
        submit_btn = page.query_selector(selector)
        if submit_btn:
            submit_btn.click()
            break

    time.sleep(3)
    return True


def export_archived_contacts(headless: bool = False):
    """Export archived contacts from all Mailchimp audiences."""

    email, password, totp_secret = get_credentials()
    download_dir = os.environ.get('MAILCHIMP_DOWNLOAD_DIR', './mailchimp_exports')
    server_prefix = os.environ.get('MAILCHIMP_SERVER_PREFIX', 'us5')

    # Create download directory
    Path(download_dir).mkdir(parents=True, exist_ok=True)
    download_dir = os.path.abspath(download_dir)

    print(f"Download directory: {download_dir}")
    print(f"Server prefix: {server_prefix}")
    print(f"Headless mode: {headless}")
    print("=" * 60)

    exported_files = []

    with sync_playwright() as p:
        # Launch browser
        browser = p.chromium.launch(
            headless=headless,
            downloads_path=download_dir
        )

        context = browser.new_context(
            accept_downloads=True,
            viewport={'width': 1920, 'height': 1080}
        )

        page = context.new_page()

        try:
            # Step 1: Login to Mailchimp
            print("\n[1/4] Logging into Mailchimp...")
            page.goto('https://login.mailchimp.com/')
            time.sleep(2)

            # Fill login form
            page.fill('input[name="username"]', email)
            page.fill('input[name="password"]', password)
            page.click('button[type="submit"]')

            # Wait for response
            time.sleep(3)

            # Check for 2FA
            if 'login' in page.url:
                if not handle_2fa(page, totp_secret):
                    print("  ERROR: Could not complete 2FA")
                    return []

            # Wait for dashboard to load
            try:
                page.wait_for_url('**/lists/**', timeout=30000)
            except PlaywrightTimeout:
                # Try waiting for admin URL
                try:
                    page.wait_for_url('**admin.mailchimp.com**', timeout=15000)
                except PlaywrightTimeout:
                    print(f"  ERROR: Login failed - current URL: {page.url}")
                    return []

            print("  Logged in successfully!")

            # Step 2: Get list of audiences
            print("\n[2/4] Fetching audience list...")
            page.goto(f'https://{server_prefix}.admin.mailchimp.com/lists/')
            page.wait_for_selector('table', timeout=30000)

            # Get all audience links
            audience_links = page.query_selector_all('a[href*="/lists/members/?id="]')
            audiences = []

            for link in audience_links:
                href = link.get_attribute('href')
                name = link.inner_text().strip()
                if href and name:
                    # Extract list ID from URL
                    list_id = href.split('id=')[-1].split('&')[0] if 'id=' in href else None
                    if list_id:
                        audiences.append({'id': list_id, 'name': name, 'url': href})

            print(f"  Found {len(audiences)} audiences")

            # Step 3: Export archived contacts from each audience
            print("\n[3/4] Exporting archived contacts...")

            for idx, audience in enumerate(audiences, 1):
                print(f"\n  [{idx}/{len(audiences)}] {audience['name']}...")

                try:
                    # Navigate to audience's archived contacts
                    archived_url = f"https://{server_prefix}.admin.mailchimp.com/lists/members/archived/?id={audience['id']}"
                    page.goto(archived_url)

                    # Wait for page to load
                    time.sleep(2)

                    # Check if there are any archived contacts
                    no_contacts = page.query_selector('text="No archived contacts"')
                    if no_contacts:
                        print(f"    No archived contacts")
                        continue

                    # Look for export button/link
                    # Try different possible selectors for export
                    export_selectors = [
                        'a:has-text("Export")',
                        'button:has-text("Export")',
                        '[data-action="export"]',
                        'a[href*="export"]',
                        '.export-link',
                        'text="Export Audience"'
                    ]

                    export_clicked = False
                    for selector in export_selectors:
                        try:
                            export_btn = page.query_selector(selector)
                            if export_btn:
                                # Set up download handler
                                with page.expect_download(timeout=60000) as download_info:
                                    export_btn.click()

                                download = download_info.value

                                # Save the download
                                filename = f"archived_{audience['id']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                                filepath = os.path.join(download_dir, filename)
                                download.save_as(filepath)

                                exported_files.append({
                                    'list_id': audience['id'],
                                    'list_name': audience['name'],
                                    'file': filepath
                                })
                                print(f"    Exported to {filename}")
                                export_clicked = True
                                break
                        except Exception:
                            continue

                    if not export_clicked:
                        # Try clicking "More options" dropdown first
                        try:
                            more_options = page.query_selector('button:has-text("More")')
                            if more_options:
                                more_options.click()
                                time.sleep(1)

                                export_link = page.query_selector('a:has-text("Export")')
                                if export_link:
                                    with page.expect_download(timeout=60000) as download_info:
                                        export_link.click()

                                    download = download_info.value
                                    filename = f"archived_{audience['id']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                                    filepath = os.path.join(download_dir, filename)
                                    download.save_as(filepath)

                                    exported_files.append({
                                        'list_id': audience['id'],
                                        'list_name': audience['name'],
                                        'file': filepath
                                    })
                                    print(f"    Exported to {filename}")
                                    export_clicked = True
                        except Exception as e:
                            print(f"    Could not find export option: {e}")

                    if not export_clicked:
                        print(f"    WARNING: Could not find export button")

                except PlaywrightTimeout:
                    print(f"    Timeout - skipping")
                except Exception as e:
                    print(f"    Error: {e}")

                # Rate limiting - don't hammer the server
                time.sleep(2)

        finally:
            browser.close()

    # Step 4: Combine all exported files
    print("\n[4/4] Combining exported files...")

    if exported_files:
        combined_file = os.path.join(download_dir, f"all_archived_contacts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")

        all_rows = []
        fieldnames = set()

        for export in exported_files:
            try:
                with open(export['file'], 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        row['_list_id'] = export['list_id']
                        row['_list_name'] = export['list_name']
                        all_rows.append(row)
                        fieldnames.update(row.keys())
            except Exception as e:
                print(f"  Error reading {export['file']}: {e}")

        if all_rows:
            # Write combined file
            fieldnames = sorted(list(fieldnames))
            # Move list identifiers to front
            for field in ['_list_name', '_list_id']:
                if field in fieldnames:
                    fieldnames.remove(field)
                    fieldnames.insert(0, field)

            with open(combined_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(all_rows)

            print(f"\n  Combined {len(all_rows)} contacts from {len(exported_files)} lists")
            print(f"  Output: {combined_file}")
        else:
            print("  No contacts to combine")
    else:
        print("  No files were exported")

    print("\n" + "=" * 60)
    print("EXPORT COMPLETE")
    print(f"Exported files: {len(exported_files)}")
    print(f"Download directory: {download_dir}")

    return exported_files


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Export archived contacts from Mailchimp')
    parser.add_argument('--headless', action='store_true', help='Run in headless mode (no browser window)')
    parser.add_argument('--visible', action='store_true', help='Run with visible browser window (default)')

    args = parser.parse_args()

    # Default to visible mode for debugging
    headless = args.headless and not args.visible

    export_archived_contacts(headless=headless)
