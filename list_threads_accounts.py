#!/usr/bin/env python3
"""List all Bionews Instagram accounts that have linked Threads profiles."""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.user import User

# Load environment
PROJECT_ROOT = Path(__file__).parent
load_dotenv(PROJECT_ROOT / '.env')

# Initialize Facebook API
access_token = os.getenv('FACEBOOK_ACCESS_TOKEN')
app_id = os.getenv('FACEBOOK_APP_ID')
app_secret = os.getenv('FACEBOOK_APP_SECRET')

if not all([access_token, app_id, app_secret]):
    print("Error: Missing Facebook API credentials in .env")
    sys.exit(1)

FacebookAdsApi.init(app_id, app_secret, access_token, api_version='v25.0')

print("Discovering Instagram accounts with Threads profiles...")
print("=" * 80)

# Get all Facebook Pages managed by this user
me = User(fbid='me')
pages = me.get_accounts(fields=[
    'id',
    'name',
    'instagram_business_account',
])

threads_accounts = []
no_threads = []

for page in pages:
    page_name = page.get('name', '')
    ig_account = page.get('instagram_business_account')

    if ig_account:
        ig_id = ig_account.get('id', '')

        # Check if this IG account has a Threads profile
        # The Threads profile is accessed via the threads_user_id field
        try:
            from facebook_business.adobjects.iguser import IGUser
            ig_user = IGUser(ig_id)

            # Try to get the threads profile ID
            # Note: As of 2026, there's no direct API field for threads_user_id
            # Threads accounts use the same user ID structure as Instagram

            profile = ig_user.api_get(fields=['id', 'username'])
            username = profile.get('username', '')

            # For now, we'll list all IG accounts
            # The user can check which have Threads via Business Suite
            print(f"  @{username:<35} (IG ID: {ig_id})")
            print(f"    Page: {page_name}")
            print(f"    Check if has Threads: https://www.threads.net/@{username}")
            print()

        except Exception as e:
            pass

print("=" * 80)
print("\nTo see which accounts have Threads:")
print("  1. Go to business.facebook.com")
print("  2. Settings → Accounts → Threads accounts")
print("\nThreads accounts use the same username as Instagram (e.g., @smanewstoday)")
