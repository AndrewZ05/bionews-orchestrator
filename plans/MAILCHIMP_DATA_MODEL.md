# Mailchimp Data Model - Complete Analysis

## Executive Summary

The Mailchimp pipeline extracts data from 18 tables organized into 3 logical groups:
- **Core Group** (6 tables): Master data and metadata
- **Campaign Group** (7 tables): Campaign performance and recipient activity
- **List Group** (5 tables): Audience configuration and growth metrics

This document provides a detailed analysis of each table, their relationships, data patterns, and best practices for querying.

---

## Table of Contents

1. [Data Model Overview](#data-model-overview)
2. [Core Group Tables](#core-group-tables)
3. [Campaign Group Tables](#campaign-group-tables)
4. [List Group Tables](#list-group-tables)
5. [Relationships and Joins](#relationships-and-joins)
6. [Data Patterns](#data-patterns)
7. [Query Examples](#query-examples)

---

## Data Model Overview

### Entity Relationship Diagram (Text)

```
LISTS (audiences)
  |
  +-- MEMBERS (subscribers)
  |     |
  |     +-- CAMPAIGN_EMAIL_ACTIVITY (opens, clicks, bounces)
  |     +-- CAMPAIGN_SENT_TO (delivery status)
  |     +-- CAMPAIGN_OPEN_DETAILS (open aggregates)
  |     +-- CAMPAIGN_CLICK_MEMBERS (click details)
  |
  +-- LIST_SEGMENTS (audience segments)
  |     |
  |     +-- LIST_SEGMENT_MEMBERS (segment membership - SNAPSHOT)
  |
  +-- LIST_MERGE_FIELDS (custom field definitions)
  +-- LIST_GROWTH_HISTORY (monthly metrics)
  +-- LIST_ACTIVITY (daily metrics)

CAMPAIGNS (email campaigns)
  |
  +-- CAMPAIGN_EMAIL_ACTIVITY (per-recipient events)
  +-- CAMPAIGN_SENT_TO (recipient list)
  +-- CAMPAIGN_OPEN_DETAILS (who opened)
  +-- CAMPAIGN_CLICK_DETAILS (link performance)
  +-- CAMPAIGN_CLICK_MEMBERS (who clicked what)
  +-- CAMPAIGN_DOMAIN_PERFORMANCE (domain metrics)
  +-- CAMPAIGN_LOCATIONS (geographic opens)
  +-- UNSUBSCRIBES (unsubscribe events)
  +-- METADATA_CAMPAIGNS (lightweight snapshots)
```

### Key Identifiers

| Entity | Primary Identifier | Secondary Identifiers |
|--------|-------------------|----------------------|
| Account | `mailchimp_account_id` | `mailchimp_account_name`, `tenant_id` |
| List | `list_id` | Combined with account_id |
| Campaign | `campaign_id`, `id` | `web_id` |
| Member | `subscriber_hash` | `email_id`, `email_address` |
| Link | `link_id` | Within campaign context |
| Segment | `segment_id` | Within list context |

---

## Core Group Tables

These tables contain master data and metadata. They form the foundation for all reporting and analysis.

### 1. CAMPAIGNS
**Purpose**: Master data for all email campaigns

**Key Information**:
- Campaign metadata (name, type, status, create/send times)
- Send configuration (recipients, settings, tracking)
- Summary metrics (emails_sent)
- Campaign structure (variate_settings, ab_split_opts, rss_opts)

**Data Pattern**:
- **Type**: Master data with incremental updates
- **Incremental Strategy**: Date-based on `send_time`
- **Lookback**: 7 days
- **Growth**: Low (only new campaigns or updates to existing)

**Primary Key**: `[mailchimp_account_id, id]`

**Important Fields**:
- `id` (STRING): Campaign ID
- `web_id` (STRING): Web interface ID (numeric-looking but must be STRING)
- `status` (STRING): draft, scheduled, sending, sent, paused
- `send_time` (TIMESTAMP): When campaign was/will be sent
- `emails_sent` (INT64): Total recipients
- `type` (STRING): regular, plaintext, absplit, rss, variate

**Nested JSON Fields**:
- `recipients`: Target audience configuration
- `settings`: Subject, from_name, reply_to, etc.
- `tracking`: Opens, clicks, HTML/text clicks settings
- `report_summary`: Aggregate metrics snapshot

**Relationships**:
- Parent to all `campaign_*` tables
- Links to `lists` via recipients.list_id (in JSON)
- Links to `metadata_campaigns` via campaign_id

**Duplicates/Repetition**:
- Many fields duplicate what's in campaign report tables
- `report_summary` JSON duplicates metrics from campaign_email_activity aggregations
- Recommendation: Use campaigns for metadata, campaign_* tables for metrics

**Query Pattern**:
```sql
-- Get sent campaigns in date range
SELECT id, web_id, status, send_time, emails_sent
FROM campaigns
WHERE send_time BETWEEN '2025-06-01' AND '2025-06-30'
  AND status = 'sent'
  AND emails_sent >= 1
ORDER BY send_time DESC
```

---

### 2. LISTS
**Purpose**: Audience/mailing list master data and configuration

**Key Information**:
- List metadata (name, visibility, permissions)
- List settings (campaign_defaults, modules)
- Contact information
- Summary statistics (in `stats` JSON field)

**Data Pattern**:
- **Type**: Master data (full snapshot)
- **Incremental Strategy**: None (full refresh)
- **Growth**: Very low (only when new lists created)

**Primary Key**: `[mailchimp_account_id, id]`

**Important Fields**:
- `id` (STRING): List ID
- `web_id` (STRING): Web interface ID
- `name` (STRING): List name
- `date_created` (TIMESTAMP): When list was created
- `list_rating` (INT64): Mailchimp's quality rating
- `subscribe_url_short` / `subscribe_url_long`: Signup URLs

**Nested JSON Fields**:
- `contact`: Organization contact info
- `campaign_defaults`: Default from_name, from_email, subject, language
- `stats`: member_count, unsubscribe_count, open_rate, click_rate, etc.

**Relationships**:
- Parent to `members`
- Parent to `list_segments`, `list_merge_fields`, `list_growth_history`, `list_activity`
- Referenced by campaigns via recipients.list_id

**Duplicates/Repetition**:
- `stats` JSON duplicates metrics available from members table aggregations
- `campaign_defaults` duplicates settings that appear in campaigns table
- Recommendation: Use lists for configuration, aggregate members for current metrics

**Query Pattern**:
```sql
-- Get active lists with subscriber counts
SELECT
  id,
  name,
  date_created,
  JSON_EXTRACT_SCALAR(stats, '$.member_count') as member_count,
  JSON_EXTRACT_SCALAR(stats, '$.open_rate') as avg_open_rate
FROM lists
WHERE visibility = 'pub'
ORDER BY date_created DESC
```

---

### 3. MEMBERS
**Purpose**: Individual subscriber profiles and subscription status

**Key Information**:
- Subscriber identity (email, name, location)
- Subscription status and history
- Engagement metrics (opens, clicks, ratings)
- Custom merge field values
- Tags and marketing permissions

**Data Pattern**:
- **Type**: Large dimension table with incremental updates
- **Incremental Strategy**: Date-based on `last_changed`
- **Lookback**: 7 days
- **Growth**: High (frequent updates as subscribers engage)

**Primary Key**: `[mailchimp_account_id, list_id, subscriber_hash]`

**Important Fields**:
- `subscriber_hash` (STRING): MD5 hash of lowercase email (unique per list)
- `email_address` (STRING): Subscriber email
- `id` (STRING): Unique member ID
- `web_id` (STRING): Web interface ID
- `status` (STRING): subscribed, unsubscribed, cleaned, pending, transactional
- `last_changed` (TIMESTAMP): Last update to member record
- `timestamp_signup` (TIMESTAMP): When they signed up
- `timestamp_opt` (TIMESTAMP): When they confirmed (double opt-in)
- `member_rating` (INT64): Engagement score (1-5 stars)
- `email_client` (STRING): Email client used for opens
- `location` (STRING): Geographic data (JSON)
- `tags_count` (INT64): Number of tags applied

**Nested JSON Fields**:
- `merge_fields`: Custom field values (FNAME, LNAME, etc.)
- `stats`: avg_open_rate, avg_click_rate
- `location`: latitude, longitude, timezone, country_code, etc.
- `marketing_permissions`: GDPR consent tracking
- `last_note`: Most recent note about subscriber

**Relationships**:
- Child of `lists`
- Joins to `campaign_email_activity` via subscriber_hash
- Joins to `campaign_sent_to` via subscriber_hash
- Joins to `campaign_open_details` via subscriber_hash
- Joins to `campaign_click_members` via subscriber_hash

**Duplicates/Repetition**:
- `stats` JSON duplicates metrics that can be calculated from campaign_email_activity
- `location` data partially duplicates campaign_locations geographic data
- Recommendation: Use members for current subscriber state, campaign tables for historical activity

**Query Pattern**:
```sql
-- Get active subscribers by list
SELECT
  list_id,
  COUNT(*) as total_subscribers,
  SUM(CASE WHEN status = 'subscribed' THEN 1 ELSE 0 END) as active,
  SUM(CASE WHEN member_rating >= 4 THEN 1 ELSE 0 END) as engaged
FROM members
WHERE mailchimp_account_id = 'account123'
GROUP BY list_id
```

---

### 4. UNSUBSCRIBES
**Purpose**: Unsubscribe events and reasons

**Key Information**:
- Who unsubscribed from which campaign
- When they unsubscribed
- Why they unsubscribed (reason text)
- Subscriber profile snapshot at time of unsubscribe

**Data Pattern**:
- **Type**: Event/fact table with incremental updates
- **Incremental Strategy**: Date-based on `timestamp`
- **Lookback**: 7 days
- **Growth**: Low to medium (depends on unsubscribe rate)

**Primary Key**: `[mailchimp_account_id, campaign_id, email_id]`

**Important Fields**:
- `campaign_id` (STRING): Which campaign triggered unsubscribe
- `email_id` (STRING): Subscriber identifier
- `email_address` (STRING): Email address
- `timestamp` (TIMESTAMP): When unsubscribe occurred
- `reason` (STRING): User-provided reason (optional)
- `list_id` (STRING): Which list they unsubscribed from

**Relationships**:
- Links to `campaigns` via campaign_id
- Links to `lists` via list_id
- Links to `members` via email_address or email_id

**Duplicates/Repetition**:
- Unsubscribe events also appear in `campaign_email_activity` (action='unsub')
- Subscriber fields (first_name, last_name, etc.) duplicate members table
- Recommendation: Use campaign_email_activity for comprehensive event stream, unsubscribes for detailed reason analysis

**Query Pattern**:
```sql
-- Unsubscribe rate by campaign
SELECT
  c.id as campaign_id,
  c.send_time,
  c.emails_sent,
  COUNT(u.email_id) as unsubscribes,
  ROUND(COUNT(u.email_id) * 100.0 / c.emails_sent, 2) as unsub_rate_pct
FROM campaigns c
LEFT JOIN unsubscribes u ON c.id = u.campaign_id
WHERE c.send_time >= '2025-06-01'
GROUP BY c.id, c.send_time, c.emails_sent
ORDER BY unsub_rate_pct DESC
```

---

### 5. METADATA_CAMPAIGNS
**Purpose**: Lightweight campaign metadata snapshots

**Key Information**:
- Campaign basic info (ID, list, send time, status)
- Email count
- Full campaign payload (JSON)
- Last refresh timestamp

**Data Pattern**:
- **Type**: Snapshot table (full refresh)
- **Incremental Strategy**: None
- **Growth**: Low (same size as campaigns table)

**Primary Key**: `[account_name, campaign_id]`

**Important Fields**:
- `account_name` (STRING): Mailchimp account name
- `campaign_id` (STRING): Campaign ID
- `list_id` (STRING): Target list
- `send_time` (TIMESTAMP): Send time
- `status` (STRING): Campaign status
- `emails_sent` (INT64): Recipient count
- `payload` (STRING): Full JSON response from API
- `last_refreshed` (TIMESTAMP): When snapshot was taken

**Relationships**:
- Duplicate of `campaigns` table data
- Links to `metadata_lists` via list_id

**Duplicates/Repetition**:
- **100% duplicate of campaigns table**
- Only difference: uses account_name instead of account_id in PK
- Recommendation: **Use campaigns table instead** - metadata_campaigns is redundant

**Query Pattern**:
```sql
-- This table is redundant - use campaigns instead
-- Kept for backward compatibility only
```

---

### 6. METADATA_LISTS
**Purpose**: Lightweight list metadata snapshots

**Key Information**:
- List basic info (ID, name, date created)
- Member count
- Full list payload (JSON)
- Last refresh timestamp

**Data Pattern**:
- **Type**: Snapshot table (full refresh)
- **Incremental Strategy**: None
- **Growth**: Very low (same size as lists table)

**Primary Key**: `[account_name, list_id]`

**Important Fields**:
- `account_name` (STRING): Mailchimp account name
- `list_id` (STRING): List ID
- `name` (STRING): List name
- `date_created` (TIMESTAMP): Creation date
- `member_count` (INT64): Current subscriber count
- `payload` (STRING): Full JSON response from API
- `last_refreshed` (TIMESTAMP): When snapshot was taken

**Relationships**:
- Duplicate of `lists` table data
- Links to `metadata_campaigns` via list_id

**Duplicates/Repetition**:
- **100% duplicate of lists table**
- Only difference: uses account_name instead of account_id in PK
- Recommendation: **Use lists table instead** - metadata_lists is redundant

**Query Pattern**:
```sql
-- This table is redundant - use lists instead
-- Kept for backward compatibility only
```

---

## Campaign Group Tables

These tables contain campaign performance data and recipient-level activity. They are all derived from the Mailchimp Reports API.

### 7. CAMPAIGN_EMAIL_ACTIVITY
**Purpose**: Per-recipient, per-event activity stream for campaigns

**Key Information**:
- Every email event for every recipient (sent, open, click, bounce, unsub)
- Event timestamps
- IP addresses and geo data for opens/clicks
- URL details for click events

**Data Pattern**:
- **Type**: Large fact/event table with incremental updates
- **Incremental Strategy**: Date-based on `activity_timestamp`
- **Load Pattern**: Multi-table extraction (campaign_group)
- **Growth**: Very high (multiple events per recipient)

**Primary Key**: `[mailchimp_account_id, campaign_id, email_id, activity_timestamp, action]`

**Important Fields**:
- `campaign_id` (STRING): Which campaign
- `email_id` (STRING): Subscriber identifier
- `subscriber_hash` (STRING): MD5 hash for joining to members
- `action` (STRING): **open, click, bounce, unsub, sent**
- `activity_timestamp` (TIMESTAMP): When event occurred
- `url` (STRING): For click events, the clicked URL
- `ip` (STRING): IP address of recipient
- `bounce_type` (STRING): For bounce events (hard, soft)

**Relationships**:
- Child of `campaigns` via campaign_id
- Joins to `members` via subscriber_hash
- Joins to `campaign_sent_to` via (campaign_id, subscriber_hash)

**Duplicates/Repetition**:
- Open events duplicate data in `campaign_open_details` (which is aggregated)
- Click events duplicate data in `campaign_click_details` and `campaign_click_members`
- Unsub events duplicate `unsubscribes` table
- Sent events duplicate `campaign_sent_to` table
- Recommendation: **This is the source of truth** - use this for detailed analysis, use aggregate tables for performance

**Volume Estimates**:
- Campaign with 10,000 recipients and 25% open rate, 5% click rate:
  - 10,000 sent events
  - ~2,500 open events (multiple opens per recipient)
  - ~500 click events (multiple clicks per recipient)
  - Total: ~13,000+ rows per campaign

**Query Pattern**:
```sql
-- Campaign engagement funnel
SELECT
  campaign_id,
  COUNT(DISTINCT email_id) as total_recipients,
  COUNT(DISTINCT CASE WHEN action = 'open' THEN email_id END) as unique_opens,
  COUNT(DISTINCT CASE WHEN action = 'click' THEN email_id END) as unique_clicks,
  COUNT(DISTINCT CASE WHEN action = 'bounce' THEN email_id END) as bounces,
  COUNT(DISTINCT CASE WHEN action = 'unsub' THEN email_id END) as unsubscribes
FROM campaign_email_activity
WHERE campaign_id = 'abc123'
GROUP BY campaign_id
```

---

### 8. CAMPAIGN_SENT_TO
**Purpose**: Delivery status for each campaign recipient

**Key Information**:
- Who received the campaign
- Delivery status (sent, hard bounce, soft bounce, etc.)
- Per-recipient open/click counts
- List membership details

**Data Pattern**:
- **Type**: Fact table (one row per recipient per campaign)
- **Incremental Strategy**: Date-based (linked to campaign send_time)
- **Load Pattern**: Multi-table extraction (campaign_group)
- **Growth**: High (one row per recipient per campaign)

**Primary Key**: `[mailchimp_account_id, campaign_id, subscriber_hash]`

**Important Fields**:
- `campaign_id` (STRING): Which campaign
- `subscriber_hash` (STRING): Recipient identifier
- `email_id` (STRING): Email identifier
- `email_address` (STRING): Email address
- `status` (STRING): **sent, hard, soft** (delivery status)
- `open_count` (INT64): Number of opens by this recipient
- `click_count` (INT64): Number of clicks by this recipient
- `last_open` (TIMESTAMP): Most recent open
- `last_click` (TIMESTAMP): Most recent click
- `list_id` (STRING): Which list they were on

**Relationships**:
- Child of `campaigns` via campaign_id
- Joins to `members` via (list_id, subscriber_hash)
- Aggregates data from `campaign_email_activity`

**Duplicates/Repetition**:
- Sent status duplicates campaign_email_activity (action='sent')
- open_count/click_count duplicate aggregations from campaign_email_activity
- Subscriber profile fields duplicate members table
- Recommendation: Use for recipient-level rollups, campaign_email_activity for event details

**Query Pattern**:
```sql
-- Campaign delivery and engagement by recipient
SELECT
  campaign_id,
  COUNT(*) as sent_count,
  SUM(CASE WHEN status = 'sent' THEN 1 ELSE 0 END) as delivered,
  SUM(CASE WHEN status = 'hard' THEN 1 ELSE 0 END) as hard_bounces,
  SUM(CASE WHEN open_count > 0 THEN 1 ELSE 0 END) as unique_opens,
  SUM(CASE WHEN click_count > 0 THEN 1 ELSE 0 END) as unique_clicks,
  SUM(open_count) as total_opens,
  SUM(click_count) as total_clicks
FROM campaign_sent_to
WHERE campaign_id = 'abc123'
GROUP BY campaign_id
```

---

### 9. CAMPAIGN_OPEN_DETAILS
**Purpose**: Per-subscriber open aggregates for campaigns

**Key Information**:
- Who opened the campaign
- How many times they opened
- First and last open timestamps

**Data Pattern**:
- **Type**: Aggregate fact table (one row per opener per campaign)
- **Incremental Strategy**: Date-based
- **Load Pattern**: Multi-table extraction (campaign_group)
- **Growth**: Medium (only recipients who opened)

**Primary Key**: `[mailchimp_account_id, campaign_id, subscriber_hash]`

**Important Fields**:
- `campaign_id` (STRING): Which campaign
- `subscriber_hash` (STRING): Who opened
- `email_id` (STRING): Email identifier
- `email_address` (STRING): Email address
- `opens_count` (INT64): Number of times opened
- `last_open` (TIMESTAMP): Most recent open

**Relationships**:
- Child of `campaigns` via campaign_id
- Joins to `members` via subscriber_hash
- Aggregates data from `campaign_email_activity` (action='open')

**Duplicates/Repetition**:
- **100% duplicate of campaign_email_activity aggregations**
- Also duplicates open_count in campaign_sent_to
- Recommendation: **Use campaign_email_activity instead** - more flexible and complete

**Query Pattern**:
```sql
-- Use campaign_email_activity instead:
SELECT
  campaign_id,
  subscriber_hash,
  COUNT(*) as opens_count,
  MIN(activity_timestamp) as first_open,
  MAX(activity_timestamp) as last_open
FROM campaign_email_activity
WHERE action = 'open'
GROUP BY campaign_id, subscriber_hash
```

---

### 10. CAMPAIGN_CLICK_DETAILS
**Purpose**: Campaign link performance aggregates

**Key Information**:
- Each unique URL in the campaign
- Total clicks and unique clickers per URL
- First and last click timestamps

**Data Pattern**:
- **Type**: Aggregate fact table (one row per link per campaign)
- **Incremental Strategy**: Date-based
- **Load Pattern**: Multi-table extraction (campaign_group)
- **Growth**: Low to medium (typically 5-20 links per campaign)

**Primary Key**: `[mailchimp_account_id, campaign_id, link_id]`

**Important Fields**:
- `campaign_id` (STRING): Which campaign
- `link_id` (STRING): Unique ID for this link
- `url` (STRING): The actual URL
- `total_clicks` (INT64): Total number of clicks
- `unique_clicks` (INT64): Number of unique clickers
- `click_percentage` (FLOAT64): % of recipients who clicked
- `last_click` (TIMESTAMP): Most recent click

**Relationships**:
- Child of `campaigns` via campaign_id
- Parent to `campaign_click_members` via (campaign_id, link_id)
- Aggregates data from `campaign_email_activity` (action='click')

**Duplicates/Repetition**:
- Aggregates campaign_email_activity click events
- Recommendation: Use for link-level reporting, campaign_email_activity for per-recipient analysis

**Query Pattern**:
```sql
-- Top performing links in a campaign
SELECT
  url,
  total_clicks,
  unique_clicks,
  click_percentage,
  ROUND(total_clicks * 1.0 / unique_clicks, 2) as clicks_per_clicker
FROM campaign_click_details
WHERE campaign_id = 'abc123'
ORDER BY total_clicks DESC
LIMIT 10
```

---

### 11. CAMPAIGN_CLICK_MEMBERS
**Purpose**: Per-recipient click metrics per link

**Key Information**:
- Who clicked which links
- How many times they clicked each link
- First and last click timestamps per link

**Data Pattern**:
- **Type**: Fact table (one row per clicker per link per campaign)
- **Incremental Strategy**: Date-based
- **Load Pattern**: Multi-table extraction (campaign_group)
- **Growth**: Medium (only recipients who clicked)

**Primary Key**: `[mailchimp_account_id, campaign_id, link_id, subscriber_hash]`

**Important Fields**:
- `campaign_id` (STRING): Which campaign
- `link_id` (STRING): Which link
- `subscriber_hash` (STRING): Who clicked
- `email_id` (STRING): Email identifier
- `email_address` (STRING): Email address
- `clicks` (INT64): Number of clicks on this link
- `last_click` (TIMESTAMP): Most recent click

**Relationships**:
- Child of `campaign_click_details` via (campaign_id, link_id)
- Joins to `members` via subscriber_hash
- Aggregates data from `campaign_email_activity` (action='click')

**Duplicates/Repetition**:
- Aggregates campaign_email_activity click events
- Subscriber fields duplicate members table
- Recommendation: Use for recipient-link analysis, campaign_email_activity for event-level detail

**Query Pattern**:
```sql
-- Find power clickers (users who click multiple links)
SELECT
  subscriber_hash,
  email_address,
  COUNT(DISTINCT link_id) as links_clicked,
  SUM(clicks) as total_clicks
FROM campaign_click_members
WHERE campaign_id = 'abc123'
GROUP BY subscriber_hash, email_address
HAVING COUNT(DISTINCT link_id) >= 3
ORDER BY total_clicks DESC
```

---

### 12. CAMPAIGN_DOMAIN_PERFORMANCE
**Purpose**: Campaign performance metrics by email domain

**Key Information**:
- Performance breakdown by recipient domain (gmail.com, yahoo.com, etc.)
- Opens, clicks, bounces per domain
- Percentage metrics per domain

**Data Pattern**:
- **Type**: Aggregate snapshot table (full refresh per campaign)
- **Incremental Strategy**: None (regenerated each extraction)
- **Load Pattern**: Multi-table extraction (campaign_group)
- **Growth**: Low (typically 10-50 domains per campaign)

**Primary Key**: `[mailchimp_account_id, campaign_id, domain]`

**Important Fields**:
- `campaign_id` (STRING): Which campaign
- `domain` (STRING): Email domain (gmail.com, yahoo.com, etc.)
- `emails_sent` (INT64): Recipients at this domain
- `bounces` (INT64): Bounces from this domain
- `opens` (INT64): Opens from this domain
- `clicks` (INT64): Clicks from this domain
- `unsubs` (INT64): Unsubscribes from this domain
- `delivered` (INT64): Successfully delivered
- `emails_pct` (FLOAT64): % of campaign sent to this domain
- `bounces_pct` (FLOAT64): Bounce rate for this domain

**Relationships**:
- Child of `campaigns` via campaign_id
- Aggregates data from `campaign_email_activity` grouped by email domain

**Duplicates/Repetition**:
- Can be calculated from campaign_sent_to + campaign_email_activity
- Recommendation: Use for domain-level reporting, recalculate if you need different time windows

**Query Pattern**:
```sql
-- Domain deliverability analysis
SELECT
  domain,
  emails_sent,
  delivered,
  bounces,
  ROUND(bounces * 100.0 / emails_sent, 2) as bounce_rate_pct,
  ROUND(opens * 100.0 / delivered, 2) as open_rate_pct
FROM campaign_domain_performance
WHERE campaign_id = 'abc123'
  AND emails_sent >= 100  -- Filter small domains
ORDER BY emails_sent DESC
```

---

### 13. CAMPAIGN_LOCATIONS
**Purpose**: Campaign open metrics by geography

**Key Information**:
- Opens by country/region
- Unique openers per location
- Percentage of opens per location

**Data Pattern**:
- **Type**: Aggregate snapshot table (full refresh per campaign)
- **Incremental Strategy**: None (regenerated each extraction)
- **Load Pattern**: Multi-table extraction (campaign_group)
- **Growth**: Low (typically 10-100 countries per campaign)

**Primary Key**: `[mailchimp_account_id, campaign_id, country]`

**Important Fields**:
- `campaign_id` (STRING): Which campaign
- `country` (STRING): Country code or name
- `opens` (INT64): Total opens from this country
- `unique_opens` (INT64): Unique openers from this country
- `region` (STRING): State/province (if available)

**Relationships**:
- Child of `campaigns` via campaign_id
- Aggregates geographic data from `campaign_email_activity` (action='open')

**Duplicates/Repetition**:
- Geographic data also in campaign_email_activity (ip field)
- Members table has location field with lat/long
- Recommendation: Use for geographic reporting, campaign_email_activity for detailed IP analysis

**Query Pattern**:
```sql
-- Top countries by engagement
SELECT
  country,
  opens,
  unique_opens,
  ROUND(opens * 1.0 / unique_opens, 2) as opens_per_person
FROM campaign_locations
WHERE campaign_id = 'abc123'
ORDER BY unique_opens DESC
LIMIT 20
```

---

## List Group Tables

These tables contain audience configuration, segmentation, and growth metrics.

### 14. LIST_MERGE_FIELDS
**Purpose**: Custom field definitions for each list

**Key Information**:
- Field metadata (name, type, tag)
- Field configuration (required, default value, options)
- Display settings (public visibility, display order)

**Data Pattern**:
- **Type**: Configuration snapshot (full refresh)
- **Incremental Strategy**: None
- **Load Pattern**: Multi-table extraction (list_group)
- **Growth**: Very low (only when fields added/modified)

**Primary Key**: `[mailchimp_account_id, list_id, merge_id]`

**Important Fields**:
- `list_id` (STRING): Which list
- `merge_id` (INT64): Unique field ID
- `tag` (STRING): Field tag (FNAME, LNAME, BIRTHDAY, etc.)
- `name` (STRING): Display name
- `type` (STRING): **text, number, address, phone, date, url, imageurl, radio, dropdown, birthday, zip**
- `required` (BOOLEAN): Is field required
- `default_value` (STRING): Default value
- `public` (BOOLEAN): Visible on signup forms
- `display_order` (INT64): Order in forms
- `options` (STRING): JSON with choices (for radio/dropdown)

**Relationships**:
- Child of `lists` via list_id
- Defines structure of merge_fields JSON in `members` table

**Duplicates/Repetition**:
- No duplicates - this is configuration data
- Recommendation: Use to understand members.merge_fields structure

**Query Pattern**:
```sql
-- List custom fields configuration
SELECT
  list_id,
  tag,
  name,
  type,
  required,
  public,
  display_order
FROM list_merge_fields
WHERE list_id = 'abc123'
ORDER BY display_order
```

---

### 15. LIST_GROWTH_HISTORY
**Purpose**: Monthly growth metrics per list

**Key Information**:
- Month-over-month subscriber additions/losses
- Breakdown by source (API, import, signup form, etc.)
- Net growth calculations

**Data Pattern**:
- **Type**: Time-series aggregate (full refresh)
- **Incremental Strategy**: None
- **Load Pattern**: Multi-table extraction (list_group)
- **Growth**: Low (one row per list per month)

**Primary Key**: `[mailchimp_account_id, list_id, month]`

**Important Fields**:
- `list_id` (STRING): Which list
- `month` (STRING): YYYY-MM format
- `existing` (INT64): Subscribers at start of month
- `imports` (INT64): Added via import
- `optins` (INT64): Added via signup form
- `pending` (INT64): Pending confirmations
- `cleaned` (INT64): Removed (cleaned)
- `unsubscribed` (INT64): Unsubscribed
- `reconfirm` (INT64): Re-confirmation sent
- `deleted` (INT64): Deleted by admin

**Relationships**:
- Child of `lists` via list_id
- Aggregates member additions/removals over time

**Duplicates/Repetition**:
- Can be calculated from members table timestamp_signup field
- Recommendation: Use for historical trend analysis

**Query Pattern**:
```sql
-- List growth trend
SELECT
  month,
  existing,
  (optins + imports) as additions,
  (unsubscribed + cleaned + deleted) as removals,
  ((optins + imports) - (unsubscribed + cleaned + deleted)) as net_growth
FROM list_growth_history
WHERE list_id = 'abc123'
ORDER BY month DESC
LIMIT 12
```

---

### 16. LIST_ACTIVITY
**Purpose**: Daily aggregated audience activity metrics

**Key Information**:
- Daily subscriber additions (emails, signups, unsubscribes)
- Activity counts per day
- Up to 180 days of history

**Data Pattern**:
- **Type**: Time-series aggregate (full refresh)
- **Incremental Strategy**: None
- **Load Pattern**: Multi-table extraction (list_group)
- **Growth**: Medium (one row per list per day, max 180 days)

**Primary Key**: `[mailchimp_account_id, list_id, day]`

**Important Fields**:
- `list_id` (STRING): Which list
- `day` (STRING): YYYY-MM-DD format
- `emails_sent` (INT64): Emails sent to this list
- `unique_opens` (INT64): Unique opens
- `recipient_clicks` (INT64): Unique clickers
- `hard_bounce` (INT64): Hard bounces
- `soft_bounce` (INT64): Soft bounces
- `subs` (INT64): New subscriptions
- `unsubs` (INT64): Unsubscribes
- `other_adds` (INT64): Other additions (imports, API)
- `other_removes` (INT64): Other removals (cleaned, deleted)

**Relationships**:
- Child of `lists` via list_id
- Aggregates daily metrics across all campaigns sent to this list

**Duplicates/Repetition**:
- Can be calculated from campaign_email_activity + members
- Recommendation: Use for daily trend analysis

**Query Pattern**:
```sql
-- List engagement trends (last 30 days)
SELECT
  day,
  emails_sent,
  unique_opens,
  recipient_clicks,
  ROUND(unique_opens * 100.0 / NULLIF(emails_sent, 0), 2) as open_rate_pct,
  ROUND(recipient_clicks * 100.0 / NULLIF(emails_sent, 0), 2) as click_rate_pct
FROM list_activity
WHERE list_id = 'abc123'
  AND day >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
ORDER BY day DESC
```

---

### 17. LIST_SEGMENTS
**Purpose**: Segment definitions per list

**Key Information**:
- Segment metadata (name, type, created date)
- Segment criteria/conditions
- Member count
- Segment configuration for targeting

**Data Pattern**:
- **Type**: Configuration snapshot (full refresh)
- **Incremental Strategy**: None
- **Load Pattern**: Multi-table extraction (list_group)
- **Growth**: Low (only when segments created/modified)

**Primary Key**: `[mailchimp_account_id, list_id, segment_id]`

**Important Fields**:
- `list_id` (STRING): Which list
- `segment_id` (STRING): Unique segment ID
- `name` (STRING): Segment name
- `type` (STRING): **saved, static, fuzzy** (saved=dynamic criteria, static=manual, fuzzy=predicted)
- `created_at` (TIMESTAMP): When created
- `updated_at` (TIMESTAMP): Last modified
- `member_count` (INT64): Current member count
- `options` (STRING): JSON with segment criteria

**Nested JSON Fields**:
- `options.match`: "any" or "all" (OR vs AND logic)
- `options.conditions`: Array of filter conditions

**Relationships**:
- Child of `lists` via list_id
- Parent to `list_segment_members` via (list_id, segment_id)
- Referenced by campaigns for targeted sends

**Duplicates/Repetition**:
- No duplicates - this is configuration data
- Recommendation: Use to understand campaign targeting

**Query Pattern**:
```sql
-- Active segments by list
SELECT
  list_id,
  segment_id,
  name,
  type,
  member_count,
  created_at
FROM list_segments
WHERE list_id = 'abc123'
ORDER BY member_count DESC
```

---

### 18. LIST_SEGMENT_MEMBERS (INACTIVE)
**Purpose**: Members assigned to list segments (SNAPSHOT TABLE)

**Key Information**:
- Current segment membership
- Point-in-time snapshot (not historical)

**Data Pattern**:
- **Type**: **SNAPSHOT - Full refresh every extraction**
- **Incremental Strategy**: None - **date ranges are IGNORED**
- **Load Pattern**: Multi-table extraction (list_group)
- **Status**: **INACTIVE** - Not recommended for use

**Primary Key**: `[mailchimp_account_id, list_id, segment_id, subscriber_hash]`

**Why INACTIVE**:
- **Snapshot only**: No historical tracking
- **No incremental support**: Always fetches complete current state
- **High cardinality**: Very large table (millions of rows)
- **Better alternative**: Use `members` table with tags
- **API limitations**: Expensive to extract, no date filtering

**Recommendation**:
**DO NOT USE THIS TABLE**. Instead:
1. Use `members.tags_count` and parse tags from members table
2. Use `list_segments` to understand segment definitions
3. Use campaign targeting data to understand which segments received campaigns

**Query Pattern**:
```sql
-- DON'T USE THIS TABLE
-- Alternative: Use members table with tags
SELECT
  m.subscriber_hash,
  m.email_address,
  m.tags_count,
  JSON_EXTRACT_ARRAY(m.tags) as tags
FROM members m
WHERE m.list_id = 'abc123'
  AND m.tags_count > 0
```

---

## Relationships and Joins

### Key Join Patterns

#### Campaign Performance Analysis
```sql
-- Full campaign metrics with member details
SELECT
  c.id as campaign_id,
  c.send_time,
  c.emails_sent,
  m.email_address,
  m.member_rating,
  a.action,
  a.activity_timestamp
FROM campaigns c
JOIN campaign_email_activity a ON c.id = a.campaign_id
JOIN members m ON a.subscriber_hash = m.subscriber_hash
  AND a.list_id = m.list_id
WHERE c.send_time >= '2025-06-01'
ORDER BY c.send_time, m.email_address, a.activity_timestamp
```

#### List Subscriber Engagement
```sql
-- Member engagement across all campaigns
SELECT
  m.email_address,
  m.member_rating,
  COUNT(DISTINCT a.campaign_id) as campaigns_received,
  COUNT(DISTINCT CASE WHEN a.action = 'open' THEN a.campaign_id END) as campaigns_opened,
  COUNT(DISTINCT CASE WHEN a.action = 'click' THEN a.campaign_id END) as campaigns_clicked
FROM members m
LEFT JOIN campaign_email_activity a ON m.subscriber_hash = a.subscriber_hash
WHERE m.list_id = 'abc123'
  AND m.status = 'subscribed'
GROUP BY m.email_address, m.member_rating
ORDER BY campaigns_clicked DESC
```

#### Campaign Link Performance
```sql
-- Detailed link analysis with clicker details
SELECT
  cd.url,
  cd.total_clicks,
  cd.unique_clicks,
  cm.email_address,
  cm.clicks as recipient_clicks,
  m.member_rating
FROM campaign_click_details cd
JOIN campaign_click_members cm
  ON cd.campaign_id = cm.campaign_id
  AND cd.link_id = cm.link_id
JOIN members m
  ON cm.subscriber_hash = m.subscriber_hash
WHERE cd.campaign_id = 'abc123'
ORDER BY cd.total_clicks DESC, cm.clicks DESC
```

---

## Data Patterns

### Incremental vs Full Refresh

**Incremental Tables** (date-based updates):
- `campaigns` (send_time)
- `members` (last_changed)
- `unsubscribes` (timestamp)
- `campaign_email_activity` (activity_timestamp)
- `campaign_sent_to` (via campaign send_time)
- `campaign_open_details` (via campaign send_time)
- `campaign_click_details` (via campaign send_time)
- `campaign_click_members` (via campaign send_time)

**Full Refresh Tables** (complete snapshot):
- `lists`
- `metadata_campaigns`
- `metadata_lists`
- `list_merge_fields`
- `list_growth_history`
- `list_activity`
- `list_segments`
- `list_segment_members`
- `campaign_domain_performance`
- `campaign_locations`

### Data Redundancy Matrix

| Data Type | Primary Source | Duplicate Locations | Recommendation |
|-----------|---------------|---------------------|----------------|
| Campaign metadata | `campaigns` | `metadata_campaigns` | Use campaigns |
| List metadata | `lists` | `metadata_lists` | Use lists |
| Member profile | `members` | campaign_* tables (snapshot) | Use members for current, campaign_* for historical |
| Open events | `campaign_email_activity` | `campaign_open_details`, `campaign_sent_to.open_count` | Use campaign_email_activity |
| Click events | `campaign_email_activity` | `campaign_click_details`, `campaign_click_members`, `campaign_sent_to.click_count` | Use campaign_email_activity |
| Unsubscribe events | `campaign_email_activity` | `unsubscribes` | Use both (activity for event, unsubscribes for reason) |
| Sent events | `campaign_email_activity` | `campaign_sent_to` | Use campaign_sent_to for rollups |
| Segment membership | `list_segment_members` | `members.tags` | Use members.tags |
| Campaign metrics | `campaigns.report_summary` | Aggregating campaign_* tables | Aggregate campaign_* tables |
| List metrics | `lists.stats` | Aggregating members + campaigns | Aggregate source tables |

---

## Query Examples

### Campaign Performance Dashboard
```sql
WITH campaign_stats AS (
  SELECT
    c.id as campaign_id,
    c.send_time,
    c.emails_sent,
    COUNT(DISTINCT CASE WHEN a.action = 'open' THEN a.subscriber_hash END) as unique_opens,
    COUNT(DISTINCT CASE WHEN a.action = 'click' THEN a.subscriber_hash END) as unique_clicks,
    COUNT(DISTINCT CASE WHEN a.action = 'bounce' THEN a.subscriber_hash END) as bounces,
    COUNT(DISTINCT CASE WHEN a.action = 'unsub' THEN a.subscriber_hash END) as unsubscribes
  FROM campaigns c
  LEFT JOIN campaign_email_activity a ON c.id = a.campaign_id
  WHERE c.send_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND c.status = 'sent'
  GROUP BY c.id, c.send_time, c.emails_sent
)
SELECT
  campaign_id,
  send_time,
  emails_sent,
  unique_opens,
  unique_clicks,
  bounces,
  unsubscribes,
  ROUND(unique_opens * 100.0 / emails_sent, 2) as open_rate_pct,
  ROUND(unique_clicks * 100.0 / emails_sent, 2) as click_rate_pct,
  ROUND(unique_clicks * 100.0 / unique_opens, 2) as ctr_pct,
  ROUND(bounces * 100.0 / emails_sent, 2) as bounce_rate_pct
FROM campaign_stats
ORDER BY send_time DESC
```

### Member Engagement Scoring
```sql
WITH member_activity AS (
  SELECT
    m.subscriber_hash,
    m.email_address,
    m.member_rating,
    m.status,
    COUNT(DISTINCT a.campaign_id) as campaigns_received,
    COUNT(DISTINCT CASE WHEN a.action = 'open' THEN a.campaign_id END) as campaigns_opened,
    COUNT(DISTINCT CASE WHEN a.action = 'click' THEN a.campaign_id END) as campaigns_clicked,
    MAX(CASE WHEN a.action = 'open' THEN a.activity_timestamp END) as last_open,
    MAX(CASE WHEN a.action = 'click' THEN a.activity_timestamp END) as last_click
  FROM members m
  LEFT JOIN campaign_email_activity a
    ON m.subscriber_hash = a.subscriber_hash
    AND a.activity_timestamp >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
  WHERE m.list_id = 'abc123'
    AND m.status = 'subscribed'
  GROUP BY m.subscriber_hash, m.email_address, m.member_rating, m.status
)
SELECT
  email_address,
  member_rating,
  campaigns_received,
  campaigns_opened,
  campaigns_clicked,
  CASE
    WHEN campaigns_clicked >= 3 THEN 'Highly Engaged'
    WHEN campaigns_opened >= 5 THEN 'Engaged'
    WHEN campaigns_opened >= 1 THEN 'Moderately Engaged'
    WHEN campaigns_received >= 3 THEN 'Low Engagement'
    ELSE 'New Subscriber'
  END as engagement_tier,
  DATE_DIFF(CURRENT_DATE(), DATE(last_open), DAY) as days_since_open,
  DATE_DIFF(CURRENT_DATE(), DATE(last_click), DAY) as days_since_click
FROM member_activity
ORDER BY campaigns_clicked DESC, campaigns_opened DESC
```

### Link Click-Through Analysis
```sql
SELECT
  c.id as campaign_id,
  c.send_time,
  cd.url,
  cd.total_clicks,
  cd.unique_clicks,
  cd.click_percentage,
  COUNT(DISTINCT cm.subscriber_hash) as unique_clickers,
  SUM(cm.clicks) as total_link_clicks,
  ROUND(AVG(m.member_rating), 2) as avg_clicker_rating
FROM campaigns c
JOIN campaign_click_details cd ON c.id = cd.campaign_id
JOIN campaign_click_members cm
  ON cd.campaign_id = cm.campaign_id
  AND cd.link_id = cm.link_id
LEFT JOIN members m ON cm.subscriber_hash = m.subscriber_hash
WHERE c.send_time >= '2025-06-01'
GROUP BY c.id, c.send_time, cd.url, cd.total_clicks, cd.unique_clicks, cd.click_percentage
ORDER BY c.send_time DESC, cd.total_clicks DESC
```

### List Health Metrics
```sql
SELECT
  l.name as list_name,
  COUNT(DISTINCT m.subscriber_hash) as total_subscribers,
  SUM(CASE WHEN m.status = 'subscribed' THEN 1 ELSE 0 END) as active,
  SUM(CASE WHEN m.status = 'unsubscribed' THEN 1 ELSE 0 END) as unsubscribed,
  SUM(CASE WHEN m.status = 'cleaned' THEN 1 ELSE 0 END) as cleaned,
  SUM(CASE WHEN m.member_rating >= 4 THEN 1 ELSE 0 END) as high_engagement,
  SUM(CASE WHEN m.member_rating <= 2 THEN 1 ELSE 0 END) as low_engagement,
  ROUND(AVG(m.member_rating), 2) as avg_rating,
  ROUND(SUM(CASE WHEN m.member_rating >= 4 THEN 1 ELSE 0 END) * 100.0 /
    NULLIF(SUM(CASE WHEN m.status = 'subscribed' THEN 1 ELSE 0 END), 0), 2) as engaged_pct
FROM lists l
LEFT JOIN members m ON l.id = m.list_id
GROUP BY l.id, l.name
ORDER BY total_subscribers DESC
```

---

## Best Practices

### 1. Use Appropriate Tables for Each Use Case

**For real-time dashboards**:
- Use `campaigns`, `members`, `campaign_email_activity`
- Avoid `metadata_*` tables (redundant)

**For historical analysis**:
- Use `campaign_email_activity` (most detailed)
- Join to `members` for current subscriber state

**For aggregated reporting**:
- Use `campaign_sent_to`, `campaign_click_details`
- Pre-aggregated, faster queries

**For configuration/setup**:
- Use `lists`, `list_segments`, `list_merge_fields`

### 2. Incremental Extraction Strategy

**Campaign reports** (campaign_group):
- Extract based on campaign `send_time`
- Use 7-30 day lookback for activity updates
- Full refresh not needed (incremental only)

**List data** (list_group):
- Full refresh recommended (small tables)
- No incremental benefit

**Core master data**:
- `campaigns`, `members`: Incremental by date
- `lists`, `metadata_*`: Full refresh

### 3. Avoid Redundant Tables

**Deprecated/Redundant**:
- `metadata_campaigns` → Use `campaigns`
- `metadata_lists` → Use `lists`
- `list_segment_members` → Use `members.tags`
- `campaign_open_details` → Aggregate `campaign_email_activity`

**Use for Specific Purposes Only**:
- `unsubscribes`: Only when you need reason text
- `campaign_domain_performance`: Only for domain analysis
- `campaign_locations`: Only for geographic analysis

### 4. Query Optimization Tips

**Always filter by**:
- `mailchimp_account_id` (if multi-tenant)
- Date range on incremental_date_fields
- `status = 'subscribed'` for members (unless analyzing unsubscribes)

**Use partitioning** (if available):
- Partition `campaign_email_activity` by `activity_timestamp`
- Partition `members` by `last_changed`

**Pre-aggregate for dashboards**:
- Create materialized views from `campaign_email_activity`
- Cache campaign-level metrics

---

## Summary

### Core Insight: Data Redundancy

The Mailchimp data model has significant redundancy by design:

1. **campaign_email_activity is the source of truth** for all email events
2. **Aggregate tables** (`campaign_sent_to`, `campaign_open_details`, etc.) are pre-computed rollups
3. **Metadata tables** (`metadata_campaigns`, `metadata_lists`) are complete duplicates
4. **Snapshot tables** (`list_segment_members`) capture point-in-time state

### Recommended Table Usage

**Primary Tables** (use these):
- `campaigns` - Campaign metadata
- `lists` - List metadata
- `members` - Subscriber profiles
- `campaign_email_activity` - All email events

**Secondary Tables** (use for specific needs):
- `campaign_sent_to` - Recipient-level rollups
- `campaign_click_details` - Link-level rollups
- `unsubscribes` - Unsubscribe reasons

**Avoid** (redundant or problematic):
- `metadata_campaigns` - Use campaigns instead
- `metadata_lists` - Use lists instead
- `list_segment_members` - Use members.tags instead
- `campaign_open_details` - Aggregate campaign_email_activity instead

### Table Groups Purpose

**core**: Master data (6 tables)
- Extract: Standard, table-by-table
- Frequency: Daily or as needed

**campaign_group**: Campaign reports (7 tables)
- Extract: Multi-table extraction (parallel)
- Frequency: After campaigns sent, with lookback

**list_group**: List configuration (5 tables)
- Extract: Multi-table extraction (parallel)
- Frequency: Weekly or when configuration changes

---

**Document Version**: 1.0
**Last Updated**: 2025-11-16
**Data Model Version**: Mailchimp Marketing API v3.0
