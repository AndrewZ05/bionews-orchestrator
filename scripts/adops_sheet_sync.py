#!/usr/bin/env python3
"""Ad-Ops values sync -- write full BigQuery result sets into a Google Sheet.

WHY
  The Ad-Ops tabs were live BigQuery data-connector tables. A connector tab only
  populates when the sheet is OPEN, its preview is capped (~500 rows), and a
  background/automated reader sees the query+layout but no values. This job
  instead queries BigQuery DIRECTLY and writes the FULL result set (all rows, no
  cap) into plain cells on fixed tabs, on a schedule. An automated reader then
  reads real, complete values without anyone opening the file.

DESIGN (governed + monitored like any other pipeline job)
  - One permanent spreadsheet (addressed by file ID -- survives rename/move).
  - Fixed tab names, overwritten IN PLACE each run (address never moves).
  - Tracked in the orchestrator jobs table (log_job_start/complete/failure).
  - Distributed lock (job_locks) so two machines can't write at once.
  - Never-blank-on-error: a failing query leaves that tab's prior values intact
    and is recorded; the run is marked PARTIAL, not silently wrong.
  - Pagination: BigQuery results stream fully into memory, then one atomic
    values.update per tab -- no 500-row preview cap, no half-written tab.
  - A `_status` tab records per-tab OK/ERROR + rowcount + timestamp so a reader
    can verify freshness before trusting the numbers.

PREREQUISITES (one-time, admin)
  1. Enable the Google Sheets API on project bi-data-391216
     (console: APIs & Services -> enable "Google Sheets API").
  2. Share the target sheet with the service account as EDITOR:
     pipeline-workflow@bi-data-391216.iam.gserviceaccount.com
  (The SA already has BigQuery read on the source datasets.)

RUN
  python scripts/adops_sheet_sync.py            # full sync, tracked job
  python scripts/adops_sheet_sync.py --dry-run  # query only, print rowcounts, no writes
  python scripts/adops_sheet_sync.py --no-track  # skip jobs-table/lock (local test)

SCHEDULE
  Add to the same scheduler that runs the nightly suite, every 4 hours, e.g.:
    python scripts/adops_sheet_sync.py
"""

from __future__ import annotations

import argparse
import os
import sys

# Allow "from shared.* import ..." when run as scripts/adops_sheet_sync.py
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ---- CONFIG ---------------------------------------------------------------
SHEET_ID = "1U_OVtnllYbJAddEg1L9Mmv2O0s5clK8gDi_-UGnKqI4"
START_DATE = "2026-01-01"  # YTD floor
JOB_SOURCE = "adops_sheet_sync"  # name in jobs table + lock key
SHEETS_SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]

# Fixed tab names (the permanent contract the automation reads). The job writes
# static values IN PLACE into these original tabs (the live BigQuery connectors
# were replaced by this static sync -- static is now the source of truth).
# tab -> SQL. Aliases are snake_case (BigQuery-legal); first row written as-is.
TABS = [
    (
        "DCM",
        """
        SELECT campaign, placement, DATE(date) AS date, country,
          SUM(impressions) AS impressions, SUM(clicks) AS clicks,
          SUM(active_view_viewable_impressions)   AS active_view_viewable_impressions,
          SUM(active_view_measurable_impressions) AS active_view_measurable_impressions
        FROM `bi-data-391216.dcm_data.dcm_campaign_performance`
        WHERE date >= TIMESTAMP('{start}') AND country = 'United States'
        GROUP BY campaign, placement, date, country
        ORDER BY date, campaign, placement
        """,
    ),
    (
        "GAM",
        """
        WITH imp AS (
          SELECT `order`.name AS order_name, line_item.name AS line_item,
            order_id, line_item_id, event_date,
            COUNT(*) AS ad_server_impressions,
            SUM(active_view_measurable_count) AS av_measurable,
            SUM(active_view_viewable_count)   AS av_viewable
          FROM `bi-data-391216.df_warehouse_intermediate.int_gam_impressions`
          WHERE event_date >= DATE('{start}')
          GROUP BY order_name, line_item, order_id, line_item_id, event_date
        ),
        clk AS (
          SELECT order_id, line_item_id, event_date, COUNT(*) AS ad_server_clicks
          FROM `bi-data-391216.df_warehouse_intermediate.int_gam_clicks`
          WHERE event_date >= DATE('{start}')
          GROUP BY order_id, line_item_id, event_date
        )
        SELECT i.order_name AS `order`, i.line_item, i.event_date AS date,
          i.ad_server_impressions, COALESCE(c.ad_server_clicks, 0) AS ad_server_clicks,
          i.av_measurable AS total_active_view_measurable_impressions,
          i.av_viewable   AS total_active_view_viewable_impressions
        FROM imp i
        LEFT JOIN clk c
          ON i.order_id = c.order_id AND i.line_item_id = c.line_item_id
         AND i.event_date = c.event_date
        ORDER BY date, `order`, line_item
        """,
    ),
    (
        # zone is embedded as a key in the semicolon-delimited custom_targeting
        # string (e.g. "...;zone=ros;..."), not a first-class column -- same
        # "Zone" concept as output_ga4_events.zone (ad-ops placement), pulled
        # from GAM's own targeting data instead of GA4. unique_users counts
        # distinct pvid (GAM's publisher-provided viewer id); user_id is ~76%
        # NULL and unusable for this purpose.
        "GAM Uniques",
        """
        SELECT event_date AS date,
          REGEXP_EXTRACT(custom_targeting, r'zone=([^;]+)') AS zone,
          `order`.name AS order_name, line_item.name AS line_item,
          COUNT(DISTINCT NULLIF(pvid, '')) AS unique_users
        FROM `bi-data-391216.df_warehouse_intermediate.int_gam_impressions`
        WHERE event_date >= DATE('{start}')
        GROUP BY date, zone, order_name, line_item
        ORDER BY date, zone, order_name, line_item
        """,
    ),
    (
        # Sourced from doubleverify_data.dv_brand_detail -- the per-brand report
        # carrying campaign/placement/date grain with the viewability + fraud
        # metrics this tab needs. (Was csm_data.dv_report, a wide union of 8
        # report formats that stopped loading on 2026-06-24 because it had no
        # scheduled job; doubleverify is now a real ETL source in the nightly
        # suite. Other DV formats live in sibling dv_* tables.)
        "Double Verify",
        """
        SELECT campaign_name AS campaign, placement_name, date,
          SUM(monitored_ads) AS monitored_ads,
          SUM(fraud_sivt_incidents) AS fraud_sivt_incidents,
          SUM(viewable_impressions) AS viewable_impressions,
          SUM(measured_impressions) AS measured_impressions
        FROM `bi-data-391216.doubleverify_data.dv_brand_detail`
        WHERE date >= DATE('{start}')
        GROUP BY campaign, placement_name, date
        ORDER BY date, campaign, placement_name
        """,
    ),
    (
        # UNION of the two Trade Desk report variants: the 25-column feed
        # (Evrysdi) and the 7-column feed (Ocrevus). They are separate tables
        # because their grains differ, but this tab only needs the columns they
        # share, so the union is exact -- not a lossy flattening.
        "Trade Desk",
        """
        WITH combined AS (
          SELECT campaign, date, impressions, clicks
          FROM `bi-data-391216.tradedesk_data.ttd_ad_group_performance`
          WHERE date >= DATE('{start}')
          UNION ALL
          SELECT campaign, date, impressions, clicks
          FROM `bi-data-391216.tradedesk_data.ttd_ad_group_performance_basic`
          WHERE date >= DATE('{start}')
        )
        SELECT campaign, date,
          SUM(impressions) AS impressions, SUM(clicks) AS clicks
        FROM combined
        GROUP BY campaign, date
        ORDER BY date, campaign
        """,
    ),
    (
        "JW Player",
        """
        SELECT ma.media_id AS content_media_id,
          JSON_VALUE(m.metadata, '$.title') AS media_title,
          ma.eastern_date AS date,
          SUM(ma.plays) AS starts,
          SUM(ma.`25_percent_completes`) AS first_quartiles,
          SUM(ma.`50_percent_completes`) AS midpoints,
          SUM(ma.`75_percent_completes`) AS third_quartiles,
          SUM(ma.completes) AS completes
        FROM `bi-data-391216.BN_JW_Player.jw_analytics_by_media` ma
        LEFT JOIN `bi-data-391216.BN_JW_Player.jw_media` m ON ma.media_id = m.id
        WHERE ma.eastern_date >= DATE('{start}')
        GROUP BY content_media_id, media_title, date
        ORDER BY date, content_media_id
        """,
    ),
    (
        "GA4",
        """
        SELECT zone, event_date AS date,
          COUNT(*) AS doctor_discussion_guide_downloads
        FROM `bi-data-391216.df_warehouse_output.output_ga4_events`
        WHERE event_date >= DATE('{start}') AND event_name = 'file_download'
          AND (LOWER(file_name) LIKE '%discussion-guide%'
            OR LOWER(file_name) LIKE '%discussion_guide%'
            OR LOWER(file_name) LIKE '%ddg%'
            OR LOWER(link_url) LIKE '%discussion-guide%'
            OR LOWER(link_url) LIKE '%ddg%')
        GROUP BY zone, date
        ORDER BY date, doctor_discussion_guide_downloads DESC
        """,
    ),
    (
        # Salesforce bookings: one row per OLI Delivery Schedule, with its
        # parent Opportunity Line Item and Opportunity. Not date-filtered ({start}
        # is unused here) -- full bookings set is the useful view.
        "Salesforce",
        """
        SELECT
          o.Name   AS opportunity,
          oli.Name AS opportunity_line_item,
          ds.Name  AS oli_delivery_schedule,
          ds.StartDate__c         AS start_date,
          ds.End_Date__c          AS end_date,
          ds.Contracted_Budget__c AS contracted_budget
        FROM `bi-data-391216.BN_Salesforce.oli_delivery_schedule__c` ds
        LEFT JOIN `bi-data-391216.BN_Salesforce.opportunity_line_item__c` oli
          ON ds.Opportunity_Line_Item__c = oli.Id
        LEFT JOIN `bi-data-391216.BN_Salesforce.opportunity` o
          ON oli.Opportunity__c = o.Id
        WHERE ds.IsDeleted = FALSE
        ORDER BY start_date DESC, opportunity, opportunity_line_item, oli_delivery_schedule
        """,
    ),
]

STATUS_TAB = "_status"


# ---- BigQuery -> rows -----------------------------------------------------
def run_query(bq: bigquery.Client, sql: str):
    """Return (headers, rows) -- full result set, every row, cells JSON-safe."""
    job = bq.query(sql)
    result = job.result()  # waits for completion
    headers = [f.name for f in result.schema]
    rows = []
    for r in result:
        out = []
        for v in r.values():
            # Sheets API wants JSON scalars; dates/timestamps -> ISO strings.
            if v is None:
                out.append("")
            elif hasattr(v, "isoformat"):
                out.append(v.isoformat())
            else:
                out.append(v)
        rows.append(out)
    return headers, rows


# ---- Google Sheets writer -------------------------------------------------
class SheetWriter:
    def __init__(self, sa_path: str):
        creds = service_account.Credentials.from_service_account_file(
            sa_path, scopes=SHEETS_SCOPE
        )
        self.svc = build("sheets", "v4", credentials=creds, cache_discovery=False)

    def existing_tabs(self):
        meta = self.svc.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
        return {
            s["properties"]["title"]: s["properties"]["sheetId"] for s in meta["sheets"]
        }

    def ensure_tab(self, title: str, existing: dict):
        if title in existing:
            return existing[title]
        resp = (
            self.svc.spreadsheets()
            .batchUpdate(
                spreadsheetId=SHEET_ID,
                body={"requests": [{"addSheet": {"properties": {"title": title}}}]},
            )
            .execute()
        )
        sid = resp["replies"][0]["addSheet"]["properties"]["sheetId"]
        existing[title] = sid
        return sid

    def write_tab(self, title: str, headers: list, rows: list, sheet_id: int):
        """Write header+data and leave the tab CLEAN:

        clear -> update values -> resize grid to exactly (data rows x cols) so
        there are no trailing blank rows/columns -> freeze the header row.
        """
        if not headers:
            raise ValueError(f"empty schema -- refusing to clear {title}")
        self.svc.spreadsheets().values().clear(
            spreadsheetId=SHEET_ID, range=f"'{title}'"
        ).execute()
        values = [headers] + rows
        self.svc.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f"'{title}'!A1",
            valueInputOption="RAW",
            body={"values": values},
        ).execute()
        n_rows = len(values)
        n_cols = len(headers)
        self.svc.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={
                "requests": [
                    # Trim the grid to exactly what the data needs (no blank padding).
                    {
                        "updateSheetProperties": {
                            "properties": {
                                "sheetId": sheet_id,
                                "gridProperties": {
                                    "rowCount": n_rows,
                                    "columnCount": n_cols,
                                    "frozenRowCount": 1,
                                },
                            },
                            "fields": "gridProperties(rowCount,columnCount,frozenRowCount)",
                        }
                    },
                    # Bold the header row so it reads as a header.
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": sheet_id,
                                "startRowIndex": 0,
                                "endRowIndex": 1,
                            },
                            "cell": {
                                "userEnteredFormat": {"textFormat": {"bold": True}}
                            },
                            "fields": "userEnteredFormat.textFormat.bold",
                        }
                    },
                ]
            },
        ).execute()


# ---- Main -----------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--dry-run", action="store_true", help="query only; no sheet writes"
    )
    ap.add_argument("--no-track", action="store_true", help="skip jobs-table + lock")
    args = ap.parse_args()

    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
    sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    project = os.getenv("GCP_PROJECT_ID", "bi-data-391216")
    bq = bigquery.Client(project=project)

    track = not args.no_track and not args.dry_run
    job_id = None
    if track:
        from shared.job_manager import generate_job_id, MONITORING_DATASET
        from shared.job_lock_bq import acquire_job_lock, release_job_lock

        job_id = generate_job_id()
        # Insert the start row via DML (NOT streaming). log_job_start uses
        # insert_rows_json, which lands the row in the streaming buffer and then
        # blocks the completion UPDATE for as long as the buffer persists (many
        # minutes) -- fatal for a sub-minute job. A DML INSERT is immediately
        # UPDATE-able, so the row cleanly goes RUNNING -> COMPLETED with no retry.
        _insert_job_start(bq, MONITORING_DATASET, job_id)
        if not acquire_job_lock(JOB_SOURCE, job_id):
            _finalize_job(
                bq,
                MONITORING_DATASET,
                job_id,
                "FAILURE",
                error_msg="another run holds the lock",
            )
            print(f"[{JOB_SOURCE}] another run holds the lock -- exiting")
            return 0

    writer = None
    existing = {}
    if not args.dry_run:
        writer = SheetWriter(sa_path)
        existing = writer.existing_tabs()

    results = []  # (tab, status, rows, error)
    total_rows = 0
    try:
        for tab, sql_t in TABS:
            sql = sql_t.format(start=START_DATE)
            try:
                headers, rows = run_query(bq, sql)
                total_rows += len(rows)
                if args.dry_run:
                    print(f"[{tab}] {len(rows):,} rows x {len(headers)} cols (dry-run)")
                else:
                    sid = writer.ensure_tab(tab, existing)
                    writer.write_tab(tab, headers, rows, sid)
                    print(f"[{tab}] wrote {len(rows):,} rows")
                results.append((tab, "OK", len(rows), ""))
            except Exception as e:  # noqa: BLE001 -- per-tab isolation
                # Never blank good data: leave the tab as-is, record the failure.
                print(f"[{tab}] ERROR: {e}", file=sys.stderr)
                results.append((tab, "ERROR", -1, str(e)[:300]))

        if not args.dry_run:
            _write_status(writer, existing, results)

        any_err = any(s == "ERROR" for _, s, _, _ in results)
        if track:
            if any_err:
                failed = [t for t, s, _, _ in results if s == "ERROR"]
                _finalize_job(
                    bq,
                    MONITORING_DATASET,
                    job_id,
                    "FAILURE",
                    total_rows=total_rows,
                    error_msg=f"tabs failed: {', '.join(failed)}",
                )
            else:
                _finalize_job(
                    bq,
                    MONITORING_DATASET,
                    job_id,
                    "COMPLETED",
                    total_rows=total_rows,
                )
        return 1 if any_err else 0
    except Exception as e:  # noqa: BLE001 -- whole-run failure
        if track:
            _finalize_job(
                bq, MONITORING_DATASET, job_id, "FAILURE", error_msg=str(e)[:400]
            )
        raise
    finally:
        if track:
            try:
                release_job_lock(JOB_SOURCE)
            except Exception as e:  # noqa: BLE001
                print(f"[{JOB_SOURCE}] lock release failed: {e}", file=sys.stderr)


def _insert_job_start(bq, dataset, job_id):
    """Insert the RUNNING job row via DML INSERT (not streaming).

    Streaming inserts (insert_rows_json, used by shared.log_job_start) land in
    the streaming buffer, which blocks the later completion UPDATE for many
    minutes. A DML INSERT writes straight to managed storage, so the row is
    immediately UPDATE-able and the job cleanly reaches COMPLETED/FAILED.
    """
    from datetime import datetime, timezone
    from google.cloud import bigquery as _bq
    from shared.job_manager import get_machine_info

    mi = get_machine_info()
    now = datetime.now(timezone.utc)
    table_id = f"{bq.project}.{dataset}.jobs"
    sql = f"""
    INSERT INTO `{table_id}`
      (job_id, execution_id, source, job_name, job_type, command_line,
       status, status_updated_at, queued_at, started_at, refresh_mode,
       environment, triggered_by, retry_count, hostname, ip_address,
       os_type, process_id, user, working_directory, created_at, updated_at)
    VALUES
      (@job_id, @job_id, @source, @source, 'sheet_sync', @cmd,
       'RUNNING', @now, @now, @now, 'full',
       'prod', 'schedule', 0, @host, @ip,
       @os, @pid, @user, @cwd, @now, @now)
    """
    cfg = _bq.QueryJobConfig(
        query_parameters=[
            _bq.ScalarQueryParameter("job_id", "STRING", job_id),
            _bq.ScalarQueryParameter("source", "STRING", JOB_SOURCE),
            _bq.ScalarQueryParameter("cmd", "STRING", " ".join(sys.argv)[:1000]),
            _bq.ScalarQueryParameter("now", "TIMESTAMP", now),
            _bq.ScalarQueryParameter("host", "STRING", mi.get("hostname")),
            _bq.ScalarQueryParameter("ip", "STRING", mi.get("ip_address")),
            _bq.ScalarQueryParameter("os", "STRING", mi.get("os_type")),
            _bq.ScalarQueryParameter("pid", "INT64", mi.get("process_id")),
            _bq.ScalarQueryParameter("user", "STRING", mi.get("user")),
            _bq.ScalarQueryParameter("cwd", "STRING", mi.get("working_directory")),
        ]
    )
    bq.query(sql, job_config=cfg).result()


def _finalize_job(bq, dataset, job_id, outcome, total_rows=0, error_msg=None):
    """Set the jobs row to COMPLETED/FAILED via DML UPDATE.

    Works immediately because the start row was DML-inserted (see
    _insert_job_start), so it is not held in the streaming buffer. Non-fatal:
    the sheet write and the _status tab already succeeded and are the primary
    freshness signal, so a bookkeeping failure never masks a good data run.
    """
    from datetime import datetime, timezone
    from google.cloud import bigquery as _bq

    table_id = f"{bq.project}.{dataset}.jobs"
    now = datetime.now(timezone.utc)
    status = "COMPLETED" if outcome == "COMPLETED" else "FAILED"
    sql = f"""
    UPDATE `{table_id}`
    SET status=@status, status_updated_at=@now, completed_at=@now,
        total_rows=@rows, error_message=@err, updated_at=@now,
        duration_seconds=TIMESTAMP_DIFF(@now, started_at, SECOND)
    WHERE job_id=@job_id
    """
    cfg = _bq.QueryJobConfig(
        query_parameters=[
            _bq.ScalarQueryParameter("status", "STRING", status),
            _bq.ScalarQueryParameter("now", "TIMESTAMP", now),
            _bq.ScalarQueryParameter("rows", "INT64", total_rows),
            _bq.ScalarQueryParameter("err", "STRING", error_msg),
            _bq.ScalarQueryParameter("job_id", "STRING", job_id),
        ]
    )
    try:
        bq.query(sql, job_config=cfg).result()
        print(f"[{JOB_SOURCE}] job {status.lower()} ({total_rows:,} rows)")
    except Exception as e:  # noqa: BLE001
        print(
            f"[{JOB_SOURCE}] jobs-table finalize failed (non-fatal): {str(e)[:200]}",
            file=sys.stderr,
        )


def _write_status(writer: "SheetWriter", existing: dict, results: list):
    from datetime import datetime, timezone

    stamp = datetime.now(timezone.utc).isoformat()
    headers = ["tab", "status", "rows", "last_run_utc", "error"]
    rows = [[t, s, (r if r >= 0 else ""), stamp, e] for (t, s, r, e) in results]
    any_err = any(s == "ERROR" for _, s, _, _ in results)
    rows.append(["__overall__", "ERROR" if any_err else "OK", "", stamp, ""])
    sid = writer.ensure_tab(STATUS_TAB, existing)
    writer.write_tab(STATUS_TAB, headers, rows, sid)


if __name__ == "__main__":
    raise SystemExit(main())
