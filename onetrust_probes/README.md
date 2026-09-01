OneTrust API Temporary Probes
=============================

These scripts are intentionally lightweight and disposable. Their only goal is to
verify which OneTrust API modules/endpoints are accessible with current OAuth
credentials and to capture response shapes for DW planning.

Prerequisites
-------------
- Python 3.9+
- Environment variables:
  - ONETRUST_HOSTNAME (e.g., your-org.onetrust.com)
  - ONETRUST_CLIENT_ID
  - ONETRUST_CLIENT_SECRET
  - Optional: ONETRUST_SCOPES (space-separated)

Quick Start
-----------
1) Verify OAuth and scopes
   python onetrust_probes/auth_probe.py

2) Access management (users, login audit)
   python onetrust_probes/access_audit_probe.py --max-pages 2

3) Consent core (subjects, purposes, collection points, preference centers)
   python onetrust_probes/consent_core_probe.py --max-pages 2

4) Receipts and preferences (may require identifier)
   python onetrust_probes/receipts_preferences_probe.py --identifier "user@example.com"

5) DSR requests + subtasks/audit
   python onetrust_probes/dsr_probe.py --language en-us --max-pages 2

6) Assessments + export detail
   python onetrust_probes/assessments_probe.py --max-pages 2

7) Generic endpoints (for any module in the API reference)
   python onetrust_probes/generic_probe.py --endpoint "/api/access/v2/users"

Notes
-----
- These probes do NOT write to data stores. They only print summaries and
  optionally dump JSON responses for inspection.
- Keep max pages low; raise only when you need full sampling.
- OneTrust modules are tenant-licensed. A 403/404 typically means the module is
  not enabled or the OAuth client lacks scopes.
