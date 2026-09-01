#!/usr/bin/env python3
"""
DEPRECATED — this script is no longer needed.

Background: previously the UI wrote to sidecar tables `question_mapping` and
`answer_mapping`, and this script bulk-applied those rows into
`lime_surveys_columnar_completed`. That two-step model is the source of the
"orphan mapping" bug (rows in the sidecar tables that never landed in the
fact table).

New model: the UI writes directly to `lime_surveys_columnar_completed`.
`question_mapping` and `answer_mapping` are now VIEWS over that table, so
there is no longer any "pending" state to apply.

If you reach this script via the UI's old Apply Changes page, that page is
also dead code (see `show_apply_changes` in
ui/limesurvey/limesurvey_mapper.py). To rebuild the wide table, run
`shared/limesurvey_build_wide_table.py` directly.
"""

import sys

if __name__ == "__main__":
    sys.stderr.write(__doc__)
    sys.exit(2)
