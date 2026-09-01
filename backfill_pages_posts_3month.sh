#!/bin/bash
# Facebook Pages/Posts/Insights Backfill - 3 Month Increments
# Runs sequential extraction batches to restore historical data
# Date: 2026-05-12

set -e

echo "================================================================================"
echo "Facebook Backfill: Pages, Posts, Page Insights, Post Insights"
echo "3-Month Increments: Jan 2025 - May 2026"
echo "================================================================================"

LOG_DIR="/c/orchestrator/logs/backfill"
mkdir -p "$LOG_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
MASTER_LOG="$LOG_DIR/backfill_pages_posts_${TIMESTAMP}.log"

# Array of backfill batches
declare -a BATCHES=(
    "2025-01-01:2025-03-31"
    "2025-04-01:2025-06-29"
    "2025-06-30:2025-09-27"
    "2025-09-28:2025-12-26"
    "2025-12-27:2026-03-26"
    "2026-03-27:2026-05-09"
)

# Initialize master log
{
    echo "================================================================================"
    echo "Facebook Backfill Job Started: $(date)"
    echo "================================================================================"
    echo ""
    echo "Backfill Schedule (6 batches, 3 months each):"
    echo ""
    for batch in "${BATCHES[@]}"; do
        IFS=':' read -r start end <<< "$batch"
        echo "  Batch: $start to $end"
    done
    echo ""
    echo "================================================================================"
    echo ""
} | tee "$MASTER_LOG"

# Track success/failure
FAILED_BATCHES=()
SUCCESSFUL_BATCHES=()

# Run each batch
for i in "${!BATCHES[@]}"; do
    BATCH_NUM=$((i + 1))
    IFS=':' read -r START_DATE END_DATE <<< "${BATCHES[$i]}"

    BATCH_LOG="$LOG_DIR/batch_${BATCH_NUM}_${START_DATE}_${END_DATE}.log"

    echo "" | tee -a "$MASTER_LOG"
    echo "================================================================================" | tee -a "$MASTER_LOG"
    echo "Batch $BATCH_NUM/$((${#BATCHES[@]})): $START_DATE to $END_DATE" | tee -a "$MASTER_LOG"
    echo "================================================================================" | tee -a "$MASTER_LOG"
    echo "Start time: $(date)" | tee -a "$MASTER_LOG"
    echo "" | tee -a "$MASTER_LOG"

    if python orchestrate.py \
        --source facebook \
        --env prod \
        --tables pages posts page_insights post_insights \
        --start-date "$START_DATE" \
        --end-date "$END_DATE" \
        2>&1 | tee "$BATCH_LOG"; then

        echo "" | tee -a "$MASTER_LOG"
        echo "✓ Batch $BATCH_NUM completed successfully" | tee -a "$MASTER_LOG"
        SUCCESSFUL_BATCHES+=("$START_DATE:$END_DATE")

    else
        echo "" | tee -a "$MASTER_LOG"
        echo "✗ Batch $BATCH_NUM FAILED" | tee -a "$MASTER_LOG"
        FAILED_BATCHES+=("$START_DATE:$END_DATE")
        # Continue with next batch instead of exiting
    fi

    echo "End time: $(date)" | tee -a "$MASTER_LOG"
    echo "" | tee -a "$MASTER_LOG"

    # Add delay between batches to avoid rate limiting
    if [ $BATCH_NUM -lt ${#BATCHES[@]} ]; then
        echo "Waiting 60 seconds before next batch..." | tee -a "$MASTER_LOG"
        sleep 60
    fi
done

# Summary
echo "" | tee -a "$MASTER_LOG"
echo "================================================================================" | tee -a "$MASTER_LOG"
echo "Backfill Complete: $(date)" | tee -a "$MASTER_LOG"
echo "================================================================================" | tee -a "$MASTER_LOG"
echo "" | tee -a "$MASTER_LOG"
echo "Results Summary:" | tee -a "$MASTER_LOG"
echo "  Successful: ${#SUCCESSFUL_BATCHES[@]}/${#BATCHES[@]} batches" | tee -a "$MASTER_LOG"
echo "  Failed: ${#FAILED_BATCHES[@]}/${#BATCHES[@]} batches" | tee -a "$MASTER_LOG"
echo "" | tee -a "$MASTER_LOG"

if [ ${#SUCCESSFUL_BATCHES[@]} -gt 0 ]; then
    echo "✓ Successful batches:" | tee -a "$MASTER_LOG"
    for batch in "${SUCCESSFUL_BATCHES[@]}"; do
        echo "    $batch" | tee -a "$MASTER_LOG"
    done
    echo "" | tee -a "$MASTER_LOG"
fi

if [ ${#FAILED_BATCHES[@]} -gt 0 ]; then
    echo "✗ Failed batches:" | tee -a "$MASTER_LOG"
    for batch in "${FAILED_BATCHES[@]}"; do
        echo "    $batch" | tee -a "$MASTER_LOG"
    done
    echo "" | tee -a "$MASTER_LOG"
    echo "Check logs in $LOG_DIR for error details" | tee -a "$MASTER_LOG"
fi

echo "================================================================================" | tee -a "$MASTER_LOG"
echo "Master log: $MASTER_LOG" | tee -a "$MASTER_LOG"
echo "================================================================================" | tee -a "$MASTER_LOG"

# Exit with failure if any batches failed
if [ ${#FAILED_BATCHES[@]} -gt 0 ]; then
    exit 1
fi

exit 0
