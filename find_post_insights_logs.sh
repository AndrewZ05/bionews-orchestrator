#!/bin/bash
cd c:/orchestrator/logs
echo "Finding post_insights extraction logs..."
grep -l "Processing table: post_insights" *.log | while read file; do
    date=$(head -4 "$file" | grep "^2024" | head -1 | cut -d' ' -f1)
    echo "$date - $file"
done | sort -r | head -10
