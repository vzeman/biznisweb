#!/bin/bash
# Generate invoices for BizniWeb orders
# This script is intended to be run daily via cron

# Set script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"

# Change to script directory
cd "$SCRIPT_DIR"

# Activate virtual environment
source venv/bin/activate

# Parse command line arguments
FROM_DATE=""
TO_DATE=""
DRY_RUN=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --from-date)
            FROM_DATE="$2"
            shift 2
            ;;
        --to-date)
            TO_DATE="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN="--dry-run"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--from-date YYYY-MM-DD] [--to-date YYYY-MM-DD] [--dry-run]"
            exit 1
            ;;
    esac
done

# If no from-date provided, use default of 2025-07-29
if [ -z "$FROM_DATE" ]; then
    FROM_DATE="2025-07-29"
fi

# Build the command
CMD="python generate_invoices.py --from-date $FROM_DATE"

# Add to-date if provided
if [ ! -z "$TO_DATE" ]; then
    CMD="$CMD --to-date $TO_DATE"
fi

# Add dry-run if specified
if [ ! -z "$DRY_RUN" ]; then
    CMD="$CMD $DRY_RUN"
fi

# Run invoice generation
echo "$(date): Starting invoice generation..."
echo "Command: $CMD"
$CMD

# Check exit status
if [ $? -eq 0 ]; then
    echo "$(date): Invoice generation completed successfully"
else
    echo "$(date): Invoice generation failed with error code $?"
fi

# Deactivate virtual environment
deactivate