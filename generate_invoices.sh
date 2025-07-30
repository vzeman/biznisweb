#!/bin/bash
# Generate invoices for BizniWeb orders
# This script is intended to be run daily via cron

# Set script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"

# Change to script directory
cd "$SCRIPT_DIR"

# Activate virtual environment
source venv/bin/activate

# Run invoice generation for last 7 days
echo "$(date): Starting invoice generation..."
python generate_invoices.py

# Check exit status
if [ $? -eq 0 ]; then
    echo "$(date): Invoice generation completed successfully"
else
    echo "$(date): Invoice generation failed with error code $?"
fi

# Deactivate virtual environment
deactivate