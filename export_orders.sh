#!/bin/bash

# BizniWeb Order Export Script
# This script sets up the environment and runs the order export

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Check if Python 3 is installed
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 is not installed. Please install Python 3.7 or higher."
    exit 1
fi

print_info "Python 3 found: $(python3 --version)"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    print_info "Creating virtual environment..."
    python3 -m venv venv
else
    print_info "Virtual environment already exists"
fi

# Activate virtual environment
print_info "Activating virtual environment..."
source venv/bin/activate

# Install/upgrade pip
print_info "Upgrading pip..."
pip install --upgrade pip --quiet

# Install requirements
print_info "Installing requirements..."
pip install -r requirements.txt --quiet

# Check if .env file exists
if [ ! -f ".env" ]; then
    if [ -f ".env.example" ]; then
        print_warning ".env file not found. Creating from .env.example..."
        cp .env.example .env
        print_error "Please edit .env file and add your BIZNISWEB_API_TOKEN"
        print_info "Run this script again after adding your API token"
        exit 1
    else
        print_error ".env file not found and no .env.example available"
        exit 1
    fi
fi

# Check if API token is set
if grep -q "your_api_token_here" .env; then
    print_error "Please update the BIZNISWEB_API_TOKEN in .env file"
    exit 1
fi

# Create data directory if it doesn't exist
if [ ! -d "data" ]; then
    print_info "Creating data directory..."
    mkdir -p data
fi

# Parse command line arguments
FROM_DATE=""
TO_DATE=""
HELP=0

while [[ $# -gt 0 ]]; do
    case $1 in
        -f|--from)
            FROM_DATE="$2"
            shift 2
            ;;
        -t|--to)
            TO_DATE="$2"
            shift 2
            ;;
        -h|--help)
            HELP=1
            shift
            ;;
        *)
            print_error "Unknown option: $1"
            HELP=1
            shift
            ;;
    esac
done

# Show help if requested or on error
if [ $HELP -eq 1 ]; then
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -f, --from DATE    Start date in YYYY-MM-DD format (default: 30 days ago)"
    echo "  -t, --to DATE      End date in YYYY-MM-DD format (default: today)"
    echo "  -h, --help         Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                           # Export last 30 days"
    echo "  $0 -f 2024-01-01            # Export from Jan 1, 2024 to today"
    echo "  $0 -f 2024-01-01 -t 2024-01-31  # Export January 2024"
    exit 0
fi

# Build command
CMD="python export_orders.py"

if [ ! -z "$FROM_DATE" ]; then
    CMD="$CMD --from-date $FROM_DATE"
fi

if [ ! -z "$TO_DATE" ]; then
    CMD="$CMD --to-date $TO_DATE"
fi

# Run the export
print_info "Running order export..."
print_info "Command: $CMD"
echo ""

# Execute the command
$CMD

# Check if export was successful
if [ $? -eq 0 ]; then
    echo ""
    print_info "Export completed successfully!"
    print_info "Check the data/ directory for exported files:"
    ls -lh data/*.csv | tail -3
else
    print_error "Export failed. Please check the error messages above."
    exit 1
fi