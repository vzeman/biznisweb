#!/bin/bash

# BizniWeb Export System - Setup Script
# This script sets up the environment and installs all required dependencies

echo "========================================="
echo "BizniWeb Export System Setup"
echo "========================================="

# Detect if virtual environment exists
if [ -d "venv" ]; then
    echo "✅ Virtual environment found"
    PIP_CMD="./venv/bin/pip"
    PYTHON_CMD="./venv/bin/python"
else
    echo "⚠️  No virtual environment found. Using system Python."
    echo "   Consider creating a virtual environment:"
    echo "   python3 -m venv venv"
    echo ""
    PIP_CMD="pip3"
    PYTHON_CMD="python3"
fi

# Check if pip is available
if ! command -v $PIP_CMD &> /dev/null; then
    echo "❌ pip not found. Please install Python and pip first."
    exit 1
fi

echo ""
echo "📦 Installing required packages..."
echo "---------------------------------"

# Upgrade pip first
echo "Upgrading pip..."
$PIP_CMD install --upgrade pip

# Install core requirements
echo ""
echo "Installing core packages..."
$PIP_CMD install -r requirements.txt

# Check installation status
echo ""
echo "🔍 Verifying installation..."
echo "----------------------------"

# Test core imports
$PYTHON_CMD -c "
import sys
packages_ok = True

try:
    from dotenv import load_dotenv
    print('✅ python-dotenv installed')
except ImportError:
    print('❌ python-dotenv NOT installed')
    packages_ok = False

try:
    from gql import gql, Client
    print('✅ gql installed')
except ImportError:
    print('❌ gql NOT installed')
    packages_ok = False

try:
    import pandas
    print('✅ pandas installed')
except ImportError:
    print('❌ pandas NOT installed')
    packages_ok = False

try:
    import requests
    print('✅ requests installed')
except ImportError:
    print('❌ requests NOT installed')
    packages_ok = False

# Optional packages
try:
    from google.ads.googleads.client import GoogleAdsClient
    print('✅ google-ads installed')
except ImportError:
    print('⚠️  google-ads NOT installed (optional)')

try:
    import google_auth_oauthlib
    print('✅ google-auth-oauthlib installed')
except ImportError:
    print('⚠️  google-auth-oauthlib NOT installed (optional)')

sys.exit(0 if packages_ok else 1)
"

if [ $? -eq 0 ]; then
    echo ""
    echo "========================================="
    echo "✅ Setup completed successfully!"
    echo "========================================="
    echo ""
    echo "Next steps:"
    echo "1. Copy .env.example to .env and add your credentials:"
    echo "   cp .env.example .env"
    echo ""
    echo "2. Edit .env and add:"
    echo "   - BIZNISWEB_API_TOKEN"
    echo "   - Google Ads credentials (optional)"
    echo "   - Facebook Ads credentials (optional)"
    echo ""
    echo "3. For Google Ads setup, run:"
    echo "   $PYTHON_CMD google_ads.py --setup"
    echo ""
    echo "4. Run the export:"
    echo "   $PYTHON_CMD export_orders.py --from-date 2025-05-01 --to-date 2025-08-27"
    echo ""
else
    echo ""
    echo "❌ Some packages failed to install."
    echo "Please check the error messages above and try:"
    echo "  $PIP_CMD install -r requirements.txt"
    exit 1
fi