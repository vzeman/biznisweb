# Google Ads API Setup Guide

This guide will help you set up Google Ads API integration to fetch advertising spend data for your reports.

## Prerequisites

1. A Google Ads account with active campaigns
2. A Google Cloud Console account
3. Python with required packages installed

## Step 1: Install Required Packages

```bash
pip install google-ads google-auth-oauthlib google-auth-httplib2
```

Or using the virtual environment:

```bash
./venv/bin/pip install google-ads google-auth-oauthlib google-auth-httplib2
```

## Step 2: Get Google Ads Developer Token

### Method 1: Direct URL (Easiest)
1. Log in to your Google Ads account
2. Go directly to: https://ads.google.com/aw/apicenter
3. If you see "API Center", proceed to step 4
4. If redirected or error, try Method 2

### Method 2: Through Google Ads Interface
1. Log in to your Google Ads account at https://ads.google.com
2. Click on **Tools & Settings** (wrench icon in top right)
3. Look under the **SETUP** column for **API Center**
   - If you don't see it, you may need to:
     - Switch to Expert Mode (not Smart Mode)
     - Use a Manager Account (MCC)
     - Or use Method 3

### Method 3: Create a Test Developer Token (Recommended for Testing)
1. You can use a test token that works with test accounts
2. Go to https://developers.google.com/google-ads/api/docs/first-call/dev-token
3. For testing purposes, you can use any string as a developer token temporarily
4. Note: Test tokens only work with test accounts, not production data

### Getting Your Production Developer Token
If API Center is not visible:
1. Create a Manager Account (MCC) at: https://ads.google.com/intl/en_us/home/tools/manager-accounts/
2. Once created, access API Center through the Manager Account
3. Apply for Basic Access (immediate approval for testing)
4. The token format will be like: `1234567890abcdefABCDEF`

### Alternative: Use Existing Token
If you already have Google Ads API access through another tool or service, you can reuse that developer token.

## Step 3: Create OAuth2 Credentials in Google Cloud Console

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Enable the Google Ads API:
   - Go to **APIs & Services** > **Library**
   - Search for "Google Ads API"
   - Click on it and press **Enable**
4. Create OAuth2 credentials:
   - Go to **APIs & Services** > **Credentials**
   - Click **+ CREATE CREDENTIALS** > **OAuth client ID**
   - If prompted, configure the OAuth consent screen first:
     - Choose "External" user type
     - Fill in required fields (app name, support email, etc.)
     - Add your email to test users
   - For Application type, select **Desktop app**
   - Name it (e.g., "BizniWeb Ads Integration")
   - Click **Create**
5. Download the credentials JSON file by clicking the download button next to your OAuth2 client

## Step 4: Generate Refresh Token

1. Run the setup script:

```bash
python google_ads.py --setup
```

Or with virtual environment:

```bash
./venv/bin/python google_ads.py --setup
```

2. When prompted, enter the path to your downloaded OAuth2 credentials JSON file
3. A browser window will open for authentication:
   - Log in to your Google account
   - Grant access to your Google Ads data
   - You'll be redirected to localhost (this is expected)
4. Copy the full URL from your browser's address bar
5. Paste it back into the terminal when prompted
6. The script will display your credentials - copy these values

## Step 5: Configure Environment Variables

1. Edit your `.env` file (copy from `.env.example` if needed)
2. Add the Google Ads credentials:

```env
# Google Ads API credentials
GOOGLE_ADS_DEVELOPER_TOKEN=your_developer_token_here
GOOGLE_ADS_CLIENT_ID=your_client_id_from_setup
GOOGLE_ADS_CLIENT_SECRET=your_client_secret_from_setup
GOOGLE_ADS_REFRESH_TOKEN=your_refresh_token_from_setup
GOOGLE_ADS_CUSTOMER_ID=1234567890  # Your Google Ads account ID without dashes

# Optional: For MCC (Manager) accounts only
# GOOGLE_ADS_LOGIN_CUSTOMER_ID=9876543210
```

### Finding Your Customer ID

1. Log in to Google Ads
2. Look at the top right corner of the page
3. Your Customer ID is displayed there (format: XXX-XXX-XXXX)
4. Enter it without dashes in the `.env` file

### For Manager (MCC) Accounts

If you're using a Manager account to access client accounts:
1. Set `GOOGLE_ADS_CUSTOMER_ID` to the client account ID you want to access
2. Set `GOOGLE_ADS_LOGIN_CUSTOMER_ID` to your Manager account ID

## Step 6: Test the Connection

Run the test script to verify everything is working:

```bash
python google_ads.py
```

Or with virtual environment:

```bash
./venv/bin/python google_ads.py
```

You should see:
- Successful connection message
- Your account name and currency
- Last 7 days of ad spend data
- Campaign performance data

## Step 7: Run the Export with Google Ads Data

Now when you run the export script, it will automatically fetch Google Ads data:

```bash
python export_orders.py --from-date 2025-05-01 --to-date 2025-08-27
```

Or with virtual environment:

```bash
./venv/bin/python export_orders.py --from-date 2025-05-01 --to-date 2025-08-27
```

The report will now include:
- Google Ads daily spend alongside Facebook Ads
- Combined advertising metrics
- Total marketing costs from both platforms
- Updated ROI calculations including all advertising expenses
- New charts comparing FB vs Google Ads performance

## Troubleshooting

### "Google Ads library not installed"
- Install the required package: `pip install google-ads`

### "Authentication failed" 
- Regenerate your refresh token by running `python google_ads.py --setup` again
- Make sure you're using the correct Google account

### "Customer not found"
- Verify your Customer ID is correct and without dashes
- Check if you need to use a Login Customer ID (for MCC accounts)

### "Insufficient permissions"
- Make sure your developer token is approved
- Verify the OAuth token has ads_read scope
- Check if the Google account has access to the Ads account

### No data returned
- Verify you have active campaigns with spend in the date range
- Check if your account currency matches the expected format
- Try a more recent date range (last 7 days)

## Data Caching

The Google Ads integration includes smart caching:
- Data older than 3 days is cached locally to reduce API calls
- Recent data is always fetched fresh to ensure accuracy
- Cache files are stored in `data/cache/google_ads/`
- To force fresh data, use: `python export_orders.py --no-cache`
- To clear all cache: `python export_orders.py --clear-cache`

## API Limits

Google Ads API has the following limits:
- Basic Access: 15,000 operations per day
- Standard Access: Multiple million operations per day
- Each query counts as one or more operations

The integration is optimized to minimize API usage through:
- Daily aggregation queries (one API call per date range)
- Local caching of historical data
- Efficient GAQL queries

## Support

For issues with:
- **This integration**: Check the error messages and this guide
- **Google Ads API**: Visit [Google Ads API Documentation](https://developers.google.com/google-ads/api/docs/start)
- **Developer Token**: Contact Google Ads API support
- **OAuth Issues**: Check Google Cloud Console settings