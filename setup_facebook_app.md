# Setting Up Facebook App for Marketing API

## Create New App (Clean Setup)

1. Go to [developers.facebook.com/apps](https://developers.facebook.com/apps)
2. Click **"Create App"**
3. Choose **"Other"** as app type
4. Select **"Business"** as app purpose
5. Fill in app details:
   - App Name: "BizniWeb Marketing Analytics" (or your choice)
   - App Contact Email: your email
   - Business Account: Select your business account

## Configure the App

1. After creation, you'll be in the app dashboard
2. Click **"Add Product"** in the left sidebar
3. Find and add these products:
   - **Marketing API** - Click "Set Up"
   - **Facebook Login** - Click "Set Up" (needed for token generation)

## Set Up Marketing API

1. Go to **Marketing API → Tools**
2. Click **"Get Token"**
3. Select your ad account
4. Choose permissions:
   - ads_read (required)
   - ads_management (optional)

## For System Users

1. Go to [business.facebook.com](https://business.facebook.com)
2. Business Settings → System Users
3. Create new system user
4. **Important:** After creating, click "Add Assets"
5. Add your Ad Account with "View Performance" role
6. THEN generate token - permissions should now appear

## Test Your Setup

Run this command to verify:
```bash
curl -G \
  -d "access_token=YOUR_TOKEN" \
  -d "fields=name,currency" \
  "https://graph.facebook.com/v21.0/act_YOUR_AD_ACCOUNT_ID"
```

## Common Issues and Fixes

### "No permissions available" for System User
- **Fix:** Make sure the system user is assigned to the ad account BEFORE generating token
- Go to Ad Accounts → Select Account → Assign Users → Add System User

### "Invalid OAuth access token"
- **Fix:** Token might be expired or incorrectly copied
- Regenerate token and copy entire string

### "Application does not have permission for this action"
- **Fix:** App needs to be added to Business Manager
- Business Settings → Apps → Add → Connect Your App

### App is in Development Mode
- Development mode is fine for your own use
- You have full permissions for accounts you admin
- No need to submit for review unless going public

## Quick Workaround

If system user tokens still don't work, use this approach:

1. **Use Page Access Token:**
   - Create a Facebook Page (if you don't have one)
   - Go to Graph API Explorer
   - Select your app
   - Get User Access Token with ads_read permission
   - Exchange for Page Access Token
   - Page tokens can be long-lived and have ad permissions

2. **Use Personal User Token:**
   - Graph API Explorer → Your App
   - Add permissions: ads_read, ads_management
   - Generate token
   - Extend token lifetime using Access Token Debugger

Remember: System User tokens are best for production, but user tokens work fine for internal tools.