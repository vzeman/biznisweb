#\!/usr/bin/env python3
"""
Alternative method to get Facebook Access Token using browser flow
"""

import webbrowser
import urllib.parse

def generate_oauth_url(app_id, redirect_uri="https://developers.facebook.com/tools/explorer/"):
    """Generate Facebook OAuth URL with required permissions"""
    
    base_url = "https://www.facebook.com/v21.0/dialog/oauth"
    
    params = {
        "client_id": app_id,
        "redirect_uri": redirect_uri,
        "scope": "ads_read,ads_management,business_management,email",
        "response_type": "token"
    }
    
    url = f"{base_url}?{urllib.parse.urlencode(params)}"
    return url

def main():
    print("Facebook Access Token Generator")
    print("="*50)
    print("\nThis will help you generate an access token with the right permissions.")
    print("\nYou'll need:")
    print("1. Your Facebook App ID")
    print("2. Be logged into Facebook as an admin of the ad account")
    
    app_id = input("\nEnter your Facebook App ID: ").strip()
    
    if not app_id:
        print("❌ App ID is required")
        return
    
    oauth_url = generate_oauth_url(app_id)
    
    print("\n" + "="*50)
    print("Instructions:")
    print("1. A browser window will open")
    print("2. Approve the permissions")
    print("3. You'll be redirected to Graph API Explorer")
    print("4. Copy the access token from the URL or page")
    print("="*50)
    
    input("\nPress Enter to open the browser...")
    
    webbrowser.open(oauth_url)
    
    print("\n✅ Browser opened\!")
    print("\nAfter getting your token:")
    print("1. Copy the access token")
    print("2. Add it to your .env file as FACEBOOK_ACCESS_TOKEN")
    print("\nThe token will be in the URL after 'access_token=' or shown on the page")

if __name__ == "__main__":
    main()
