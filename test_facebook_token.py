#!/usr/bin/env python3
"""
Quick script to test your Facebook Access Token
"""

import requests
import sys

def test_token(access_token, ad_account_id=None):
    """Test if the Facebook access token is valid"""
    
    # Test 1: Check token validity
    print("Testing token validity...")
    url = f"https://graph.facebook.com/v21.0/me"
    params = {"access_token": access_token}
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        print(f"✅ Token is valid! User ID: {data.get('id')}, Name: {data.get('name', 'N/A')}")
    else:
        print(f"❌ Token validation failed: {response.json()}")
        return False
    
    # Test 2: Check token permissions
    print("\nChecking token permissions...")
    url = f"https://graph.facebook.com/v21.0/me/permissions"
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        permissions = response.json().get('data', [])
        print("Token permissions:")
        for perm in permissions:
            status = "✅" if perm.get('status') == 'granted' else "❌"
            print(f"  {status} {perm.get('permission')}")
        
        # Check if ads_read is granted
        has_ads_read = any(p.get('permission') == 'ads_read' and p.get('status') == 'granted' 
                          for p in permissions)
        if not has_ads_read:
            print("\n⚠️  Warning: 'ads_read' permission not granted. You need this to fetch ad data.")
    
    # Test 3: Check ad account access (if account ID provided)
    if ad_account_id:
        print(f"\nTesting access to ad account {ad_account_id}...")
        
        # Ensure account ID has correct format
        if not ad_account_id.startswith('act_'):
            ad_account_id = f'act_{ad_account_id}'
        
        url = f"https://graph.facebook.com/v21.0/{ad_account_id}"
        params = {
            "access_token": access_token,
            "fields": "name,currency,account_status"
        }
        
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Ad account accessible!")
            print(f"   Account Name: {data.get('name')}")
            print(f"   Currency: {data.get('currency')}")
            print(f"   Status: {data.get('account_status')}")
            
            # Try to fetch some recent spend data
            print("\nFetching last 7 days spend...")
            url = f"https://graph.facebook.com/v21.0/{ad_account_id}/insights"
            params = {
                "access_token": access_token,
                "date_preset": "last_7d",
                "fields": "spend,impressions,clicks"
            }
            
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json().get('data', [])
                if data:
                    stats = data[0]
                    print(f"   Last 7 days spend: ${stats.get('spend', '0')}")
                    print(f"   Impressions: {stats.get('impressions', '0')}")
                    print(f"   Clicks: {stats.get('clicks', '0')}")
                else:
                    print("   No spend data in last 7 days")
        else:
            print(f"❌ Cannot access ad account: {response.json()}")
            print("\nMake sure:")
            print("1. The ad account ID is correct")
            print("2. Your user/app has access to this ad account")
            print("3. The token has 'ads_read' permission")
    
    return True

if __name__ == "__main__":
    print("Facebook Access Token Tester")
    print("="*50)
    
    access_token = input("Enter your Facebook Access Token: ").strip()
    
    if not access_token:
        print("❌ No token provided")
        sys.exit(1)
    
    ad_account = input("Enter your Ad Account ID (optional, press Enter to skip): ").strip()
    
    test_token(access_token, ad_account if ad_account else None)