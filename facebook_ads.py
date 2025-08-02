#!/usr/bin/env python3
"""
Facebook Ads API integration for fetching marketing spend data
"""

import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class FacebookAdsClient:
    def __init__(self):
        """Initialize Facebook Ads client with credentials from environment"""
        self.access_token = os.getenv('FACEBOOK_ACCESS_TOKEN')
        self.ad_account_id = os.getenv('FACEBOOK_AD_ACCOUNT_ID')
        self.app_id = os.getenv('FACEBOOK_APP_ID')
        self.app_secret = os.getenv('FACEBOOK_APP_SECRET')
        
        # API version - use latest stable version
        self.api_version = 'v21.0'
        self.base_url = f'https://graph.facebook.com/{self.api_version}'
        
        # Validate required credentials
        if not all([self.access_token, self.ad_account_id]):
            print("Warning: Facebook Ads credentials not fully configured. Ad spend data will not be available.")
            self.is_configured = False
        else:
            self.is_configured = True
            # Ensure ad_account_id has correct format
            if not self.ad_account_id.startswith('act_'):
                self.ad_account_id = f'act_{self.ad_account_id}'
    
    def get_daily_spend(self, date_from: datetime, date_to: datetime) -> Dict[str, float]:
        """
        Fetch daily ad spend from Facebook Ads API
        
        Args:
            date_from: Start date
            date_to: End date
            
        Returns:
            Dictionary mapping date strings to spend amounts in EUR
        """
        if not self.is_configured:
            return {}
        
        try:
            # Format dates for Facebook API
            since = date_from.strftime('%Y-%m-%d')
            until = date_to.strftime('%Y-%m-%d')
            
            # Build the insights endpoint URL
            url = f'{self.base_url}/{self.ad_account_id}/insights'
            
            # Parameters for the API request
            params = {
                'access_token': self.access_token,
                'fields': 'spend,impressions,clicks,cpc,cpm,ctr',
                'time_range': f'{{"since":"{since}","until":"{until}"}}',
                'time_increment': 1,  # Daily breakdown
                'level': 'account',
                'limit': 500
            }
            
            # Make the API request
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            # Process the response
            daily_spend = {}
            
            if 'data' in data:
                for day_data in data['data']:
                    date_str = day_data.get('date_start', '')
                    spend = float(day_data.get('spend', 0))
                    
                    # Store additional metrics if needed
                    daily_metrics = {
                        'spend': spend,
                        'impressions': int(day_data.get('impressions', 0)),
                        'clicks': int(day_data.get('clicks', 0)),
                        'cpc': float(day_data.get('cpc', 0)),
                        'cpm': float(day_data.get('cpm', 0)),
                        'ctr': float(day_data.get('ctr', 0))
                    }
                    
                    # For now, just return spend amount
                    # You can modify this to return full metrics if needed
                    daily_spend[date_str] = spend
            
            return daily_spend
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching Facebook Ads data: {e}")
            return {}
        except Exception as e:
            print(f"Unexpected error processing Facebook Ads data: {e}")
            return {}
    
    def get_campaign_spend(self, date_from: datetime, date_to: datetime) -> List[Dict[str, Any]]:
        """
        Fetch campaign-level spend data
        
        Args:
            date_from: Start date
            date_to: End date
            
        Returns:
            List of campaign spend data
        """
        if not self.is_configured:
            return []
        
        try:
            since = date_from.strftime('%Y-%m-%d')
            until = date_to.strftime('%Y-%m-%d')
            
            # First get campaigns
            campaigns_url = f'{self.base_url}/{self.ad_account_id}/campaigns'
            campaigns_params = {
                'access_token': self.access_token,
                'fields': 'id,name,status',
                'limit': 500
            }
            
            campaigns_response = requests.get(campaigns_url, params=campaigns_params)
            campaigns_response.raise_for_status()
            campaigns_data = campaigns_response.json()
            
            campaign_spend = []
            
            if 'data' in campaigns_data:
                for campaign in campaigns_data['data']:
                    campaign_id = campaign['id']
                    campaign_name = campaign['name']
                    
                    # Get insights for each campaign
                    insights_url = f'{self.base_url}/{campaign_id}/insights'
                    insights_params = {
                        'access_token': self.access_token,
                        'fields': 'spend,impressions,clicks,reach',
                        'time_range': f'{{"since":"{since}","until":"{until}"}}',
                        'level': 'campaign'
                    }
                    
                    insights_response = requests.get(insights_url, params=insights_params)
                    
                    if insights_response.status_code == 200:
                        insights_data = insights_response.json()
                        
                        if 'data' in insights_data and insights_data['data']:
                            data = insights_data['data'][0]
                            campaign_spend.append({
                                'campaign_id': campaign_id,
                                'campaign_name': campaign_name,
                                'spend': float(data.get('spend', 0)),
                                'impressions': int(data.get('impressions', 0)),
                                'clicks': int(data.get('clicks', 0)),
                                'reach': int(data.get('reach', 0))
                            })
            
            return campaign_spend
            
        except Exception as e:
            print(f"Error fetching campaign data: {e}")
            return []
    
    def test_connection(self) -> bool:
        """
        Test if the Facebook Ads API connection is working
        
        Returns:
            True if connection successful, False otherwise
        """
        if not self.is_configured:
            print("Facebook Ads API not configured")
            return False
        
        try:
            # Try to fetch account information
            url = f'{self.base_url}/{self.ad_account_id}'
            params = {
                'access_token': self.access_token,
                'fields': 'name,currency,account_status'
            }
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            print(f"Successfully connected to Facebook Ads account: {data.get('name', 'Unknown')}")
            print(f"Account currency: {data.get('currency', 'Unknown')}")
            
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"Failed to connect to Facebook Ads API: {e}")
            return False


def main():
    """Test function to verify Facebook Ads integration"""
    client = FacebookAdsClient()
    
    if client.test_connection():
        # Test fetching last 7 days of data
        date_to = datetime.now()
        date_from = date_to - timedelta(days=7)
        
        print(f"\nFetching ad spend from {date_from.strftime('%Y-%m-%d')} to {date_to.strftime('%Y-%m-%d')}")
        
        daily_spend = client.get_daily_spend(date_from, date_to)
        
        if daily_spend:
            print("\nDaily Ad Spend:")
            total = 0
            for date, spend in sorted(daily_spend.items()):
                print(f"  {date}: €{spend:.2f}")
                total += spend
            print(f"  Total: €{total:.2f}")
        else:
            print("No spend data available")


if __name__ == "__main__":
    main()