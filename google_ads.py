#!/usr/bin/env python3
"""
Google Ads API integration for fetching marketing spend data
"""

import os
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv
from logger_config import get_logger

# Load environment variables
load_dotenv()

# Set up logging
logger = get_logger('google_ads')

class GoogleAdsClient:
    def __init__(self):
        """Initialize Google Ads client with credentials from environment"""
        self.developer_token = os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN')
        self.client_id = os.getenv('GOOGLE_ADS_CLIENT_ID')
        self.client_secret = os.getenv('GOOGLE_ADS_CLIENT_SECRET')
        self.refresh_token = os.getenv('GOOGLE_ADS_REFRESH_TOKEN')
        self.customer_id = os.getenv('GOOGLE_ADS_CUSTOMER_ID', '7592903323')
        self.login_customer_id = os.getenv('GOOGLE_ADS_LOGIN_CUSTOMER_ID')  # Optional, for MCC accounts
        
        # Cache configuration
        self.cache_dir = Path('data/cache/google_ads')
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_days_threshold = 3  # Days from today that should always be fetched fresh
        
        # Validate required credentials
        if not all([self.developer_token, self.client_id, self.client_secret, self.refresh_token]):
            logger.warning("Google Ads credentials not fully configured. Ad spend data will not be available.")
            self.is_configured = False
            self.client = None
        else:
            self.is_configured = True
            self._initialize_client()
    
    def _initialize_client(self):
        """Initialize the Google Ads API client"""
        try:
            from google.ads.googleads.client import GoogleAdsClient as GAClient
            from google.ads.googleads.errors import GoogleAdsException
            
            # Create configuration dictionary
            config = {
                "developer_token": self.developer_token,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self.refresh_token,
                "use_proto_plus": True
            }
            
            # Add login customer ID if provided (for MCC accounts)
            if self.login_customer_id:
                config["login_customer_id"] = self.login_customer_id.replace('-', '')
            
            # Initialize client
            self.client = GAClient.load_from_dict(config)
            self.GoogleAdsException = GoogleAdsException

        except ImportError:
            logger.warning("Google Ads library not installed. Please run: pip install google-ads")
            self.is_configured = False
            self.client = None
        except Exception as e:
            logger.error(f"Error initializing Google Ads client: {e}")
            self.is_configured = False
            self.client = None
    
    def get_cache_filename(self, date_from: datetime, date_to: datetime) -> Path:
        """Generate cache filename for a date range"""
        from_str = date_from.strftime('%Y%m%d')
        to_str = date_to.strftime('%Y%m%d')
        return self.cache_dir / f"google_ads_{from_str}_{to_str}.json"
    
    def should_use_cache(self, date: datetime) -> bool:
        """Determine if cache should be used for a given date"""
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        date_normalized = date.replace(hour=0, minute=0, second=0, microsecond=0)
        days_ago = (today - date_normalized).days
        
        # Always fetch fresh data for recent days
        return days_ago > self.cache_days_threshold
    
    def load_from_cache(self, date_from: datetime, date_to: datetime) -> Optional[Dict[str, float]]:
        """Load Google Ads data from cache"""
        cache_file = self.get_cache_filename(date_from, date_to)
        if not cache_file.exists():
            return None
        
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Check if cache is still valid
                cached_at = datetime.fromisoformat(data.get('cached_at', ''))
                if (datetime.now() - cached_at).days > 30:  # Expire cache after 30 days
                    return None
                logger.info(f"Loaded Google Ads data from cache ({len(data.get('daily_spend', {}))} days)")
                return data.get('daily_spend', {})
        except Exception as e:
            logger.error(f"Error loading Google Ads cache: {e}")
            return None
    
    def save_to_cache(self, date_from: datetime, date_to: datetime, daily_spend: Dict[str, float]):
        """Save Google Ads data to cache"""
        cache_file = self.get_cache_filename(date_from, date_to)
        
        try:
            cache_data = {
                'date_from': date_from.strftime('%Y-%m-%d'),
                'date_to': date_to.strftime('%Y-%m-%d'),
                'cached_at': datetime.now().isoformat(),
                'daily_spend': daily_spend
            }
            
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)

            if daily_spend:
                logger.info(f"Cached Google Ads data for {len(daily_spend)} days")
        except Exception as e:
            logger.error(f"Error saving Google Ads cache: {e}")
    
    def get_daily_spend(self, date_from: datetime, date_to: datetime) -> Dict[str, float]:
        """
        Fetch daily ad spend from Google Ads API with caching
        
        Args:
            date_from: Start date
            date_to: End date
            
        Returns:
            Dictionary mapping date strings to spend amounts in EUR
        """
        if not self.is_configured or not self.client:
            return {}
        
        # Check if we should use cache for the entire date range
        if self.should_use_cache(date_to):
            cached_data = self.load_from_cache(date_from, date_to)
            if cached_data is not None:
                return cached_data
        
        # Otherwise, fetch from API
        try:
            # Format dates for Google Ads API (YYYY-MM-DD format)
            since = date_from.strftime('%Y-%m-%d')
            until = date_to.strftime('%Y-%m-%d')

            logger.info(f"Fetching Google Ads data from API for {since} to {until}...")

            # Remove hyphens from customer ID
            customer_id = self.customer_id.replace('-', '')
            
            # Build the GAQL query for daily metrics
            query = f"""
                SELECT 
                    segments.date,
                    metrics.cost_micros,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.average_cpc,
                    metrics.average_cpm,
                    metrics.ctr
                FROM customer
                WHERE segments.date BETWEEN '{since}' AND '{until}'
                ORDER BY segments.date
            """
            
            # Get the GoogleAdsService
            ga_service = self.client.get_service("GoogleAdsService")
            
            # Execute the query
            response = ga_service.search_stream(
                customer_id=customer_id,
                query=query
            )
            
            # Process the response
            daily_spend = {}
            
            for batch in response:
                for row in batch.results:
                    date_str = row.segments.date
                    # Convert micros to actual currency (divide by 1,000,000)
                    spend = row.metrics.cost_micros / 1_000_000
                    
                    daily_spend[date_str] = spend
            
            # Cache the data if the entire range is cacheable
            if self.should_use_cache(date_to):
                self.save_to_cache(date_from, date_to, daily_spend)
            
            return daily_spend

        except self.GoogleAdsException as e:
            logger.error(f"Google Ads API error: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error processing Google Ads data: {e}")
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
        if not self.is_configured or not self.client:
            return []
        
        try:
            since = date_from.strftime('%Y-%m-%d')
            until = date_to.strftime('%Y-%m-%d')
            
            # Remove hyphens from customer ID
            customer_id = self.customer_id.replace('-', '')
            
            # Build the GAQL query for campaign metrics
            query = f"""
                SELECT 
                    campaign.id,
                    campaign.name,
                    campaign.status,
                    metrics.cost_micros,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.conversions
                FROM campaign
                WHERE segments.date BETWEEN '{since}' AND '{until}'
            """
            
            # Get the GoogleAdsService
            ga_service = self.client.get_service("GoogleAdsService")
            
            # Execute the query
            response = ga_service.search_stream(
                customer_id=customer_id,
                query=query
            )
            
            campaign_spend = []
            
            for batch in response:
                for row in batch.results:
                    campaign_spend.append({
                        'campaign_id': row.campaign.id,
                        'campaign_name': row.campaign.name,
                        'status': row.campaign.status.name,
                        'spend': row.metrics.cost_micros / 1_000_000,
                        'impressions': row.metrics.impressions,
                        'clicks': row.metrics.clicks,
                        'conversions': row.metrics.conversions
                    })
            
            return campaign_spend

        except Exception as e:
            logger.error(f"Error fetching campaign data: {e}")
            return []
    
    def test_connection(self) -> bool:
        """
        Test if the Google Ads API connection is working
        
        Returns:
            True if connection successful, False otherwise
        """
        if not self.is_configured or not self.client:
            logger.warning("Google Ads API not configured or client not initialized")
            return False
        
        try:
            # Remove hyphens from customer ID
            customer_id = self.customer_id.replace('-', '')
            
            # Simple query to test connection
            query = """
                SELECT 
                    customer.id,
                    customer.descriptive_name,
                    customer.currency_code
                FROM customer
                LIMIT 1
            """
            
            # Get the GoogleAdsService
            ga_service = self.client.get_service("GoogleAdsService")
            
            # Execute the query
            response = ga_service.search(
                customer_id=customer_id,
                query=query
            )
            
            # Process the response
            for row in response:
                logger.info(f"Successfully connected to Google Ads account: {row.customer.descriptive_name}")
                logger.info(f"Customer ID: {row.customer.id}")
                logger.info(f"Currency: {row.customer.currency_code}")
                return True

            return True

        except Exception as e:
            logger.error(f"Failed to connect to Google Ads API: {e}")
            return False
    
    def generate_refresh_token(self):
        """
        Helper method to generate a refresh token for first-time setup
        This should be run once to get the refresh token
        """
        try:
            from google_auth_oauthlib.flow import Flow

            logger.info("\n=== Google Ads OAuth2 Setup ===")
            logger.info("1. First, create OAuth2 credentials in Google Cloud Console")
            logger.info("2. Download the credentials JSON file")
            logger.info("3. Run this method with the path to your credentials file")

            credentials_path = input("Enter path to your OAuth2 credentials JSON file: ").strip()

            if not os.path.exists(credentials_path):
                logger.error(f"File not found: {credentials_path}")
                return
            
            # OAuth2 scope for Google Ads
            SCOPES = ['https://www.googleapis.com/auth/adwords']
            
            # Create the flow
            flow = Flow.from_client_secrets_file(
                credentials_path,
                scopes=SCOPES,
                redirect_uri='http://localhost:8080'
            )
            
            # Get the authorization URL
            auth_url, _ = flow.authorization_url(prompt='consent')

            logger.info(f"\nOpen this URL in your browser:\n{auth_url}")
            logger.info("\nAfter authorization, you'll be redirected to localhost:8080")
            logger.info("Copy the full URL from your browser and paste it here:")

            redirect_response = input("Paste the full redirect URL: ").strip()

            # Get the token
            flow.fetch_token(authorization_response=redirect_response)

            # Get credentials
            credentials = flow.credentials

            logger.info("\n=== Your Google Ads Credentials ===")
            logger.info(f"GOOGLE_ADS_CLIENT_ID={flow.client_config['client_id']}")
            logger.info(f"GOOGLE_ADS_CLIENT_SECRET={flow.client_config['client_secret']}")
            logger.info(f"GOOGLE_ADS_REFRESH_TOKEN={credentials.refresh_token}")
            logger.info("\nAdd these to your .env file along with:")
            logger.info("GOOGLE_ADS_DEVELOPER_TOKEN=<your_developer_token>")
            logger.info("GOOGLE_ADS_CUSTOMER_ID=7592903323")

        except ImportError:
            logger.error("Please install required packages:")
            logger.error("pip install google-auth-oauthlib google-auth-httplib2")
        except Exception as e:
            logger.error(f"Error during OAuth2 setup: {e}")


def main():
    """Test function to verify Google Ads integration"""
    client = GoogleAdsClient()

    if not client.is_configured:
        logger.info("\nGoogle Ads not configured. To set up:")
        logger.info("1. Get a developer token from https://ads.google.com/aw/apicenter")
        logger.info("2. Create OAuth2 credentials in Google Cloud Console")
        logger.info("3. Run: python google_ads.py --setup")
        return
    
    if client.test_connection():
        # Test fetching last 7 days of data
        date_to = datetime.now()
        date_from = date_to - timedelta(days=7)

        logger.info(f"\nFetching ad spend from {date_from.strftime('%Y-%m-%d')} to {date_to.strftime('%Y-%m-%d')}")

        daily_spend = client.get_daily_spend(date_from, date_to)

        if daily_spend:
            logger.info("\nDaily Ad Spend:")
            total = 0
            for date, spend in sorted(daily_spend.items()):
                logger.info(f"  {date}: €{spend:.2f}")
                total += spend
            logger.info(f"  Total: €{total:.2f}")
        else:
            logger.info("No spend data available")

        # Test campaign data
        logger.info("\n" + "="*50)
        campaign_data = client.get_campaign_spend(date_from, date_to)
        if campaign_data:
            logger.info("\nCampaign Performance:")
            for campaign in campaign_data:
                logger.info(f"\n  {campaign['campaign_name']} ({campaign['status']})")
                logger.info(f"    Spend: €{campaign['spend']:.2f}")
                logger.info(f"    Clicks: {campaign['clicks']}")
                logger.info(f"    Impressions: {campaign['impressions']}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--setup":
        client = GoogleAdsClient()
        client.generate_refresh_token()
    else:
        main()