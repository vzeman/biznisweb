#!/usr/bin/env python3
"""
Facebook Ads API integration for fetching marketing spend data
"""

import os
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
from urllib.parse import urlparse, urlunparse
import requests
from dotenv import load_dotenv
from http_client import build_retry_session, resolve_timeout
from logger_config import get_logger

# Load environment variables
load_dotenv()

# Set up logging
logger = get_logger('facebook_ads')

class FacebookTokenError(RuntimeError):
    """Raised when Facebook OAuth token is invalid or expired."""

class FacebookAdsClient:
    def __init__(self):
        """Initialize Facebook Ads client with credentials from environment"""
        self.access_token = os.getenv('FACEBOOK_ACCESS_TOKEN')
        self.ad_account_id = os.getenv('FACEBOOK_AD_ACCOUNT_ID')
        self.app_id = os.getenv('FACEBOOK_APP_ID')
        self.app_secret = os.getenv('FACEBOOK_APP_SECRET')
        self.request_timeout = resolve_timeout(os.getenv('FACEBOOK_API_TIMEOUT_SEC'))
        
        # API version - use latest stable version
        self.api_version = 'v21.0'
        self.base_url = f'https://graph.facebook.com/{self.api_version}'
        
        # Cache configuration (project-aware if REPORT_DATA_DIR is provided)
        base_data_dir = Path(os.getenv('REPORT_DATA_DIR', 'data'))
        self.cache_dir = base_data_dir / 'cache' / 'facebook_ads'
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_days_threshold = 3  # Days from today that should always be fetched fresh
        
        # Validate required credentials
        if not all([self.access_token, self.ad_account_id]):
            logger.warning("Facebook Ads credentials not fully configured. Ad spend data will not be available.")
            self.is_configured = False
            self.session = build_retry_session(timeout=self.request_timeout)
        else:
            self.is_configured = True
            self.session = build_retry_session(
                headers={'Authorization': f'Bearer {self.access_token}'},
                timeout=self.request_timeout,
            )
            # Ensure ad_account_id has correct format
            if not self.ad_account_id.startswith('act_'):
                self.ad_account_id = f'act_{self.ad_account_id}'

    @staticmethod
    def _sanitize_url(url: str) -> str:
        """Remove query string (incl. access_token) from URLs before logging."""
        if not url:
            return ""
        try:
            parsed = urlparse(url)
            return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))
        except Exception:
            # Fallback redaction for raw strings.
            return re.sub(r"access_token=[^&\\s]+", "access_token=***", url)

    def _log_request_exception(self, context: str, exc: requests.exceptions.RequestException) -> Dict[str, Any]:
        """
        Log structured Facebook API error details without leaking access token.
        """
        response = getattr(exc, "response", None)
        details: Dict[str, Any] = {
            "status_code": None,
            "code": None,
            "subcode": None,
            "type": None,
            "message": str(exc),
            "endpoint": None,
        }
        if response is None:
            logger.error(f"{context}: {exc.__class__.__name__}: {exc}")
            return details

        safe_url = self._sanitize_url(getattr(response, "url", ""))
        status_code = response.status_code
        error_code = None
        error_subcode = None
        error_type = None
        error_message = ""

        try:
            payload = response.json()
            error_obj = payload.get("error", {}) if isinstance(payload, dict) else {}
            error_code = error_obj.get("code")
            error_subcode = error_obj.get("error_subcode")
            error_type = error_obj.get("type")
            error_message = error_obj.get("message", "")
        except Exception:
            error_message = response.text[:500] if response.text else str(exc)

        logger.error(
            f"{context}: status={status_code} code={error_code} subcode={error_subcode} "
            f"type={error_type} message={error_message} endpoint={safe_url}"
        )

        if error_code == 190:
            logger.error(
                "Facebook access token is invalid/expired (OAuth code 190). "
                "Please generate a new token and update FACEBOOK_ACCESS_TOKEN."
            )

        details.update({
            "status_code": status_code,
            "code": error_code,
            "subcode": error_subcode,
            "type": error_type,
            "message": error_message,
            "endpoint": safe_url,
        })
        return details

    def _get_json(self, url: str, params: Optional[Dict[str, Any]], context: str) -> Dict[str, Any]:
        response = self.session.get(url, params=params)
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else {}
    
    def get_cache_filename(self, date_from: datetime, date_to: datetime) -> Path:
        """Generate cache filename for a date range"""
        from_str = date_from.strftime('%Y%m%d')
        to_str = date_to.strftime('%Y%m%d')
        return self.cache_dir / f"fb_ads_{from_str}_{to_str}.json"
    
    def should_use_cache(self, date: datetime) -> bool:
        """Determine if cache should be used for a given date"""
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        date_normalized = date.replace(hour=0, minute=0, second=0, microsecond=0)
        days_ago = (today - date_normalized).days
        
        # Always fetch fresh data for recent days
        return days_ago > self.cache_days_threshold
    
    def load_from_cache(self, date_from: datetime, date_to: datetime) -> Optional[Dict[str, float]]:
        """Load Facebook Ads data from cache"""
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
                logger.info(f"Loaded Facebook Ads data from cache ({len(data.get('daily_spend', {}))} days)")
                return data.get('daily_spend', {})
        except Exception as e:
            logger.error(f"Error loading Facebook Ads cache: {e}")
            return None
    
    def save_to_cache(self, date_from: datetime, date_to: datetime, daily_spend: Dict[str, float]):
        """Save Facebook Ads data to cache"""
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
                logger.info(f"Cached Facebook Ads data for {len(daily_spend)} days")
        except Exception as e:
            logger.error(f"Error saving Facebook Ads cache: {e}")
    
    def get_daily_spend(self, date_from: datetime, date_to: datetime) -> Dict[str, float]:
        """
        Fetch daily ad spend from Facebook Ads API with caching

        Args:
            date_from: Start date
            date_to: End date

        Returns:
            Dictionary mapping date strings to spend amounts in EUR
        """
        use_cache = self.should_use_cache(date_to)
        if use_cache:
            cached_data = self.load_from_cache(date_from, date_to)
            if cached_data is not None:
                return cached_data

        # If not configured, return empty after checking cache
        if not self.is_configured:
            return {}

        # Otherwise, fetch from API
        try:
            # Format dates for Facebook API
            since = date_from.strftime('%Y-%m-%d')
            until = date_to.strftime('%Y-%m-%d')

            logger.info(f"Fetching Facebook Ads data from API for {since} to {until}...")

            # Build the insights endpoint URL
            url = f'{self.base_url}/{self.ad_account_id}/insights'

            # Parameters for the API request
            params = {
                'fields': 'spend,impressions,clicks,cpc,cpm,ctr',
                'time_range': f'{{"since":"{since}","until":"{until}"}}',
                'time_increment': 1,  # Daily breakdown
                'level': 'account',
                'limit': 500
            }

            # Make the API request
            data = self._get_json(url, params, "Error fetching Facebook Ads data")

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

            # Cache the data if the entire range is cacheable
            if use_cache:
                self.save_to_cache(date_from, date_to, daily_spend)

            return daily_spend

        except requests.exceptions.RequestException as e:
            details = self._log_request_exception("Error fetching Facebook Ads data", e)
            if details.get("code") == 190:
                raise FacebookTokenError(
                    f"Facebook token invalid/expired: {details.get('message', 'OAuth error')}"
                ) from e
            return {}
        except Exception as e:
            logger.error(f"Unexpected error processing Facebook Ads data: {e}")
            return {}

    def get_daily_metrics(self, date_from: datetime, date_to: datetime) -> Dict[str, Dict[str, Any]]:
        """
        Fetch detailed daily metrics from Facebook Ads API

        Args:
            date_from: Start date
            date_to: End date

        Returns:
            Dictionary mapping date strings to full metrics dict
        """
        if not self.is_configured:
            return {}

        try:
            since = date_from.strftime('%Y-%m-%d')
            until = date_to.strftime('%Y-%m-%d')

            logger.info(f"Fetching detailed Facebook Ads metrics from API for {since} to {until}...")

            url = f'{self.base_url}/{self.ad_account_id}/insights'

            params = {
                'fields': 'spend,impressions,clicks,cpc,cpm,ctr,reach,frequency,unique_clicks,cost_per_unique_click',
                'time_range': f'{{"since":"{since}","until":"{until}"}}',
                'time_increment': 1,
                'level': 'account',
                'limit': 500
            }

            data = self._get_json(url, params, "Error fetching detailed Facebook Ads metrics")
            daily_metrics = {}

            if 'data' in data:
                for day_data in data['data']:
                    date_str = day_data.get('date_start', '')
                    daily_metrics[date_str] = {
                        'spend': float(day_data.get('spend', 0)),
                        'impressions': int(day_data.get('impressions', 0)),
                        'clicks': int(day_data.get('clicks', 0)),
                        'cpc': float(day_data.get('cpc', 0)),
                        'cpm': float(day_data.get('cpm', 0)),
                        'ctr': float(day_data.get('ctr', 0)),
                        'reach': int(day_data.get('reach', 0)),
                        'frequency': float(day_data.get('frequency', 0)),
                        'unique_clicks': int(day_data.get('unique_clicks', 0)),
                        'cost_per_unique_click': float(day_data.get('cost_per_unique_click', 0))
                    }

            logger.info(f"Retrieved detailed metrics for {len(daily_metrics)} days")
            return daily_metrics

        except requests.exceptions.RequestException as e:
            self._log_request_exception("Error fetching detailed Facebook Ads metrics", e)
            return {}
        except Exception as e:
            logger.error(f"Unexpected error processing detailed Facebook Ads metrics: {e}")
            return {}
    
    def get_campaign_spend(self, date_from: datetime, date_to: datetime) -> List[Dict[str, Any]]:
        """
        Fetch campaign-level spend data with full metrics

        Args:
            date_from: Start date
            date_to: End date

        Returns:
            List of campaign spend data with metrics
        """
        if not self.is_configured:
            return []

        try:
            since = date_from.strftime('%Y-%m-%d')
            until = date_to.strftime('%Y-%m-%d')

            logger.info(f"Fetching campaign-level data for {since} to {until}...")

            # First get campaigns
            campaigns_url = f'{self.base_url}/{self.ad_account_id}/campaigns'
            campaigns_params = {
                'fields': 'id,name,status,objective',
                'limit': 500
            }

            campaigns_data = self._get_json(campaigns_url, campaigns_params, "Error fetching campaign list")

            campaign_spend = []

            if 'data' in campaigns_data:
                for campaign in campaigns_data['data']:
                    campaign_id = campaign['id']
                    campaign_name = campaign['name']
                    campaign_status = campaign.get('status', 'UNKNOWN')
                    campaign_objective = campaign.get('objective', 'UNKNOWN')

                    # Get insights for each campaign
                    insights_url = f'{self.base_url}/{campaign_id}/insights'
                    insights_params = {
                        'fields': 'spend,impressions,clicks,reach,cpc,cpm,ctr,frequency,unique_clicks,cost_per_unique_click,actions,conversions,cost_per_action_type,conversion_values',
                        'time_range': f'{{"since":"{since}","until":"{until}"}}',
                        'level': 'campaign'
                    }

                    insights_data = self._get_json(insights_url, insights_params, "Error fetching campaign insights")

                    if 'data' in insights_data and insights_data['data']:
                        data = insights_data['data'][0]
                        spend = float(data.get('spend', 0))
                        impressions = int(data.get('impressions', 0))
                        clicks = int(data.get('clicks', 0))
                        reach = int(data.get('reach', 0))

                        # Extract conversion data
                        actions = data.get('actions', [])
                        conversions_count = 0
                        purchases_count = 0
                        add_to_cart_count = 0

                        for action in actions:
                            action_type = action.get('action_type', '')
                            value = int(action.get('value', 0))

                            if 'purchase' in action_type or 'conversion' in action_type:
                                conversions_count += value
                            if action_type == 'offsite_conversion.fb_pixel_purchase':
                                purchases_count = value
                            if action_type == 'offsite_conversion.fb_pixel_add_to_cart':
                                add_to_cart_count = value

                        # Extract cost per action
                        cost_per_action_types = data.get('cost_per_action_type', [])
                        cost_per_conversion = 0
                        cost_per_purchase = 0

                        for cpa in cost_per_action_types:
                            action_type = cpa.get('action_type', '')
                            value = float(cpa.get('value', 0))

                            if action_type == 'offsite_conversion.fb_pixel_purchase':
                                cost_per_purchase = value
                            elif 'purchase' in action_type or 'conversion' in action_type:
                                cost_per_conversion = value if cost_per_conversion == 0 else cost_per_conversion

                        # Calculate conversion rate
                        conversion_rate = (conversions_count / clicks * 100) if clicks > 0 else 0
                        purchase_rate = (purchases_count / clicks * 100) if clicks > 0 else 0

                        campaign_spend.append({
                            'campaign_id': campaign_id,
                            'campaign_name': campaign_name,
                            'status': campaign_status,
                            'objective': campaign_objective,
                            'spend': spend,
                            'impressions': impressions,
                            'clicks': clicks,
                            'reach': reach,
                            'cpc': float(data.get('cpc', 0)),
                            'cpm': float(data.get('cpm', 0)),
                            'ctr': float(data.get('ctr', 0)),
                            'frequency': float(data.get('frequency', 0)),
                            'unique_clicks': int(data.get('unique_clicks', 0)),
                            'cost_per_unique_click': float(data.get('cost_per_unique_click', 0)),
                            'conversions': conversions_count,
                            'purchases': purchases_count,
                            'add_to_cart': add_to_cart_count,
                            'conversion_rate': conversion_rate,
                            'purchase_rate': purchase_rate,
                            'cost_per_conversion': cost_per_conversion,
                            'cost_per_purchase': cost_per_purchase
                        })

            # Sort by spend descending
            campaign_spend.sort(key=lambda x: x['spend'], reverse=True)
            logger.info(f"Retrieved data for {len(campaign_spend)} campaigns")
            return campaign_spend

        except requests.exceptions.RequestException as e:
            self._log_request_exception("Error fetching campaign data", e)
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching campaign data: {e}")
            return []

    def get_adset_performance(self, date_from: datetime, date_to: datetime) -> List[Dict[str, Any]]:
        """
        Fetch ad set level performance data

        Args:
            date_from: Start date
            date_to: End date

        Returns:
            List of ad set performance data
        """
        if not self.is_configured:
            return []

        try:
            since = date_from.strftime('%Y-%m-%d')
            until = date_to.strftime('%Y-%m-%d')

            logger.info(f"Fetching ad set performance data for {since} to {until}...")

            # Get ad sets with insights
            url = f'{self.base_url}/{self.ad_account_id}/adsets'
            params = {
                'fields': 'id,name,status,campaign_id,targeting',
                'limit': 500
            }

            adsets_data = self._get_json(url, params, "Error fetching ad set list")

            adset_performance = []

            if 'data' in adsets_data:
                for adset in adsets_data['data']:
                    adset_id = adset['id']

                    # Get insights for each ad set
                    insights_url = f'{self.base_url}/{adset_id}/insights'
                    insights_params = {
                        'fields': 'spend,impressions,clicks,reach,cpc,cpm,ctr,frequency',
                        'time_range': f'{{"since":"{since}","until":"{until}"}}',
                        'level': 'adset'
                    }

                    insights_data = self._get_json(insights_url, insights_params, "Error fetching ad set insights")

                    if 'data' in insights_data and insights_data['data']:
                        data = insights_data['data'][0]
                        spend = float(data.get('spend', 0))

                        if spend > 0:  # Only include ad sets with spend
                            adset_performance.append({
                                'adset_id': adset_id,
                                'adset_name': adset['name'],
                                'status': adset.get('status', 'UNKNOWN'),
                                'campaign_id': adset.get('campaign_id', ''),
                                'spend': spend,
                                'impressions': int(data.get('impressions', 0)),
                                'clicks': int(data.get('clicks', 0)),
                                'reach': int(data.get('reach', 0)),
                                'cpc': float(data.get('cpc', 0)),
                                'cpm': float(data.get('cpm', 0)),
                                'ctr': float(data.get('ctr', 0)),
                                'frequency': float(data.get('frequency', 0))
                            })

            # Sort by spend descending
            adset_performance.sort(key=lambda x: x['spend'], reverse=True)
            logger.info(f"Retrieved data for {len(adset_performance)} ad sets")
            return adset_performance

        except requests.exceptions.RequestException as e:
            self._log_request_exception("Error fetching ad set data", e)
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching ad set data: {e}")
            return []

    def get_ads_performance(self, date_from: datetime, date_to: datetime, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Fetch individual ad level performance data

        Args:
            date_from: Start date
            date_to: End date
            limit: Maximum number of ads to return

        Returns:
            List of ad performance data
        """
        if not self.is_configured:
            return []

        try:
            since = date_from.strftime('%Y-%m-%d')
            until = date_to.strftime('%Y-%m-%d')

            logger.info(f"Fetching individual ad performance data for {since} to {until}...")

            # Get ads with insights directly using the insights endpoint
            url = f'{self.base_url}/{self.ad_account_id}/insights'
            params = {
                'fields': 'ad_id,ad_name,spend,impressions,clicks,reach,cpc,cpm,ctr,frequency',
                'time_range': f'{{"since":"{since}","until":"{until}"}}',
                'level': 'ad',
                'limit': limit,
                'sort': 'spend_descending'
            }

            data = self._get_json(url, params, "Error fetching individual ad data")

            ads_performance = []

            if 'data' in data:
                for ad_data in data['data']:
                    spend = float(ad_data.get('spend', 0))
                    if spend > 0:
                        ads_performance.append({
                            'ad_id': ad_data.get('ad_id', ''),
                            'ad_name': ad_data.get('ad_name', 'Unknown'),
                            'spend': spend,
                            'impressions': int(ad_data.get('impressions', 0)),
                            'clicks': int(ad_data.get('clicks', 0)),
                            'reach': int(ad_data.get('reach', 0)),
                            'cpc': float(ad_data.get('cpc', 0)),
                            'cpm': float(ad_data.get('cpm', 0)),
                            'ctr': float(ad_data.get('ctr', 0)),
                            'frequency': float(ad_data.get('frequency', 0))
                        })

            logger.info(f"Retrieved data for {len(ads_performance)} individual ads")
            return ads_performance

        except requests.exceptions.RequestException as e:
            self._log_request_exception("Error fetching individual ad data", e)
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching individual ad data: {e}")
            return []

    def get_hourly_stats(self, date_from: datetime, date_to: datetime) -> List[Dict[str, Any]]:
        """
        Fetch hourly aggregated stats from Facebook Ads API

        Args:
            date_from: Start date
            date_to: End date

        Returns:
            List of hourly stats with metrics aggregated by hour of day
        """
        if not self.is_configured:
            return []

        try:
            since = date_from.strftime('%Y-%m-%d')
            until = date_to.strftime('%Y-%m-%d')

            logger.info(f"Fetching hourly stats for {since} to {until}...")

            url = f'{self.base_url}/{self.ad_account_id}/insights'
            params = {
                'fields': 'spend,impressions,clicks,cpc,cpm,ctr,reach',
                'time_range': f'{{"since":"{since}","until":"{until}"}}',
                'breakdowns': 'hourly_stats_aggregated_by_advertiser_time_zone',
                'level': 'account',
                'limit': 500
            }

            data = self._get_json(url, params, "Error fetching hourly stats")
            hourly_stats = []

            if 'data' in data:
                for hour_data in data['data']:
                    hour_range = hour_data.get('hourly_stats_aggregated_by_advertiser_time_zone', '')
                    # Parse hour from format "00:00:00 - 00:59:59"
                    hour = int(hour_range.split(':')[0]) if hour_range else 0

                    hourly_stats.append({
                        'hour': hour,
                        'hour_range': hour_range,
                        'spend': float(hour_data.get('spend', 0)),
                        'impressions': int(hour_data.get('impressions', 0)),
                        'clicks': int(hour_data.get('clicks', 0)),
                        'cpc': float(hour_data.get('cpc', 0)),
                        'cpm': float(hour_data.get('cpm', 0)),
                        'ctr': float(hour_data.get('ctr', 0)),
                        'reach': int(hour_data.get('reach', 0))
                    })

            # Sort by hour
            hourly_stats.sort(key=lambda x: x['hour'])
            logger.info(f"Retrieved hourly stats for {len(hourly_stats)} hours")
            return hourly_stats

        except requests.exceptions.RequestException as e:
            self._log_request_exception("Error fetching hourly stats", e)
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching hourly stats: {e}")
            return []

    def get_day_of_week_stats(self, date_from: datetime, date_to: datetime) -> List[Dict[str, Any]]:
        """
        Calculate stats aggregated by day of week

        Args:
            date_from: Start date
            date_to: End date

        Returns:
            List of day-of-week stats
        """
        if not self.is_configured:
            return []

        try:
            # Get daily metrics first
            daily_metrics = self.get_daily_metrics(date_from, date_to)

            if not daily_metrics:
                return []

            # Aggregate by day of week
            dow_stats = {i: {'spend': 0, 'impressions': 0, 'clicks': 0, 'days': 0} for i in range(7)}
            dow_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

            for date_str, metrics in daily_metrics.items():
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                dow = date_obj.weekday()  # 0 = Monday
                dow_stats[dow]['spend'] += metrics.get('spend', 0)
                dow_stats[dow]['impressions'] += metrics.get('impressions', 0)
                dow_stats[dow]['clicks'] += metrics.get('clicks', 0)
                dow_stats[dow]['days'] += 1

            result = []
            for dow, stats in dow_stats.items():
                if stats['days'] > 0:
                    avg_spend = stats['spend'] / stats['days']
                    avg_impressions = stats['impressions'] / stats['days']
                    avg_clicks = stats['clicks'] / stats['days']
                    ctr = (stats['clicks'] / stats['impressions'] * 100) if stats['impressions'] > 0 else 0
                    cpc = stats['spend'] / stats['clicks'] if stats['clicks'] > 0 else 0
                    cpm = (stats['spend'] / stats['impressions'] * 1000) if stats['impressions'] > 0 else 0

                    result.append({
                        'day_of_week': dow_names[dow],
                        'day_num': dow,
                        'total_spend': stats['spend'],
                        'total_impressions': stats['impressions'],
                        'total_clicks': stats['clicks'],
                        'avg_spend': avg_spend,
                        'avg_impressions': avg_impressions,
                        'avg_clicks': avg_clicks,
                        'ctr': ctr,
                        'cpc': cpc,
                        'cpm': cpm,
                        'days_count': stats['days']
                    })

            logger.info(f"Calculated day-of-week stats for {len(result)} days")
            return result

        except Exception as e:
            logger.error(f"Error calculating day-of-week stats: {e}")
            return []

    def test_connection(self) -> bool:
        """
        Test if the Facebook Ads API connection is working
        
        Returns:
            True if connection successful, False otherwise
        """
        if not self.is_configured:
            logger.warning("Facebook Ads API not configured")
            return False
        
        try:
            # Try to fetch account information
            url = f'{self.base_url}/{self.ad_account_id}'
            params = {
                'fields': 'name,currency,account_status'
            }
            
            data = self._get_json(url, params, "Failed to connect to Facebook Ads API")
            logger.info(f"Successfully connected to Facebook Ads account: {data.get('name', 'Unknown')}")
            logger.info(f"Account currency: {data.get('currency', 'Unknown')}")

            return True

        except requests.exceptions.RequestException as e:
            self._log_request_exception("Failed to connect to Facebook Ads API", e)
            return False


def main():
    """Test function to verify Facebook Ads integration"""
    client = FacebookAdsClient()
    
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


if __name__ == "__main__":
    main()
