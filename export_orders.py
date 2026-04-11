#!/usr/bin/env python3
"""
Export orders from BizniWeb GraphQL API to CSV
"""

import os
import csv
import argparse
import time
import json
import copy
import traceback
import hashlib
import base64
import re
import unicodedata
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import calendar
import numpy as np
from logger_config import get_logger
from weather_client import WeatherClient
from reporting_core import (
    BASE_DEFAULT_PROJECT,
    apply_project_runtime,
    build_cfo_kpi_payload,
    load_project_env,
    load_project_runtime,
    load_project_settings,
    resolve_biznisweb_api_url,
    resolve_reporting_defaults,
    sanitize_output_tag,
)

try:
    from dotenv import load_dotenv
except ImportError:
    print("âťŚ Missing package: python-dotenv")
    print("Please run: pip install python-dotenv")
    exit(1)

try:
    from gql import gql, Client
    from gql.transport.requests import RequestsHTTPTransport
except ImportError:
    print("âťŚ Missing package: gql")
    print("Please run: pip install 'gql[all]>=3.5.0'")
    exit(1)

try:
    import pandas as pd
except ImportError:
    print("âťŚ Missing package: pandas")
    print("Please run: pip install pandas>=2.0.0")
    exit(1)

# Optional packages - don't fail if missing
try:
    from facebook_ads import FacebookAdsClient
except ImportError:
    print("âš ď¸Ź  Facebook Ads integration not available (missing facebook-business package)")
    class FacebookAdsClient:
        def __init__(self):
            self.is_configured = False
        def get_daily_spend(self, *args, **kwargs):
            return {}

try:
    from google_ads import GoogleAdsClient
except ImportError:
    print("âš ď¸Ź  Google Ads integration not available (missing google-ads package)")
    print("   To enable, run: pip install google-ads google-auth-oauthlib google-auth-httplib2")
    class GoogleAdsClient:
        def __init__(self):
            self.is_configured = False
        def get_daily_spend(self, *args, **kwargs):
            return {}

from html_report_generator import generate_html_report, generate_email_strategy_report

# Load base environment variables from repo root .env (if present).
# Accept UTF-8 BOM so local PowerShell rewrites do not break the first key.
load_dotenv(encoding="utf-8-sig")

# Set up logging
logger = get_logger('export_orders')

# Prevent UnicodeEncodeError on Windows consoles with legacy codepages.
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(errors="replace")
        sys.stderr.reconfigure(errors="replace")
    except Exception:
        pass

DEFAULT_PROJECT = BASE_DEFAULT_PROJECT
GRAPHQL_TIMEOUT_SEC = int(
    os.getenv("BIZNISWEB_API_TIMEOUT_SEC", os.getenv("REPORT_HTTP_READ_TIMEOUT_SEC", "30"))
)

# Fixed costs
PACKAGING_COST_PER_ORDER = 0.3  # EUR per order
SHIPPING_SUBSIDY_PER_ORDER = 0.2  # legacy alias; use SHIPPING_NET_PER_ORDER semantics below
SHIPPING_NET_PER_ORDER = SHIPPING_SUBSIDY_PER_ORDER  # positive = business cost, negative = shipping profit
FIXED_MONTHLY_COST = 0  # EUR per month (Marek, Uctovnictvo)
ZERO_MARGIN_BRANDS: List[str] = []  # Optional list of brands that should always run at 0 product margin
ZERO_COST_BRANDS: List[str] = []  # Optional list of brands that should always run at 0 product cost
ZERO_COST_LABEL_PATTERNS: List[str] = []  # Optional label patterns forced to 0 product cost
MARGIN_15_BRANDS: List[str] = []  # Optional brands forced to 15% product margin
MARGIN_15_LABEL_PATTERNS: List[str] = []  # Optional label patterns forced to 15% product margin
EXCLUDE_ZERO_PRICE_LABEL_PATTERNS: List[str] = []  # Optional label patterns excluded only when line price is 0
MANUAL_FB_ADS_TOTAL: Optional[float] = None  # Optional fixed total FB spend for selected report range
MANUAL_GOOGLE_ADS_TOTAL: Optional[float] = None  # Optional fixed total Google spend for selected report range
PREFER_MANUAL_ADS_TOTALS = False
WEATHER_SETTINGS: Dict[str, Any] = {
    "enabled": False,
    "timezone": "Europe/Bratislava",
    "locations": []
}
ENABLE_EMAIL_STRATEGY_REPORT = False

# Currency conversion rates to EUR
# These should be updated regularly or fetched from an API
CURRENCY_RATES_TO_EUR = {
    'EUR': 1.0,
    'CZK': 0.04,  # 1 CZK = ~0.04 EUR (1 EUR = ~25 CZK)
    'HUF': 0.0025,  # 1 HUF = ~0.0025 EUR (1 EUR = ~400 HUF)
    'PLN': 0.23,  # 1 PLN = ~0.23 EUR (1 EUR = ~4.3 PLN)
    'USD': 0.92,  # 1 USD = ~0.92 EUR
}

# Product expense mapping (expense per item in EUR)
# Keys are SKU/EAN codes or hash-based SKUs (H-XXXXXXXX) for products without EAN
# Products not found in this mapping will default to 1.0 EUR expense
LEGACY_VEVO_PRODUCT_EXPENSES = {
    # === PARFUMY 500ml ===
    '8586024430327': 7.02,    # No.01 Cotton Paradise (500ml)
    '8586024430358': 9.38,    # No.02 Sweet Paradise (500ml)
    '8586024430433': 4.79,    # No.06 Royal Cotton (500ml)
    '8586024430457': 5.64,    # No.07 Ylang Absolute (500ml)
    '8586024430471': 6.53,    # No.08 Cotton Dream (500ml)
    '8586024430495': 5.80,    # No.09 Pure Garden (500ml)

    # === PARFUMY 200ml ===
    '8586024430310': 3.35,    # No.01 Cotton Paradise (200ml)
    '8586024430341': 4.29,    # No.02 Sweet Paradise (200ml)
    '8586024430426': 2.46,    # No.06 Royal Cotton (200ml)
    '8586024430440': 2.79,    # No.07 Ylang Absolute (200ml)
    '8586024430464': 3.15,    # No.08 Cotton Dream (200ml)
    '8586024430488': 2.86,    # No.09 Pure Garden (200ml)

    # === SADY VZORIEK ===
    'H-7D043A91': 1.79,       # Sada vzoriek vĹˇetkĂ˝ch vĂ´nĂ­ Vevo 6 x 10ml
    'H-E77D4634': 0.86,       # Sada vzoriek najpredĂˇvanejĹˇĂ­ch vĂ´nĂ­ Vevo 3 x 10ml
    'H-125E3A73': 1.79,       # Sada vzoriek vĹˇetkĂ˝ch vĂ´nĂ­ Vevo Natural 6 x 10ml
    'H-31566B7A': 0.85,       # Sada vzoriek najpredĂˇvanejĹˇĂ­ch vĂ´nĂ­ Vevo Natural 3 x 10ml

    # === PREMIUM VZORKY ===
    'H-A2620358': 0.42,       # Premium No.07 Ylang Absolute (Vzorka 10ml)
    'H-D00F4D4A': 0.47,       # Premium No.08 Cotton Dream (Vzorka 10ml)
    'H-45E7507C': 0.43,       # Premium No.09 Pure Garden (Vzorka 10ml)
    'H-29C4BDE2': 1.32,       # Vzorky parfumov do prania Vevo Premium 3 x 10ml

    # === NATURAL VZORKY ===
    '8586024430334': 0.30,    # Natural No.01 Cotton Paradise (Vzorka 10ml)
    'H-34DA3CE0': 0.35,       # Natural No.02 Sweet Paradise (Vzorka 10ml)
    'H-B3DCA297': 0.26,       # Natural No.06 Royal Cotton (Vzorka 10ml)
    'H-5854129C': 0.28,       # Natural No.07 Ylang Absolute (Vzorka 10ml)
    'H-4F7230B9': 0.29,       # Natural No.08 Cotton Dream (Vzorka 10ml)
    'H-A1EA61E5': 0.28,       # Natural No.09 Pure Garden (Vzorka 10ml)

    # === DOPLNKY ===
    'H-8F8BF46E': 0.31,       # Odmerka Vevo 7ml drevenĂˇ
    'H-3583EAEC': 0.65,       # Vevo Shot - koncentrĂˇt na ÄŤistenie prĂˇÄŤky 100ml
    'H-F03DF99A': 2.43,       # PracĂ­ gĂ©l hypoalergĂ©nny z MarseillskĂ©ho mydla 1L
    '8594201618000': 2.43,    # PracĂ­ gĂ©l hypoalergĂ©nny (EAN variant)
    'H-C633B766': 2.43,       # PracĂ­ gĂ©l LevanduÄľa 1L
    'H-95B10CAD': 2.43,       # PracĂ­ gĂ©l RuĹľa 1L
    'H-231AAF25': 2.83,       # PerkarbonĂˇt sodnĂ˝ PLUS 1kg
    'H-A2C58C41': 2.43,       # Strong PINK ÄŤistiaca pasta 500g
    'H-5916EC93': 4.80,       # ÄŚistiÄŤ podlĂˇh do robotickĂ©ho mopu
    'H-65B41890': 1.00,       # Biely ocot v spreji 500 ml
    'H-29C4BDE2': 1.00,       # InteriĂ©rovĂ˝ sprej Vevo Premium Ĺ korica & IhliÄŤie 150ml

    # === NULOVĂ‰ NĂKLADY ===
    'H-36CA74A7': 0,          # Tringelt
    'H-A5F3BBB3': 0,          # Poistenie proti rozbitiu
}
PRODUCT_EXPENSES = dict(LEGACY_VEVO_PRODUCT_EXPENSES)

def parse_input_date(value: str) -> datetime:
    """Parse common CLI/env date formats for safer project onboarding."""
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unsupported date format '{value}'. Use YYYY-MM-DD.")

# nove ceny nakladov
# PRODUCT_EXPENSES = {
#     'Sada vzoriek najpredĂˇvanejĹˇĂ­ch vĂ´nĂ­ Vevo 6 x 10ml': 1.38,
#     'Sada vzoriek vĹˇetkĂ˝ch vĂ´nĂ­ Vevo 6 x 10ml': 1.38,
#     'Sada najpredĂˇvanejĹˇĂ­ch vzoriek Vevo 6 x 10ml': 1.38,
#     'Sada 6 najpredĂˇvanejĹˇĂ­ch vzoriek po 1ks': 1.38,
#     'Sada najpredĂˇvanejĹˇĂ­ch vzoriek 6 x 10ml': 1.38,
#     'Sada vzorkĹŻ vĹˇech vĹŻnĂ­ Vevo (6 Ă— 10 ml)': 1.38,
#     'Sada vzoriek najpredĂˇvanejĹˇĂ­ch vĂ´nĂ­ Vevo 3 x 10ml': 0.69,
#     'Parfum do prania Vevo No.08 Cotton Dream (500ml)': 3.13,
#     'Vevo No.08 Cotton Dream mosĂłparfĂĽm (500ml)': 3.13,
#     'Parfum do prania Vevo No.07 Ylang Absolute (200ml)': 1.79,
#     'Vevo No.07 Ylang Absolute mosĂłparfĂĽm (200ml)': 1.79,
#     'Parfum do prania Vevo No.08 Cotton Dream (200ml)': 1.79,
#     'Parfum do prania Vevo No.09 Pure Garden (200ml)': 1.79,
#     'Parfum do prania Vevo No.01 Cotton Paradise (500ml)': 3.13,
#     'Parfum do prania Vevo No.01 Cotton Paradise (200ml)': 1.79,
#     'Parfum do prania Vevo No.09 Pure Garden (500ml)': 3.13,
#     'ParfĂ©m na pranĂ­ Vevo No.09 Pure Garden (500ml)': 1.79,
#     'Parfum do prania Vevo No.06 Royal Cotton (200ml)': 1.79,
#     'Parfum do prania Vevo No.02 Sweet Paradise (200ml)': 1.79,
#     'Odmerka Vevo 7ml drevenĂˇ na parfum do prania': 0.31,
#     'Parfum do prania Vevo No.02 Sweet Paradise (500ml)': 3.13,
#     'Parfum do prania Vevo No.07 Ylang Absolute (Vzorka 10ml)': 0.22,
#     'Parfum do prania Vevo No.07 Ylang Absolute (Vzorka)': 0.22,
#     'Parfum do prania Vevo No.06 Royal Cotton (500ml)': 3.13,
#     'Parfum do prania Vevo No.08 Cotton Dream (Vzorka 10ml)': 0.22,
#     'Parfum do prania Vevo No.08 Cotton Dream (Vzorka)': 0.22,
#     'Parfum do prania Vevo No.07 Ylang Absolute (500ml)': 3.13,
#     'Parfum do prania Vevo No.09 Pure Garden (Vzorka 10ml)': 0.22,
#     'Parfum do prania Vevo No.09 Pure Garden (Vzorka)': 0.22,
#     'Parfum do prania Vevo No.02 Sweet Paradise (Vzorka 10ml)': 0.22,
#     'Parfum do prania Vevo No.02 Sweet Paradise (Vzorka)': 0.22,
#     'Parfum do prania Vevo No.06 Royal Cotton (Vzorka 10ml)': 0.22,
#     'Parfum do prania Vevo No.06 Royal Cotton (Vzorka)': 0.22,
#     'Parfum do prania Vevo No.03 Lavender Kiss (Vzorka)': 0.22,
#     'Tringelt': 0,
#     'Parfum do prania Vevo No.01 Cotton Paradise (Vzorka 10ml)': 0.22,
#     'Poistenie proti rozbitiu': 0,
#     'Vevo Shot - koncentrĂˇt na ÄŤistenie prĂˇÄŤky 100ml': 0.65,
#     'Vevo Shot â€“ koncentrĂˇt na ÄŤiĹˇtÄ›nĂ­ praÄŤky 100 ml': 0.65
# }

# GraphQL query with fragments
ORDER_QUERY = gql("""
query GetOrders($filter: OrderFilter, $params: OrderParams) {
  getOrderList(filter: $filter, params: $params) {
    data {
      id
      order_num
      external_ref
      pur_date
      var_symb
      last_change
      oss
      oss_country
      status {
        id
        name
        color
      }
      customer {
        ... on Company {
          company_name
          company_id
          vat_id
          vat_id2
          name
          surname
          email
        }
        ... on Person {
          name
          surname
          email
        }
        ... on UnauthenticatedEmail {
          name
          surname
          email
        }
      }
      invoice_address {
        street
        descriptive_number
        orientation_number
        city
        zip
        country
      }
      items {
        item_label
        ean
        import_code
        warehouse_number
        quantity
        tax_rate
        weight {
          value
          unit
        }
        recycle_fee {
          value
          formatted
          is_net_price
          currency {
            symbol
            code
          }
        }
        price {
          value
          formatted
          is_net_price
          currency {
            symbol
            code
          }
        }
        sum {
          value
          formatted
          is_net_price
          currency {
            symbol
            code
          }
        }
        sum_with_tax {
          value
          formatted
          is_net_price
          currency {
            symbol
            code
          }
        }
      }
      sum {
        value
        formatted
        is_net_price
        currency {
          symbol
          code
        }
      }
    }
    pageInfo {
      hasNextPage
      hasPreviousPage
      nextCursor
      previousCursor
      pageIndex
      totalPages
    }
  }
}
""")


class BizniWebExporter:
    def __init__(
        self,
        api_url: str,
        api_token: str,
        project_name: str = DEFAULT_PROJECT,
        output_tag: str = "",
        artifact_subdir: str = "",
        enable_period_bundle: bool = True,
    ):
        """Initialize the exporter with API credentials"""
        self.api_url = api_url
        self.api_token = api_token
        self.project_name = project_name
        self.output_tag = sanitize_output_tag(output_tag)
        normalized_subdir = str(artifact_subdir or "").strip().strip("/\\")
        self.artifact_subdir = normalized_subdir
        self.enable_period_bundle = enable_period_bundle
        self.project_settings = load_project_settings(project_name)
        self.reporting_defaults = resolve_reporting_defaults(project_name, self.project_settings)
        self.project_root_dir = Path("data") / project_name
        self.data_dir = self.project_root_dir / normalized_subdir if normalized_subdir else self.project_root_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # Share project data directory with optional ad clients for cache isolation.
        os.environ["REPORT_PROJECT"] = project_name
        os.environ["REPORT_DATA_DIR"] = str(self.project_root_dir.resolve())
        os.environ["REPORT_OUTPUT_TAG"] = self.output_tag

        transport = RequestsHTTPTransport(
            url=api_url,
            headers={'BW-API-Key': f'Token {api_token}'},
            verify=True,
            retries=3,
            timeout=GRAPHQL_TIMEOUT_SEC,
        )
        self.client = Client(transport=transport, fetch_schema_from_transport=False)
        self.fb_client = FacebookAdsClient()
        self.google_ads_client = GoogleAdsClient()
        self.cache_dir = self.project_root_dir / 'cache'
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.weather_settings = copy.deepcopy(WEATHER_SETTINGS)
        self.weather_cache_dir = self.project_root_dir / 'weather_cache'
        self.weather_cache_dir.mkdir(parents=True, exist_ok=True)
        self.weather_client = None
        if self.weather_settings.get("enabled") and self.weather_settings.get("locations"):
            self.weather_client = WeatherClient(
                cache_dir=self.weather_cache_dir,
                timezone=self.weather_settings.get("timezone", "Europe/Bratislava"),
            )
        self.cache_days_threshold = 7  # Days from today that should always be fetched fresh (changed from 3 to 7)
        self.customer_first_order_dates = {}  # Track first order date for each customer
        self.excluded_orders = []  # Track orders with failed/excluded statuses for segmentation

    def output_path(self, filename: str) -> Path:
        """Build a path inside project-specific output directory."""
        path = self.data_dir / filename
        if not self.output_tag:
            return path
        return path.with_name(f"{path.stem}__{self.output_tag}{path.suffix}")

    @staticmethod
    def _order_purchase_datetime(order: Dict[str, Any]) -> Optional[datetime]:
        raw_value = order.get('pur_date') or order.get('purchase_date') or order.get('last_change')
        if not raw_value:
            return None
        parsed = pd.to_datetime(raw_value, errors='coerce')
        if pd.isna(parsed):
            return None
        return parsed.to_pydatetime()

    def _filter_orders_by_range(
        self,
        orders: List[Dict[str, Any]],
        date_from: datetime,
        date_to: datetime,
    ) -> List[Dict[str, Any]]:
        start_date = date_from.date()
        end_date = date_to.date()
        filtered: List[Dict[str, Any]] = []
        for order in orders:
            purchase_dt = self._order_purchase_datetime(order)
            if purchase_dt is None:
                continue
            if start_date <= purchase_dt.date() <= end_date:
                filtered.append(order)
        return filtered

    @staticmethod
    def _format_period_range_en(date_from: datetime, date_to: datetime) -> str:
        return f"{date_from.strftime('%b %d, %Y')} - {date_to.strftime('%b %d, %Y')}"

    @staticmethod
    def _format_period_range_sk(date_from: datetime, date_to: datetime) -> str:
        return f"{date_from.strftime('%d.%m.%Y')} - {date_to.strftime('%d.%m.%Y')}"

    def _build_period_variant_specs(
        self,
        date_from: datetime,
        date_to: datetime,
    ) -> List[Dict[str, Any]]:
        total_days = (date_to.date() - date_from.date()).days + 1
        candidates: List[Tuple[str, int, str]] = [
            ('7d', 7, '7D'),
            ('30d', 30, '30D'),
            ('90d', 90, '90D'),
        ]
        specs: List[Dict[str, Any]] = []
        seen_ranges = set()

        for key, days, label in candidates:
            if total_days <= days:
                continue
            variant_from = max(date_from, date_to - timedelta(days=days - 1))
            range_key = (variant_from.date().isoformat(), date_to.date().isoformat())
            if range_key in seen_ranges:
                continue
            seen_ranges.add(range_key)
            specs.append({
                'key': key,
                'label': label,
                'date_from': variant_from,
                'date_to': date_to,
                'range_en': self._format_period_range_en(variant_from, date_to),
                'range_sk': self._format_period_range_sk(variant_from, date_to),
            })

        specs.append({
            'key': 'full',
            'label': 'FULL',
            'date_from': date_from,
            'date_to': date_to,
            'range_en': self._format_period_range_en(date_from, date_to),
            'range_sk': self._format_period_range_sk(date_from, date_to),
        })
        return specs

    def _build_period_switcher_payload(
        self,
        *,
        current_key: str,
        current_path: Path,
        specs: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        options = []
        current_spec = next((spec for spec in specs if spec['key'] == current_key), specs[-1])
        for spec in specs:
            href = os.path.relpath(spec['report_path'], start=current_path.parent).replace("\\", "/")
            options.append({
                'key': spec['key'],
                'label': spec['label'],
                'href': href,
                'range_en': spec['range_en'],
                'range_sk': spec['range_sk'],
            })
        return {
            'label_en': 'Report period',
            'label_sk': 'Obdobie reportu',
            'current_key': current_key,
            'current_range_en': current_spec['range_en'],
            'current_range_sk': current_spec['range_sk'],
            'options': options,
        }

    @staticmethod
    def _build_embedded_period_reports(period_switcher: Optional[Dict[str, Any]]) -> Dict[str, str]:
        switcher = period_switcher or {}
        if str(switcher.get("current_key") or "") != "full":
            return {}
        embedded_specs = switcher.get("_embedded_specs") or []
        embedded_reports: Dict[str, str] = {}
        for spec in embedded_specs:
            key = str(spec.get("key") or "")
            report_path_raw = spec.get("report_path")
            if not key or not report_path_raw:
                continue
            report_path = Path(report_path_raw)
            if not report_path.exists():
                continue
            with open(report_path, "r", encoding="utf-8-sig") as handle:
                report_html = handle.read()
            embedded_reports[key] = base64.b64encode(report_html.encode("utf-8")).decode("ascii")
        return embedded_reports

    def _build_period_switcher_bundle(
        self,
        orders: List[Dict[str, Any]],
        date_from: datetime,
        date_to: datetime,
    ) -> Optional[Dict[str, Any]]:
        if not self.enable_period_bundle:
            return None

        specs = self._build_period_variant_specs(date_from, date_to)
        if len(specs) <= 1:
            return None

        main_report_path = self.output_path(f"report_{date_from.strftime('%Y%m%d')}-{date_to.strftime('%Y%m%d')}.html")
        bundle_root = Path("_periods") / main_report_path.stem

        for spec in specs:
            if spec['key'] == 'full':
                spec['report_path'] = main_report_path
                continue

            artifact_subdir = str(bundle_root / spec['key'])
            child_exporter = BizniWebExporter(
                self.api_url,
                self.api_token,
                project_name=self.project_name,
                output_tag=self.output_tag,
                artifact_subdir=artifact_subdir,
                enable_period_bundle=False,
            )
            spec['report_path'] = child_exporter.output_path(
                f"report_{spec['date_from'].strftime('%Y%m%d')}-{spec['date_to'].strftime('%Y%m%d')}.html"
            )
            spec['exporter'] = child_exporter

        for spec in specs:
            if spec['key'] == 'full':
                continue
            filtered_orders = self._filter_orders_by_range(orders, spec['date_from'], spec['date_to'])
            switcher_payload = self._build_period_switcher_payload(
                current_key=spec['key'],
                current_path=spec['report_path'],
                specs=specs,
            )
            spec['exporter'].export_to_csv(
                filtered_orders,
                spec['date_from'],
                spec['date_to'],
                period_switcher=switcher_payload,
            )

        payload = self._build_period_switcher_payload(
            current_key='full',
            current_path=main_report_path,
            specs=specs,
        )
        payload["_embedded_specs"] = [
            {"key": spec["key"], "report_path": str(spec["report_path"])}
            for spec in specs
            if spec["key"] != "full"
        ]
        return payload

    def _belongs_to_active_output_variant(self, file: Path) -> bool:
        if self.output_tag:
            return file.stem.endswith(f"__{self.output_tag}")
        return "__" not in file.stem

    @staticmethod
    def _build_source_entry(
        *,
        key: str,
        label: str,
        status: str,
        mode: str,
        message: str,
        healthy: bool,
        **extra: Any,
    ) -> Dict[str, Any]:
        entry: Dict[str, Any] = {
            "key": key,
            "label": label,
            "status": status,
            "mode": mode,
            "message": message,
            "healthy": healthy,
        }
        entry.update(extra)
        return entry

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        try:
            if pd.isna(value):
                return None
        except TypeError:
            pass
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _count_missing_values(frame: pd.DataFrame, column: str) -> int:
        if frame.empty or column not in frame.columns:
            return 0
        series = frame[column]
        normalized = series.astype(str).str.strip().str.lower()
        return int(series.isna().sum() + normalized.isin({"", "nan", "none", "null"}).sum())

    @classmethod
    def _build_attribution_qa(
        cls,
        *,
        cost_per_order: Optional[Dict[str, Any]],
        fb_campaigns: Optional[List[Dict[str, Any]]],
        total_orders: int,
    ) -> Dict[str, Any]:
        reconciliation = ((cost_per_order or {}).get("fb_spend_reconciliation") or {})
        summary = ((cost_per_order or {}).get("campaign_attribution_summary") or {})
        campaign_rows = list((cost_per_order or {}).get("campaign_attribution") or [])
        daily_source_spend = cls._safe_float(summary.get("daily_source_spend"))
        if daily_source_spend is None:
            daily_source_spend = cls._safe_float(reconciliation.get("daily_source_spend"))
        campaign_source_spend = cls._safe_float(summary.get("campaign_source_spend"))
        if campaign_source_spend is None:
            campaign_source_spend = cls._safe_float(reconciliation.get("campaign_source_spend"))
        coverage_ratio = cls._safe_float(summary.get("coverage_ratio"))
        if coverage_ratio is None:
            coverage_ratio = cls._safe_float(reconciliation.get("coverage_ratio"))
        estimated_orders_total = cls._safe_float(summary.get("estimated_orders_total"))
        oversubscription_ratio = cls._safe_float(summary.get("oversubscription_ratio"))
        warnings: List[str] = []

        if (daily_source_spend or 0) > 0 and (campaign_source_spend is None or campaign_source_spend <= 0):
            warnings.append("Campaign-level Facebook spend is missing while daily Facebook spend exists.")
        elif coverage_ratio is None and (daily_source_spend or 0) > 0:
            warnings.append("Campaign spend coverage ratio is unavailable for a period with Facebook spend.")
        elif coverage_ratio is not None and (coverage_ratio < 0.90 or coverage_ratio > 1.10):
            warnings.append(
                f"Campaign spend coverage ratio is {coverage_ratio:.2f}x. Expected range is 0.90x-1.10x."
            )

        if total_orders > 0 and not campaign_rows and (daily_source_spend or 0) > 0:
            warnings.append("Campaign attribution table is empty although Facebook daily spend exists.")
        if oversubscription_ratio is not None and oversubscription_ratio > 1.05:
            warnings.append(
                f"Attributed campaign orders sum to {oversubscription_ratio:.2f}x of total orders. Attribution fallback is likely overstating campaign output."
            )

        platform_rows_checked = 0
        platform_cost_mismatch_count = 0
        for row in fb_campaigns or []:
            spend = cls._safe_float(row.get("spend"))
            platform_conversions = cls._safe_float(row.get("platform_conversions", row.get("conversions")))
            platform_cpa = cls._safe_float(
                row.get("cost_per_platform_conversion", row.get("cost_per_conversion"))
            )
            if spend is None:
                continue
            if platform_conversions is None or platform_conversions <= 0:
                continue
            platform_rows_checked += 1
            expected_cpa = spend / platform_conversions
            if platform_cpa is None or abs(expected_cpa - platform_cpa) > 0.05:
                platform_cost_mismatch_count += 1

        if platform_cost_mismatch_count > 0:
            warnings.append(
                f"{platform_cost_mismatch_count} campaign row(s) have platform CPA that does not match spend/platform conversions."
            )

        status = "warning" if warnings else "ok"
        message = (
            "Campaign attribution QA passed: coverage, attribution totals, and platform CPA are within tolerance."
            if not warnings
            else warnings[0]
        )
        return {
            "key": "attribution",
            "label": "Campaign Attribution QA",
            "status": status,
            "healthy": not warnings,
            "message": message,
            "warnings": warnings,
            "coverage_ratio": round(coverage_ratio, 4) if coverage_ratio is not None else None,
            "oversubscription_ratio": round(oversubscription_ratio, 4) if oversubscription_ratio is not None else None,
            "daily_source_spend": round(daily_source_spend, 2) if daily_source_spend is not None else None,
            "campaign_source_spend": round(campaign_source_spend, 2) if campaign_source_spend is not None else None,
            "estimated_orders_total": round(estimated_orders_total, 1) if estimated_orders_total is not None else None,
            "total_orders": int(total_orders),
            "campaign_rows": len(campaign_rows),
            "platform_rows_checked": platform_rows_checked,
            "platform_cost_mismatch_count": platform_cost_mismatch_count,
            "attribution_method": summary.get("attribution_method") or "unknown",
        }

    @staticmethod
    def _finalize_source_health(source_health: Dict[str, Any]) -> Dict[str, Any]:
        sources = list((source_health.get("sources") or {}).values())
        degraded = [source["label"] for source in sources if source.get("status") in {"warning", "error"}]
        qa_checks = list((source_health.get("qa") or {}).values())
        qa_warnings = [check.get("label") or check.get("key") or "QA" for check in qa_checks if check.get("status") == "warning"]
        qa_errors = [check.get("label") or check.get("key") or "QA" for check in qa_checks if check.get("status") in {"error", "critical"}]
        qa_failure_count = int(sum(int(check.get("failure_count") or 0) for check in qa_checks))
        qa_warning_count = int(sum(int(check.get("warning_count") or 0) for check in qa_checks))
        if degraded:
            overall_status = "partial"
        elif qa_errors:
            overall_status = "critical"
        elif qa_warnings:
            overall_status = "warning"
        else:
            overall_status = "full"
        source_health["overall_status"] = overall_status
        source_health["is_partial"] = bool(degraded)
        source_health["partial_sources"] = degraded
        source_health["qa_status"] = "critical" if qa_errors else ("warning" if qa_warnings else "ok")
        source_health["qa_warnings"] = qa_warnings
        source_health["qa_errors"] = qa_errors
        source_health["qa_failure_count"] = qa_failure_count
        source_health["qa_warning_count"] = qa_warning_count
        source_health["qa_check_count"] = len(qa_checks)
        if degraded:
            source_health["summary"] = (
                "Partial data: "
                + ", ".join(degraded)
                + " did not load cleanly in this run. Metrics depending on these sources may be incomplete or zero-filled."
            )
        elif qa_errors:
            source_health["summary"] = (
                "Critical data QA issues were raised for "
                + ", ".join(qa_errors)
                + ". Treat affected metrics as unsafe for decision-making until the assertions are resolved."
            )
        elif qa_warnings:
            source_health["summary"] = (
                "Data sources loaded, but QA warnings were raised for "
                + ", ".join(qa_warnings)
                + ". Treat attribution-style metrics with caution."
            )
        else:
            source_health["summary"] = "All enabled external sources loaded successfully for this run."
        return source_health

    def _write_data_quality_file(
        self,
        source_health: Dict[str, Any],
        date_from: datetime,
        date_to: datetime,
    ) -> Path:
        quality_path = self.output_path(f"data_quality_{date_from.strftime('%Y%m%d')}-{date_to.strftime('%Y%m%d')}.json")
        with open(quality_path, "w", encoding="utf-8") as f:
            json.dump(source_health, f, ensure_ascii=False, indent=2)
        print(f"Data quality metadata saved: {quality_path}")
        return quality_path

    def _geo_confidence_settings(self, level: str) -> Dict[str, int]:
        raw = dict((self.project_settings or {}).get("geo_confidence") or {})
        defaults = {
            "country": {
                "ignore_orders_below": 5,
                "observe_orders_below": 20,
                "chart_min_orders": 5,
            },
            "city": {
                "ignore_orders_below": 3,
                "observe_orders_below": 10,
                "chart_min_orders": 3,
            },
        }
        scope = dict(defaults.get(level, {}))
        scope.update(
            {
                str(k): int(v)
                for k, v in dict(raw.get(level) or {}).items()
                if str(k) in {"ignore_orders_below", "observe_orders_below", "chart_min_orders"}
            }
        )
        scope["ignore_orders_below"] = max(int(scope.get("ignore_orders_below", 1)), 1)
        scope["observe_orders_below"] = max(int(scope.get("observe_orders_below", scope["ignore_orders_below"] + 1)), scope["ignore_orders_below"] + 1)
        scope["chart_min_orders"] = max(int(scope.get("chart_min_orders", scope["ignore_orders_below"])), 1)
        return scope

    def _geo_confidence_payload(self, orders: Any, *, level: str) -> Dict[str, Any]:
        order_count = int(round(self._safe_float(orders) or 0))
        settings = self._geo_confidence_settings(level)
        ignore_below = settings["ignore_orders_below"]
        observe_below = settings["observe_orders_below"]

        if order_count < ignore_below:
            status = "ignore"
            label = "Ignore"
        elif order_count < observe_below:
            status = "observe"
            label = "Observe"
        else:
            status = "ready"
            label = "Ready"

        confidence_score = 100.0 if order_count >= observe_below else round((order_count / observe_below) * 100, 1)
        return {
            "confidence_status": status,
            "confidence_label": label,
            "confidence_score": confidence_score,
            "low_sample": status != "ready",
            "hide_economics": order_count < settings["chart_min_orders"],
            "ignore_orders_below": ignore_below,
            "observe_orders_below": observe_below,
            "chart_min_orders": settings["chart_min_orders"],
        }

    def _build_geo_qa(self, country_analysis: Optional[pd.DataFrame], geo_profitability: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        country_df = country_analysis.copy() if isinstance(country_analysis, pd.DataFrame) else pd.DataFrame()
        geo_table = (geo_profitability or {}).get("table")
        geo_df = geo_table.copy() if isinstance(geo_table, pd.DataFrame) else pd.DataFrame()

        warnings: List[str] = []
        ignore_count = 0
        observe_count = 0
        ready_count = 0
        ignore_orders = 0.0
        observe_orders = 0.0
        ignore_revenue = 0.0
        observe_revenue = 0.0
        total_orders = 0.0
        total_revenue = 0.0

        if not geo_df.empty and "confidence_status" in geo_df.columns:
            if "orders" in geo_df.columns:
                total_orders = float(pd.to_numeric(geo_df["orders"], errors="coerce").fillna(0).sum())
            if "revenue" in geo_df.columns:
                total_revenue = float(pd.to_numeric(geo_df["revenue"], errors="coerce").fillna(0).sum())
            ignore_count = int((geo_df["confidence_status"] == "ignore").sum())
            observe_count = int((geo_df["confidence_status"] == "observe").sum())
            ready_count = int((geo_df["confidence_status"] == "ready").sum())
            if total_orders > 0 and "orders" in geo_df.columns:
                ignore_orders = float(pd.to_numeric(geo_df.loc[geo_df["confidence_status"] == "ignore", "orders"], errors="coerce").fillna(0).sum())
                observe_orders = float(pd.to_numeric(geo_df.loc[geo_df["confidence_status"] == "observe", "orders"], errors="coerce").fillna(0).sum())
            if total_revenue > 0 and "revenue" in geo_df.columns:
                ignore_revenue = float(pd.to_numeric(geo_df.loc[geo_df["confidence_status"] == "ignore", "revenue"], errors="coerce").fillna(0).sum())
                observe_revenue = float(pd.to_numeric(geo_df.loc[geo_df["confidence_status"] == "observe", "revenue"], errors="coerce").fillna(0).sum())
            if ignore_count > 0:
                warnings.append(
                    f"{ignore_count} country row(s) are below the minimum geo sample threshold and should not be treated as strategic market insight."
                )
            if observe_count > 0:
                warnings.append(
                    f"{observe_count} country row(s) are in observe mode only. Treat margin/CPO reads as directional rather than decisive."
                )

        unknown_country_rate = None
        if not country_df.empty and "country" in country_df.columns:
            total_country_orders = float(country_df["orders"].sum()) if "orders" in country_df.columns else 0.0
            unknown_orders = float(country_df.loc[country_df["country"].astype(str).str.lower() == "unknown", "orders"].sum()) if total_country_orders > 0 else 0.0
            unknown_country_rate = round((unknown_orders / total_country_orders) * 100, 2) if total_country_orders > 0 else 0.0
            if unknown_country_rate > 0:
                warnings.append(f"Unknown country coverage is {unknown_country_rate:.2f}% of orders.")

        ignore_order_share_pct = round((ignore_orders / total_orders) * 100, 2) if total_orders > 0 else 0.0
        observe_order_share_pct = round((observe_orders / total_orders) * 100, 2) if total_orders > 0 else 0.0
        ignore_revenue_share_pct = round((ignore_revenue / total_revenue) * 100, 2) if total_revenue > 0 else 0.0
        observe_revenue_share_pct = round((observe_revenue / total_revenue) * 100, 2) if total_revenue > 0 else 0.0
        if ignore_order_share_pct >= 10 or ignore_revenue_share_pct >= 10:
            warnings.append(
                f"Low-confidence geo rows still represent {ignore_order_share_pct:.2f}% of orders and {ignore_revenue_share_pct:.2f}% of revenue."
            )

        message = (
            "Geo confidence guardrails passed: country sample sizes are large enough for strategic comparison."
            if not warnings
            else warnings[0]
        )
        return {
            "key": "geo",
            "label": "Geo confidence",
            "status": "warning" if warnings else "ok",
            "healthy": not warnings,
            "message": message,
            "warnings": warnings,
            "warning_count": len(warnings),
            "failure_count": 0,
            "ignore_count": ignore_count,
            "observe_count": observe_count,
            "ready_count": ready_count,
            "unknown_country_rate": unknown_country_rate,
            "ignore_order_share_pct": ignore_order_share_pct,
            "observe_order_share_pct": observe_order_share_pct,
            "ignore_revenue_share_pct": ignore_revenue_share_pct,
            "observe_revenue_share_pct": observe_revenue_share_pct,
            "low_confidence_order_share_pct": round(ignore_order_share_pct + observe_order_share_pct, 2),
            "low_confidence_revenue_share_pct": round(ignore_revenue_share_pct + observe_revenue_share_pct, 2),
        }

    def _build_data_assertions_qa(
        self,
        *,
        financial_metrics: Optional[Dict[str, Any]],
        consistency_checks: Optional[Dict[str, Any]],
        refunds_analysis: Optional[Dict[str, Any]],
        day_of_week_analysis: Optional[pd.DataFrame],
        advanced_dtc_metrics: Optional[Dict[str, Any]],
        country_analysis: Optional[pd.DataFrame],
        geo_profitability: Optional[Dict[str, Any]],
        cost_per_order: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        warnings: List[str] = []
        failures: List[str] = []
        metrics = financial_metrics or {}
        checks = consistency_checks or {}
        refund_summary = (refunds_analysis or {}).get("summary") or {}
        dow_df = day_of_week_analysis.copy() if isinstance(day_of_week_analysis, pd.DataFrame) else pd.DataFrame()
        attach_df = (advanced_dtc_metrics or {}).get("attach_rate")
        attach_df = attach_df.copy() if isinstance(attach_df, pd.DataFrame) else pd.DataFrame()
        country_df = country_analysis.copy() if isinstance(country_analysis, pd.DataFrame) else pd.DataFrame()
        geo_table = (geo_profitability or {}).get("table")
        geo_df = geo_table.copy() if isinstance(geo_table, pd.DataFrame) else pd.DataFrame()
        campaign_rows = list((cost_per_order or {}).get("campaign_attribution") or [])
        attribution_summary = (cost_per_order or {}).get("campaign_attribution_summary") or {}

        required_financial_keys = [
            "pre_ad_contribution_per_order",
            "break_even_cac",
            "payback_orders",
            "contribution_ltv_cac",
            "cm1_profit",
            "cm1_profit_per_order",
            "cm1_profit_per_customer",
            "cm2_profit",
            "cm3_profit",
        ]
        missing_financial_keys = [key for key in required_financial_keys if metrics.get(key) is None]
        if missing_financial_keys:
            failures.append(
                "Critical economics registry keys are missing: " + ", ".join(missing_financial_keys[:5])
            )

        shell_parity_failures = 0
        shell_pre_ad = self._safe_float(metrics.get("pre_ad_contribution_per_order"))
        cm1_per_order = self._safe_float(metrics.get("cm1_profit_per_order"))
        break_even_cac = self._safe_float(metrics.get("break_even_cac"))
        cm1_per_customer = self._safe_float(metrics.get("cm1_profit_per_customer"))
        payback_orders = self._safe_float(metrics.get("payback_orders"))
        current_fb_cac = self._safe_float(metrics.get("current_fb_cac"))
        expected_payback = None
        if current_fb_cac is not None and cm1_per_order not in (None, 0):
            expected_payback = current_fb_cac / cm1_per_order
        if shell_pre_ad is not None and cm1_per_order is not None and abs(shell_pre_ad - cm1_per_order) > 0.05:
            shell_parity_failures += 1
        if break_even_cac is not None and cm1_per_customer is not None and abs(break_even_cac - cm1_per_customer) > 0.05:
            shell_parity_failures += 1
        if payback_orders is not None and expected_payback is not None and abs(payback_orders - expected_payback) > 0.05:
            shell_parity_failures += 1
        if shell_parity_failures > 0:
            failures.append(
                f"{shell_parity_failures} shell/library economics parity check(s) failed."
            )

        if refund_summary.get("refund_orders") and metrics.get("refund_rate_pct") is None:
            failures.append("Refund orders exist but refund summary metrics are missing from the registry.")

        if checks:
            if checks.get("roas_ok") is False:
                failures.append(
                    f"ROAS consistency delta is {checks.get('roas_delta')}. Reported and derived ROAS do not match."
                )
            if checks.get("company_margin_ok") is False:
                failures.append(
                    f"Company margin consistency delta is {checks.get('company_margin_delta_pct')} percentage points."
                )
            if checks.get("cac_ok") is False:
                failures.append(
                    f"CAC consistency delta is {checks.get('cac_delta')}. Check spend/new-customer denominator alignment."
                )

        day_name_missing = self._count_missing_values(dow_df, "day_name")
        if day_name_missing > 0:
            warnings.append(f"{day_name_missing} weekday effectiveness row(s) are missing day_name.")

        anchor_missing = self._count_missing_values(attach_df, "anchor_item")
        attached_missing = self._count_missing_values(attach_df, "attached_item")
        if anchor_missing > 0:
            warnings.append(f"{anchor_missing} attach-rate row(s) are missing anchor_item.")
        if attached_missing > 0:
            warnings.append(f"{attached_missing} attach-rate row(s) are missing attached_item.")
        anchor_orders_col = "anchor_orders" if "anchor_orders" in attach_df.columns else ("key_orders" if "key_orders" in attach_df.columns else None)
        if not attach_df.empty and anchor_orders_col:
            anchor_orders_missing = int(attach_df[anchor_orders_col].isna().sum())
            if anchor_orders_missing > 0:
                warnings.append(f"{anchor_orders_missing} attach-rate row(s) are missing anchor_orders.")
        else:
            anchor_orders_missing = 0

        country_missing = self._count_missing_values(country_df, "country")
        geo_country_missing = self._count_missing_values(geo_df, "country")
        if country_missing > 0:
            warnings.append(f"{country_missing} geographic country row(s) are missing country labels.")
        if geo_country_missing > 0:
            warnings.append(f"{geo_country_missing} geo profitability row(s) are missing country labels.")

        platform_cpa_mismatches = 0
        attributed_cpa_mismatches = 0
        for row in campaign_rows:
            spend = self._safe_float(row.get("spend"))
            platform_conversions = self._safe_float(row.get("platform_conversions", row.get("conversions")))
            platform_cpa = self._safe_float(row.get("cost_per_platform_conversion", row.get("cost_per_conversion")))
            if spend is not None and platform_conversions not in (None, 0):
                expected = spend / platform_conversions
                if platform_cpa is None or abs(expected - platform_cpa) > 0.05:
                    platform_cpa_mismatches += 1

            attributed_orders = self._safe_float(row.get("attributed_orders_est"))
            attributed_cpa = self._safe_float(row.get("cost_per_attributed_order"))
            if spend is not None and attributed_orders not in (None, 0):
                expected = spend / attributed_orders
                if attributed_cpa is None or abs(expected - attributed_cpa) > 0.05:
                    attributed_cpa_mismatches += 1

        if platform_cpa_mismatches > 0:
            failures.append(
                f"{platform_cpa_mismatches} campaign row(s) have platform CPA that does not match spend/platform conversions."
            )
        if attributed_cpa_mismatches > 0:
            failures.append(
                f"{attributed_cpa_mismatches} campaign row(s) have cost_per_attributed_order that does not match spend/attributed_orders_est."
            )

        attributed_orders_total = self._safe_float(attribution_summary.get("estimated_orders_total"))
        total_orders = self._safe_float(attribution_summary.get("total_orders") or metrics.get("total_orders"))
        attributed_orders_ratio = None
        if attributed_orders_total is not None and total_orders and total_orders > 0:
            attributed_orders_ratio = attributed_orders_total / total_orders
            if attributed_orders_ratio > 1.05:
                failures.append(
                    f"Attributed campaign orders sum to {attributed_orders_total:.1f}, which exceeds total orders ({total_orders:.0f}) beyond tolerance."
                )

        label_row_total = len(dow_df.index) + len(attach_df.index) + len(country_df.index) + len(geo_df.index)
        missing_label_total = day_name_missing + anchor_missing + attached_missing + anchor_orders_missing + country_missing + geo_country_missing
        null_label_rate_pct = round((missing_label_total / label_row_total) * 100, 2) if label_row_total > 0 else 0.0
        if null_label_rate_pct > 0:
            warnings.append(f"Dimension completeness warning: {null_label_rate_pct:.2f}% of labeled QA rows are missing a required label.")

        message = (
            "Data assertions passed: economics registry, arithmetic and dimensions are within tolerance."
            if not (failures or warnings)
            else (failures + warnings)[0]
        )
        return {
            "key": "data_assertions",
            "label": "Data assertions",
            "status": "critical" if failures else ("warning" if warnings else "ok"),
            "healthy": not failures,
            "message": message,
            "warnings": warnings,
            "failures": failures,
            "warning_count": len(warnings),
            "failure_count": len(failures),
            "missing_financial_keys": missing_financial_keys,
            "shell_parity_failures": shell_parity_failures,
            "day_name_missing": day_name_missing,
            "attach_anchor_missing": anchor_missing,
            "attach_attached_missing": attached_missing,
            "anchor_orders_missing": anchor_orders_missing,
            "country_missing": country_missing,
            "geo_country_missing": geo_country_missing,
            "missing_label_total": missing_label_total,
            "null_label_rate_pct": null_label_rate_pct,
            "platform_cpa_mismatches": platform_cpa_mismatches,
            "attributed_cpa_mismatches": attributed_cpa_mismatches,
            "attributed_orders_ratio": round(attributed_orders_ratio, 4) if attributed_orders_ratio is not None else None,
            "attributed_orders_tolerance_breached": bool(attributed_orders_ratio is not None and attributed_orders_ratio > 1.05),
            "label_row_total": label_row_total,
        }

    def _build_margin_stability_qa(self, date_agg: Optional[pd.DataFrame]) -> Dict[str, Any]:
        date_df = date_agg.copy() if isinstance(date_agg, pd.DataFrame) else pd.DataFrame()
        required = {"date", "total_revenue", "pre_ad_contribution_profit", "fixed_daily_cost", "net_profit"}
        if date_df.empty or not required.issubset(set(date_df.columns)):
            return {
                "key": "margin_stability",
                "label": "Margin stability",
                "status": "ok",
                "healthy": True,
                "message": "Margin stability QA skipped because the daily contribution series is unavailable.",
                "warnings": [],
            }

        frame = date_df.sort_values("date").copy()
        revenue = frame["total_revenue"].fillna(0.0)
        frame["pre_ad_margin_with_fixed_pct"] = np.where(
            revenue > 0,
            ((frame["pre_ad_contribution_profit"] - frame["fixed_daily_cost"]) / revenue) * 100,
            np.nan,
        )
        frame["company_margin_with_fixed_pct"] = np.where(
            revenue > 0,
            (frame["net_profit"] / revenue) * 100,
            np.nan,
        )
        frame["pre_ad_margin_with_fixed_pct_ma7"] = frame["pre_ad_margin_with_fixed_pct"].rolling(7, min_periods=1).mean()
        frame["company_margin_with_fixed_pct_ma7"] = frame["company_margin_with_fixed_pct"].rolling(7, min_periods=1).mean()

        raw_extreme_days = int((frame["pre_ad_margin_with_fixed_pct"].abs() > 150).fillna(False).sum())
        smoothed_extreme_days = int((frame["pre_ad_margin_with_fixed_pct_ma7"].abs() > 100).fillna(False).sum())
        min_smoothed = self._safe_float(frame["pre_ad_margin_with_fixed_pct_ma7"].min())
        max_smoothed = self._safe_float(frame["pre_ad_margin_with_fixed_pct_ma7"].max())

        warnings: List[str] = []
        if smoothed_extreme_days > 0:
            warnings.append(
                f"7-day smoothed fixed-margin series still shows {smoothed_extreme_days} extreme day(s) beyond +/-100%."
            )
        elif raw_extreme_days > 0:
            warnings.append(
                f"Raw fixed-margin series has {raw_extreme_days} extreme day(s), but the 7-day smoothing stays within tolerance."
            )

        message = (
            "Smoothed fixed-margin series is within tolerance."
            if not warnings
            else warnings[0]
        )
        return {
            "key": "margin_stability",
            "label": "Margin stability",
            "status": "warning" if warnings else "ok",
            "healthy": not warnings,
            "message": message,
            "warnings": warnings,
            "warning_count": len(warnings),
            "failure_count": 0,
            "raw_extreme_days": raw_extreme_days,
            "smoothed_extreme_days": smoothed_extreme_days,
            "min_smoothed_margin_pct": round(min_smoothed, 2) if min_smoothed is not None else None,
            "max_smoothed_margin_pct": round(max_smoothed, 2) if max_smoothed is not None else None,
        }

    @staticmethod
    def get_product_sku(ean: str, title: str) -> str:
        """
        Get a consistent product SKU/identifier.
        Uses EAN if available, otherwise creates a short hash from the title.
        """
        if pd.notna(ean) and str(ean).strip() and str(ean).strip() != '':
            return str(ean).strip()
        # Create a short hash from the title (8 characters)
        title_hash = hashlib.md5(str(title).encode()).hexdigest()[:8].upper()
        return f"H-{title_hash}"

    def add_product_sku_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add a consistent product_sku column to the dataframe."""
        df['product_sku'] = df.apply(
            lambda row: self.get_product_sku(row.get('item_ean'), row.get('item_label', 'Unknown')),
            axis=1
        )
        return df

    @staticmethod
    def _is_sample_item_label(label: Any) -> bool:
        text = str(label or "").strip().lower()
        if not text:
            return False
        sample_keywords = ['vzor', 'vzorka', 'vzorky', 'sample']
        return any(keyword in text for keyword in sample_keywords)

    @staticmethod
    def _fullsize_size_bucket(label: Any) -> Optional[str]:
        text = str(label or "").strip().lower()
        if not text:
            return None
        if '500ml' in text or '500 ml' in text:
            return '500ml'
        if '200ml' in text or '200 ml' in text:
            return '200ml'
        return None

    @classmethod
    def _is_fullsize_item_label(cls, label: Any) -> bool:
        return cls._fullsize_size_bucket(label) is not None

    @staticmethod
    def _normalize_match_text(value: Any) -> str:
        """Normalize strings for robust contains-based matching across accents/encoding/punctuation."""
        text = unicodedata.normalize('NFKD', str(value or ''))
        text = ''.join(ch for ch in text if not unicodedata.combining(ch)).lower()
        text = re.sub(r'[^a-z0-9]+', ' ', text)
        return re.sub(r'\s+', ' ', text).strip()

    @classmethod
    def _matches_patterns(cls, label: str, patterns: List[str]) -> bool:
        if not label or not patterns:
            return False
        normalized_label = cls._normalize_match_text(label)
        for pattern in patterns:
            normalized_pattern = cls._normalize_match_text(pattern)
            if normalized_pattern and normalized_pattern in normalized_label:
                return True
        return False

    def _bundle_accessory_config(self) -> Dict[str, Any]:
        config = self.project_settings.get("bundle_accessory_model") or {}
        return config if isinstance(config, dict) else {}

    def _product_family_groups_config(self) -> List[Dict[str, Any]]:
        groups = self.project_settings.get("product_family_groups") or []
        return groups if isinstance(groups, list) else []

    def _match_named_group(self, label: Any, groups: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
        for group in groups or []:
            if self._matches_patterns(str(label or ""), group.get("patterns") or []):
                group_key = str(group.get("key") or "").strip()
                group_label = str(group.get("label") or group_key).strip()
                if group_key:
                    return group_key, group_label
        return None, None

    @staticmethod
    def _source_proxy_from_spend(fb_spend: Any, google_spend: Any) -> Tuple[str, str]:
        fb_value = float(fb_spend or 0.0)
        google_value = float(google_spend or 0.0)
        if fb_value > 0 and google_value > 0:
            return "mixed_paid_day", "Mixed paid day"
        if fb_value > 0:
            return "facebook_paid_day", "Facebook-paid day"
        if google_value > 0:
            return "google_paid_day", "Google-paid day"
        return "organic_unknown_day", "Organic / unknown day"

    def analyze_acquisition_source_product_family_cube(
        self,
        orders_df: pd.DataFrame,
        item_df: pd.DataFrame,
        customer_orders: pd.DataFrame,
        revenue_col: str,
    ) -> dict:
        family_groups = self._product_family_groups_config()
        if not family_groups or orders_df.empty or item_df.empty or customer_orders.empty:
            return {
                "summary": {},
                "cube_rows": pd.DataFrame(),
                "source_rows": pd.DataFrame(),
                "family_rows": pd.DataFrame(),
            }

        required_item_columns = {"order_num", "item_label", "item_total_without_tax"}
        if not required_item_columns.issubset(item_df.columns):
            return {
                "summary": {},
                "cube_rows": pd.DataFrame(),
                "source_rows": pd.DataFrame(),
                "family_rows": pd.DataFrame(),
            }

        classified_rows = []
        for row in item_df[["order_num", "item_label", "item_total_without_tax"]].itertuples(index=False):
            family_key, family_label = self._match_named_group(row.item_label, family_groups)
            if not family_key:
                continue
            classified_rows.append(
                {
                    "order_num": row.order_num,
                    "product_family_key": family_key,
                    "product_family_label": family_label,
                    "item_total_without_tax": float(row.item_total_without_tax or 0.0),
                }
            )

        if not classified_rows:
            return {
                "summary": {},
                "cube_rows": pd.DataFrame(),
                "source_rows": pd.DataFrame(),
                "family_rows": pd.DataFrame(),
            }

        classified_df = pd.DataFrame(classified_rows)
        family_by_order = (
            classified_df.groupby(["order_num", "product_family_key", "product_family_label"], as_index=False)
            .agg(order_family_revenue=("item_total_without_tax", "sum"))
            .sort_values(["order_num", "order_family_revenue"], ascending=[True, False])
            .drop_duplicates(subset=["order_num"])
        )
        order_family_map = family_by_order.set_index("order_num")[["product_family_key", "product_family_label"]]

        order_source_frame = orders_df[
            [
                "order_num",
                "customer_email",
                "purchase_datetime",
                revenue_col,
                "pre_ad_contribution",
                "fb_ads_daily_spend",
                "google_ads_daily_spend",
            ]
        ].drop_duplicates(subset=["order_num"]).copy()
        order_source_frame = order_source_frame.merge(order_family_map, left_on="order_num", right_index=True, how="left")
        order_source_frame["product_family_key"] = order_source_frame["product_family_key"].fillna("other_unclassified")
        order_source_frame["product_family_label"] = order_source_frame["product_family_label"].fillna("Other / unclassified")
        proxy_pairs = order_source_frame.apply(
            lambda row: self._source_proxy_from_spend(row.get("fb_ads_daily_spend"), row.get("google_ads_daily_spend")),
            axis=1,
        )
        order_source_frame["source_proxy_key"] = proxy_pairs.apply(lambda value: value[0])
        order_source_frame["source_proxy_label"] = proxy_pairs.apply(lambda value: value[1])

        first_orders = (
            order_source_frame.sort_values(["customer_email", "purchase_datetime"])
            .drop_duplicates(subset=["customer_email"], keep="first")
            .copy()
        )
        first_orders["first_order_revenue"] = first_orders[revenue_col]
        first_orders["first_order_contribution"] = first_orders["pre_ad_contribution"]

        customer_enriched = customer_orders.merge(
            first_orders[
                [
                    "customer_email",
                    "source_proxy_key",
                    "source_proxy_label",
                    "product_family_key",
                    "product_family_label",
                ]
            ],
            on="customer_email",
            how="left",
        )

        def _repeat_within_days(group: pd.DataFrame, days: int) -> int:
            return int(((group["days_since_first"] > 0) & (group["days_since_first"] <= days)).any())

        customer_level_rows = []
        for customer_email, group in customer_enriched.groupby("customer_email"):
            first_row = group.sort_values("purchase_datetime").iloc[0]
            window_90 = group[group["days_since_first"] <= 90].copy()
            customer_level_rows.append(
                {
                    "customer_email": customer_email,
                    "source_proxy_key": first_row.get("source_proxy_key"),
                    "source_proxy_label": first_row.get("source_proxy_label"),
                    "product_family_key": first_row.get("product_family_key"),
                    "product_family_label": first_row.get("product_family_label"),
                    "first_order_revenue": float(first_row.get("first_order_revenue") or 0.0),
                    "first_order_contribution": float(first_row.get("first_order_contribution") or 0.0),
                    "orders_total": int(group["order_num"].nunique()),
                    "repeat_60d_flag": _repeat_within_days(group, 60),
                    "repeat_90d_flag": _repeat_within_days(group, 90),
                    "revenue_ltv_90d": float(window_90[revenue_col].sum()),
                    "contribution_ltv_90d": float(window_90["pre_ad_contribution"].sum()),
                }
            )

        customer_level = pd.DataFrame(customer_level_rows)
        if customer_level.empty:
            return {
                "summary": {},
                "cube_rows": pd.DataFrame(),
                "source_rows": pd.DataFrame(),
                "family_rows": pd.DataFrame(),
            }

        cube_rows = (
            customer_level.groupby(
                ["source_proxy_key", "source_proxy_label", "product_family_key", "product_family_label"],
                as_index=False,
            )
            .agg(
                new_customers=("customer_email", "nunique"),
                first_order_revenue=("first_order_revenue", "sum"),
                first_order_contribution=("first_order_contribution", "sum"),
                repeat_60d_customers=("repeat_60d_flag", "sum"),
                repeat_90d_customers=("repeat_90d_flag", "sum"),
                revenue_ltv_90d=("revenue_ltv_90d", "sum"),
                contribution_ltv_90d=("contribution_ltv_90d", "sum"),
            )
        )
        cube_rows["first_order_aov"] = cube_rows.apply(
            lambda row: (row["first_order_revenue"] / row["new_customers"]) if row["new_customers"] > 0 else 0.0,
            axis=1,
        )
        cube_rows["first_order_contribution_per_order"] = cube_rows.apply(
            lambda row: (row["first_order_contribution"] / row["new_customers"]) if row["new_customers"] > 0 else 0.0,
            axis=1,
        )
        cube_rows["repeat_60d_rate_pct"] = cube_rows.apply(
            lambda row: (row["repeat_60d_customers"] / row["new_customers"] * 100) if row["new_customers"] > 0 else 0.0,
            axis=1,
        )
        cube_rows["repeat_90d_rate_pct"] = cube_rows.apply(
            lambda row: (row["repeat_90d_customers"] / row["new_customers"] * 100) if row["new_customers"] > 0 else 0.0,
            axis=1,
        )
        cube_rows["revenue_ltv_90d_per_customer"] = cube_rows.apply(
            lambda row: (row["revenue_ltv_90d"] / row["new_customers"]) if row["new_customers"] > 0 else 0.0,
            axis=1,
        )
        cube_rows["contribution_ltv_90d_per_customer"] = cube_rows.apply(
            lambda row: (row["contribution_ltv_90d"] / row["new_customers"]) if row["new_customers"] > 0 else 0.0,
            axis=1,
        )
        cube_rows = cube_rows.sort_values(
            ["new_customers", "contribution_ltv_90d_per_customer", "repeat_90d_rate_pct"],
            ascending=[False, False, False],
        ).reset_index(drop=True)

        source_rows = (
            cube_rows.groupby(["source_proxy_key", "source_proxy_label"], as_index=False)
            .agg(
                new_customers=("new_customers", "sum"),
                revenue_ltv_90d=("revenue_ltv_90d", "sum"),
                contribution_ltv_90d=("contribution_ltv_90d", "sum"),
            )
        )
        source_rows["contribution_ltv_90d_per_customer"] = source_rows.apply(
            lambda row: (row["contribution_ltv_90d"] / row["new_customers"]) if row["new_customers"] > 0 else 0.0,
            axis=1,
        )

        family_rows = (
            cube_rows.groupby(["product_family_key", "product_family_label"], as_index=False)
            .agg(
                new_customers=("new_customers", "sum"),
                first_order_revenue=("first_order_revenue", "sum"),
                contribution_ltv_90d=("contribution_ltv_90d", "sum"),
                repeat_90d_customers=("repeat_90d_customers", "sum"),
            )
        )
        family_rows["repeat_90d_rate_pct"] = family_rows.apply(
            lambda row: (row["repeat_90d_customers"] / row["new_customers"] * 100) if row["new_customers"] > 0 else 0.0,
            axis=1,
        )
        family_rows["contribution_ltv_90d_per_customer"] = family_rows.apply(
            lambda row: (row["contribution_ltv_90d"] / row["new_customers"]) if row["new_customers"] > 0 else 0.0,
            axis=1,
        )

        return {
            "summary": {
                "proxy_method": "first_order_day_paid_spend_presence",
                "source_count": int(source_rows["source_proxy_key"].nunique()) if not source_rows.empty else 0,
                "family_count": int(family_rows["product_family_key"].nunique()) if not family_rows.empty else 0,
                "cube_rows": int(len(cube_rows)),
                "new_customers_covered": int(cube_rows["new_customers"].sum()) if not cube_rows.empty else 0,
            },
            "cube_rows": cube_rows,
            "source_rows": source_rows.sort_values("new_customers", ascending=False).reset_index(drop=True),
            "family_rows": family_rows.sort_values("new_customers", ascending=False).reset_index(drop=True),
        }

    def analyze_bundle_accessory_model(
        self,
        orders_df: pd.DataFrame,
        item_df: pd.DataFrame,
        revenue_col: str,
    ) -> dict:
        config = self._bundle_accessory_config()
        if not config.get("enabled"):
            return {
                "summary": {},
                "pair_rows": pd.DataFrame(),
                "device_family_rows": pd.DataFrame(),
                "accessory_group_rows": pd.DataFrame(),
            }

        anchor_groups = config.get("anchor_groups") or []
        accessory_groups = config.get("accessory_groups") or []
        if not anchor_groups or not accessory_groups or orders_df.empty or item_df.empty:
            return {
                "summary": {},
                "pair_rows": pd.DataFrame(),
                "device_family_rows": pd.DataFrame(),
                "accessory_group_rows": pd.DataFrame(),
            }

        classified_rows = []
        for row in item_df[["order_num", "item_label", "product_sku"]].itertuples(index=False):
            anchor_key, anchor_label = self._match_named_group(row.item_label, anchor_groups)
            accessory_key, accessory_label = self._match_named_group(row.item_label, accessory_groups)
            if not anchor_key and not accessory_key:
                continue
            classified_rows.append(
                {
                    "order_num": row.order_num,
                    "item_label": row.item_label,
                    "product_sku": row.product_sku,
                    "anchor_group_key": anchor_key,
                    "anchor_group_label": anchor_label,
                    "accessory_group_key": accessory_key,
                    "accessory_group_label": accessory_label,
                }
            )

        classified = pd.DataFrame(classified_rows)
        if classified.empty:
            return {
                "summary": {},
                "pair_rows": pd.DataFrame(),
                "device_family_rows": pd.DataFrame(),
                "accessory_group_rows": pd.DataFrame(),
            }

        anchor_matches = classified[classified["anchor_group_key"].notna()][
            ["order_num", "anchor_group_key", "anchor_group_label"]
        ].drop_duplicates()
        accessory_matches = classified[classified["accessory_group_key"].notna()][
            ["order_num", "accessory_group_key", "accessory_group_label"]
        ].drop_duplicates()
        if anchor_matches.empty or accessory_matches.empty:
            return {
                "summary": {
                    "anchor_group_count": int(anchor_matches["anchor_group_key"].nunique()) if not anchor_matches.empty else 0,
                    "accessory_group_count": int(accessory_matches["accessory_group_key"].nunique()) if not accessory_matches.empty else 0,
                },
                "pair_rows": pd.DataFrame(),
                "device_family_rows": pd.DataFrame(),
                "accessory_group_rows": pd.DataFrame(),
            }

        orders_lookup = orders_df[["order_num", revenue_col, "pre_ad_contribution"]].drop_duplicates(subset=["order_num"]).copy()
        orders_lookup[revenue_col] = pd.to_numeric(orders_lookup[revenue_col], errors="coerce").fillna(0.0)
        orders_lookup["pre_ad_contribution"] = pd.to_numeric(orders_lookup["pre_ad_contribution"], errors="coerce").fillna(0.0)

        pair_matches = anchor_matches.merge(accessory_matches, on="order_num", how="inner")
        pair_rows = []
        for (anchor_key, anchor_label), anchor_group_df in anchor_matches.groupby(["anchor_group_key", "anchor_group_label"]):
            anchor_order_nums = set(anchor_group_df["order_num"].astype(str))
            anchor_orders_df = orders_lookup[orders_lookup["order_num"].astype(str).isin(anchor_order_nums)].copy()
            anchor_orders = len(anchor_order_nums)
            if anchor_orders == 0 or anchor_orders_df.empty:
                continue

            anchor_avg_aov = float(anchor_orders_df[revenue_col].mean())
            anchor_avg_contribution = float(anchor_orders_df["pre_ad_contribution"].mean())
            anchor_pairs = pair_matches[pair_matches["anchor_group_key"] == anchor_key]
            if anchor_pairs.empty:
                continue

            for (accessory_key, accessory_label), pair_df in anchor_pairs.groupby(["accessory_group_key", "accessory_group_label"]):
                attached_order_nums = set(pair_df["order_num"].astype(str))
                attached_orders_df = anchor_orders_df[anchor_orders_df["order_num"].astype(str).isin(attached_order_nums)].copy()
                without_orders_df = anchor_orders_df[~anchor_orders_df["order_num"].astype(str).isin(attached_order_nums)].copy()
                attached_orders = len(attached_order_nums)
                if attached_orders == 0 or attached_orders_df.empty:
                    continue

                avg_order_value_with = float(attached_orders_df[revenue_col].mean())
                avg_contribution_with = float(attached_orders_df["pre_ad_contribution"].mean())
                avg_order_value_without = float(without_orders_df[revenue_col].mean()) if not without_orders_df.empty else np.nan
                avg_contribution_without = float(without_orders_df["pre_ad_contribution"].mean()) if not without_orders_df.empty else np.nan

                pair_rows.append(
                    {
                        "anchor_group_key": anchor_key,
                        "anchor_group_label": anchor_label,
                        "accessory_group_key": accessory_key,
                        "accessory_group_label": accessory_label,
                        "anchor_orders": int(anchor_orders),
                        "attached_orders": int(attached_orders),
                        "attach_rate_pct": round((attached_orders / anchor_orders * 100), 1),
                        "anchor_avg_order_value": round(anchor_avg_aov, 2),
                        "anchor_avg_pre_ad_contribution": round(anchor_avg_contribution, 2),
                        "avg_order_value_with_accessory": round(avg_order_value_with, 2),
                        "avg_order_value_without_accessory": round(avg_order_value_without, 2) if pd.notna(avg_order_value_without) else np.nan,
                        "avg_pre_ad_contribution_with_accessory": round(avg_contribution_with, 2),
                        "avg_pre_ad_contribution_without_accessory": round(avg_contribution_without, 2) if pd.notna(avg_contribution_without) else np.nan,
                        "revenue_uplift_per_order": round(avg_order_value_with - avg_order_value_without, 2) if pd.notna(avg_order_value_without) else np.nan,
                        "contribution_uplift_per_order": round(avg_contribution_with - avg_contribution_without, 2) if pd.notna(avg_contribution_without) else np.nan,
                    }
                )

        pair_rows_df = pd.DataFrame(pair_rows)
        if pair_rows_df.empty:
            return {
                "summary": {
                    "anchor_group_count": int(anchor_matches["anchor_group_key"].nunique()),
                    "accessory_group_count": int(accessory_matches["accessory_group_key"].nunique()),
                    "anchor_orders_total": int(anchor_matches["order_num"].nunique()),
                    "device_family_count": int(anchor_matches["anchor_group_key"].nunique()),
                },
                "pair_rows": pd.DataFrame(),
                "device_family_rows": pd.DataFrame(),
                "accessory_group_rows": pd.DataFrame(),
            }

        pair_rows_df = pair_rows_df.sort_values(
            ["anchor_orders", "attach_rate_pct", "contribution_uplift_per_order"],
            ascending=[False, False, False],
        ).reset_index(drop=True)

        device_family_rows = []
        for (anchor_key, anchor_label), anchor_df in pair_rows_df.groupby(["anchor_group_key", "anchor_group_label"]):
            ranked = anchor_df.sort_values(
                ["contribution_uplift_per_order", "attach_rate_pct", "attached_orders"],
                ascending=[False, False, False],
                na_position="last",
            )
            best_row = ranked.iloc[0]
            device_family_rows.append(
                {
                    "anchor_group_key": anchor_key,
                    "anchor_group_label": anchor_label,
                    "anchor_orders": int(anchor_df["anchor_orders"].max()),
                    "anchor_avg_order_value": round(float(anchor_df["anchor_avg_order_value"].max()), 2),
                    "anchor_avg_pre_ad_contribution": round(float(anchor_df["anchor_avg_pre_ad_contribution"].max()), 2),
                    "best_accessory_group_key": best_row["accessory_group_key"],
                    "best_accessory_group_label": best_row["accessory_group_label"],
                    "best_attach_rate_pct": round(float(best_row["attach_rate_pct"]), 1),
                    "best_contribution_uplift_per_order": round(float(best_row["contribution_uplift_per_order"]), 2)
                    if pd.notna(best_row["contribution_uplift_per_order"]) else np.nan,
                    "best_revenue_uplift_per_order": round(float(best_row["revenue_uplift_per_order"]), 2)
                    if pd.notna(best_row["revenue_uplift_per_order"]) else np.nan,
                }
            )
        device_family_rows_df = pd.DataFrame(device_family_rows).sort_values("anchor_orders", ascending=False)

        accessory_group_rows = []
        for (accessory_key, accessory_label), accessory_df in pair_rows_df.groupby(["accessory_group_key", "accessory_group_label"]):
            weighted_attach = np.average(
                accessory_df["attach_rate_pct"],
                weights=accessory_df["anchor_orders"].clip(lower=1),
            ) if not accessory_df.empty else np.nan
            best_anchor = accessory_df.sort_values(
                ["contribution_uplift_per_order", "attach_rate_pct", "attached_orders"],
                ascending=[False, False, False],
                na_position="last",
            ).iloc[0]
            accessory_group_rows.append(
                {
                    "accessory_group_key": accessory_key,
                    "accessory_group_label": accessory_label,
                    "covered_anchor_groups": int(accessory_df["anchor_group_key"].nunique()),
                    "pair_rows": int(len(accessory_df)),
                    "attached_orders_total": int(accessory_df["attached_orders"].sum()),
                    "weighted_attach_rate_pct": round(float(weighted_attach), 1) if pd.notna(weighted_attach) else np.nan,
                    "avg_contribution_uplift_per_order": round(float(accessory_df["contribution_uplift_per_order"].dropna().mean()), 2)
                    if accessory_df["contribution_uplift_per_order"].notna().any() else np.nan,
                    "best_anchor_group_label": best_anchor["anchor_group_label"],
                }
            )
        accessory_group_rows_df = pd.DataFrame(accessory_group_rows).sort_values("attached_orders_total", ascending=False)

        summary = {
            "anchor_group_count": int(anchor_matches["anchor_group_key"].nunique()),
            "accessory_group_count": int(accessory_matches["accessory_group_key"].nunique()),
            "anchor_orders_total": int(anchor_matches["order_num"].nunique()),
            "device_family_count": int(device_family_rows_df["anchor_group_key"].nunique()) if not device_family_rows_df.empty else 0,
            "pair_row_count": int(len(pair_rows_df)),
            "best_attach_rate_pct": round(float(pair_rows_df["attach_rate_pct"].max()), 1) if not pair_rows_df.empty else np.nan,
            "best_contribution_uplift_per_order": round(float(pair_rows_df["contribution_uplift_per_order"].max()), 2)
            if pair_rows_df["contribution_uplift_per_order"].notna().any() else np.nan,
        }

        return {
            "summary": summary,
            "pair_rows": pair_rows_df,
            "device_family_rows": device_family_rows_df,
            "accessory_group_rows": accessory_group_rows_df,
        }

    @staticmethod
    def _distribute_total_spend(total: float, date_from: datetime, date_to: datetime) -> Dict[str, float]:
        """
        Distribute a fixed total spend across all dates in the selected period.
        Uses cent-level allocation so the sum matches the requested total exactly.
        """
        total = float(total or 0)
        if date_to < date_from:
            return {}
        days = (date_to.date() - date_from.date()).days + 1
        if days <= 0:
            return {}

        total_cents = int(round(total * 100))
        base_cents = total_cents // days
        remainder = total_cents % days

        distributed: Dict[str, float] = {}
        for idx in range(days):
            day = date_from.date() + timedelta(days=idx)
            cents = base_cents + (1 if idx < remainder else 0)
            distributed[day.strftime('%Y-%m-%d')] = cents / 100.0
        return distributed

    def add_order_revenue_net_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add canonical order-level net revenue used across analytics.
        Definition: sum(item_total_without_tax) per order_num (EUR, net of VAT).
        """
        if 'item_total_without_tax' not in df.columns:
            df['order_revenue_net'] = df.get('order_total', 0)
            return df

        order_net_map = df.groupby('order_num')['item_total_without_tax'].sum().to_dict()
        df['order_revenue_net'] = df['order_num'].map(order_net_map).fillna(0).round(2)
        return df

    def deduplicate_orders(self, orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Remove duplicated raw orders by stable key (order_num preferred, fallback id).
        Keeps the last occurrence.
        """
        dedup_map: Dict[str, Dict[str, Any]] = {}
        unknown_orders: List[Dict[str, Any]] = []
        duplicate_count = 0

        for order in orders:
            order_num = str(order.get('order_num') or '').strip()
            order_id = str(order.get('id') or '').strip()
            dedup_key = order_num or (f"id:{order_id}" if order_id else '')

            if not dedup_key:
                # Very rare: no order_num and no id. Keep as-is to avoid data loss.
                unknown_orders.append(order)
                continue

            if dedup_key in dedup_map:
                duplicate_count += 1
            dedup_map[dedup_key] = order

        deduped_orders = list(dedup_map.values()) + unknown_orders
        if duplicate_count > 0:
            logger.warning(f"Removed {duplicate_count} duplicated raw orders before flattening")
            print(f"Deduplication: removed {duplicate_count} duplicated raw orders")

        return deduped_orders
    
    def get_daily_fixed_cost(self, date: datetime) -> float:
        """Calculate daily fixed cost based on days in the month"""
        days_in_month = calendar.monthrange(date.year, date.month)[1]
        return FIXED_MONTHLY_COST / days_in_month
    
    def convert_to_eur(self, amount: float, currency: str) -> float:
        """Convert amount from given currency to EUR"""
        if not currency or not amount:
            return 0.0
        
        currency = currency.upper()
        if currency not in CURRENCY_RATES_TO_EUR:
            print(f"Warning: Unknown currency {currency}, treating as EUR")
            return amount
        
        return amount * CURRENCY_RATES_TO_EUR[currency]
    
    def fetch_orders_for_month(self, date_from: datetime, date_to: datetime) -> List[Dict[str, Any]]:
        """
        Fetch orders for a specific date range (typically one month) with retry logic

        Note: API filter requires partner token, so we fetch all orders and filter client-side
        """
        all_orders = []
        has_next_page = True
        cursor = None
        max_retries = 3
        retry_delay = 10
        page_delay = 0.5  # 500ms delay between pages
        consecutive_errors = 0

        logger.info(f"Fetching orders for month from API (will filter client-side for {date_from.strftime('%Y-%m-%d')} to {date_to.strftime('%Y-%m-%d')})")

        while has_next_page:
            # Remove filter parameter as it requires partner token
            variables = {
                'params': {
                    'limit': 30,
                    'order_by': 'pur_date',
                    'sort': 'ASC'
                }
            }
            
            if cursor is not None:
                variables['params']['cursor'] = cursor
            
            retry_count = 0
            success = False
            
            while retry_count < max_retries and not success:
                try:
                    result = self.client.execute(ORDER_QUERY, variable_values=variables)
                    orders_data = result.get('getOrderList', {})
                    orders = orders_data.get('data', [])
                    all_orders.extend(orders)
                    
                    page_info = orders_data.get('pageInfo', {})
                    has_next_page = page_info.get('hasNextPage', False)
                    cursor = page_info.get('nextCursor')
                    
                    print(f"Fetched {len(orders)} orders (total: {len(all_orders)})")
                    success = True
                    consecutive_errors = 0  # Reset error counter on success

                    # Delay between pages to avoid overwhelming the API
                    if has_next_page:
                        time.sleep(page_delay)

                except Exception as e:
                    retry_count += 1
                    consecutive_errors += 1

                    # Log the full error details
                    error_msg = str(e)
                    logger.debug(f"GraphQL error details: {error_msg}")

                    # Print full stack trace for debugging
                    if os.getenv('DEBUG'):
                        logger.debug("Full stack trace:")
                        logger.debug(traceback.format_exc())

                    if retry_count < max_retries:
                        logger.warning(f"Error fetching orders (attempt {retry_count}/{max_retries}): {error_msg[:200]}")
                        print(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    else:
                        logger.error(f"Error fetching orders after {max_retries} attempts: {error_msg[:200]}")
                        logger.error(f"Full error: {error_msg}")
                        logger.error(f"Stack trace:\n{traceback.format_exc()}")

                        # If we've had too many consecutive errors and have some data, return what we have
                        if consecutive_errors >= 3 and all_orders:
                            logger.info(f"Returning {len(all_orders)} orders fetched so far due to persistent errors")
                            has_next_page = False
                        else:
                            # Otherwise just break this pagination loop
                            has_next_page = False
                        break

        logger.info(f"Fetched {len(all_orders)} total orders from API for month")

        # Filter by date range (client-side since API filter requires partner token)
        date_filtered_orders = []
        for order in all_orders:
            pur_date_str = order.get('pur_date', '')
            if pur_date_str:
                try:
                    # Parse date (format: YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
                    pur_date = datetime.strptime(pur_date_str.split()[0], '%Y-%m-%d')
                    # Check if order is within date range
                    if date_from <= pur_date <= date_to:
                        date_filtered_orders.append(order)
                except (ValueError, IndexError) as e:
                    logger.warning(f"Could not parse date '{pur_date_str}' for order {order.get('order_num', 'unknown')}: {e}")
                    continue

        logger.info(f"Filtered to {len(date_filtered_orders)} orders within date range for month")

        # Filter out orders with excluded statuses
        excluded_statuses = [
            'Storno',
            'Platba online - platnosĹĄ vyprĹˇala',
            'Platba online - platba zamietnutĂˇ',
            'ÄŚakĂˇ na Ăşhradu',
            'GoPay - platebni metoda potvrzena'
        ]

        filtered_orders = []
        excluded_counts = {}

        for order in date_filtered_orders:
            status = order.get('status', {}) or {}
            status_name = status.get('name', '')

            if status_name not in excluded_statuses:
                filtered_orders.append(order)
            else:
                excluded_counts[status_name] = excluded_counts.get(status_name, 0) + 1

        # Report excluded orders
        if excluded_counts:
            print("\nFiltered out orders:")
            for status, count in excluded_counts.items():
                print(f"  - {status}: {count} orders")

        logger.info(f"Final count after status filtering for month: {len(filtered_orders)} orders")

        return filtered_orders

    def get_cache_filename(self, date: datetime) -> Path:
        """Generate cache filename for a specific date"""
        date_str = date.strftime('%Y-%m-%d')
        return self.cache_dir / f"orders_{date_str}.json"
    
    def should_use_cache(self, date: datetime) -> bool:
        """Determine if cache should be used for a given date"""
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        date_normalized = date.replace(hour=0, minute=0, second=0, microsecond=0)
        days_ago = (today - date_normalized).days
        
        # Always fetch fresh data for recent days
        if days_ago <= self.cache_days_threshold:
            return False
        
        # Use cache for older data
        cache_file = self.get_cache_filename(date)
        return cache_file.exists()
    
    def load_from_cache(self, date: datetime) -> Optional[List[Dict[str, Any]]]:
        """Load orders from cache for a specific date"""
        cache_file = self.get_cache_filename(date)
        if not cache_file.exists():
            return None
        
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                print(f"  Loaded {len(data.get('orders', []))} orders from cache for {date.strftime('%Y-%m-%d')}")
                return data.get('orders', [])
        except Exception as e:
            print(f"  Error loading cache for {date.strftime('%Y-%m-%d')}: {e}")
            return None
    
    def save_to_cache(self, date: datetime, orders: List[Dict[str, Any]]):
        """Save orders to cache for a specific date"""
        cache_file = self.get_cache_filename(date)
        
        try:
            # Filter orders for this specific date
            date_str = date.strftime('%Y-%m-%d')
            day_orders = []
            
            for order in orders:
                # Handle different date formats
                purchase_date = order.get('purchase_date', '')
                if purchase_date:
                    # Extract just the date part if it includes time
                    if ' ' in purchase_date:
                        purchase_date = purchase_date.split(' ')[0]
                    
                    if purchase_date == date_str:
                        day_orders.append(order)
            
            cache_data = {
                'date': date_str,
                'cached_at': datetime.now().isoformat(),
                'order_count': len(day_orders),
                'orders': day_orders
            }
            
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            
            if day_orders:
                print(f"  Cached {len(day_orders)} orders for {date_str}")
        except Exception as e:
            print(f"  Error saving cache for {date.strftime('%Y-%m-%d')}: {e}")
    
    def _group_consecutive_dates(self, dates: List[datetime]) -> List[Tuple[datetime, datetime]]:
        """Group consecutive dates into ranges"""
        if not dates:
            return []
        
        dates = sorted(dates)
        ranges = []
        start = dates[0]
        end = dates[0]
        
        for date in dates[1:]:
            if (date - end).days == 1:
                end = date
            else:
                ranges.append((start, end))
                start = date
                end = date
        
        ranges.append((start, end))
        return ranges
    
    def fetch_all_orders_bulk(self, max_orders: int = 900, start_cursor: str = None, sort_order: str = 'DESC') -> tuple[List[Dict[str, Any]], str]:
        """
        Fetch orders from API in bulk, stopping before hitting API limits

        Args:
            max_orders: Maximum orders to fetch (default 900 to stay under ~960 API limit)
            start_cursor: Cursor to continue from (for pagination across batches)
            sort_order: Sort order for orders (DESC = newest first, ASC = oldest first)

        Returns:
            Tuple of (orders list, next cursor)
        """
        all_orders = []
        has_next_page = True
        cursor = start_cursor
        max_retries = 3
        retry_delay = 10
        page_delay = 0.5  # 500ms delay between pages

        logger.info(f"Fetching up to {max_orders} orders from API in bulk ({sort_order} order)" + (f", continuing from cursor" if cursor else ""))

        while has_next_page and len(all_orders) < max_orders:
            variables = {
                'params': {
                    'limit': 30,
                    'order_by': 'pur_date',
                    'sort': sort_order  # DESC = newest first (for recent orders), ASC = oldest first (for historical)
                }
            }

            if cursor is not None:
                variables['params']['cursor'] = cursor

            retry_count = 0
            success = False

            while retry_count < max_retries and not success:
                try:
                    result = self.client.execute(ORDER_QUERY, variable_values=variables)
                    orders_data = result.get('getOrderList', {})
                    orders = orders_data.get('data', [])
                    all_orders.extend(orders)

                    page_info = orders_data.get('pageInfo', {})
                    has_next_page = page_info.get('hasNextPage', False)
                    cursor = page_info.get('nextCursor')

                    print(f"Fetched {len(orders)} orders (total: {len(all_orders)})")
                    success = True

                    # Stop if we're approaching the limit
                    if len(all_orders) >= max_orders:
                        logger.info(f"Reached {len(all_orders)} orders, stopping to avoid API limits")
                        has_next_page = False

                    # Delay between pages to avoid overwhelming the API
                    if has_next_page:
                        time.sleep(page_delay)

                except Exception as e:
                    retry_count += 1
                    error_msg = str(e)

                    if retry_count < max_retries:
                        logger.warning(f"Error fetching orders (attempt {retry_count}/{max_retries}): {error_msg[:200]}")
                        print(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    else:
                        logger.error(f"Error fetching orders after {max_retries} attempts: {error_msg[:200]}")
                        logger.info(f"Returning {len(all_orders)} orders fetched before error")
                        has_next_page = False
                        break

        logger.info(f"Bulk fetch complete: {len(all_orders)} orders" + (f", next cursor available" if cursor else ""))
        return all_orders, cursor

    def fetch_orders(self, date_from: datetime, date_to: datetime) -> List[Dict[str, Any]]:
        """Fetch all orders within the specified date range, using cache for older data"""
        all_orders = []
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

        # Clear excluded orders from previous runs
        self.excluded_orders = []

        print(f"\nProcessing date range: {date_from.strftime('%Y-%m-%d')} to {date_to.strftime('%Y-%m-%d')}")
        print(f"Cache policy: Using cache for data older than {self.cache_days_threshold} days")

        # Check which dates need fetching (not in cache)
        dates_to_fetch = []
        current_date = date_from

        while current_date <= date_to:
            days_ago = (today - current_date).days

            # Check if we should use cache for this date
            if self.should_use_cache(current_date):
                cached_orders = self.load_from_cache(current_date)
                if cached_orders:
                    all_orders.extend(cached_orders)
                    current_date += timedelta(days=1)
                    continue

            # This date needs fetching
            dates_to_fetch.append(current_date)
            current_date += timedelta(days=1)

        # If we have dates to fetch, do bulk fetches in batches
        if dates_to_fetch:
            print(f"\nFetching orders from API for {len(dates_to_fetch)} uncached dates...")
            dates_to_fetch_set = {d.strftime('%Y-%m-%d') for d in dates_to_fetch}

            # Determine sort order based on what dates we're fetching
            # If fetching recent dates (within last 7 days), use DESC to get newest first
            # If fetching older dates, use ASC to get oldest first (more efficient for historical data)
            recent_dates = [d for d in dates_to_fetch if (today - d).days <= self.cache_days_threshold]
            primary_sort_order = 'DESC' if recent_dates else 'ASC'

            logger.info(
                f"Using {primary_sort_order} order: {len(recent_dates)} recent dates, "
                f"{len(dates_to_fetch) - len(recent_dates)} older dates"
            )

            # Fetch in multiple batches until we truly cover the requested boundary.
            # NOTE: We cannot stop based on "all dates found" because some dates may
            # legitimately have zero orders.
            orders_by_date = {}
            max_batches = 200  # Safety limit for pathological pagination loops
            earliest_needed = min(dates_to_fetch)
            latest_needed = max(dates_to_fetch)

            def run_bulk_pass(
                sort_order: str,
                target_earliest: Optional[datetime] = None,
                target_latest: Optional[datetime] = None,
            ) -> Dict[str, Any]:
                """Run one directional bulk pass and collect orders by date."""
                next_cursor = None
                batch_num = 1
                reached_boundary = False
                oldest_seen = None
                latest_seen = None

                while batch_num <= max_batches:
                    print(f"Batch {batch_num} ({sort_order} order)...")
                    bulk_orders, next_cursor = self.fetch_all_orders_bulk(
                        max_orders=900,
                        start_cursor=next_cursor,
                        sort_order=sort_order
                    )

                    if not bulk_orders:
                        logger.warning(
                            f"Empty batch returned in {sort_order} mode (cursor_present={bool(next_cursor)}). "
                            "Stopping current pass."
                        )
                        break

                    parsed_batch_dates = []
                    for order in bulk_orders:
                        pur_date_str = order.get('pur_date', '')
                        if pur_date_str:
                            try:
                                pur_date = datetime.strptime(pur_date_str.split()[0], '%Y-%m-%d')
                                parsed_batch_dates.append(pur_date)
                                date_key = pur_date.strftime('%Y-%m-%d')

                                # Only keep orders within requested range
                                if date_key in dates_to_fetch_set:
                                    if date_key not in orders_by_date:
                                        orders_by_date[date_key] = []
                                    orders_by_date[date_key].append(order)
                            except (ValueError, IndexError):
                                continue

                    latest_in_batch = max(parsed_batch_dates) if parsed_batch_dates else None
                    oldest_in_batch = min(parsed_batch_dates) if parsed_batch_dates else None

                    if oldest_in_batch is not None:
                        oldest_seen = oldest_in_batch if oldest_seen is None else min(oldest_seen, oldest_in_batch)
                    if latest_in_batch is not None:
                        latest_seen = latest_in_batch if latest_seen is None else max(latest_seen, latest_in_batch)

                    # Stop once pagination reached pass target boundary.
                    if sort_order == 'DESC' and target_earliest is not None and oldest_in_batch is not None and oldest_in_batch <= target_earliest:
                        logger.info(
                            f"Reached start boundary in DESC mode (oldest_in_batch={oldest_in_batch.strftime('%Y-%m-%d')}, "
                            f"needed={target_earliest.strftime('%Y-%m-%d')})"
                        )
                        reached_boundary = True
                        break

                    if sort_order == 'ASC' and target_latest is not None and latest_in_batch is not None and latest_in_batch >= target_latest:
                        logger.info(
                            f"Reached end boundary in ASC mode (latest_in_batch={latest_in_batch.strftime('%Y-%m-%d')}, "
                            f"needed={target_latest.strftime('%Y-%m-%d')})"
                        )
                        reached_boundary = True
                        break

                    if not next_cursor:
                        logger.info(f"No next cursor available in {sort_order} mode, stopping current pass")
                        break

                    batch_num += 1
                    time.sleep(1)  # Small delay between batches

                if batch_num > max_batches:
                    logger.warning(
                        f"Stopped after max_batches={max_batches} in {sort_order} mode without fully confirming range boundary "
                        f"({date_from.strftime('%Y-%m-%d')} to {date_to.strftime('%Y-%m-%d')})"
                    )

                return {
                    'reached_boundary': reached_boundary,
                    'oldest_seen': oldest_seen,
                    'latest_seen': latest_seen,
                }

            # Primary pass
            primary_stats = run_bulk_pass(
                sort_order=primary_sort_order,
                target_earliest=earliest_needed if primary_sort_order == 'DESC' else None,
                target_latest=latest_needed if primary_sort_order == 'ASC' else None,
            )

            # Fallback pass from the opposite side when primary direction fails to reach boundary.
            # This protects full-history exports from intermittent cursor/page failures.
            if not primary_stats['reached_boundary']:
                if primary_sort_order == 'DESC':
                    # We already have newest side; fetch oldest side up to overlap with DESC data.
                    fallback_target_latest = primary_stats['oldest_seen'] or latest_needed
                    logger.warning(
                        "Primary DESC pass did not reach full historical boundary. "
                        f"Running ASC fallback up to {fallback_target_latest.strftime('%Y-%m-%d')}."
                    )
                    run_bulk_pass(
                        sort_order='ASC',
                        target_latest=fallback_target_latest,
                    )
                else:
                    # We already have oldest side; fetch newest side down to overlap with ASC data.
                    fallback_target_earliest = primary_stats['latest_seen'] or earliest_needed
                    logger.warning(
                        "Primary ASC pass did not reach latest boundary. "
                        f"Running DESC fallback down to {fallback_target_earliest.strftime('%Y-%m-%d')}."
                    )
                    run_bulk_pass(
                        sort_order='DESC',
                        target_earliest=fallback_target_earliest,
                    )

            # Cache and add orders for each date
            for date in dates_to_fetch:
                date_str = date.strftime('%Y-%m-%d')
                day_orders = orders_by_date.get(date_str, [])
                days_ago = (today - date).days

                # Filter by status
                filtered_orders = self._filter_by_status(day_orders)

                # Validate that all orders are actually from the requested date
                validated_orders = []
                seen_day_order_keys = set()
                for order in filtered_orders:
                    pur_date_str = order.get('pur_date', '')
                    if pur_date_str:
                        try:
                            order_date = datetime.strptime(pur_date_str.split()[0], '%Y-%m-%d')
                            if order_date.strftime('%Y-%m-%d') == date_str:
                                dedupe_key = order.get('order_num') or order.get('id')
                                if dedupe_key in seen_day_order_keys:
                                    continue
                                seen_day_order_keys.add(dedupe_key)
                                validated_orders.append(order)
                            else:
                                logger.warning(f"Order {order.get('order_num', 'unknown')} has date {order_date.strftime('%Y-%m-%d')} but was in {date_str} bucket")
                        except (ValueError, IndexError) as e:
                            logger.warning(f"Could not validate date for order {order.get('order_num', 'unknown')}: {e}")

                print(f"  {date_str}: {len(validated_orders)} orders")
                all_orders.extend(validated_orders)

                # Cache if appropriate
                if days_ago > self.cache_days_threshold:
                    self.save_to_cache_simple(date, validated_orders)

        # Final validation: ensure all orders are within the overall date range
        final_validated_orders = []
        seen_final_order_keys = set()
        out_of_range_count = 0
        for order in all_orders:
            pur_date_str = order.get('pur_date', '')
            if pur_date_str:
                try:
                    order_date = datetime.strptime(pur_date_str.split()[0], '%Y-%m-%d')
                    if date_from <= order_date <= date_to:
                        dedupe_key = order.get('order_num') or order.get('id')
                        if dedupe_key in seen_final_order_keys:
                            continue
                        seen_final_order_keys.add(dedupe_key)
                        final_validated_orders.append(order)
                    else:
                        out_of_range_count += 1
                        logger.warning(f"Order {order.get('order_num', 'unknown')} date {order_date.strftime('%Y-%m-%d')} is outside requested range {date_from.strftime('%Y-%m-%d')} to {date_to.strftime('%Y-%m-%d')}")
                except (ValueError, IndexError):
                    # If we can't parse the date, skip it
                    out_of_range_count += 1

        if out_of_range_count > 0:
            logger.warning(f"Filtered out {out_of_range_count} orders outside the requested date range")

        return final_validated_orders

    def _filter_by_status(self, orders: List[Dict[str, Any]], track_excluded: bool = True) -> List[Dict[str, Any]]:
        """Filter out orders with excluded statuses.

        Args:
            orders: List of orders to filter
            track_excluded: If True, store excluded orders for later segmentation analysis
        """
        excluded_statuses = [
            'Storno',
            'Platba online - platnosĹĄ vyprĹˇala',
            'Platba online - platba zamietnutĂˇ',
            'ÄŚakĂˇ na Ăşhradu',
            'GoPay - platebni metoda potvrzena'
        ]

        # Statuses for failed payment segmentation (subset of excluded)
        failed_payment_statuses = [
            'Platba online - platnosĹĄ vyprĹˇala',
            'Platba online - platba zamietnutĂˇ'
        ]

        filtered_orders = []
        for order in orders:
            status = order.get('status', {}) or {}
            status_name = status.get('name', '')

            if status_name not in excluded_statuses:
                filtered_orders.append(order)
            elif track_excluded and status_name in failed_payment_statuses:
                # Track failed payment orders for segmentation
                self.excluded_orders.append(order)

        return filtered_orders
    
    def save_to_cache_simple(self, date: datetime, orders: List[Dict[str, Any]]):
        """Save orders to cache for a specific date (simplified version for single-day fetches)"""
        cache_file = self.get_cache_filename(date)
        
        try:
            cache_data = {
                'date': date.strftime('%Y-%m-%d'),
                'cached_at': datetime.now().isoformat(),
                'order_count': len(orders),
                'orders': orders
            }
            
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            
            if orders:
                print(f"  Cached {len(orders)} orders for {date.strftime('%Y-%m-%d')}")
        except Exception as e:
            print(f"  Error saving cache for {date.strftime('%Y-%m-%d')}: {e}")
    
    def _fetch_orders_original(self, date_from: datetime, date_to: datetime) -> List[Dict[str, Any]]:
        """Original fetch orders method (renamed for use in new caching logic)"""
        all_orders = []
        
        # Generate weekly ranges
        current_date = date_from
        week_number = 1
        
        while current_date <= date_to:
            # Calculate week end (7 days from current date, but not beyond date_to)
            week_end = min(current_date + timedelta(days=6), date_to)
            
            print(f"  Week {week_number} ({current_date.strftime('%Y-%m-%d')} to {week_end.strftime('%Y-%m-%d')})...")
            
            try:
                week_orders = self.fetch_orders_for_period(current_date, week_end)
                if week_orders:
                    all_orders.extend(week_orders)
                    print(f"  Successfully fetched {len(week_orders)} orders for week {week_number}")
                else:
                    print(f"  No orders fetched for week {week_number}")
            except Exception as e:
                print(f"  Failed to fetch week {week_number}: {e}")
                # Try fetching in smaller chunks (3-day periods)
                print(f"  Trying to fetch week {week_number} in smaller chunks...")
                chunk_start = current_date
                while chunk_start <= week_end:
                    chunk_end = min(chunk_start + timedelta(days=2), week_end)
                    try:
                        print(f"    Fetching {chunk_start.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}...")
                        chunk_orders = self.fetch_orders_for_period(chunk_start, chunk_end)
                        if chunk_orders:
                            all_orders.extend(chunk_orders)
                            print(f"    Got {len(chunk_orders)} orders")
                    except Exception as e:
                        print(f"    Failed to fetch chunk: {e}")
                    chunk_start = chunk_end + timedelta(days=1)
            
            # Move to next week
            current_date = week_end + timedelta(days=1)
            week_number += 1
            
            # Wait 2 seconds between weekly requests to avoid overwhelming the API
            if current_date <= date_to:
                time.sleep(2)
        
        return all_orders
    
    def fetch_orders_for_period(self, date_from: datetime, date_to: datetime) -> List[Dict[str, Any]]:
        """
        Fetch orders for a specific date range (typically one week)

        Note: API filter requires partner token, so we fetch all orders and filter client-side
        """
        all_orders = []
        has_next_page = True
        cursor = None
        max_retries = 3
        retry_delay = 10
        page_delay = 0.5  # 500ms delay between pages
        consecutive_errors = 0

        logger.info(f"Fetching orders from API (will filter client-side for {date_from.strftime('%Y-%m-%d')} to {date_to.strftime('%Y-%m-%d')})")

        while has_next_page:
            # Remove filter parameter as it requires partner token
            variables = {
                'params': {
                    'limit': 30,
                    'order_by': 'pur_date',
                    'sort': 'ASC'
                }
            }

            if cursor is not None:
                variables['params']['cursor'] = cursor

            retry_count = 0
            success = False

            while retry_count < max_retries and not success:
                try:
                    result = self.client.execute(ORDER_QUERY, variable_values=variables)
                    orders_data = result.get('getOrderList', {})
                    orders = orders_data.get('data', [])
                    all_orders.extend(orders)

                    page_info = orders_data.get('pageInfo', {})
                    has_next_page = page_info.get('hasNextPage', False)
                    cursor = page_info.get('nextCursor')

                    print(f"Fetched {len(orders)} orders (total: {len(all_orders)})")
                    success = True
                    consecutive_errors = 0  # Reset error counter on success

                    # Delay between pages to avoid overwhelming the API
                    if has_next_page:
                        time.sleep(page_delay)

                except Exception as e:
                    retry_count += 1
                    consecutive_errors += 1

                    # Log the full error details
                    error_msg = str(e)
                    logger.debug(f"GraphQL error details: {error_msg}")

                    # Print full stack trace for debugging
                    if os.getenv('DEBUG'):
                        logger.debug("Full stack trace:")
                        logger.debug(traceback.format_exc())

                    if retry_count < max_retries:
                        logger.warning(f"Error fetching orders (attempt {retry_count}/{max_retries}): {error_msg[:200]}")
                        print(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    else:
                        logger.error(f"Error fetching orders after {max_retries} attempts: {error_msg[:200]}")
                        logger.error(f"Full error: {error_msg}")
                        logger.error(f"Stack trace:\n{traceback.format_exc()}")

                        # If we've had too many consecutive errors and have some data, return what we have
                        if consecutive_errors >= 3 and all_orders:
                            logger.info(f"Returning {len(all_orders)} orders fetched so far due to persistent errors")
                            has_next_page = False
                        else:
                            # Otherwise just break this pagination loop
                            has_next_page = False
                        break

        logger.info(f"Fetched {len(all_orders)} total orders from API")

        # Filter by date range (client-side since API filter requires partner token)
        date_filtered_orders = []
        for order in all_orders:
            pur_date_str = order.get('pur_date', '')
            if pur_date_str:
                try:
                    # Parse date (format: YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
                    pur_date = datetime.strptime(pur_date_str.split()[0], '%Y-%m-%d')
                    # Check if order is within date range
                    if date_from <= pur_date <= date_to:
                        date_filtered_orders.append(order)
                except (ValueError, IndexError) as e:
                    logger.warning(f"Could not parse date '{pur_date_str}' for order {order.get('order_num', 'unknown')}: {e}")
                    continue

        logger.info(f"Filtered to {len(date_filtered_orders)} orders within date range")

        # Filter out orders with excluded statuses
        excluded_statuses = [
            'Storno',
            'Platba online - platnosĹĄ vyprĹˇala',
            'Platba online - platba zamietnutĂˇ',
            'ÄŚakĂˇ na Ăşhradu',
            'GoPay - platebni metoda potvrzena'
        ]

        filtered_orders = []
        excluded_counts = {}

        for order in date_filtered_orders:
            status = order.get('status', {}) or {}
            status_name = status.get('name', '')

            if status_name not in excluded_statuses:
                filtered_orders.append(order)
            else:
                excluded_counts[status_name] = excluded_counts.get(status_name, 0) + 1

        # Report excluded orders
        if excluded_counts:
            print("\nFiltered out orders:")
            for status, count in excluded_counts.items():
                print(f"  - {status}: {count} orders")

        logger.info(f"Final count after status filtering: {len(filtered_orders)} orders")

        return filtered_orders
    
    def flatten_order(self, order: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Flatten order data for CSV export - one row per order item"""
        flattened_rows = []
        
        # Extract common order data
        customer = order.get('customer', {}) or {}
        invoice_addr = order.get('invoice_address', {}) or {}
        delivery_addr = order.get('delivery_address', {}) or {}
        status = order.get('status', {}) or {}
        order_sum = order.get('sum', {}) or {}
        
        # Get order currency
        order_currency = order_sum.get('currency', {}).get('code') if order_sum.get('currency') else 'EUR'
        
        # Convert order total to EUR
        order_total_original = order_sum.get('value', 0) or 0
        order_total_eur = self.convert_to_eur(order_total_original, order_currency)
        
        # Customer info
        customer_name = customer.get('company_name', '')
        if not customer_name:
            customer_name = f"{customer.get('name', '')} {customer.get('surname', '')}".strip()
        
        # Base order data
        base_data = {
            'order_id': order.get('id'),
            'order_num': order.get('order_num'),
            'external_ref': order.get('external_ref'),
            'purchase_date': order.get('pur_date'),
            'var_symbol': order.get('var_symb'),
            'last_change': order.get('last_change'),
            'oss': order.get('oss'),
            'oss_country': order.get('oss_country'),
            
            # Status
            'status_id': status.get('id'),
            'status_name': status.get('name'),
            
            # Customer
            'customer_name': customer_name,
            'customer_company_id': customer.get('company_id'),
            'customer_vat_id': customer.get('vat_id'),
            'customer_email': customer.get('email'),
            'customer_phone': customer.get('phone'),
            
            # Invoice address
            'invoice_street': invoice_addr.get('street'),
            'invoice_descriptive_num': invoice_addr.get('descriptive_number'),
            'invoice_orientation_num': invoice_addr.get('orientation_number'),
            'invoice_city': invoice_addr.get('city'),
            'invoice_zip': invoice_addr.get('zip'),
            'invoice_country': invoice_addr.get('country'),
            
            # Delivery address
            'delivery_street': delivery_addr.get('street') if delivery_addr else None,
            'delivery_descriptive_num': delivery_addr.get('descriptive_number') if delivery_addr else None,
            'delivery_orientation_num': delivery_addr.get('orientation_number') if delivery_addr else None,
            'delivery_city': delivery_addr.get('city') if delivery_addr else None,
            'delivery_zip': delivery_addr.get('zip') if delivery_addr else None,
            'delivery_country': delivery_addr.get('country') if delivery_addr else None,
            
            # Order total
            'order_total_original': order_total_original,
            'order_total': order_total_eur,  # Converted to EUR
            'order_total_formatted': order_sum.get('formatted'),
            'order_currency': order_currency,
            
            # Packaging cost (fixed per order)
            'packaging_cost': PACKAGING_COST_PER_ORDER,
        }
        
        # Create a row for each item
        items = order.get('items', [])
        
        if items:
            item_rows = []
            for item in items:
                item_price = item.get('price', {}) or {}
                item_sum = item.get('sum', {}) or {}
                item_sum_with_tax = item.get('sum_with_tax', {}) or {}
                weight = item.get('weight', {}) or {}
                recycle_fee = item.get('recycle_fee', {}) or {}
                
                # Get item currency (prefer explicit line totals, then unit price, then order currency).
                item_currency = (
                    (item_sum.get('currency', {}) or {}).get('code')
                    or (item_sum_with_tax.get('currency', {}) or {}).get('code')
                    or (item_price.get('currency', {}) or {}).get('code')
                    or order_currency
                )

                item_price_value_original = item_price.get('value', 0) or 0
                item_price_value = self.convert_to_eur(item_price_value_original, item_currency)
                item_quantity = item.get('quantity', 1) or 1
                item_tax_rate = item.get('tax_rate', 0) or 0
                tax_multiplier = (1 + item_tax_rate / 100) if item_tax_rate > 0 else 1.0
                item_line_net_original = item_sum.get('value')
                item_line_gross_original = item_sum_with_tax.get('value')

                # BizniWeb returns reliable line totals in `sum` (net) and `sum_with_tax` (gross).
                # The `is_net_price` flags on these payloads are not reliable enough to drive business
                # logic, so prefer the explicit total fields and use VAT math only as fallback.
                if item_line_net_original is not None:
                    item_total_without_tax = self.convert_to_eur(item_line_net_original, item_currency)
                    if item_line_gross_original is not None:
                        item_total_with_tax = self.convert_to_eur(item_line_gross_original, item_currency)
                    else:
                        item_total_with_tax = item_total_without_tax * tax_multiplier
                elif item_line_gross_original is not None:
                    item_total_with_tax = self.convert_to_eur(item_line_gross_original, item_currency)
                    if item_tax_rate > 0:
                        item_total_without_tax = item_total_with_tax / tax_multiplier
                    else:
                        item_total_without_tax = item_total_with_tax
                else:
                    # Final fallback when line sums are missing: BizniWeb unit prices behave as net in
                    # the live data for both Roy and Vevo, even when `is_net_price` is inverted.
                    item_total_without_tax = item_price_value * item_quantity
                    item_total_with_tax = item_total_without_tax * tax_multiplier
                item_tax_amount = item_total_with_tax - item_total_without_tax
                
                # Get expense per item from mapping (using product_sku - EAN or hash)
                item_label = item.get('item_label', '')
                item_ean = item.get('ean', '')
                product_sku = self.get_product_sku(item_ean, item_label)

                # Optional exclusion for zero-priced gift lines (e.g. free promo gifts).
                if (
                    EXCLUDE_ZERO_PRICE_LABEL_PATTERNS
                    and self._matches_patterns(item_label, EXCLUDE_ZERO_PRICE_LABEL_PATTERNS)
                    and round(item_total_with_tax, 2) == 0
                ):
                    continue

                # Force 0 cost for configured brands, then optional 0 margin brands, otherwise use configured costs.
                force_zero_cost = False
                force_zero_margin = False
                force_margin_15 = False
                if (ZERO_COST_BRANDS or ZERO_MARGIN_BRANDS or MARGIN_15_BRANDS) and item_label:
                    label_lc = str(item_label).lower()
                    force_zero_cost = any(brand in label_lc for brand in ZERO_COST_BRANDS)
                    force_zero_margin = any(brand in label_lc for brand in ZERO_MARGIN_BRANDS)
                    force_margin_15 = any(brand in label_lc for brand in MARGIN_15_BRANDS)
                if item_label and not force_zero_cost:
                    force_zero_cost = self._matches_patterns(item_label, ZERO_COST_LABEL_PATTERNS)
                if item_label and not force_margin_15:
                    force_margin_15 = self._matches_patterns(item_label, MARGIN_15_LABEL_PATTERNS)
                if force_zero_cost:
                    expense_per_item = 0.0
                elif force_zero_margin and item_quantity:
                    expense_per_item = item_total_without_tax / item_quantity
                elif force_margin_15 and item_quantity:
                    # Keep product margin at 15%: cost = 85% of net unit selling price.
                    expense_per_item = (item_total_without_tax / item_quantity) * 0.85
                else:
                    # First try SKU, then title for backward compatibility, default to 1.0 for unknown products.
                    expense_per_item = PRODUCT_EXPENSES.get(product_sku, PRODUCT_EXPENSES.get(item_label, 1.0))
                total_expense = expense_per_item * item_quantity
                
                # Calculate profit and ROI (Note: FB ads will be added at aggregation level)
                # At item level, we only have product expense
                item_profit_before_ads = item_total_without_tax - total_expense
                item_roi_before_ads = (item_profit_before_ads / total_expense * 100) if total_expense > 0 else 0
                
                row = base_data.copy()
                row.update({
                    'total_items_in_order': None,
                    'item_number': None,
                    'item_label': item.get('item_label'),
                    'item_ean': item.get('ean'),
                    'item_import_code': item.get('import_code'),
                    'item_warehouse_number': item.get('warehouse_number'),
                    'item_quantity': item_quantity,
                    'item_tax_rate': item_tax_rate,
                    'item_weight': weight.get('value'),
                    'item_weight_unit': weight.get('unit'),
                    'item_currency': item_currency,
                    'item_unit_price_original': item_price_value_original,
                    'item_unit_price': item_price_value,  # In EUR
                    'item_line_sum_original': item_line_net_original,
                    'item_line_sum_with_tax_original': item_line_gross_original,
                    'item_total_with_tax': round(item_total_with_tax, 2),  # In EUR
                    'item_total_without_tax': round(item_total_without_tax, 2),  # In EUR
                    'item_tax_amount': round(item_tax_amount, 2),  # In EUR
                    'item_recycle_fee': recycle_fee.get('value'),
                    'expense_per_item': expense_per_item,
                    'total_expense': round(total_expense, 2),
                    'profit_before_ads': round(item_profit_before_ads, 2),
                    'roi_before_ads': round(item_roi_before_ads, 2),
                })
                item_rows.append(row)

            # If all order rows were excluded (e.g. zero-price gifts only), skip this order in export.
            if not item_rows:
                return flattened_rows

            total_items = len(item_rows)
            for idx, row in enumerate(item_rows, 1):
                row['total_items_in_order'] = total_items
                row['item_number'] = idx
                flattened_rows.append(row)
        else:
            # If no items, create one row with order data only
            base_data['total_items_in_order'] = 0
            base_data['item_number'] = None
            flattened_rows.append(base_data)
        
        return flattened_rows
    
    def cleanup_data_folder(self):
        """Clean up old data files before starting new export"""
        data_dir = self.data_dir
        if data_dir.exists():
            # Remove only files that belong to the active output variant.
            for pattern in ['*.csv', '*.html', '*.json']:
                for file in data_dir.glob(pattern):
                    if not self._belongs_to_active_output_variant(file):
                        continue
                    try:
                        file.unlink()
                        print(f"Removed old file: {file.name}")
                    except Exception as e:
                        print(f"Warning: Could not remove {file.name}: {e}")
        else:
            # Create data directory if it doesn't exist
            data_dir.mkdir(exist_ok=True)
    
    def export_to_csv(
        self,
        orders: List[Dict[str, Any]],
        date_from: datetime,
        date_to: datetime,
        period_switcher: Dict[str, Any] = None,
    ) -> str:
        """Export orders to CSV file"""
        # Clean up old data files first
        print("Cleaning up old data files...")
        self.cleanup_data_folder()

        # Safety dedup for long historical runs / cursor overlap edge cases.
        orders = self.deduplicate_orders(orders)
        if period_switcher is None:
            period_switcher = self._build_period_switcher_bundle(orders, date_from, date_to)
        embedded_period_reports = self._build_embedded_period_reports(period_switcher)

        source_health: Dict[str, Any] = {
            "project": self.project_name,
            "generated_at_utc": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            "date_range": {
                "from": date_from.strftime("%Y-%m-%d"),
                "to": date_to.strftime("%Y-%m-%d"),
            },
            "sources": {
                "biznisweb_orders": self._build_source_entry(
                    key="biznisweb_orders",
                    label="BizniWeb Orders",
                    status="ok",
                    mode="api",
                    message=f"Fetched and deduplicated {len(orders)} orders from BizniWeb GraphQL.",
                    healthy=True,
                    orders=len(orders),
                ),
            },
        }
        
        # Fetch Facebook Ads spend data
        fb_daily_spend = {}
        fb_detailed_metrics = {}
        fb_campaigns = []
        fb_hourly_stats = []
        fb_dow_stats = []
        if PREFER_MANUAL_ADS_TOTALS and MANUAL_FB_ADS_TOTAL is not None:
            fb_daily_spend = self._distribute_total_spend(MANUAL_FB_ADS_TOTAL, date_from, date_to)
            print(
                f"Using manual Facebook Ads total: {MANUAL_FB_ADS_TOTAL:.2f} EUR "
                f"distributed across {len(fb_daily_spend)} days"
            )
            source_health["sources"]["facebook_ads"] = self._build_source_entry(
                key="facebook_ads",
                label="Facebook Ads",
                status="manual",
                mode="manual_total_distribution",
                message=f"Manual Facebook Ads total {MANUAL_FB_ADS_TOTAL:.2f} EUR distributed across the selected date range.",
                healthy=True,
                active_days=len(fb_daily_spend),
                total_eur=round(float(MANUAL_FB_ADS_TOTAL), 2),
            )
        elif self.fb_client.is_configured:
            print("Testing Facebook Ads connection...")
            if not self.fb_client.test_connection():
                logger.warning("Facebook Ads connection test failed; report will continue with zero-filled FB metrics.")
                source_health["sources"]["facebook_ads"] = self._build_source_entry(
                    key="facebook_ads",
                    label="Facebook Ads",
                    status="error",
                    mode="api",
                    message="Configured, but API connection test failed. FB-based metrics in this run may be incomplete or zero-filled.",
                    healthy=False,
                )
            else:
                print("Fetching Facebook Ads spend data...")
                fb_daily_spend = self.fb_client.get_daily_spend(date_from, date_to)
                if fb_daily_spend:
                    print(f"Retrieved Facebook Ads data for {len(fb_daily_spend)} days")

                # Fetch detailed metrics for Facebook Ads report
                print("Fetching detailed Facebook Ads metrics...")
                fb_detailed_metrics = self.fb_client.get_daily_metrics(date_from, date_to)
                if fb_detailed_metrics:
                    print(f"Retrieved detailed FB metrics for {len(fb_detailed_metrics)} days")

                # Fetch campaign-level performance
                print("Fetching Facebook campaign performance...")
                fb_campaigns = self.fb_client.get_campaign_spend(date_from, date_to)
                if fb_campaigns:
                    print(f"Retrieved data for {len(fb_campaigns)} campaigns")

                # Fetch hourly stats
                print("Fetching Facebook hourly stats...")
                fb_hourly_stats = self.fb_client.get_hourly_stats(date_from, date_to)
                if fb_hourly_stats:
                    print(f"Retrieved hourly stats for {len(fb_hourly_stats)} hours")

                # Fetch day of week stats
                print("Fetching Facebook day-of-week stats...")
                fb_dow_stats = self.fb_client.get_day_of_week_stats(date_from, date_to)
                if fb_dow_stats:
                    print(f"Retrieved day-of-week stats for {len(fb_dow_stats)} days")

                source_health["sources"]["facebook_ads"] = self._build_source_entry(
                    key="facebook_ads",
                    label="Facebook Ads",
                    status="ok",
                    mode="api",
                    message=f"Facebook Ads API connected successfully. Daily spend loaded for {len(fb_daily_spend)} active days.",
                    healthy=True,
                    active_days=len(fb_daily_spend),
                    detailed_days=len(fb_detailed_metrics),
                    campaign_count=len(fb_campaigns),
                    hourly_rows=len(fb_hourly_stats),
                )
        elif MANUAL_FB_ADS_TOTAL is not None:
            source_health["sources"]["facebook_ads"] = self._build_source_entry(
                key="facebook_ads",
                label="Facebook Ads",
                status="disabled",
                mode="manual_total_available_but_disabled",
                message=(
                    f"Manual Facebook Ads total {MANUAL_FB_ADS_TOTAL:.2f} EUR is configured, "
                    "but manual ads mode is disabled. No FB spend loaded."
                ),
                healthy=True,
            )
        else:
            source_health["sources"]["facebook_ads"] = self._build_source_entry(
                key="facebook_ads",
                label="Facebook Ads",
                status="disabled",
                mode="not_configured",
                message="Facebook Ads integration is not configured for this project/runtime.",
                healthy=True,
            )
        
        # Fetch Google Ads spend data
        google_ads_daily_spend = {}
        if PREFER_MANUAL_ADS_TOTALS and MANUAL_GOOGLE_ADS_TOTAL is not None:
            google_ads_daily_spend = self._distribute_total_spend(MANUAL_GOOGLE_ADS_TOTAL, date_from, date_to)
            print(
                f"Using manual Google Ads total: {MANUAL_GOOGLE_ADS_TOTAL:.2f} EUR "
                f"distributed across {len(google_ads_daily_spend)} days"
            )
            source_health["sources"]["google_ads"] = self._build_source_entry(
                key="google_ads",
                label="Google Ads",
                status="manual",
                mode="manual_total_distribution",
                message=f"Manual Google Ads total {MANUAL_GOOGLE_ADS_TOTAL:.2f} EUR distributed across the selected date range.",
                healthy=True,
                active_days=len(google_ads_daily_spend),
                total_eur=round(float(MANUAL_GOOGLE_ADS_TOTAL), 2),
            )
        elif self.google_ads_client.is_configured:
            print("Testing Google Ads connection...")
            if not self.google_ads_client.test_connection():
                logger.warning("Google Ads connection test failed; report will continue with zero-filled Google metrics.")
                source_health["sources"]["google_ads"] = self._build_source_entry(
                    key="google_ads",
                    label="Google Ads",
                    status="error",
                    mode="api",
                    message="Configured, but API connection test failed. Google Ads metrics in this run may be incomplete or zero-filled.",
                    healthy=False,
                )
            else:
                print("Fetching Google Ads spend data...")
                google_ads_daily_spend = self.google_ads_client.get_daily_spend(date_from, date_to)
                if google_ads_daily_spend:
                    print(f"Retrieved Google Ads data for {len(google_ads_daily_spend)} days")
                source_health["sources"]["google_ads"] = self._build_source_entry(
                    key="google_ads",
                    label="Google Ads",
                    status="ok",
                    mode="api",
                    message=f"Google Ads API connected successfully. Daily spend loaded for {len(google_ads_daily_spend)} active days.",
                    healthy=True,
                    active_days=len(google_ads_daily_spend),
                )
        elif MANUAL_GOOGLE_ADS_TOTAL is not None:
            source_health["sources"]["google_ads"] = self._build_source_entry(
                key="google_ads",
                label="Google Ads",
                status="disabled",
                mode="manual_total_available_but_disabled",
                message=(
                    f"Manual Google Ads total {MANUAL_GOOGLE_ADS_TOTAL:.2f} EUR is configured, "
                    "but manual ads mode is disabled. No Google spend loaded."
                ),
                healthy=True,
            )
        else:
            source_health["sources"]["google_ads"] = self._build_source_entry(
                key="google_ads",
                label="Google Ads",
                status="disabled",
                mode="not_configured",
                message="Google Ads integration is not configured for this project/runtime.",
                healthy=True,
            )
        
        # Flatten all orders
        all_rows = []
        for order in orders:
            all_rows.extend(self.flatten_order(order))
        
        # Create filename
        filename = self.output_path(f"export_{date_from.strftime('%Y%m%d')}-{date_to.strftime('%Y%m%d')}.csv")
        
        # Convert to DataFrame for easier CSV export
        df = pd.DataFrame(all_rows)

        # Safety dedup on flattened rows to avoid duplicate item rows in revenue/cost analytics.
        # Uses order_num + item_* shape as requested.
        dedup_cols = [
            'order_num',
            'item_number',
            'item_label',
            'item_ean',
            'item_quantity',
            'item_tax_rate',
            'item_total_without_tax',
            'item_total_with_tax'
        ]
        dedup_cols = [col for col in dedup_cols if col in df.columns]
        if dedup_cols:
            before_rows = len(df)
            df = df.drop_duplicates(subset=dedup_cols).reset_index(drop=True)
            removed_rows = before_rows - len(df)
            if removed_rows > 0:
                logger.warning(f"Removed {removed_rows} duplicated flattened rows before analytics")
                print(f"Deduplication: removed {removed_rows} duplicated item rows")

        # Add consistent product SKU column (EAN if available, otherwise hash of title)
        df = self.add_product_sku_column(df)
        # Add canonical order-level net revenue (unified revenue definition for report analytics)
        df = self.add_order_revenue_net_column(df)

        # Add Facebook Ads spend column
        if fb_daily_spend:
            # Convert purchase_date to date format for matching
            df['purchase_date_only'] = pd.to_datetime(df['purchase_date']).dt.strftime('%Y-%m-%d')
            df['fb_ads_daily_spend'] = df['purchase_date_only'].map(fb_daily_spend).fillna(0)
        else:
            df['fb_ads_daily_spend'] = 0
        
        # Add Google Ads spend column
        if google_ads_daily_spend:
            # Ensure purchase_date_only exists
            if 'purchase_date_only' not in df.columns:
                df['purchase_date_only'] = pd.to_datetime(df['purchase_date']).dt.strftime('%Y-%m-%d')
            df['google_ads_daily_spend'] = df['purchase_date_only'].map(google_ads_daily_spend).fillna(0)
        else:
            df['google_ads_daily_spend'] = 0
        
        # Reorder columns for better readability
        column_order = [
            'order_num', 'order_id', 'external_ref', 'purchase_date', 'status_name',
            'total_items_in_order', 'item_number',
            'product_sku', 'item_label', 'item_ean', 'item_quantity', 
            'item_currency', 'item_unit_price_original', 'item_unit_price',
            'item_total_without_tax', 'item_tax_rate', 'item_tax_amount', 'item_total_with_tax',
            'expense_per_item', 'total_expense', 'fb_ads_daily_spend', 'google_ads_daily_spend', 'profit_before_ads', 'roi_before_ads',
            'customer_name', 'customer_email', 'customer_company_id', 'customer_vat_id',
            'order_currency', 'order_total_original', 'order_total', 'order_revenue_net',
            'invoice_street', 'invoice_city', 'invoice_zip', 'invoice_country',
            'delivery_street', 'delivery_city', 'delivery_zip', 'delivery_country'
        ]
        
        # Only include columns that exist
        column_order = [col for col in column_order if col in df.columns]
        # Add any remaining columns
        remaining_cols = [col for col in df.columns if col not in column_order]
        column_order.extend(remaining_cols)
        
        df = df[column_order]
        
        # Save to CSV
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        
        # Analyze returning customers
        returning_customers_analysis = self.analyze_returning_customers(df)
        
        # Calculate CLV and return time analysis
        clv_return_time_analysis = self.calculate_clv_and_return_time(df)

        # Analyze order size distribution
        order_size_distribution = self.analyze_order_size_distribution(df)

        # Analyze item combinations
        item_combinations = self.analyze_item_combinations(df, min_count=5)

        # New analytics
        day_of_week_analysis = self.analyze_day_of_week(df)
        week_of_month_analysis = self.analyze_week_of_month(df)
        day_of_month_analysis = self.analyze_day_of_month(df)
        advanced_dtc_metrics = self.analyze_advanced_dtc_metrics(df)
        day_hour_heatmap = self.analyze_day_hour_heatmap(df)
        country_analysis, city_analysis = self.analyze_geographic(df)
        geo_profitability = self.analyze_geo_profitability(df, fb_campaigns)
        b2b_analysis = self.analyze_b2b_vs_b2c(df)
        product_margins = self.analyze_product_margins(df)
        product_trends = self.analyze_product_trends(df)
        customer_concentration = self.analyze_customer_concentration(df)
        order_status = self.analyze_order_status(df)
        ads_effectiveness = self.analyze_ads_effectiveness(df)
        new_vs_returning_revenue = self.analyze_new_vs_returning_revenue(df)
        refunds_analysis = self.analyze_refunds(df)

        # Repeat purchase cohort analysis
        cohort_analysis = self.analyze_repeat_purchase_cohorts(df)

        # Item-based retention analyses
        first_item_retention = self.analyze_retention_by_first_order_item(df)
        same_item_repurchase = self.analyze_same_item_repurchase(df)
        time_to_nth_by_first_item = self.analyze_time_to_nth_by_first_item(df)
        sample_funnel_analysis = self.analyze_sample_funnel(df)

        # Customer email segmentation analysis
        # Combine filtered orders with excluded (failed payment) orders for complete customer view
        all_orders_for_segmentation = orders + self.excluded_orders
        customer_email_segments = self.analyze_customer_email_segments(df, all_orders_for_segmentation)

        # Create aggregated reports
        date_product_agg, date_agg, items_agg, month_agg, ltv_by_date = self.create_aggregated_reports(df, date_from, date_to, fb_daily_spend, google_ads_daily_spend)
        weather_analysis = self.analyze_weather_impact(date_agg, date_from, date_to)
        if self.weather_settings.get("enabled") and self.weather_settings.get("locations"):
            if weather_analysis:
                weather_days = len(weather_analysis.get("daily", [])) if weather_analysis.get("daily") is not None else 0
                source_health["sources"]["weather"] = self._build_source_entry(
                    key="weather",
                    label="Weather",
                    status="ok",
                    mode="api",
                    message=f"Weather enrichment loaded successfully for {weather_days} daily rows.",
                    healthy=True,
                    active_days=weather_days,
                    provider=weather_analysis.get("source", "Open-Meteo Historical Weather API"),
                    location=weather_analysis.get("location_label", ""),
                )
            else:
                source_health["sources"]["weather"] = self._build_source_entry(
                    key="weather",
                    label="Weather",
                    status="error",
                    mode="api",
                    message="Weather enrichment was enabled, but no usable weather dataset was produced for this run.",
                    healthy=False,
                )
        else:
            source_health["sources"]["weather"] = self._build_source_entry(
                key="weather",
                label="Weather",
                status="disabled",
                mode="not_configured",
                message="Weather enrichment is not configured for this project/runtime.",
                healthy=True,
            )

        # Calculate financial metrics
        financial_metrics = self.calculate_financial_metrics(df, date_agg, clv_return_time_analysis)
        consistency_checks = self.validate_metric_consistency(date_agg, financial_metrics, clv_return_time_analysis)
        cfo_kpi_payload = build_cfo_kpi_payload(
            date_agg=date_agg,
            export_df=df,
            fixed_daily_cost_eur=float(os.getenv("CFO_FIXED_DAILY_COST_EUR", "70")),
        )

        # Cost Per Order analysis with campaign attribution
        # Use the same revenue source as financial summary to keep ROAS definitions aligned.
        cost_per_order = self.analyze_cost_per_order(
            df,
            fb_campaigns,
            reference_total_revenue=financial_metrics.get('total_revenue')
        )
        source_health.setdefault("qa", {})["attribution"] = self._build_attribution_qa(
            cost_per_order=cost_per_order,
            fb_campaigns=fb_campaigns,
            total_orders=int(df["order_num"].nunique()) if "order_num" in df.columns else len(orders),
        )
        source_health.setdefault("qa", {})["geo"] = self._build_geo_qa(
            country_analysis=country_analysis,
            geo_profitability=geo_profitability,
        )
        source_health.setdefault("qa", {})["data_assertions"] = self._build_data_assertions_qa(
            financial_metrics=financial_metrics,
            consistency_checks=consistency_checks,
            refunds_analysis=refunds_analysis,
            day_of_week_analysis=day_of_week_analysis,
            advanced_dtc_metrics=advanced_dtc_metrics,
            country_analysis=country_analysis,
            geo_profitability=geo_profitability,
            cost_per_order=cost_per_order,
        )
        source_health.setdefault("qa", {})["margin_stability"] = self._build_margin_stability_qa(
            date_agg=date_agg,
        )
        source_health = self._finalize_source_health(source_health)

        # Display aggregated data
        self.display_aggregated_data(date_product_agg, date_agg, month_agg)

        # Display returning customer analysis
        self.display_returning_customers_analysis(returning_customers_analysis)

        # Display CLV and return time analysis
        self.display_clv_return_time_analysis(clv_return_time_analysis)

        # Generate HTML report
        print("Generating HTML report...")
        html_content = generate_html_report(
            date_agg, date_product_agg, items_agg,
            date_from, date_to, self.reporting_defaults["reporting_system_name"], fb_daily_spend, google_ads_daily_spend,
            returning_customers_analysis, clv_return_time_analysis,
            order_size_distribution, item_combinations,
            day_of_week_analysis=day_of_week_analysis,
            week_of_month_analysis=week_of_month_analysis,
            day_of_month_analysis=day_of_month_analysis,
            weather_analysis=weather_analysis,
            advanced_dtc_metrics=advanced_dtc_metrics,
            day_hour_heatmap=day_hour_heatmap,
            country_analysis=country_analysis,
            city_analysis=city_analysis,
            geo_profitability=geo_profitability,
            b2b_analysis=b2b_analysis,
            product_margins=product_margins,
            product_trends=product_trends,
            customer_concentration=customer_concentration,
            financial_metrics=financial_metrics,
            order_status=order_status,
            ads_effectiveness=ads_effectiveness,
            new_vs_returning_revenue=new_vs_returning_revenue,
            refunds_analysis=refunds_analysis,
            customer_email_segments=customer_email_segments,
            cohort_analysis=cohort_analysis,
            first_item_retention=first_item_retention,
            same_item_repurchase=same_item_repurchase,
            time_to_nth_by_first_item=time_to_nth_by_first_item,
            sample_funnel_analysis=sample_funnel_analysis,
            fb_detailed_metrics=fb_detailed_metrics,
            fb_campaigns=fb_campaigns,
            cost_per_order=cost_per_order,
            fb_hourly_stats=fb_hourly_stats,
            fb_dow_stats=fb_dow_stats,
            ltv_by_date=ltv_by_date,
            consistency_checks=consistency_checks,
            cfo_kpi_payload=cfo_kpi_payload,
            source_health=source_health,
            period_switcher=period_switcher,
            embedded_period_reports=embedded_period_reports,
            dashboard_variant='default',
        )
        html_filename = self.output_path(f"report_{date_from.strftime('%Y%m%d')}-{date_to.strftime('%Y%m%d')}.html")
        # Write with UTF-8 BOM to avoid mojibake when a server/browser mis-detects charset
        with open(html_filename, 'w', encoding='utf-8-sig') as f:
            f.write(html_content)
        print(f"HTML report saved: {html_filename}")
        self._write_data_quality_file(source_health, date_from, date_to)

        # Generate Email Strategy Report
        if ENABLE_EMAIL_STRATEGY_REPORT and customer_email_segments and cohort_analysis:
            print("Generating Email Strategy Report...")
            email_strategy_html = generate_email_strategy_report(
                customer_email_segments, cohort_analysis, date_from, date_to,
                report_title=self.reporting_defaults["reporting_system_name"],
            )
            email_strategy_filename = self.output_path(f"email_strategy_{date_from.strftime('%Y%m%d')}-{date_to.strftime('%Y%m%d')}.html")
            # Keep the same robust encoding strategy for secondary HTML report
            with open(email_strategy_filename, 'w', encoding='utf-8-sig') as f:
                f.write(email_strategy_html)
            print(f"Email Strategy Report saved: {email_strategy_filename}")

        return str(filename)
    
    def create_aggregated_reports(self, df: pd.DataFrame, date_from: datetime, date_to: datetime, fb_daily_spend: Dict[str, float] = None, google_ads_daily_spend: Dict[str, float] = None):
        """Create aggregated CSV reports"""
        # Convert purchase_date to datetime and extract date only
        if 'purchase_date_only' not in df.columns:
            df['purchase_date_only'] = pd.to_datetime(df['purchase_date']).dt.date
        else:
            df['purchase_date_only'] = pd.to_datetime(df['purchase_date_only']).dt.date
        
        # 1. Group by date and product (using product_sku for consistent grouping)
        print("Creating date-product aggregation...")
        date_product_agg = df.groupby(['purchase_date_only', 'product_sku']).agg({
            'item_label': 'first',  # Keep product name for display
            'item_quantity': 'sum',
            'item_total_without_tax': 'sum',
            'total_expense': 'sum',
            'profit_before_ads': 'sum',
            'order_num': 'count'
        }).reset_index()

        date_product_agg.columns = ['date', 'product_sku', 'product_name', 'total_quantity', 'total_revenue', 'product_expense', 'profit', 'order_count']
        
        # Calculate ROI based on product expense only (no FB ads)
        date_product_agg['roi_percent'] = date_product_agg.apply(
            lambda row: round((row['profit'] / row['product_expense'] * 100) if row['product_expense'] > 0 else 0, 2),
            axis=1
        )
        
        # Round financial values
        date_product_agg['total_revenue'] = date_product_agg['total_revenue'].round(2)
        date_product_agg['product_expense'] = date_product_agg['product_expense'].round(2)
        date_product_agg['profit'] = date_product_agg['profit'].round(2)
        
        # Sort by date and product SKU
        date_product_agg = date_product_agg.sort_values(['date', 'product_sku'])
        
        # Save date-product aggregation
        date_product_filename = self.output_path(f"aggregate_by_date_product_{date_from.strftime('%Y%m%d')}-{date_to.strftime('%Y%m%d')}.csv")
        date_product_agg.to_csv(date_product_filename, index=False, encoding='utf-8-sig')
        print(f"Date-product aggregation saved: {date_product_filename}")
        
        # 2. Group by date only
        print("Creating date-only aggregation...")
        date_agg = df.groupby('purchase_date_only').agg({
            'item_quantity': 'sum',
            'item_total_without_tax': 'sum',
            'total_expense': 'sum',
            'profit_before_ads': 'sum',
            'fb_ads_daily_spend': 'first' if 'fb_ads_daily_spend' in df.columns else lambda x: 0,
            'google_ads_daily_spend': 'first' if 'google_ads_daily_spend' in df.columns else lambda x: 0,
            'order_num': 'nunique',  # Count unique orders
            'item_label': 'count'     # Count total items
        }).reset_index()
        
        date_agg.columns = ['date', 'total_quantity', 'total_revenue', 'product_expense', 'profit_before_ads', 'fb_ads_spend', 'google_ads_spend', 'unique_orders', 'total_items']

        # Fill in missing dates with zero values for orders but preserve ad spend data
        # Create a complete date range
        complete_date_range = pd.date_range(start=date_from, end=date_to, freq='D').date
        complete_date_df = pd.DataFrame({'date': complete_date_range})

        # Merge with existing data to include all dates
        date_agg = complete_date_df.merge(date_agg, on='date', how='left')

        # Fill missing order-related values with 0
        date_agg['total_quantity'] = date_agg['total_quantity'].fillna(0).astype(int)
        date_agg['total_revenue'] = date_agg['total_revenue'].fillna(0)
        date_agg['product_expense'] = date_agg['product_expense'].fillna(0)
        date_agg['profit_before_ads'] = date_agg['profit_before_ads'].fillna(0)
        date_agg['unique_orders'] = date_agg['unique_orders'].fillna(0).astype(int)
        date_agg['total_items'] = date_agg['total_items'].fillna(0).astype(int)

        # Fill ad spend from dictionaries for dates with no orders
        # First fill NaN values with values from dictionaries, then fill any remaining with 0
        if fb_daily_spend:
            # Create a mapping function for date to string
            date_agg['date_str'] = date_agg['date'].apply(lambda d: d.strftime('%Y-%m-%d'))
            # Fill NaN fb_ads_spend values with values from dictionary
            date_agg['fb_ads_spend'] = date_agg.apply(
                lambda row: fb_daily_spend.get(row['date_str'], 0) if pd.isna(row['fb_ads_spend']) else row['fb_ads_spend'],
                axis=1
            )
            date_agg = date_agg.drop('date_str', axis=1)
        date_agg['fb_ads_spend'] = date_agg['fb_ads_spend'].fillna(0)

        if google_ads_daily_spend:
            # Create a mapping function for date to string
            date_agg['date_str'] = date_agg['date'].apply(lambda d: d.strftime('%Y-%m-%d'))
            # Fill NaN google_ads_spend values with values from dictionary
            date_agg['google_ads_spend'] = date_agg.apply(
                lambda row: google_ads_daily_spend.get(row['date_str'], 0) if pd.isna(row['google_ads_spend']) else row['google_ads_spend'],
                axis=1
            )
            date_agg = date_agg.drop('date_str', axis=1)
        date_agg['google_ads_spend'] = date_agg['google_ads_spend'].fillna(0)

        # Add variable per-order logistics costs
        # Packaging and net shipping both scale with number of orders.
        date_agg['packaging_cost'] = date_agg['unique_orders'] * PACKAGING_COST_PER_ORDER
        date_agg['shipping_net_cost'] = date_agg['unique_orders'] * SHIPPING_NET_PER_ORDER
        date_agg['shipping_subsidy_cost'] = date_agg['shipping_net_cost']  # backward-compatible alias

        # Add daily fixed cost based on the date
        date_agg['fixed_daily_cost'] = date_agg['date'].apply(lambda d: round(self.get_daily_fixed_cost(pd.Timestamp(d)), 2))

        # Company-level cost (includes fixed overhead)
        # Total cost = product expense + ads + packaging + net shipping + fixed daily cost
        date_agg['total_cost'] = (
            date_agg['product_expense']
            + date_agg['fb_ads_spend']
            + date_agg['google_ads_spend']
            + date_agg['packaging_cost']
            + date_agg['shipping_net_cost']
            + date_agg['fixed_daily_cost']
        )

        # Company net profit: Revenue - All costs (including fixed overhead)
        date_agg['net_profit'] = date_agg['total_revenue'] - date_agg['total_cost']

        # Pre-ad contribution view (CM1): excludes fixed overhead and ad spend
        date_agg['pre_ad_contribution_cost'] = (
            date_agg['product_expense']
            + date_agg['packaging_cost']
            + date_agg['shipping_net_cost']
        )
        date_agg['pre_ad_contribution_profit'] = date_agg['total_revenue'] - date_agg['pre_ad_contribution_cost']
        date_agg['pre_ad_contribution_margin_pct'] = date_agg.apply(
            lambda row: round((row['pre_ad_contribution_profit'] / row['total_revenue'] * 100) if row['total_revenue'] > 0 else 0, 2),
            axis=1
        )
        date_agg['pre_ad_contribution_profit_per_order'] = date_agg.apply(
            lambda row: round((row['pre_ad_contribution_profit'] / row['unique_orders']) if row['unique_orders'] > 0 else 0, 2),
            axis=1
        )
        date_agg['cm1_profit'] = date_agg['pre_ad_contribution_profit']
        date_agg['cm1_margin_pct'] = date_agg['pre_ad_contribution_margin_pct']
        date_agg['cm1_profit_per_order'] = date_agg['pre_ad_contribution_profit_per_order']

        # Post-ad contribution view (CM2): excludes fixed overhead, includes ad spend
        date_agg['contribution_cost'] = (
            date_agg['product_expense']
            + date_agg['packaging_cost']
            + date_agg['shipping_net_cost']
            + date_agg['fb_ads_spend']
            + date_agg['google_ads_spend']
        )
        date_agg['contribution_profit'] = date_agg['total_revenue'] - date_agg['contribution_cost']
        date_agg['contribution_margin_pct'] = date_agg.apply(
            lambda row: round((row['contribution_profit'] / row['total_revenue'] * 100) if row['total_revenue'] > 0 else 0, 2),
            axis=1
        )
        date_agg['contribution_profit_per_order'] = date_agg.apply(
            lambda row: round((row['contribution_profit'] / row['unique_orders']) if row['unique_orders'] > 0 else 0, 2),
            axis=1
        )
        # Explicit post-ad aliases (terminology clarity)
        date_agg['post_ad_contribution_cost'] = date_agg['contribution_cost']
        date_agg['post_ad_contribution_profit'] = date_agg['contribution_profit']
        date_agg['post_ad_contribution_margin_pct'] = date_agg['contribution_margin_pct']
        date_agg['post_ad_contribution_profit_per_order'] = date_agg['contribution_profit_per_order']
        date_agg['cm2_profit'] = date_agg['post_ad_contribution_profit']
        date_agg['cm2_margin_pct'] = date_agg['post_ad_contribution_margin_pct']
        date_agg['cm2_profit_per_order'] = date_agg['post_ad_contribution_profit_per_order']

        # Calculate ROI: (Profit / Total Cost) * 100
        date_agg['roi_percent'] = date_agg.apply(
            lambda row: round((row['net_profit'] / row['total_cost'] * 100) if row['total_cost'] > 0 else 0, 2),
            axis=1
        )
        date_agg['cm3_profit'] = date_agg['net_profit']
        date_agg['cm3_margin_pct'] = date_agg.apply(
            lambda row: round((row['net_profit'] / row['total_revenue'] * 100) if row['total_revenue'] > 0 else 0, 2),
            axis=1
        )
        date_agg['cm3_profit_per_order'] = date_agg.apply(
            lambda row: round((row['net_profit'] / row['unique_orders']) if row['unique_orders'] > 0 else 0, 2),
            axis=1
        )

        # Round financial values
        date_agg['total_revenue'] = date_agg['total_revenue'].round(2)
        date_agg['product_expense'] = date_agg['product_expense'].round(2)
        date_agg['fb_ads_spend'] = date_agg['fb_ads_spend'].round(2)
        date_agg['google_ads_spend'] = date_agg['google_ads_spend'].round(2)
        date_agg['packaging_cost'] = date_agg['packaging_cost'].round(2)
        date_agg['shipping_subsidy_cost'] = date_agg['shipping_subsidy_cost'].round(2)
        date_agg['total_cost'] = date_agg['total_cost'].round(2)
        date_agg['net_profit'] = date_agg['net_profit'].round(2)
        date_agg['pre_ad_contribution_cost'] = date_agg['pre_ad_contribution_cost'].round(2)
        date_agg['pre_ad_contribution_profit'] = date_agg['pre_ad_contribution_profit'].round(2)
        date_agg['pre_ad_contribution_margin_pct'] = date_agg['pre_ad_contribution_margin_pct'].round(2)
        date_agg['pre_ad_contribution_profit_per_order'] = date_agg['pre_ad_contribution_profit_per_order'].round(2)
        date_agg['contribution_cost'] = date_agg['contribution_cost'].round(2)
        date_agg['contribution_profit'] = date_agg['contribution_profit'].round(2)
        date_agg['contribution_margin_pct'] = date_agg['contribution_margin_pct'].round(2)
        date_agg['contribution_profit_per_order'] = date_agg['contribution_profit_per_order'].round(2)

        # Sort by date
        date_agg = date_agg.sort_values('date')
        
        # Save date aggregation
        date_filename = self.output_path(f"aggregate_by_date_{date_from.strftime('%Y%m%d')}-{date_to.strftime('%Y%m%d')}.csv")
        date_agg.to_csv(date_filename, index=False, encoding='utf-8-sig')
        print(f"Date aggregation saved: {date_filename}")
        
        # 2b. Create monthly aggregation
        print("Creating monthly aggregation...")
        
        # Add month column to date_agg for grouping
        date_agg_copy = date_agg.copy()
        date_agg_copy['month'] = pd.to_datetime(date_agg_copy['date']).dt.to_period('M')
        
        # Group by month
        month_agg = date_agg_copy.groupby('month').agg({
            'unique_orders': 'sum',
            'total_items': 'sum',
            'total_quantity': 'sum',
            'total_revenue': 'sum',
            'product_expense': 'sum',
            'packaging_cost': 'sum',
            'shipping_subsidy_cost': 'sum',
            'fixed_daily_cost': 'sum',
            'fb_ads_spend': 'sum',
            'google_ads_spend': 'sum',
            'total_cost': 'sum',
            'net_profit': 'sum',
            'pre_ad_contribution_cost': 'sum',
            'pre_ad_contribution_profit': 'sum',
            'contribution_cost': 'sum',
            'contribution_profit': 'sum'
        }).reset_index()
        
        # Calculate ROI for each month
        month_agg['roi_percent'] = month_agg.apply(
            lambda row: round((row['net_profit'] / row['total_cost'] * 100) if row['total_cost'] > 0 else 0, 2),
            axis=1
        )
        month_agg['contribution_margin_pct'] = month_agg.apply(
            lambda row: round((row['contribution_profit'] / row['total_revenue'] * 100) if row['total_revenue'] > 0 else 0, 2),
            axis=1
        )
        month_agg['pre_ad_contribution_margin_pct'] = month_agg.apply(
            lambda row: round((row['pre_ad_contribution_profit'] / row['total_revenue'] * 100) if row['total_revenue'] > 0 else 0, 2),
            axis=1
        )
        month_agg['contribution_profit_per_order'] = month_agg.apply(
            lambda row: round((row['contribution_profit'] / row['unique_orders']) if row['unique_orders'] > 0 else 0, 2),
            axis=1
        )
        month_agg['pre_ad_contribution_profit_per_order'] = month_agg.apply(
            lambda row: round((row['pre_ad_contribution_profit'] / row['unique_orders']) if row['unique_orders'] > 0 else 0, 2),
            axis=1
        )
        # Explicit post-ad aliases (terminology clarity)
        month_agg['post_ad_contribution_margin_pct'] = month_agg['contribution_margin_pct']
        month_agg['post_ad_contribution_profit_per_order'] = month_agg['contribution_profit_per_order']
        month_agg['cm1_profit'] = month_agg['pre_ad_contribution_profit']
        month_agg['cm1_margin_pct'] = month_agg['pre_ad_contribution_margin_pct']
        month_agg['cm1_profit_per_order'] = month_agg['pre_ad_contribution_profit_per_order']
        month_agg['cm2_profit'] = month_agg['contribution_profit']
        month_agg['cm2_margin_pct'] = month_agg['post_ad_contribution_margin_pct']
        month_agg['cm2_profit_per_order'] = month_agg['post_ad_contribution_profit_per_order']
        month_agg['cm3_profit'] = month_agg['net_profit']
        month_agg['cm3_margin_pct'] = month_agg.apply(
            lambda row: round((row['net_profit'] / row['total_revenue'] * 100) if row['total_revenue'] > 0 else 0, 2),
            axis=1
        )
        month_agg['cm3_profit_per_order'] = month_agg.apply(
            lambda row: round((row['net_profit'] / row['unique_orders']) if row['unique_orders'] > 0 else 0, 2),
            axis=1
        )
        
        # Convert month period to string for display
        month_agg['month'] = month_agg['month'].astype(str)
        
        # Save monthly aggregation
        month_filename = self.output_path(f"aggregate_by_month_{date_from.strftime('%Y%m%d')}-{date_to.strftime('%Y%m%d')}.csv")
        month_agg.to_csv(month_filename, index=False, encoding='utf-8-sig')
        print(f"Monthly aggregation saved: {month_filename}")
        
        # 3. Group by items only (across all dates) - use product_sku for consistent grouping
        print("Creating items aggregation...")

        items_agg = df.groupby('product_sku').agg({
            'item_label': 'first',  # Keep product name for display
            'item_quantity': 'sum',
            'item_total_without_tax': 'sum',
            'total_expense': 'sum',
            'profit_before_ads': 'sum',
            'order_num': 'nunique'  # Count unique orders
        }).reset_index()

        items_agg.columns = ['product_sku', 'product_name', 'total_quantity', 'total_revenue', 'product_expense', 'profit', 'order_count']
        
        # Calculate ROI based on product expense only (no FB ads)
        items_agg['roi_percent'] = items_agg.apply(
            lambda row: round((row['profit'] / row['product_expense'] * 100) if row['product_expense'] > 0 else 0, 2),
            axis=1
        )
        
        # Round financial values
        items_agg['total_revenue'] = items_agg['total_revenue'].round(2)
        items_agg['product_expense'] = items_agg['product_expense'].round(2)
        items_agg['profit'] = items_agg['profit'].round(2)
        
        # Sort by total revenue descending
        items_agg = items_agg.sort_values('total_revenue', ascending=False)
        
        # Save items aggregation
        items_filename = self.output_path(f"aggregate_by_items_{date_from.strftime('%Y%m%d')}-{date_to.strftime('%Y%m%d')}.csv")
        items_agg.to_csv(items_filename, index=False, encoding='utf-8-sig')
        print(f"Items aggregation saved: {items_filename}")

        # 4. Calculate Customer Lifetime Revenue by Acquisition Date
        print("Calculating customer lifetime revenue by acquisition date...")

        # Group by customer to get their first purchase date and total lifetime revenue
        customer_lifetime = df.groupby('customer_email').agg({
            'purchase_date_only': 'min',  # First purchase date
            'item_total_without_tax': 'sum',  # Total lifetime revenue
            'order_num': 'nunique'  # Total number of orders
        }).reset_index()

        customer_lifetime.columns = ['customer_email', 'first_purchase_date', 'lifetime_revenue', 'total_orders']

        # Now aggregate by first purchase date to get total lifetime revenue attributed to each acquisition date
        ltv_by_date = customer_lifetime.groupby('first_purchase_date').agg({
            'lifetime_revenue': 'sum',
            'customer_email': 'count',  # Count of customers acquired
            'total_orders': 'sum'  # Total orders these customers made over their lifetime
        }).reset_index()

        ltv_by_date.columns = ['date', 'ltv_revenue', 'customers_acquired', 'total_lifetime_orders']

        # Fill in missing dates with zeros
        ltv_complete_date_df = pd.DataFrame({'date': complete_date_range})
        ltv_by_date = ltv_complete_date_df.merge(ltv_by_date, on='date', how='left')
        ltv_by_date['ltv_revenue'] = ltv_by_date['ltv_revenue'].fillna(0).round(2)
        ltv_by_date['customers_acquired'] = ltv_by_date['customers_acquired'].fillna(0).astype(int)
        ltv_by_date['total_lifetime_orders'] = ltv_by_date['total_lifetime_orders'].fillna(0).astype(int)

        # Sort by date
        ltv_by_date = ltv_by_date.sort_values('date')

        # Save LTV by acquisition date
        ltv_filename = self.output_path(f"ltv_by_acquisition_date_{date_from.strftime('%Y%m%d')}-{date_to.strftime('%Y%m%d')}.csv")
        ltv_by_date.to_csv(ltv_filename, index=False, encoding='utf-8-sig')
        print(f"LTV by acquisition date saved: {ltv_filename}")

        # Return aggregated data for display
        return date_product_agg, date_agg, items_agg, month_agg, ltv_by_date
    
    def display_aggregated_data(self, date_product_agg: pd.DataFrame, date_agg: pd.DataFrame, month_agg: pd.DataFrame = None):
        """Display aggregated data with nice formatting"""
        
        # If we have monthly data, display each month separately first
        if month_agg is not None and not month_agg.empty:
            # Convert date column to datetime for grouping
            date_agg_copy = date_agg.copy()
            date_agg_copy['month'] = pd.to_datetime(date_agg_copy['date']).dt.to_period('M')
            
            # Display each month's daily data separately
            for month_period in date_agg_copy['month'].unique():
                month_str = str(month_period)
                month_data = date_agg_copy[date_agg_copy['month'] == month_period]
                
                print("\n" + "="*220)
                print(f"DAILY SUMMARY FOR {month_str.upper()}")
                print("Fixed Costs = Packaging + Net Shipping + Fixed Daily Cost | AOV = Avg Order Value | FB/Order = Avg FB Cost per Order")
                print("="*220)
                
                print(f"\n{'Date':<12} {'Orders':>8} {'Items':>8} {'Revenue (â‚¬)':>12} {'AOV (â‚¬)':>8} {'Product (â‚¬)':>12} {'Fixed Costs (â‚¬)':>14} {'FB Ads (â‚¬)':>12} {'Google Ads (â‚¬)':>14} {'Total Cost (â‚¬)':>14} {'Profit (â‚¬)':>12} {'ROI %':>8}")
                print("-"*240)
                
                month_orders = 0
                month_items = 0
                month_revenue = 0
                month_product_expense = 0
                month_packaging = 0
                month_shipping = 0
                month_fixed = 0
                month_fb_ads = 0
                month_google_ads = 0
                month_net_profit = 0
                
                for _, row in month_data.iterrows():
                    date_str = str(row['date'])
                    fixed_costs = row['packaging_cost'] + row.get('shipping_net_cost', row.get('shipping_subsidy_cost', 0)) + row['fixed_daily_cost']
                    aov = row['total_revenue'] / row['unique_orders'] if row['unique_orders'] > 0 else 0
                    fb_per_order = row['fb_ads_spend'] / row['unique_orders'] if row['unique_orders'] > 0 else 0
                    google_ads = row.get('google_ads_spend', 0)
                    print(f"{date_str:<12} {row['unique_orders']:>8} {row['total_items']:>8} "
                          f"{row['total_revenue']:>12.2f} {aov:>8.2f} {row['product_expense']:>12.2f} "
                          f"{fixed_costs:>14.2f} "
                          f"{row['fb_ads_spend']:>12.2f} {google_ads:>14.2f} {row['total_cost']:>14.2f} {row['net_profit']:>12.2f} {row['roi_percent']:>8.2f}")
                    month_orders += row['unique_orders']
                    month_items += row['total_items']
                    month_revenue += row['total_revenue']
                    month_product_expense += row['product_expense']
                    month_packaging += row['packaging_cost']
                    month_shipping += row.get('shipping_net_cost', row.get('shipping_subsidy_cost', 0))
                    month_fixed += row['fixed_daily_cost']
                    month_fb_ads += row['fb_ads_spend']
                    month_google_ads += google_ads
                    month_net_profit += row['net_profit']

                # Monthly total
                month_fixed_costs = month_packaging + month_shipping + month_fixed
                month_cost = month_product_expense + month_packaging + month_shipping + month_fixed + month_fb_ads + month_google_ads
                month_roi = (month_net_profit / month_cost * 100) if month_cost > 0 else 0
                month_aov = month_revenue / month_orders if month_orders > 0 else 0
                month_fb_per_order = month_fb_ads / month_orders if month_orders > 0 else 0
                
                print("-"*240)
                print(f"{'MONTH TOTAL':<12} {month_orders:>8} {month_items:>8} "
                      f"{month_revenue:>12.2f} {month_aov:>8.2f} {month_product_expense:>12.2f} "
                      f"{month_fixed_costs:>14.2f} "
                      f"{month_fb_ads:>12.2f} {month_google_ads:>14.2f} {month_cost:>14.2f} {month_net_profit:>12.2f} {month_roi:>8.2f}")
        
        # Display monthly summary if available
        if month_agg is not None and not month_agg.empty:
            print("\n" + "="*220)
            print("MONTHLY SUMMARY")
            print("="*220)
            print(f"\n{'Month':<12} {'Orders':>8} {'Items':>8} {'Revenue (â‚¬)':>12} {'AOV (â‚¬)':>8} {'Product (â‚¬)':>12} {'Fixed Costs (â‚¬)':>14} {'FB Ads (â‚¬)':>12} {'Google Ads (â‚¬)':>14} {'Total Cost (â‚¬)':>14} {'Profit (â‚¬)':>12} {'ROI %':>8}")
            print("-"*240)
            
            month_total_orders = 0
            month_total_items = 0
            month_total_revenue = 0
            month_total_product_expense = 0
            month_total_packaging = 0
            month_total_shipping = 0
            month_total_fixed = 0
            month_total_fb_ads = 0
            month_total_google_ads = 0
            month_total_net_profit = 0
            
            for _, row in month_agg.iterrows():
                month_str = str(row['month'])
                fixed_costs = row['packaging_cost'] + row.get('shipping_net_cost', row.get('shipping_subsidy_cost', 0)) + row['fixed_daily_cost']
                aov = row['total_revenue'] / row['unique_orders'] if row['unique_orders'] > 0 else 0
                fb_per_order = row['fb_ads_spend'] / row['unique_orders'] if row['unique_orders'] > 0 else 0
                google_ads = row.get('google_ads_spend', 0)
                print(f"{month_str:<12} {row['unique_orders']:>8} {row['total_items']:>8} "
                      f"{row['total_revenue']:>12.2f} {aov:>8.2f} {row['product_expense']:>12.2f} "
                      f"{fixed_costs:>14.2f} "
                      f"{row['fb_ads_spend']:>12.2f} {google_ads:>14.2f} {row['total_cost']:>14.2f} "
                      f"{row['net_profit']:>12.2f} {row['roi_percent']:>8.2f}")
                month_total_orders += row['unique_orders']
                month_total_items += row['total_items']
                month_total_revenue += row['total_revenue']
                month_total_product_expense += row['product_expense']
                month_total_packaging += row['packaging_cost']
                month_total_shipping += row.get('shipping_net_cost', row.get('shipping_subsidy_cost', 0))
                month_total_fixed += row['fixed_daily_cost']
                month_total_fb_ads += row['fb_ads_spend']
                month_total_google_ads += google_ads
                month_total_net_profit += row['net_profit']

            # Calculate total for monthly summary
            month_total_fixed_costs = month_total_packaging + month_total_shipping + month_total_fixed
            month_total_cost = month_total_product_expense + month_total_packaging + month_total_shipping + month_total_fixed + month_total_fb_ads + month_total_google_ads
            month_total_roi = (month_total_net_profit / month_total_cost * 100) if month_total_cost > 0 else 0
            month_total_aov = month_total_revenue / month_total_orders if month_total_orders > 0 else 0
            month_total_fb_per_order = month_total_fb_ads / month_total_orders if month_total_orders > 0 else 0
            
            print("-"*240)
            print(f"{'TOTAL':<12} {month_total_orders:>8} {month_total_items:>8} "
                  f"{month_total_revenue:>12.2f} {month_total_aov:>8.2f} {month_total_product_expense:>12.2f} "
                  f"{month_total_fixed_costs:>14.2f} "
                  f"{month_total_fb_ads:>12.2f} {month_total_google_ads:>14.2f} {month_total_cost:>14.2f} "
                  f"{month_total_net_profit:>12.2f} {month_total_roi:>8.2f}")
        
        # Display all products
        print("\n" + "="*80)
        print("ALL PRODUCTS BY REVENUE")
        print("="*80)
        
        # Aggregate products across all dates
        product_summary = date_product_agg.groupby('product_name').agg({
            'total_quantity': 'sum',
            'total_revenue': 'sum',
            'product_expense': 'sum',
            'profit': 'sum',
            'order_count': 'sum'
        }).reset_index()
        
        # Calculate aggregated ROI (without FB ads)
        product_summary['roi_percent'] = product_summary.apply(
            lambda row: round((row['profit'] / row['product_expense'] * 100) if row['product_expense'] > 0 else 0, 2),
            axis=1
        )
        
        product_summary = product_summary.sort_values('total_revenue', ascending=False)
        
        print(f"\n{'Product':<40} {'Qty':>6} {'Revenue':>10} {'Product Cost':>12} {'Profit':>10} {'ROI %':>8}")
        print("-"*100)
        
        for _, row in product_summary.iterrows():
            product_name = row['product_name'][:40]  # Truncate long names
            print(f"{product_name:<40} {row['total_quantity']:>6} "
                  f"{row['total_revenue']:>10.2f} {row['product_expense']:>12.2f} "
                  f"{row['profit']:>10.2f} {row['roi_percent']:>8.2f}")
        
        print("\n")
    
    def analyze_returning_customers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Analyze returning customers and calculate weekly percentages"""
        print("\nAnalyzing returning customers...")
        revenue_col = 'order_revenue_net' if 'order_revenue_net' in df.columns else 'order_total'
        
        # Convert purchase_date to datetime
        df['purchase_datetime'] = pd.to_datetime(df['purchase_date'])
        df['purchase_date_only'] = df['purchase_datetime'].dt.date
        
        # Extract week information
        df['year_week'] = df['purchase_datetime'].dt.to_period('W')
        
        # Get unique customers per order (one row per order)
        orders_df = df[['order_num', 'customer_email', 'purchase_datetime', 'year_week', revenue_col]].drop_duplicates(subset=['order_num'])
        
        # Track first purchase date for each customer
        customer_first_purchase = orders_df.groupby('customer_email')['purchase_datetime'].min().to_dict()
        
        # Determine if each order is from a returning customer
        orders_df['is_returning'] = orders_df.apply(
            lambda row: row['purchase_datetime'] > customer_first_purchase[row['customer_email']], axis=1
        )
        
        # Calculate weekly statistics
        weekly_stats = orders_df.groupby('year_week').agg({
            'order_num': 'count',  # Total orders
            'is_returning': 'sum',  # Returning customer orders
            'customer_email': 'nunique'  # Unique customers
        }).reset_index()
        
        weekly_stats.columns = ['week', 'total_orders', 'returning_orders', 'unique_customers']
        
        # Calculate new customer orders
        weekly_stats['new_orders'] = weekly_stats['total_orders'] - weekly_stats['returning_orders']
        
        # Calculate percentages
        weekly_stats['returning_percentage'] = (weekly_stats['returning_orders'] / weekly_stats['total_orders'] * 100).round(2)
        weekly_stats['new_percentage'] = (weekly_stats['new_orders'] / weekly_stats['total_orders'] * 100).round(2)
        
        # Add week start date for better visualization
        weekly_stats['week_start'] = weekly_stats['week'].apply(lambda x: x.start_time.date())
        
        # Sort by week
        weekly_stats = weekly_stats.sort_values('week')
        
        # Save to CSV
        filename = self.output_path(f"returning_customers_analysis_{df['purchase_datetime'].min().strftime('%Y%m%d')}-{df['purchase_datetime'].max().strftime('%Y%m%d')}.csv")
        weekly_stats.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"Returning customers analysis saved: {filename}")
        
        return weekly_stats

    def analyze_new_vs_returning_revenue(self, df: pd.DataFrame) -> dict:
        """Analyze revenue split between new and returning customers (order-level, net revenue)."""
        print("\nAnalyzing new vs returning revenue split...")
        revenue_col = 'order_revenue_net' if 'order_revenue_net' in df.columns else 'order_total'

        orders_df = df[['order_num', 'customer_email', 'purchase_date', revenue_col]].drop_duplicates(subset=['order_num']).copy()
        orders_df['purchase_datetime'] = pd.to_datetime(orders_df['purchase_date'])
        orders_df['purchase_date_only'] = orders_df['purchase_datetime'].dt.date

        first_purchase_map = orders_df.groupby('customer_email')['purchase_datetime'].min().to_dict()
        orders_df['is_returning'] = orders_df.apply(
            lambda row: row['purchase_datetime'] > first_purchase_map.get(row['customer_email'], row['purchase_datetime']),
            axis=1
        )

        daily = orders_df.groupby(['purchase_date_only', 'is_returning'])[revenue_col].sum().reset_index()
        daily_pivot = daily.pivot(index='purchase_date_only', columns='is_returning', values=revenue_col).fillna(0).reset_index()
        daily_pivot = daily_pivot.rename(
            columns={
                False: 'new_revenue',
                True: 'returning_revenue',
                'purchase_date_only': 'date'
            }
        )

        if 'new_revenue' not in daily_pivot.columns:
            daily_pivot['new_revenue'] = 0.0
        if 'returning_revenue' not in daily_pivot.columns:
            daily_pivot['returning_revenue'] = 0.0

        daily_pivot['total_revenue'] = daily_pivot['new_revenue'] + daily_pivot['returning_revenue']
        daily_pivot['new_revenue_share_pct'] = daily_pivot.apply(
            lambda row: round((row['new_revenue'] / row['total_revenue'] * 100) if row['total_revenue'] > 0 else 0, 2),
            axis=1
        )
        daily_pivot['returning_revenue_share_pct'] = daily_pivot.apply(
            lambda row: round((row['returning_revenue'] / row['total_revenue'] * 100) if row['total_revenue'] > 0 else 0, 2),
            axis=1
        )
        daily_pivot = daily_pivot.sort_values('date')

        total_new_revenue = float(daily_pivot['new_revenue'].sum())
        total_returning_revenue = float(daily_pivot['returning_revenue'].sum())
        total_revenue = total_new_revenue + total_returning_revenue

        result = {
            'summary': {
                'new_revenue': round(total_new_revenue, 2),
                'returning_revenue': round(total_returning_revenue, 2),
                'total_revenue': round(total_revenue, 2),
                'new_revenue_share_pct': round((total_new_revenue / total_revenue * 100) if total_revenue > 0 else 0, 1),
                'returning_revenue_share_pct': round((total_returning_revenue / total_revenue * 100) if total_revenue > 0 else 0, 1),
            },
            'daily': daily_pivot
        }

        print(
            f"New vs returning revenue: new=â‚¬{result['summary']['new_revenue']:.2f}, "
            f"returning=â‚¬{result['summary']['returning_revenue']:.2f}"
        )
        return result

    def analyze_repeat_purchase_cohorts(self, df: pd.DataFrame) -> dict:
        """
        Analyze repeat purchase cohorts with detailed metrics:
        - Cohort by first purchase month
        - Time to nth order (2nd, 3rd, 4th, 5th+)
        - Time between consecutive orders
        - Cohort retention rates
        - Order frequency distribution
        - Revenue progression by order number
        """
        print("\nAnalyzing repeat purchase cohorts...")
        revenue_col = 'order_revenue_net' if 'order_revenue_net' in df.columns else 'order_total'

        # Ensure datetime column exists
        df['purchase_datetime'] = pd.to_datetime(df['purchase_date'])

        # Get unique orders with customer info (include total_items_in_order for metrics)
        orders_df = df[['order_num', 'customer_email', 'purchase_datetime', revenue_col, 'total_items_in_order']].drop_duplicates(subset=['order_num'])
        orders_df = orders_df.sort_values(['customer_email', 'purchase_datetime'])

        # Add order number for each customer (1st, 2nd, 3rd, etc.)
        orders_df['customer_order_num'] = orders_df.groupby('customer_email').cumcount() + 1

        # Get first order date for each customer (defines their cohort)
        customer_first_order = orders_df.groupby('customer_email').agg({
            'purchase_datetime': 'min',
            revenue_col: 'first'
        }).reset_index()
        customer_first_order.columns = ['customer_email', 'first_order_date', 'first_order_value']
        customer_first_order['cohort_month'] = customer_first_order['first_order_date'].dt.to_period('M')

        # Merge cohort info back to orders
        orders_df = orders_df.merge(
            customer_first_order[['customer_email', 'first_order_date', 'cohort_month']],
            on='customer_email'
        )

        # Calculate time since first order for each order
        orders_df['days_since_first_order'] = (orders_df['purchase_datetime'] - orders_df['first_order_date']).dt.days

        # Calculate time between consecutive orders
        orders_df['prev_order_date'] = orders_df.groupby('customer_email')['purchase_datetime'].shift(1)
        orders_df['days_since_prev_order'] = (orders_df['purchase_datetime'] - orders_df['prev_order_date']).dt.days

        result = {}

        # === 1. TIME TO NTH ORDER ANALYSIS ===
        print("  Calculating time to nth order...")
        time_to_nth_order = []
        for order_num in range(2, 7):  # 2nd through 6th order
            nth_orders = orders_df[orders_df['customer_order_num'] == order_num]
            if len(nth_orders) > 0:
                time_to_nth_order.append({
                    'order_number': f'{order_num}{"st" if order_num == 1 else "nd" if order_num == 2 else "rd" if order_num == 3 else "th"} Order',
                    'order_num_value': order_num,
                    'customer_count': len(nth_orders),
                    'avg_days_from_first': round(nth_orders['days_since_first_order'].mean(), 1),
                    'median_days_from_first': round(nth_orders['days_since_first_order'].median(), 1),
                    'min_days_from_first': int(nth_orders['days_since_first_order'].min()),
                    'max_days_from_first': int(nth_orders['days_since_first_order'].max()),
                    'avg_days_from_prev': round(nth_orders['days_since_prev_order'].mean(), 1) if order_num > 1 else 0,
                    'median_days_from_prev': round(nth_orders['days_since_prev_order'].median(), 1) if order_num > 1 else 0,
                    'avg_order_value': round(nth_orders[revenue_col].mean(), 2)
                })

        result['time_to_nth_order'] = pd.DataFrame(time_to_nth_order)

        # === 2. TIME BETWEEN ORDERS DISTRIBUTION ===
        print("  Analyzing time between orders...")
        repeat_orders = orders_df[orders_df['customer_order_num'] > 1].copy()
        if len(repeat_orders) > 0:
            # Create buckets for time between orders
            def categorize_time_between(days):
                if pd.isna(days):
                    return None
                if days <= 7:
                    return '0-7 days'
                elif days <= 14:
                    return '8-14 days'
                elif days <= 30:
                    return '15-30 days'
                elif days <= 60:
                    return '31-60 days'
                elif days <= 90:
                    return '61-90 days'
                else:
                    return '90+ days'

            repeat_orders['time_bucket'] = repeat_orders['days_since_prev_order'].apply(categorize_time_between)
            time_distribution = repeat_orders.groupby('time_bucket').size().reset_index(name='count')

            # Ensure proper ordering
            bucket_order = ['0-7 days', '8-14 days', '15-30 days', '31-60 days', '61-90 days', '90+ days']
            time_distribution['bucket_order'] = time_distribution['time_bucket'].apply(
                lambda x: bucket_order.index(x) if x in bucket_order else 99
            )
            time_distribution = time_distribution.sort_values('bucket_order').drop('bucket_order', axis=1)
            time_distribution['percentage'] = round(time_distribution['count'] / time_distribution['count'].sum() * 100, 1)

            result['time_between_orders'] = time_distribution

            # === 2b. TIME BETWEEN ORDERS BY ORDER NUMBER (1stâ†’2nd, 2ndâ†’3rd, etc.) ===
            print("  Analyzing time between orders by order number...")
            time_by_order_num = []
            for order_num in range(2, 7):  # 2nd through 6th order
                order_transitions = repeat_orders[repeat_orders['customer_order_num'] == order_num]
                if len(order_transitions) >= 3:  # Min 3 data points
                    transition_label = f'{order_num-1}â†’{order_num}'
                    time_by_order_num.append({
                        'transition': transition_label,
                        'order_num': order_num,
                        'count': len(order_transitions),
                        'avg_days': round(order_transitions['days_since_prev_order'].mean(), 1),
                        'median_days': round(order_transitions['days_since_prev_order'].median(), 1),
                        'min_days': int(order_transitions['days_since_prev_order'].min()),
                        'max_days': int(order_transitions['days_since_prev_order'].max())
                    })

            result['time_between_by_order_num'] = pd.DataFrame(time_by_order_num)
        else:
            result['time_between_orders'] = pd.DataFrame()
            result['time_between_by_order_num'] = pd.DataFrame()

        # === 3. COHORT RETENTION ANALYSIS ===
        print("  Calculating cohort retention...")
        cohort_data = []

        for cohort in orders_df['cohort_month'].unique():
            cohort_customers = orders_df[orders_df['cohort_month'] == cohort]['customer_email'].unique()
            total_customers = len(cohort_customers)

            if total_customers < 3:  # Skip very small cohorts
                continue

            # Count how many made 2nd, 3rd, 4th, 5th+ order
            orders_per_customer = orders_df[orders_df['customer_email'].isin(cohort_customers)].groupby('customer_email').size()

            cohort_data.append({
                'cohort': str(cohort),
                'total_customers': total_customers,
                'made_2nd_order': len(orders_per_customer[orders_per_customer >= 2]),
                'made_3rd_order': len(orders_per_customer[orders_per_customer >= 3]),
                'made_4th_order': len(orders_per_customer[orders_per_customer >= 4]),
                'made_5th_order': len(orders_per_customer[orders_per_customer >= 5]),
                'retention_2nd_pct': round(len(orders_per_customer[orders_per_customer >= 2]) / total_customers * 100, 1),
                'retention_3rd_pct': round(len(orders_per_customer[orders_per_customer >= 3]) / total_customers * 100, 1),
                'retention_4th_pct': round(len(orders_per_customer[orders_per_customer >= 4]) / total_customers * 100, 1),
                'retention_5th_pct': round(len(orders_per_customer[orders_per_customer >= 5]) / total_customers * 100, 1),
                'avg_orders_per_customer': round(orders_per_customer.mean(), 2),
                'total_orders': orders_per_customer.sum()
            })

        result['cohort_retention'] = pd.DataFrame(cohort_data)

        # Sort cohort retention by cohort month
        if not result['cohort_retention'].empty:
            result['cohort_retention'] = result['cohort_retention'].sort_values('cohort').reset_index(drop=True)

        # === 3b. TIME-BIAS-FREE COHORT ANALYSIS (only mature cohorts 90+ days old) ===
        print("  Calculating time-bias-free cohort retention (90+ day cohorts only)...")
        today = pd.Timestamp.now()
        mature_cohort_data = []

        for cohort in orders_df['cohort_month'].unique():
            # Calculate cohort age (days since end of cohort month)
            cohort_end = pd.Period(cohort, 'M').end_time
            cohort_age_days = (today - cohort_end).days

            # Only include cohorts that are 90+ days old
            if cohort_age_days < 90:
                continue

            cohort_customers = orders_df[orders_df['cohort_month'] == cohort]['customer_email'].unique()
            total_customers = len(cohort_customers)

            if total_customers < 3:  # Skip very small cohorts
                continue

            # Count how many made 2nd, 3rd, 4th, 5th+ order
            orders_per_customer = orders_df[orders_df['customer_email'].isin(cohort_customers)].groupby('customer_email').size()

            mature_cohort_data.append({
                'cohort': str(cohort),
                'cohort_age_days': cohort_age_days,
                'total_customers': total_customers,
                'made_2nd_order': len(orders_per_customer[orders_per_customer >= 2]),
                'made_3rd_order': len(orders_per_customer[orders_per_customer >= 3]),
                'made_4th_order': len(orders_per_customer[orders_per_customer >= 4]),
                'made_5th_order': len(orders_per_customer[orders_per_customer >= 5]),
                'retention_2nd_pct': round(len(orders_per_customer[orders_per_customer >= 2]) / total_customers * 100, 1),
                'retention_3rd_pct': round(len(orders_per_customer[orders_per_customer >= 3]) / total_customers * 100, 1),
                'retention_4th_pct': round(len(orders_per_customer[orders_per_customer >= 4]) / total_customers * 100, 1),
                'retention_5th_pct': round(len(orders_per_customer[orders_per_customer >= 5]) / total_customers * 100, 1),
                'avg_orders_per_customer': round(orders_per_customer.mean(), 2),
                'total_orders': orders_per_customer.sum()
            })

        result['mature_cohort_retention'] = pd.DataFrame(mature_cohort_data)

        # Sort by cohort month
        if not result['mature_cohort_retention'].empty:
            result['mature_cohort_retention'] = result['mature_cohort_retention'].sort_values('cohort').reset_index(drop=True)

            # Calculate average retention across mature cohorts (weighted by customer count)
            total_mature_customers = result['mature_cohort_retention']['total_customers'].sum()
            if total_mature_customers > 0:
                weighted_2nd = (result['mature_cohort_retention']['retention_2nd_pct'] * result['mature_cohort_retention']['total_customers']).sum() / total_mature_customers
                weighted_3rd = (result['mature_cohort_retention']['retention_3rd_pct'] * result['mature_cohort_retention']['total_customers']).sum() / total_mature_customers
                # Store for later adding to summary
                result['_mature_cohort_stats'] = {
                    'true_retention_2nd_pct': round(weighted_2nd, 1),
                    'true_retention_3rd_pct': round(weighted_3rd, 1),
                    'mature_cohorts_count': len(result['mature_cohort_retention'])
                }
                print(f"    Mature cohorts (90+ days): {len(result['mature_cohort_retention'])}")
                print(f"    True 2nd order retention: {result['_mature_cohort_stats']['true_retention_2nd_pct']}%")
                print(f"    True 3rd order retention: {result['_mature_cohort_stats']['true_retention_3rd_pct']}%")

        # === 4. ORDER FREQUENCY DISTRIBUTION ===
        print("  Analyzing order frequency distribution...")
        orders_per_customer = orders_df.groupby('customer_email').size().reset_index(name='order_count')

        def categorize_order_count(count):
            if count == 1:
                return '1 order'
            elif count == 2:
                return '2 orders'
            elif count == 3:
                return '3 orders'
            elif count == 4:
                return '4 orders'
            elif count == 5:
                return '5 orders'
            else:
                return '6+ orders'

        orders_per_customer['frequency_bucket'] = orders_per_customer['order_count'].apply(categorize_order_count)
        frequency_dist = orders_per_customer.groupby('frequency_bucket').agg({
            'customer_email': 'count',
            'order_count': 'sum'
        }).reset_index()
        frequency_dist.columns = ['frequency', 'customer_count', 'total_orders']

        # Ensure proper ordering
        freq_order = ['1 order', '2 orders', '3 orders', '4 orders', '5 orders', '6+ orders']
        frequency_dist['freq_order'] = frequency_dist['frequency'].apply(
            lambda x: freq_order.index(x) if x in freq_order else 99
        )
        frequency_dist = frequency_dist.sort_values('freq_order').drop('freq_order', axis=1)
        frequency_dist['customer_pct'] = round(frequency_dist['customer_count'] / frequency_dist['customer_count'].sum() * 100, 1)
        frequency_dist['orders_pct'] = round(frequency_dist['total_orders'] / frequency_dist['total_orders'].sum() * 100, 1)

        result['order_frequency'] = frequency_dist

        # === 5. REVENUE BY ORDER NUMBER ===
        print("  Analyzing revenue by order number...")
        revenue_by_order_num = orders_df.groupby('customer_order_num').agg({
            revenue_col: ['mean', 'sum', 'count'],
            'total_items_in_order': 'mean'
        }).reset_index()
        revenue_by_order_num.columns = ['order_number', 'avg_order_value', 'total_revenue', 'order_count', 'avg_items_per_order']
        revenue_by_order_num = revenue_by_order_num[revenue_by_order_num['order_count'] >= 3]  # Min 3 orders to include
        revenue_by_order_num['avg_order_value'] = revenue_by_order_num['avg_order_value'].round(2)
        revenue_by_order_num['total_revenue'] = revenue_by_order_num['total_revenue'].round(2)
        revenue_by_order_num['avg_items_per_order'] = revenue_by_order_num['avg_items_per_order'].round(2)
        # Calculate avg price per item
        revenue_by_order_num['avg_price_per_item'] = (revenue_by_order_num['avg_order_value'] / revenue_by_order_num['avg_items_per_order']).round(2)

        # Limit to first 10 orders for display
        revenue_by_order_num = revenue_by_order_num[revenue_by_order_num['order_number'] <= 10]

        result['revenue_by_order_num'] = revenue_by_order_num

        # === 6. SUMMARY STATISTICS ===
        print("  Calculating summary statistics...")
        total_customers = orders_df['customer_email'].nunique()
        repeat_customers = len(orders_per_customer[orders_per_customer['order_count'] > 1])

        # Average time to repeat purchase (for customers who made 2nd order)
        second_orders = orders_df[orders_df['customer_order_num'] == 2]
        avg_days_to_2nd = second_orders['days_since_first_order'].mean() if len(second_orders) > 0 else None
        median_days_to_2nd = second_orders['days_since_first_order'].median() if len(second_orders) > 0 else None

        # Average time between all repeat orders
        avg_days_between = repeat_orders['days_since_prev_order'].mean() if len(repeat_orders) > 0 else None
        median_days_between = repeat_orders['days_since_prev_order'].median() if len(repeat_orders) > 0 else None

        result['summary'] = {
            'total_customers': total_customers,
            'repeat_customers': repeat_customers,
            'repeat_rate_pct': round(repeat_customers / total_customers * 100, 1) if total_customers > 0 else 0,
            'one_time_customers': total_customers - repeat_customers,
            'avg_orders_per_customer': round(orders_df['order_num'].nunique() / total_customers, 2) if total_customers > 0 else 0,
            'avg_days_to_2nd_order': round(avg_days_to_2nd, 1) if avg_days_to_2nd else None,
            'median_days_to_2nd_order': round(median_days_to_2nd, 1) if median_days_to_2nd else None,
            'avg_days_between_orders': round(avg_days_between, 1) if avg_days_between else None,
            'median_days_between_orders': round(median_days_between, 1) if median_days_between else None,
            'max_orders_by_customer': int(orders_per_customer['order_count'].max()),
            'customers_with_5plus_orders': len(orders_per_customer[orders_per_customer['order_count'] >= 5])
        }

        # Add mature cohort stats to summary if available
        if '_mature_cohort_stats' in result:
            result['summary'].update(result['_mature_cohort_stats'])
            del result['_mature_cohort_stats']

        # Save cohort analysis to CSV
        cohort_filename = self.output_path(f"cohort_analysis_{df['purchase_datetime'].min().strftime('%Y%m%d')}-{df['purchase_datetime'].max().strftime('%Y%m%d')}.csv")
        if not result['cohort_retention'].empty:
            result['cohort_retention'].to_csv(cohort_filename, index=False, encoding='utf-8-sig')
            print(f"Cohort analysis saved: {cohort_filename}")

        print(f"  Cohort analysis complete:")
        print(f"    - Total customers: {result['summary']['total_customers']}")
        print(f"    - Repeat customers: {result['summary']['repeat_customers']} ({result['summary']['repeat_rate_pct']}%)")
        print(f"    - Avg days to 2nd order: {result['summary']['avg_days_to_2nd_order']}")
        print(f"    - Avg days between orders: {result['summary']['avg_days_between_orders']}")

        return result

    def analyze_sample_funnel(self, df: pd.DataFrame, windows: Optional[List[int]] = None, top_n: int = 12) -> dict:
        """
        Model Vevo sample funnel from first-order sample entry to repeat/full-size conversion.

        Entry cohort definition:
        - customer's first order contains at least one sample item
        - customer's first order does not contain a full-size item

        Conversions are measured from the first sample-entry order to:
        - any repeat order
        - any full-size order
        - first 200ml order
        - first 500ml order
        """
        windows = windows or [7, 14, 30, 60, 90]
        print(f"\nAnalyzing sample funnel ({', '.join(str(window) for window in windows)} day windows)...")

        required_columns = {'order_num', 'customer_email', 'purchase_date', 'item_label'}
        if not required_columns.issubset(df.columns):
            print("  Sample funnel skipped: required columns are missing.")
            return {'summary': {}, 'window_conversion': pd.DataFrame(), 'entry_product_conversion': pd.DataFrame()}

        export_df = df.copy()
        export_df['purchase_datetime'] = pd.to_datetime(export_df['purchase_date'], errors='coerce')
        export_df = export_df.dropna(subset=['purchase_datetime'])
        export_df['customer_email'] = export_df['customer_email'].astype(str).str.strip().str.lower()
        export_df = export_df[
            export_df['customer_email'].notna() &
            export_df['customer_email'].ne('') &
            export_df['customer_email'].ne('nan')
        ].copy()

        if export_df.empty:
            print("  Sample funnel skipped: no eligible customer journeys after cleanup.")
            return {'summary': {}, 'window_conversion': pd.DataFrame(), 'entry_product_conversion': pd.DataFrame()}

        revenue_candidates = [
            'order_revenue_net',
            'order_total',
            'item_total_revenue_net',
            'item_total_without_tax',
            'item_total_revenue',
        ]
        revenue_col = next((column for column in revenue_candidates if column in export_df.columns), None)

        item_flags = export_df[['customer_email', 'order_num', 'purchase_datetime', 'item_label', 'product_sku']].copy()
        item_flags['is_sample'] = item_flags['item_label'].apply(self._is_sample_item_label)
        item_flags['size_bucket'] = item_flags['item_label'].apply(self._fullsize_size_bucket)
        item_flags['is_fullsize'] = item_flags['size_bucket'].notna()

        order_flags = item_flags.groupby(['customer_email', 'order_num'], as_index=False).agg(
            purchase_datetime=('purchase_datetime', 'min'),
            contains_sample=('is_sample', 'any'),
            contains_fullsize=('is_fullsize', 'any'),
            contains_200=('size_bucket', lambda values: any(value == '200ml' for value in values)),
            contains_500=('size_bucket', lambda values: any(value == '500ml' for value in values)),
        )

        orders_df = order_flags.copy()
        if revenue_col is not None:
            order_values = export_df[['customer_email', 'order_num', 'purchase_datetime', revenue_col]].drop_duplicates(
                subset=['customer_email', 'order_num']
            ).copy()
            order_values = order_values.rename(columns={revenue_col: 'order_revenue'})
            orders_df = orders_df.merge(
                order_values[['customer_email', 'order_num', 'order_revenue']],
                on=['customer_email', 'order_num'],
                how='left'
            )
        else:
            orders_df['order_revenue'] = 0.0
        orders_df['order_revenue'] = orders_df['order_revenue'].fillna(0.0)
        orders_df = orders_df.sort_values(['customer_email', 'purchase_datetime', 'order_num'])

        first_orders = orders_df.groupby('customer_email', as_index=False).first()
        sample_entry = first_orders[
            first_orders['contains_sample'] &
            (~first_orders['contains_fullsize'])
        ].copy()

        if sample_entry.empty:
            print("  Sample funnel result: no first-order sample-entry cohort found.")
            return {'summary': {}, 'window_conversion': pd.DataFrame(), 'entry_product_conversion': pd.DataFrame()}

        sample_entry = sample_entry.rename(columns={
            'order_num': 'entry_order_num',
            'purchase_datetime': 'entry_purchase_datetime',
            'order_revenue': 'entry_order_revenue',
        })

        sample_entry_customers = sample_entry['customer_email'].tolist()
        later_orders = orders_df.merge(
            sample_entry[['customer_email', 'entry_order_num', 'entry_purchase_datetime']],
            on='customer_email',
            how='inner'
        )
        later_orders = later_orders[
            (later_orders['purchase_datetime'] > later_orders['entry_purchase_datetime']) |
            (
                (later_orders['purchase_datetime'] == later_orders['entry_purchase_datetime']) &
                (later_orders['order_num'] != later_orders['entry_order_num'])
            )
        ].copy()
        later_orders['days_since_entry'] = (
            later_orders['purchase_datetime'] - later_orders['entry_purchase_datetime']
        ).dt.total_seconds() / 86400.0

        repeat_days = later_orders.groupby('customer_email')['days_since_entry'].min().rename('days_to_repeat')
        fullsize_days = later_orders[later_orders['contains_fullsize']].groupby('customer_email')['days_since_entry'].min().rename('days_to_any_fullsize')
        fullsize_200_days = later_orders[later_orders['contains_200']].groupby('customer_email')['days_since_entry'].min().rename('days_to_200ml')
        fullsize_500_days = later_orders[later_orders['contains_500']].groupby('customer_email')['days_since_entry'].min().rename('days_to_500ml')

        cohort_df = sample_entry[['customer_email', 'entry_order_num', 'entry_purchase_datetime', 'entry_order_revenue']].copy()
        cohort_df = cohort_df.merge(repeat_days, on='customer_email', how='left')
        cohort_df = cohort_df.merge(fullsize_days, on='customer_email', how='left')
        cohort_df = cohort_df.merge(fullsize_200_days, on='customer_email', how='left')
        cohort_df = cohort_df.merge(fullsize_500_days, on='customer_email', how='left')

        window_rows = []
        cohort_size = len(cohort_df)
        for window in windows:
            repeat_customers = int((cohort_df['days_to_repeat'].fillna(window + 1) <= window).sum())
            any_fullsize_customers = int((cohort_df['days_to_any_fullsize'].fillna(window + 1) <= window).sum())
            fullsize_200_customers = int((cohort_df['days_to_200ml'].fillna(window + 1) <= window).sum())
            fullsize_500_customers = int((cohort_df['days_to_500ml'].fillna(window + 1) <= window).sum())
            window_rows.append({
                'window_days': window,
                'cohort_customers': cohort_size,
                'repeat_customers': repeat_customers,
                'repeat_pct': round(repeat_customers / cohort_size * 100, 1) if cohort_size > 0 else 0.0,
                'fullsize_any_customers': any_fullsize_customers,
                'fullsize_any_pct': round(any_fullsize_customers / cohort_size * 100, 1) if cohort_size > 0 else 0.0,
                'fullsize_200_customers': fullsize_200_customers,
                'fullsize_200_pct': round(fullsize_200_customers / cohort_size * 100, 1) if cohort_size > 0 else 0.0,
                'fullsize_500_customers': fullsize_500_customers,
                'fullsize_500_pct': round(fullsize_500_customers / cohort_size * 100, 1) if cohort_size > 0 else 0.0,
            })
        window_conversion = pd.DataFrame(window_rows)

        entry_sample_items = item_flags.merge(
            sample_entry[['customer_email', 'entry_order_num']],
            left_on=['customer_email', 'order_num'],
            right_on=['customer_email', 'entry_order_num'],
            how='inner'
        )
        entry_sample_items = entry_sample_items[entry_sample_items['is_sample']].copy()

        entry_rows = []
        for (item_name, item_sku), group in entry_sample_items.groupby(['item_label', 'product_sku']):
            customers = group['customer_email'].dropna().unique().tolist()
            item_cohort = cohort_df[cohort_df['customer_email'].isin(customers)]
            customer_count = len(item_cohort)
            if customer_count == 0:
                continue
            row = {
                'item_name': item_name,
                'item_sku': item_sku,
                'entry_customers': customer_count,
            }
            for window in (30, 60, 90):
                row[f'repeat_{window}d_pct'] = round(
                    (item_cohort['days_to_repeat'].fillna(window + 1) <= window).sum() / customer_count * 100, 1
                )
                row[f'fullsize_any_{window}d_pct'] = round(
                    (item_cohort['days_to_any_fullsize'].fillna(window + 1) <= window).sum() / customer_count * 100, 1
                )
                row[f'fullsize_200_{window}d_pct'] = round(
                    (item_cohort['days_to_200ml'].fillna(window + 1) <= window).sum() / customer_count * 100, 1
                )
                row[f'fullsize_500_{window}d_pct'] = round(
                    (item_cohort['days_to_500ml'].fillna(window + 1) <= window).sum() / customer_count * 100, 1
                )
            entry_rows.append(row)

        entry_product_conversion = pd.DataFrame(entry_rows)
        if not entry_product_conversion.empty:
            entry_product_conversion = entry_product_conversion.sort_values(
                ['entry_customers', 'fullsize_any_60d_pct'],
                ascending=[False, False]
            ).head(top_n).reset_index(drop=True)

        summary = {
            'entry_customers': cohort_size,
            'entry_orders': int(sample_entry['entry_order_num'].nunique()),
            'entry_revenue': round(float(sample_entry['entry_order_revenue'].sum() or 0.0), 2),
            'sample_first_order_share_pct': round(cohort_size / max(first_orders['customer_email'].nunique(), 1) * 100, 1),
            'avg_entry_order_value': round(float(sample_entry['entry_order_revenue'].mean() or 0.0), 2),
            'repeat_30d_pct': round(
                (cohort_df['days_to_repeat'].fillna(31) <= 30).sum() / cohort_size * 100, 1
            ) if cohort_size > 0 else 0.0,
            'fullsize_any_30d_pct': round(
                (cohort_df['days_to_any_fullsize'].fillna(31) <= 30).sum() / cohort_size * 100, 1
            ) if cohort_size > 0 else 0.0,
            'fullsize_any_60d_pct': round(
                (cohort_df['days_to_any_fullsize'].fillna(61) <= 60).sum() / cohort_size * 100, 1
            ) if cohort_size > 0 else 0.0,
            'fullsize_200_60d_pct': round(
                (cohort_df['days_to_200ml'].fillna(61) <= 60).sum() / cohort_size * 100, 1
            ) if cohort_size > 0 else 0.0,
            'fullsize_500_60d_pct': round(
                (cohort_df['days_to_500ml'].fillna(61) <= 60).sum() / cohort_size * 100, 1
            ) if cohort_size > 0 else 0.0,
            'median_days_to_fullsize': round(float(cohort_df['days_to_any_fullsize'].median()), 1) if cohort_df['days_to_any_fullsize'].notna().any() else None,
            'median_days_to_repeat': round(float(cohort_df['days_to_repeat'].median()), 1) if cohort_df['days_to_repeat'].notna().any() else None,
            'top_entry_product': entry_product_conversion.iloc[0]['item_name'] if not entry_product_conversion.empty else None,
        }

        window_filename = self.output_path(
            f"sample_funnel_windows_{df['purchase_datetime'].min().strftime('%Y%m%d')}-{df['purchase_datetime'].max().strftime('%Y%m%d')}.csv"
        )
        window_conversion.to_csv(window_filename, index=False, encoding='utf-8-sig')
        if not entry_product_conversion.empty:
            product_filename = self.output_path(
                f"sample_funnel_products_{df['purchase_datetime'].min().strftime('%Y%m%d')}-{df['purchase_datetime'].max().strftime('%Y%m%d')}.csv"
            )
            entry_product_conversion.to_csv(product_filename, index=False, encoding='utf-8-sig')
            print(f"  Sample funnel products saved: {product_filename}")
        print(f"  Sample funnel windows saved: {window_filename}")
        print(
            "  Sample funnel summary: "
            f"entry_customers={summary['entry_customers']}, "
            f"fullsize_any_30d={summary['fullsize_any_30d_pct']}%, "
            f"fullsize_any_60d={summary['fullsize_any_60d_pct']}%"
        )

        return {
            'summary': summary,
            'window_conversion': window_conversion,
            'entry_product_conversion': entry_product_conversion,
        }

    def analyze_retention_by_first_order_item(self, df: pd.DataFrame, min_first_orders: int = 50, top_n: int = 20) -> dict:
        """
        Analyze customer retention based on items in their first order.
        For top N items that appear in first orders (min occurrences), calculate:
        - Total customers whose first order contained this item
        - How many made 2nd, 3rd, 4th, 5th order
        - Retention percentages

        Args:
            df: DataFrame with order data
            min_first_orders: Minimum number of first orders containing the item to include in analysis
            top_n: Number of top items to analyze (by first order occurrence count)

        Returns:
            dict with 'item_retention' DataFrame and 'summary' dict
        """
        print(f"\nAnalyzing retention by first order item (top {top_n} items, min {min_first_orders} first orders)...")

        # Ensure datetime column exists
        df['purchase_datetime'] = pd.to_datetime(df['purchase_date'])

        # Get unique orders with customer info
        orders_df = df[['order_num', 'customer_email', 'purchase_datetime', 'item_label', 'product_sku']].copy()
        orders_df = orders_df.sort_values(['customer_email', 'purchase_datetime'])

        # Get first order for each customer
        first_order_per_customer = orders_df.groupby('customer_email')['order_num'].first().reset_index()
        first_order_per_customer.columns = ['customer_email', 'first_order_num']

        # Get items in first orders
        first_order_items = orders_df.merge(
            first_order_per_customer,
            left_on=['customer_email', 'order_num'],
            right_on=['customer_email', 'first_order_num']
        )[['customer_email', 'item_label', 'product_sku']]

        # Count how many first orders contain each item
        item_first_order_counts = first_order_items.groupby(['item_label', 'product_sku'])['customer_email'].nunique().reset_index()
        item_first_order_counts.columns = ['item_name', 'item_sku', 'first_order_count']

        # Filter items with minimum first orders and get top N
        qualified_items = item_first_order_counts[item_first_order_counts['first_order_count'] >= min_first_orders]
        qualified_items = qualified_items.nlargest(top_n, 'first_order_count')

        if qualified_items.empty:
            print(f"  No items found with {min_first_orders}+ first order occurrences")
            return {'item_retention': pd.DataFrame(), 'summary': {}}

        # Calculate total orders per customer
        orders_per_customer = orders_df.groupby('customer_email')['order_num'].nunique().reset_index()
        orders_per_customer.columns = ['customer_email', 'total_orders']

        # Calculate retention for each qualified item
        item_retention_data = []

        for _, item_row in qualified_items.iterrows():
            item_name = item_row['item_name']
            item_sku = item_row['item_sku']

            # Get customers whose first order contained this item
            customers_with_item = first_order_items[
                (first_order_items['item_label'] == item_name) &
                (first_order_items['product_sku'] == item_sku)
            ]['customer_email'].unique()

            # Get order counts for these customers
            customer_orders = orders_per_customer[orders_per_customer['customer_email'].isin(customers_with_item)]
            total_customers = len(customer_orders)

            if total_customers == 0:
                continue

            # Calculate retention rates
            made_2nd = len(customer_orders[customer_orders['total_orders'] >= 2])
            made_3rd = len(customer_orders[customer_orders['total_orders'] >= 3])
            made_4th = len(customer_orders[customer_orders['total_orders'] >= 4])
            made_5th = len(customer_orders[customer_orders['total_orders'] >= 5])

            item_retention_data.append({
                'item_name': item_name,
                'item_sku': item_sku,
                'first_order_customers': total_customers,
                'made_2nd_order': made_2nd,
                'made_3rd_order': made_3rd,
                'made_4th_order': made_4th,
                'made_5th_order': made_5th,
                'retention_2nd_pct': round(made_2nd / total_customers * 100, 1),
                'retention_3rd_pct': round(made_3rd / total_customers * 100, 1),
                'retention_4th_pct': round(made_4th / total_customers * 100, 1),
                'retention_5th_pct': round(made_5th / total_customers * 100, 1),
                'avg_orders_per_customer': round(customer_orders['total_orders'].mean(), 2)
            })

        item_retention_df = pd.DataFrame(item_retention_data)

        # Sort by retention rate (2nd order) descending
        if not item_retention_df.empty:
            item_retention_df = item_retention_df.sort_values('retention_2nd_pct', ascending=False).reset_index(drop=True)

        # Calculate summary statistics
        summary = {}
        if not item_retention_df.empty:
            summary = {
                'total_items_analyzed': len(item_retention_df),
                'avg_retention_2nd_pct': round(item_retention_df['retention_2nd_pct'].mean(), 1),
                'avg_retention_3rd_pct': round(item_retention_df['retention_3rd_pct'].mean(), 1),
                'best_retention_item': item_retention_df.iloc[0]['item_name'] if len(item_retention_df) > 0 else None,
                'best_retention_2nd_pct': item_retention_df.iloc[0]['retention_2nd_pct'] if len(item_retention_df) > 0 else 0,
                'worst_retention_item': item_retention_df.iloc[-1]['item_name'] if len(item_retention_df) > 0 else None,
                'worst_retention_2nd_pct': item_retention_df.iloc[-1]['retention_2nd_pct'] if len(item_retention_df) > 0 else 0,
                'retention_spread': round(item_retention_df['retention_2nd_pct'].max() - item_retention_df['retention_2nd_pct'].min(), 1) if len(item_retention_df) > 0 else 0
            }

        print(f"  First order item retention analysis complete:")
        print(f"    - Items analyzed: {len(item_retention_df)}")
        if summary:
            print(f"    - Avg 2nd order retention: {summary['avg_retention_2nd_pct']}%")
            print(f"    - Best item: {summary['best_retention_item'][:40]}... ({summary['best_retention_2nd_pct']}%)" if summary['best_retention_item'] else "")
            print(f"    - Retention spread: {summary['retention_spread']}%")

        return {
            'item_retention': item_retention_df,
            'summary': summary
        }

    def analyze_same_item_repurchase(self, df: pd.DataFrame, min_orders: int = 50, top_n: int = 20) -> dict:
        """
        Analyze retention based on same item repurchase across multiple orders.
        For each item, calculate how often customers who bought it buy it again.

        Args:
            df: DataFrame with order data
            min_orders: Minimum total orders containing the item to include in analysis
            top_n: Number of top items to analyze (by total order count)

        Returns:
            dict with:
            - 'item_repurchase': DataFrame with repurchase rates per item
            - 'customer_item_frequency': DataFrame with customer-item purchase frequency distribution
            - 'summary': Summary statistics
        """
        print(f"\nAnalyzing same item repurchase retention (top {top_n} items, min {min_orders} orders)...")

        # Ensure datetime column exists
        df['purchase_datetime'] = pd.to_datetime(df['purchase_date'])

        # Get item purchases with customer info
        item_purchases = df[['order_num', 'customer_email', 'purchase_datetime', 'item_label', 'product_sku', 'item_quantity']].copy()
        item_purchases = item_purchases.rename(columns={'item_label': 'item_name', 'product_sku': 'item_sku', 'item_quantity': 'quantity'})

        # Count total orders per item (unique orders, not quantity)
        item_order_counts = item_purchases.groupby(['item_name', 'item_sku'])['order_num'].nunique().reset_index()
        item_order_counts.columns = ['item_name', 'item_sku', 'total_orders']

        # Filter items with minimum orders and get top N
        qualified_items = item_order_counts[item_order_counts['total_orders'] >= min_orders]
        qualified_items = qualified_items.nlargest(top_n, 'total_orders')

        if qualified_items.empty:
            print(f"  No items found with {min_orders}+ orders")
            return {'item_repurchase': pd.DataFrame(), 'customer_item_frequency': pd.DataFrame(), 'summary': {}}

        # Calculate repurchase metrics for each qualified item
        item_repurchase_data = []

        for _, item_row in qualified_items.iterrows():
            item_name = item_row['item_name']
            item_sku = item_row['item_sku']

            # Get all purchases of this item
            item_orders = item_purchases[
                (item_purchases['item_name'] == item_name) &
                (item_purchases['item_sku'] == item_sku)
            ]

            # Count unique orders per customer for this item
            customer_item_orders = item_orders.groupby('customer_email')['order_num'].nunique().reset_index()
            customer_item_orders.columns = ['customer_email', 'item_order_count']

            total_customers = len(customer_item_orders)
            if total_customers == 0:
                continue

            # Calculate repurchase rates
            bought_2_times = len(customer_item_orders[customer_item_orders['item_order_count'] >= 2])
            bought_3_times = len(customer_item_orders[customer_item_orders['item_order_count'] >= 3])
            bought_4_times = len(customer_item_orders[customer_item_orders['item_order_count'] >= 4])
            bought_5_times = len(customer_item_orders[customer_item_orders['item_order_count'] >= 5])

            # Calculate average time between repurchases for this item
            repeat_purchasers = customer_item_orders[customer_item_orders['item_order_count'] >= 2]['customer_email'].unique()
            avg_days_between = None

            if len(repeat_purchasers) > 0:
                days_between_list = []
                for customer in repeat_purchasers:
                    customer_item_dates = item_orders[item_orders['customer_email'] == customer].sort_values('purchase_datetime')['purchase_datetime'].values
                    for i in range(1, len(customer_item_dates)):
                        days = (customer_item_dates[i] - customer_item_dates[i-1]) / pd.Timedelta(days=1)
                        days_between_list.append(days)
                if days_between_list:
                    avg_days_between = round(np.mean(days_between_list), 1)

            item_repurchase_data.append({
                'item_name': item_name,
                'item_sku': item_sku,
                'total_orders': item_row['total_orders'],
                'unique_customers': total_customers,
                'bought_2_times': bought_2_times,
                'bought_3_times': bought_3_times,
                'bought_4_times': bought_4_times,
                'bought_5_times': bought_5_times,
                'repurchase_2x_pct': round(bought_2_times / total_customers * 100, 1),
                'repurchase_3x_pct': round(bought_3_times / total_customers * 100, 1),
                'repurchase_4x_pct': round(bought_4_times / total_customers * 100, 1),
                'repurchase_5x_pct': round(bought_5_times / total_customers * 100, 1),
                'avg_purchases_per_customer': round(customer_item_orders['item_order_count'].mean(), 2),
                'avg_days_between_repurchase': avg_days_between
            })

        item_repurchase_df = pd.DataFrame(item_repurchase_data)

        # Sort by repurchase rate (2x) descending
        if not item_repurchase_df.empty:
            item_repurchase_df = item_repurchase_df.sort_values('repurchase_2x_pct', ascending=False).reset_index(drop=True)

        # Create frequency distribution of customer-item purchases
        all_customer_item_orders = []
        for _, item_row in qualified_items.iterrows():
            item_orders = item_purchases[
                (item_purchases['item_name'] == item_row['item_name']) &
                (item_purchases['item_sku'] == item_row['item_sku'])
            ]
            customer_counts = item_orders.groupby('customer_email')['order_num'].nunique()
            all_customer_item_orders.extend(customer_counts.tolist())

        frequency_dist = []
        if all_customer_item_orders:
            from collections import Counter
            count_dist = Counter(all_customer_item_orders)
            for freq in sorted(count_dist.keys())[:10]:  # Limit to first 10 frequencies
                label = f'{freq}x' if freq < 10 else '10+x'
                frequency_dist.append({
                    'purchase_frequency': label,
                    'customer_count': count_dist[freq],
                    'percentage': round(count_dist[freq] / len(all_customer_item_orders) * 100, 1)
                })

        customer_item_freq_df = pd.DataFrame(frequency_dist)

        # Calculate summary statistics
        summary = {}
        if not item_repurchase_df.empty:
            summary = {
                'total_items_analyzed': len(item_repurchase_df),
                'avg_repurchase_2x_pct': round(item_repurchase_df['repurchase_2x_pct'].mean(), 1),
                'avg_repurchase_3x_pct': round(item_repurchase_df['repurchase_3x_pct'].mean(), 1),
                'best_repurchase_item': item_repurchase_df.iloc[0]['item_name'] if len(item_repurchase_df) > 0 else None,
                'best_repurchase_2x_pct': item_repurchase_df.iloc[0]['repurchase_2x_pct'] if len(item_repurchase_df) > 0 else 0,
                'worst_repurchase_item': item_repurchase_df.iloc[-1]['item_name'] if len(item_repurchase_df) > 0 else None,
                'worst_repurchase_2x_pct': item_repurchase_df.iloc[-1]['repurchase_2x_pct'] if len(item_repurchase_df) > 0 else 0,
                'avg_days_between_repurchase': round(
                    item_repurchase_df[item_repurchase_df['avg_days_between_repurchase'].notna()]['avg_days_between_repurchase'].mean(), 1
                ) if item_repurchase_df['avg_days_between_repurchase'].notna().any() else None,
                'repurchase_spread': round(item_repurchase_df['repurchase_2x_pct'].max() - item_repurchase_df['repurchase_2x_pct'].min(), 1) if len(item_repurchase_df) > 0 else 0
            }

        print(f"  Same item repurchase analysis complete:")
        print(f"    - Items analyzed: {len(item_repurchase_df)}")
        if summary:
            print(f"    - Avg 2x repurchase rate: {summary['avg_repurchase_2x_pct']}%")
            print(f"    - Best item: {summary['best_repurchase_item'][:40]}... ({summary['best_repurchase_2x_pct']}%)" if summary['best_repurchase_item'] else "")
            print(f"    - Avg days between repurchase: {summary['avg_days_between_repurchase']}")

        return {
            'item_repurchase': item_repurchase_df,
            'customer_item_frequency': customer_item_freq_df,
            'summary': summary
        }

    def analyze_time_to_nth_by_first_item(self, df: pd.DataFrame, min_first_orders: int = 50, top_n: int = 20) -> dict:
        """
        Analyze time to nth order based on items in customer's first order.
        For top N items that appear in first orders, calculate:
        - Average/median days to 2nd, 3rd, 4th, 5th order
        - Comparison of timing between different first-order items

        Args:
            df: DataFrame with order data
            min_first_orders: Minimum number of first orders containing the item
            top_n: Number of top items to analyze

        Returns:
            dict with 'time_to_nth_by_item' DataFrame and 'summary' dict
        """
        print(f"\nAnalyzing time to nth order by first order item (top {top_n} items, min {min_first_orders} first orders)...")

        # Ensure datetime column exists
        df['purchase_datetime'] = pd.to_datetime(df['purchase_date'])

        # Get unique orders with customer info
        orders_df = df[['order_num', 'customer_email', 'purchase_datetime', 'item_label', 'product_sku']].copy()
        orders_df = orders_df.sort_values(['customer_email', 'purchase_datetime'])

        # Get first order for each customer
        first_order_per_customer = orders_df.groupby('customer_email').agg({
            'order_num': 'first',
            'purchase_datetime': 'first'
        }).reset_index()
        first_order_per_customer.columns = ['customer_email', 'first_order_num', 'first_order_date']

        # Get items in first orders
        first_order_items = orders_df.merge(
            first_order_per_customer,
            left_on=['customer_email', 'order_num'],
            right_on=['customer_email', 'first_order_num']
        )[['customer_email', 'item_label', 'product_sku', 'first_order_date']]

        # Count how many first orders contain each item
        item_first_order_counts = first_order_items.groupby(['item_label', 'product_sku'])['customer_email'].nunique().reset_index()
        item_first_order_counts.columns = ['item_name', 'item_sku', 'first_order_count']

        # Filter items with minimum first orders and get top N
        qualified_items = item_first_order_counts[item_first_order_counts['first_order_count'] >= min_first_orders]
        qualified_items = qualified_items.nlargest(top_n, 'first_order_count')

        if qualified_items.empty:
            print(f"  No items found with {min_first_orders}+ first order occurrences")
            return {'time_to_nth_by_item': pd.DataFrame(), 'summary': {}}

        # Get all orders per customer with order numbers
        all_orders = orders_df.drop_duplicates(subset=['customer_email', 'order_num']).copy()
        all_orders = all_orders.sort_values(['customer_email', 'purchase_datetime'])
        all_orders['customer_order_num'] = all_orders.groupby('customer_email').cumcount() + 1

        # Merge with first order date
        all_orders = all_orders.merge(
            first_order_per_customer[['customer_email', 'first_order_date']],
            on='customer_email'
        )
        all_orders['days_since_first_order'] = (all_orders['purchase_datetime'] - all_orders['first_order_date']).dt.days

        # Calculate time to nth order for each qualified item
        time_to_nth_data = []

        for _, item_row in qualified_items.iterrows():
            item_name = item_row['item_name']
            item_sku = item_row['item_sku']

            # Get customers whose first order contained this item
            customers_with_item = first_order_items[
                (first_order_items['item_label'] == item_name) &
                (first_order_items['product_sku'] == item_sku)
            ]['customer_email'].unique()

            # Get orders for these customers
            customer_orders = all_orders[all_orders['customer_email'].isin(customers_with_item)]

            item_data = {
                'item_name': item_name,
                'item_sku': item_sku,
                'first_order_customers': len(customers_with_item)
            }

            # Calculate time to nth order for orders 2-5
            for order_num in range(2, 6):
                nth_orders = customer_orders[customer_orders['customer_order_num'] == order_num]
                if len(nth_orders) > 0:
                    item_data[f'customers_{order_num}nd'] = len(nth_orders)
                    item_data[f'avg_days_to_{order_num}nd'] = round(nth_orders['days_since_first_order'].mean(), 1)
                    item_data[f'median_days_to_{order_num}nd'] = round(nth_orders['days_since_first_order'].median(), 1)
                else:
                    item_data[f'customers_{order_num}nd'] = 0
                    item_data[f'avg_days_to_{order_num}nd'] = None
                    item_data[f'median_days_to_{order_num}nd'] = None

            time_to_nth_data.append(item_data)

        time_to_nth_df = pd.DataFrame(time_to_nth_data)

        # Sort by avg days to 2nd order (ascending - faster return is better)
        if not time_to_nth_df.empty:
            time_to_nth_df = time_to_nth_df.sort_values('avg_days_to_2nd', ascending=True, na_position='last').reset_index(drop=True)

        # Calculate summary statistics
        summary = {}
        if not time_to_nth_df.empty:
            valid_2nd = time_to_nth_df[time_to_nth_df['avg_days_to_2nd'].notna()]
            if len(valid_2nd) > 0:
                summary = {
                    'total_items_analyzed': len(time_to_nth_df),
                    'avg_days_to_2nd_overall': round(valid_2nd['avg_days_to_2nd'].mean(), 1),
                    'fastest_return_item': valid_2nd.iloc[0]['item_name'] if len(valid_2nd) > 0 else None,
                    'fastest_return_days': valid_2nd.iloc[0]['avg_days_to_2nd'] if len(valid_2nd) > 0 else None,
                    'slowest_return_item': valid_2nd.iloc[-1]['item_name'] if len(valid_2nd) > 0 else None,
                    'slowest_return_days': valid_2nd.iloc[-1]['avg_days_to_2nd'] if len(valid_2nd) > 0 else None,
                    'days_spread': round(valid_2nd['avg_days_to_2nd'].max() - valid_2nd['avg_days_to_2nd'].min(), 1)
                }

        print(f"  Time to nth order by first item analysis complete:")
        print(f"    - Items analyzed: {len(time_to_nth_df)}")
        if summary:
            print(f"    - Avg days to 2nd order: {summary.get('avg_days_to_2nd_overall', 'N/A')}")
            if summary.get('fastest_return_item'):
                print(f"    - Fastest return: {summary['fastest_return_item'][:40]}... ({summary['fastest_return_days']} days)")
            print(f"    - Days spread: {summary.get('days_spread', 'N/A')}")

        return {
            'time_to_nth_by_item': time_to_nth_df,
            'summary': summary
        }

    def calculate_clv_and_return_time(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate Customer Lifetime Value and average return time"""
        print("\nCalculating CLV and customer return time...")
        revenue_col = 'order_revenue_net' if 'order_revenue_net' in df.columns else 'order_total'
        
        # Convert purchase_date to datetime
        df['purchase_datetime'] = pd.to_datetime(df['purchase_date'])
        df['purchase_date_only'] = df['purchase_datetime'].dt.date
        df['year_week'] = df['purchase_datetime'].dt.to_period('W')

        # One row per day spend table to avoid multiplying daily ad spend by order count
        daily_spend_df = df.groupby('purchase_date_only').agg({
            'fb_ads_daily_spend': 'first'
        }).reset_index()
        daily_spend_df['year_week'] = pd.to_datetime(daily_spend_df['purchase_date_only']).dt.to_period('W')
        weekly_fb_spend_map = daily_spend_df.groupby('year_week')['fb_ads_daily_spend'].sum().to_dict()
        
        # Get unique orders with customer info and FB ads spend
        orders_df = df[['order_num', 'customer_email', 'purchase_datetime', 'year_week', revenue_col, 'fb_ads_daily_spend']].drop_duplicates(subset=['order_num'])
        orders_df = orders_df.sort_values(['customer_email', 'purchase_datetime'])
        
        # Calculate CLV per customer (total revenue from customer)
        customer_clv = orders_df.groupby('customer_email')[revenue_col].sum().to_dict()
        
        # Calculate return times (days between orders)
        customer_return_times = {}
        customer_orders_count = {}
        
        for customer_email in orders_df['customer_email'].unique():
            customer_orders = orders_df[orders_df['customer_email'] == customer_email].sort_values('purchase_datetime')
            customer_orders_count[customer_email] = len(customer_orders)
            
            if len(customer_orders) > 1:
                # Calculate days between consecutive orders
                purchase_dates = customer_orders['purchase_datetime'].values
                return_times = []
                for i in range(1, len(purchase_dates)):
                    days_between = (purchase_dates[i] - purchase_dates[i-1]) / pd.Timedelta(days=1)
                    return_times.append(days_between)
                customer_return_times[customer_email] = np.mean(return_times) if return_times else None
            else:
                customer_return_times[customer_email] = None
        
        # Calculate weekly aggregations
        weekly_clv_stats = []
        
        for week in orders_df['year_week'].unique():
            week_orders = orders_df[orders_df['year_week'] == week]
            week_customers = week_orders['customer_email'].unique()
            
            # Calculate average CLV for customers who ordered this week
            week_clvs = [customer_clv[c] for c in week_customers]
            avg_clv = np.mean(week_clvs) if week_clvs else 0
            
            # Calculate average return time for returning customers this week
            week_return_times = []
            new_customers = 0
            returning_customers = 0
            
            for customer in week_customers:
                # Check if this is the customer's first order
                customer_first_order = orders_df[orders_df['customer_email'] == customer].iloc[0]
                if customer_first_order['year_week'] == week:
                    new_customers += 1
                else:
                    returning_customers += 1
                    if customer_return_times[customer] is not None:
                        week_return_times.append(customer_return_times[customer])
            
            avg_return_time = np.mean(week_return_times) if week_return_times else None
            
            # Calculate CAC (Customer Acquisition Cost) for the week
            # CAC = Total Marketing Costs / Number of New Customers
            week_fb_spend = weekly_fb_spend_map.get(week, 0)
            cac = round(week_fb_spend / new_customers, 2) if new_customers > 0 else 0
            
            # Calculate LTV/CAC ratio
            ltv_cac_ratio = round(avg_clv / cac, 2) if cac > 0 else 0
            
            weekly_clv_stats.append({
                'week': week,
                'week_start': week.start_time.date(),
                'unique_customers': len(week_customers),
                'new_customers': new_customers,
                'returning_customers': returning_customers,
                'avg_clv': round(avg_clv, 2),
                'avg_return_time_days': round(avg_return_time, 1) if avg_return_time else None,
                'total_revenue': week_orders[revenue_col].sum(),
                'fb_ads_spend': round(week_fb_spend, 2),
                'cac': cac,
                'ltv_cac_ratio': ltv_cac_ratio
            })
        
        # Convert to DataFrame
        weekly_clv_df = pd.DataFrame(weekly_clv_stats)
        weekly_clv_df = weekly_clv_df.sort_values('week')
        
        # Calculate cumulative CLV (how CLV is growing over time)
        all_customers_by_week = {}
        cumulative_clv = []
        
        for week in weekly_clv_df['week']:
            week_orders = orders_df[orders_df['year_week'] <= week]
            week_customer_clv = week_orders.groupby('customer_email')[revenue_col].sum()
            cumulative_clv.append(week_customer_clv.mean() if len(week_customer_clv) > 0 else 0)
        
        weekly_clv_df['cumulative_avg_clv'] = cumulative_clv
        weekly_clv_df['cumulative_avg_clv'] = weekly_clv_df['cumulative_avg_clv'].round(2)

        # Calculate cumulative CAC (running average of acquisition cost across all time)
        cumulative_cac = []
        cumulative_fb_spend = 0
        cumulative_new_customers = 0

        for idx, row in weekly_clv_df.iterrows():
            cumulative_fb_spend += row['fb_ads_spend']
            cumulative_new_customers += row['new_customers']
            avg_cac = cumulative_fb_spend / cumulative_new_customers if cumulative_new_customers > 0 else 0
            cumulative_cac.append(round(avg_cac, 2))

        weekly_clv_df['cumulative_avg_cac'] = cumulative_cac

        # Save to CSV
        filename = self.output_path(f"clv_return_time_analysis_{df['purchase_datetime'].min().strftime('%Y%m%d')}-{df['purchase_datetime'].max().strftime('%Y%m%d')}.csv")
        weekly_clv_df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"CLV and return time analysis saved: {filename}")
        
        return weekly_clv_df
    
    def analyze_order_size_distribution(self, df: pd.DataFrame) -> pd.DataFrame:
        """Analyze distribution of order sizes (number of items per order) grouped by day"""
        print("\nAnalyzing order size distribution...")

        # Convert purchase_date to datetime
        df['purchase_datetime'] = pd.to_datetime(df['purchase_date'])
        df['purchase_date_only'] = df['purchase_datetime'].dt.date

        # Get unique orders with their total items count per order
        orders_df = df[['order_num', 'purchase_date_only', 'total_items_in_order']].drop_duplicates(subset=['order_num'])

        # Define order size categories
        def categorize_order_size(items_count):
            if items_count == 1:
                return '1 item'
            elif items_count == 2:
                return '2 items'
            elif items_count == 3:
                return '3 items'
            elif items_count == 4:
                return '4 items'
            else:
                return '5+ items'

        orders_df['order_size_category'] = orders_df['total_items_in_order'].apply(categorize_order_size)

        # Group by date and order size category
        distribution = orders_df.groupby(['purchase_date_only', 'order_size_category']).size().reset_index(name='order_count')

        # Pivot to get categories as columns
        distribution_pivot = distribution.pivot(index='purchase_date_only', columns='order_size_category', values='order_count').fillna(0)

        # Ensure all categories are present (even if 0)
        for category in ['1 item', '2 items', '3 items', '4 items', '5+ items']:
            if category not in distribution_pivot.columns:
                distribution_pivot[category] = 0

        # Sort columns in order
        distribution_pivot = distribution_pivot[['1 item', '2 items', '3 items', '4 items', '5+ items']]

        # Reset index to get date as a column
        distribution_pivot = distribution_pivot.reset_index()

        # Sort by date
        distribution_pivot = distribution_pivot.sort_values('purchase_date_only')

        # Save to CSV
        filename = self.output_path(f"order_size_distribution_{df['purchase_datetime'].min().strftime('%Y%m%d')}-{df['purchase_datetime'].max().strftime('%Y%m%d')}.csv")
        distribution_pivot.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"Order size distribution saved: {filename}")

        return distribution_pivot

    def analyze_item_combinations(self, df: pd.DataFrame, min_count: int = 5) -> pd.DataFrame:
        """
        Analyze frequently ordered item combinations (grouped by product_sku).

        Args:
            df: DataFrame with order data (one row per item)
            min_count: Minimum number of times a combination must appear to be included

        Returns:
            DataFrame with combination analysis
        """
        from itertools import combinations
        from collections import Counter

        print("\nAnalyzing item combinations...")

        # Exclude items that should not be counted in combinations
        excluded_items = ['Tringelt']
        df_filtered = df[~df['item_label'].isin(excluded_items)].copy()
        print(f"Excluded {len(df) - len(df_filtered)} items ({', '.join(excluded_items)}) from combination analysis")

        # Create a mapping from product_sku to item_label for display
        sku_to_label = df_filtered.groupby('product_sku')['item_label'].first().to_dict()

        # Create a mapping from product_sku to average unit price
        sku_to_price = df_filtered.groupby('product_sku')['item_unit_price'].mean().to_dict()

        # Group items by order using product_sku
        order_items = df_filtered.groupby('order_num')['product_sku'].apply(lambda x: frozenset(x.unique())).reset_index()
        order_items.columns = ['order_num', 'items']

        # Filter to orders with 2 or more unique items
        multi_item_orders = order_items[order_items['items'].apply(len) >= 2]

        if multi_item_orders.empty:
            print("No orders with 2+ unique items found")
            return pd.DataFrame()

        print(f"Found {len(multi_item_orders)} orders with 2+ unique items")

        # Count all combinations (2, 3, 4, 5, etc.)
        combination_counts = Counter()

        for _, row in multi_item_orders.iterrows():
            items = sorted(row['items'])  # Sort for consistent ordering
            # Generate combinations of different sizes (2, 3, 4, 5, etc.)
            for combo_size in range(2, min(len(items) + 1, 6)):  # Up to 5 items in combo
                for combo in combinations(items, combo_size):
                    combination_counts[combo] += 1

        # Filter by minimum count
        filtered_combinations = {k: v for k, v in combination_counts.items() if v >= min_count}

        if not filtered_combinations:
            print(f"No combinations found with count >= {min_count}")
            return pd.DataFrame()

        # Create DataFrame
        combo_data = []
        for combo, count in filtered_combinations.items():
            # Convert product SKUs to labels for display
            combo_labels = [sku_to_label.get(sku, sku) for sku in combo]
            # Calculate total price for the combination
            combo_price = sum(sku_to_price.get(sku, 0) for sku in combo)
            combo_data.append({
                'combination_size': len(combo),
                'combination': '\n'.join(combo_labels),
                'combination_skus': '\n'.join(combo),
                'count': count,
                'price': round(combo_price, 2)
            })

        combo_df = pd.DataFrame(combo_data)
        combo_df = combo_df.sort_values('count', ascending=False)

        # Save to CSV
        filename = self.output_path(f"item_combinations_{df['purchase_datetime'].min().strftime('%Y%m%d')}-{df['purchase_datetime'].max().strftime('%Y%m%d')}.csv")
        combo_df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"Item combinations saved: {filename}")
        print(f"Found {len(combo_df)} combinations with count >= {min_count}")

        return combo_df

    def analyze_day_of_week(self, df: pd.DataFrame) -> pd.DataFrame:
        """Analyze orders and revenue by day of week"""
        print("\nAnalyzing day of week patterns...")

        df['day_of_week'] = pd.to_datetime(df['purchase_date']).dt.dayofweek
        df['day_name'] = pd.to_datetime(df['purchase_date']).dt.day_name()

        # Aggregate by day of week (using unique orders)
        orders_per_day = df.groupby(['day_of_week', 'day_name']).agg({
            'order_num': 'nunique',
            'item_total_without_tax': 'sum',
            'profit_before_ads': 'sum',
            'fb_ads_daily_spend': lambda x: x.drop_duplicates().sum(),
            'google_ads_daily_spend': lambda x: x.drop_duplicates().sum()
        }).reset_index()

        orders_per_day.columns = ['day_of_week', 'day_name', 'orders', 'revenue', 'profit', 'fb_spend', 'google_spend']
        orders_per_day = orders_per_day.sort_values('day_of_week')

        # Calculate averages and percentages
        total_orders = orders_per_day['orders'].sum()
        total_revenue = orders_per_day['revenue'].sum()
        orders_per_day['orders_pct'] = (orders_per_day['orders'] / total_orders * 100).round(1)
        orders_per_day['revenue_pct'] = (orders_per_day['revenue'] / total_revenue * 100).round(1)
        orders_per_day['aov'] = (orders_per_day['revenue'] / orders_per_day['orders']).round(2)

        print(f"Day of week analysis complete")
        return orders_per_day

    def analyze_week_of_month(self, df: pd.DataFrame) -> pd.DataFrame:
        """Analyze orders, revenue, and profitability by week-in-month using equal 4x7-day windows."""
        print("\nAnalyzing week-of-month patterns...")

        wom_df = df.copy()
        wom_df['purchase_datetime_wom'] = pd.to_datetime(wom_df['purchase_date'])
        wom_df['purchase_date_only'] = wom_df['purchase_datetime_wom'].dt.date
        wom_df['year_month'] = wom_df['purchase_datetime_wom'].dt.to_period('M').astype(str)
        wom_df['day_in_month'] = wom_df['purchase_datetime_wom'].dt.day
        wom_df['date_only_ts'] = wom_df['purchase_datetime_wom'].dt.normalize()

        # Use only days 1..28 => exact 4 equal 7-day buckets each month.
        wom_df = wom_df[wom_df['day_in_month'] <= 28].copy()

        # Use full months only (remove partial first/last month from selected range).
        min_date = wom_df['date_only_ts'].min()
        max_date = wom_df['date_only_ts'].max()
        if pd.isna(min_date) or pd.isna(max_date):
            base = pd.DataFrame({'week_of_month': [1, 2, 3, 4]})
            base['orders'] = 0
            base['revenue'] = 0.0
            base['profit'] = 0.0
            base['active_days'] = 0
            base['active_months'] = 0
            base['calendar_days'] = 0
            base['week_label'] = base['week_of_month'].apply(lambda w: f'Week {int(w)}')
            base['orders_pct'] = 0.0
            base['revenue_pct'] = 0.0
            base['aov'] = 0.0
            base['profit_margin_pct'] = 0.0
            base['avg_daily_revenue'] = 0.0
            base['avg_daily_profit'] = 0.0
            base['avg_orders_per_day'] = 0.0
            base['active_day_ratio_pct'] = 0.0
            print("Week-of-month analysis complete (empty dataset)")
            return base

        full_start = min_date if min_date.day == 1 else (min_date + pd.offsets.MonthBegin(1))
        max_month_last_day = (max_date + pd.offsets.MonthEnd(0)).day
        full_end = max_date if max_date.day == max_month_last_day else (max_date - pd.offsets.MonthEnd(1))

        has_full_month_window = full_start <= full_end
        if has_full_month_window:
            wom_df = wom_df[(wom_df['date_only_ts'] >= full_start) & (wom_df['date_only_ts'] <= full_end)].copy()
            calendar_df = pd.DataFrame({'date': pd.date_range(start=full_start, end=full_end, freq='D')})
        else:
            # Fallback for short ranges that don't contain any full month.
            calendar_df = pd.DataFrame({'date': pd.date_range(start=min_date, end=max_date, freq='D')})

        calendar_df = calendar_df[calendar_df['date'].dt.day <= 28].copy()

        wom_df['week_of_month'] = ((wom_df['day_in_month'] - 1) // 7) + 1
        calendar_df['week_of_month'] = ((calendar_df['date'].dt.day - 1) // 7) + 1

        wom_orders_agg = wom_df.groupby('week_of_month').agg({
            'order_num': 'nunique',
            'item_total_without_tax': 'sum',
            'profit_before_ads': 'sum',
            'purchase_date_only': 'nunique',
            'year_month': 'nunique'
        }).reset_index()

        wom_orders_agg.columns = [
            'week_of_month', 'orders', 'revenue', 'profit', 'active_days', 'active_months'
        ]

        calendar_days = calendar_df.groupby('week_of_month').agg({
            'date': 'nunique'
        }).reset_index().rename(columns={'date': 'calendar_days'})

        wom_agg = pd.DataFrame({'week_of_month': [1, 2, 3, 4]})
        wom_agg = wom_agg.merge(wom_orders_agg, on='week_of_month', how='left')
        wom_agg = wom_agg.merge(calendar_days, on='week_of_month', how='left')

        for col in ['orders', 'revenue', 'profit', 'active_days', 'active_months', 'calendar_days']:
            wom_agg[col] = wom_agg[col].fillna(0)
        wom_agg['orders'] = wom_agg['orders'].astype(int)
        wom_agg['active_days'] = wom_agg['active_days'].astype(int)
        wom_agg['active_months'] = wom_agg['active_months'].astype(int)
        wom_agg['calendar_days'] = wom_agg['calendar_days'].astype(int)
        wom_agg['week_label'] = wom_agg['week_of_month'].apply(lambda w: f'Week {int(w)}')

        total_orders = wom_agg['orders'].sum()
        total_revenue = wom_agg['revenue'].sum()

        wom_agg['orders_pct'] = (
            (wom_agg['orders'] / total_orders * 100).round(1) if total_orders > 0 else 0
        )
        wom_agg['revenue_pct'] = (
            (wom_agg['revenue'] / total_revenue * 100).round(1) if total_revenue > 0 else 0
        )
        wom_agg['aov'] = (wom_agg['revenue'] / wom_agg['orders']).replace(
            [float('inf'), float('-inf')], 0
        ).fillna(0).round(2)
        wom_agg['profit_margin_pct'] = ((wom_agg['profit'] / wom_agg['revenue']) * 100).replace(
            [float('inf'), float('-inf')], 0
        ).fillna(0).round(1)

        # Normalize by total calendar days in each month phase (not only active order days).
        wom_agg['avg_daily_revenue'] = (wom_agg['revenue'] / wom_agg['calendar_days']).replace(
            [float('inf'), float('-inf')], 0
        ).fillna(0).round(2)
        wom_agg['avg_daily_profit'] = (wom_agg['profit'] / wom_agg['calendar_days']).replace(
            [float('inf'), float('-inf')], 0
        ).fillna(0).round(2)
        wom_agg['avg_orders_per_day'] = (wom_agg['orders'] / wom_agg['calendar_days']).replace(
            [float('inf'), float('-inf')], 0
        ).fillna(0).round(2)
        wom_agg['active_day_ratio_pct'] = (wom_agg['active_days'] / wom_agg['calendar_days'] * 100).replace(
            [float('inf'), float('-inf')], 0
        ).fillna(0).round(1)

        wom_agg = wom_agg.sort_values('week_of_month').reset_index(drop=True)

        if has_full_month_window:
            print(
                f"Week-of-month analysis complete (full months window: {full_start.strftime('%Y-%m-%d')} to {full_end.strftime('%Y-%m-%d')})"
            )
        else:
            print("Week-of-month analysis complete (fallback: no full-month window in range)")
        return wom_agg

    def analyze_day_of_month(self, df: pd.DataFrame) -> pd.DataFrame:
        """Analyze orders, revenue, and profitability by day number within month (1-31)."""
        print("\nAnalyzing day-of-month patterns...")

        dom_df = df.copy()
        dom_df['purchase_datetime_dom'] = pd.to_datetime(dom_df['purchase_date'])
        dom_df['purchase_date_only'] = dom_df['purchase_datetime_dom'].dt.date
        dom_df['day_in_month'] = dom_df['purchase_datetime_dom'].dt.day
        dom_df['year_month'] = dom_df['purchase_datetime_dom'].dt.to_period('M').astype(str)
        dom_df['date_only_ts'] = dom_df['purchase_datetime_dom'].dt.normalize()

        min_date = dom_df['date_only_ts'].min()
        max_date = dom_df['date_only_ts'].max()
        if pd.isna(min_date) or pd.isna(max_date):
            base = pd.DataFrame({'day_in_month': list(range(1, 32))})
            base['orders'] = 0
            base['revenue'] = 0.0
            base['profit'] = 0.0
            base['active_days'] = 0
            base['calendar_days'] = 0
            base['day_label'] = base['day_in_month'].apply(lambda d: f"{int(d)}.")
            base['orders_pct'] = 0.0
            base['revenue_pct'] = 0.0
            base['aov'] = 0.0
            base['profit_margin_pct'] = 0.0
            base['avg_revenue_per_occurrence'] = 0.0
            base['avg_profit_per_occurrence'] = 0.0
            base['avg_orders_per_occurrence'] = 0.0
            base['active_day_ratio_pct'] = 0.0
            print("Day-of-month analysis complete (empty dataset)")
            return base

        # Use full months only for unbiased phase-of-month comparison.
        full_start = min_date if min_date.day == 1 else (min_date + pd.offsets.MonthBegin(1))
        max_month_last_day = (max_date + pd.offsets.MonthEnd(0)).day
        full_end = max_date if max_date.day == max_month_last_day else (max_date - pd.offsets.MonthEnd(1))

        has_full_month_window = full_start <= full_end
        if has_full_month_window:
            dom_df = dom_df[(dom_df['date_only_ts'] >= full_start) & (dom_df['date_only_ts'] <= full_end)].copy()
            calendar_df = pd.DataFrame({'date': pd.date_range(start=full_start, end=full_end, freq='D')})
        else:
            # Fallback for short ranges that don't contain any full month.
            calendar_df = pd.DataFrame({'date': pd.date_range(start=min_date, end=max_date, freq='D')})

        calendar_df['day_in_month'] = calendar_df['date'].dt.day

        dom_orders_agg = dom_df.groupby('day_in_month').agg({
            'order_num': 'nunique',
            'item_total_without_tax': 'sum',
            'profit_before_ads': 'sum',
            'purchase_date_only': 'nunique'
        }).reset_index()
        dom_orders_agg.columns = ['day_in_month', 'orders', 'revenue', 'profit', 'active_days']

        calendar_days = calendar_df.groupby('day_in_month').agg({
            'date': 'nunique'
        }).reset_index().rename(columns={'date': 'calendar_days'})

        dom_agg = pd.DataFrame({'day_in_month': list(range(1, 32))})
        dom_agg = dom_agg.merge(dom_orders_agg, on='day_in_month', how='left')
        dom_agg = dom_agg.merge(calendar_days, on='day_in_month', how='left')

        for col in ['orders', 'revenue', 'profit', 'active_days', 'calendar_days']:
            dom_agg[col] = dom_agg[col].fillna(0)
        dom_agg['orders'] = dom_agg['orders'].astype(int)
        dom_agg['active_days'] = dom_agg['active_days'].astype(int)
        dom_agg['calendar_days'] = dom_agg['calendar_days'].astype(int)
        dom_agg['day_label'] = dom_agg['day_in_month'].apply(lambda d: f"{int(d)}.")

        total_orders = dom_agg['orders'].sum()
        total_revenue = dom_agg['revenue'].sum()

        dom_agg['orders_pct'] = (
            (dom_agg['orders'] / total_orders * 100).round(1) if total_orders > 0 else 0
        )
        dom_agg['revenue_pct'] = (
            (dom_agg['revenue'] / total_revenue * 100).round(1) if total_revenue > 0 else 0
        )
        dom_agg['aov'] = (dom_agg['revenue'] / dom_agg['orders']).replace(
            [float('inf'), float('-inf')], 0
        ).fillna(0).round(2)
        dom_agg['profit_margin_pct'] = ((dom_agg['profit'] / dom_agg['revenue']) * 100).replace(
            [float('inf'), float('-inf')], 0
        ).fillna(0).round(1)

        # Fair phase comparison: normalize by number of calendar occurrences of each day.
        dom_agg['avg_revenue_per_occurrence'] = (dom_agg['revenue'] / dom_agg['calendar_days']).replace(
            [float('inf'), float('-inf')], 0
        ).fillna(0).round(2)
        dom_agg['avg_profit_per_occurrence'] = (dom_agg['profit'] / dom_agg['calendar_days']).replace(
            [float('inf'), float('-inf')], 0
        ).fillna(0).round(2)
        dom_agg['avg_orders_per_occurrence'] = (dom_agg['orders'] / dom_agg['calendar_days']).replace(
            [float('inf'), float('-inf')], 0
        ).fillna(0).round(2)
        dom_agg['active_day_ratio_pct'] = (dom_agg['active_days'] / dom_agg['calendar_days'] * 100).replace(
            [float('inf'), float('-inf')], 0
        ).fillna(0).round(1)

        dom_agg = dom_agg.sort_values('day_in_month').reset_index(drop=True)

        if has_full_month_window:
            print(
                f"Day-of-month analysis complete (full months window: {full_start.strftime('%Y-%m-%d')} to {full_end.strftime('%Y-%m-%d')})"
            )
        else:
            print("Day-of-month analysis complete (fallback: no full-month window in range)")
        return dom_agg

    def analyze_weather_impact(
        self,
        date_agg: pd.DataFrame,
        date_from: datetime,
        date_to: datetime
    ) -> Optional[dict]:
        """Analyze whether weather conditions correlate with daily business performance."""
        print("\nAnalyzing weather impact...")

        if not self.weather_client or not self.weather_settings.get("enabled"):
            print("Weather analysis skipped: weather integration disabled")
            return None

        try:
            weather_df = self.weather_client.get_daily_weather(
                date_from=date_from,
                date_to=date_to,
                locations=self.weather_settings.get("locations", []),
            )
        except Exception as exc:
            logger.warning(f"Weather analysis skipped due to fetch error: {exc}")
            return None

        if weather_df.empty:
            print("Weather analysis skipped: no weather data returned")
            return None

        weather_df = weather_df.copy()
        weather_df["date"] = pd.to_datetime(weather_df["date"]).dt.date

        analysis_df = date_agg.copy()
        analysis_df["date"] = pd.to_datetime(analysis_df["date"]).dt.date
        analysis_df["aov"] = analysis_df.apply(
            lambda row: round((row["total_revenue"] / row["unique_orders"]) if row["unique_orders"] > 0 else 0, 2),
            axis=1
        )
        analysis_df["weekday"] = pd.to_datetime(analysis_df["date"]).dt.day_name()

        weekday_baseline = analysis_df.groupby("weekday").agg({
            "total_revenue": "mean",
            "net_profit": "mean",
            "unique_orders": "mean",
            "aov": "mean",
        }).rename(columns={
            "total_revenue": "weekday_expected_revenue",
            "net_profit": "weekday_expected_profit",
            "unique_orders": "weekday_expected_orders",
            "aov": "weekday_expected_aov",
        }).reset_index()

        merged = analysis_df.merge(weather_df, on="date", how="left")
        merged = merged.merge(weekday_baseline, on="weekday", how="left")

        valid = merged[merged["temperature_2m_mean"].notna()].copy()
        if valid.empty:
            print("Weather analysis skipped: merged dataset has no valid weather rows")
            return None

        valid["weather_code"] = pd.to_numeric(valid["weather_code"], errors="coerce").fillna(0).astype(int)
        valid["precipitation_sum"] = pd.to_numeric(valid["precipitation_sum"], errors="coerce").fillna(0.0)
        valid["precipitation_hours"] = pd.to_numeric(valid["precipitation_hours"], errors="coerce").fillna(0.0)
        valid["wind_speed_10m_max"] = pd.to_numeric(valid["wind_speed_10m_max"], errors="coerce").fillna(0.0)

        valid["weather_bad_score"] = (
            (valid["precipitation_sum"] >= 1.0).astype(int) * 2
            + (valid["precipitation_hours"] >= 2.0).astype(int)
            + (valid["temperature_2m_mean"] <= 5.0).astype(int)
            + (valid["wind_speed_10m_max"] >= 25.0).astype(int)
            + (valid["weather_code"] >= 50).astype(int)
        )

        def classify_weather_bucket(row: pd.Series) -> str:
            if row["weather_bad_score"] >= 3:
                return "Bad"
            if (
                row["precipitation_sum"] <= 0.1
                and row["weather_code"] < 50
                and 10.0 <= row["temperature_2m_mean"] <= 25.0
                and row["wind_speed_10m_max"] < 20.0
            ):
                return "Good"
            return "Neutral"

        valid["weather_bucket"] = valid.apply(classify_weather_bucket, axis=1)
        valid["revenue_vs_weekday_baseline"] = valid["total_revenue"] - valid["weekday_expected_revenue"]
        valid["profit_vs_weekday_baseline"] = valid["net_profit"] - valid["weekday_expected_profit"]
        valid["orders_vs_weekday_baseline"] = valid["unique_orders"] - valid["weekday_expected_orders"]
        valid["aov_vs_weekday_baseline"] = valid["aov"] - valid["weekday_expected_aov"]

        overall_avg_revenue = valid["total_revenue"].mean()
        overall_avg_profit = valid["net_profit"].mean()
        overall_avg_orders = valid["unique_orders"].mean()

        bucket_summary = valid.groupby("weather_bucket").agg({
            "date": "count",
            "total_revenue": "mean",
            "net_profit": "mean",
            "unique_orders": "mean",
            "aov": "mean",
            "temperature_2m_mean": "mean",
            "precipitation_sum": "mean",
            "revenue_vs_weekday_baseline": "mean",
            "profit_vs_weekday_baseline": "mean",
            "orders_vs_weekday_baseline": "mean",
            "aov_vs_weekday_baseline": "mean",
        }).reset_index().rename(columns={
            "date": "days",
            "total_revenue": "avg_daily_revenue",
            "net_profit": "avg_daily_profit",
            "unique_orders": "avg_daily_orders",
            "aov": "avg_aov",
            "temperature_2m_mean": "avg_temperature",
            "precipitation_sum": "avg_precipitation",
        })

        bucket_summary["revenue_uplift_pct"] = bucket_summary.apply(
            lambda row: round((row["avg_daily_revenue"] / overall_avg_revenue - 1) * 100, 2) if overall_avg_revenue else 0,
            axis=1
        )
        bucket_summary["profit_uplift_pct"] = bucket_summary.apply(
            lambda row: round((row["avg_daily_profit"] / overall_avg_profit - 1) * 100, 2) if overall_avg_profit else 0,
            axis=1
        )
        bucket_summary["orders_uplift_pct"] = bucket_summary.apply(
            lambda row: round((row["avg_daily_orders"] / overall_avg_orders - 1) * 100, 2) if overall_avg_orders else 0,
            axis=1
        )

        bucket_order = ["Good", "Neutral", "Bad"]
        bucket_summary["bucket_order"] = bucket_summary["weather_bucket"].map({label: idx for idx, label in enumerate(bucket_order)})
        bucket_summary = bucket_summary.sort_values("bucket_order").drop(columns=["bucket_order"]).reset_index(drop=True)

        def safe_corr(series_a: pd.Series, series_b: pd.Series) -> Optional[float]:
            mask = series_a.notna() & series_b.notna()
            if mask.sum() < 5:
                return None
            clean_a = series_a[mask]
            clean_b = series_b[mask]
            if clean_a.nunique() < 2 or clean_b.nunique() < 2:
                return None
            corr = clean_a.corr(clean_b)
            if pd.isna(corr):
                return None
            return round(float(corr), 3)

        correlations = {
            "rain_revenue": safe_corr(valid["precipitation_sum"], valid["total_revenue"]),
            "rain_profit": safe_corr(valid["precipitation_sum"], valid["net_profit"]),
            "rain_orders": safe_corr(valid["precipitation_sum"], valid["unique_orders"]),
            "temp_revenue": safe_corr(valid["temperature_2m_mean"], valid["total_revenue"]),
            "temp_profit": safe_corr(valid["temperature_2m_mean"], valid["net_profit"]),
            "temp_orders": safe_corr(valid["temperature_2m_mean"], valid["unique_orders"]),
            "bad_score_revenue": safe_corr(valid["weather_bad_score"], valid["total_revenue"]),
            "bad_score_profit": safe_corr(valid["weather_bad_score"], valid["net_profit"]),
            "bad_score_orders": safe_corr(valid["weather_bad_score"], valid["unique_orders"]),
        }

        lag_correlations: Dict[str, Dict[str, Optional[float]]] = {}
        valid = valid.sort_values("date").reset_index(drop=True)
        for lag in (0, 1, 2):
            weather_score = valid["weather_bad_score"].shift(lag) if lag > 0 else valid["weather_bad_score"]
            lag_correlations[f"lag_{lag}_day"] = {
                "revenue": safe_corr(weather_score, valid["total_revenue"]),
                "profit": safe_corr(weather_score, valid["net_profit"]),
                "orders": safe_corr(weather_score, valid["unique_orders"]),
            }

        analysis_csv = valid[[
            "date",
            "weekday",
            "weather_bucket",
            "weather_bad_score",
            "weather_code",
            "temperature_2m_mean",
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "precipitation_hours",
            "wind_speed_10m_max",
            "total_revenue",
            "net_profit",
            "unique_orders",
            "aov",
            "revenue_vs_weekday_baseline",
            "profit_vs_weekday_baseline",
            "orders_vs_weekday_baseline",
            "aov_vs_weekday_baseline",
        ]].copy()
        analysis_filename = self.output_path(
            f"weather_impact_{date_from.strftime('%Y%m%d')}-{date_to.strftime('%Y%m%d')}.csv"
        )
        analysis_csv.to_csv(analysis_filename, index=False, encoding="utf-8-sig")
        print(f"Weather impact analysis saved: {analysis_filename}")

        location_names = [str(location.get("name", "Location")) for location in self.weather_settings.get("locations", [])]
        return {
            "daily": valid,
            "bucket_summary": bucket_summary,
            "correlations": correlations,
            "lag_correlations": lag_correlations,
            "location_label": ", ".join(location_names),
            "timezone": self.weather_settings.get("timezone", "Europe/Bratislava"),
            "source": "Open-Meteo Historical Weather API",
        }

    def analyze_advanced_dtc_metrics(self, df: pd.DataFrame) -> dict:
        """
        Advanced DTC unit-economics metrics:
        1) First-order contribution margin
        2) First-order vs repeat contribution/order
        3) Contribution LTV/CAC (customer-level, pre-ad contribution)
        4) Cohort payback in days
        7) Contribution by basket size
        8) SKU contribution Pareto (80/20)
        9) Attach-rate for key products
        10) Margin stability index
        11) Payday window index
        """
        print("\nAnalyzing advanced DTC metrics...")
        revenue_col = 'order_revenue_net' if 'order_revenue_net' in df.columns else 'order_total'

        # ---- Build robust order-level frame
        orders_df = df[['order_num', 'customer_email', 'purchase_date', revenue_col, 'total_items_in_order']].drop_duplicates(subset=['order_num']).copy()
        order_spend = (
            df.groupby('order_num')[['fb_ads_daily_spend', 'google_ads_daily_spend']]
            .first()
            .reset_index()
        )
        orders_df = orders_df.merge(order_spend, on='order_num', how='left')
        orders_df['fb_ads_daily_spend'] = pd.to_numeric(orders_df['fb_ads_daily_spend'], errors='coerce').fillna(0.0)
        orders_df['google_ads_daily_spend'] = pd.to_numeric(orders_df['google_ads_daily_spend'], errors='coerce').fillna(0.0)
        orders_df['purchase_datetime'] = pd.to_datetime(orders_df['purchase_date'])
        orders_df['purchase_date_only'] = orders_df['purchase_datetime'].dt.date
        orders_df['cohort_month'] = orders_df['purchase_datetime'].dt.to_period('M').astype(str)

        product_cost_by_order = df.groupby('order_num')['total_expense'].sum()
        orders_df['product_cost'] = orders_df['order_num'].map(product_cost_by_order).fillna(0)
        orders_df['packaging_cost'] = PACKAGING_COST_PER_ORDER
        orders_df['shipping_net_cost'] = SHIPPING_NET_PER_ORDER
        orders_df['shipping_subsidy_cost'] = orders_df['shipping_net_cost']
        orders_df['pre_ad_contribution'] = (
            orders_df[revenue_col] - orders_df['product_cost'] - orders_df['packaging_cost'] - orders_df['shipping_net_cost']
        )

        # Mark first vs repeat orders
        first_order_date = orders_df.groupby('customer_email')['purchase_datetime'].min().to_dict()
        orders_df['is_returning'] = orders_df.apply(
            lambda row: row['purchase_datetime'] > first_order_date.get(row['customer_email'], row['purchase_datetime']),
            axis=1
        )
        first_orders = orders_df[~orders_df['is_returning']].copy()
        repeat_orders = orders_df[orders_df['is_returning']].copy()

        # ---- 1) + 2) first-order contribution metrics
        first_orders_revenue = float(first_orders[revenue_col].sum())
        first_orders_contribution = float(first_orders['pre_ad_contribution'].sum())
        first_order_contribution_margin_pct = (
            (first_orders_contribution / first_orders_revenue * 100) if first_orders_revenue > 0 else 0.0
        )
        first_order_contribution_per_order = (
            (first_orders_contribution / len(first_orders)) if len(first_orders) > 0 else 0.0
        )
        repeat_orders_contribution = float(repeat_orders['pre_ad_contribution'].sum())
        repeat_order_contribution_per_order = (
            (repeat_orders_contribution / len(repeat_orders)) if len(repeat_orders) > 0 else 0.0
        )

        # ---- 3) contribution LTV/CAC (customer-level)
        total_pre_ad_contribution = float(orders_df['pre_ad_contribution'].sum())
        total_customers = int(orders_df['customer_email'].nunique())
        contribution_ltv = (total_pre_ad_contribution / total_customers) if total_customers > 0 else 0.0
        new_customers = int(len(first_orders))
        daily_fb_spend = (
            df.assign(_d=pd.to_datetime(df['purchase_date']).dt.date)
            .groupby('_d')['fb_ads_daily_spend']
            .first()
            .sum()
        )
        paid_cac_fb = (daily_fb_spend / new_customers) if new_customers > 0 else 0.0
        contribution_ltv_cac = (contribution_ltv / paid_cac_fb) if paid_cac_fb > 0 else 0.0

        # ---- 4) cohort payback in days (monthly acquisition cohorts)
        # Cohort CAC is estimated as monthly FB spend / new customers acquired in that month.
        daily_spend_df = (
            df.assign(_d=pd.to_datetime(df['purchase_date']).dt.normalize())
            .groupby('_d')[['fb_ads_daily_spend', 'google_ads_daily_spend']]
            .first()
            .reset_index()
        )
        daily_spend_df['cohort_month'] = daily_spend_df['_d'].dt.to_period('M').astype(str)
        monthly_fb_spend = daily_spend_df.groupby('cohort_month')['fb_ads_daily_spend'].sum().to_dict()
        monthly_google_spend = daily_spend_df.groupby('cohort_month')['google_ads_daily_spend'].sum().to_dict()
        monthly_paid_spend = {
            month: float(monthly_fb_spend.get(month, 0.0)) + float(monthly_google_spend.get(month, 0.0))
            for month in set(monthly_fb_spend.keys()) | set(monthly_google_spend.keys())
        }

        first_order_map = (
            orders_df.sort_values(['customer_email', 'purchase_datetime'])
            .groupby('customer_email')
            .first()[['purchase_datetime', 'cohort_month']]
            .rename(columns={'purchase_datetime': 'first_purchase_datetime'})
        )
        customer_orders = orders_df.sort_values(['customer_email', 'purchase_datetime']).copy()
        customer_orders = customer_orders.merge(
            first_order_map[['first_purchase_datetime', 'cohort_month']],
            left_on='customer_email', right_index=True, how='left', suffixes=('', '_first')
        )
        customer_orders['days_since_first'] = (
            customer_orders['purchase_datetime'] - customer_orders['first_purchase_datetime']
        ).dt.days
        analysis_end = customer_orders['purchase_datetime'].max().normalize()

        cohort_rows = []
        for cohort_month, group in first_order_map.groupby('cohort_month'):
            cohort_customers = group.index.tolist()
            cohort_new_customers = len(cohort_customers)
            cohort_spend = float(monthly_fb_spend.get(cohort_month, 0.0))
            cohort_cac = (cohort_spend / cohort_new_customers) if cohort_new_customers > 0 else 0.0

            payback_days_list = []
            for customer in cohort_customers:
                c_orders = customer_orders[customer_orders['customer_email'] == customer].sort_values('purchase_datetime')
                c_orders = c_orders[['days_since_first', 'pre_ad_contribution']].copy()
                if c_orders.empty:
                    continue
                c_orders['cum_contribution'] = c_orders['pre_ad_contribution'].cumsum()
                reached = c_orders[c_orders['cum_contribution'] >= cohort_cac]
                if cohort_cac <= 0:
                    payback_days_list.append(0)
                elif not reached.empty:
                    payback_days_list.append(int(reached.iloc[0]['days_since_first']))

            recovered_customers = len(payback_days_list)
            recovery_rate_pct = (recovered_customers / cohort_new_customers * 100) if cohort_new_customers > 0 else 0.0
            cohort_rows.append({
                'cohort_month': cohort_month,
                'new_customers': cohort_new_customers,
                'cohort_fb_spend': round(cohort_spend, 2),
                'cohort_cac': round(cohort_cac, 2),
                'recovered_customers': recovered_customers,
                'recovery_rate_pct': round(recovery_rate_pct, 1),
                'avg_payback_days': round(float(np.mean(payback_days_list)), 1) if payback_days_list else np.nan,
                'median_payback_days': round(float(np.median(payback_days_list)), 1) if payback_days_list else np.nan,
            })

        cohort_payback = pd.DataFrame(cohort_rows).sort_values('cohort_month') if cohort_rows else pd.DataFrame()

        # ---- 4b) cohort-normalized CAC / LTV / payback by horizon
        cohort_unit_rows = []
        cohort_horizons = (30, 60, 90, 180)
        for cohort_month, group in first_order_map.groupby('cohort_month'):
            cohort_customers = group.index.tolist()
            cohort_new_customers = len(cohort_customers)
            if cohort_new_customers == 0:
                continue

            cohort_fb_spend = float(monthly_fb_spend.get(cohort_month, 0.0))
            cohort_google_spend = float(monthly_google_spend.get(cohort_month, 0.0))
            cohort_paid_spend = float(monthly_paid_spend.get(cohort_month, 0.0))
            cohort_fb_cac = (cohort_fb_spend / cohort_new_customers) if cohort_new_customers > 0 else 0.0
            cohort_blended_cac = (cohort_paid_spend / cohort_new_customers) if cohort_new_customers > 0 else 0.0

            cohort_first_dates = group['first_purchase_datetime'].sort_values()
            cohort_start_date = cohort_first_dates.min().normalize()
            cohort_latest_first_date = cohort_first_dates.max().normalize()
            cohort_age_days = int((analysis_end - cohort_latest_first_date).days)
            cohort_customer_orders = customer_orders[
                customer_orders['customer_email'].isin(cohort_customers)
            ].copy()

            row = {
                'cohort_month': cohort_month,
                'new_customers': cohort_new_customers,
                'cohort_start_date': cohort_start_date.strftime('%Y-%m-%d'),
                'cohort_latest_first_date': cohort_latest_first_date.strftime('%Y-%m-%d'),
                'cohort_age_days': cohort_age_days,
                'cohort_fb_spend': round(cohort_fb_spend, 2),
                'cohort_google_spend': round(cohort_google_spend, 2),
                'cohort_paid_spend': round(cohort_paid_spend, 2),
                'cohort_fb_cac': round(cohort_fb_cac, 2),
                'cohort_blended_cac': round(cohort_blended_cac, 2),
            }

            for horizon in cohort_horizons:
                mature = cohort_age_days >= horizon
                horizon_orders = cohort_customer_orders[cohort_customer_orders['days_since_first'] <= horizon].copy()
                revenue_ltv = (float(horizon_orders[revenue_col].sum()) / cohort_new_customers) if mature else np.nan
                contribution_ltv_h = (float(horizon_orders['pre_ad_contribution'].sum()) / cohort_new_customers) if mature else np.nan

                revenue_ltv_cac_h = np.nan
                contribution_ltv_cac_h = np.nan
                if mature and cohort_blended_cac > 0:
                    revenue_ltv_cac_h = revenue_ltv / cohort_blended_cac
                    contribution_ltv_cac_h = contribution_ltv_h / cohort_blended_cac

                payback_days_list = []
                if mature and cohort_blended_cac > 0:
                    for customer in cohort_customers:
                        c_orders = horizon_orders[horizon_orders['customer_email'] == customer].sort_values('purchase_datetime')
                        if c_orders.empty:
                            continue
                        c_orders = c_orders[['days_since_first', 'pre_ad_contribution']].copy()
                        c_orders['cum_contribution'] = c_orders['pre_ad_contribution'].cumsum()
                        reached = c_orders[c_orders['cum_contribution'] >= cohort_blended_cac]
                        if not reached.empty:
                            payback_days_list.append(int(reached.iloc[0]['days_since_first']))

                recovered_customers_h = len(payback_days_list) if mature and cohort_blended_cac > 0 else np.nan
                recovery_rate_h = (
                    recovered_customers_h / cohort_new_customers * 100
                    if mature and cohort_blended_cac > 0 else np.nan
                )

                row.update({
                    f'revenue_ltv_{horizon}d': round(revenue_ltv, 2) if pd.notna(revenue_ltv) else np.nan,
                    f'contribution_ltv_{horizon}d': round(contribution_ltv_h, 2) if pd.notna(contribution_ltv_h) else np.nan,
                    f'revenue_ltv_cac_{horizon}d': round(revenue_ltv_cac_h, 2) if pd.notna(revenue_ltv_cac_h) else np.nan,
                    f'contribution_ltv_cac_{horizon}d': round(contribution_ltv_cac_h, 2) if pd.notna(contribution_ltv_cac_h) else np.nan,
                    f'payback_recovered_{horizon}d': int(recovered_customers_h) if pd.notna(recovered_customers_h) else np.nan,
                    f'payback_recovery_{horizon}d_pct': round(recovery_rate_h, 1) if pd.notna(recovery_rate_h) else np.nan,
                    f'avg_payback_{horizon}d_days': round(float(np.mean(payback_days_list)), 1) if payback_days_list else np.nan,
                    f'median_payback_{horizon}d_days': round(float(np.median(payback_days_list)), 1) if payback_days_list else np.nan,
                })

            cohort_unit_rows.append(row)

        cohort_unit_economics = (
            pd.DataFrame(cohort_unit_rows).sort_values('cohort_month').reset_index(drop=True)
            if cohort_unit_rows else pd.DataFrame()
        )

        mature_weighted_summary = {}
        if not cohort_unit_economics.empty:
            weight_col = cohort_unit_economics['new_customers'].fillna(0)
            for horizon in cohort_horizons:
                ratio_col = f'contribution_ltv_cac_{horizon}d'
                recovery_col = f'payback_recovery_{horizon}d_pct'
                valid_ratio = cohort_unit_economics[ratio_col].notna()
                valid_recovery = cohort_unit_economics[recovery_col].notna()
                if valid_ratio.any():
                    mature_weighted_summary[f'mature_{horizon}d_contribution_ltv_cac'] = round(
                        float((cohort_unit_economics.loc[valid_ratio, ratio_col] * weight_col.loc[valid_ratio]).sum() / weight_col.loc[valid_ratio].sum()),
                        2,
                    )
                if valid_recovery.any():
                    mature_weighted_summary[f'mature_{horizon}d_payback_recovery_pct'] = round(
                        float((cohort_unit_economics.loc[valid_recovery, recovery_col] * weight_col.loc[valid_recovery]).sum() / weight_col.loc[valid_recovery].sum()),
                        1,
                    )

        # ---- 7) contribution by basket size
        def basket_bucket(items_count):
            try:
                v = int(items_count)
            except (TypeError, ValueError):
                return 'unknown'
            if v <= 1:
                return '1 item'
            if v == 2:
                return '2 items'
            if v == 3:
                return '3 items'
            if v == 4:
                return '4 items'
            return '5+ items'

        orders_df['basket_size'] = orders_df['total_items_in_order'].apply(basket_bucket)
        basket_contrib = orders_df.groupby('basket_size').agg({
            'order_num': 'count',
            revenue_col: 'sum',
            'pre_ad_contribution': 'sum'
        }).reset_index()
        basket_contrib.columns = ['basket_size', 'orders', 'revenue', 'pre_ad_contribution']
        basket_contrib['contribution_per_order'] = basket_contrib.apply(
            lambda row: round((row['pre_ad_contribution'] / row['orders']) if row['orders'] > 0 else 0, 2), axis=1
        )
        basket_contrib['contribution_margin_pct'] = basket_contrib.apply(
            lambda row: round((row['pre_ad_contribution'] / row['revenue'] * 100) if row['revenue'] > 0 else 0, 1), axis=1
        )
        basket_order = {'1 item': 1, '2 items': 2, '3 items': 3, '4 items': 4, '5+ items': 5, 'unknown': 99}
        basket_contrib['basket_order'] = basket_contrib['basket_size'].map(basket_order).fillna(99)
        basket_contrib = basket_contrib.sort_values('basket_order').drop(columns=['basket_order'])

        # ---- 8) SKU Pareto on pre-ad contribution (with proportional order overhead allocation)
        item_df = df[['order_num', 'product_sku', 'item_label', 'item_total_without_tax', 'total_expense']].copy()
        order_item_revenue = item_df.groupby('order_num')['item_total_without_tax'].sum().rename('order_item_revenue')
        item_df = item_df.merge(order_item_revenue, on='order_num', how='left')
        item_df['item_rev_share'] = item_df.apply(
            lambda row: (row['item_total_without_tax'] / row['order_item_revenue']) if row['order_item_revenue'] > 0 else 0,
            axis=1
        )
        overhead_per_order = PACKAGING_COST_PER_ORDER + SHIPPING_NET_PER_ORDER
        item_df['allocated_overhead'] = item_df['item_rev_share'] * overhead_per_order
        item_df['pre_ad_contribution'] = item_df['item_total_without_tax'] - item_df['total_expense'] - item_df['allocated_overhead']

        sku_pareto = item_df.groupby('product_sku').agg({
            'item_label': 'first',
            'order_num': 'nunique',
            'item_total_without_tax': 'sum',
            'total_expense': 'sum',
            'pre_ad_contribution': 'sum'
        }).reset_index()
        sku_pareto.columns = ['sku', 'product', 'orders', 'revenue', 'cost', 'pre_ad_contribution']
        sku_pareto = sku_pareto.sort_values('pre_ad_contribution', ascending=False).reset_index(drop=True)
        total_contrib_sku = float(sku_pareto['pre_ad_contribution'].sum())
        if total_contrib_sku != 0:
            sku_pareto['contribution_share_pct'] = (sku_pareto['pre_ad_contribution'] / total_contrib_sku * 100).round(2)
            sku_pareto['cum_contribution_share_pct'] = sku_pareto['contribution_share_pct'].cumsum().round(2)
        else:
            sku_pareto['contribution_share_pct'] = 0.0
            sku_pareto['cum_contribution_share_pct'] = 0.0
        sku_pareto_80_count = int((sku_pareto['cum_contribution_share_pct'] < 80).sum() + 1) if not sku_pareto.empty else 0

        # ---- 9) attach rate for key products (top 10 by order penetration)
        order_sku = item_df.groupby('order_num')['product_sku'].apply(lambda x: set(x.dropna().astype(str))).reset_index()
        total_orders = len(order_sku)
        sku_order_count = {}
        for skus in order_sku['product_sku']:
            for sku in skus:
                sku_order_count[sku] = sku_order_count.get(sku, 0) + 1

        key_skus = sorted(sku_order_count.keys(), key=lambda s: sku_order_count[s], reverse=True)[:10]
        sku_to_label = item_df.groupby('product_sku')['item_label'].first().to_dict()
        attach_rows = []
        order_sku_sets = order_sku['product_sku'].tolist()
        for key_sku in key_skus:
            key_orders = sku_order_count.get(key_sku, 0)
            if key_orders == 0:
                continue
            co_counts = {}
            for skus in order_sku_sets:
                if key_sku in skus:
                    for other in skus:
                        if other == key_sku:
                            continue
                        co_counts[other] = co_counts.get(other, 0) + 1
            top_attach = sorted(co_counts.items(), key=lambda x: x[1], reverse=True)[:3]
            for other_sku, co_count in top_attach:
                attach_rows.append({
                    'key_sku': key_sku,
                    'key_product': sku_to_label.get(key_sku, key_sku),
                    'key_orders': key_orders,
                    'attached_sku': other_sku,
                    'attached_product': sku_to_label.get(other_sku, other_sku),
                    'attached_orders': int(co_count),
                    'attach_rate_pct': round((co_count / key_orders * 100), 1),
                    'key_penetration_pct': round((key_orders / total_orders * 100), 1) if total_orders > 0 else 0.0
                })
        attach_rate = pd.DataFrame(attach_rows).sort_values(['key_orders', 'attach_rate_pct'], ascending=[False, False]) if attach_rows else pd.DataFrame()
        if not attach_rate.empty:
            attach_rate['anchor_item'] = attach_rate['key_product']
            attach_rate['anchor_orders'] = attach_rate['key_orders']
            attach_rate['attached_item'] = attach_rate['attached_product']

        # ---- Roy bundle/accessory model (config-driven, optional per project)
        bundle_accessory_model = self.analyze_bundle_accessory_model(orders_df, item_df, revenue_col)
        acquisition_product_family_cube = self.analyze_acquisition_source_product_family_cube(
            orders_df=orders_df,
            item_df=item_df,
            customer_orders=customer_orders,
            revenue_col=revenue_col,
        )

        # ---- 10) margin stability index (daily pre-ad margin volatility)
        daily_margin = orders_df.groupby('purchase_date_only').agg({
            revenue_col: 'sum',
            'pre_ad_contribution': 'sum',
            'order_num': 'count'
        }).reset_index()
        daily_margin.columns = ['date', 'revenue', 'pre_ad_contribution', 'orders']
        daily_margin['pre_ad_margin_pct'] = daily_margin.apply(
            lambda row: (row['pre_ad_contribution'] / row['revenue'] * 100) if row['revenue'] > 0 else 0,
            axis=1
        )
        daily_margin = daily_margin.sort_values('date')
        daily_margin['pre_ad_margin_7d_ma'] = daily_margin['pre_ad_margin_pct'].rolling(window=7, min_periods=1).mean()
        daily_margin['pre_ad_contribution_margin_pct'] = daily_margin['pre_ad_margin_pct']

        margin_mean = float(daily_margin['pre_ad_margin_pct'].mean()) if not daily_margin.empty else 0.0
        margin_std = float(daily_margin['pre_ad_margin_pct'].std(ddof=0)) if len(daily_margin) > 1 else 0.0
        margin_cv_pct = (margin_std / abs(margin_mean) * 100) if abs(margin_mean) > 1e-9 else 0.0
        margin_stability_index = max(0.0, min(100.0, 100.0 - (margin_std * 2.0)))

        # ---- 11) payday window index (phase-of-month windows)
        window_defs = [
            (1, 7, '1-7'),
            (8, 14, '8-14'),
            (15, 21, '15-21'),
            (22, 28, '22-28'),
            (29, 31, '29-31')
        ]
        orders_df['day_in_month'] = orders_df['purchase_datetime'].dt.day
        min_dt = orders_df['purchase_datetime'].dt.normalize().min()
        max_dt = orders_df['purchase_datetime'].dt.normalize().max()
        full_start = min_dt if min_dt.day == 1 else (min_dt + pd.offsets.MonthBegin(1))
        max_month_last_day = (max_dt + pd.offsets.MonthEnd(0)).day
        full_end = max_dt if max_dt.day == max_month_last_day else (max_dt - pd.offsets.MonthEnd(1))
        if full_start <= full_end:
            phase_orders = orders_df[
                (orders_df['purchase_datetime'].dt.normalize() >= full_start) &
                (orders_df['purchase_datetime'].dt.normalize() <= full_end)
            ].copy()
            phase_calendar = pd.DataFrame({'date': pd.date_range(start=full_start, end=full_end, freq='D')})
        else:
            phase_orders = orders_df.copy()
            phase_calendar = pd.DataFrame({'date': pd.date_range(start=min_dt, end=max_dt, freq='D')})
        phase_orders['window'] = phase_orders['day_in_month'].apply(
            lambda d: '1-7' if d <= 7 else ('8-14' if d <= 14 else ('15-21' if d <= 21 else ('22-28' if d <= 28 else '29-31')))
        )
        phase_calendar['window'] = phase_calendar['date'].dt.day.apply(
            lambda d: '1-7' if d <= 7 else ('8-14' if d <= 14 else ('15-21' if d <= 21 else ('22-28' if d <= 28 else '29-31')))
        )

        payday_window = phase_orders.groupby('window').agg({
            'order_num': 'count',
            revenue_col: 'sum',
            'pre_ad_contribution': 'sum'
        }).reset_index()
        payday_window.columns = ['window', 'orders', 'revenue', 'pre_ad_contribution']
        cal_days = phase_calendar.groupby('window')['date'].count().reset_index().rename(columns={'date': 'calendar_days'})
        payday_window = payday_window.merge(cal_days, on='window', how='right').fillna(0)
        payday_window['orders'] = payday_window['orders'].astype(int)
        payday_window['calendar_days'] = payday_window['calendar_days'].astype(int)
        payday_window['avg_orders_per_day'] = payday_window.apply(
            lambda row: (row['orders'] / row['calendar_days']) if row['calendar_days'] > 0 else 0,
            axis=1
        )
        payday_window['avg_revenue_per_day'] = payday_window.apply(
            lambda row: (row['revenue'] / row['calendar_days']) if row['calendar_days'] > 0 else 0,
            axis=1
        )
        payday_window['avg_profit_per_day'] = payday_window.apply(
            lambda row: (row['pre_ad_contribution'] / row['calendar_days']) if row['calendar_days'] > 0 else 0,
            axis=1
        )
        overall_avg_revenue = (
            (payday_window['revenue'].sum() / payday_window['calendar_days'].sum())
            if payday_window['calendar_days'].sum() > 0 else 0
        )
        overall_avg_profit = (
            (payday_window['pre_ad_contribution'].sum() / payday_window['calendar_days'].sum())
            if payday_window['calendar_days'].sum() > 0 else 0
        )
        payday_window['revenue_index'] = payday_window.apply(
            lambda row: (row['avg_revenue_per_day'] / overall_avg_revenue * 100) if overall_avg_revenue > 0 else 0,
            axis=1
        )
        payday_window['profit_index'] = payday_window.apply(
            lambda row: (row['avg_profit_per_day'] / overall_avg_profit * 100) if overall_avg_profit != 0 else 0,
            axis=1
        )
        window_order = {'1-7': 1, '8-14': 2, '15-21': 3, '22-28': 4, '29-31': 5}
        payday_window['window_order'] = payday_window['window'].map(window_order).fillna(99)
        payday_window = payday_window.sort_values('window_order').drop(columns=['window_order'])
        if not sku_pareto.empty:
            sku_pareto['cum_contribution_pct'] = sku_pareto['cum_contribution_share_pct']

        result = {
            'summary': {
                'first_order_contribution_margin_pct': round(first_order_contribution_margin_pct, 2),
                'first_order_contribution_per_order': round(first_order_contribution_per_order, 2),
                'repeat_order_contribution_per_order': round(repeat_order_contribution_per_order, 2),
                'contribution_ltv': round(contribution_ltv, 2),
                'paid_cac_fb': round(paid_cac_fb, 2),
                'contribution_ltv_cac': round(contribution_ltv_cac, 2),
                'margin_mean_pct': round(margin_mean, 2),
                'margin_std_pp': round(margin_std, 2),
                'margin_cv_pct': round(margin_cv_pct, 2),
                'margin_stability_index': round(margin_stability_index, 1),
                'sku_pareto_80_count': sku_pareto_80_count,
                'sku_total_count': int(len(sku_pareto)),
                **mature_weighted_summary,
            },
            'cohort_payback': cohort_payback,
            'cohort_unit_economics': cohort_unit_economics,
            'basket_contribution': basket_contrib,
            'sku_pareto': sku_pareto,
            'attach_rate': attach_rate,
            'bundle_accessory_model': bundle_accessory_model,
            'acquisition_product_family_cube': acquisition_product_family_cube,
            'daily_margin': daily_margin,
            'payday_window': payday_window,
        }

        print(
            f"Advanced DTC metrics complete: first-order margin={result['summary']['first_order_contribution_margin_pct']:.2f}%, "
            f"contribution LTV/CAC={result['summary']['contribution_ltv_cac']:.2f}x"
        )
        return result
    def analyze_day_hour_heatmap(self, df: pd.DataFrame) -> pd.DataFrame:
        """Analyze orders by day of week and hour of day for heatmap visualization"""
        print("\nAnalyzing day/hour heatmap patterns...")

        # Parse purchase_date to extract day of week and hour
        df['purchase_datetime_full'] = pd.to_datetime(df['purchase_date'])
        df['day_of_week'] = df['purchase_datetime_full'].dt.dayofweek  # 0=Monday, 6=Sunday
        df['hour_of_day'] = df['purchase_datetime_full'].dt.hour

        # Day names for display
        day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

        # Aggregate by day of week and hour (using unique orders)
        heatmap_data = df.groupby(['day_of_week', 'hour_of_day']).agg({
            'order_num': 'nunique'
        }).reset_index()
        heatmap_data.columns = ['day_of_week', 'hour', 'orders']

        # Create complete matrix (all combinations of day 0-6 and hour 0-23)
        complete_matrix = []
        for day in range(7):
            for hour in range(24):
                row = heatmap_data[(heatmap_data['day_of_week'] == day) & (heatmap_data['hour'] == hour)]
                if len(row) > 0:
                    orders = row['orders'].values[0]
                else:
                    orders = 0
                complete_matrix.append({
                    'day_of_week': day,
                    'day_name': day_names[day],
                    'hour': hour,
                    'orders': orders
                })

        result = pd.DataFrame(complete_matrix)
        print(f"Day/hour heatmap analysis complete")
        return result

    def analyze_geographic(self, df: pd.DataFrame) -> tuple:
        """Analyze orders by geographic location"""
        print("\nAnalyzing geographic distribution...")

        geo_df = df.copy()
        geo_df['geo_country'] = geo_df.get('delivery_country')
        geo_df['geo_country'] = geo_df['geo_country'].replace('', np.nan)
        if 'invoice_country' in geo_df.columns:
            geo_df['geo_country'] = geo_df['geo_country'].fillna(geo_df['invoice_country'])
        geo_df['geo_country'] = geo_df['geo_country'].fillna('Unknown')

        geo_df['geo_city'] = geo_df.get('delivery_city')
        geo_df['geo_city'] = geo_df['geo_city'].replace('', np.nan)
        if 'invoice_city' in geo_df.columns:
            geo_df['geo_city'] = geo_df['geo_city'].fillna(geo_df['invoice_city'])
        geo_df['geo_city'] = geo_df['geo_city'].apply(
            lambda value: str(value).strip() if pd.notna(value) and str(value).strip() else np.nan
        )

        # By country
        country_agg = geo_df.groupby('geo_country').agg({
            'order_num': 'nunique',
            'item_total_without_tax': 'sum',
            'profit_before_ads': 'sum'
        }).reset_index()
        country_agg.columns = ['country', 'orders', 'revenue', 'profit']
        country_agg = country_agg.sort_values('revenue', ascending=False)
        country_agg['revenue_pct'] = (country_agg['revenue'] / country_agg['revenue'].sum() * 100).round(1)
        country_meta = country_agg['orders'].apply(lambda value: self._geo_confidence_payload(value, level="country"))
        country_agg = pd.concat([country_agg, pd.DataFrame(country_meta.tolist(), index=country_agg.index)], axis=1)

        # By city (top 20), prefer delivery city and fallback to invoice city if delivery is missing.
        city_source = geo_df[geo_df['geo_city'].notna()].copy()
        city_agg = city_source.groupby(['geo_city', 'geo_country']).agg({
            'order_num': 'nunique',
            'item_total_without_tax': 'sum',
            'profit_before_ads': 'sum'
        }).reset_index()
        city_agg.columns = ['city', 'country', 'orders', 'revenue', 'profit']
        city_agg = city_agg.sort_values(['revenue', 'orders'], ascending=[False, False]).head(20)
        city_agg['revenue_pct'] = (city_agg['revenue'] / geo_df['item_total_without_tax'].sum() * 100).round(1)
        city_meta = city_agg['orders'].apply(lambda value: self._geo_confidence_payload(value, level="city"))
        city_agg = pd.concat([city_agg, pd.DataFrame(city_meta.tolist(), index=city_agg.index)], axis=1)

        print(f"Geographic analysis complete: {len(country_agg)} countries, showing top 20 cities")
        return country_agg, city_agg

    def analyze_geo_profitability(self, df: pd.DataFrame, fb_campaigns: list = None) -> dict:
        """
        Analyze SK/CZ/HU profitability with estimated FB spend attribution by campaign name.
        Returns country-level contribution margin and FB CPO.
        """
        print("\nAnalyzing geo profitability (SK/CZ/HU)...")

        # Build one row per order with country + order economics.
        # Prefer delivery country, fallback to invoice country if missing.
        geo_df = df.copy()
        if 'delivery_country' in geo_df.columns:
            geo_df['geo_country'] = geo_df['delivery_country']
        else:
            geo_df['geo_country'] = None
        geo_df['geo_country'] = geo_df['geo_country'].replace('', np.nan)
        if 'invoice_country' in geo_df.columns:
            geo_df['geo_country'] = geo_df['geo_country'].fillna(geo_df['invoice_country'])

        order_level = geo_df.groupby('order_num').agg({
            'geo_country': 'first',
            'item_total_without_tax': 'sum',
            'total_expense': 'sum'
        }).reset_index()
        order_level.columns = ['order_num', 'country', 'revenue', 'product_cost']
        order_level['country'] = order_level['country'].fillna('unknown').astype(str).str.lower().str.strip()

        # Normalize common country aliases.
        alias_map = {
            'slovakia': 'sk',
            'slovensko': 'sk',
            'czech republic': 'cz',
            'cesko': 'cz',
            'ÄŤesko': 'cz',
            'hungary': 'hu',
            'madarsko': 'hu',
            'maÄŹarsko': 'hu',
        }
        order_level['country'] = order_level['country'].replace(alias_map)
        order_level = order_level[order_level['country'].isin(['sk', 'cz', 'hu'])]

        if order_level.empty:
            return {
                'table': pd.DataFrame(),
                'fb_spend_by_country': {'sk': 0.0, 'cz': 0.0, 'hu': 0.0},
                'fb_spend_unattributed': 0.0
            }

        geo = order_level.groupby('country').agg({
            'order_num': 'nunique',
            'revenue': 'sum',
            'product_cost': 'sum'
        }).reset_index()
        geo.columns = ['country', 'orders', 'revenue', 'product_cost']

        geo['packaging_cost'] = geo['orders'] * PACKAGING_COST_PER_ORDER
        geo['shipping_net_cost'] = geo['orders'] * SHIPPING_NET_PER_ORDER
        geo['shipping_subsidy_cost'] = geo['shipping_net_cost']

        fb_spend_by_country = {'sk': 0.0, 'cz': 0.0, 'hu': 0.0}
        fb_spend_unattributed = 0.0

        def infer_campaign_country(campaign_name: str) -> Optional[str]:
            name = str(campaign_name or '').upper()
            if re.search(r'(^|[^A-Z])SK([^A-Z]|$)', name) or 'SLOVAK' in name:
                return 'sk'
            if re.search(r'(^|[^A-Z])CZ([^A-Z]|$)', name) or 'CZECH' in name:
                return 'cz'
            if re.search(r'(^|[^A-Z])HU([^A-Z]|$)', name) or 'HUNGAR' in name:
                return 'hu'
            return None

        for campaign in (fb_campaigns or []):
            spend = float(campaign.get('spend', 0) or 0)
            country = infer_campaign_country(campaign.get('campaign_name', ''))
            if country and country in fb_spend_by_country:
                fb_spend_by_country[country] += spend
            else:
                fb_spend_unattributed += spend

        geo['fb_ads_spend'] = geo['country'].map(fb_spend_by_country).fillna(0)
        geo['contribution_cost'] = geo['product_cost'] + geo['packaging_cost'] + geo['shipping_net_cost'] + geo['fb_ads_spend']
        geo['contribution_profit'] = geo['revenue'] - geo['contribution_cost']
        geo['contribution_margin_pct'] = geo.apply(
            lambda row: round((row['contribution_profit'] / row['revenue'] * 100) if row['revenue'] > 0 else 0, 2),
            axis=1
        )
        geo['fb_cpo'] = geo.apply(
            lambda row: round((row['fb_ads_spend'] / row['orders']) if row['orders'] > 0 else 0, 2),
            axis=1
        )
        geo['avg_order_value'] = geo.apply(
            lambda row: round((row['revenue'] / row['orders']) if row['orders'] > 0 else 0, 2),
            axis=1
        )
        geo_meta = geo['orders'].apply(lambda value: self._geo_confidence_payload(value, level="country"))
        geo = pd.concat([geo, pd.DataFrame(geo_meta.tolist(), index=geo.index)], axis=1)
        geo['contribution_profit_guarded'] = geo.apply(
            lambda row: row['contribution_profit'] if not bool(row.get('hide_economics')) else np.nan,
            axis=1,
        )
        geo['contribution_margin_pct_guarded'] = geo.apply(
            lambda row: row['contribution_margin_pct'] if not bool(row.get('hide_economics')) else np.nan,
            axis=1,
        )
        geo['fb_cpo_guarded'] = geo.apply(
            lambda row: row['fb_cpo'] if not bool(row.get('hide_economics')) else np.nan,
            axis=1,
        )

        # Round financial values for display.
        for col in ['revenue', 'product_cost', 'packaging_cost', 'shipping_subsidy_cost', 'fb_ads_spend', 'contribution_cost', 'contribution_profit', 'contribution_profit_guarded']:
            geo[col] = geo[col].round(2)

        geo = geo.sort_values('revenue', ascending=False).reset_index(drop=True)
        print(f"Geo profitability complete: {len(geo)} countries, unattributed FB spend=EUR {fb_spend_unattributed:.2f}")

        return {
            'table': geo,
            'fb_spend_by_country': {k: round(v, 2) for k, v in fb_spend_by_country.items()},
            'fb_spend_unattributed': round(fb_spend_unattributed, 2)
        }

    def analyze_b2b_vs_b2c(self, df: pd.DataFrame) -> pd.DataFrame:
        """Analyze B2B vs B2C orders"""
        print("\nAnalyzing B2B vs B2C split...")

        # B2B = has company VAT ID or company ID
        df['is_b2b'] = df.apply(
            lambda row: pd.notna(row.get('customer_vat_id')) and str(row.get('customer_vat_id', '')).strip() != ''
                        or pd.notna(row.get('customer_company_id')) and str(row.get('customer_company_id', '')).strip() != '',
            axis=1
        )

        b2b_agg = df.groupby('is_b2b').agg({
            'order_num': 'nunique',
            'item_total_without_tax': 'sum',
            'profit_before_ads': 'sum',
            'customer_email': 'nunique'
        }).reset_index()

        b2b_agg.columns = ['is_b2b', 'orders', 'revenue', 'profit', 'unique_customers']
        b2b_agg['customer_type'] = b2b_agg['is_b2b'].map({True: 'B2B (Companies)', False: 'B2C (Individuals)'})
        b2b_agg['aov'] = (b2b_agg['revenue'] / b2b_agg['orders']).round(2)
        b2b_agg['orders_pct'] = (b2b_agg['orders'] / b2b_agg['orders'].sum() * 100).round(1)
        b2b_agg['revenue_pct'] = (b2b_agg['revenue'] / b2b_agg['revenue'].sum() * 100).round(1)

        print(f"B2B vs B2C analysis complete")
        return b2b_agg

    def analyze_product_margins(self, df: pd.DataFrame) -> pd.DataFrame:
        """Analyze profit margins by product (grouped by product_sku)"""
        print("\nAnalyzing product margins...")

        product_margins = df.groupby('product_sku').agg({
            'item_label': 'first',  # Keep product name for display
            'item_quantity': 'sum',
            'item_total_without_tax': 'sum',
            'total_expense': 'sum',
            'profit_before_ads': 'sum',
            'order_num': 'nunique'
        }).reset_index()

        product_margins.columns = ['sku', 'product', 'quantity', 'revenue', 'cost', 'profit', 'orders']
        product_margins['margin_pct'] = ((product_margins['profit'] / product_margins['revenue']) * 100).round(1)
        product_margins['margin_pct'] = product_margins['margin_pct'].fillna(0)
        product_margins = product_margins.sort_values('margin_pct', ascending=False)

        print(f"Product margin analysis complete: {len(product_margins)} products")
        return product_margins

    def analyze_product_trends(self, df: pd.DataFrame) -> pd.DataFrame:
        """Analyze product sales trends (growing vs declining) - grouped by product_sku"""
        print("\nAnalyzing product trends...")

        df['week'] = pd.to_datetime(df['purchase_date']).dt.isocalendar().week
        df['year_week'] = pd.to_datetime(df['purchase_date']).dt.strftime('%Y-W%W')

        # Get first and last half of the period
        all_weeks = df['year_week'].unique()
        if len(all_weeks) < 4:
            print("Not enough weeks for trend analysis")
            return pd.DataFrame()

        mid_point = len(all_weeks) // 2
        first_half_weeks = all_weeks[:mid_point]
        second_half_weeks = all_weeks[mid_point:]

        # Aggregate by product_sku for each half
        first_half = df[df['year_week'].isin(first_half_weeks)].groupby('product_sku').agg({
            'item_label': 'first',  # Keep product name for display
            'item_quantity': 'sum',
            'item_total_without_tax': 'sum'
        }).reset_index()
        first_half.columns = ['sku', 'product', 'qty_first', 'revenue_first']

        second_half = df[df['year_week'].isin(second_half_weeks)].groupby('product_sku').agg({
            'item_label': 'first',  # Keep product name for display
            'item_quantity': 'sum',
            'item_total_without_tax': 'sum'
        }).reset_index()
        second_half.columns = ['sku', 'product', 'qty_second', 'revenue_second']

        # Merge and calculate growth - merge on sku
        trends = first_half.merge(second_half, on='sku', how='outer', suffixes=('', '_r'))
        # Use product name from whichever half has data
        trends['product'] = trends['product'].combine_first(trends['product_r'])
        trends = trends.drop(columns=['product_r'], errors='ignore')
        trends = trends.fillna(0)
        trends['qty_growth_pct'] = ((trends['qty_second'] - trends['qty_first']) / trends['qty_first'].replace(0, 1) * 100).round(1)
        trends['revenue_growth_pct'] = ((trends['revenue_second'] - trends['revenue_first']) / trends['revenue_first'].replace(0, 1) * 100).round(1)

        # Classify trend
        def classify_trend(row):
            if row['qty_first'] == 0 and row['qty_second'] > 0:
                return 'New'
            elif row['qty_second'] == 0 and row['qty_first'] > 0:
                return 'Discontinued'
            elif row['qty_growth_pct'] > 20:
                return 'Growing'
            elif row['qty_growth_pct'] < -20:
                return 'Declining'
            else:
                return 'Stable'

        trends['trend'] = trends.apply(classify_trend, axis=1)
        trends['total_qty'] = trends['qty_first'] + trends['qty_second']
        trends['total_revenue'] = trends['revenue_first'] + trends['revenue_second']
        trends = trends.sort_values('total_revenue', ascending=False)

        print(f"Product trends analysis complete: {len(trends)} products")
        return trends

    def analyze_customer_concentration(self, df: pd.DataFrame) -> dict:
        """Analyze customer concentration (top customers % of revenue)"""
        print("\nAnalyzing customer concentration...")

        customer_revenue = df.groupby('customer_email').agg({
            'order_num': 'nunique',
            'item_total_without_tax': 'sum',
            'profit_before_ads': 'sum'
        }).reset_index()
        customer_revenue.columns = ['customer', 'orders', 'revenue', 'profit']
        customer_revenue = customer_revenue.sort_values('revenue', ascending=False)

        total_revenue = customer_revenue['revenue'].sum()
        total_customers = len(customer_revenue)

        # Calculate concentration metrics for 10%, 20%, 30%, 40%, 50% of customers
        concentration_levels = [10, 20, 30, 40, 50]
        level_counts = {}
        level_revenue = {}
        level_revenue_share = {}

        for level in concentration_levels:
            count = max(1, int(total_customers * level / 100))
            level_counts[level] = count
            level_revenue[level] = customer_revenue.head(count)['revenue'].sum()
            level_revenue_share[level] = round(level_revenue[level] / total_revenue * 100, 1) if total_revenue > 0 else 0

        # Top 10 customers by revenue (absolute, not percentage)
        top_10_customers = customer_revenue.head(10).copy()
        top_10_customers['revenue_pct'] = (top_10_customers['revenue'] / total_revenue * 100).round(1)

        concentration = {
            'total_customers': total_customers,
            'level_counts': level_counts,
            'level_revenue': level_revenue,
            'level_revenue_share': level_revenue_share,
            # Keep backward compatibility
            'top_10_pct_revenue_share': level_revenue_share.get(10, 0),
            'top_20_pct_revenue_share': level_revenue_share.get(20, 0),
            'top_10_customers': top_10_customers,
            'avg_revenue_per_customer': round(total_revenue / total_customers, 2) if total_customers > 0 else 0,
            'median_revenue_per_customer': round(customer_revenue['revenue'].median(), 2)
        }

        # Repeat purchase rate
        repeat_customers = len(customer_revenue[customer_revenue['orders'] > 1])
        concentration['repeat_purchase_rate'] = round(repeat_customers / total_customers * 100, 1) if total_customers > 0 else 0
        concentration['repeat_customers'] = repeat_customers
        concentration['one_time_customers'] = total_customers - repeat_customers

        print(f"Customer concentration analysis complete: {total_customers} customers")
        return concentration

    def calculate_financial_metrics(self, df: pd.DataFrame, date_agg: pd.DataFrame, clv_return_time_analysis: pd.DataFrame = None) -> dict:
        """Calculate additional financial metrics"""
        print("\nCalculating financial metrics...")
        revenue_col = 'order_revenue_net' if 'order_revenue_net' in df.columns else 'order_total'

        # Use date_agg to keep one consistent source of truth for summary metrics
        # and avoid ad-spend duplication from item-level rows.
        total_revenue = date_agg['total_revenue'].sum()
        total_orders = date_agg['unique_orders'].sum() if 'unique_orders' in date_agg.columns else df['order_num'].nunique()
        total_customers = df['customer_email'].nunique()
        total_fb_spend = date_agg['fb_ads_spend'].sum() if 'fb_ads_spend' in date_agg.columns else 0
        total_google_spend = date_agg['google_ads_spend'].sum() if 'google_ads_spend' in date_agg.columns else 0
        total_ad_spend = total_fb_spend + total_google_spend
        total_product_cost = date_agg['product_expense'].sum() if 'product_expense' in date_agg.columns else df['total_expense'].sum()
        total_packaging_cost = date_agg['packaging_cost'].sum() if 'packaging_cost' in date_agg.columns else 0
        total_shipping_net = (
            date_agg['shipping_net_cost'].sum()
            if 'shipping_net_cost' in date_agg.columns
            else (date_agg['shipping_subsidy_cost'].sum() if 'shipping_subsidy_cost' in date_agg.columns else 0)
        )
        total_fixed_overhead = date_agg['fixed_daily_cost'].sum() if 'fixed_daily_cost' in date_agg.columns else 0
        total_company_cost = date_agg['total_cost'].sum() if 'total_cost' in date_agg.columns else (total_product_cost + total_ad_spend + total_packaging_cost + total_shipping_net + total_fixed_overhead)
        total_company_profit = date_agg['net_profit'].sum() if 'net_profit' in date_agg.columns else (df['profit_before_ads'].sum() - total_fb_spend - total_google_spend)
        total_contribution_cost = date_agg['contribution_cost'].sum() if 'contribution_cost' in date_agg.columns else (total_product_cost + total_packaging_cost + total_shipping_net + total_ad_spend)
        total_contribution_profit = date_agg['contribution_profit'].sum() if 'contribution_profit' in date_agg.columns else (total_revenue - total_contribution_cost)
        # Break-even CAC is based on contribution before ad spend:
        # Revenue - Product Cost - Packaging - Net shipping (fixed overhead excluded by design).
        total_pre_ad_contribution = total_revenue - total_product_cost - total_packaging_cost - total_shipping_net
        pre_ad_contribution_per_order = (total_pre_ad_contribution / total_orders) if total_orders > 0 else 0
        pre_ad_contribution_per_customer = (total_pre_ad_contribution / total_customers) if total_customers > 0 else 0

        total_new_customers = 0
        if clv_return_time_analysis is not None and not clv_return_time_analysis.empty and 'new_customers' in clv_return_time_analysis.columns:
            total_new_customers = clv_return_time_analysis['new_customers'].sum()
        current_fb_cac = (total_fb_spend / total_new_customers) if total_new_customers > 0 else 0
        blended_cac = (total_ad_spend / total_new_customers) if total_new_customers > 0 else 0
        # Keep units aligned: CAC is per acquired customer, so break-even must also be per customer.
        break_even_cac = pre_ad_contribution_per_customer
        break_even_cac_order_based = pre_ad_contribution_per_order
        cac_headroom = break_even_cac - current_fb_cac
        cac_headroom_pct = (cac_headroom / break_even_cac * 100) if break_even_cac != 0 else 0
        contribution_ltv_cac = (pre_ad_contribution_per_customer / current_fb_cac) if current_fb_cac > 0 else 0
        avg_return_cycle_days = None
        if clv_return_time_analysis is not None and not clv_return_time_analysis.empty and 'avg_return_time_days' in clv_return_time_analysis.columns:
            valid_return_days = clv_return_time_analysis['avg_return_time_days'].dropna()
            if not valid_return_days.empty:
                avg_return_cycle_days = float(valid_return_days.mean())

        # Estimated payback period:
        # orders = current FB CAC / pre-ad contribution per order
        # days = max(orders - 1, 0) * avg return cycle (if available)
        if pre_ad_contribution_per_order > 0:
            payback_orders = (current_fb_cac / pre_ad_contribution_per_order) if current_fb_cac > 0 else 0.0
        else:
            payback_orders = None

        if payback_orders is not None and avg_return_cycle_days is not None:
            payback_days_estimated = max(payback_orders - 1, 0) * avg_return_cycle_days
        else:
            payback_days_estimated = None

        # Optional investor-style lens: how many orders to recover CAC
        # after ad spend is already included in per-order contribution.
        post_ad_contribution_per_order = (total_contribution_profit / total_orders) if total_orders > 0 else 0
        if post_ad_contribution_per_order > 0:
            post_ad_payback_orders = (current_fb_cac / post_ad_contribution_per_order) if current_fb_cac > 0 else 0.0
        else:
            post_ad_payback_orders = None

        if post_ad_payback_orders is not None and avg_return_cycle_days is not None:
            post_ad_payback_days_estimated = max(post_ad_payback_orders - 1, 0) * avg_return_cycle_days
        else:
            post_ad_payback_days_estimated = None

        payback_weekly_orders = []
        payback_weekly_labels = []
        if clv_return_time_analysis is not None and not clv_return_time_analysis.empty and 'cac' in clv_return_time_analysis.columns:
            for _, row in clv_return_time_analysis.iterrows():
                label = str(row['week_start']) if 'week_start' in clv_return_time_analysis.columns else str(row.get('week', ''))
                cac_week = row.get('cac', 0) or 0
                weekly_payback = (cac_week / pre_ad_contribution_per_order) if pre_ad_contribution_per_order > 0 else 0
                payback_weekly_labels.append(label)
                payback_weekly_orders.append(round(float(weekly_payback), 2))

        # New vs Returning revenue split (order-level, deduplicated)
        orders_df = df[['order_num', 'customer_email', 'purchase_date', revenue_col]].drop_duplicates(subset=['order_num']).copy()
        orders_df['purchase_datetime'] = pd.to_datetime(orders_df['purchase_date'])
        first_purchase_map = orders_df.groupby('customer_email')['purchase_datetime'].min().to_dict()
        orders_df['is_returning'] = orders_df.apply(
            lambda row: row['purchase_datetime'] > first_purchase_map.get(row['customer_email'], row['purchase_datetime']),
            axis=1
        )
        new_revenue = float(orders_df.loc[~orders_df['is_returning'], revenue_col].sum())
        returning_revenue = float(orders_df.loc[orders_df['is_returning'], revenue_col].sum())
        total_split_revenue = new_revenue + returning_revenue
        new_revenue_share_pct = (new_revenue / total_split_revenue * 100) if total_split_revenue > 0 else 0
        returning_revenue_share_pct = (returning_revenue / total_split_revenue * 100) if total_split_revenue > 0 else 0

        metrics = {
            'roas': round(total_revenue / total_ad_spend, 2) if total_ad_spend > 0 else 0,
            'roas_fb': round(total_revenue / total_fb_spend, 2) if total_fb_spend > 0 else 0,
            'roas_google': round(total_revenue / total_google_spend, 2) if total_google_spend > 0 else 0,
            'mer': round(total_revenue / total_ad_spend, 2) if total_ad_spend > 0 else 0,
            'revenue_per_customer': round(total_revenue / total_customers, 2) if total_customers > 0 else 0,
            'orders_per_customer': round(total_orders / total_customers, 2) if total_customers > 0 else 0,
            'cost_per_order': round(total_company_cost / total_orders, 2) if total_orders > 0 else 0,
            'profit_margin_pct': round(total_company_profit / total_revenue * 100, 1) if total_revenue > 0 else 0,  # backward-compatible key (company margin)
            'company_profit_margin_pct': round(total_company_profit / total_revenue * 100, 1) if total_revenue > 0 else 0,
            'product_gross_margin_pct': round((total_revenue - total_product_cost) / total_revenue * 100, 1) if total_revenue > 0 else 0,
            'pre_ad_contribution_margin_pct': round(total_pre_ad_contribution / total_revenue * 100, 1) if total_revenue > 0 else 0,
            'contribution_margin_pct': round(total_contribution_profit / total_revenue * 100, 1) if total_revenue > 0 else 0,
            'post_ad_contribution_margin_pct': round(total_contribution_profit / total_revenue * 100, 1) if total_revenue > 0 else 0,
            'company_net_profit': round(total_company_profit, 2),
            'pre_ad_contribution_profit': round(total_pre_ad_contribution, 2),
            'contribution_profit': round(total_contribution_profit, 2),
            'post_ad_contribution_profit': round(total_contribution_profit, 2),
            'pre_ad_contribution_profit_per_order': round(total_pre_ad_contribution / total_orders, 2) if total_orders > 0 else 0,
            'contribution_profit_per_order': round(total_contribution_profit / total_orders, 2) if total_orders > 0 else 0,
            'post_ad_contribution_profit_per_order': round(total_contribution_profit / total_orders, 2) if total_orders > 0 else 0,
            'pre_ad_contribution_per_order': round(pre_ad_contribution_per_order, 2),
            'pre_ad_contribution_per_customer': round(pre_ad_contribution_per_customer, 2),
            'cm1_profit': round(total_pre_ad_contribution, 2),
            'cm1_margin_pct': round(total_pre_ad_contribution / total_revenue * 100, 1) if total_revenue > 0 else 0,
            'cm1_profit_per_order': round(pre_ad_contribution_per_order, 2),
            'cm1_profit_per_customer': round(pre_ad_contribution_per_customer, 2),
            'cm2_profit': round(total_contribution_profit, 2),
            'cm2_margin_pct': round(total_contribution_profit / total_revenue * 100, 1) if total_revenue > 0 else 0,
            'cm2_profit_per_order': round(total_contribution_profit / total_orders, 2) if total_orders > 0 else 0,
            'cm3_profit': round(total_company_profit, 2),
            'cm3_margin_pct': round(total_company_profit / total_revenue * 100, 1) if total_revenue > 0 else 0,
            'cm3_profit_per_order': round(total_company_profit / total_orders, 2) if total_orders > 0 else 0,
            'cm_taxonomy_payment_fees_mode': 'excluded_not_modeled',
            'cm_taxonomy_note': 'CM1 excludes payment fees because the current reporting model does not ingest them separately.',
            'break_even_cac': round(break_even_cac, 2),
            'break_even_cac_order_based': round(break_even_cac_order_based, 2),
            'current_fb_cac': round(current_fb_cac, 2),
            'paid_cac': round(current_fb_cac, 2),
            'blended_cac': round(blended_cac, 2),
            'blended_cac_scope': 'tracked_ads_fb_google',
            'cac_headroom': round(cac_headroom, 2),
            'cac_headroom_pct': round(cac_headroom_pct, 1),
            'contribution_ltv_cac': round(contribution_ltv_cac, 2),
            'payback_orders': round(payback_orders, 2) if payback_orders is not None else None,
            'payback_days_estimated': round(payback_days_estimated, 1) if payback_days_estimated is not None else None,
            'post_ad_payback_orders': round(post_ad_payback_orders, 2) if post_ad_payback_orders is not None else None,
            'post_ad_payback_days_estimated': round(post_ad_payback_days_estimated, 1) if post_ad_payback_days_estimated is not None else None,
            'payback_days_note': 'estimate_based_on_avg_return_cycle',
            'avg_return_cycle_days': round(avg_return_cycle_days, 1) if avg_return_cycle_days is not None else None,
            'payback_weekly_orders': payback_weekly_orders,
            'payback_weekly_labels': payback_weekly_labels,
            'new_revenue': round(new_revenue, 2),
            'returning_revenue': round(returning_revenue, 2),
            'new_revenue_share_pct': round(new_revenue_share_pct, 1),
            'returning_revenue_share_pct': round(returning_revenue_share_pct, 1),
            'ad_spend_per_order': round(total_ad_spend / total_orders, 2),
            'total_ad_spend': round(total_ad_spend, 2),
            'total_revenue': round(total_revenue, 2),
            'total_packaging_cost': round(total_packaging_cost, 2),
            'total_shipping_subsidy': round(total_shipping_net, 2),  # backward-compatible key
            'total_shipping_net': round(total_shipping_net, 2),
            'shipping_net_semantics': 'positive_cost_negative_profit',
            'total_fixed_overhead': round(total_fixed_overhead, 2),
            'total_new_customers': int(total_new_customers),
            'total_orders': total_orders,
            'total_customers': total_customers
        }

        # Weekly profit margin trend
        if 'net_profit' in date_agg.columns and 'total_revenue' in date_agg.columns:
            date_agg['profit_margin_pct'] = (date_agg['net_profit'] / date_agg['total_revenue'] * 100).round(1)

        print(f"Financial metrics calculated: ROAS={metrics['roas']}x")
        return metrics

    def validate_metric_consistency(self, date_agg: pd.DataFrame, financial_metrics: dict, clv_return_time_analysis: pd.DataFrame = None) -> dict:
        """Validate key metric equations to catch data consistency issues early."""
        checks = {}

        total_revenue = date_agg['total_revenue'].sum() if 'total_revenue' in date_agg.columns else 0
        total_ad_spend = 0
        if 'fb_ads_spend' in date_agg.columns:
            total_ad_spend += date_agg['fb_ads_spend'].sum()
        if 'google_ads_spend' in date_agg.columns:
            total_ad_spend += date_agg['google_ads_spend'].sum()
        total_company_profit = date_agg['net_profit'].sum() if 'net_profit' in date_agg.columns else 0

        roas_expected = (total_revenue / total_ad_spend) if total_ad_spend > 0 else 0
        roas_reported = financial_metrics.get('roas', 0)
        checks['roas_expected'] = round(roas_expected, 4)
        checks['roas_reported'] = round(roas_reported, 4)
        checks['roas_delta'] = round(roas_reported - roas_expected, 4)
        checks['roas_ok'] = abs(checks['roas_delta']) <= 0.01

        margin_expected = (total_company_profit / total_revenue * 100) if total_revenue > 0 else 0
        margin_reported = financial_metrics.get('company_profit_margin_pct', financial_metrics.get('profit_margin_pct', 0))
        checks['company_margin_expected_pct'] = round(margin_expected, 4)
        checks['company_margin_reported_pct'] = round(margin_reported, 4)
        checks['company_margin_delta_pct'] = round(margin_reported - margin_expected, 4)
        checks['company_margin_ok'] = abs(checks['company_margin_delta_pct']) <= 0.05

        if clv_return_time_analysis is not None and not clv_return_time_analysis.empty:
            total_new_customers = clv_return_time_analysis['new_customers'].sum() if 'new_customers' in clv_return_time_analysis.columns else 0
            total_fb_spend = clv_return_time_analysis['fb_ads_spend'].sum() if 'fb_ads_spend' in clv_return_time_analysis.columns else 0
            cac_expected = (total_fb_spend / total_new_customers) if total_new_customers > 0 else 0
            cac_reported = financial_metrics.get('current_fb_cac', cac_expected)
            cac_delta = cac_reported - cac_expected
            total_orders = date_agg['unique_orders'].sum() if 'unique_orders' in date_agg.columns else 0
            cac_if_orders = (total_fb_spend / total_orders) if total_orders > 0 else 0
            checks['cac_expected'] = round(cac_expected, 4)
            checks['cac_reported'] = round(cac_reported, 4)
            checks['cac_delta'] = round(cac_delta, 4)
            checks['cac_formula'] = 'fb_ads_spend / new_customers'
            checks['cac_new_customers'] = int(total_new_customers)
            checks['cac_if_orders_denominator'] = round(cac_if_orders, 4)
            checks['cac_spend_source'] = 'fb_ads_spend'
            checks['cac_ok'] = abs(checks['cac_delta']) <= 0.01
        else:
            checks['cac_expected'] = 0
            checks['cac_reported'] = 0
            checks['cac_delta'] = 0
            checks['cac_formula'] = 'n/a'
            checks['cac_new_customers'] = 0
            checks['cac_if_orders_denominator'] = 0
            checks['cac_spend_source'] = 'n/a'
            checks['cac_ok'] = False

        if not checks['roas_ok']:
            logger.warning(f"Consistency check failed: ROAS mismatch (expected={checks['roas_expected']}, reported={checks['roas_reported']})")
        if not checks['company_margin_ok']:
            logger.warning(
                "Consistency check failed: Company margin mismatch "
                f"(expected={checks['company_margin_expected_pct']}%, reported={checks['company_margin_reported_pct']}%)"
            )

        logger.info(
            "Consistency checks: "
            f"roas_ok={checks['roas_ok']}, "
            f"company_margin_ok={checks['company_margin_ok']}, "
            f"cac_ok={checks['cac_ok']}"
        )

        return checks

    def analyze_order_status(self, df: pd.DataFrame) -> pd.DataFrame:
        """Analyze order status distribution"""
        print("\nAnalyzing order status distribution...")

        status_agg = df.groupby('status_name').agg({
            'order_num': 'nunique',
            'item_total_without_tax': 'sum'
        }).reset_index()
        status_agg.columns = ['status', 'orders', 'revenue']
        status_agg = status_agg.sort_values('orders', ascending=False)
        status_agg['orders_pct'] = (status_agg['orders'] / status_agg['orders'].sum() * 100).round(1)

        print(f"Order status analysis complete: {len(status_agg)} statuses")
        return status_agg

    def analyze_refunds(self, df: pd.DataFrame) -> dict:
        """
        Analyze refund/return rate trend.

        Current source: order statuses (e.g. Vratene/Vraceno/Returned).
        Note: explicit credit-note document type is not exposed in current reporting dataset.
        """
        print("\nAnalyzing refunds/returns...")
        revenue_col = 'order_revenue_net' if 'order_revenue_net' in df.columns else 'order_total'

        orders_df = df[['order_num', 'purchase_date', 'status_name', revenue_col]].drop_duplicates(subset=['order_num']).copy()
        orders_df['purchase_datetime'] = pd.to_datetime(orders_df['purchase_date'])
        orders_df['date'] = orders_df['purchase_datetime'].dt.date
        orders_df['status_name'] = orders_df['status_name'].fillna('').astype(str)

        def normalize_status(status: str) -> str:
            # Normalize to lowercase ASCII to avoid diacritics/encoding mismatches.
            normalized = unicodedata.normalize('NFKD', status)
            return ''.join(ch for ch in normalized if not unicodedata.combining(ch)).lower()

        orders_df['status_name_norm'] = orders_df['status_name'].apply(normalize_status)

        explicit_refund_statuses = {
            'vratene',
            'vraceno',
            'returned',
            'refunded',
        }

        orders_df['is_refund'] = orders_df['status_name_norm'].apply(
            lambda s: (s in explicit_refund_statuses)
            or ('vraten' in s)
            or ('vracen' in s)
            or ('refund' in s)
            or ('dobropis' in s)
        )

        daily_total = orders_df.groupby('date').agg({
            'order_num': 'nunique'
        }).reset_index().rename(columns={'order_num': 'total_orders'})

        daily_refunds = orders_df[orders_df['is_refund']].groupby('date').agg({
            'order_num': 'nunique',
            revenue_col: 'sum'
        }).reset_index().rename(columns={
            'order_num': 'refund_orders',
            revenue_col: 'refund_amount'
        })

        daily = daily_total.merge(daily_refunds, on='date', how='left')
        daily['refund_orders'] = daily['refund_orders'].fillna(0).astype(int)
        daily['refund_amount'] = daily['refund_amount'].fillna(0).round(2)
        daily['refund_rate_pct'] = daily.apply(
            lambda row: round((row['refund_orders'] / row['total_orders'] * 100) if row['total_orders'] > 0 else 0, 2),
            axis=1
        )
        daily = daily.sort_values('date')

        total_orders = int(daily['total_orders'].sum())
        total_refund_orders = int(daily['refund_orders'].sum())
        total_refund_amount = float(daily['refund_amount'].sum())
        refund_rate_pct = round((total_refund_orders / total_orders * 100) if total_orders > 0 else 0, 2)

        summary = {
            'total_orders': total_orders,
            'refund_orders': total_refund_orders,
            'refund_rate_pct': refund_rate_pct,
            'refund_amount': round(total_refund_amount, 2),
            'source': 'order_status'
        }

        print(
            f"Refund analysis complete: {total_refund_orders}/{total_orders} orders "
            f"({refund_rate_pct:.2f}%), amount={total_refund_amount:.2f} EUR"
        )

        return {
            'summary': summary,
            'daily': daily
        }

    def analyze_ads_effectiveness(self, df: pd.DataFrame) -> dict:
        """Analyze relationship between ad spend and orders/revenue"""
        print("\nAnalyzing ads effectiveness...")

        # Convert to date only (remove time component)
        df['purchase_date_only'] = pd.to_datetime(df['purchase_date']).dt.date

        # Daily aggregation for correlation - use date only
        daily_data = df.groupby('purchase_date_only').agg({
            'order_num': 'nunique',
            'item_total_without_tax': 'sum',
            'fb_ads_daily_spend': 'first',
            'google_ads_daily_spend': 'first',
            'profit_before_ads': 'sum'
        }).reset_index()
        daily_data.columns = ['date', 'orders', 'revenue', 'fb_spend', 'google_spend', 'profit']
        daily_data['total_ad_spend'] = daily_data['fb_spend'] + daily_data['google_spend']

        # Calculate correlations
        correlations = {}
        if len(daily_data) > 5:
            correlations['fb_orders'] = round(daily_data['fb_spend'].corr(daily_data['orders']), 3)
            correlations['fb_revenue'] = round(daily_data['fb_spend'].corr(daily_data['revenue']), 3)
            correlations['google_orders'] = round(daily_data['google_spend'].corr(daily_data['orders']), 3)
            correlations['google_revenue'] = round(daily_data['google_spend'].corr(daily_data['revenue']), 3)
            correlations['total_ads_orders'] = round(daily_data['total_ad_spend'].corr(daily_data['orders']), 3)
            correlations['total_ads_revenue'] = round(daily_data['total_ad_spend'].corr(daily_data['revenue']), 3)
            correlations['total_ads_profit'] = round(daily_data['total_ad_spend'].corr(daily_data['profit']), 3)
            correlations['spend_orders_correlation'] = correlations['total_ads_orders']
            correlations['spend_revenue_correlation'] = correlations['total_ads_revenue']
            correlations['spend_profit_correlation'] = correlations['total_ads_profit']

        # Calculate optimal spend ranges with 10â‚¬ increments
        # Group by spend ranges and calculate average orders/revenue
        max_spend = daily_data['fb_spend'].max()
        # Create bins in 10â‚¬ increments up to the max spend
        spend_bins = list(range(0, int(max_spend) + 20, 10))
        spend_labels = [f'{spend_bins[i]}-{spend_bins[i+1]}â‚¬' for i in range(len(spend_bins) - 1)]
        daily_data['fb_spend_range'] = pd.cut(daily_data['fb_spend'], bins=spend_bins, labels=spend_labels, include_lowest=True)
        spend_effectiveness = daily_data.groupby('fb_spend_range', observed=True).agg({
            'orders': 'mean',
            'revenue': 'mean',
            'fb_spend': 'mean',
            'profit': 'mean'
        }).reset_index()
        spend_effectiveness.columns = ['spend_range', 'avg_orders', 'avg_revenue', 'avg_spend', 'avg_profit']

        # Calculate ROAS per spend range
        spend_effectiveness['roas'] = (spend_effectiveness['avg_revenue'] / spend_effectiveness['avg_spend']).round(2)
        spend_effectiveness['roas'] = spend_effectiveness['roas'].replace([float('inf'), float('-inf')], 0).fillna(0)

        # Find best performing spend range
        best_roas_range = spend_effectiveness.loc[spend_effectiveness['roas'].idxmax(), 'spend_range'] if not spend_effectiveness.empty else 'N/A'
        best_profit_range = spend_effectiveness.loc[spend_effectiveness['avg_profit'].idxmax(), 'spend_range'] if not spend_effectiveness.empty else 'N/A'

        # Day of week ad effectiveness
        daily_data['day_of_week'] = pd.to_datetime(daily_data['date']).dt.day_name()
        dow_effectiveness = daily_data.groupby('day_of_week').agg({
            'fb_spend': 'mean',
            'orders': 'mean',
            'revenue': 'mean',
            'profit': 'mean'
        }).reset_index()
        dow_effectiveness['roas'] = (dow_effectiveness['revenue'] / dow_effectiveness['fb_spend']).round(2)
        dow_effectiveness['roas'] = dow_effectiveness['roas'].replace([float('inf'), float('-inf')], 0).fillna(0)

        # Order days by weekday
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        dow_effectiveness['day_order'] = dow_effectiveness['day_of_week'].map({d: i for i, d in enumerate(day_order)})
        dow_effectiveness = dow_effectiveness.sort_values('day_order')
        dow_effectiveness['day_name'] = dow_effectiveness['day_of_week']
        dow_effectiveness['avg_fb_spend'] = dow_effectiveness['fb_spend']
        dow_effectiveness['avg_orders'] = dow_effectiveness['orders']
        dow_effectiveness['avg_revenue'] = dow_effectiveness['revenue']
        dow_effectiveness['avg_profit'] = dow_effectiveness['profit']

        result = {
            'correlations': correlations,
            'spend_effectiveness': spend_effectiveness,
            'dow_effectiveness': dow_effectiveness,
            'best_roas_range': best_roas_range,
            'best_profit_range': best_profit_range,
            'daily_data': daily_data[['date', 'orders', 'revenue', 'fb_spend', 'google_spend', 'profit']].copy()
        }

        # Recommendations
        recommendations = []
        if correlations.get('fb_orders', 0) > 0.3:
            recommendations.append("Strong positive correlation between FB spend and orders - increasing spend likely effective")
        elif correlations.get('fb_orders', 0) < 0:
            recommendations.append("Negative correlation between FB spend and orders - consider optimizing ad targeting")

        if correlations.get('fb_revenue', 0) > correlations.get('fb_orders', 0):
            recommendations.append("FB ads drive higher value orders - focus on revenue optimization")

        result['recommendations'] = recommendations

        print(f"Ads effectiveness analysis complete. FB-Orders correlation: {correlations.get('fb_orders', 'N/A')}")
        return result

    def analyze_cost_per_order(self, df: pd.DataFrame, fb_campaigns: list = None, reference_total_revenue: float = None) -> dict:
        """
        Analyze estimated Cost Per Order (CPO) using time-based correlation.
        Since we don't have direct attribution, we estimate which campaigns drive orders.

        Args:
            df: Item-level dataframe
            fb_campaigns: Campaign-level Facebook data
            reference_total_revenue: Optional normalized revenue total used across report sections
        """
        print("\nAnalyzing Cost Per Order estimation...")

        # Convert to date only
        df['purchase_date_only'] = pd.to_datetime(df['purchase_date']).dt.date

        # Daily aggregation
        daily_data = df.groupby('purchase_date_only').agg({
            'order_num': 'nunique',
            'item_total_without_tax': 'sum',
            'fb_ads_daily_spend': 'first',
            'google_ads_daily_spend': 'first'
        }).reset_index()
        daily_data.columns = ['date', 'orders', 'revenue', 'fb_spend', 'google_spend']
        daily_data['total_ad_spend'] = daily_data['fb_spend'] + daily_data['google_spend']
        daily_data['date'] = pd.to_datetime(daily_data['date'])

        result = {
            'daily_cpo': [],
            'overall_cpo': 0,
            'fb_cpo': 0,
            'time_lagged_analysis': {},
            'campaign_attribution': [],
            'cpo_trend': [],
            'best_cpo_days': [],
            'worst_cpo_days': []
        }

        total_orders = daily_data['orders'].sum()
        total_fb_spend = daily_data['fb_spend'].sum()
        total_google_spend = daily_data['google_spend'].sum()
        total_ad_spend = total_fb_spend + total_google_spend
        total_revenue_raw = daily_data['revenue'].sum()
        total_revenue = float(reference_total_revenue) if reference_total_revenue is not None else total_revenue_raw

        # Overall CPO
        result['overall_cpo'] = total_ad_spend / total_orders if total_orders > 0 else 0
        result['fb_cpo'] = total_fb_spend / total_orders if total_orders > 0 else 0
        result['google_cpo'] = total_google_spend / total_orders if total_orders > 0 else 0
        result['total_orders'] = total_orders
        result['total_fb_spend'] = total_fb_spend
        result['total_revenue'] = total_revenue
        result['total_revenue_raw'] = round(total_revenue_raw, 2)
        result['total_revenue_source'] = 'financial_metrics.total_revenue' if reference_total_revenue is not None else 'daily_data.revenue'
        result['fb_spend_reconciliation'] = {
            'daily_source_spend': round(total_fb_spend, 2),
            'campaign_source_spend': None,
            'difference': None,
            'difference_pct': None
        }

        # Daily CPO trend
        daily_cpo = []
        for _, row in daily_data.iterrows():
            if row['orders'] > 0:
                cpo = row['fb_spend'] / row['orders']
                daily_cpo.append({
                    'date': row['date'].strftime('%Y-%m-%d'),
                    'orders': int(row['orders']),
                    'fb_spend': row['fb_spend'],
                    'revenue': row['revenue'],
                    'cpo': cpo,
                    'roas': row['revenue'] / row['fb_spend'] if row['fb_spend'] > 0 else 0
                })
        result['daily_cpo'] = daily_cpo

        # Time-lagged correlation analysis (do orders follow spend with delay?)
        if len(daily_data) > 7:
            # Calculate correlation with different time lags
            time_lags = {}
            for lag in range(0, 4):  # 0 to 3 day lag
                if lag == 0:
                    orders_shifted = daily_data['orders']
                else:
                    orders_shifted = daily_data['orders'].shift(-lag)

                valid_mask = ~orders_shifted.isna()
                if valid_mask.sum() > 5:
                    corr = daily_data.loc[valid_mask, 'fb_spend'].corr(orders_shifted[valid_mask])
                    time_lags[f'{lag}_day'] = round(corr, 3) if not pd.isna(corr) else 0

            result['time_lagged_analysis'] = time_lags

            # Find best lag
            if time_lags:
                best_lag = max(time_lags, key=lambda k: time_lags[k])
                result['best_attribution_lag'] = best_lag
                result['best_lag_correlation'] = time_lags[best_lag]

        # Weekly CPO trend (smoother than daily)
        daily_data['week'] = daily_data['date'].dt.isocalendar().week
        daily_data['year'] = daily_data['date'].dt.year
        weekly_data = daily_data.groupby(['year', 'week']).agg({
            'orders': 'sum',
            'fb_spend': 'sum',
            'revenue': 'sum',
            'date': 'min'
        }).reset_index()
        weekly_data['cpo'] = weekly_data['fb_spend'] / weekly_data['orders']
        weekly_data['cpo'] = weekly_data['cpo'].replace([float('inf'), float('-inf')], 0).fillna(0)

        result['weekly_cpo'] = [
            {
                'week_start': row['date'].strftime('%Y-%m-%d'),
                'orders': int(row['orders']),
                'fb_spend': row['fb_spend'],
                'cpo': row['cpo']
            }
            for _, row in weekly_data.iterrows()
        ]

        # Campaign attribution estimation (proportional to clicks/spend)
        if fb_campaigns:
            total_campaign_spend = sum(c.get('spend', 0) for c in fb_campaigns)
            total_campaign_clicks = sum(c.get('clicks', 0) for c in fb_campaigns)
            diff = total_fb_spend - total_campaign_spend
            diff_pct = (diff / total_fb_spend * 100) if total_fb_spend > 0 else 0
            result['fb_spend_reconciliation'] = {
                'daily_source_spend': round(total_fb_spend, 2),
                'campaign_source_spend': round(total_campaign_spend, 2),
                'difference': round(diff, 2),
                'difference_pct': round(diff_pct, 2),
                'coverage_ratio': round((total_campaign_spend / total_fb_spend), 4) if total_fb_spend > 0 else None,
            }

            campaign_attribution = []
            for campaign in fb_campaigns:
                spend = campaign.get('spend', 0)
                clicks = campaign.get('clicks', 0)

                if spend > 0:
                    # Estimate orders proportionally to spend
                    spend_share = spend / total_campaign_spend if total_campaign_spend > 0 else 0
                    estimated_orders_by_spend = total_orders * spend_share

                    # Estimate orders proportionally to clicks
                    click_share = clicks / total_campaign_clicks if total_campaign_clicks > 0 else 0
                    estimated_orders_by_clicks = total_orders * click_share

                    # Weighted average (60% clicks, 40% spend as clicks are better signal)
                    estimated_orders = estimated_orders_by_clicks * 0.6 + estimated_orders_by_spend * 0.4

                    # Calculate estimated CPO for campaign
                    estimated_cpo = spend / estimated_orders if estimated_orders > 0 else 0

                    # Calculate estimated revenue share
                    revenue_share = estimated_orders / total_orders if total_orders > 0 else 0
                    estimated_revenue = total_revenue * revenue_share

                    # ROAS for campaign
                    estimated_roas = estimated_revenue / spend if spend > 0 else 0

                    campaign_attribution.append({
                        'campaign_name': campaign.get('campaign_name', 'Unknown'),
                        'campaign_id': campaign.get('campaign_id', ''),
                        'spend': spend,
                        'clicks': clicks,
                        'impressions': campaign.get('impressions', 0),
                        'ctr': campaign.get('ctr', 0),
                        'cpc': campaign.get('cpc', 0),
                        'estimated_orders': round(estimated_orders, 1),
                        'attributed_orders_est': round(estimated_orders, 1),
                        'estimated_cpo': round(estimated_cpo, 2),
                        'cost_per_attributed_order': round(estimated_cpo, 2),
                        'estimated_revenue': round(estimated_revenue, 2),
                        'estimated_roas': round(estimated_roas, 2),
                        'spend_share_pct': round(spend_share * 100, 1),
                        'click_share_pct': round(click_share * 100, 1),
                        'attribution_method': '0.6_click_share + 0.4_spend_share',
                    })

            # Sort by estimated CPO (best first)
            campaign_attribution.sort(key=lambda x: x['estimated_cpo'] if x['estimated_cpo'] > 0 else float('inf'))
            result['campaign_attribution'] = campaign_attribution
            estimated_orders_total = sum(row['estimated_orders'] for row in campaign_attribution)
            result['campaign_attribution_summary'] = {
                'campaign_source_spend': round(total_campaign_spend, 2),
                'daily_source_spend': round(total_fb_spend, 2),
                'coverage_ratio': round((total_campaign_spend / total_fb_spend), 4) if total_fb_spend > 0 else None,
                'estimated_orders_total': round(estimated_orders_total, 1),
                'oversubscription_ratio': round((estimated_orders_total / total_orders), 4) if total_orders > 0 else None,
                'attribution_method': '0.6_click_share + 0.4_spend_share',
            }

        # Find best and worst CPO days
        if daily_cpo:
            sorted_by_cpo = sorted([d for d in daily_cpo if d['cpo'] > 0], key=lambda x: x['cpo'])
            result['best_cpo_days'] = sorted_by_cpo[:5]  # 5 best days
            result['worst_cpo_days'] = sorted_by_cpo[-5:][::-1]  # 5 worst days

        # Hourly order analysis (aggregate orders by hour of day)
        df['purchase_hour'] = pd.to_datetime(df['purchase_date']).dt.hour
        hourly_orders = df.groupby('purchase_hour').agg({
            'order_num': 'nunique',
            'item_total_without_tax': 'sum'
        }).reset_index()
        hourly_orders.columns = ['hour', 'orders', 'revenue']

        result['hourly_orders'] = [
            {
                'hour': int(row['hour']),
                'orders': int(row['orders']),
                'revenue': float(row['revenue'])
            }
            for _, row in hourly_orders.iterrows()
        ]

        print(f"Cost Per Order analysis complete. Overall FB CPO: â‚¬{result['fb_cpo']:.2f}")
        return result

    def display_returning_customers_analysis(self, analysis: pd.DataFrame):
        """Display returning customers analysis"""
        print("\n" + "="*120)
        print("RETURNING CUSTOMERS ANALYSIS - WEEKLY AGGREGATION")
        print("="*120)
        
        print(f"\n{'Week':>10} {'Week Start':>12} {'Total Orders':>13} {'New':>8} {'New %':>8} {'Returning':>11} {'Return %':>10} {'Unique Customers':>17}")
        print("-"*120)
        
        total_orders = 0
        total_new = 0
        total_returning = 0
        total_unique = 0
        
        for _, row in analysis.iterrows():
            week_str = str(row['week'])
            week_start = row['week_start'].strftime('%Y-%m-%d')
            print(f"{week_str:>10} {week_start:>12} {row['total_orders']:>13} "
                  f"{row['new_orders']:>8} {row['new_percentage']:>7.1f}% "
                  f"{row['returning_orders']:>11} {row['returning_percentage']:>9.1f}% "
                  f"{row['unique_customers']:>17}")
            
            total_orders += row['total_orders']
            total_new += row['new_orders']
            total_returning += row['returning_orders']
            total_unique += row['unique_customers']
        
        # Calculate overall percentages
        overall_new_pct = (total_new / total_orders * 100) if total_orders > 0 else 0
        overall_returning_pct = (total_returning / total_orders * 100) if total_orders > 0 else 0
        
        print("-"*120)
        print(f"{'TOTAL':>10} {' ':>12} {total_orders:>13} "
              f"{total_new:>8} {overall_new_pct:>7.1f}% "
              f"{total_returning:>11} {overall_returning_pct:>9.1f}% "
              f"{total_unique:>17}")
        
        print("\n")
    
    def display_clv_return_time_analysis(self, analysis: pd.DataFrame):
        """Display CLV and return time analysis"""
        print("\n" + "="*160)
        print("CUSTOMER LIFETIME VALUE, CAC & RETURN TIME ANALYSIS - WEEKLY AGGREGATION")
        print("="*160)
        
        print(f"\n{'Week':>10} {'Week Start':>12} {'Customers':>10} {'New':>8} {'Returning':>10} {'Avg CLV (â‚¬)':>12} {'Cumulative CLV (â‚¬)':>18} {'CAC (â‚¬)':>10} {'Avg Return Days':>16} {'Revenue (â‚¬)':>12}")
        print("-"*160)
        
        total_customers = 0
        total_new = 0
        total_returning = 0
        total_revenue = 0
        
        for _, row in analysis.iterrows():
            week_str = str(row['week'])
            week_start = row['week_start'].strftime('%Y-%m-%d')
            return_time = f"{row['avg_return_time_days']:.1f}" if pd.notna(row['avg_return_time_days']) else "N/A"
            cac = row.get('cac', 0)
            
            print(f"{week_str:>10} {week_start:>12} {row['unique_customers']:>10} "
                  f"{row['new_customers']:>8} {row['returning_customers']:>10} "
                  f"{row['avg_clv']:>12.2f} {row['cumulative_avg_clv']:>18.2f} "
                  f"{cac:>10.2f} "
                  f"{return_time:>16} {row['total_revenue']:>12.2f}")
            
            total_customers += row['unique_customers']
            total_new += row['new_customers']
            total_returning += row['returning_customers']
            total_revenue += row['total_revenue']
        
        # Calculate overall averages
        overall_avg_clv = analysis['avg_clv'].mean()
        final_cumulative_clv = analysis['cumulative_avg_clv'].iloc[-1] if not analysis.empty else 0
        overall_avg_return = analysis['avg_return_time_days'].mean()
        return_time_str = f"{overall_avg_return:.1f}" if pd.notna(overall_avg_return) else "N/A"
        
        # Calculate overall CAC
        total_fb_spend = analysis['fb_ads_spend'].sum() if 'fb_ads_spend' in analysis.columns else 0
        overall_cac = total_fb_spend / total_new if total_new > 0 else 0
        
        print("-"*160)
        print(f"{'TOTAL':>10} {' ':>12} {total_customers:>10} "
              f"{total_new:>8} {total_returning:>10} "
              f"{overall_avg_clv:>12.2f} {final_cumulative_clv:>18.2f} "
              f"{overall_cac:>10.2f} "
              f"{return_time_str:>16} {total_revenue:>12.2f}")
        
        print("\n")

    def analyze_customer_email_segments(self, df: pd.DataFrame, all_orders_raw: list = None) -> dict:
        """
        Analyze customers and segment them for email marketing campaigns.

        Segments:
        1. One-time buyers (inactive 30+ days): Bought once with status "OdoslanĂˇ",
           order was at least 30 days ago
        2. Repeat buyers (inactive 90+ days): Bought 2+ times with status "OdoslanĂˇ",
           last order was 90+ days ago
        3. Failed payment customers: All orders have status "Platba online - platnosĹĄ vyprĹˇala"
           or "Platba online - platba zamietnutĂˇ"
        4. Additional segments discovered from data patterns

        Returns dict with DataFrames for each segment
        """
        print("\nAnalyzing customer segments for email marketing...")
        revenue_col = 'order_revenue_net' if 'order_revenue_net' in df.columns else 'order_total'

        today = datetime.now()

        # Convert purchase_date to datetime if not already
        df['purchase_datetime'] = pd.to_datetime(df['purchase_date'])

        # Get unique orders with customer info
        orders_df = df[['order_num', 'customer_email', 'customer_name', 'purchase_datetime',
                        'status_name', revenue_col, 'invoice_city', 'invoice_country']].drop_duplicates(subset=['order_num'])

        # Filter to only "OdoslanĂˇ" (shipped) orders for segments 1 and 2
        shipped_orders = orders_df[orders_df['status_name'] == 'OdoslanĂˇ'].copy()

        # Calculate per-customer stats from shipped orders
        customer_stats = shipped_orders.groupby('customer_email').agg({
            'order_num': 'count',
            'purchase_datetime': ['min', 'max'],
            revenue_col: 'sum',
            'customer_name': 'first',
            'invoice_city': 'first',
            'invoice_country': 'first'
        }).reset_index()
        customer_stats.columns = ['email', 'order_count', 'first_order_date', 'last_order_date',
                                   'total_revenue', 'name', 'city', 'country']

        # Calculate days since last order
        customer_stats['days_since_last_order'] = (today - customer_stats['last_order_date']).dt.days
        customer_stats['days_since_first_order'] = (today - customer_stats['first_order_date']).dt.days

        segments = {}

        # ==== SEGMENT 1: One-time buyers inactive 30+ days ====
        # Customers with exactly 1 order, first order was 30+ days ago
        one_time_inactive = customer_stats[
            (customer_stats['order_count'] == 1) &
            (customer_stats['days_since_first_order'] >= 30)
        ].copy()
        one_time_inactive = one_time_inactive.sort_values('days_since_first_order', ascending=False)
        segments['one_time_buyers_30_days'] = {
            'data': one_time_inactive,
            'description': 'ZĂˇkaznĂ­ci, ktorĂ­ nakĂşpili raz (objednĂˇvka "OdoslanĂˇ") a od objednĂˇvky uplynulo viac ako 30 dnĂ­',
            'description_en': 'Customers who bought once (status "Shipped") and order was 30+ days ago',
            'count': len(one_time_inactive),
            'email_purpose': 'Re-engagement - motivĂˇcia k druhĂ©mu nĂˇkupu',
            'send_timing': '30-45 dnĂ­ po prvej objednĂˇvke',
            'send_timing_en': '30-45 days after first order',
            'priority': 3,
            'discount_suggestion': '15% na druhĂş objednĂˇvku',
            'email_template': 'ChĂ˝bate nĂˇm! Tu je 15% zÄľava na VaĹˇu ÄŹalĹˇiu objednĂˇvku.'
        }
        print(f"  Segment 1 (One-time buyers, 30+ days inactive): {len(one_time_inactive)} customers")

        # ==== SEGMENT 2: Repeat buyers inactive 90+ days ====
        # Customers with 2+ orders, last order was 90+ days ago
        repeat_inactive = customer_stats[
            (customer_stats['order_count'] >= 2) &
            (customer_stats['days_since_last_order'] >= 90)
        ].copy()
        repeat_inactive = repeat_inactive.sort_values('days_since_last_order', ascending=False)
        segments['repeat_buyers_90_days'] = {
            'data': repeat_inactive,
            'description': 'ZĂˇkaznĂ­ci, ktorĂ­ nakĂşpili 2x a viac (objednĂˇvky "OdoslanĂˇ") ale poslednĂˇ objednĂˇvka bola pred 90+ dĹami',
            'description_en': 'Customers who bought 2+ times (status "Shipped") but last order was 90+ days ago',
            'count': len(repeat_inactive),
            'email_purpose': 'Win-back - nĂˇvrat vernĂ˝ch zĂˇkaznĂ­kov',
            'send_timing': 'IhneÄŹ - sĂş v riziku odchodu',
            'send_timing_en': 'Immediately - at risk of churning',
            'priority': 2,
            'discount_suggestion': '20% + doprava zadarmo',
            'email_template': 'VĂˇĹˇ obÄľĂşbenĂ˝ parfum ÄŤakĂˇ! Ĺ peciĂˇlna ponuka pre vernĂ˝ch zĂˇkaznĂ­kov.'
        }
        print(f"  Segment 2 (Repeat buyers, 90+ days inactive): {len(repeat_inactive)} customers")

        # ==== SEGMENT 3: Failed payment customers ====
        # Process raw orders to find customers with ONLY failed payments
        failed_payment_customers = pd.DataFrame()

        if all_orders_raw:
            # Extract customer emails from failed payment orders
            failed_statuses = ['Platba online - platnosĹĄ vyprĹˇala', 'Platba online - platba zamietnutĂˇ']

            failed_orders = []
            all_customer_orders = {}  # Track all orders per customer email

            for order in all_orders_raw:
                customer = order.get('customer', {}) or {}
                email = customer.get('email', '')
                status = (order.get('status', {}) or {}).get('name', '')

                if email:
                    if email not in all_customer_orders:
                        all_customer_orders[email] = {'failed': 0, 'other': 0, 'orders': []}

                    if status in failed_statuses:
                        all_customer_orders[email]['failed'] += 1
                        all_customer_orders[email]['orders'].append(order)
                    else:
                        all_customer_orders[email]['other'] += 1

            # Find customers with ONLY failed orders (no successful ones)
            failed_only_customers = []
            for email, data in all_customer_orders.items():
                if data['failed'] > 0 and data['other'] == 0:
                    # Get the latest order for customer info
                    latest_order = max(data['orders'], key=lambda x: x.get('pur_date', ''))
                    customer = latest_order.get('customer', {}) or {}
                    name = customer.get('company_name', '')
                    if not name:
                        name = f"{customer.get('name', '')} {customer.get('surname', '')}".strip()

                    failed_only_customers.append({
                        'email': email,
                        'name': name,
                        'failed_order_count': data['failed'],
                        'last_attempt_date': latest_order.get('pur_date', ''),
                        'city': (latest_order.get('invoice_address', {}) or {}).get('city', ''),
                        'country': (latest_order.get('invoice_address', {}) or {}).get('country', '')
                    })

            if failed_only_customers:
                failed_payment_customers = pd.DataFrame(failed_only_customers)
                failed_payment_customers['last_attempt_date'] = pd.to_datetime(failed_payment_customers['last_attempt_date'])
                failed_payment_customers = failed_payment_customers.sort_values('last_attempt_date', ascending=False)

        segments['failed_payment_only'] = {
            'data': failed_payment_customers,
            'description': 'ZĂˇkaznĂ­ci, ktorĂ­ nedokonÄŤili Ĺľiadnu objednĂˇvku - vĹˇetky ich objednĂˇvky majĂş stav "Platba online - platnosĹĄ vyprĹˇala" alebo "Platba online - platba zamietnutĂˇ"',
            'description_en': 'Customers who never completed any order - all their orders have failed payment status',
            'count': len(failed_payment_customers),
            'email_purpose': 'Recovery - pomoc s dokonÄŤenĂ­m objednĂˇvky',
            'send_timing': '24-48 hodĂ­n po neĂşspeĹˇnej platbe',
            'send_timing_en': '24-48 hours after failed payment',
            'priority': 1,
            'discount_suggestion': '10% + pomoc s platbou',
            'email_template': 'VaĹˇa objednĂˇvka ÄŤakĂˇ! PomĂ´Ĺľeme VĂˇm dokonÄŤiĹĄ nĂˇkup.'
        }
        print(f"  Segment 3 (Failed payment only): {len(failed_payment_customers)} customers")

        # ==== ADDITIONAL SEGMENTS ====

        # Segment 4: High-value one-time buyers (spent above average, haven't returned)
        avg_order_value = customer_stats['total_revenue'].mean()
        high_value_one_time = customer_stats[
            (customer_stats['order_count'] == 1) &
            (customer_stats['total_revenue'] > avg_order_value) &
            (customer_stats['days_since_first_order'] >= 14)  # At least 2 weeks ago
        ].copy()
        high_value_one_time = high_value_one_time.sort_values('total_revenue', ascending=False)
        segments['high_value_one_time'] = {
            'data': high_value_one_time,
            'description': f'ZĂˇkaznĂ­ci s jednou objednĂˇvkou nad priemernou hodnotu (â‚¬{avg_order_value:.2f}), ktorĂ­ sa nevrĂˇtili',
            'description_en': f'One-time buyers who spent above average (â‚¬{avg_order_value:.2f}) but never returned',
            'count': len(high_value_one_time),
            'email_purpose': 'VIP re-engagement - osobnejĹˇĂ­ prĂ­stup k hodnotnĂ˝m zĂˇkaznĂ­kom',
            'send_timing': '14-21 dnĂ­ po prvej objednĂˇvke',
            'send_timing_en': '14-21 days after first order',
            'priority': 2,
            'discount_suggestion': '15% + osobnĂˇ sprĂˇva',
            'email_template': 'ÄŽakujeme za veÄľkĂş objednĂˇvku! Pripravili sme pre VĂˇs exkluzĂ­vnu ponuku.'
        }
        print(f"  Segment 4 (High-value one-time): {len(high_value_one_time)} customers")

        # Segment 5: Recent buyers who might need refill (bought 14-60 days ago)
        recent_buyers = customer_stats[
            (customer_stats['days_since_last_order'] >= 14) &
            (customer_stats['days_since_last_order'] <= 60)
        ].copy()
        recent_buyers = recent_buyers.sort_values('days_since_last_order', ascending=True)
        segments['recent_buyers_14_60_days'] = {
            'data': recent_buyers,
            'description': 'ZĂˇkaznĂ­ci, ktorĂ­ nakĂşpili pred 14-60 dĹami - ideĂˇlny ÄŤas na pripomenutie',
            'description_en': 'Customers who bought 14-60 days ago - perfect time for a reminder',
            'count': len(recent_buyers),
            'email_purpose': 'Reminder - pripomenutie produktu, cross-sell',
            'send_timing': 'SegmentovaĹĄ podÄľa dnĂ­ a posielaĹĄ priebeĹľne',
            'send_timing_en': 'Segment by days and send continuously',
            'priority': 3,
            'discount_suggestion': 'Doprava zadarmo nad Xâ‚¬',
            'email_template': 'Nezabudnite na doplnenie zĂˇsob! MĂˇme pre VĂˇs novinky.'
        }
        print(f"  Segment 5 (Recent 14-60 days): {len(recent_buyers)} customers")

        # Segment 6: VIP customers (3+ orders) - for loyalty program
        vip_customers = customer_stats[
            customer_stats['order_count'] >= 3
        ].copy()
        vip_customers = vip_customers.sort_values('total_revenue', ascending=False)
        segments['vip_customers'] = {
            'data': vip_customers,
            'description': 'VIP zĂˇkaznĂ­ci - nakĂşpili 3x a viac, najvernejĹˇĂ­ zĂˇkaznĂ­ci',
            'description_en': 'VIP customers - bought 3+ times, most loyal customers',
            'count': len(vip_customers),
            'email_purpose': 'Loyalty - ĹˇpeciĂˇlne ponuky, poÄŹakovanie, program lojality',
            'send_timing': 'Pravidelne 1x mesaÄŤne',
            'send_timing_en': 'Regularly once a month',
            'priority': 4,
            'discount_suggestion': 'VIP zÄľava 15-20%, prednostnĂ˝ prĂ­stup k novinkĂˇm',
            'email_template': 'ExkluzĂ­vne pre VIP: NovĂˇ vĂ´Ĺa eĹˇte pred ostatnĂ˝mi!'
        }
        print(f"  Segment 6 (VIP 3+ orders): {len(vip_customers)} customers")

        # Segment 7: Churning customers (2+ orders, last 60-90 days ago)
        churning_customers = customer_stats[
            (customer_stats['order_count'] >= 2) &
            (customer_stats['days_since_last_order'] >= 60) &
            (customer_stats['days_since_last_order'] < 90)
        ].copy()
        churning_customers = churning_customers.sort_values('days_since_last_order', ascending=False)
        segments['churning_customers'] = {
            'data': churning_customers,
            'description': 'ZĂˇkaznĂ­ci v riziku odchodu - nakĂşpili 2x+, poslednĂˇ objednĂˇvka pred 60-90 dĹami',
            'description_en': 'At-risk customers - bought 2+ times, last order 60-90 days ago',
            'count': len(churning_customers),
            'email_purpose': 'Prevention - zabrĂˇniĹĄ strate zĂˇkaznĂ­ka',
            'send_timing': 'IhneÄŹ - poslednĂˇ Ĺˇanca pred stratou',
            'send_timing_en': 'Immediately - last chance before losing them',
            'priority': 1,
            'discount_suggestion': '20% + limitovanĂˇ ponuka',
            'email_template': 'VĹˇimli sme si, Ĺľe dlhĹˇie nenakupujete. MĂˇme pre VĂˇs ĹˇpeciĂˇlnu ponuku!'
        }
        print(f"  Segment 7 (Churning 60-90 days): {len(churning_customers)} customers")

        # Segment 8: Long-term dormant (180+ days since last order)
        long_dormant = customer_stats[
            customer_stats['days_since_last_order'] >= 180
        ].copy()
        long_dormant = long_dormant.sort_values('total_revenue', ascending=False)
        segments['long_dormant'] = {
            'data': long_dormant,
            'description': 'Dlhodobo neaktĂ­vni zĂˇkaznĂ­ci - poslednĂˇ objednĂˇvka pred 180+ dĹami',
            'description_en': 'Long-term dormant customers - last order 180+ days ago',
            'count': len(long_dormant),
            'email_purpose': 'Re-activation - agresĂ­vna zÄľava alebo ĹˇpeciĂˇlna ponuka',
            'send_timing': 'IhneÄŹ',
            'send_timing_en': 'Immediately',
            'priority': 5,
            'discount_suggestion': '20-30%'
        }
        print(f"  Segment 8 (Long dormant 180+ days): {len(long_dormant)} customers")

        # ==== NEW SEGMENTS BASED ON COHORT ANALYSIS ====

        # Segment 9: Sample buyers who haven't converted (bought sample set, no full-size)
        # Get orders with product info
        orders_with_products = df[['order_num', 'customer_email', 'item_label', 'purchase_datetime']].copy()

        # Identify sample set purchases
        sample_keywords = ['vzor', 'sample', 'sada vzor', 'vzoriek', 'vzorky']
        orders_with_products['is_sample'] = orders_with_products['item_label'].str.lower().apply(
            lambda x: any(kw in str(x).lower() for kw in sample_keywords) if pd.notna(x) else False
        )

        # Full-size products (200ml, 500ml bottles)
        fullsize_keywords = ['200ml', '500ml', '200 ml', '500 ml']
        orders_with_products['is_fullsize'] = orders_with_products['item_label'].str.lower().apply(
            lambda x: any(kw in str(x).lower() for kw in fullsize_keywords) if pd.notna(x) else False
        )

        # Group by customer
        customer_products = orders_with_products.groupby('customer_email').agg({
            'is_sample': 'any',
            'is_fullsize': 'any',
            'purchase_datetime': 'max'
        }).reset_index()
        customer_products.columns = ['email', 'bought_sample', 'bought_fullsize', 'last_order_date']
        customer_products['days_since_last'] = (today - customer_products['last_order_date']).dt.days

        # Sample buyers who never bought full-size, 7-30 days ago
        sample_not_converted = customer_products[
            (customer_products['bought_sample'] == True) &
            (customer_products['bought_fullsize'] == False) &
            (customer_products['days_since_last'] >= 7) &
            (customer_products['days_since_last'] <= 60)
        ].copy()

        # Merge with customer stats for more info
        sample_not_converted = sample_not_converted.merge(
            customer_stats[['email', 'name', 'order_count', 'total_revenue', 'city', 'country']],
            on='email', how='left'
        )
        sample_not_converted = sample_not_converted.sort_values('days_since_last', ascending=True)

        segments['sample_not_converted'] = {
            'data': sample_not_converted,
            'description': 'ZĂˇkaznĂ­ci, ktorĂ­ kĂşpili vzorky ale eĹˇte nekĂşpili plnĂş veÄľkosĹĄ (7-60 dnĂ­)',
            'description_en': 'Customers who bought samples but never bought full-size products (7-60 days ago)',
            'count': len(sample_not_converted),
            'email_purpose': 'Conversion - konverzia zo vzoriek na plnĂş veÄľkosĹĄ',
            'send_timing': '7-14 dnĂ­ po nĂˇkupe vzoriek',
            'send_timing_en': '7-14 days after sample purchase',
            'priority': 1,
            'discount_suggestion': '10-15% na prvĂş plnĂş veÄľkosĹĄ',
            'email_template': 'KtorĂˇ vĂ´Ĺa sa VĂˇm najviac pĂˇÄŤila? Teraz so zÄľavou X%!'
        }
        print(f"  Segment 9 (Sample not converted): {len(sample_not_converted)} customers")

        # Segment 10: Optimal reorder timing (approaching 20-day avg reorder time)
        # Customers with last order 15-25 days ago (sweet spot for reorder reminder)
        optimal_reorder = customer_stats[
            (customer_stats['days_since_last_order'] >= 15) &
            (customer_stats['days_since_last_order'] <= 25)
        ].copy()
        optimal_reorder = optimal_reorder.sort_values('days_since_last_order', ascending=True)
        segments['optimal_reorder_timing'] = {
            'data': optimal_reorder,
            'description': 'ZĂˇkaznĂ­ci v optimĂˇlnom ÄŤase na opĂ¤tovnĂ˝ nĂˇkup (15-25 dnĂ­ od poslednej objednĂˇvky)',
            'description_en': 'Customers at optimal reorder timing (15-25 days since last order)',
            'count': len(optimal_reorder),
            'email_purpose': 'Reorder - pripomenutie na doplnenie zĂˇsob',
            'send_timing': 'IhneÄŹ (sĂş v optimĂˇlnom okne)',
            'send_timing_en': 'Immediately (within optimal window)',
            'priority': 2,
            'discount_suggestion': '5-10% alebo doprava zadarmo',
            'email_template': 'DochĂˇdza VĂˇm parfum do prania? Objednajte teraz!'
        }
        print(f"  Segment 10 (Optimal reorder 15-25 days): {len(optimal_reorder)} customers")

        # Segment 11: New customers welcome sequence (0-7 days)
        new_customers = customer_stats[
            (customer_stats['order_count'] == 1) &
            (customer_stats['days_since_first_order'] <= 7)
        ].copy()
        new_customers = new_customers.sort_values('first_order_date', ascending=False)
        segments['new_customers_welcome'] = {
            'data': new_customers,
            'description': 'NovĂ­ zĂˇkaznĂ­ci - prvĂˇ objednĂˇvka v poslednĂ˝ch 7 dĹoch',
            'description_en': 'New customers - first order within last 7 days',
            'count': len(new_customers),
            'email_purpose': 'Welcome - privĂ­tanie, tipy na pouĹľitie produktu',
            'send_timing': '3 dni po doruÄŤenĂ­',
            'send_timing_en': '3 days after delivery',
            'priority': 3,
            'discount_suggestion': 'Ĺ˝iadna zÄľava, len hodnota',
            'email_template': 'Ako sa VĂˇm pĂˇÄŤi vĂˇĹˇ novĂ˝ parfum? Tipy na pouĹľitie...'
        }
        print(f"  Segment 11 (New customers 0-7 days): {len(new_customers)} customers")

        # Segment 12: Second order encouragement (8-14 days, first-timers)
        second_order_timing = customer_stats[
            (customer_stats['order_count'] == 1) &
            (customer_stats['days_since_first_order'] >= 8) &
            (customer_stats['days_since_first_order'] <= 14)
        ].copy()
        second_order_timing = second_order_timing.sort_values('days_since_first_order', ascending=True)
        segments['second_order_encouragement'] = {
            'data': second_order_timing,
            'description': 'ZĂˇkaznĂ­ci pripravenĂ­ na druhĂ˝ nĂˇkup (8-14 dnĂ­ po prvej objednĂˇvke)',
            'description_en': 'Customers ready for second order (8-14 days after first purchase)',
            'count': len(second_order_timing),
            'email_purpose': 'Second order - motivĂˇcia k druhĂ©mu nĂˇkupu',
            'send_timing': '10-12 dnĂ­ po prvej objednĂˇvke',
            'send_timing_en': '10-12 days after first order',
            'priority': 2,
            'discount_suggestion': '10% na druhĂş objednĂˇvku',
            'email_template': 'PĂˇÄŤil sa VĂˇm nĂˇĹˇ produkt? ZĂ­skajte 10% na ÄŹalĹˇĂ­ nĂˇkup!'
        }
        print(f"  Segment 12 (Second order timing 8-14 days): {len(second_order_timing)} customers")

        # Save segments to CSV files
        for segment_name, segment_data in segments.items():
            if not segment_data['data'].empty:
                filename = self.output_path(f"email_segment_{segment_name}.csv")
                segment_data['data'].to_csv(filename, index=False, encoding='utf-8-sig')
                print(f"    Saved: {filename}")

        print(f"\nCustomer segmentation complete: {len(segments)} segments created")

        return segments


def main():
    """Main function to handle command line arguments and run the export"""
    parser = argparse.ArgumentParser(description='Export orders from BizniWeb API')
    parser.add_argument(
        '--project',
        type=str,
        default=os.getenv('REPORT_PROJECT', DEFAULT_PROJECT),
        help='Project name (loads projects/<project>/settings.json and optional .env)'
    )
    parser.add_argument(
        '--from-date',
        type=str,
        help='From date in YYYY-MM-DD format (default: 2025-05-06)'
    )
    parser.add_argument(
        '--to-date',
        type=str,
        help='To date in YYYY-MM-DD format (default: today)'
    )
    parser.add_argument(
        '--clear-cache',
        action='store_true',
        help='Clear all cached data before running'
    )
    parser.add_argument(
        '--no-cache',
        action='store_true',
        help='Disable cache and fetch all data fresh from API'
    )
    parser.add_argument(
        '--output-tag',
        type=str,
        default=os.getenv('REPORT_OUTPUT_TAG', ''),
        help='Optional output tag for parallel test artifacts (e.g. ui_test)'
    )

    args = parser.parse_args()
    project_name = (args.project or DEFAULT_PROJECT).strip()

    # Load project-specific env first so API credentials are isolated per shop.
    load_project_env(project_name, logger=logger)
    runtime = load_project_runtime(
        project_name,
        settings=load_project_settings(project_name),
        legacy_product_expenses=LEGACY_VEVO_PRODUCT_EXPENSES,
        default_currency_rates=CURRENCY_RATES_TO_EUR,
        default_packaging_cost_per_order=PACKAGING_COST_PER_ORDER,
        default_shipping_subsidy_per_order=SHIPPING_SUBSIDY_PER_ORDER,
        default_fixed_monthly_cost=FIXED_MONTHLY_COST,
    )
    apply_project_runtime(runtime, globals())

    api_url = runtime.api_url
    api_token = runtime.api_token
    if not api_token:
        logger.error(
            f"BIZNISWEB_API_TOKEN not found for project '{project_name}'. "
            f"Set it in projects/{project_name}/.env or environment variables."
        )
        raise ValueError(
            f"BIZNISWEB_API_TOKEN not found for project '{project_name}'. "
            f"Please configure credentials."
        )
    
    # Parse dates
    if args.to_date:
        date_to = parse_input_date(args.to_date)
    else:
        date_to = datetime.now()

    if args.from_date:
        date_from = parse_input_date(args.from_date)
    else:
        default_from_raw = os.getenv("REPORT_FROM_DATE", "2025-05-03")
        date_from = parse_input_date(default_from_raw)

    print(
        f"Exporting project '{project_name}' orders from "
        f"{date_from.strftime('%Y-%m-%d')} to {date_to.strftime('%Y-%m-%d')}"
    )
    
    # Initialize exporter
    exporter = BizniWebExporter(
        api_url,
        api_token,
        project_name=project_name,
        output_tag=args.output_tag,
    )
    
    # Handle cache options
    if args.clear_cache:
        print("Clearing cache...")
        import shutil
        if exporter.cache_dir.exists():
            shutil.rmtree(exporter.cache_dir)
            exporter.cache_dir.mkdir(parents=True, exist_ok=True)
            print("Cache cleared.")
    
    if args.no_cache:
        print("Cache disabled for this run.")
        exporter.cache_days_threshold = float('inf')  # Never use cache
    
    # Fetch orders
    print("Fetching orders...")
    orders = exporter.fetch_orders(date_from, date_to)
    
    if not orders:
        print("No orders found in the specified date range.")
        return
    
    print(f"Found {len(orders)} orders")
    
    # Export to CSV
    print("Exporting to CSV...")
    filename = exporter.export_to_csv(orders, date_from, date_to)
    
    print(f"Export completed: {filename}")


if __name__ == "__main__":
    main()
