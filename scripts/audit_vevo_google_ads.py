#!/usr/bin/env python3
"""Read-only VEVO Google Ads audit; secrets stay in memory, output is aggregates."""

import argparse
import json
import logging
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

import boto3
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.protobuf.json_format import MessageToDict


METRICS = (
    "metrics.cost_micros, metrics.impressions, metrics.clicks, "
    "metrics.conversions, metrics.conversions_value, metrics.all_conversions, "
    "metrics.all_conversions_value"
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="codex")
    parser.add_argument("--region", default="eu-central-1")
    parser.add_argument("--secret-id", required=True)
    parser.add_argument("--date-from", type=date.fromisoformat, required=True)
    parser.add_argument("--date-to", type=date.fromisoformat, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.date_from > args.date_to:
        parser.error("date-from must be before date-to")

    logging.disable(logging.CRITICAL)
    secret = json.loads(
        boto3.Session(profile_name=args.profile, region_name=args.region)
        .client("secretsmanager")
        .get_secret_value(SecretId=args.secret_id)["SecretString"]
    )
    config = {
        key: secret["GOOGLE_ADS_" + key.upper()]
        for key in ("developer_token", "client_id", "client_secret", "refresh_token")
    }
    config["use_proto_plus"] = True
    if secret.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID"):
        config["login_customer_id"] = secret["GOOGLE_ADS_LOGIN_CUSTOMER_ID"].replace(
            "-", ""
        )
    customer_id = secret["GOOGLE_ADS_CUSTOMER_ID"].replace("-", "")
    service = GoogleAdsClient.load_from_dict(config).get_service("GoogleAdsService")

    def query(statement):
        return [
            MessageToDict(row._pb, preserving_proto_field_name=True)
            for row in service.search(customer_id=customer_id, query=statement)
        ]

    identity = query(
        "SELECT customer.id, customer.descriptive_name, customer.currency_code, "
        "customer.time_zone FROM customer LIMIT 1"
    )[0]["customer"]
    if identity["id"] != "7592903323" or identity["descriptive_name"] != "Vevo.sk":
        raise ValueError("Unexpected Google Ads account identity; audit stopped")
    if identity["currency_code"] != "EUR":
        raise ValueError("Unexpected account currency; audit stopped")
    where = (
        f"WHERE segments.date BETWEEN '{args.date_from}' AND '{args.date_to}'"
    )
    queries = {
        "customer_month": f"SELECT segments.month, {METRICS} FROM customer {where}",
        "customer_daily": f"SELECT segments.date, {METRICS} FROM customer {where}",
        "campaign_month": (
            "SELECT campaign.id, campaign.name, campaign.status, "
            "campaign.advertising_channel_type, segments.month, "
            f"{METRICS} FROM campaign {where}"
        ),
        "country_month": (
            "SELECT user_location_view.country_criterion_id, "
            "user_location_view.targeting_location, segments.month, "
            f"{METRICS} FROM user_location_view {where}"
        ),
        "country_campaign_month": (
            "SELECT campaign.id, campaign.name, "
            "user_location_view.country_criterion_id, "
            "user_location_view.targeting_location, segments.month, "
            f"{METRICS} FROM user_location_view {where}"
        ),
        "conversion_action_month": (
            "SELECT segments.month, segments.conversion_action_name, "
            "segments.conversion_action_category, metrics.conversions, "
            "metrics.conversions_value, metrics.all_conversions, "
            f"metrics.all_conversions_value FROM customer {where}"
        ),
        "campaign_settings": (
            "SELECT campaign.id, campaign.name, campaign.status, "
            "campaign.bidding_strategy_type, "
            "campaign.geo_target_type_setting.positive_geo_target_type, "
            "campaign.geo_target_type_setting.negative_geo_target_type, "
            "campaign_budget.amount_micros FROM campaign "
            "WHERE campaign.status != 'REMOVED'"
        ),
        "conversion_action_settings": (
            "SELECT conversion_action.id, conversion_action.name, "
            "conversion_action.status, conversion_action.type, "
            "conversion_action.category, conversion_action.counting_type, "
            "conversion_action.primary_for_goal, "
            "conversion_action.value_settings.default_currency_code, "
            "conversion_action.value_settings.default_value, "
            "conversion_action.value_settings.always_use_default_value "
            "FROM conversion_action WHERE conversion_action.status != 'REMOVED'"
        ),
        "ad_destinations": (
            "SELECT campaign.id, campaign.name, ad_group_ad.status, "
            "ad_group_ad.ad.final_urls FROM ad_group_ad "
            "WHERE campaign.status != 'REMOVED' AND ad_group_ad.status != 'REMOVED'"
        ),
        "geo_target_countries": (
            "SELECT geo_target_constant.id, geo_target_constant.name, "
            "geo_target_constant.country_code FROM geo_target_constant "
            "WHERE geo_target_constant.target_type = 'Country'"
        ),
    }
    result = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "date_from": args.date_from.isoformat(),
        "date_to": args.date_to.isoformat(),
        "account": identity,
        "method": {
            "country_basis": "user_location_view physical user country, all targeting flags",
            "country_docs": "https://developers.google.com/google-ads/api/fields/v22/user_location_view",
            "value_basis": "Ads-attributed conversion value, not accounting revenue",
            "settings_time_basis": "current settings at query time, not historical settings",
            "cost_basis": "account EUR micros; divide by 1000000",
        },
        "queries": queries,
        "data": {},
        "errors": {},
    }
    for name, statement in queries.items():
        try:
            result["data"][name] = query(statement)
            print(f"{name}: {len(result['data'][name])} rows", flush=True)
        except GoogleAdsException as exc:
            # Error codes only: never serialize exceptions or credential-bearing requests.
            codes = [str(error.error_code) for error in exc.failure.errors]
            result["errors"][name] = codes
            print(f"{name}: API error codes {codes}", flush=True)

    if not result["errors"]:
        countries = {
            row["geo_target_constant"]["id"]: row["geo_target_constant"]["country_code"]
            for row in result["data"]["geo_target_countries"]
        }
        monthly = defaultdict(lambda: defaultdict(float))
        for row in result["data"]["country_month"]:
            month = row["segments"]["month"]
            country = countries.get(row["user_location_view"]["country_criterion_id"], "UNKNOWN")
            for metric, value in row["metrics"].items():
                monthly[(month, country)][metric] += float(value)
        result["country_month_totals"] = [
            {"month": month, "country": country, **metrics}
            for (month, country), metrics in sorted(monthly.items())
        ]
        result["reconciliation"] = []
        for row in result["data"]["customer_month"]:
            month = row["segments"]["month"]
            check = {"month": month, "country_minus_customer": {}}
            for metric, total in row["metrics"].items():
                country_total = sum(values[metric] for (m, _), values in monthly.items() if m == month)
                delta = country_total - float(total)
                check["country_minus_customer"][metric] = delta
                if abs(delta) > 0.000001:
                    result["errors"]["reconciliation"] = "Country totals do not match customer totals"
            result["reconciliation"].append(check)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Saved sanitized aggregate audit to {args.output}")
    if result["errors"]:
        raise SystemExit(2)


if __name__ == "__main__":
    try:
        main()
    except (SystemExit, KeyboardInterrupt):
        raise
    except Exception as exc:
        # Top-level failures cannot leak tokens through library exception text.
        print(f"Audit stopped: {type(exc).__name__}")
        raise SystemExit(1) from None
