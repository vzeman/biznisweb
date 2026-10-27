"""Independent verifier fixtures never import the production country calculator."""
import copy
from datetime import date, timedelta
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from scripts.verify_country_cfo_payload import (
    ACCOUNT, COUNTRIES, METRICS, WINDOWS, days, load_payload, sha, verify_country_cfo_payload,
)


LAST = "2026-10-26"
FIRST = (date.fromisoformat(LAST) - timedelta(days=119)).isoformat()


def fixture():
    defs = [{"key": key, "direction": "down" if key == "cac" else "up"} for key in sorted(METRICS)]
    country_metrics = {
        "revenue": 100, "profit": 45, "orders": 1, "aov": 100, "cac": 15, "roas": 100 / 15,
        "pre_ad_contribution_margin": 60, "post_ad_margin": 45,
        "company_margin_with_fixed": 45 - 70 / 3,
    }
    countries, global_windows = {}, {}
    for country in COUNTRIES:
        windows = {}
        for window in WINDOWS:
            windows[window] = {
                "metrics": dict(country_metrics),
                "secondary_metrics": {"company_margin_with_fixed": 45 - 70 / 3,
                                      "revenue_credit_adjustment": -20 if country == "sk" else 0},
                "trend": {"dates": [(date.fromisoformat(LAST) - timedelta(days=1)).isoformat(), LAST],
                          "metrics": {key: [None if key == "cac" else 0, value] for key, value in country_metrics.items()}},
            }
        countries[country] = {
            "country": country, "status": "ok", "metric_defs": copy.deepcopy(defs), "windows": windows,
            "comparisons": {window: {key: {"vs_previous": None} for key in METRICS} for window in WINDOWS},
            "metadata": {"date_from": FIRST, "date_to": LAST, "calendar_complete": True, "calendar_days": 120},
        }
    for window, count in WINDOWS.items():
        fixed = 70 * (count or 120)
        metrics = {**country_metrics, "revenue": 300, "profit": 135, "orders": 3,
                   "company_margin_with_fixed": (135 - fixed) / 300 * 100}
        global_windows[window] = {
            "metrics": metrics, "secondary_metrics": {"company_margin_with_fixed": 135 - fixed},
            "trend": copy.deepcopy(countries["sk"]["windows"][window]["trend"]),
        }
    payload = {
        "project": "vevo", "date_from": FIRST, "date_to": LAST,
        "dashboard": {"kpis": {
            "metric_defs": defs, "windows": global_windows,
            "comparisons": copy.deepcopy(countries["sk"]["comparisons"]),
            "country_order": list(COUNTRIES), "countries": countries,
            "country_metadata": {"company_minus_displayed_countries": {"fixed_daily_cost": 70 * 119, "total_revenue": 0, "unique_orders": 0}},
        }},
    }
    google = {
        "status": "ok", "basis": "google_user_location_daily", "currency": "EUR", "customer_id": "123456",
        "date_from": FIRST, "date_to": LAST,
        "daily_spend_by_country": {day: ({country: 5 for country in COUNTRIES} if day == LAST else {}) for day in days(FIRST, LAST)},
        "daily_account_spend": {day: 15 if day == LAST else 0 for day in days(FIRST, LAST)},
        "total_spend": 15, "account_total_spend": 15,
    }
    rows = []
    for country in COUNTRIES:
        row = dict(order_num=country, purchase_date=LAST, delivery_country=country, invoice_country="sk",
                   order_revenue_net="100", order_credit_net_adjustment="-20" if country == "sk" else "0",
                   customer_email=country + "@example.test", customer_first_purchase_date=LAST)
        rows.append(row)
        if country == "sk":
            rows.append(dict(row))  # Repeated per-item order revenue/credit, counted once.
    daily = [dict(date=day, unique_orders=3 if day == LAST else 0, fixed_daily_cost=70) for day in days(FIRST, LAST)]
    return payload, google, rows, daily


def meta_fixture(directory):
    release = "a" * 32
    prefix = f"data/vevo/reporting/runtime/probes/{release}/artifacts/source-inputs/meta/"
    manifest = {"project": "vevo", "complete": True, "currency": "EUR", "meta_account_id": ACCOUNT,
                "capture_mode": "probe", "release_id": release, "report_from_date": FIRST, "report_to_date": LAST,
                "native_responses": [], "query_groups": {}}
    def store(kind, path, params, body):
        index = len(manifest["native_responses"]) + 1
        raw = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
        record = {"key": prefix + f"{index:04d}-response.json", "size": len(raw), "sha256": sha(raw),
                  "original_response_sha256": sha(raw), "origin": "https://graph.facebook.com", "status": 200,
                  "method": "GET", "path": path, "public_params": params, "kind": kind,
                  "headers_or_credentials_persisted": False}
        (directory / f"{index:04d}-response.json").write_bytes(raw)
        (directory / f"{index:04d}-receipt.json").write_bytes(json.dumps(record, sort_keys=True, separators=(",", ":")).encode())
        manifest["native_responses"].append(record)
        return index
    scopes = [(FIRST, LAST)] + [(max(FIRST, (date.fromisoformat(LAST) - timedelta(days=n - 1)).isoformat()), LAST) for n in (7, 30, 90)]
    for first, last in scopes:
        identity = store("identity", f"/v21.0/{ACCOUNT}", {"fields": "currency"}, {"id": ACCOUNT, "currency": "EUR"})
        query = {"fields": "spend", "level": "account", "breakdowns": "country", "time_increment": "1", "limit": "500",
                 "time_range": {"since": first, "until": last}}
        record_params = {**query, "time_range": json.dumps(query["time_range"])}
        body = {"data": [dict(country=country.upper(), spend="10", date_start=LAST, date_stop=LAST) for country in COUNTRIES]}
        page = store("country_daily", f"/v21.0/{ACCOUNT}/insights", record_params, body)
        manifest["query_groups"][first] = {"kind": "country_daily", "complete": True, "query": query,
                                           "pages": [page], "currency_metadata_page": identity}
    return manifest


class CountryCfoVerificationTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)
        self.payload, self.google, self.orders, self.daily = fixture()
        self.meta = meta_fixture(self.folder)

    def verify(self, **overrides):
        options = dict(meta_manifest=self.meta, meta_directory=self.folder, google_daily_source=self.google,
                       financial_rows=self.orders, global_daily=self.daily)
        options.update(overrides)
        return verify_country_cfo_payload(self.payload, **options)

    def test_independent_sources_and_literal_identities_pass_without_granting_approval(self):
        proof = self.verify()
        self.assertTrue(proof["passed"], proof)
        self.assertTrue(proof["checks"]["all_ad_sources_verified"])
        self.assertTrue(proof["checks"]["customer_cac_from_financial_csv"])
        self.assertTrue(proof["checks"]["allocated_fixed_from_daily_source"])
        self.assertEqual("already_adjusted_canonical_order_revenue", proof["financial_csv_revenue_mode"])
        self.assertFalse(proof["financial_approval"])
        self.assertFalse(proof["publication_approval"])
        self.assertFalse(proof["creditnote_fulfillment_source_independently_verified"])

    def test_no_sources_never_implies_source_or_financial_approval(self):
        proof = self.verify(meta_manifest=None, meta_directory=None, google_daily_source=None, financial_rows=None, global_daily=None)
        self.assertTrue(proof["passed"], proof)
        self.assertFalse(proof["checks"]["all_ad_sources_verified"])
        self.assertFalse(proof["checks"]["customer_cac_from_financial_csv"])
        self.assertFalse(proof["financial_approval"])

    def test_corrupted_aov_margin_order_or_nonfinite_value_is_rejected(self):
        for key, value in (("aov", 999), ("post_ad_margin", -999), ("orders", 1.5), ("revenue", float("nan"))):
            with self.subTest(metric=key):
                payload = copy.deepcopy(self.payload)
                payload["dashboard"]["kpis"]["countries"]["cz"]["windows"]["daily"]["metrics"][key] = value
                self.assertFalse(verify_country_cfo_payload(payload)["passed"])

    def test_duplicated_full_fixed_cost_per_country_is_rejected(self):
        for country in COUNTRIES:
            card = self.payload["dashboard"]["kpis"]["countries"][country]["windows"]["all_time"]
            card["secondary_metrics"]["company_margin_with_fixed"] = 45 - 8400
            card["metrics"]["company_margin_with_fixed"] = 45 - 8400
        result = self.verify()
        self.assertFalse(result["passed"])
        self.assertIn("shared_fixed_conservation", result["errors"])

    def test_missing_country_misaligned_dates_or_short_trend_is_rejected(self):
        for mutation in ("missing", "date", "trend"):
            with self.subTest(mutation=mutation):
                payload = copy.deepcopy(self.payload)
                countries = payload["dashboard"]["kpis"]["countries"]
                if mutation == "missing":
                    del countries["hu"]
                elif mutation == "date":
                    countries["hu"]["metadata"]["date_to"] = FIRST
                else:
                    countries["hu"]["windows"]["daily"]["trend"]["metrics"]["profit"].pop()
                self.assertFalse(verify_country_cfo_payload(payload)["passed"])

    def test_google_missing_zero_day_and_wrong_currency_are_rejected(self):
        for mutation in ("day", "currency", "account"):
            with self.subTest(mutation=mutation):
                source = copy.deepcopy(self.google)
                if mutation == "day":
                    del source["daily_spend_by_country"][FIRST]
                elif mutation == "currency":
                    source["currency"] = "HUF"
                else:
                    source["daily_account_spend"][LAST] = 999
                result = self.verify(google_daily_source=source)
                self.assertFalse(result["passed"])
                self.assertFalse(result["checks"]["all_ad_sources_verified"])

    def test_native_missing_range_hash_or_currency_cannot_be_approved(self):
        for mutation in ("range", "hash", "currency"):
            with self.subTest(mutation=mutation):
                manifest = copy.deepcopy(self.meta)
                if mutation == "range":
                    manifest["query_groups"].pop(next(iter(manifest["query_groups"])))
                elif mutation == "hash":
                    manifest["native_responses"][0]["sha256"] = "f" * 64
                else:
                    manifest["currency"] = "USD"
                result = self.verify(meta_manifest=manifest)
                self.assertFalse(result["passed"])
                self.assertFalse(result["checks"]["native_meta_four_ranges"])

    def test_wrong_source_ads_mer_profit_or_cac_is_rejected(self):
        for key, value in (("roas", 3), ("profit", 46), ("cac", 20)):
            with self.subTest(metric=key):
                payload = copy.deepcopy(self.payload)
                payload["dashboard"]["kpis"]["countries"]["sk"]["windows"]["daily"]["metrics"][key] = value
                proof = verify_country_cfo_payload(payload, meta_manifest=self.meta, meta_directory=self.folder,
                                                  google_daily_source=self.google, financial_rows=self.orders)
                self.assertFalse(proof["passed"])
                self.assertFalse(proof["checks"]["all_ad_sources_verified"])

    def test_repeated_item_credit_is_counted_once_and_wrong_disclosure_fails(self):
        self.assertTrue(self.verify()["passed"])
        self.payload["dashboard"]["kpis"]["countries"]["sk"]["windows"]["all_time"]["secondary_metrics"]["revenue_credit_adjustment"] = -40
        self.assertFalse(self.verify()["passed"])

    def test_customer_history_cac_none_is_valid_and_fabricated_acquisition_fails(self):
        self.orders[-1]["customer_first_purchase_date"] = FIRST
        hu = self.payload["dashboard"]["kpis"]["countries"]["hu"]
        for card in hu["windows"].values():
            card["metrics"]["cac"] = None
            card["trend"]["metrics"]["cac"] = [None, None]
        self.assertTrue(self.verify()["passed"])
        hu["windows"]["daily"]["metrics"]["cac"] = 15
        self.assertFalse(self.verify()["passed"])

    def test_native_source_does_not_accept_raw_body_tampering(self):
        target = self.folder / "0002-response.json"
        target.write_bytes(target.read_bytes().replace(b'"10"', b'"11"'))
        proof = self.verify()
        self.assertFalse(proof["passed"])
        self.assertIn("native_meta_source_invalid", proof["errors"])

    def test_embedded_html_and_duplicate_json_keys_are_handled_independently(self):
        target = self.folder / "report.html"
        target.write_text('<html><script id="report-dashboard-json" type="application/json">' + json.dumps(self.payload) + '</script></html>', encoding="utf-8")
        self.assertEqual(self.payload, load_payload(target))
        target.write_text('{"dashboard":{},"dashboard":{}}', encoding="utf-8")
        with self.assertRaises(ValueError):
            load_payload(target)


if __name__ == "__main__":
    unittest.main()
