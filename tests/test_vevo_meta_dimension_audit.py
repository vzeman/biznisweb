from __future__ import annotations

import json
import unittest

from scripts.audit_vevo_meta_dimensions import (
    FAIL_MARKER,
    MetaDimensionAuditError,
    build_audit_summary,
    main,
)


class VevoMetaDimensionAuditTests(unittest.TestCase):
    def test_aggregate_coverage_contains_no_ad_identity_or_url(self) -> None:
        insights = [
            {"ad_id": "101", "campaign_id": "201", "adset_id": "301", "clicks": "10", "spend": "20.50"},
            {"ad_id": "102", "campaign_id": "202", "adset_id": "302", "clicks": "5", "spend": "9.50"},
        ]
        ads = {
            "101": {
                "creative": {
                    "url_tags": (
                        "utm_source=meta&utm_medium=paid_social&utm_id={{campaign.id}}&"
                        "utm_content={{ad.id}}&meta_adset_id={{adset.id}}&meta_placement={{placement}}"
                    )
                }
            },
            "102": {
                "creative": {
                    "object_story_spec": {
                        "link_data": {"link": "https://www.vevo.sk/?utm_source=meta&fbclid=forbidden"}
                    }
                }
            },
        }
        summary = build_audit_summary(
            insights,
            ads,
            since="2026-07-22",
            until="2026-08-20",
            api_version="v21.0",
        )
        self.assertEqual(summary["traffic_ads"], 2)
        self.assertEqual(summary["collector_compatible_all_dimensions"]["ads"], 1)
        self.assertEqual(summary["collector_compatible_all_dimensions"]["click_coverage_pct"], 66.67)
        self.assertEqual(summary["recommended_macro_contract_all_dimensions"]["spend_eur"], 20.5)
        self.assertEqual(summary["forbidden_click_identifier_parameter_ads"], 1)
        serialized = json.dumps(summary).lower()
        for forbidden in ("101", "201", "301", "https://", "fbclid=forbidden"):
            self.assertNotIn(forbidden, serialized)

    def test_static_stable_ids_are_collector_compatible_but_not_recommended_macros(self) -> None:
        summary = build_audit_summary(
            [{"ad_id": "101", "campaign_id": "201", "adset_id": "301", "clicks": "1", "spend": "1"}],
            {
                "101": {
                    "creative": {
                        "url_tags": (
                            "utm_source=facebook&utm_medium=cpc&utm_id=201&utm_content=101&"
                            "meta_adset_id=301&meta_placement=instagram_feed"
                        )
                    }
                }
            },
            since="2026-07-22",
            until="2026-08-20",
            api_version="v21.0",
        )
        self.assertEqual(summary["collector_compatible_all_dimensions"]["ads"], 1)
        self.assertEqual(summary["recommended_macro_contract_all_dimensions"]["ads"], 0)

    def test_empty_delivery_window_fails_closed(self) -> None:
        with self.assertRaisesRegex(MetaDimensionAuditError, "no ad-level delivery"):
            build_audit_summary([], {}, since="2026-07-22", until="2026-08-20", api_version="v21.0")

    def test_full_storefront_placement_allowlist_is_collector_compatible(self) -> None:
        summary = build_audit_summary(
            [{"ad_id": "101", "campaign_id": "201", "adset_id": "301", "clicks": "1", "spend": "1"}],
            {
                "101": {
                    "id": "101",
                    "campaign_id": "201",
                    "adset_id": "301",
                    "creative": {
                        "url_tags": (
                            "utm_source=facebook&utm_medium=cpc&utm_id=201&utm_content=101&"
                            "meta_adset_id=301&meta_placement=facebook_marketplace"
                        )
                    },
                }
            },
            since="2026-07-22",
            until="2026-08-20",
            api_version="v21.0",
        )
        self.assertEqual(summary["collector_compatible_all_dimensions"]["ads"], 1)

    def test_main_emits_only_sanitized_failure_marker(self) -> None:
        from contextlib import redirect_stderr
        from io import StringIO
        from unittest.mock import patch

        stderr = StringIO()
        with patch("scripts.audit_vevo_meta_dimensions.FacebookAdsClient"), patch(
            "scripts.audit_vevo_meta_dimensions.run_audit",
            side_effect=MetaDimensionAuditError("Meta Graph read failed during delivery insights"),
        ), redirect_stderr(stderr):
            self.assertEqual(main(), 1)
        self.assertEqual(
            stderr.getvalue().strip(),
            FAIL_MARKER + "Meta Graph read failed during delivery insights",
        )


if __name__ == "__main__":
    unittest.main()
