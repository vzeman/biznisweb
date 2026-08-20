#!/usr/bin/env python3
from __future__ import annotations

import json
import pathlib
import py_compile
import sys


ROOT = pathlib.Path(__file__).resolve().parents[1]


def read(rel_path: str) -> str:
    return (ROOT / rel_path).read_text(encoding="utf-8")


def require(text: str, needle: str, message: str) -> None:
    if needle not in text:
        raise AssertionError(message)


def forbid(text: str, needle: str, message: str) -> None:
    if needle in text:
        raise AssertionError(message)


def main() -> int:
    try:
        facebook_ads = read("facebook_ads.py")
        export_orders = read("export_orders.py")
        daily_runner = read("daily_report_runner.py")
        html_report_generator = read("html_report_generator.py")
        dashboard_modern = read("dashboard_modern.py")
        http_client = read("http_client.py")
        weather_client = read("weather_client.py")
        read("templates/reporting-client/settings.template.json")
        read("templates/reporting-client/.env.example")
        read("templates/reporting-client/product_expenses.json")
        read("templates/reporting-client/README_CLIENT_SETUP.md")
        read("projects/vevo/product_name_aliases.json")
        growthbook_collector = read("growthbook_collector/handler.py")
        growthbook_collector_server = read("growthbook_collector/server.py")
        growthbook_collector_dockerfile = read("growthbook_collector/Dockerfile")
        growthbook_collector_host_gate = read("growthbook_collector/host_gate.sh")
        growthbook_registry = read("growthbook_collector/experiments.json")
        growthbook_registry_config = json.loads(growthbook_registry)
        growthbook_reporting = read("reporting_core/experiments.py")
        growthbook_event_io = read("reporting_core/experiment_io.py")
        growthbook_order_adapter = read("reporting_core/experiment_orders.py")
        growthbook_reconciler = read("scripts/reconcile_growthbook_facts.py")
        growthbook_template = read("infra/vevo-growthbook/template.yaml")
        growthbook_deploy_workflow = read(
            ".github/workflows/deploy-vevo-growthbook-preview.yml"
        )
        growthbook_verify_workflow = read(
            ".github/workflows/verify-vevo-growthbook-preview.yml"
        )
        growthbook_reporting_config = json.loads(read("projects/vevo/growthbook_reporting.json"))
        growthbook_storefront = read("storefront/vevo-growthbook/vevo-growthbook.js")
        growthbook_gtm_builder = read("scripts/build_vevo_growthbook_gtm_tag.py")
        growthbook_preview_config = json.loads(
            read("storefront/vevo-growthbook/config.preview.example.json")
        )
        gitleaks_ignore_entries = {
            line.strip()
            for line in read(".gitleaksignore").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }
        read("scripts/reporting_qa_smoke.py")
        read("scripts/import_product_expenses_excel.py")

        if gitleaks_ignore_entries != {
            "projects/vevo/growthbook_workspace.json:generic-api-key:328"
        }:
            raise AssertionError(
                "Gitleaks ignore must remain limited to the reviewed metric false positive"
            )

        require(
            facebook_ads,
            "headers={'Authorization': f'Bearer {self.access_token}'}",
            "facebook_ads.py must send Meta token via Authorization header.",
        )
        forbid(
            facebook_ads,
            "params = {\"access_token\"",
            "facebook_ads.py must not send Meta token via query string params.",
        )
        require(
            facebook_ads,
            "def _sanitize_url",
            "facebook_ads.py must sanitize logged URLs.",
        )
        require(
            http_client,
            "class TimeoutRetrySession",
            "http_client.py must provide the shared timeout-aware session wrapper.",
        )
        require(
            weather_client,
            "build_retry_session(timeout=self.request_timeout)",
            "weather_client.py must use the shared retry session.",
        )
        require(
            export_orders,
            "source_health",
            "export_orders.py must track source health for partial-data mode.",
        )
        require(
            export_orders,
            "_finalize_source_health",
            "export_orders.py must finalize source-health metadata for each run.",
        )
        require(
            export_orders,
            "_build_attribution_qa",
            "export_orders.py must build attribution QA guardrails before report export.",
        )
        require(
            export_orders,
            "campaign_attribution_summary",
            "export_orders.py must keep campaign attribution summary metadata.",
        )
        require(
            export_orders,
            "_build_geo_qa",
            "export_orders.py must build geo confidence QA metadata before report export.",
        )
        require(
            export_orders,
            "_build_data_assertions_qa",
            "export_orders.py must build data assertion QA metadata before report export.",
        )
        require(
            export_orders,
            "\"qa_failure_count\"",
            "export_orders.py must aggregate QA failure counts into source health metadata.",
        )
        require(
            export_orders,
            "\"qa_warning_count\"",
            "export_orders.py must aggregate QA warning counts into source health metadata.",
        )
        require(
            export_orders,
            "\"null_label_rate_pct\"",
            "export_orders.py must expose null label rate in data assertions QA.",
        )
        require(
            export_orders,
            "_build_margin_stability_qa",
            "export_orders.py must build smoothed margin stability QA before report export.",
        )
        require(
            export_orders,
            "_build_product_expense_coverage_qa",
            "export_orders.py must build product cost coverage QA before report export.",
        )
        require(
            export_orders,
            "\"is_partial\"",
            "export_orders.py must persist partial-data state in source health metadata.",
        )
        require(
            daily_runner,
            "report_html",
            "daily_report_runner.py must still attach the generated main HTML report artifact.",
        )
        require(
            daily_runner,
            "build_data_quality_summary",
            "daily_report_runner.py must summarize data quality in the email body.",
        )
        require(
            daily_runner,
            "ReportQaFailures",
            "daily_report_runner.py must publish QA failure CloudWatch metrics.",
        )
        require(
            daily_runner,
            "ReportQaWarnings",
            "daily_report_runner.py must publish QA warning CloudWatch metrics.",
        )
        require(
            html_report_generator + dashboard_modern,
            "Partial Data",
            "HTML rendering layer must expose explicit partial-data status for generated reports.",
        )
        require(
            dashboard_modern,
            "Attribution QA guardrails",
            "Modern dashboard must surface attribution QA warnings explicitly.",
        )
        require(
            dashboard_modern,
            "Geo confidence guardrails",
            "Modern dashboard must surface geo confidence guardrails explicitly.",
        )
        require(
            dashboard_modern,
            "Data assertions",
            "Modern dashboard must surface data assertion warnings explicitly.",
        )
        require(
            dashboard_modern,
            "Critical failures",
            "Modern dashboard must expose QA failure counts in data assertions cards.",
        )
        require(
            dashboard_modern,
            "Smoothed fixed-margin alerts",
            "Modern dashboard must surface smoothed fixed-margin alerts explicitly.",
        )
        require(
            dashboard_modern,
            "Product cost coverage",
            "Modern dashboard must surface product cost coverage explicitly.",
        )
        require(
            dashboard_modern,
            "Missing-cost revenue share",
            "Modern dashboard must expose fallback revenue share explicitly.",
        )
        require(
            dashboard_modern,
            "CM1 / CM2 / CM3 taxonomy",
            "Modern dashboard must surface normalized CM taxonomy explicitly.",
        )
        require(
            dashboard_modern,
            "qa_rows.append",
            "Modern dashboard must keep rendering QA cards alongside source-health cards.",
        )
        require(
            dashboard_modern,
            "hero-alert",
            "Modern dashboard must surface attribution QA warnings in the hero shell before deeper sections.",
        )
        require(
            read(".github/workflows/observability-check.yml"),
            "observability_snapshot.py",
            "Reporting repo must keep an observability workflow baseline.",
        )
        require(
            growthbook_collector,
            '"IfNoneMatch": "*"',
            "GrowthBook collector must keep conditional idempotent S3 writes.",
        )
        require(
            growthbook_collector,
            "set(payload) != expected_fields",
            "GrowthBook collector must reject non-allowlisted event fields.",
        )
        forbid(
            growthbook_collector,
            "Access-Control-Allow-Origin\": \"*",
            "GrowthBook collector must never use wildcard CORS.",
        )
        for marker in (
            'HOST_MARKER = "VEVO_GROWTHBOOK_COLLECTOR_HOST_OK"',
            'self.path != "/v1/events"',
            "def log_message",
        ):
            require(
                growthbook_collector_server,
                marker,
                f"GrowthBook collector host adapter lost safety marker: {marker}",
            )
        for marker in (
            "USER 10001:10001",
            'CMD ["python", "-m", "growthbook_collector.server"]',
            'CMD ["curl", "-fsS", "http://127.0.0.1:8080/health"]',
        ):
            require(
                growthbook_collector_dockerfile,
                marker,
                f"GrowthBook collector image lost safety marker: {marker}",
            )
        require(
            growthbook_collector_host_gate,
            "curl -fsS http://127.0.0.1:8080/marker.json",
            "GrowthBook collector must prove its marker with curl on localhost.",
        )
        require(
            growthbook_registry,
            '"production": {}',
            "GrowthBook production registry must remain empty before rollout approval.",
        )
        require(
            growthbook_storefront,
            "var PRODUCTION_ACTIVATION = false;",
            "GrowthBook storefront must remain hard-disabled for Production.",
        )
        forbid(
            growthbook_storefront,
            "PRODUCTION_ACTIVATION = true",
            "GrowthBook storefront Production activation requires a reviewed rollout change.",
        )
        require(
            growthbook_storefront,
            "@growthbook/growthbook@1.7.0/dist/bundles/index.min.js",
            "GrowthBook storefront must keep the reviewed SDK bundle pin.",
        )
        require(
            growthbook_storefront,
            "sha384-LE9sSbxrM6BIe5z0T5qNuBymAEx7Iwp14FYi2TtCWSalftZaK5cG7ckbe3hNSRPK",
            "GrowthBook storefront must keep the verified SDK SRI marker.",
        )
        require(
            growthbook_storefront,
            "web-vitals@6.0.1/dist/web-vitals.iife.js",
            "GrowthBook storefront must use the pinned official Web Vitals build.",
        )
        require(
            growthbook_storefront,
            "sha384-xduvx5szsAXW0V0fxOYjfsvz/Zl93SEZcLM+BK+7y6Spco3N+8g8NjbtUIAWCCAQ",
            "GrowthBook storefront must keep the verified Web Vitals SRI marker.",
        )
        forbid(
            growthbook_storefront,
            "auto.min.js",
            "GrowthBook auto bundle would duplicate exposure tracking into GA4/GTM.",
        )
        for required_safety_marker in [
            'credentials: "omit"',
            'referrerPolicy: "no-referrer"',
            "disableVisualExperiments: true",
            "disableJsInjection: true",
            "disableUrlRedirectExperiments: true",
            'typeof result.value === "string"',
            "library.setPolyfills",
            "options.consent & options.ANALYTIC",
            '#product-detail .s1-detailCart .s1-submitCart',
            'collector.port ||',
        ]:
            require(
                growthbook_storefront,
                required_safety_marker,
                f"GrowthBook storefront lost safety marker: {required_safety_marker}",
            )
        for forbidden_storefront_marker in [
            "innerHTML",
            "document.write",
            "eval(",
            "fbclid",
            "_fbp",
            "_fbc",
        ]:
            forbid(
                growthbook_storefront,
                forbidden_storefront_marker,
                f"GrowthBook storefront contains forbidden marker: {forbidden_storefront_marker}",
            )
        require(
            growthbook_gtm_builder,
            'payload["environment"] != "preview"',
            "GrowthBook GTM builder must remain Preview-only.",
        )
        if growthbook_preview_config.get("environment") != "preview":
            raise AssertionError("GrowthBook example config must remain Preview-only.")
        require(
            growthbook_reporting,
            "authoritative order must use the exact PII-free schema",
            "GrowthBook reporting must reject order rows outside the PII-free boundary.",
        )
        require(
            growthbook_reporting,
            "one event_id has conflicting payloads",
            "GrowthBook reporting must fail closed on conflicting event IDs.",
        )
        require(
            growthbook_reporting,
            "ambiguous_transaction_device",
            "GrowthBook reporting must prevent cross-device double attribution.",
        )
        require(
            growthbook_event_io,
            'partition_prefix = f"{normalized_prefix}/event_date={current.isoformat()}/"',
            "GrowthBook raw loader must enumerate exact server-receipt date partitions.",
        )
        forbid(
            growthbook_event_io,
            'Prefix=normalized_prefix',
            "GrowthBook raw loader must never scan the broad raw prefix.",
        )
        require(
            growthbook_order_adapter,
            'if set(fact) != ORDER_FIELDS',
            "GrowthBook order adapter must enforce the exact PII-free output schema.",
        )
        require(
            growthbook_reconciler,
            'GROWTHBOOK_FACT_PUBLISH_ENABLED',
            "GrowthBook fact publication must keep the environment write gate.",
        )
        require(
            growthbook_reconciler,
            'if args.publish and not _enabled',
            "GrowthBook fact publication must require both explicit CLI and runtime gates.",
        )
        require(
            growthbook_template,
            "DenyRawWritesWithoutIfNoneMatch",
            "GrowthBook event bucket must enforce conditional raw writes.",
        )
        require(
            growthbook_template,
            "BlockPublicPolicy: true",
            "GrowthBook event bucket must block public policies.",
        )
        for marker in (
            "Type: AWS::ECS::Service",
            "Scheme: internal",
            "ConnectionType: VPC_LINK",
            "Condition: ActivatePublicRoute",
            "Default: 'false'",
            "ReadonlyRootFilesystem: true",
            "AssignPublicIp: !Ref TaskAssignPublicIp",
            "CollectorImageUri",
        ):
            require(
                growthbook_template,
                marker,
                f"GrowthBook Fargate foundation lost safety marker: {marker}",
            )
        forbid(
            growthbook_template,
            "AWS::Lambda",
            "GrowthBook collector must remain host-verifiable on ECS/Fargate.",
        )
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "SOURCE_SCHEDULE: vevo-daily-report-email",
            '"PublicRouteEnabled": "false"',
            "/app/growthbook_collector/host_gate.sh",
            "COLLECTOR_LOCALHOST_HEALTH_OK:preview",
            "COLLECTOR_LOCALHOST_MARKER_OK:/app",
            "--phase candidate",
            "--phase activate",
            '--change-set-type "${CHANGE_TYPE}"',
            '"${STACK_STATUS}" == "REVIEW_IN_PROGRESS"',
            "stale-review-change-sets.txt",
            "cmp -s raw-objects-before.json raw-objects-after.json",
        ):
            require(
                growthbook_deploy_workflow,
                marker,
                f"GrowthBook protected deploy lost safety marker: {marker}",
            )
        forbid(
            growthbook_deploy_workflow,
            "source_schedule:",
            "GrowthBook deploy must not accept an arbitrary network source schedule.",
        )
        require(
            growthbook_deploy_workflow,
            'rejected != {"accepted": False, "code": "invalid_event"}',
            "GrowthBook deploy must expect the collector's generic public rejection code.",
        )
        forbid(
            growthbook_deploy_workflow,
            '"code": "field_set_mismatch"',
            "GrowthBook deploy must not depend on a masked internal validation reason.",
        )
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            'if stack["StackStatus"] != "UPDATE_COMPLETE"',
            'parameters.get("PublicRouteEnabled") != "true"',
            'route.get("RouteKey") != "POST /v1/events"',
            '"code": "invalid_event"',
            '"code": "origin_not_allowed"',
            "cmp -s raw-objects-before.json raw-objects-after.json",
            "COLLECTOR_ACTIVE_PUBLIC_NO_WRITE_OK",
        ):
            require(
                growthbook_verify_workflow,
                marker,
                f"GrowthBook active verifier lost safety marker: {marker}",
            )
        forbid(
            growthbook_template,
            "s3:DeleteObject",
            "GrowthBook runtime policies must not delete experiment objects.",
        )
        growthbook_policy_section = growthbook_template.split("  GrowthBookReadOnlyPolicy:", 1)[1].split(
            "  CollectorTarget5xxAlarm:", 1
        )[0]
        forbid(
            growthbook_policy_section,
            "experiment-events/raw",
            "GrowthBook identity must not read raw experiment events.",
        )
        forbid(
            growthbook_policy_section,
            "Resource: '*'",
            "GrowthBook identity must not receive wildcard resources.",
        )
        if growthbook_reporting_config.get("metric_contract_version") != "vevo_cm1_v1_2026-08-20":
            raise AssertionError("VEVO GrowthBook reporting must keep the frozen CM1 metric contract.")
        if growthbook_reporting_config.get("cart_window_hours") != 24:
            raise AssertionError("VEVO GrowthBook primary cart window must remain 24 hours.")
        if growthbook_reporting_config.get("order_window_days") != 7:
            raise AssertionError("VEVO GrowthBook purchase attribution window must remain 7 days.")
        if growthbook_reporting_config.get("maturity_checkpoint_days") != 14:
            raise AssertionError("VEVO GrowthBook maturity checkpoint must remain 14 days.")
        preview_registry = growthbook_registry_config.get("environments", {}).get("preview", {})
        for experiment_id, weights in growthbook_reporting_config.get("expected_variation_weights", {}).items():
            registry_variations = preview_registry.get(experiment_id, {}).get("variations", [])
            if set(registry_variations) != set(weights):
                raise AssertionError(
                    f"GrowthBook registry/reporting variation mismatch for {experiment_id}."
                )

        for rel_path in [
            "http_client.py",
            "facebook_ads.py",
            "google_ads.py",
            "weather_client.py",
            "export_orders.py",
            "daily_report_runner.py",
            "generate_invoices.py",
            "unpaid_order_cancellation.py",
            "unpaid_order_cancellation_runner.py",
            "roy_picking_lists_pdf.py",
            "scripts/observability_snapshot.py",
            "scripts/scaffold_client.py",
            "scripts/import_product_expenses_excel.py",
            "scripts/reporting_qa_smoke.py",
            "growthbook_collector/handler.py",
            "growthbook_collector/server.py",
            "reporting_core/experiments.py",
            "reporting_core/experiment_io.py",
            "reporting_core/experiment_orders.py",
            "scripts/reconcile_growthbook_facts.py",
            "scripts/build_vevo_growthbook_gtm_tag.py",
            "scripts/validate_growthbook_changeset.py",
            "scripts/validate_growthbook_workspace.py",
        ]:
            py_compile.compile(str(ROOT / rel_path), doraise=True)

        print("security_ci.py: OK")
        return 0
    except Exception as exc:  # pragma: no cover - CI failure path
        print(f"security_ci.py: FAIL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
