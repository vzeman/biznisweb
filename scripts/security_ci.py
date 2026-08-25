#!/usr/bin/env python3
from __future__ import annotations

import hashlib
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
        root_text = str(ROOT)
        if root_text not in sys.path:
            sys.path.insert(0, root_text)
        from scripts.validate_growthbook_aa_measurement_window import (
            MeasurementWindowError,
            validate_measurement_window,
        )
        from scripts.validate_growthbook_aa_completion import (
            validate as validate_aa_completion,
        )
        from scripts.build_growthbook_cta_baseline_observation import (
            validate_manifest as validate_cta_baseline_manifest,
        )
        from scripts.record_growthbook_cta_activation import (
            validate_manifest as validate_cta_activation_manifest,
        )

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
        growthbook_scheduled_reconciler = read(
            "scripts/run_scheduled_growthbook_reconciliation.py"
        )
        growthbook_reconciliation_parameters = read(
            "scripts/build_growthbook_reconciliation_parameters.py"
        )
        growthbook_template = read("infra/vevo-growthbook/template.yaml")
        growthbook_reconciliation_template = read(
            "infra/vevo-growthbook-reconciliation/template.yaml"
        )
        growthbook_deploy_workflow = read(
            ".github/workflows/deploy-vevo-growthbook-preview.yml"
        )
        growthbook_verify_workflow = read(
            ".github/workflows/verify-vevo-growthbook-preview.yml"
        )
        growthbook_reader_workflow = read(
            ".github/workflows/provision-vevo-growthbook-preview-reader.yml"
        )
        growthbook_reconciliation_workflow = read(
            ".github/workflows/deploy-vevo-growthbook-reconciliation.yml"
        )
        growthbook_reconciliation_recovery_workflow = read(
            ".github/workflows/recover-vevo-growthbook-reconciliation-rollback.yml"
        )
        growthbook_natural_reconciliation_workflow = read(
            ".github/workflows/verify-vevo-growthbook-natural-reconciliation.yml"
        )
        growthbook_natural_evidence_recorder = read(
            "scripts/record_growthbook_natural_evidence.py"
        )
        growthbook_meta_audit = read("scripts/audit_vevo_meta_dimensions.py")
        growthbook_meta_audit_workflow = read(
            ".github/workflows/audit-vevo-growthbook-meta-population.yml"
        )
        growthbook_production_preflight_workflow = read(
            ".github/workflows/preflight-vevo-growthbook-production-foundation.yml"
        )
        growthbook_production_foundation_workflow = read(
            ".github/workflows/deploy-vevo-growthbook-production-foundation.yml"
        )
        growthbook_host_gate_runtime_resolver = read(
            "scripts/resolve_growthbook_host_gate_runtime.py"
        )
        growthbook_foundation_diagnostic_workflow = read(
            ".github/workflows/diagnose-vevo-growthbook-production-foundation.yml"
        )
        growthbook_zero_collector_workflow = read(
            ".github/workflows/verify-vevo-growthbook-zero-collector.yml"
        )
        growthbook_zero_collector_recorder = read(
            "scripts/record_growthbook_zero_collector_observation.py"
        )
        growthbook_post_publish_zero_collector_recorder = read(
            "scripts/record_growthbook_post_publish_zero_collector_observation.py"
        )
        growthbook_foundation_bucket_summarizer = read(
            "scripts/summarize_growthbook_foundation_bucket.py"
        )
        growthbook_foundation_recovery_workflow = read(
            ".github/workflows/recover-vevo-growthbook-production-foundation-evidence.yml"
        )
        growthbook_foundation_recovery_verifier = read(
            "scripts/verify_growthbook_foundation_recovery.py"
        )
        growthbook_production_foundation_recorder = read(
            "scripts/record_growthbook_foundation_evidence.py"
        )
        growthbook_production_reader_workflow = read(
            ".github/workflows/provision-vevo-growthbook-production-reader.yml"
        )
        growthbook_production_reader_recorder = read(
            "scripts/record_growthbook_production_reader_evidence.py"
        )
        growthbook_production_clone_recorder = read(
            "scripts/record_growthbook_production_clone_evidence.py"
        )
        growthbook_production_clone_runbook = read(
            "projects/vevo/GROWTHBOOK_PRODUCTION_CLONE_RUNBOOK.md"
        )
        growthbook_production_clone_observation_bytes = (
            ROOT
            / "projects"
            / "vevo"
            / "vevo-growthbook-production-clone-observation.json"
        ).read_bytes()
        normalized_growthbook_production_clone_observation_bytes = (
            growthbook_production_clone_observation_bytes.replace(b"\r\n", b"\n")
        )
        growthbook_production_clone_observation = json.loads(
            growthbook_production_clone_observation_bytes
        )
        growthbook_production_device_outcomes_sql = read(
            "projects/vevo/growthbook_sql/device_outcomes_production.sql"
        )
        growthbook_production_performance_vitals_sql = read(
            "projects/vevo/growthbook_sql/performance_vitals_production.sql"
        )
        growthbook_workspace_config = json.loads(
            read("projects/vevo/growthbook_workspace.json")
        )
        growthbook_reporting_config = json.loads(
            read("projects/vevo/growthbook_reporting.json")
        )
        growthbook_aa_evaluator = read("scripts/evaluate_growthbook_aa.py")
        growthbook_receipt_summarizer = read("scripts/summarize_growthbook_receipts.py")
        growthbook_aa_snapshot_assembler = read(
            "scripts/assemble_growthbook_aa_snapshot.py"
        )
        growthbook_aa_snapshot_workflow = read(
            ".github/workflows/build-vevo-growthbook-production-aa-snapshot.yml"
        )
        growthbook_aa_manual_qa_builder = read(
            "scripts/build_growthbook_aa_manual_qa_evidence.py"
        )
        growthbook_aa_manual_qa_workflow = read(
            ".github/workflows/verify-vevo-growthbook-production-aa-manual-qa.yml"
        )
        growthbook_aa_automated_builder = read(
            "scripts/build_growthbook_aa_automated_evidence.py"
        )
        growthbook_aa_automated_workflow = read(
            ".github/workflows/collect-vevo-growthbook-production-aa-evidence.yml"
        )
        growthbook_aa_measurement_window_validator = read(
            "scripts/validate_growthbook_aa_measurement_window.py"
        )
        growthbook_aa_window_checkpoint_recorder = read(
            "scripts/record_growthbook_aa_window_checkpoint.py"
        )
        growthbook_aa_window_checkpoint_workflow = read(
            ".github/workflows/check-vevo-growthbook-production-aa-window.yml"
        )
        growthbook_aa_evidence_gate_recorder = read(
            "scripts/record_growthbook_aa_evidence_gates.py"
        )
        growthbook_aa_completion_recorder = read(
            "scripts/record_growthbook_aa_completion.py"
        )
        growthbook_aa_completion_validator = read(
            "scripts/validate_growthbook_aa_completion.py"
        )
        growthbook_cta_baseline_builder = read(
            "scripts/build_growthbook_cta_baseline_observation.py"
        )
        growthbook_cta_baseline_workflow = read(
            ".github/workflows/collect-vevo-growthbook-cta-baseline.yml"
        )
        growthbook_cta_baseline_manifest = json.loads(
            read("projects/vevo/growthbook_cta_baseline.json")
        )
        growthbook_cta_activation_recorder = read(
            "scripts/record_growthbook_cta_activation.py"
        )
        growthbook_cta_runtime_release_validator = read(
            "scripts/validate_growthbook_cta_runtime_release.py"
        )
        growthbook_cta_runtime_builder = read(
            "scripts/build_growthbook_cta_runtime_readiness.py"
        )
        growthbook_cta_runtime_workflow = read(
            ".github/workflows/deploy-vevo-growthbook-production-cta-runtime.yml"
        )
        growthbook_cta_activation_manifest = json.loads(
            read("projects/vevo/growthbook_cta_activation.json")
        )
        growthbook_aa_snapshot_manifest = json.loads(
            read("projects/vevo/growthbook_aa_snapshot.json")
        )
        growthbook_production_aa_activation = json.loads(
            read("projects/vevo/growthbook_production_aa_activation.json")
        )
        growthbook_production_reconciliation_evidence = json.loads(
            read(
                "projects/vevo/growthbook_production_reconciliation_deploy_evidence.json"
            )
        )
        growthbook_aa_acceptance = json.loads(
            read("projects/vevo/growthbook_aa_acceptance.json")
        )
        growthbook_storefront = read("storefront/vevo-growthbook/vevo-growthbook.js")
        growthbook_gtm_builder = read("scripts/build_vevo_growthbook_gtm_tag.py")
        growthbook_preview_config = json.loads(
            read("storefront/vevo-growthbook/config.preview.example.json")
        )
        growthbook_production_config = json.loads(
            read("storefront/vevo-growthbook/config.production.example.json")
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
            'params = {"access_token"',
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
            '"qa_failure_count"',
            "export_orders.py must aggregate QA failure counts into source health metadata.",
        )
        require(
            export_orders,
            '"qa_warning_count"',
            "export_orders.py must aggregate QA warning counts into source health metadata.",
        )
        require(
            export_orders,
            '"null_label_rate_pct"',
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
            '"is_partial"',
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
            'RECEIPT_MARKER = "VEVO_GROWTHBOOK_COLLECTOR_RECEIPT"',
            "GrowthBook collector must emit the sanitized A/A receipt marker.",
        )
        receipt_marker_section = growthbook_collector.split(
            "def _emit_receipt_marker", 1
        )[1].split("def _headers", 1)[0]
        for forbidden_receipt_field in (
            "event_id",
            "event_name",
            "device_id",
            "transaction_id",
            "page_path",
            "utm_",
            "meta_",
            "record",
            "payload",
        ):
            forbid(
                receipt_marker_section.lower(),
                forbidden_receipt_field.lower(),
                "GrowthBook collector receipt marker contains forbidden field: "
                f"{forbidden_receipt_field}",
            )
        require(
            growthbook_collector,
            "set(payload) != expected_fields",
            "GrowthBook collector must reject non-allowlisted event fields.",
        )
        forbid(
            growthbook_collector,
            'Access-Control-Allow-Origin": "*',
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
        preview_aa_registry = (
            growthbook_registry_config.get("environments", {})
            .get("preview", {})
            .get("vevo-sk-aa-001")
        )
        preview_cta_registry = (
            growthbook_registry_config.get("environments", {})
            .get("preview", {})
            .get("vevo-sk-product-cta-color-001")
        )
        production_registry = growthbook_registry_config.get("environments", {}).get(
            "production"
        )
        workspace_state = growthbook_workspace_config.get("state")
        aa_only_registry = {"vevo-sk-aa-001": preview_aa_registry}
        cta_only_registry = {
            "vevo-sk-product-cta-color-001": preview_cta_registry
        }
        if workspace_state == "production_aa_running_activation_verified_pro_quantiles_blocked":
            if production_registry != aa_only_registry:
                raise AssertionError(
                    "GrowthBook Production registry must contain only the exact reviewed A/A contract while A/A is running."
                )
        elif workspace_state == "production_aa_completed_cta_sample_freeze_pending_pro_quantiles_blocked":
            if production_registry not in (aa_only_registry, cta_only_registry):
                raise AssertionError(
                    "Post-A/A Production registry must contain exactly one reviewed A/A or CTA contract."
                )
        elif workspace_state == "production_cta_running_activation_verified_pro_quantiles_blocked":
            if production_registry != cta_only_registry:
                raise AssertionError(
                    "GrowthBook Production registry must contain only the exact reviewed CTA contract while CTA is running."
                )
        else:
            if production_registry != aa_only_registry:
                raise AssertionError(
                    "GrowthBook Production registry differs from the reviewed pre-CTA contract."
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
            "#product-detail .s1-detailCart .s1-submitCart",
            "collector.port ||",
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
        for marker in (
            "VEVO_GROWTHBOOK_PRODUCTION_CLIENT_KEY",
            "VEVO_GROWTHBOOK_PRODUCTION_COLLECTOR_URL",
            "expected_production_collector_host_sha256",
            "Production collector host does not match reviewed evidence",
            "client.replace(",
            "PRODUCTION_DISABLED_MARKER",
            "PRODUCTION_ENABLED_MARKER",
        ):
            require(
                growthbook_gtm_builder,
                marker,
                f"GrowthBook Production GTM builder lost safety marker: {marker}",
            )
        if (
            growthbook_preview_config.get("environment") != "preview"
            or growthbook_preview_config.get("enableDevMode") is not True
        ):
            raise AssertionError("GrowthBook Preview example config drifted.")
        if (
            growthbook_production_config.get("environment") != "production"
            or growthbook_production_config.get("enableDevMode") is not False
            or "REPLACE_WITH_PRODUCTION_CLIENT_KEY"
            not in str(growthbook_production_config.get("clientKey"))
            or "REPLACE_WITH_PRODUCTION_COLLECTOR"
            not in str(growthbook_production_config.get("collectorUrl"))
        ):
            raise AssertionError("GrowthBook Production example config drifted.")
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
            "Prefix=normalized_prefix",
            "GrowthBook raw loader must never scan the broad raw prefix.",
        )
        require(
            growthbook_order_adapter,
            "if set(fact) != ORDER_FIELDS",
            "GrowthBook order adapter must enforce the exact PII-free output schema.",
        )
        require(
            growthbook_reconciler,
            "GROWTHBOOK_FACT_PUBLISH_ENABLED",
            "GrowthBook fact publication must keep the environment write gate.",
        )
        require(
            growthbook_reconciler,
            "if args.publish and not _enabled",
            "GrowthBook fact publication must require both explicit CLI and runtime gates.",
        )
        for marker in (
            "scheduled reconciliation accepts no arguments",
            "GROWTHBOOK_FACT_PUBLISH_ENABLED",
            "rolling_partition_days",
            "max_raw_events",
            "GROWTHBOOK_SCHEDULED_RECONCILIATION_OK",
            "GROWTHBOOK_SCHEDULED_RECONCILIATION_FAILURE",
        ):
            require(
                growthbook_scheduled_reconciler,
                marker,
                f"Scheduled GrowthBook reconciler lost safety marker: {marker}",
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
            "Default: DISABLED",
            "Type: AWS::Scheduler::Schedule",
            "AllowedPattern: '^arn:aws[a-z-]*:ecs:[a-z0-9-]+:[0-9]{12}:task-definition/vevo-growthbook-reconcile-(preview|production):[1-9][0-9]*$'",
            "IsProduction: !Equals [!Ref Environment, production]",
            "QueueName: !Sub vevo-growthbook-reconcile-${Environment}-dlq",
            "RoleName: !Sub vevo-growthbook-reconcile-${Environment}-scheduler",
            "Name: !Sub vevo-growthbook-reconcile-${Environment}",
            "- 'cron(30 3 * * ? *)'",
            "- 'cron(45 3 * * ? *)'",
            "Action: ecs:RunTask",
            "Action: iam:PassRole",
            "EnableExecuteCommand: false",
            "MaximumEventAgeInSeconds: 3600",
            "MaximumRetryAttempts: 2",
            "GROWTHBOOK_SCHEDULED_RECONCILIATION_FAILURE",
            "GROWTHBOOK_SCHEDULED_RECONCILIATION_OK",
            "ScheduledReconciliationProductionFailure",
            "ScheduledReconciliationProductionSuccess",
            "schedule-group/default",
        ):
            require(
                growthbook_reconciliation_template,
                marker,
                f"GrowthBook reconciliation template lost safety marker: {marker}",
            )
        for forbidden_action in ("s3:DeleteObject", "scheduler:UpdateSchedule"):
            forbid(
                growthbook_reconciliation_template,
                forbidden_action,
                f"GrowthBook reconciliation template must not grant {forbidden_action}.",
            )
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "growthbook_reconciliation_production",
            "GROWTHBOOK_ENVIRONMENT: ${{ inputs.environment }}",
            "IMAGE_TAG: git-${{ github.sha }}",
            "REPORTING_POLICY_DOCUMENT_EXACT_OK",
            "aws iam attach-role-policy",
            "REPORTING_POLICY_ATTACHED_OK",
            "--phase candidate > candidate-parameters.json",
            "--parameters file://candidate-parameters.json",
            "GROWTHBOOK_RECONCILIATION_DISABLED_STACK_OK",
            "GROWTHBOOK_RECONCILE_LOCALHOST_MARKER_OK:/app",
            "GROWTHBOOK_RECONCILIATION_ONE_SHOT_OK",
            "--phase activate > activation-parameters.json",
            "--parameters file://activation-parameters.json",
            "GROWTHBOOK_RECONCILIATION_SCHEDULE_READBACK_OK",
            "VEVO reporting source schedule changed",
            "reconciliation stack requires read-only diagnosis before deploy",
            "SANITIZED_RECONCILIATION_STACK_DIAGNOSTIC:",
            "Upload sanitized deployment evidence only",
            "uses: actions/upload-artifact@v4.6.2",
            "path: ${{ env.EVIDENCE_FILE }}",
        ):
            require(
                growthbook_reconciliation_workflow,
                marker,
                f"GrowthBook reconciliation workflow lost safety marker: {marker}",
            )
        for forbidden_action in (
            "aws scheduler update-schedule",
            "s3api delete-object",
            "cloudformation delete-stack",
            "aws iam detach-role-policy",
        ):
            forbid(
                growthbook_reconciliation_workflow,
                forbidden_action,
                f"GrowthBook reconciliation workflow must not use {forbidden_action}.",
            )
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "Require exact manual read-only confirmation",
            '[[ "${CONFIRM_VERIFICATION}" == "true" ]]',
            "VERIFY_NOT_BEFORE_UTC: '2026-08-23T01:40:00Z'",
            "EXPECTED_CLUSTER_ARN: arn:aws:ecs:eu-central-1:919341186960:cluster/vevo-reporting-cluster",
            "EXPECTED_CONTAINER_NAME: reporting",
            "EXPECTED_LOG_GROUP: /ecs/vevo-reporting-daily",
            "EXPECTED_LOG_PREFIX: ecs",
            "workspace.get('reconciliation_checkpoint', {}).get(",
            "natural retention-recovery verification is not due until",
            "cloudformation describe-stacks",
            "scheduler get-schedule",
            "ecs describe-task-definition",
            "logs filter-log-events",
            "ecs describe-tasks",
            "cloudtrail lookup-events",
            "sqs get-queue-attributes",
            "scripts/verify_growthbook_natural_reconciliation.py",
            '--evidence-output "${EVIDENCE_FILE}"',
            '--workflow-run-id "${GITHUB_RUN_ID}"',
            '--main-commit "${GITHUB_SHA}"',
            "Upload sanitized natural reconciliation evidence only",
            "uses: actions/upload-artifact@v4.6.2",
            "path: vevo-growthbook-natural-reconciliation-evidence.json",
            "retention-days: 14",
            "AWS mutations: `none`",
        ):
            require(
                growthbook_natural_reconciliation_workflow,
                marker,
                f"GrowthBook natural reconciliation verifier lost safety marker: {marker}",
            )
        for removed_schedule_marker in ("schedule:", "cron:", "EVENT_NAME"):
            forbid(
                growthbook_natural_reconciliation_workflow,
                removed_schedule_marker,
                "GrowthBook natural verifier one-time cloud schedule was not removed: "
                f"{removed_schedule_marker}",
            )
        for forbidden_action in (
            "aws cloudformation create-",
            "aws cloudformation update-",
            "aws cloudformation execute-",
            "aws cloudformation delete-",
            "aws ecs run-task",
            "aws ecs register-task-definition",
            "aws scheduler create-",
            "aws scheduler update-",
            "aws scheduler delete-",
            "aws sqs send-message",
            "aws sqs delete-",
            "aws logs put-",
            "aws logs delete-",
            "aws cloudwatch put-",
            "aws cloudwatch delete-",
            "aws cloudwatch set-alarm-state",
            "aws s3api put-",
            "aws s3api delete-",
            "aws athena start-query-execution",
            "ads_update",
            "submit",
        ):
            forbid(
                growthbook_natural_reconciliation_workflow.lower(),
                forbidden_action,
                f"GrowthBook natural reconciliation verifier must remain read-only: {forbidden_action}",
            )
        for forbidden_artifact_path in (
            "path: natural-stack.json",
            "path: natural-schedule.json",
            "path: natural-task-state.json",
            "path: natural-task-logs.json",
            "path: natural-cloudtrail.json",
            "path: natural-dlq.json",
            "path: source-schedule.json",
        ):
            forbid(
                growthbook_natural_reconciliation_workflow,
                forbidden_artifact_path,
                "GrowthBook natural evidence must not upload raw AWS artifact: "
                f"{forbidden_artifact_path}",
            )
        if (
            growthbook_natural_reconciliation_workflow.count(
                "uses: actions/upload-artifact@v4.6.2"
            )
            != 1
        ):
            raise AssertionError(
                "GrowthBook natural verifier must upload exactly one sanitized artifact."
            )
        for marker in (
            "This is an offline, fail-closed manifest transformation.",
            "validate_natural_evidence",
            "evidence bytes are not canonical",
            "manifest change-set boundary drift",
            "production-allocation=0:reader=false:clone=false",
        ):
            require(
                growthbook_natural_evidence_recorder,
                marker,
                f"GrowthBook natural evidence recorder lost safety marker: {marker}",
            )
        for forbidden_client in (
            "import boto3",
            "import requests",
            "import httpx",
            "import socket",
            "import subprocess",
            "from facebook_ads",
        ):
            forbid(
                growthbook_natural_evidence_recorder,
                forbidden_client,
                "GrowthBook natural evidence recorder must remain offline: "
                f"{forbidden_client}",
            )
        for marker in (
            '"ScheduleState": "DISABLED"',
            '"ParameterValue": "ENABLED"',
            '"UsePreviousValue": True',
            '"SubnetIds": _required(environ, "SUBNET_IDS")',
        ):
            require(
                growthbook_reconciliation_parameters,
                marker,
                f"GrowthBook reconciliation parameter builder lost safety marker: {marker}",
            )
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "ROLLBACK_COMPLETE",
            "ROLLBACK_CLEANUP_HARD_GATE_OK:",
            "aws cloudformation delete-stack",
            "aws cloudformation wait stack-delete-complete",
            "aws sqs delete-queue",
            "cmp -s source-schedule-before.json source-schedule-after.json",
        ):
            require(
                growthbook_reconciliation_recovery_workflow,
                marker,
                f"GrowthBook reconciliation recovery lost safety marker: {marker}",
            )
        for forbidden_action in (
            "ecs run-task",
            "scheduler delete-schedule",
            "s3api delete-object",
            "iam delete-role",
        ):
            forbid(
                growthbook_reconciliation_recovery_workflow.lower(),
                forbidden_action,
                f"GrowthBook reconciliation recovery must not use {forbidden_action}.",
            )
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "META_AUDIT_PREDEPLOY_OK:",
            "META_AUDIT_HARD_GATE_OK:",
            "META_AUDIT_TASK_STOPPED_READBACK:",
            "META_AUDIT_TASK_OK:",
            "python -m scripts.audit_vevo_meta_dimensions",
            "VEVO_META_DIMENSION_AUDIT_OK:",
            "VEVO_META_DIMENSION_AUDIT_FAIL:",
            "VEVO_GROWTHBOOK_REPORTING_POPULATION_AUDIT_OK:",
            "Production GrowthBook allocation: \\`0%\\` (unchanged)",
        ):
            require(
                growthbook_meta_audit_workflow,
                marker,
                f"GrowthBook Meta/population audit workflow lost safety marker: {marker}",
            )
        for forbidden_action in (
            "aws scheduler update-schedule",
            "aws cloudformation delete-stack",
            "aws s3api delete-object",
            "aws sqs delete-queue",
            "ads_update",
            "ads_archive",
            "adcreatives_create",
        ):
            forbid(
                growthbook_meta_audit_workflow.lower(),
                forbidden_action,
                f"GrowthBook Meta/population audit must not use {forbidden_action}.",
            )
        for marker in (
            "FORBIDDEN_QUERY_KEYS",
            "VEVO_META_DIMENSION_AUDIT_OK:",
            "VEVO_META_DIMENSION_AUDIT_FAIL:",
            "VEVO_META_DIMENSION_AUDIT_START:schema=1",
            "client._get_json",
            "unexpected_internal_error",
        ):
            require(
                growthbook_meta_audit,
                marker,
                f"GrowthBook Meta audit lost safety marker: {marker}",
            )
        for forbidden_call in (
            "._post_json(",
            ".post(",
            ".put(",
            ".patch(",
            ".delete(",
        ):
            forbid(
                growthbook_meta_audit,
                forbidden_call,
                f"GrowthBook Meta audit must remain GET-only: {forbidden_call}",
            )
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "PRODUCTION_STACK_NAME: vevo-growthbook-production",
            "PRODUCTION_SERVICE_NAME: vevo-growthbook-collector-production",
            "GROWTHBOOK_ENVIRONMENT=production",
            "Production experiment registry must remain empty",
            "parameters.get('PublicRouteEnabled') != 'false'",
            "PRODUCTION_FOUNDATION_PREFLIGHT_OK:",
            "PLANNED_PRODUCTION_IDENTITY:instance-id=N/A:Fargate",
            "AWS mutations: `none`",
        ):
            require(
                growthbook_production_preflight_workflow,
                marker,
                f"GrowthBook Production preflight lost safety marker: {marker}",
            )
        for forbidden_action in (
            "aws cloudformation create-",
            "aws cloudformation update-",
            "aws cloudformation delete-",
            "aws cloudformation execute-",
            "aws ecs run-task",
            "aws ecs register-task-definition",
            "aws iam create-",
            "aws iam attach-",
            "aws iam put-",
            "aws iam delete-",
            "aws glue create-",
            "aws athena start-query-execution",
            "aws s3api put-",
            "aws s3api delete-",
        ):
            forbid(
                growthbook_production_preflight_workflow.lower(),
                forbidden_action,
                f"GrowthBook Production preflight must remain read-only: {forbidden_action}",
            )
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "natural retention recovery must be verified before foundation deploy",
            "workspace.get('reconciliation_checkpoint', {}).get(",
            "sanitized natural reconciliation evidence is absent",
            "natural reconciliation artifact identity is incomplete",
            "natural reconciliation evidence SHA-256 mismatch",
            "natural reconciliation evidence identity/safety drift",
            "natural reconciliation evidence timestamp schema drift",
            "natural reconciliation sanitized count drift",
            "natural reconciliation runtime/control evidence drift",
            "Production deployment gate is false",
            "Production foundation deployment gate is false",
            "successful read-only Production preflight evidence drift",
            "PREVIEW_RUNTIME_IDENTITY_OK:",
            "PLANNED_PRODUCTION_IDENTITY:",
            "PREDEPLOY_PRODUCTION_MODE_HARD_GATE_OK:",
            "scripts/resolve_growthbook_host_gate_runtime.py",
            "--expected-log-prefix collector",
            "--expected-private-cidr 172.31.0.0/16",
            "--change-set-type CREATE",
            "--phase production-foundation",
            "'PublicRouteEnabled': 'false'",
            "PRODUCTION_FOUNDATION_HARD_GATE_OK:",
            "COLLECTOR_LOCALHOST_HEALTH_OK:production:",
            "COLLECTOR_LOCALHOST_MARKER_OK:/app:",
            "PRODUCTION_FOUNDATION_ROUTE_DISABLED_OK:",
            "GROWTHBOOK_FOUNDATION_EVIDENCE_READY:",
            "Upload sanitized Production foundation evidence only",
            "path: vevo-growthbook-production-foundation-evidence.json",
            "retention-days: 14",
            "GrowthBook reader credentials: `not created`",
        ):
            require(
                growthbook_production_foundation_workflow,
                marker,
                f"GrowthBook Production foundation workflow lost safety marker: {marker}",
            )
        for marker in (
            "optional_absent_taskdef_bound",
            "host-gate ECS log stream contradicts task definition",
            'task.get("clusterArn") == expected_cluster_arn',
            'container.get("imageDigest") == expected_image_digest',
            "private_ip in private_network",
            "raw=false",
        ):
            require(
                growthbook_host_gate_runtime_resolver,
                marker,
                f"GrowthBook host-gate resolver lost safety marker: {marker}",
            )
        for forbidden_action in (
            "--change-set-type update",
            "'publicrouteenabled': 'true'",
            "docker push",
            "ecr create-repository",
            "cloudformation update-stack",
            "cloudformation delete-stack",
            "aws scheduler update-",
            "aws scheduler create-",
            "aws scheduler delete-",
            "aws s3api put-",
            "aws s3api delete-",
            "aws athena start-query-execution",
            "aws iam attach-",
            "aws iam put-",
            "aws iam delete-",
            "ads_update",
            "adcreatives_create",
            "submit",
        ):
            forbid(
                growthbook_production_foundation_workflow.lower(),
                forbidden_action,
                f"GrowthBook Production foundation workflow violated its boundary: {forbidden_action}",
            )
        for forbidden_artifact_path in (
            "path: deployed-production-stack.json",
            "path: production-service.json",
            "path: production-service-task.json",
            "path: production-host-gate-task.json",
            "path: production-host-gate.log",
            "path: production-task-definition.json",
        ):
            forbid(
                growthbook_production_foundation_workflow,
                forbidden_artifact_path,
                "GrowthBook Production foundation evidence must not upload raw AWS data: "
                f"{forbidden_artifact_path}",
            )
        if (
            growthbook_production_foundation_workflow.count(
                "uses: actions/upload-artifact@v4.6.2"
            )
            != 1
        ):
            raise AssertionError(
                "GrowthBook Production foundation must upload exactly one sanitized artifact."
            )
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "confirm_diagnostic:",
            "foundation evidence is already recorded",
            "GrowthBook Production allocation must remain zero",
            "Production experiment registry must remain empty",
            "Configure AWS credentials for read-only diagnostic",
            "cloudformation describe-stacks",
            "ecs describe-services",
            "elbv2 describe-target-health",
            "apigatewayv2 get-routes",
            "s3api get-public-access-block",
            "s3api get-bucket-policy-status",
            "s3api get-bucket-encryption",
            "s3api list-objects-v2",
            "scripts/summarize_growthbook_foundation_bucket.py",
            "FOUNDATION_DIAGNOSTIC_RUNTIME_OK:",
            "target=healthy:route=false:bucket-public=false:mutation=none",
            "AWS mutations: `none`",
            "no keys or content",
        ):
            require(
                growthbook_foundation_diagnostic_workflow,
                marker,
                f"GrowthBook foundation diagnostic lost safety marker: {marker}",
            )
        for marker in (
            "Return only safe class counts; never return or print object keys.",
            "bucket listing is truncated",
            "bucket listing count mismatch",
            "raw-events=",
            "athena-results=",
            "unexpected=",
            "keys=false:content=false",
        ):
            require(
                growthbook_foundation_bucket_summarizer,
                marker,
                f"GrowthBook foundation bucket summarizer lost safety marker: {marker}",
            )
        for forbidden_action in (
            "cloudformation create-",
            "cloudformation update-",
            "cloudformation execute-",
            "cloudformation delete-",
            "ecs run-task",
            "ecs update-service",
            "s3api put-",
            "s3api delete-",
            "iam create-",
            "iam delete-",
            "athena start-query-execution",
            "scheduler update-",
            "ads_update",
            "submit",
            "upload-artifact",
        ):
            forbid(
                growthbook_foundation_diagnostic_workflow.lower(),
                forbidden_action,
                f"GrowthBook foundation diagnostic must remain read-only: {forbidden_action}",
            )
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "OBSERVATION_FROM_UTC: '2026-08-24T14:34:30Z'",
            "OBSERVATION_THROUGH_UTC: '2026-08-24T14:38:00Z'",
            "gtm_published_zero_allocation_verification_pending",
            "vevo_growthbook_post_publish_zero_collector_observation",
            "ZERO_COLLECTOR_LOCAL_GATE_OK:",
            "Configure AWS credentials for bounded read-only observation",
            "ZERO_COLLECTOR_RUNTIME_GATE_OK:",
            "instance-id=N/A:Fargate:private-ip=",
            "runtime-path-source=immutable-image-prior-localhost-marker:",
            "logs filter-log-events",
            "--query 'events[].eventId'",
            "ZERO_COLLECTOR_OBSERVATION_OK:",
            "contains_cloudwatch_messages",
            "contains_event_or_request_ids",
            "AWS mutations: `none`",
        ):
            require(
                growthbook_zero_collector_workflow,
                marker,
                f"GrowthBook zero-collector workflow lost safety marker: {marker}",
            )
        for forbidden_action in (
            "cloudformation create-",
            "cloudformation update-",
            "cloudformation execute-",
            "cloudformation delete-",
            "ecs run-task",
            "ecs update-service",
            "logs put-",
            "logs delete-",
            "s3api put-",
            "s3api delete-",
            "iam create-",
            "iam delete-",
            "athena start-query-execution",
            "scheduler update-",
            "ads_update",
            "submit",
        ):
            forbid(
                growthbook_zero_collector_workflow.lower(),
                forbidden_action,
                f"GrowthBook zero-collector workflow must remain read-only: {forbidden_action}",
            )
        if (
            growthbook_zero_collector_workflow.count(
                "uses: actions/upload-artifact@v4.6.2"
            )
            != 1
        ):
            raise AssertionError(
                "GrowthBook zero-collector workflow must upload one sanitized artifact."
            )
        for marker in (
            "EXPECTED_SAFETY =",
            "ALLOWED_CHANGED_PATHS =",
            "canonical_evidence_bytes",
            "_changed_leaf_paths",
            '"api_request_count"',
            '"accepted_receipt_count"',
            '"zero_collector_request_verified"] = True',
            '"activation_allowed"',
            '"production_allocation_percent"',
        ):
            require(
                growthbook_zero_collector_recorder,
                marker,
                f"GrowthBook zero-collector recorder lost safety marker: {marker}",
            )
        for forbidden_client in (
            "import boto3",
            "from boto3",
            "import requests",
            "urllib.request",
            "import subprocess",
            "import socket",
            "selenium",
            "playwright",
        ):
            forbid(
                growthbook_zero_collector_recorder.lower(),
                forbidden_client,
                f"GrowthBook zero-collector recorder must remain offline: {forbidden_client}",
            )
        for marker in (
            "EXPECTED_SOURCE =",
            "EXPECTED_SAFETY =",
            "ALLOWED_CHANGED_PATHS =",
            "canonical_evidence_bytes",
            "_changed_leaf_paths",
            'post_publish["zero_collector_request_verified"] = True',
            'post_publish["growthbook_start_allowed"] = True',
            'scope["start_growthbook_experiment_exp_19g6mmt5wugpk"] = True',
            'scope["publish_growthbook_feature_revision_3"] = True',
            '"production_allocation_percent": 0',
        ):
            require(
                growthbook_post_publish_zero_collector_recorder,
                marker,
                (
                    "GrowthBook post-publish zero-collector recorder lost "
                    f"safety marker: {marker}"
                ),
            )
        for forbidden_client in (
            "import boto3",
            "from boto3",
            "import requests",
            "urllib.request",
            "import subprocess",
            "import socket",
            "selenium",
            "playwright",
        ):
            forbid(
                growthbook_post_publish_zero_collector_recorder.lower(),
                forbidden_client,
                (
                    "GrowthBook post-publish zero-collector recorder must remain "
                    f"offline: {forbidden_client}"
                ),
            )
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "RECOVER_32612205628",
            "GITHUB_RUN_ATTEMPT",
            "actions: read",
            "foundation evidence is already recorded",
            "Production allocation must remain zero",
            "Production experiment registry must remain empty",
            "scripts/verify_growthbook_foundation_recovery.py",
            "a successful foundation evidence recovery already exists",
            "FOUNDATION_RECOVERY_SINGLE_SUCCESS_GATE_OK:",
            "cloudformation list-stack-resources",
            "scripts/resolve_growthbook_host_gate_runtime.py",
            "aws ecs run-task",
            "COLLECTOR_LOCALHOST_HEALTH_OK:production:",
            "COLLECTOR_LOCALHOST_MARKER_OK:/app:",
            "FOUNDATION_RECOVERY_HOST_GATE_OK:",
            "scripts/summarize_growthbook_foundation_bucket.py",
            "s3api list-multipart-uploads",
            "bucket has incomplete multipart uploads",
            "Production GrowthBook reader absence could not be proven.",
            "FOUNDATION_RECOVERY_READER_ABSENCE_OK:",
            "FOUNDATION_RECOVERY_RUNTIME_OK:",
            "route=false:bucket=empty:credentials=none:allocation=0",
            "build_foundation_recovery_evidence",
            "schema=2",
            "Upload sanitized Production foundation recovery evidence only",
            "path: vevo-growthbook-production-foundation-evidence.json",
            "retention-days: 14",
        ):
            require(
                growthbook_foundation_recovery_workflow,
                marker,
                f"GrowthBook foundation recovery lost safety marker: {marker}",
            )
        if (
            growthbook_foundation_recovery_workflow.lower().count("aws ecs run-task")
            != 1
        ):
            raise AssertionError(
                "GrowthBook foundation recovery must run exactly one temporary ECS host gate."
            )
        for forbidden_action in (
            "cloudformation create-",
            "cloudformation update-",
            "cloudformation delete-",
            "s3api put-",
            "s3api delete-",
            "iam create-",
            "iam attach-",
            "iam put-",
            "iam delete-",
            "athena start-query-execution",
            "scheduler create-",
            "scheduler update-",
            "scheduler delete-",
            "docker push",
            "ads_update",
            "adcreatives_create",
            "submit",
        ):
            forbid(
                growthbook_foundation_recovery_workflow.lower(),
                forbidden_action,
                f"GrowthBook foundation recovery violated its boundary: {forbidden_action}",
            )
        for raw_artifact_path in (
            "path: creation-run.json",
            "path: creation-jobs.json",
            "path: prior-recovery-runs.json",
            "path: recovery-stack.json",
            "path: recovery-stack-resources.json",
            "path: recovery-service-task.json",
            "path: recovery-host-gate.log",
            "path: recovery-bucket-listing.json",
            "path: recovery-multipart-uploads.json",
        ):
            forbid(
                growthbook_foundation_recovery_workflow,
                raw_artifact_path,
                "GrowthBook foundation recovery must not upload raw state: "
                f"{raw_artifact_path}",
            )
        if (
            growthbook_foundation_recovery_workflow.count(
                "uses: actions/upload-artifact@v4.6.2"
            )
            != 1
        ):
            raise AssertionError(
                "GrowthBook foundation recovery must upload exactly one sanitized artifact."
            )
        for marker in (
            "EXPECTED_CREATION_RUN_ID = 32612205628",
            "EXPECTED_CREATION_MAIN_COMMIT",
            "EXPECTED_STEP_CONCLUSIONS",
            "creation run conclusion drift",
            "live stack resource allowlist drift",
            "CollectorPostRoute",
            "raw=false",
        ):
            require(
                growthbook_foundation_recovery_verifier,
                marker,
                f"GrowthBook foundation recovery verifier lost safety marker: {marker}",
            )
        for marker in (
            "The recorder is offline and fail closed.",
            "validate_foundation_evidence",
            "build_foundation_recovery_evidence",
            "foundation creation provenance mismatch",
            "ecs_run_task_for_localhost_verification_only",
            "foundation evidence bytes are not canonical",
            "foundation manifest change-set boundary drift",
            "route=false:allocation=0:reader-ready=true:clone=false",
        ):
            require(
                growthbook_production_foundation_recorder,
                marker,
                "GrowthBook Production foundation recorder lost safety marker: "
                f"{marker}",
            )
        for forbidden_client in (
            "import boto3",
            "import requests",
            "import httpx",
            "import socket",
            "import subprocess",
            "from facebook_ads",
        ):
            forbid(
                growthbook_production_foundation_recorder,
                forbidden_client,
                "GrowthBook Production foundation recorder must remain offline: "
                f"{forbidden_client}",
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
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "IAM_USER_NAME: vevo-growthbook-preview-reader",
            "IAM_USER_PATH: /vevo/growthbook/preview/",
            "GROWTHBOOK_IDENTITY_PREPROVISION_GATE",
            'parameters.get("PublicRouteEnabled") != "true"',
            "aws ecs run-task",
            "identity-host-gate-task.json",
            "COLLECTOR_LOCALHOST_HEALTH_OK:preview:${ACTIVE_VERSION}",
            "COLLECTOR_LOCALHOST_MARKER_OK:${RUNTIME_PATH}:${ACTIVE_VERSION}",
            "if not set(resources) <= allowed_resources:",
            "if observed_actions != allowed_actions:",
            "aws iam create-user",
            "aws iam attach-user-policy",
            "aws iam create-access-key",
            "aws iam list-access-keys",
            "aws iam list-user-tags",
            "openssl cms -encrypt -binary -aes-256-cbc",
            "uses: actions/upload-artifact@v4.6.2",
            "retention-days: 1",
            "GROWTHBOOK_PREVIEW_READER_ACTIVE",
            "GROWTHBOOK_PREVIEW_READER_FAILED_RUN_REVOKED",
        ):
            require(
                growthbook_reader_workflow,
                marker,
                f"GrowthBook Preview reader workflow lost safety marker: {marker}",
            )
        for forbidden_reader_marker in (
            "cat ${CREDENTIAL_JSON}",
            'cat "${CREDENTIAL_JSON}"',
            "GITHUB_ENV} < ${CREDENTIAL_JSON}",
            "GITHUB_OUTPUT} < ${CREDENTIAL_JSON}",
            "s3:DeleteObject",
        ):
            forbid(
                growthbook_reader_workflow,
                forbidden_reader_marker,
                f"GrowthBook Preview reader workflow contains unsafe marker: {forbidden_reader_marker}",
            )
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "IAM_USER_NAME: vevo-growthbook-production-reader",
            "IAM_USER_PATH: /vevo/growthbook/production/",
            "natural retention recovery must be verified before reader provisioning",
            "workspace.get('reconciliation_checkpoint', {}).get(",
            "Production foundation deployment is not recorded as verified",
            "Production reader provisioning gate is false",
            "Production reader evidence state must remain pending",
            "verified_downloaded_sha256_recorded",
            "foundation_evidence_artifact_sha256",
            "validate_foundation_evidence",
            "canonical_evidence_bytes",
            "READER_EVIDENCE_FILE: vevo-growthbook-production-reader-evidence.json",
            "scripts/record_growthbook_production_reader_evidence.py build",
            '--foundation-workflow-run-id "${FOUNDATION_WORKFLOW_RUN_ID}"',
            '--foundation-sha256 "${FOUNDATION_EVIDENCE_SHA256}"',
            "vevo-growthbook-production-reader-evidence.json",
            "Production foundation redeployment gate must be closed",
            "GrowthBook clone must remain disabled during reader provisioning",
            "PRODUCTION_READER_LOCAL_RELEASE_GATE_OK:",
            "parameters.get('PublicRouteEnabled') != 'false'",
            "PRODUCTION_READER_SERVICE_IDENTITY_OK:",
            "PRODUCTION_READER_PREPROVISION_HARD_GATE_OK:",
            "scripts/summarize_growthbook_foundation_bucket.py",
            "s3api list-multipart-uploads",
            "Production experiment bucket has incomplete multipart uploads",
            "scripts/resolve_growthbook_host_gate_runtime.py",
            "--expected-private-cidr 172.31.0.0/16",
            "log-stream-source=${PRODUCTION_READER_HOST_LOG_STREAM_SOURCE}",
            "Production GrowthBook reader absence could not be proven before host gate.",
            "Production GrowthBook reader absence could not be proven immediately before creation.",
            "PRODUCTION_READER_IAM_ABSENCE_OK:phase=pre-host:",
            "PRODUCTION_READER_IAM_ABSENCE_OK:phase=pre-create:",
            "COLLECTOR_LOCALHOST_HEALTH_OK:production:",
            "COLLECTOR_LOCALHOST_MARKER_OK:${RUNTIME_PATH}:",
            "if not set(resources) <= allowed_resources:",
            "if observed_actions != allowed_actions:",
            "if observed_resources != allowed_resources:",
            "aws iam create-user",
            "aws iam attach-user-policy",
            "aws iam create-access-key",
            "openssl cms -encrypt -binary -aes-256-cbc",
            "name: vevo-growthbook-production-reader-credentials-${{ github.run_id }}",
            "path: vevo-growthbook-production-reader.cms",
            "retention-days: 1",
            "name: vevo-growthbook-production-reader-evidence-${{ github.run_id }}",
            "path: vevo-growthbook-production-reader-evidence.json",
            "retention-days: 14",
            "GROWTHBOOK_PRODUCTION_READER_ACTIVE",
            "GROWTHBOOK_PRODUCTION_READER_FAILED_RUN_REVOKED",
            "GrowthBook control plane: `unchanged`",
        ):
            require(
                growthbook_production_reader_workflow,
                marker,
                f"GrowthBook Production reader workflow lost safety marker: {marker}",
            )
        if (
            growthbook_production_reader_workflow.count(
                "uses: actions/upload-artifact@v4.6.2"
            )
            != 2
        ):
            raise AssertionError(
                "GrowthBook Production reader must upload separate credential and evidence artifacts."
            )
        credential_upload = growthbook_production_reader_workflow.index(
            "Upload encrypted one-time Production credential handoff"
        )
        evidence_upload = growthbook_production_reader_workflow.index(
            "Upload sanitized Production reader evidence", credential_upload
        )
        success_confirmation = growthbook_production_reader_workflow.index(
            "Confirm successful Production reader provisioning", evidence_upload
        )
        if not credential_upload < evidence_upload < success_confirmation:
            raise AssertionError(
                "Production reader success must follow both artifact uploads."
            )
        if (
            "vevo-growthbook-production-reader-evidence.json"
            in (
                growthbook_production_reader_workflow[credential_upload:evidence_upload]
            )
        ):
            raise AssertionError(
                "Sanitized reader evidence must not share the credential artifact."
            )
        if (
            "vevo-growthbook-production-reader.cms"
            in (
                growthbook_production_reader_workflow[
                    evidence_upload:success_confirmation
                ]
            )
        ):
            raise AssertionError(
                "Encrypted credentials must not share the reader evidence artifact."
            )
        local_reader_gate = growthbook_production_reader_workflow.index(
            "PRODUCTION_READER_LOCAL_RELEASE_GATE_OK:"
        )
        reader_credentials = growthbook_production_reader_workflow.index(
            "Configure authenticated AWS reader-provisioning identity"
        )
        if local_reader_gate >= reader_credentials:
            raise AssertionError(
                "GrowthBook Production reader local release gate must precede AWS credentials."
            )
        cleanup_start = growthbook_production_reader_workflow.index(
            "cleanup_failed_provision() {"
        )
        cleanup_end = growthbook_production_reader_workflow.index(
            "trap cleanup_failed_provision ERR", cleanup_start
        )
        cleanup_block = growthbook_production_reader_workflow[cleanup_start:cleanup_end]
        marker_guard = cleanup_block.index('if [[ -f "${CREATED_MARKER}" ]]')
        for cleanup_action in (
            "aws iam delete-access-key",
            "aws iam detach-user-policy",
            "aws iam delete-user",
        ):
            if cleanup_block.index(cleanup_action) <= marker_guard:
                raise AssertionError(
                    "Production reader cleanup can revoke only an identity created by this run."
                )
        for stateful_workflow in (
            growthbook_natural_reconciliation_workflow,
            growthbook_production_foundation_workflow,
            growthbook_foundation_recovery_workflow,
            growthbook_production_reader_workflow,
        ):
            forbid(
                stateful_workflow,
                "workspace.get('workspace', {}).get('recurring_schedule'",
                "GrowthBook workflow reads recurring state from a non-authoritative path.",
            )
        for forbidden_production_reader_marker in (
            "vevo-growthbook-preview-reader",
            "/vevo/growthbook/preview/",
            "cat ${CREDENTIAL_JSON}",
            'cat "${CREDENTIAL_JSON}"',
            "GITHUB_ENV} < ${CREDENTIAL_JSON}",
            "GITHUB_OUTPUT} < ${CREDENTIAL_JSON}",
            "s3:DeleteObject",
            "cloudformation create-",
            "cloudformation update-",
            "cloudformation delete-",
            "apigatewayv2 create-",
            "apigatewayv2 update-",
            "apigatewayv2 delete-",
            "aws s3api put-",
            "aws s3api delete-",
            "aws scheduler create-",
            "aws scheduler update-",
            "aws scheduler delete-",
            "tagmanager",
            "ads_update",
            "adcreatives_create",
        ):
            forbid(
                growthbook_production_reader_workflow.lower(),
                forbidden_production_reader_marker.lower(),
                "GrowthBook Production reader workflow contains unsafe marker: "
                f"{forbidden_production_reader_marker}",
            )
        for forbidden_production_reader_recorder_marker in (
            "import boto3",
            "import requests",
            "import httpx",
            "import socket",
            "import subprocess",
            "facebook_ads",
            "tagmanager",
            "playwright",
            "selenium",
        ):
            forbid(
                growthbook_production_reader_recorder.lower(),
                forbidden_production_reader_recorder_marker.lower(),
                "GrowthBook Production reader evidence recorder must remain offline: "
                f"{forbidden_production_reader_recorder_marker}",
            )
        for marker in (
            '"vevo_growthbook_production_reader_evidence"',
            '"foundation_evidence_provenance"',
            '"contains_access_key_id": False',
            '"contains_secret_access_key": False',
            '"reader evidence bytes are not canonical"',
            '"reader evidence SHA-256 mismatch"',
            '"Production reader manifest change-set boundary drift"',
            "clone-ready=true",
            "validate_foundation_evidence",
            "canonical_evidence_bytes",
        ):
            require(
                growthbook_production_reader_recorder,
                marker,
                "GrowthBook Production reader evidence recorder lost safety marker: "
                f"{marker}",
            )
        production_reader_state = growthbook_workspace_config.get("athena", {}).get(
            "production", {}
        )
        recorded_reader = production_reader_state.get("successful_reader_provisioning")
        if (
            production_reader_state.get("credentials_created") is not True
            or production_reader_state.get("reader_provisioning_status")
            != "verified_active_encrypted_handoff_ready_for_growthbook"
            or production_reader_state.get("reader_provisioning_allowed") is not False
            or production_reader_state.get("reader_evidence_artifact_status")
            != "verified_downloaded_sha256_recorded"
            or production_reader_state.get("reader_evidence_contains_credentials")
            is not False
            or production_reader_state.get("reader_provisioning_run_id")
            != "32614706434"
            or production_reader_state.get("reader_provisioning_main_commit")
            != "79f1eb4b1b29bb65efbdbe310b0033e1a5a1f594"
            or production_reader_state.get("reader_evidence_artifact_sha256")
            != "1715f2b41a1bfd1d58524bdbad8369afc63b76a30d145f959a9cc742370b01d7"
            or not isinstance(recorded_reader, dict)
            or recorded_reader.get("workflow_run_id") != "32614706434"
            or recorded_reader.get("main_commit")
            != "79f1eb4b1b29bb65efbdbe310b0033e1a5a1f594"
            or recorded_reader.get("safety", {}).get("contains_plaintext_credentials")
            is not False
            or recorded_reader.get("safety", {}).get("contains_access_key_id")
            is not False
            or recorded_reader.get("safety", {}).get("contains_secret_access_key")
            is not False
        ):
            raise AssertionError(
                "GrowthBook Production reader evidence must remain exact and secret-free in source control."
            )
        for forbidden_production_clone_recorder_marker in (
            "import boto3",
            "import requests",
            "import httpx",
            "import socket",
            "import subprocess",
            "facebook_ads",
            "tagmanager",
            "playwright",
            "selenium",
        ):
            forbid(
                growthbook_production_clone_recorder.lower(),
                forbidden_production_clone_recorder_marker.lower(),
                "GrowthBook Production clone evidence recorder must remain offline: "
                f"{forbidden_production_clone_recorder_marker}",
            )
        for marker in (
            '"vevo_growthbook_production_clone_observation"',
            '"reader_evidence_provenance"',
            '"Production clone manifest change-set boundary drift"',
            '"clone observation bytes are not canonical"',
            '"Production assignment query must remain empty before traffic"',
            '"Preview connection was repointed"',
            '"paid Pro upgrade was not authorized"',
            '"production_allocation_percent": 0',
            "clone=verified:production-aa=false",
            "validate_reader_evidence",
            "canonical_evidence_bytes",
        ):
            require(
                growthbook_production_clone_recorder,
                marker,
                "GrowthBook Production clone evidence recorder lost safety marker: "
                f"{marker}",
            )
        for marker in (
            "Status: Production clone verified complete; Production A/A activation remains a separate reviewed gate",
            "vevo-growthbook-production-reader",
            "Production allocation is `0%`",
            "Preview connection repointing is forbidden",
            "Do not create the three p75 Quantile metrics",
            "record_growthbook_production_clone_evidence.py build",
            "record_growthbook_production_clone_evidence.py record",
            "Browser entry of these credentials requires explicit action-time confirmation",
            "do not automatically delete or repoint anything",
        ):
            require(
                growthbook_production_clone_runbook,
                marker,
                f"GrowthBook Production clone runbook lost safety marker: {marker}",
            )
        for production_sql_name, production_sql in (
            ("device outcomes", growthbook_production_device_outcomes_sql),
            ("performance vitals", growthbook_production_performance_vitals_sql),
        ):
            for marker in (
                "UNION ALL",
                "__growthbook_schema_only__",
                "FROM (VALUES (1)) AS schema_seed(x)",
                "WHERE '{{ experimentId }}' = '%'",
            ):
                require(
                    production_sql,
                    marker,
                    "GrowthBook Production "
                    f"{production_sql_name} SQL lost schema-probe safety marker: {marker}",
                )
        production_clone_state = production_reader_state.get("growthbook_clone", {})
        if (
            production_clone_state.get("status") != "verified_complete"
            or production_clone_state.get("clone_allowed") is not False
            or production_clone_state.get("mutation_status")
            != "created_and_query_verified"
            or production_clone_state.get("observation_status")
            != "verified_canonical_sha256_recorded"
            or production_clone_state.get("observation_sha256")
            != "b2f96b7047321f11da4f00c7886c4b9422d7759428534f8fd5534ee1299f2030"
            or hashlib.sha256(
                normalized_growthbook_production_clone_observation_bytes
            ).hexdigest()
            != production_clone_state.get("observation_sha256")
            or production_clone_state.get("successful_clone_verification")
            != growthbook_production_clone_observation
            or production_clone_state.get("target_data_source_id") != "ds_19g6mmt5stlp6"
            or production_clone_state.get("target_fact_table_ids")
            != {
                "vevo_device_outcomes_v1": "ftb_19g6mmt5tg48t",
                "vevo_performance_vitals_v1": "ftb_19g6lmt5ueyhu",
            }
            or production_clone_state.get("target_metric_ids")
            != {
                "vevo_add_to_cart_24h": "fact__2CeKm6X4Ez3PK8cRiuiKCL",
                "vevo_purchase_conversion_7d": "fact__2CeKm9AKQS2TwG6zRP2qBh",
                "vevo_revenue_per_exposed_device_7d": "fact__2CeKmFKftMtEguy95LunK5",
                "vevo_cm1_per_exposed_device_7d": "fact__2CeKmHDNgQ79FAHCSdZbU7",
                "vevo_average_order_value_7d": "fact__2CeKmKu3wGQyMM3eGPzSrE",
                "vevo_cancelled_order_rate_14d": "fact__2CeKmPJ6cGJsgkMnNvWgcn",
                "vevo_refunded_order_rate_14d": "fact__2CeKmSCrBNLu3xJXCT3zaf",
                "vevo_client_error_device_rate_24h": "fact__2CeKmUx5FqfRes5CGS6WAH",
            }
        ):
            raise AssertionError(
                "GrowthBook Production clone evidence must remain exact, canonical, and traffic-disabled."
            )
        forbid(
            growthbook_template,
            "s3:DeleteObject",
            "GrowthBook runtime policies must not delete experiment objects.",
        )
        growthbook_policy_section = growthbook_template.split(
            "  GrowthBookReadOnlyPolicy:", 1
        )[1].split("  CollectorTarget5xxAlarm:", 1)[0]
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
        if (
            growthbook_reporting_config.get("metric_contract_version")
            != "vevo_cm1_v1_2026-08-20"
        ):
            raise AssertionError(
                "VEVO GrowthBook reporting must keep the frozen CM1 metric contract."
            )
        if growthbook_reporting_config.get("cart_window_hours") != 24:
            raise AssertionError(
                "VEVO GrowthBook primary cart window must remain 24 hours."
            )
        if growthbook_reporting_config.get("order_window_days") != 7:
            raise AssertionError(
                "VEVO GrowthBook purchase attribution window must remain 7 days."
            )
        if growthbook_reporting_config.get("maturity_checkpoint_days") != 14:
            raise AssertionError(
                "VEVO GrowthBook maturity checkpoint must remain 14 days."
            )
        expected_aa_acceptance = {
            "schema_version": 1,
            "experiment_id": "vevo-sk-aa-001",
            "timezone": "Europe/Bratislava",
            "variations": ["control", "variant"],
            "expected_variation_weights": {"control": 0.5, "variant": 0.5},
            "required_production_allocation_percent": 100,
            "minimum_full_calendar_days": 7,
            "minimum_eligible_devices": 1000,
            "minimum_measured_page_loads_per_arm": 200,
            "minimum_exact_joined_transactions": 1,
            "minimum_meta_exposures": 1,
            "minimum_complete_stable_meta_dimension_exposures": 1,
            "privacy_sample_max_rows": 100,
            "srm_p_value_min": 0.001,
            "split_percent_min": 48,
            "split_percent_max": 52,
            "pipeline_count_difference_max_percent": 2,
            "growthbook_reporting_count_difference_max_percent": 2,
            "duplicate_event_rate_max_percent": 0.5,
            "exact_order_join_rate_min_percent": 98,
            "lcp_degradation_absolute_ms": 200,
            "lcp_degradation_relative_percent": 10,
            "inp_degradation_absolute_ms": 20,
            "inp_degradation_relative_percent": 10,
            "cls_degradation_absolute_milli": 20,
            "client_error_rate_increase_max_percentage_points": 0.5,
        }
        if growthbook_aa_acceptance != expected_aa_acceptance:
            raise AssertionError("VEVO GrowthBook A/A acceptance contract drifted.")
        for forbidden_aa_marker in (
            "import boto3",
            "import requests",
            "import httpx",
            "import socket",
            "import subprocess",
            "facebook_ads",
            "tagmanager",
            "update_experiment",
        ):
            forbid(
                growthbook_aa_evaluator.lower(),
                forbidden_aa_marker.lower(),
                f"GrowthBook A/A evaluator must remain offline: {forbidden_aa_marker}",
            )
        for required_aa_marker in (
            '"PASS"',
            '"FAIL"',
            '"NOT_READY"',
            '"winner_calls_allowed": False',
            'ZoneInfo(config["timezone"])',
            '"growthbook_reporting_variation_parity"',
            '"exact_order_join"',
            '"performance_guardrails"',
            '"consent_boundary"',
            '"commerce_health_and_rollback"',
        ):
            require(
                growthbook_aa_evaluator,
                required_aa_marker,
                f"GrowthBook A/A evaluator lost required gate marker: {required_aa_marker}",
            )
        for forbidden_receipt_summarizer_marker in (
            "import boto3",
            "import requests",
            "import httpx",
            "import socket",
            "import subprocess",
            "facebook_ads",
            "tagmanager",
        ):
            forbid(
                growthbook_receipt_summarizer.lower(),
                forbidden_receipt_summarizer_marker.lower(),
                "GrowthBook receipt summarizer must remain offline: "
                f"{forbidden_receipt_summarizer_marker}",
            )
        for required_receipt_summarizer_marker in (
            '"evidence_type": "vevo_growthbook_collector_receipt_counts"',
            '"contains_raw_log_events": False',
            '"contains_event_or_device_ids": False',
            'not payload.get("nextToken")',
            "set(receipt) == EXPECTED_RECEIPT_KEYS",
            'receipt["marker"] == RECEIPT_MARKER',
            'receipt["accepted"] is True',
            'type(receipt["duplicate"]) is bool',
        ):
            require(
                growthbook_receipt_summarizer,
                required_receipt_summarizer_marker,
                "GrowthBook receipt summarizer lost safety marker: "
                f"{required_receipt_summarizer_marker}",
            )
        for forbidden_snapshot_assembler_marker in (
            "import boto3",
            "import requests",
            "import httpx",
            "import socket",
            "import subprocess",
            "facebook_ads",
            "tagmanager",
        ):
            forbid(
                growthbook_aa_snapshot_assembler.lower(),
                forbidden_snapshot_assembler_marker.lower(),
                "GrowthBook A/A snapshot assembler must remain offline: "
                f"{forbidden_snapshot_assembler_marker}",
            )
        for required_snapshot_assembler_marker in (
            "evidence must use canonical JSON bytes",
            "SHA-256 mismatch",
            "component window mismatch",
            '"contains_raw_aws_payloads"',
            '"contains_cloudwatch_messages"',
            '"contains_event_or_device_ids"',
            '"contains_customer_or_order_data"',
            "evaluate(snapshot, config)",
        ):
            require(
                growthbook_aa_snapshot_assembler,
                required_snapshot_assembler_marker,
                "GrowthBook A/A snapshot assembler lost safety marker: "
                f"{required_snapshot_assembler_marker}",
            )
        try:
            validate_measurement_window(
                growthbook_aa_snapshot_manifest,
                growthbook_production_aa_activation,
                growthbook_aa_acceptance,
                growthbook_production_reconciliation_evidence,
            )
        except MeasurementWindowError as exc:
            raise AssertionError(
                f"GrowthBook A/A manifest lifecycle is invalid: {exc}"
            ) from exc
        validate_aa_completion()
        validate_cta_baseline_manifest(growthbook_cta_baseline_manifest)
        validate_cta_activation_manifest(growthbook_cta_activation_manifest)
        snapshot_boundaries = growthbook_aa_snapshot_manifest.get(
            "release_boundaries", {}
        )
        for boundary in ("main_only", "github_artifact_reads_only"):
            if snapshot_boundaries.get(boundary) is not True:
                raise AssertionError(
                    f"GrowthBook A/A snapshot boundary drift: {boundary}."
                )
        for boundary in (
            "aws_credentials_allowed",
            "aws_api_calls_allowed",
            "growthbook_mutation_allowed",
            "gtm_mutation_allowed",
            "meta_ads_mutation_allowed",
            "biznisweb_mutation_allowed",
            "winner_calls_allowed",
            "cta_activation_allowed",
        ):
            if snapshot_boundaries.get(boundary) is not False:
                raise AssertionError(
                    f"GrowthBook A/A snapshot mutation gate opened: {boundary}."
                )
        snapshot_output = growthbook_aa_snapshot_manifest.get("output", {})
        if (
            snapshot_output.get("artifact_name") != "vevo-growthbook-aa-snapshot"
            or snapshot_output.get("retention_days") != 14
        ):
            raise AssertionError("GrowthBook A/A snapshot output identity drift.")
        for output_boundary in (
            "contains_component_artifacts",
            "contains_raw_aws_payloads",
            "contains_cloudwatch_messages",
            "contains_event_or_device_ids",
            "contains_customer_or_order_data",
        ):
            if snapshot_output.get(output_boundary) is not False:
                raise AssertionError(
                    f"GrowthBook A/A snapshot output boundary opened: {output_boundary}."
                )
        for required_snapshot_workflow_marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "permissions:\n  contents: read\n  actions: read",
            "snapshot_build_allowed') is not True",
            "validate_growthbook_aa_measurement_window.py",
            "pre-registered A/A stopping rule is not resolved",
            "Production GrowthBook clone must be complete and re-closed",
            'gh api "repos/${GITHUB_REPOSITORY}/actions/runs/${AUTOMATED_RUN_ID}"',
            'gh run download "${AUTOMATED_RUN_ID}"',
            'gh run download "${MANUAL_QA_RUN_ID}"',
            "scripts/assemble_growthbook_aa_snapshot.py",
            "scripts/evaluate_growthbook_aa.py",
            "winner=false:cta=unchanged",
            "uses: actions/upload-artifact@v4.6.2",
        ):
            require(
                growthbook_aa_snapshot_workflow,
                required_snapshot_workflow_marker,
                "GrowthBook A/A snapshot workflow lost safety marker: "
                f"{required_snapshot_workflow_marker}",
            )
        if (
            growthbook_aa_snapshot_workflow.count(
                "uses: actions/upload-artifact@v4.6.2"
            )
            != 1
        ):
            raise AssertionError(
                "GrowthBook A/A snapshot must upload exactly one artifact."
            )
        for forbidden_snapshot_workflow_marker in (
            "configure-aws-credentials",
            "aws ",
            "boto3",
            "start-query-execution",
            "ecs run-task",
            "register-task-definition",
            "cloudformation create-",
            "cloudformation update-",
            "cloudformation delete-",
            "tagmanager",
            "ads_update",
            "adcreatives_create",
            "biznisweb_api_token",
            "curl ",
            "wget ",
        ):
            forbid(
                growthbook_aa_snapshot_workflow.lower(),
                forbidden_snapshot_workflow_marker.lower(),
                "GrowthBook A/A snapshot workflow must remain artifact-read-only: "
                f"{forbidden_snapshot_workflow_marker}",
            )
        for forbidden_manual_qa_builder_marker in (
            "import boto3",
            "import requests",
            "import httpx",
            "import socket",
            "import subprocess",
            "facebook_ads",
            "tagmanager",
            "playwright",
            "selenium",
        ):
            forbid(
                growthbook_aa_manual_qa_builder.lower(),
                forbidden_manual_qa_builder_marker.lower(),
                "GrowthBook A/A manual QA builder must remain offline: "
                f"{forbidden_manual_qa_builder_marker}",
            )
        for required_manual_qa_builder_marker in (
            "manual QA observation must use canonical JSON bytes",
            "observation SHA-256 mismatch",
            '"vevo_growthbook_aa_manual_qa_evidence"',
            '"source_run_id": workflow_run_id',
            '"source_main_commit": main_commit',
            "frozen 100 percent Production allocation",
            '"contains_event_or_device_ids"',
            '"contains_customer_or_order_data"',
            '"unplanned_mutation_observed"',
        ):
            require(
                growthbook_aa_manual_qa_builder,
                required_manual_qa_builder_marker,
                "GrowthBook A/A manual QA builder lost safety marker: "
                f"{required_manual_qa_builder_marker}",
            )
        for required_manual_qa_workflow_marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "manual QA evidence producer gate is closed",
            "validate_growthbook_aa_measurement_window.py",
            "pre-registered A/A stopping rule is not resolved",
            "manual QA observation differs from the pre-registered window",
            "manual QA evidence window is not complete",
            "reviewed browser QA observation is not recorded",
            "row.get('tracking_key'): row",
            "Production A/A is not the only running experiment",
            "CTA A/B must remain unstarted during manual A/A QA",
            "PRODUCTION_AA_MANUAL_QA_LOCAL_GATE_OK:",
            "scripts/build_growthbook_aa_manual_qa_evidence.py",
            '--workflow-run-id "${GITHUB_RUN_ID}"',
            '--main-commit "${GITHUB_SHA}"',
            "uses: actions/upload-artifact@v4.6.2",
            "winner=false:cta=unchanged",
        ):
            require(
                growthbook_aa_manual_qa_workflow,
                required_manual_qa_workflow_marker,
                "GrowthBook A/A manual QA workflow lost safety marker: "
                f"{required_manual_qa_workflow_marker}",
            )
        if (
            growthbook_aa_manual_qa_workflow.count(
                "uses: actions/upload-artifact@v4.6.2"
            )
            != 1
        ):
            raise AssertionError(
                "GrowthBook A/A manual QA must upload exactly one artifact."
            )
        for forbidden_manual_qa_workflow_marker in (
            "configure-aws-credentials",
            "aws ",
            "boto3",
            "curl ",
            "wget ",
            "requests",
            "httpx",
            "playwright",
            "selenium",
            "tagmanager",
            "ads_update",
            "adcreatives_create",
            "biznisweb_api_token",
            "gh api",
            "gh run",
        ):
            forbid(
                growthbook_aa_manual_qa_workflow.lower(),
                forbidden_manual_qa_workflow_marker.lower(),
                "GrowthBook A/A manual QA workflow must remain offline: "
                f"{forbidden_manual_qa_workflow_marker}",
            )
        for forbidden_automated_builder_marker in (
            "import boto3",
            "import requests",
            "import httpx",
            "import socket",
            "import subprocess",
            "facebook_ads",
            "tagmanager",
            "playwright",
            "selenium",
        ):
            forbid(
                growthbook_aa_automated_builder.lower(),
                forbidden_automated_builder_marker.lower(),
                "GrowthBook A/A automated evidence builder must remain offline: "
                f"{forbidden_automated_builder_marker}",
            )
        for required_automated_builder_marker in (
            "automated observation must use canonical JSON bytes",
            "observation SHA-256 mismatch",
            '"vevo_growthbook_aa_automated_evidence"',
            '"source_run_id": workflow_run_id',
            '"source_main_commit": main_commit',
            "automated source must be read-only",
            '"contains_raw_aws_payloads"',
            '"contains_cloudwatch_messages"',
            '"contains_event_or_device_ids"',
            '"contains_customer_or_order_data"',
            '"mutation_observed"',
        ):
            require(
                growthbook_aa_automated_builder,
                required_automated_builder_marker,
                "GrowthBook A/A automated evidence builder lost safety marker: "
                f"{required_automated_builder_marker}",
            )
        for required_automated_workflow_marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "automated evidence producer gate is closed",
            "validate_growthbook_aa_measurement_window.py",
            "frozen Production A/A evidence window is not recorded",
            "pre-registered A/A stopping rule is not resolved",
            "canonical Production reporting quality is not recorded",
            "Production localhost and marker hard gate is missing",
            "Production GrowthBook clone must be complete and re-closed",
            "row.get('tracking_key'): row",
            "PRODUCTION_AA_AUTOMATED_LOCAL_GATE_OK:",
            "PRODUCTION_AA_RUNTIME_HARD_GATE_OK:",
            "PRODUCTION_AA_GLUE_SCHEMA_OK:",
            "scripts/summarize_growthbook_receipts.py",
            "aws s3api get-object",
            "aws athena start-query-execution",
            "aws athena get-query-results",
            "scripts/build_growthbook_aa_automated_evidence.py",
            '--workflow-run-id "${GITHUB_RUN_ID}"',
            '--main-commit "${GITHUB_SHA}"',
            "Remove all temporary AWS responses and aggregate query files",
            "uses: actions/upload-artifact@v4.6.2",
            "winner=false:cta=unchanged",
        ):
            require(
                growthbook_aa_automated_workflow,
                required_automated_workflow_marker,
                "GrowthBook A/A automated evidence workflow lost safety marker: "
                f"{required_automated_workflow_marker}",
            )
        for forbidden_measurement_window_validator_marker in (
            "import boto3",
            "import requests",
            "import httpx",
            "import socket",
            "import subprocess",
            "urllib",
            "facebook_ads",
            "tagmanager",
            "playwright",
            "selenium",
        ):
            forbid(
                growthbook_aa_measurement_window_validator.lower(),
                forbidden_measurement_window_validator_marker.lower(),
                "GrowthBook A/A measurement-window validator must remain offline: "
                f"{forbidden_measurement_window_validator_marker}",
            )
        for required_measurement_window_validator_marker in (
            "frozen_start_and_stopping_rule_before_outcome_readback",
            "first_full_local_date",
            "minimum_full_calendar_days",
            "minimum_eligible_devices",
            "minimum_through_utc",
            "outcome_blind_resolution_required",
            "whole_local_day_extensions_only",
            "cumulative_eligible_devices_without_arm_outcome_readback",
            "post_hoc_window_change_allowed",
            "Production reconciliation schedule drift",
        ):
            require(
                growthbook_aa_measurement_window_validator,
                required_measurement_window_validator_marker,
                "GrowthBook A/A measurement-window validator lost marker: "
                f"{required_measurement_window_validator_marker}",
            )
        for offline_checkpoint_marker in (
            "import boto3",
            "import requests",
            "import httpx",
            "import socket",
            "import subprocess",
            "urllib",
            "facebook_ads",
            "tagmanager",
            "playwright",
            "selenium",
        ):
            forbid(
                growthbook_aa_window_checkpoint_recorder.lower(),
                offline_checkpoint_marker.lower(),
                "GrowthBook A/A window checkpoint recorder must remain offline: "
                f"{offline_checkpoint_marker}",
            )
        for required_checkpoint_marker in (
            "canonical_evidence_bytes",
            "expected_workflow_run_id",
            "expected_main_commit",
            "expected_evidence_sha256",
            "independently supplied evidence SHA-256 mismatch",
            "evidence SHA-256 mismatch",
            "A/A window is already resolved",
            "resolved_by_preregistered_sample_stopping_rule",
            "resolved_waiting_for_reviewed_producer_open",
            "snapshot_build_allowed",
            "producer_allowed",
        ):
            require(
                growthbook_aa_window_checkpoint_recorder,
                required_checkpoint_marker,
                "GrowthBook A/A window checkpoint recorder lost safety marker: "
                f"{required_checkpoint_marker}",
            )
        for offline_evidence_gate_marker in (
            "import boto3",
            "import requests",
            "import httpx",
            "import socket",
            "import subprocess",
            "urllib",
            "facebook_ads",
            "tagmanager",
            "playwright",
            "selenium",
        ):
            forbid(
                growthbook_aa_evidence_gate_recorder.lower(),
                offline_evidence_gate_marker.lower(),
                "GrowthBook A/A evidence-gate recorder must remain offline: "
                f"{offline_evidence_gate_marker}",
            )
        for required_evidence_gate_marker in (
            "independently supplied {field} SHA-256 mismatch",
            "quality report predates the resolved A/A window",
            "quality report eligible devices differ from the stopping checkpoint",
            "manual QA observation differs from the resolved A/A window",
            "expected_workflow_run_id",
            "expected_main_commit",
            "expected evidence SHA-256 is invalid",
            "snapshot_build_allowed",
            "producer_allowed",
            "aws=false:network=false:biznisweb=false:meta=false:commerce=false",
        ):
            require(
                growthbook_aa_evidence_gate_recorder,
                required_evidence_gate_marker,
                "GrowthBook A/A evidence-gate recorder lost safety marker: "
                f"{required_evidence_gate_marker}",
            )
        for offline_completion_marker in (
            "import boto3",
            "import requests",
            "import httpx",
            "import socket",
            "import subprocess",
            "urllib",
            "facebook_ads",
            "tagmanager",
            "playwright",
            "selenium",
        ):
            forbid(
                growthbook_aa_completion_recorder.lower(),
                offline_completion_marker.lower(),
                "GrowthBook A/A completion recorder must remain offline: "
                f"{offline_completion_marker}",
            )
        for required_completion_marker in (
            "A/A decision differs from independent evaluation",
            "A/A completion requires PASS",
            "manual_growthbook_stop_allowed",
            "automatic_growthbook_mutation_allowed",
            "stop_exact_aa_experiment_and_remove_only_its_production_live_rule",
            "aa_production_live_rule_count",
            "cta_production_live_rule_count",
            "price_cart_checkout_order_mutation_performed",
            "VEVO_AA_PASS_RECORDED:",
            "VEVO_AA_STOP_RECORDED:",
        ):
            require(
                growthbook_aa_completion_recorder,
                required_completion_marker,
                "GrowthBook A/A completion recorder lost safety marker: "
                f"{required_completion_marker}",
            )
        for required_completion_validator_marker in (
            "A/A stop observation is not canonical JSON",
            "A/A stop observation exists before completion",
            "validate_manifest",
        ):
            require(
                growthbook_aa_completion_validator,
                required_completion_validator_marker,
                "GrowthBook A/A completion validator lost safety marker: "
                f"{required_completion_validator_marker}",
            )
        for offline_cta_baseline_marker in (
            "import boto3",
            "import requests",
            "import httpx",
            "import socket",
            "import subprocess",
            "urllib",
            "facebook_ads",
            "tagmanager",
            "playwright",
            "selenium",
        ):
            forbid(
                growthbook_cta_baseline_builder.lower(),
                offline_cta_baseline_marker.lower(),
                "GrowthBook CTA baseline builder must remain offline: "
                f"{offline_cta_baseline_marker}",
            )
        for required_cta_baseline_marker in (
            "CTA baseline requires verified A/A PASS and stop readback",
            "CTA baseline 24-hour follow-up is incomplete",
            "variation_breakdown_allowed",
            "contains_device_or_event_identity",
            "contains_customer_or_order_data",
            "CTA SQL template SHA-256 drift",
            "CTA baseline must not emit or filter an arm breakdown",
            "CTA baseline must not select raw rows",
            "VEVO_CTA_BASELINE_READY:",
        ):
            require(
                growthbook_cta_baseline_builder,
                required_cta_baseline_marker,
                "GrowthBook CTA baseline builder lost safety marker: "
                f"{required_cta_baseline_marker}",
            )
        for required_cta_baseline_workflow_marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "Require verified A/A completion before AWS credentials",
            "PRODUCTION_CTA_BASELINE_LOCAL_GATE_OK:",
            "PRODUCTION_CTA_BASELINE_RUNTIME_HARD_GATE_OK:",
            "instance-id=N/A:Fargate:private-ip=",
            "localhost-marker=inherited-verified",
            "Run one aggregate-only Athena CTA baseline query",
            "--max-results 2",
            "variation-breakdown=false:activation=false",
            "Remove all temporary AWS responses and aggregate query files",
            "UI test: `not applicable; read-only aggregate workflow makes no UI change`",
        ):
            require(
                growthbook_cta_baseline_workflow,
                required_cta_baseline_workflow_marker,
                "GrowthBook CTA baseline workflow lost safety marker: "
                f"{required_cta_baseline_workflow_marker}",
            )
        if (
            growthbook_cta_baseline_workflow.count(
                "uses: actions/upload-artifact@v4.6.2"
            )
            != 1
        ):
            raise AssertionError(
                "GrowthBook CTA baseline must upload exactly one artifact."
            )
        for forbidden_cta_baseline_workflow_marker in (
            "cloudformation create-",
            "cloudformation update-",
            "cloudformation delete-",
            "ecs run-task",
            "ecs update-service",
            "register-task-definition",
            "iam create-",
            "iam update-",
            "iam delete-",
            "s3api put-object",
            "s3api delete-object",
            "glue create-",
            "glue update-",
            "glue delete-",
            "tagmanager",
            "ads_update",
            "adcreatives_create",
            "biznisweb_api_token",
            "curl ",
            "wget ",
        ):
            forbid(
                growthbook_cta_baseline_workflow.lower(),
                forbidden_cta_baseline_workflow_marker.lower(),
                "GrowthBook CTA baseline workflow mutation path detected: "
                f"{forbidden_cta_baseline_workflow_marker}",
            )
        for offline_cta_activation_marker in (
            "import boto3",
            "import requests",
            "import httpx",
            "import socket",
            "import subprocess",
            "urllib",
            "facebook_ads",
            "tagmanager",
            "playwright",
            "selenium",
        ):
            forbid(
                growthbook_cta_activation_recorder.lower(),
                offline_cta_activation_marker.lower(),
                "GrowthBook CTA activation recorder must remain offline: "
                f"{offline_cta_activation_marker}",
            )
        for required_cta_activation_marker in (
            "CTA activation requires verified A/A PASS and stop readback",
            "collector Production registry is not CTA-only",
            "CTA localhost marker is not verified",
            "CTA events exist before activation",
            "A/A Production allocation is nonzero",
            "GTM has unprocessed changes",
            "CTA is not the only active Production experiment",
            "automatic_growthbook_mutation_allowed",
            "price_product_cart_checkout_order_mutation_allowed",
            "VEVO_CTA_START_REVIEW_OPENED:",
            "VEVO_CTA_START_RECORDED:",
        ):
            require(
                growthbook_cta_activation_recorder,
                required_cta_activation_marker,
                "GrowthBook CTA activation recorder lost safety marker: "
                f"{required_cta_activation_marker}",
            )
        for offline_cta_runtime_source_name, offline_cta_runtime_source in (
            (
                "release validator",
                growthbook_cta_runtime_release_validator,
            ),
            (
                "canonical builder",
                growthbook_cta_runtime_builder,
            ),
        ):
            for forbidden_cta_runtime_source_marker in (
                "import boto3",
                "import requests",
                "import httpx",
                "import socket",
                "import subprocess",
                "urllib",
                "facebook_ads",
                "tagmanager",
                "playwright",
                "selenium",
            ):
                forbid(
                    offline_cta_runtime_source.lower(),
                    forbidden_cta_runtime_source_marker.lower(),
                    f"GrowthBook CTA runtime {offline_cta_runtime_source_name} must remain offline: "
                    f"{forbidden_cta_runtime_source_marker}",
                )
        for required_cta_runtime_release_marker in (
            "CTA activation manifest is not waiting",
            "A/A stop observation file/hash binding drift",
            "A/A stop observation is not canonical JSON",
            "CTA design contract SHA-256 drift",
            "CTA decision contract SHA-256 drift",
            "checked-in storefront must remain compile-time Production-disabled",
            "VEVO_CTA_RUNTIME_RELEASE_GATE_OK:",
        ):
            require(
                growthbook_cta_runtime_release_validator,
                required_cta_runtime_release_marker,
                "GrowthBook CTA runtime release validator lost safety marker: "
                f"{required_cta_runtime_release_marker}",
            )
        for required_cta_runtime_builder_marker in (
            "Production registry is not CTA-only",
            "Production CTA contract differs from Preview",
            "CTA events exist before start",
            '"host_gate_task_id": host_gate_task_id',
            '"host_gate_private_ip": host_gate_private_ip',
            '"contains_event_or_device_ids": False',
            '"price_product_cart_checkout_order_mutated": False',
            "VEVO_CTA_RUNTIME_OBSERVATION_OK:",
        ):
            require(
                growthbook_cta_runtime_builder,
                required_cta_runtime_builder_marker,
                "GrowthBook CTA runtime canonical builder lost safety marker: "
                f"{required_cta_runtime_builder_marker}",
            )
        for required_cta_runtime_workflow_marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "validate_growthbook_cta_runtime_release.py",
            "Confirm exact current Fargate instance IP service and path before code deploy",
            "VEVO_CTA_PREDEPLOY_HARD_GATE_OK:",
            "candidate['PublicRouteEnabled'] = 'true'",
            "--phase candidate",
            "COLLECTOR_LOCALHOST_HEALTH_OK:production:git-${GITHUB_SHA}",
            "COLLECTOR_LOCALHOST_MARKER_OK:/app:git-${GITHUB_SHA}",
            "COLLECTOR_REGISTRY_OK:production:${REGISTRY_SHA256}:vevo-sk-product-cta-color-001",
            "VEVO_CTA_SERVICE_RUNTIME_OK:",
            "SELECT COUNT(*) AS cta_events_before_start",
            "VEVO_CTA_ZERO_EVENTS_OK:",
            "build_growthbook_cta_runtime_readiness.py",
            "Remove all temporary AWS responses queries and logs",
            "uses: actions/upload-artifact@v4.6.2",
            "Restore the exact preceding collector runtime after a failed candidate gate",
            "PREVIOUS_IMAGE_IDENTIFIER",
            "VEVO_CTA_RUNTIME_ROLLBACK_OK:",
            "GTM/Meta Ads/BiznisWeb/commerce mutations: \\`none\\`",
        ):
            require(
                growthbook_cta_runtime_workflow,
                required_cta_runtime_workflow_marker,
                "GrowthBook CTA runtime workflow lost safety marker: "
                f"{required_cta_runtime_workflow_marker}",
            )
        for forbidden_cta_runtime_workflow_marker in (
            "--phase activate",
            "--phase deactivate",
            "apigatewayv2 create-route",
            "apigatewayv2 delete-route",
            "cloudformation delete-stack",
            "ecs update-service",
            "register-task-definition",
            "api.growthbook.io",
            "graph.facebook.com",
            "biznisweb_api_token",
        ):
            forbid(
                growthbook_cta_runtime_workflow.lower(),
                forbidden_cta_runtime_workflow_marker.lower(),
                "GrowthBook CTA runtime workflow contains unsafe mutation path: "
                f"{forbidden_cta_runtime_workflow_marker}",
            )
        require(
            growthbook_collector_host_gate,
            "python -m growthbook_collector.runtime_marker",
            "GrowthBook collector host gate lost packaged registry marker",
        )
        for required_checkpoint_workflow_marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "outcome-blind A/A checkpoint is outside its daily gate",
            "snapshot build opened before A/A window resolution",
            "producer opened before A/A window resolution",
            "PRODUCTION_AA_WINDOW_LOCAL_GATE_OK:",
            "PRODUCTION_AA_WINDOW_RUNTIME_GATE_OK:",
            "PRODUCTION_AA_WINDOW_CONTROL_GATE_OK:",
            "SELECT COUNT(DISTINCT device_id) AS eligible_devices",
            "header != ['eligible_devices']",
            "arm_counts_read': False",
            "arm_outcomes_read': False",
            "outcome_metrics_read': False",
            "validate_checkpoint_evidence(evidence, expected, index)",
            "Remove every temporary AWS response and query file",
            "uses: actions/upload-artifact@v4.6.2",
            "Snapshot/producer/CTA/winner gates changed: `none`",
        ):
            require(
                growthbook_aa_window_checkpoint_workflow,
                required_checkpoint_workflow_marker,
                "GrowthBook A/A window checkpoint workflow lost safety marker: "
                f"{required_checkpoint_workflow_marker}",
            )
        if (
            growthbook_aa_window_checkpoint_workflow.count(
                "uses: actions/upload-artifact@v4.6.2"
            )
            != 1
        ):
            raise AssertionError(
                "GrowthBook A/A window checkpoint must upload exactly one artifact."
            )
        for forbidden_checkpoint_workflow_marker in (
            "cloudformation create-",
            "cloudformation update-",
            "cloudformation delete-",
            "ecs run-task",
            "ecs update-service",
            "register-task-definition",
            "scheduler update-schedule",
            "scheduler create-schedule",
            "s3api put-object",
            "s3api delete-object",
            "tagmanager",
            "ads_update",
            "adcreatives_create",
            "biznisweb_api_token",
            "curl ",
            "wget ",
        ):
            forbid(
                growthbook_aa_window_checkpoint_workflow.lower(),
                forbidden_checkpoint_workflow_marker.lower(),
                "GrowthBook A/A window checkpoint workflow mutation path detected: "
                f"{forbidden_checkpoint_workflow_marker}",
            )
        if (
            growthbook_aa_automated_workflow.count(
                "uses: actions/upload-artifact@v4.6.2"
            )
            != 1
        ):
            raise AssertionError(
                "GrowthBook A/A automated evidence must upload exactly one artifact."
            )
        for forbidden_automated_workflow_marker in (
            "cloudformation create-",
            "cloudformation update-",
            "cloudformation delete-",
            "ecs run-task",
            "ecs update-service",
            "register-task-definition",
            "iam create-",
            "iam update-",
            "iam delete-",
            "s3api put-object",
            "s3api delete-object",
            "glue create-",
            "glue update-",
            "glue delete-",
            "tagmanager",
            "ads_update",
            "adcreatives_create",
            "biznisweb_api_token",
            "curl ",
            "wget ",
        ):
            forbid(
                growthbook_aa_automated_workflow.lower(),
                forbidden_automated_workflow_marker.lower(),
                "GrowthBook A/A automated evidence workflow gained a mutation path: "
                f"{forbidden_automated_workflow_marker}",
            )
        preview_registry = growthbook_registry_config.get("environments", {}).get(
            "preview", {}
        )
        for experiment_id, weights in growthbook_reporting_config.get(
            "expected_variation_weights", {}
        ).items():
            registry_variations = preview_registry.get(experiment_id, {}).get(
                "variations", []
            )
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
            "scripts/run_scheduled_growthbook_reconciliation.py",
            "scripts/summarize_growthbook_natural_task_readback.py",
            "scripts/build_growthbook_reconciliation_parameters.py",
            "scripts/audit_vevo_meta_dimensions.py",
            "scripts/build_vevo_growthbook_gtm_tag.py",
            "scripts/evaluate_growthbook_aa.py",
            "scripts/summarize_growthbook_receipts.py",
            "scripts/assemble_growthbook_aa_snapshot.py",
            "scripts/build_growthbook_aa_manual_qa_evidence.py",
            "scripts/build_growthbook_aa_automated_evidence.py",
            "scripts/record_growthbook_aa_evidence_gates.py",
            "scripts/record_growthbook_aa_completion.py",
            "scripts/validate_growthbook_aa_completion.py",
            "scripts/build_growthbook_cta_baseline_observation.py",
            "scripts/record_growthbook_cta_activation.py",
            "scripts/validate_growthbook_cta_runtime_release.py",
            "scripts/build_growthbook_cta_runtime_readiness.py",
            "growthbook_collector/runtime_marker.py",
            "scripts/record_growthbook_production_reader_evidence.py",
            "scripts/record_growthbook_foundation_evidence.py",
            "scripts/summarize_growthbook_foundation_bucket.py",
            "scripts/verify_growthbook_foundation_recovery.py",
            "scripts/record_growthbook_production_clone_evidence.py",
            "scripts/evaluate_growthbook_cta.py",
            "scripts/record_growthbook_cta_lifecycle_reconciliation.py",
            "scripts/validate_growthbook_cta_design.py",
            "scripts/validate_growthbook_changeset.py",
            "scripts/validate_growthbook_reconciliation_changeset.py",
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
