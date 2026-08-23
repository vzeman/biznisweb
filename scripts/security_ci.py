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
        growthbook_workspace_config = json.loads(
            read("projects/vevo/growthbook_workspace.json")
        )
        growthbook_reporting_config = json.loads(read("projects/vevo/growthbook_reporting.json"))
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
        growthbook_aa_snapshot_manifest = json.loads(
            read("projects/vevo/growthbook_aa_snapshot.json")
        )
        growthbook_aa_acceptance = json.loads(
            read("projects/vevo/growthbook_aa_acceptance.json")
        )
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
        for marker in (
            'scheduled reconciliation accepts no arguments',
            'GROWTHBOOK_FACT_PUBLISH_ENABLED',
            'rolling_partition_days',
            'max_raw_events',
            'GROWTHBOOK_SCHEDULED_RECONCILIATION_OK',
            'GROWTHBOOK_SCHEDULED_RECONCILIATION_FAILURE',
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
            "RoleName: vevo-growthbook-reconcile-preview-scheduler",
            "Action: ecs:RunTask",
            "Action: iam:PassRole",
            "EnableExecuteCommand: false",
            "MaximumEventAgeInSeconds: 3600",
            "MaximumRetryAttempts: 2",
            "GROWTHBOOK_SCHEDULED_RECONCILIATION_FAILURE",
            "GROWTHBOOK_SCHEDULED_RECONCILIATION_OK",
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
        ):
            forbid(
                growthbook_reconciliation_workflow,
                forbidden_action,
                f"GrowthBook reconciliation workflow must not use {forbidden_action}.",
            )
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "Require exact manual read-only confirmation",
            "[[ \"${CONFIRM_VERIFICATION}\" == \"true\" ]]",
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
            "--evidence-output \"${EVIDENCE_FILE}\"",
            "--workflow-run-id \"${GITHUB_RUN_ID}\"",
            "--main-commit \"${GITHUB_SHA}\"",
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
        if growthbook_natural_reconciliation_workflow.count(
            "uses: actions/upload-artifact@v4.6.2"
        ) != 1:
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
        for forbidden_call in ("._post_json(", ".post(", ".put(", ".patch(", ".delete("):
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
            "task.get(\"clusterArn\") == expected_cluster_arn",
            "container.get(\"imageDigest\") == expected_image_digest",
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
        if growthbook_production_foundation_workflow.count(
            "uses: actions/upload-artifact@v4.6.2"
        ) != 1:
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
        if growthbook_foundation_recovery_workflow.lower().count("aws ecs run-task") != 1:
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
        if growthbook_foundation_recovery_workflow.count(
            "uses: actions/upload-artifact@v4.6.2"
        ) != 1:
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
            'COLLECTOR_LOCALHOST_HEALTH_OK:preview:${ACTIVE_VERSION}',
            'COLLECTOR_LOCALHOST_MARKER_OK:${RUNTIME_PATH}:${ACTIVE_VERSION}',
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
            "cat \"${CREDENTIAL_JSON}\"",
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
        if growthbook_production_reader_workflow.count(
            "uses: actions/upload-artifact@v4.6.2"
        ) != 2:
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
        if "vevo-growthbook-production-reader-evidence.json" in (
            growthbook_production_reader_workflow[credential_upload:evidence_upload]
        ):
            raise AssertionError(
                "Sanitized reader evidence must not share the credential artifact."
            )
        if "vevo-growthbook-production-reader.cms" in (
            growthbook_production_reader_workflow[evidence_upload:success_confirmation]
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
            "cat \"${CREDENTIAL_JSON}\"",
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
            or production_reader_state.get("reader_evidence_contains_credentials") is not False
            or production_reader_state.get("reader_provisioning_run_id") != "32614706434"
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
            or recorded_reader.get("safety", {}).get("contains_access_key_id") is not False
            or recorded_reader.get("safety", {}).get("contains_secret_access_key") is not False
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
            "Status: Production foundation and reader evidence verified; reviewed UI clone allowed but not started",
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
        production_clone_state = production_reader_state.get("growthbook_clone", {})
        if (
            production_clone_state.get("status")
            != "reader_verified_ready_for_reviewed_growthbook_clone"
            or production_clone_state.get("clone_allowed") is not True
            or production_clone_state.get("mutation_status") != "not_started"
            or production_clone_state.get("observation_status") != "not_recorded"
            or production_clone_state.get("observation_sha256") is not None
            or production_clone_state.get("successful_clone_verification") is not None
            or production_clone_state.get("target_data_source_id") is not None
            or any(
                value is not None
                for group in ("target_fact_table_ids", "target_metric_ids")
                for value in (production_clone_state.get(group) or {}).values()
            )
        ):
            raise AssertionError(
                "GrowthBook Production clone must remain reader-ready and unexecuted in source control."
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
            'set(receipt) == EXPECTED_RECEIPT_KEYS',
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
        if growthbook_aa_snapshot_manifest.get("snapshot_build_allowed") is not False:
            raise AssertionError("GrowthBook A/A snapshot build must remain disabled before evidence.")
        for evidence_group in ("automated_evidence", "manual_qa_evidence"):
            evidence = growthbook_aa_snapshot_manifest.get(evidence_group, {})
            if evidence.get("status") != "not_recorded" or any(
                evidence.get(field) is not None
                for field in ("run_id", "main_commit", "sha256")
            ):
                raise AssertionError(
                    f"GrowthBook A/A snapshot evidence opened early: {evidence_group}."
                )
        manual_qa_manifest = growthbook_aa_snapshot_manifest.get(
            "manual_qa_evidence", {}
        )
        automated_manifest = growthbook_aa_snapshot_manifest.get(
            "automated_evidence", {}
        )
        if (
            automated_manifest.get("producer_allowed") is not False
            or automated_manifest.get("window_status") != "not_recorded"
            or automated_manifest.get("from_utc") is not None
            or automated_manifest.get("through_utc") is not None
            or automated_manifest.get("quality_report_status") != "not_recorded"
            or automated_manifest.get("quality_report_key") is not None
            or automated_manifest.get("quality_report_sha256") is not None
        ):
            raise AssertionError("GrowthBook A/A automated producer opened early.")
        if (
            manual_qa_manifest.get("producer_allowed") is not False
            or manual_qa_manifest.get("observation_status") != "not_recorded"
            or manual_qa_manifest.get("observation_sha256") is not None
        ):
            raise AssertionError("GrowthBook A/A manual QA producer opened early.")
        snapshot_boundaries = growthbook_aa_snapshot_manifest.get("release_boundaries", {})
        for boundary in ("main_only", "github_artifact_reads_only"):
            if snapshot_boundaries.get(boundary) is not True:
                raise AssertionError(f"GrowthBook A/A snapshot boundary drift: {boundary}.")
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
                raise AssertionError(f"GrowthBook A/A snapshot mutation gate opened: {boundary}.")
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
            "Production GrowthBook clone must be complete and re-closed",
            "gh api \"repos/${GITHUB_REPOSITORY}/actions/runs/${AUTOMATED_RUN_ID}\"",
            "gh run download \"${AUTOMATED_RUN_ID}\"",
            "gh run download \"${MANUAL_QA_RUN_ID}\"",
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
        if growthbook_aa_snapshot_workflow.count(
            "uses: actions/upload-artifact@v4.6.2"
        ) != 1:
            raise AssertionError("GrowthBook A/A snapshot must upload exactly one artifact.")
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
        if growthbook_aa_manual_qa_workflow.count(
            "uses: actions/upload-artifact@v4.6.2"
        ) != 1:
            raise AssertionError("GrowthBook A/A manual QA must upload exactly one artifact.")
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
            "frozen Production A/A evidence window is not recorded",
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
        if growthbook_aa_automated_workflow.count(
            "uses: actions/upload-artifact@v4.6.2"
        ) != 1:
            raise AssertionError("GrowthBook A/A automated evidence must upload exactly one artifact.")
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
