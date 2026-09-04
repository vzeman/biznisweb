from __future__ import annotations

import copy
import json
import pathlib
import subprocess
import sys
import unittest

from scripts.suspend_growthbook_preview import ALLOWED, STACK, RECONCILIATION, TEMPLATES, build_template, build_template_body, template_load, validate_changes, validate_manifest, validate_template_delta

ROOT = pathlib.Path(__file__).resolve().parents[1]


def changes(stack):
    return {"Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE", "Changes": [{"ResourceChange": {
        "LogicalResourceId": name, "ResourceType": kind, "Action": "Modify", "Replacement": "False", "Scope": ["Properties"],
        "Details": [{"Target": {"Attribute": "Properties", "Name": field}} for field in fields]}}
        for name, (kind, fields) in ALLOWED[stack].items()]}


def active_template(proposed, stack):
    old = copy.deepcopy(proposed)
    del old["Parameters"]["PreviewSuspended"]
    del old["Conditions"]["IsPreviewSuspended"]
    del old["Rules"]
    for logical, (_, fields) in ALLOWED[stack].items():
        for field in fields:
            old["Resources"][logical]["Properties"][field] = copy.deepcopy(proposed["Resources"][logical]["Properties"][field]["Fn::If"][2])
    if stack == RECONCILIATION:
        old["Outputs"]["ScheduleState"]["Value"] = {"Ref": "ScheduleState"}
    return old


class PreviewSuspendTests(unittest.TestCase):
    def test_verified_suspension_closes_replay_and_preserves_safety_boundary(self):
        state = json.loads((ROOT / "projects/vevo/growthbook_preview_lifecycle.json").read_text(encoding="utf-8"))
        self.assertEqual(state["status"], "suspended_verified")
        self.assertEqual(state["desired_state"], "suspended")
        for field in ("deletion_allowed", "production_mutation_allowed", "ordinary_preview_deploy_allowed"):
            self.assertIs(state[field], False)
        with self.assertRaisesRegex(ValueError, "sleep transition not open"):
            validate_manifest(state)
        evidence = state["execution_evidence"]
        self.assertEqual(evidence["workflow"], ".github/workflows/suspend-vevo-growthbook-preview.yml")
        self.assertEqual(evidence["workflow_run_id"], "33887188363")
        self.assertEqual(evidence["main_commit"], "595a39091f990cbe4028c9ea7e83185d08f771fe")
        for field in ("zip_sha256", "json_sha256", "approved_manifest_sha256"):
            self.assertRegex(evidence[field], r"^[a-f0-9]{64}$")
        verified = evidence["verification"]
        for field in ("successful_exact_main_run", "github_zip_digest_verified", "single_canonical_json_verified",
                      "approved_manifest_blob_verified", "resource_inventory_unchanged", "protected_fingerprints_unchanged",
                      "localhost_before_and_after_verified", "load_balancer_retained", "ordinary_preview_deploy_blocked"):
            self.assertIs(verified[field], True)
        for field in ("data_read", "data_deleted", "automatic_resume_allowed"):
            self.assertIs(verified[field], False)
        self.assertEqual(verified["collector_desired_count"], 0)
        self.assertEqual(verified["collector_running_count"], 0)
        self.assertEqual(verified["preview_schedule_state"], "DISABLED")
        self.assertEqual(verified["diagnostic_tasks_stopped"], 4)
        self.assertEqual(set(evidence["final_preview_stack_sha256"]), {STACK, RECONCILIATION})

    def test_only_reviewed_changes_pass(self):
        for stack in ALLOWED:
            validate_changes(changes(stack), stack)

    def test_add_remove_replacement_and_unrelated_property_fail(self):
        for field, value in (("Action", "Remove"), ("Action", "Add"), ("Replacement", "True"), ("ResourceType", "AWS::S3::Bucket")):
            payload = changes(STACK)
            payload["Changes"][0]["ResourceChange"][field] = value
            with self.assertRaises(ValueError):
                validate_changes(payload, STACK)
        payload = changes(STACK)
        payload["Changes"][0]["ResourceChange"]["Details"][0]["Target"]["Name"] = "TaskDefinition"
        with self.assertRaises(ValueError):
            validate_changes(payload, STACK)

    def test_duplicate_extra_missing_or_unready_changes_fail(self):
        for mutation in (lambda p: p["Changes"].append(copy.deepcopy(p["Changes"][0])), lambda p: p["Changes"].pop(), lambda p: p.update(Status="FAILED"), lambda p: p.update(ExecutionStatus="EXECUTE_COMPLETE")):
            payload = changes(STACK)
            mutation(payload)
            with self.assertRaises(ValueError):
                validate_changes(payload, STACK)

    def test_exact_template_sleep_delta(self):
        for stack, path in TEMPLATES.items():
            new = template_load((ROOT / path).read_text(encoding="utf-8"))
            old = active_template(new, stack)
            validate_template_delta(old, new, stack)
            old["Description"] += " drift"
            with self.assertRaises(ValueError):
                validate_template_delta(old, new, stack)

    def test_legacy_preview_template_is_not_replaced_by_new_shared_template(self):
        source = template_load((ROOT / TEMPLATES[RECONCILIATION]).read_text(encoding="utf-8"))
        old = active_template(source, RECONCILIATION)
        old.pop("Conditions")
        old["Description"] = "legacy immutable Preview-only description"
        old["Resources"]["ReconciliationFailureAlarm"]["Properties"]["AlarmDescription"] = "legacy"
        new = build_template(old, source, RECONCILIATION)
        validate_template_delta(old, new, RECONCILIATION)
        self.assertEqual(new["Description"], old["Description"])
        self.assertEqual(new["Resources"]["ReconciliationFailureAlarm"], old["Resources"]["ReconciliationFailureAlarm"])

    def test_yaml_getatt_and_all_unrelated_text_are_preserved(self):
        source = template_load((ROOT / TEMPLATES[STACK]).read_text(encoding="utf-8"))
        old = """Parameters:
  Environment:
    Type: String
Conditions:
  Existing: !Equals [!Ref Environment, preview]
Resources:
  CollectorService:
    Type: AWS::ECS::Service
    Properties:
      DesiredCount: 1
      TaskDefinition: !GetAtt ExactOriginal.Arn
  CollectorHealthyHostAlarm:
    Type: AWS::CloudWatch::Alarm
    Properties:
      Threshold: 1
      TreatMissingData: breaching
      Untouched: !GetAtt [OriginalList, Arn]
"""
        result = build_template_body(old, source, STACK)
        self.assertIn("      TaskDefinition: !GetAtt ExactOriginal.Arn\n", result)
        self.assertIn("      Untouched: !GetAtt [OriginalList, Arn]\n", result)
        self.assertEqual(result.count("!GetAtt"), old.count("!GetAtt"))
        validate_template_delta(template_load(old), template_load(result), STACK)

    def test_missing_conditions_and_tagged_schedule_value_preserved(self):
        source = template_load((ROOT / TEMPLATES[RECONCILIATION]).read_text(encoding="utf-8"))
        old = """Parameters:
  Environment:
    Type: String
Resources:
  ReconciliationSchedule:
    Type: AWS::Scheduler::Schedule
    Properties:
      State: !Ref ScheduleState
      Arn: !GetAtt Original.Arn
  ReconciliationMissingSuccessAlarm:
    Type: AWS::CloudWatch::Alarm
    Properties:
      Threshold: 1
      TreatMissingData: breaching
Outputs:
  ScheduleState:
    Value: !Ref ScheduleState
"""
        result = build_template_body(old, source, RECONCILIATION)
        self.assertIn("      Arn: !GetAtt Original.Arn\n", result)
        self.assertIn("State: !If [IsPreviewSuspended, DISABLED, !Ref ScheduleState]", result)
        validate_template_delta(template_load(old), template_load(result), RECONCILIATION)

    def test_already_migrated_template_is_not_overwritten(self):
        source = template_load((ROOT / TEMPLATES[STACK]).read_text(encoding="utf-8"))
        with self.assertRaises(ValueError):
            build_template(source, source, STACK)

    def test_production_cannot_enter_suspension_condition(self):
        for path in TEMPLATES.values():
            new = template_load((ROOT / path).read_text(encoding="utf-8"))
            self.assertEqual(new["Conditions"]["IsPreviewSuspended"]["Fn::And"][0], {"Fn::Equals": [{"Ref": "Environment"}, "preview"]})
            self.assertEqual(new["Rules"]["PreviewOnlySuspension"]["Assertions"][0]["Assert"], {"Fn::Equals": [{"Ref": "Environment"}, "preview"]})

    def test_data_retention_and_alb_remain_unconditional(self):
        new = template_load((ROOT / TEMPLATES[STACK]).read_text(encoding="utf-8"))
        self.assertEqual(new["Resources"]["ExperimentDataBucket"]["DeletionPolicy"], "Retain")
        for logical in ("ExperimentDataBucket", "CollectorLoadBalancer", "ExperimentCatalogDatabase", "GrowthBookAthenaWorkGroup", "GrowthBookReadOnlyPolicy", "CollectorPostRoute"):
            self.assertNotEqual(new["Resources"][logical].get("Condition"), "IsPreviewSuspended")
        self.assertEqual(new["Resources"]["CollectorTaskDefinition"]["Properties"]["Cpu"], "256")

    def test_ordinary_preview_deploy_is_blocked_but_production_passes(self):
        for environment, expected in (("preview", 1), ("production", 0)):
            run = subprocess.run([sys.executable, "scripts/growthbook_preview_lifecycle_gate.py", "--environment", environment], cwd=ROOT, capture_output=True, text=True)
            self.assertEqual(run.returncode, expected, run.stdout + run.stderr)

    def test_existing_deploy_guards_precede_aws(self):
        for name in ("deploy-vevo-growthbook-preview.yml", "deploy-vevo-growthbook-reconciliation.yml", "verify-vevo-growthbook-preview.yml"):
            source = (ROOT / ".github/workflows" / name).read_text(encoding="utf-8")
            self.assertLess(source.index("scripts/growthbook_preview_lifecycle_gate.py"), source.index("aws-actions/configure-aws-credentials"))

    def test_workflow_is_confirmed_main_only_with_no_schedule(self):
        source = (ROOT / ".github/workflows/suspend-vevo-growthbook-preview.yml").read_text(encoding="utf-8")
        self.assertIn("github.ref == 'refs/heads/main' && inputs.confirm_suspend == true", source)
        self.assertNotIn("schedule:", source)
        self.assertEqual(source.count("actions/upload-artifact@"), 1)
        self.assertLess(source.index("suspend_growthbook_preview.py --validate"), source.index("aws-actions/configure-aws-credentials"))
        self.assertIn("PyYAML==6.0.3 cfn-lint==1.55.1", source)

    def test_no_data_queries_build_or_direct_service_mutations(self):
        source = (ROOT / "scripts/suspend_growthbook_preview.py").read_text(encoding="utf-8")
        for forbidden in (".update_service(", ".delete_stack(", ".delete_object(", ".get_object(", ".start_query_execution(", ".register_task_definition("):
            self.assertNotIn(forbidden, source)
        self.assertLess(source.index('phase="before"'), source.index("cf.create_change_set("))
        self.assertLess(source.index("validate_changes(cf.describe_change_set"), source.index("cf.execute_change_set("))
        self.assertGreater(source.index('phase="after"'), source.index("cf.execute_change_set("))


if __name__ == "__main__":
    unittest.main()
