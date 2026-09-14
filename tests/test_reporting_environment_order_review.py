"""Independent regression boundaries for AWS environment-array ordering."""
from copy import deepcopy
import unittest
from unittest.mock import Mock, patch

from scripts import deploy_vevo_report as deploy
from scripts import reporting_readiness as readiness
from scripts import reporting_runtime_binding as binding
from tests import test_reporting_runtime_binding as authority_fixtures
from tests import test_vevo_report_deployment as deploy_fixtures


def definition():
    result = deploy_fixtures.definition()
    result["taskDefinitionArn"] = f"arn:aws:ecs:{binding.REGION}:{binding.ACCOUNT}:task-definition/vevo-reporting-daily:123"
    result["containerDefinitions"][0]["image"] = deploy_fixtures.IMAGE
    return result


class EnvironmentSnapshotReviewTests(unittest.TestCase):
    def test_unique_environment_reorder_is_equal_without_mutating_input(self):
        source = definition()
        source["containerDefinitions"][0]["environment"].reverse()
        original = deepcopy(source)
        result = binding.definition_snapshot(source)
        self.assertEqual(binding.definition_snapshot(definition()), result)
        self.assertEqual(original, source)
        self.assertEqual(result, binding.definition_snapshot(result))
        result["containerDefinitions"][0]["environment"][0]["value"] = "changed-copy"
        self.assertEqual(original, source)

    def test_absent_environment_and_explicit_empty_are_distinct(self):
        absent = definition()
        absent["containerDefinitions"][0].pop("environment")
        empty = deepcopy(absent)
        empty["containerDefinitions"][0]["environment"] = []
        self.assertNotEqual(binding.definition_snapshot(absent), binding.definition_snapshot(empty))
        self.assertNotIn("environment", binding.definition_snapshot(absent)["containerDefinitions"][0])

    def test_invalid_environment_cannot_be_sorted_into_authority(self):
        valid = {"name": "ONE", "value": "value"}
        invalid = [None, {}, "value", [None], [{}], [dict(valid), dict(valid)],
                   [dict(valid), {"name": "ONE", "value": "different"}],
                   [{"name": "", "value": "value"}], [{"name": 1, "value": "value"}],
                   [{"name": "ONE", "value": False}], [{"name": "ONE"}],
                   [{"value": "value"}], [{**valid, "extra": "unsupported"}]]
        for rows in invalid:
            source = definition()
            source["containerDefinitions"][0]["environment"] = rows
            with self.subTest(rows=rows), self.assertRaises(binding.BindingError):
                binding.definition_snapshot(source)

    def test_container_shape_is_validated_without_silently_dropping_rows(self):
        for rows in (None, {}, "containers", [None], ["reporting"]):
            source = definition()
            source["containerDefinitions"] = rows
            with self.subTest(rows=rows), self.assertRaises(binding.BindingError):
                binding.definition_snapshot(source)

    def test_other_arrays_retain_order_and_exact_values(self):
        source = definition()
        container = source["containerDefinitions"][0]
        container["command"] = ["python", "daily_report_runner.py"]
        container["secrets"].append({"name": "SECOND", "valueFrom": "second-reference"})
        container["mountPoints"] = [{"sourceVolume": "one", "containerPath": "/one"},
                                    {"sourceVolume": "two", "containerPath": "/two"}]
        source["volumes"] = [{"name": "one"}, {"name": "two"}]
        source["containerDefinitions"].append({"name": "secondary", "image": "separate"})
        baseline = binding.definition_snapshot(source)
        for key in ("command", "secrets", "mountPoints", "volumes", "containerDefinitions"):
            changed = deepcopy(source)
            rows = changed[key] if key in ("volumes", "containerDefinitions") else changed["containerDefinitions"][0][key]
            rows.reverse()
            with self.subTest(key=key):
                self.assertNotEqual(baseline, binding.definition_snapshot(changed))


class EnvironmentAuthorityReviewTests(unittest.TestCase):
    def test_real_runtime_reader_accepts_only_environment_order_drift(self):
        record = authority_fixtures.promotion(authority_fixtures.loaded(authority_fixtures.baseline()))
        loaded = authority_fixtures.loaded(record)
        current = deepcopy(record["task_definition"])
        current["containerDefinitions"][0]["environment"].reverse()
        binding.validate_runtime(loaded, record["schedule"], current)
        for kind in ("missing", "value", "duplicate", "command", "secrets", "memory"):
            changed = deepcopy(current)
            container = changed["containerDefinitions"][0]
            if kind == "missing":
                container["environment"].pop()
            elif kind == "value":
                container["environment"][0]["value"] = "changed"
            elif kind == "duplicate":
                container["environment"].append(deepcopy(container["environment"][0]))
            elif kind == "command":
                container["command"] = ["daily_report_runner.py", "python"]
            elif kind == "secrets":
                container["secrets"].reverse()
            else:
                changed["memory"] = "8192"
            with self.subTest(kind=kind), self.assertRaises(binding.BindingError):
                binding.validate_runtime(loaded, record["schedule"], changed)

    def test_readiness_accepts_reorder_but_rejects_every_other_requested_change(self):
        actual = definition()
        arn = actual["taskDefinitionArn"]
        protected = {"definitions": {arn: binding.definition_snapshot(actual)}}
        requested = deepcopy(actual)
        requested.pop("taskDefinitionArn")
        requested["containerDefinitions"][0]["environment"].reverse()
        self.assertEqual(deploy_fixtures.IMAGE.rsplit("@", 1)[1], readiness.verify_definition(protected, arn, requested))
        for kind in ("missing", "value", "duplicate", "extra-field", "unexpected-arn", "command", "role"):
            changed = deepcopy(requested)
            container = changed["containerDefinitions"][0]
            if kind == "missing":
                container["environment"].pop()
            elif kind == "value":
                container["environment"][0]["value"] = "changed"
            elif kind == "duplicate":
                container["environment"].append(deepcopy(container["environment"][0]))
            elif kind == "extra-field":
                changed["unexpected"] = "not-an-AWS-request-field"
            elif kind == "unexpected-arn":
                changed["taskDefinitionArn"] = arn
            elif kind == "command":
                container["command"] = ["python", "generate_invoices.py"]
            else:
                changed["taskRoleArn"] = "foreign-role"
            with self.subTest(kind=kind), self.assertRaises(binding.BindingError):
                readiness.verify_definition(protected, arn, changed)


class EnvironmentRegistrationReviewTests(unittest.TestCase):
    def transaction(self, mutation=None):
        result = deploy_fixtures.DeploymentTests().managed_transaction()
        obj = result[0]
        obj.binding.definition_snapshot = binding.definition_snapshot
        register = obj.ecs.register_task_definition
        def response(**request):
            response = register(**request)
            container = response["taskDefinition"]["containerDefinitions"][0]
            container["environment"].reverse()
            if mutation == "missing":
                container["environment"].pop()
            elif mutation == "value":
                container["environment"][0]["value"] = "changed"
            elif mutation == "duplicate":
                container["environment"].append(deepcopy(container["environment"][0]))
            elif mutation == "command":
                container["command"] = ["daily_report_runner.py", "python"]
            elif mutation == "memory":
                response["taskDefinition"]["memory"] = "8192"
            return response
        obj.ecs.register_task_definition = Mock(side_effect=response)
        return result

    def test_real_transaction_accepts_registered_environment_reordering(self):
        obj, state, original, pointer, writes, phases = self.transaction()
        with patch.object(deploy, "current_main"), patch.dict(deploy.os.environ, {"GITHUB_RUN_ID": "new-run"}):
            obj.run()
        self.assertEqual(["DISABLED", "DISABLED", "ENABLED"], [row["State"] for row in writes])
        self.assertNotEqual(original["Target"], state["Target"])
        self.assertEqual(obj.release_id, pointer["record"]["release_id"])
        self.assertIn("promotion-readback-verified", phases)
        obj.probe.assert_called_once()
        self.assertEqual(2, obj.ecs.register_task_definition.call_count)

    def test_real_transaction_rejects_non_order_drift_before_probe_or_promotion(self):
        for kind in ("missing", "value", "duplicate", "command", "memory"):
            obj, state, original, pointer, writes, _ = self.transaction(kind)
            with self.subTest(kind=kind), patch.object(deploy, "current_main"), \
                 patch.dict(deploy.os.environ, {"GITHUB_RUN_ID": "new-run"}), \
                 self.assertRaises((RuntimeError, binding.BindingError)):
                obj.run()
            self.assertEqual(original, state)
            self.assertEqual("old", pointer["record_sha256"])
            self.assertTrue(all(row["Target"] == original["Target"] for row in writes))
            obj.probe.assert_not_called()
            self.assertEqual(2, obj.ecs.register_task_definition.call_count)
