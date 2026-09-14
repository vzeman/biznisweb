import copy
import unittest

from scripts.reconcile_vevo_report_alarms import corrected_config


class AlarmBindingTests(unittest.TestCase):
    def setUp(self):
        self.alarm = {"AlarmName": "vevo-reporting-run-failed", "MetricName": "ReportRunFailed",
                      "Namespace": "VevoReporting", "Dimensions": [{"Name": "Project", "Value": "vevo"}],
                      "ActionsEnabled": True, "AlarmActions": ["existing-route"], "OKActions": ["existing-ok-route"],
                      "TreatMissingData": "notBreaching", "Threshold": 1.0, "Period": 3600,
                      "EvaluationPeriods": 1, "ComparisonOperator": "GreaterThanOrEqualToThreshold",
                      "Statistic": "Sum", "StateValue": "OK"}
        self.fields = set(self.alarm) - {"StateValue"}

    def test_only_namespace_changes_and_provider_snapshot_is_not_mutated(self):
        snapshot = copy.deepcopy(self.alarm)
        before, after = corrected_config(self.alarm, self.fields, "BizniswebReporting")
        self.assertEqual({**before, "Namespace": "BizniswebReporting"}, after)
        self.assertEqual(snapshot, self.alarm)
        self.assertEqual(before, {**after, "Namespace": "VevoReporting"})

    def test_correct_binding_is_idempotent(self):
        self.alarm["Namespace"] = "BizniswebReporting"
        before, after = corrected_config(self.alarm, self.fields, "BizniswebReporting")
        self.assertEqual(before, after)

    def test_foreign_or_unexpected_metric_policy_cannot_be_rebound(self):
        for key, value in [("AlarmName", "roy-reporting-run-failed"), ("MetricName", "UnrelatedMetric"),
                           ("Namespace", "OtherSystem"), ("ActionsEnabled", False), ("AlarmActions", []),
                           ("Dimensions", [{"Name": "Project", "Value": "roy"}])]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                corrected_config({**self.alarm, key: value}, self.fields, "BizniswebReporting")
