"""One proven pre-probe lease return, never stale-ETag or recursive admission."""
import copy
import json
import unittest
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError

from scripts import reporting_current_rerun_recovery as recovery
from scripts import vevo_report_image_release as release
from tests import test_vevo_report_current_rerun_recovery as prior

FAILED = "a" * 32
FAILED_SOURCE = "b" * 40
NEW_IMAGE = prior.IMAGE.rsplit("@", 1)[0] + "@sha256:" + "9" * 64


class PreprobeReturnTests(unittest.TestCase):
    def setUp(self):
        self.old = prior.AdmissionTests()
        self.old.setUp()
        self.controller, self.blobs = self.old.controller()
        self.value = self.old.chain()
        self.value.update(preflight_sha256=release.sha(next(iter(self.old.raws.values()))),
                          original_readback_key=self.controller.recovery_readback_key,
                          original_readback_sha256=self.controller.recovery_readback_sha256)
        with patch.object(recovery, "OBSERVER_SHA256", release.sha(prior.OBSERVER)):
            self.value.update(recovery.validate_archive(self.blobs[self.controller.recovery_readback_key],
                self.controller.recovery_readback_sha256, self.value, self.old.raws,
                {key: release.sha(raw) for key, raw in self.old.raws.items()}))
        original = {"kind": "current-rerun", "release_id": prior.OWNER,
            "receipt_key": self.value["receipt_key"], "receipt_sha256": self.value["receipt_sha256"],
            "preflight_sha256": self.value["preflight_sha256"], "readback_key": self.value["original_readback_key"],
            "readback_sha256": self.value["original_readback_sha256"], "lease_etag": self.value["lease_etag"],
            "terminal_evidence_kind": "fresh-ecs-plus-archived-observer-projection",
            "terminal_observation_sha256": self.value["terminal_observation_sha256"],
            "observer_script_sha256": self.value["observer_script_sha256"],
            "archive_manifest_sha256": self.value["archive_manifest_sha256"]}
        self.candidate = release.image_only_definition(self.old.definition, NEW_IMAGE)
        self.candidate.update(taskDefinitionArn=prior.DEFINITION.rsplit(":", 1)[0] + ":99", status="INACTIVE")
        common = {"schema_version": 1, "project": "vevo", "release_id": FAILED, "source_commit": FAILED_SOURCE,
                  "report_to_date": prior.DAY, "known_task_arns": {}, "promoted": False,
                  "rerun_attempted": False, "legacy_current_binding_changed": False}
        self.records = [{**copy.deepcopy(common), "phase": phase, "at": f"2026-10-22T11:00:0{index}+00:00"}
                        for index, phase in enumerate(recovery.PREPROBE_PHASES)]
        before = self.records[0]
        before.update(schedule=copy.deepcopy(self.old.paused), source_definition=copy.deepcopy(self.old.definition),
                      image=NEW_IMAGE, original_live_outputs=copy.deepcopy(self.old.records[0]["original_live_outputs"]),
                      protected_schedules=copy.deepcopy(self.old.records[0]["protected_schedules"]),
                      protected_peer=copy.deepcopy(self.old.records[0]["protected_peer"]), recovered_from=original)
        self.records[1].update(previous_owner=prior.OWNER, previous_lease_etag=self.value["lease_etag"],
                               recovery_receipt_sha256=self.value["receipt_sha256"])
        for index in (2, 3, 9):
            self.records[index]["schedule"] = copy.deepcopy(self.old.paused)
        self.records[4]["definition"] = release.image_only_definition(self.old.definition, NEW_IMAGE)
        self.records[5]["task_definition"] = self.candidate["taskDefinitionArn"]
        self.records[6]["role_arn"] = f"arn:aws:iam::{release.ACCOUNT}:role/{release.VEVO.role_prefix}{FAILED}"
        self.records[7]["start_uncertain"] = False
        self.records[8]["previous_owner"] = prior.OWNER
        self.records[9].update(paused_recovery_owner=prior.OWNER, all_owned_tasks_stopped=True)
        self.lease = {"schema_version": 1, "owner": prior.OWNER, "state": "uncertain", "generation": "7" * 32,
                      "updated_at": "2026-10-22T11:00:08.500000+00:00", "expires_at": "2026-10-22T11:20:08.500000+00:00"}
        self.document = {"schema_version": 1, "kind": "current-rerun-preprobe-return", "project": "vevo",
            "failed_release_id": FAILED, "source_commit": FAILED_SOURCE, "report_to_date": prior.DAY,
            "checked_at": "2026-10-22T12:05:00+00:00", "original_recovery": copy.deepcopy(original),
            "returned_lease": {"value": copy.deepcopy(self.lease), "etag": '"returned-etag"'},
            "schedule": copy.deepcopy(self.old.paused), "source_definition": copy.deepcopy(self.old.definition),
            "candidate_definition": copy.deepcopy(self.candidate), "live_outputs": copy.deepcopy(before["original_live_outputs"]),
            "protected_schedules": copy.deepcopy(before["protected_schedules"]), "protected_peer": copy.deepcopy(before["protected_peer"]),
            "candidate_probe_role_absent": True, "owned_tasks": [], "active_reporting_tasks": []}
        self.sync_journal()
        self.controller.iam = Mock()
        self.controller.iam.get_role.side_effect = ClientError({"Error": {"Code": "NoSuchEntity"}}, "GetRole")
        self.controller.tasks = Mock(return_value=[])
        self.controller.definition.side_effect = lambda arn: self.candidate if arn == self.candidate["taskDefinitionArn"] else self.old.definition
        self.controller.s3.list_objects_v2.side_effect = self.list_objects

    def sync_journal(self):
        self.raws = {release.VEVO.release_prefix + FAILED + f"/{index:04d}-{row['phase']}.json": release.canonical(row)
                     for index, row in enumerate(self.records, 1)}
        self.document["receipts"] = [{"key": key, "sha256": release.sha(raw)} for key, raw in self.raws.items()]
        self.blobs.update(self.raws)

    def bind_document(self):
        raw = release.canonical(self.document)
        digest = release.sha(raw)
        self.controller.recovery_preprobe_return_key = "data/vevo/reporting/recovery-handoffs/" + FAILED + "/" + digest + "/preprobe-return.json"
        self.controller.recovery_preprobe_return_sha256 = digest
        self.blobs[self.controller.recovery_preprobe_return_key] = raw

    def list_objects(self, **kwargs):
        prefix = kwargs["Prefix"]
        if prefix == prior.PREFIX:
            return {"Contents": [{"Key": key} for key in self.old.raws]}
        if prefix == release.VEVO.release_prefix + FAILED + "/":
            return {"Contents": [{"Key": key} for key in self.raws]}
        if prefix == release.VEVO.probe_prefix + FAILED + "/":
            return {"Contents": []}
        raise AssertionError("unexpected prefix")

    def inspect(self, *, bind=True):
        if bind:
            self.bind_document()
        with patch.object(recovery, "OBSERVER_SHA256", release.sha(prior.OBSERVER)), \
             patch.object(recovery, "read_private", side_effect=lambda _s3, key, _limit: self.blobs[key]), \
             patch.object(release, "committed_gate_sha", return_value=prior.GATE), \
             patch.object(recovery, "read_lease", return_value=(self.lease, '"returned-etag"')):
            return self.controller.inspect_paused_recovery()

    def test_complete_original_archive_plus_single_return_binds_both_lease_generations(self):
        proof = self.inspect()
        self.assertEqual('"returned-etag"', proof["lease_etag"])
        returned = proof["preprobe_return"]
        self.assertEqual('"old-etag"', returned["original_lease_etag"])
        self.assertEqual(proof["lease_etag"], returned["returned_lease_etag"])
        self.assertEqual(self.controller.recovery_preprobe_return_key, returned["key"])
        self.assertEqual(self.document["receipts"][-1]["sha256"], returned["terminal_receipt_sha256"])
        self.assertEqual(release.sha(release.canonical(self.lease)), returned["returned_lease_sha256"])
        self.controller.s3.put_object.assert_not_called()
        self.controller.ecs.run_task.assert_not_called()
        self.controller.iam.get_role.assert_called_once_with(RoleName=release.VEVO.role_prefix + FAILED)

    def test_old_archive_alone_still_rejects_changed_etag(self):
        with self.assertRaisesRegex(RuntimeError, "current-recovery-lease-drift"):
            self.inspect(bind=False)
        self.controller.s3.put_object.assert_not_called()

    def test_exact_ten_receipts_no_dispatch_and_one_handoff_only(self):
        for change in ("missing", "extra", "dispatch", "task", "promoted", "rerun", "uncertain", "old-etag", "owner",
                       "return-owner", "final-owner", "source", "project", "date", "time", "recursive"):
            self.setUp()
            if change == "missing": self.records.pop(4)
            elif change == "extra": self.records.append(copy.deepcopy(self.records[-1]))
            elif change == "dispatch": self.records[6]["phase"] = "probe-dispatch-requested"
            elif change == "task": self.records[7]["known_task_arns"] = {"probe": "unexpected"}
            elif change in {"promoted", "rerun", "uncertain"}:
                self.records[7][{"promoted": "promoted", "rerun": "rerun_attempted", "uncertain": "start_uncertain"}[change]] = True
            elif change == "old-etag": self.records[1]["previous_lease_etag"] = '"different"'
            elif change == "owner": self.records[1]["previous_owner"] = FAILED
            elif change == "return-owner": self.records[8]["previous_owner"] = FAILED
            elif change == "final-owner": self.records[9]["paused_recovery_owner"] = FAILED
            elif change == "source": self.records[7]["source_commit"] = "f" * 40
            elif change == "project": self.records[7]["project"] = "roy"
            elif change == "date": self.records[7]["report_to_date"] = "2026-10-20"
            elif change == "time": self.records[7]["at"] = "2026-10-22T06:00:00Z"
            else:
                self.records[0]["recovered_from"]["preprobe_return"] = {"previous": "earlier retry"}
                self.document["original_recovery"] = copy.deepcopy(self.records[0]["recovered_from"])
            self.sync_journal()
            with self.subTest(change=change), self.assertRaises((RuntimeError, release.binding.BindingError)):
                self.inspect()
            self.controller.s3.put_object.assert_not_called()

    def test_handoff_cannot_change_original_boundaries_or_lease_generation(self):
        for change in ("old-receipt", "old-archive", "old-source", "schedule", "outputs", "peer", "protected", "candidate-active",
                       "candidate-image", "candidate-role", "role-present", "task-present", "etag-old", "lease-owner", "lease-state",
                       "lease-before-return", "lease-after-final", "observed-before-final", "missing-hash"):
            self.setUp()
            if change == "old-receipt": self.document["original_recovery"]["receipt_sha256"] = "0" * 64
            elif change == "old-archive": self.document["original_recovery"]["readback_sha256"] = "0" * 64
            elif change == "old-source": self.document["source_definition"]["taskDefinitionArn"] = "foreign"
            elif change == "schedule": self.document["schedule"]["State"] = "ENABLED"
            elif change == "outputs": self.document["live_outputs"] = {}
            elif change == "peer": self.document["protected_peer"] = {}
            elif change == "protected": self.document["protected_schedules"] = {}
            elif change == "candidate-active": self.document["candidate_definition"]["status"] = "ACTIVE"
            elif change == "candidate-image": self.document["candidate_definition"]["containerDefinitions"][0]["image"] = prior.IMAGE
            elif change == "candidate-role": self.document["candidate_definition"]["taskRoleArn"] = "foreign"
            elif change == "role-present": self.document["candidate_probe_role_absent"] = False
            elif change == "task-present": self.document["owned_tasks"] = ["unexpected"]
            elif change == "etag-old": self.document["returned_lease"]["etag"] = '"old-etag"'
            elif change == "lease-owner": self.document["returned_lease"]["value"]["owner"] = FAILED
            elif change == "lease-state": self.document["returned_lease"]["value"]["state"] = "active"
            elif change == "lease-before-return": self.document["returned_lease"]["value"]["updated_at"] = "2026-10-22T10:00:00Z"
            elif change == "lease-after-final": self.document["returned_lease"]["value"]["updated_at"] = "2026-10-22T11:00:10Z"
            elif change == "observed-before-final": self.document["checked_at"] = "2026-10-22T10:00:00Z"
            else: self.document["receipts"][0]["sha256"] = "0" * 64
            with self.subTest(change=change), self.assertRaises((RuntimeError, release.binding.BindingError)):
                self.inspect()
            self.controller.s3.put_object.assert_not_called()

    def test_fresh_candidate_role_tasks_objects_and_full_lease_must_agree(self):
        for change in ("candidate", "role-present", "role-denied", "task", "probe-object", "lease-value", "etag"):
            self.setUp()
            if change == "candidate": self.candidate["status"] = "ACTIVE"
            elif change == "role-present": self.controller.iam.get_role.side_effect = None
            elif change == "role-denied": self.controller.iam.get_role.side_effect = ClientError({"Error": {"Code": "AccessDenied"}}, "GetRole")
            elif change == "task": self.controller.tasks.return_value = [{"startedBy": FAILED, "lastStatus": "STOPPED"}]
            elif change == "probe-object":
                original = self.list_objects
                self.controller.s3.list_objects_v2.side_effect = lambda **kwargs: ({"Contents": [{"Key": "unexpected"}]}
                    if kwargs["Prefix"].startswith(release.VEVO.probe_prefix) else original(**kwargs))
            elif change == "lease-value": self.lease["generation"] = "8" * 32
            else: self.document["returned_lease"]["etag"] = '"not-the-current-etag"'
            with self.subTest(change=change), self.assertRaises((RuntimeError, release.binding.BindingError)):
                self.inspect()
            self.controller.s3.put_object.assert_not_called()

    def test_hash_scope_and_journal_change_before_second_boundary_read_reject(self):
        self.bind_document()
        self.blobs[self.controller.recovery_preprobe_return_key] += b" "
        with self.assertRaisesRegex(RuntimeError, "readback-hash"):
            self.inspect(bind=False)
        self.setUp()
        self.inspect()
        last = next(reversed(self.raws))
        self.blobs[last] += b" "
        with patch.object(recovery, "read_private", side_effect=lambda _s3, key, _limit: self.blobs[key]), \
             self.assertRaisesRegex(RuntimeError, "journal-drift"):
            recovery.verify_preprobe_return_boundary(self.controller)
        self.controller.s3.put_object.assert_not_called()

    def test_real_constructor_rejects_incomplete_pairs_and_other_recovery_modes_before_clients(self):
        base = {"recover_current_rerun": prior.OWNER, "recovery_receipt_sha256": "a" * 64,
                "recovery_readback_key": "private-original-archive", "recovery_readback_sha256": "b" * 64,
                "recovery_preprobe_return_key": "private-return", "recovery_preprobe_return_sha256": "c" * 64}
        changes = ({"recovery_preprobe_return_key": None}, {"recovery_preprobe_return_sha256": None},
                   {"recover_current_rerun": None}, {"recover_current_rerun": None, "recover_paused_release": prior.OWNER},
                   {"recover_paused_release": prior.OWNER}, {"project": "roy"})
        for changed in changes:
            session = Mock()
            with self.subTest(changed=changed), self.assertRaisesRegex(RuntimeError, "arguments"):
                release.ImageRelease(session, prior.HEAD, prior.DAY, 7200, **{**base, **changed})
            session.client.assert_not_called()
        instance = release.ImageRelease(Mock(), prior.HEAD, prior.DAY, 7200, **base)
        self.assertEqual(base["recovery_preprobe_return_key"], instance.recovery_preprobe_return_key)
        self.assertEqual(base["recovery_preprobe_return_sha256"], instance.recovery_preprobe_return_sha256)

    def test_cli_passes_the_pair_and_rejects_missing_key_or_digest(self):
        common = ["release", "run", "--commit", prior.HEAD, "--to-date", prior.DAY,
                  "--recover-current-rerun", prior.OWNER, "--recovery-receipt-sha256", "a" * 64,
                  "--recovery-readback-key", "private-archive", "--recovery-readback-sha256", "b" * 64]
        pair = ["--recovery-preprobe-return-key", "private-return", "--recovery-preprobe-return-sha256", "c" * 64]
        for suffix in (pair, pair[:2], pair[2:]):
            with self.subTest(pair=suffix), patch.object(release.sys, "argv", common + suffix), \
                 patch("boto3.Session", return_value=Mock()), patch.object(release, "ImageRelease") as controller:
                if suffix == pair:
                    release.main()
                    self.assertEqual("private-return", controller.call_args.kwargs["recovery_preprobe_return_key"])
                    self.assertEqual("c" * 64, controller.call_args.kwargs["recovery_preprobe_return_sha256"])
                    controller.return_value.run.assert_called_once_with()
                else:
                    with self.assertRaisesRegex(RuntimeError, "preprobe-return-arguments"):
                        release.main()
                    controller.assert_not_called()


if __name__ == "__main__":
    unittest.main()
