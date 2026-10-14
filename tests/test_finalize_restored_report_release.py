import copy
from datetime import datetime, timedelta, timezone
import io
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError
from scripts import finalize_restored_report_release as final
from tests.test_vevo_report_image_release import definition, IMAGE


NOW = datetime(2026, 10, 14, 8, tzinfo=timezone.utc)
RID, SOURCE, CONTROL, GATE = "a" * 32, "b" * 40, "c" * 40, "d" * 64


def error(code):
    return ClientError({"Error": {"Code": code}}, "Synthetic")


def fixture(project="vevo"):
    policy = final.project_policy(project)
    source = definition()
    source["family"] = policy.family
    source["taskRoleArn"] = f"arn:aws:iam::{final.ACCOUNT}:role/BiznisWebReportingTaskRole-{project}"
    env = source["containerDefinitions"][0]["environment"]
    env.append({"name": "REPORT_FROM_DATE", "value": "2025-05-03"})
    for row in env:
        if row["name"] == "REPORT_PROJECT":
            row["value"] = project
        if row["name"] == "REPORT_S3_PREFIX":
            row["value"] = policy.sink
    arn = f"arn:aws:ecs:{final.REGION}:{final.ACCOUNT}:task-definition/{policy.family}:"
    source.update(taskDefinitionArn=arn + "1", status="ACTIVE")
    original = {"Name": policy.service, "State": "ENABLED", "Target": {"Arn": final.CLUSTER,
        "EcsParameters": {"TaskDefinitionArn": arn + "1", "LaunchType": "FARGATE"}}}
    peer = final.PROJECTS["roy" if project == "vevo" else "vevo"]
    schedules = {policy.service: original, peer.service: {"Name": peer.service, "State": "ENABLED"}}
    outputs = {policy.sink + "/latest/generation.json": {"ETag": "original"}}
    before = {"schedule": original, "source_definition": source, "image": IMAGE,
              "original_live_outputs": outputs, "protected_schedules": {peer.service: schedules[peer.service]},
              "recovered_from": None, "independent_contract_sha256": "7" * 64}
    rows = [{"schema_version": 1, "phase": phase, "project": project, "release_id": RID,
             "source_commit": SOURCE, "report_to_date": "2026-10-12", "at": (NOW + timedelta(seconds=i)).isoformat(),
             "promoted": False, "rerun_attempted": False, "legacy_current_binding_changed": False,
             "known_task_arns": {}} for i, phase in enumerate(final.PHASES)]
    rows[0].update(before)
    rows[1]["lease_key"] = policy.lease_key
    paused = copy.deepcopy(original)
    paused["State"] = "DISABLED"
    rows[2]["schedule"] = rows[3]["schedule"] = paused
    rows[4]["definition"] = final.image_only_definition(source, IMAGE, policy=policy)
    rows[5]["task_definition"] = arn + "2"
    overrides = final.task_overrides(RID, SOURCE, IMAGE, GATE, "2026-10-12", probe=True, policy=policy)
    rows[6]["role_arn"] = overrides["taskRoleArn"]
    rows[7].update(overrides=overrides, task_definition=arn + "2", client_token=RID + "-probe")
    task_arn = final.CLUSTER.replace(":cluster/", ":task/") + "/" + "e" * 32
    rows[8]["task_arn"] = task_arn
    identity = {"project": project, "release_id": RID, "source_commit": SOURCE, "image_digest": IMAGE.rsplit("@", 1)[1],
        "task_arn": task_arn, "task_definition": arn + "2", "instance_id": "N/A:Fargate", "service": policy.service,
        "path": "/app", "marker": policy.probe_marker, "gate_sha256": GATE, "private_ip": "172.31.1.2"}
    rows[9]["identity"] = copy.deepcopy(identity)
    prefix = policy.probe_prefix + RID + "/"
    manifest = {"project": project, "synthetic": True}
    complete = dict(identity, phase="report-verified", localhost_marker_sha256=final.sha(final.canonical(identity)),
        provider_writes=False, email_sent=False, live_outputs_changed=False, skip_invoices=True, skip_inline_guard=True,
        output_manifest_key=prefix + "artifacts/output-manifest.json", output_manifest_sha256=final.digest(manifest))
    task = {"taskArn": task_arn, "clusterArn": final.CLUSTER, "launchType": "FARGATE", "startedBy": RID,
        "taskDefinitionArn": arn + "2", "lastStatus": "STOPPED", "desiredStatus": "STOPPED",
        "startedAt": NOW.isoformat(), "stoppedAt": (NOW + timedelta(seconds=1)).isoformat(), "overrides": overrides,
        "containers": [{"name": "reporting", "lastStatus": "STOPPED", "exitCode": 0, "image": IMAGE,
            "imageDigest": identity["image_digest"], "networkInterfaces": [{"privateIpv4Address": identity["private_ip"]}]}]}
    rows[10].update(identity=identity, complete=complete, terminal_task=task)
    rows[11].update(review_key=prefix + "review/independent-source.json", bindings={
        "schema_version": 1, "project": project, "release_id": RID, "source_commit": SOURCE,
        "image_digest": identity["image_digest"], "report_from_date": "2025-05-03", "report_to_date": "2026-10-12",
        "probe_manifest_sha256": complete["output_manifest_sha256"], "source_contract_sha256": "7" * 64,
        "approved": True})
    rows[12]["start_uncertain"] = False
    for row in rows[8:]:
        row["known_task_arns"] = {"probe": task_arn}
    candidate = copy.deepcopy(rows[4]["definition"])
    candidate.update(taskDefinitionArn=arn + "2", status="INACTIVE")
    return policy, rows, schedules, candidate, manifest


def history(rows, policy):
    return final.validate_history(rows, policy=policy, release_id=RID, release_source=SOURCE, gate_sha=GATE)


def lock(state="uncertain", owner=RID):
    return {"schema_version": 1, "owner": owner, "state": state,
            "updated_at": (NOW - timedelta(days=1)).isoformat(),
            "expires_at": (NOW - timedelta(days=1, minutes=-20)).isoformat(), "generation": "f" * 32}


class HistoryTests(unittest.TestCase):
    def test_both_projects_and_raw_schedule_metadata_are_supported(self):
        for project in final.PROJECTS:
            policy, rows, *_ = fixture(project)
            rows[0]["schedule"]["CreationDate"] = "ignored-control-metadata"
            self.assertEqual(history(rows, policy)["identity"]["service"], policy.service)

    def test_waiting_protocol_binds_every_expected_review_field_without_claiming_approval(self):
        policy, rows, *_ = fixture()
        for key in rows[11]["bindings"]:
            bad = copy.deepcopy(rows)
            bad[11]["bindings"][key] = "tampered"
            with self.subTest(field=key), self.assertRaises(RuntimeError):
                history(bad, policy)
        for value in (1, False):
            bad = copy.deepcopy(rows)
            bad[11]["bindings"]["approved"] = value
            with self.subTest(approved=value), self.assertRaises(RuntimeError):
                history(bad, policy)
        rows[0]["independent_contract_sha256"] = "0" * 64
        with self.assertRaisesRegex(RuntimeError, "review-binding"):
            history(rows, policy)

    def test_live_approval_unknown_sequence_or_foreign_binding_rejected(self):
        for mutate in (
            lambda r: r[5].update(project="foreign"), lambda r: r[3].update(source_commit="f" * 40),
            lambda r: r[4].update(promoted=True), lambda r: r[12].update(rerun_attempted=True),
            lambda r: r[12].update(start_uncertain=True), lambda r: r[8].update(dispatch_uncertain=True),
            lambda r: r[11].update(phase="independent-review-verified"),
            lambda r: r[11]["known_task_arns"].update(live="unrelated"),
            lambda r: r[0]["schedule"].update(State="DISABLED"),
            lambda r: r[10]["identity"].update(path="/untrusted"),
            lambda r: r[10]["complete"].update(email_sent=True),
            lambda r: r[10]["complete"].update(localhost_marker_sha256="0" * 64),
            lambda r: r[6].update(role_arn="foreign"),
            lambda r: r[7]["overrides"]["containerOverrides"][0]["command"].append("--unsafe"),
            lambda r: r[1].update(lease_key=final.PROJECTS["roy"].lease_key),
        ):
            policy, rows, *_ = fixture()
            mutate(rows)
            with self.subTest(change=mutate), self.assertRaises((RuntimeError, ValueError)):
                history(rows, policy)

    def test_archived_probe_allowed_only_for_exact_missing_and_zero_exit(self):
        policy, rows, *_ = fixture()
        h, task = history(rows, policy), rows[10]["terminal_task"]
        good = {"tasks": [], "failures": [{"arn": h["task"], "reason": "MISSING"}]}
        self.assertTrue(final.terminal_probe(good, task, h)["archived_because_missing"])
        self.assertFalse(final.terminal_probe({"tasks": [task]}, task, h)["archived_because_missing"])
        for response in ({"tasks": []}, {"tasks": [], "failures": [{"arn": h["task"], "reason": "ACCESS_DENIED"}]},
                         {"tasks": [], "failures": [{"arn": "foreign", "reason": "MISSING"}]},
                         {"tasks": [task], "failures": [{"arn": h["task"], "reason": "MISSING"}]}):
            with self.subTest(response=response), self.assertRaisesRegex(RuntimeError, "current-response"):
                final.terminal_probe(response, task, h)
        for change in (lambda t: t.update(startedBy="foreign"), lambda t: t.update(lastStatus="RUNNING"),
                       lambda t: t["containers"][0].update(exitCode=137),
                       lambda t: t["containers"][0].update(imageDigest="sha256:" + "0" * 64),
                       lambda t: t["overrides"].update(taskRoleArn="foreign"),
                       lambda t: t["containers"][0]["networkInterfaces"][0].update(privateIpv4Address="172.31.1.3")):
            bad = copy.deepcopy(task)
            change(bad)
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                final.terminal_probe(good, bad, h)


class CaptureTests(unittest.TestCase):
    def setUp(self):
        self.policy, self.rows, self.schedules, self.candidate, self.manifest = fixture()
        self.schedules = copy.deepcopy(self.schedules)
        self.args = types.SimpleNamespace(project="vevo", release_id=RID, release_source=SOURCE,
            control_source=CONTROL, failure_sha256=final.digest(self.rows[-1]), lease_sha256=final.digest(lock()), lease_etag='"old"')
        self.obj = object.__new__(final.Inspector)
        self.obj.args, self.obj.policy = self.args, self.policy
        self.obj.session, self.obj.reader, self.obj.s3 = Mock(), Mock(), Mock()
        self.obj.session.region_name = final.REGION
        self.obj.session.client.return_value.get_caller_identity.return_value = {"Account": final.ACCOUNT}
        def app_service(ServiceArn):
            name, service_id = ServiceArn.split("/")[-2:]
            return {"Service": {"ServiceArn": ServiceArn, "ServiceName": name, "ServiceId": service_id,
                                "Status": "RUNNING", "SourceConfiguration": {"synthetic_auth": "never-persist-me"}}}
        self.obj.session.client.return_value.describe_service.side_effect = app_service
        self.reader = self.obj.reader
        self.reader.all_schedules.side_effect = lambda: copy.deepcopy(self.schedules)
        self.reader.definition.side_effect = lambda arn: copy.deepcopy(self.candidate if arn.endswith(":2") else self.rows[0]["source_definition"])
        self.reader.ecs.describe_tasks.return_value = {"tasks": [self.rows[10]["terminal_task"]]}
        self.reader.iam.get_role.side_effect = error("NoSuchEntity")
        self.reader.output_snapshot.side_effect = lambda p: self.rows[0]["original_live_outputs"] if p == self.policy else {"new_peer_generation": "allowed"}
        prefix = self.policy.release_prefix + RID + "/"
        self.keys = [prefix + f"{i:04d}-{phase}.json" for i, phase in enumerate(final.PHASES, 1)]
        self.obj.s3.list_objects_v2.return_value = {"Contents": [{"Key": k} for k in self.keys]}
        self.blobs = dict(zip(self.keys, self.rows))
        h = history(self.rows, self.policy)
        self.blobs.update({h["probe_prefix"] + "markers/ready.json": h["identity"],
            h["probe_prefix"] + "authorize.json": {"phase": "host-authorized", "release_id": RID,
                "task_arn": h["task"], "ready_sha256": final.sha(final.canonical(h["identity"]))},
            h["complete"]["output_manifest_key"]: self.manifest})
        def fetch(_s3, key, optional=False):
            if key not in self.blobs and optional:
                return None
            value = self.blobs[key]
            raw = final.canonical(value) if key.endswith("markers/ready.json") else final.binding.canonical_bytes(value)
            return value, raw, '"object"'
        self.leases = {"vevo": (lock(), '"old"'), "roy": (lock("released", "peer-owner"), '"peer"')}
        for target, kwargs in (
            ("private_object", {"side_effect": fetch}), ("read_lease", {"side_effect": lambda s3, p: self.leases[p]}),
            ("control_source", {"return_value": "1" * 64}), ("committed_gate_sha", {"return_value": GATE}),
        ):
            patcher = patch.object(final, target, **kwargs)
            patcher.start()
            self.addCleanup(patcher.stop)
        patcher = patch.object(final.binding, "require_private_bucket")
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_restored_current_peer_generation_is_new_baseline(self):
        result = self.obj.capture()
        self.assertEqual(result["peer_outputs"], {"new_peer_generation": "allowed"})
        self.assertEqual(result["own_lease"][0]["state"], "uncertain")
        self.assertNotIn("never-persist-me", repr(result))
        self.assertEqual(set(result["apprunner"]), set(final.PROJECTS))
        self.obj.s3.put_object.assert_not_called()
        self.reader.no_report_tasks.assert_called_once()

    def test_foreign_owner_etag_active_peer_fail_closed(self):
        for key, value in (("owner", "foreign"), ("state", "active")):
            with self.subTest(key=key):
                self.leases["vevo"][0][key] = value
                with self.assertRaisesRegex(RuntimeError, "lease-drift"):
                    self.obj.capture()
                self.leases["vevo"] = (lock(), '"old"')
        self.leases["vevo"] = (lock(), '"new"')
        with self.assertRaisesRegex(RuntimeError, "lease-drift"):
            self.obj.capture()
        self.leases["vevo"] = (lock(), '"old"')
        self.leases["roy"][0]["state"] = "uncertain"
        with self.assertRaisesRegex(RuntimeError, "peer-not-released"):
            self.obj.capture()

    def test_changed_schedule_candidate_outputs_role_tasks_or_approval_refuse(self):
        alterations = (
            (lambda: self.schedules[self.policy.service].update(State="DISABLED"), "schedule-not-restored"),
            (lambda: self.candidate.update(status="ACTIVE"), "candidate-not-inactive"),
            (lambda: self.schedules["roy-daily-report-email"].update(State="DISABLED"), "protected-schedule"),
            (lambda: setattr(self.reader.output_snapshot, "side_effect", lambda p: {"changed": True}), "own-outputs"),
            (lambda: setattr(self.reader.iam.get_role, "side_effect", None), "role-present"),
            (lambda: setattr(self.reader.no_report_tasks, "side_effect", RuntimeError("active-task")), "active-task"),
            (lambda: self.blobs.update({self.policy.probe_prefix + RID + "/review/independent-source.json": {"approved": True}}), "approval-exists"),
        )
        # Each variant gets a fresh fixture so no rejection is hidden behind another.
        for change, reason in alterations:
            self.setUp()
            change()
            with self.subTest(reason=reason), self.assertRaisesRegex(RuntimeError, reason):
                self.obj.capture()
            self.obj.s3.put_object.assert_not_called()

    def test_incomplete_receipts_or_role_access_denied_cannot_be_absence(self):
        self.obj.s3.list_objects_v2.return_value["IsTruncated"] = True
        with self.assertRaisesRegex(RuntimeError, "inventory"):
            self.obj.capture()
        self.obj.s3.list_objects_v2.return_value.pop("IsTruncated")
        self.reader.iam.get_role.side_effect = error("AccessDenied")
        with self.assertRaisesRegex(RuntimeError, "role-read-failed"):
            self.obj.capture()
        self.obj.s3.put_object.assert_not_called()

    def test_candidate_must_be_unused_even_in_unchanged_peer_configuration(self):
        peer = self.schedules["roy-daily-report-email"]
        peer["Target"] = {"EcsParameters": {"TaskDefinitionArn": self.candidate["taskDefinitionArn"]}}
        self.rows[0]["protected_schedules"]["roy-daily-report-email"] = copy.deepcopy(peer)
        with self.assertRaisesRegex(RuntimeError, "candidate-not-inactive-unused"):
            self.obj.capture()


class FinalizeTests(unittest.TestCase):
    def fixture(self):
        obj = Mock()
        obj.policy, obj.args = final.project_policy("vevo"), types.SimpleNamespace(release_id=RID)
        snapshot = {"own_lease": [lock(), '"old"'], "peer_outputs": {"current": 1}}
        proof = {"schema_version": 1, "phase": "restored-release-inspected", "passed": True,
                 "snapshot": snapshot, "captured_at": NOW.isoformat()}
        obj.capture.side_effect = lambda **kw: dict(snapshot, own_lease=list(kw["expected_lease"])) if kw else copy.deepcopy(snapshot)
        return obj, snapshot, proof

    def test_intent_precedes_single_cas_and_postproof(self):
        obj, snapshot, proof = self.fixture()
        sequence = []
        def archive(s3, key, value):
            sequence.append(key.rsplit("/", 1)[1])
            if key.endswith("intent.json"):
                self.assertEqual(value["expected_lease"], tuple(snapshot["own_lease"]))
                self.assertEqual(value["desired_lease"]["state"], "released")
            return {"key": key, "sha256": "1" * 64}
        with patch.object(final, "private_object", return_value=None), patch.object(final, "immutable_put", side_effect=archive), \
                patch.object(final, "single_release_cas", side_effect=lambda *a: sequence.append("CAS") or '"new"') as cas:
            result, _ = final.finalize(obj, proof, final.digest(proof), "f" * 32, now=lambda: NOW)
        self.assertEqual(sequence, ["intent.json", "CAS", "complete.json"])
        self.assertTrue(result["passed"])
        self.assertEqual(result["snapshot"]["own_lease"][0]["owner"], RID)
        cas.assert_called_once()

    def test_stale_forged_or_future_inspect_never_writes(self):
        for age in (900, 901, -1):
            obj, _, proof = self.fixture()
            proof["captured_at"] = (NOW - timedelta(seconds=age)).isoformat()
            with patch.object(final, "immutable_put") as put, self.assertRaisesRegex(RuntimeError, "expired"):
                final.finalize(obj, proof, final.digest(proof), "f" * 32, now=lambda: NOW)
            put.assert_not_called()
        obj, _, proof = self.fixture()
        with self.assertRaisesRegex(RuntimeError, "sha256"):
            final.finalize(obj, proof, "0" * 64, "f" * 32, now=lambda: NOW)
        obj.capture.assert_not_called()

    def test_intent_collision_and_peer_drift_block_cas(self):
        obj, snapshot, proof = self.fixture()
        with patch.object(final, "private_object", return_value={}), patch.object(final, "single_release_cas") as cas, \
                self.assertRaisesRegex(RuntimeError, "intent-already-exists"):
            final.finalize(obj, proof, final.digest(proof), "f" * 32, now=lambda: NOW)
        cas.assert_not_called()

    def test_new_operation_uuid_does_not_bypass_release_scoped_intent(self):
        obj, _, proof = self.fixture()
        with patch.object(final, "private_object", return_value={}) as get, patch.object(final, "immutable_put") as put:
            for operation in ("e" * 32, "f" * 32):
                with self.assertRaisesRegex(RuntimeError, "intent-already-exists"):
                    final.finalize(obj, proof, final.digest(proof), operation, now=lambda: NOW)
        self.assertEqual(get.call_args_list[0].args[1], get.call_args_list[1].args[1])
        put.assert_not_called()

    def test_current_peer_boundary_change_after_intent_rejects(self):
        for field in ("peer_outputs", "apprunner", "scheduled_definitions", "peer_lease"):
            obj, snapshot, proof = self.fixture()
            obj.capture.side_effect = [snapshot, dict(snapshot, **{field: {"changed": 2}})]
            with self.subTest(field=field), patch.object(final, "private_object", return_value=None), \
                    patch.object(final, "immutable_put"), patch.object(final, "single_release_cas") as cas, \
                    self.assertRaisesRegex(RuntimeError, "before-cas"):
                final.finalize(obj, proof, final.digest(proof), "e" * 32, now=lambda: NOW)
            cas.assert_not_called()

    def test_post_cas_drift_has_no_rollback_or_success_receipt(self):
        obj, snapshot, proof = self.fixture()
        obj.capture.side_effect = [snapshot, snapshot, dict(snapshot, peer_outputs={"changed": 2})]
        with patch.object(final, "private_object", return_value=None), patch.object(final, "immutable_put") as put, \
                patch.object(final, "single_release_cas", return_value='"new"') as cas, self.assertRaisesRegex(RuntimeError, "after-cas"):
            final.finalize(obj, proof, final.digest(proof), "f" * 32, now=lambda: NOW)
        cas.assert_called_once()
        self.assertEqual(put.call_count, 2)
        self.assertTrue(put.call_args.args[1].endswith("/failed.json"))
        self.assertFalse(put.call_args.args[2]["automatic_retry_or_rollback"])


class CASTests(unittest.TestCase):
    def test_lost_ack_exact_readback_only_no_retry(self):
        old, desired = (lock(), '"old"'), lock("released")
        lease = Mock()
        lease._read.side_effect = [old, (desired, '"new"')]
        lease._put.side_effect = TimeoutError("lost-response")
        self.assertEqual(final.single_release_cas(lease, old, desired), '"new"')
        lease._put.assert_called_once_with(desired, old[1])

    def test_readback_failure_foreign_race_unchanged_etag_never_replays(self):
        old, desired = (lock(), '"old"'), lock("released")
        for outcome in (TimeoutError(), (lock("released", "foreign"), '"new"'), (desired, '"old"'), old):
            lease = Mock()
            lease._read.side_effect = [old, outcome]
            lease._put.side_effect = TimeoutError()
            with self.subTest(outcome=outcome), self.assertRaises(RuntimeError):
                final.single_release_cas(lease, old, desired)
            lease._put.assert_called_once()
        lease._read.side_effect = [(lock("uncertain", "foreign"), '"other"')]
        lease._put.reset_mock()
        with self.assertRaisesRegex(RuntimeError, "precondition-drift"):
            final.single_release_cas(lease, old, desired)
        lease._put.assert_not_called()

    def test_real_lease_writer_uses_exact_conditional_encrypted_request(self):
        old, desired = (lock(), '"old"'), lock("released")
        for project in final.PROJECTS:
            s3 = Mock()
            blobs = [final.binding.canonical_bytes(old[0]), final.binding.canonical_bytes(desired)]
            s3.get_object.side_effect = [{"Body": io.BytesIO(raw), "ContentLength": len(raw),
                "ServerSideEncryption": "AES256", "ETag": etag} for raw, etag in zip(blobs, ['"old"', '"new"'])]
            lease = final.binding.MigrationLease(s3, owner=RID) if project == "vevo" else final.ScopedImageLease(s3, owner=RID, project=project)
            self.assertEqual(final.single_release_cas(lease, old, desired), '"new"')
            kwargs = s3.put_object.call_args.kwargs
            self.assertEqual(kwargs["Key"], final.project_policy(project).lease_key)
            self.assertEqual(kwargs["IfMatch"], '"old"')
            self.assertEqual(kwargs["ExpectedBucketOwner"], final.ACCOUNT)
            self.assertEqual(kwargs["ServerSideEncryption"], "AES256")
            s3.put_object.assert_called_once()


class PrivateReceiptTests(unittest.TestCase):
    def test_immutable_receipt_requires_owner_encryption_no_overwrite_and_full_readback(self):
        s3 = Mock()
        value = {"schema_version": 1, "synthetic": True}
        raw = final.binding.canonical_bytes(value)
        s3.get_object.return_value = {"Body": io.BytesIO(raw), "ContentLength": len(raw),
                                     "ServerSideEncryption": "AES256", "ETag": '"receipt"'}
        result = final.immutable_put(s3, "private/intent.json", value)
        self.assertEqual(result["sha256"], final.sha(raw))
        s3.put_object.assert_called_once()
        request = s3.put_object.call_args.kwargs
        self.assertEqual(request["ExpectedBucketOwner"], final.ACCOUNT)
        self.assertEqual(request["IfNoneMatch"], "*")
        self.assertEqual(request["ServerSideEncryption"], "AES256")
        self.assertEqual(s3.get_object.call_args.kwargs["ExpectedBucketOwner"], final.ACCOUNT)

    def test_lost_intent_ack_has_no_retry_or_lease_write(self):
        s3 = Mock()
        s3.put_object.side_effect = TimeoutError()
        with self.assertRaises(TimeoutError):
            final.immutable_put(s3, "private/intent.json", {"synthetic": True})
        s3.put_object.assert_called_once()
        s3.get_object.assert_not_called()

    def test_absence_requires_exact_no_such_key_and_strict_private_metadata(self):
        s3 = Mock()
        for code in ("AccessDenied", "NoSuchBucket", "404"):
            s3.get_object.side_effect = error(code)
            with self.subTest(code=code), self.assertRaises(ClientError):
                final.private_object(s3, "private", optional=True)
        s3.get_object.side_effect = error("NoSuchKey")
        self.assertIsNone(final.private_object(s3, "private", optional=True))
        s3.get_object.side_effect = None
        for encryption, size, raw in (("aws:kms", 2, b"{}"), ("AES256", 3, b"{}"), ("AES256", 0, b"")):
            s3.get_object.return_value = {"Body": io.BytesIO(raw), "ContentLength": size,
                "ServerSideEncryption": encryption, "ETag": '"etag"'}
            with self.subTest(encryption=encryption, size=size), self.assertRaises(RuntimeError):
                final.private_object(s3, "private")


class LocalInputTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        (self.root / "data").mkdir()
        patcher = patch.object(final, "ROOT", self.root)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_lexical_scope_traversal_and_existing_reparse_are_rejected(self):
        for path in (self.root / "outside.json", self.root / "data" / ".." / "escape.json"):
            with self.subTest(path=str(path)), self.assertRaisesRegex(RuntimeError, "local-path"):
                final.local_path(path)
        target = self.root / "data" / "proof.json"
        target.write_bytes(b"{}")
        original = Path.lstat
        for mode, attrs in ((0o120777, 0), (0o100600, 0x400)):
            def fake(path):
                return types.SimpleNamespace(st_mode=mode, st_file_attributes=attrs) if path == target else original(path)
            with patch.object(Path, "lstat", fake), self.assertRaisesRegex(RuntimeError, "alias"):
                final.local_path(target, must_exist=True)

    def test_reparse_ancestor_is_rejected_before_resolve(self):
        directory = self.root / "data" / "junction"
        directory.mkdir()
        original = Path.lstat
        def fake(path):
            return types.SimpleNamespace(st_mode=0o040777, st_file_attributes=0x400) if path == directory else original(path)
        with patch.object(Path, "lstat", fake), self.assertRaisesRegex(RuntimeError, "alias"):
            final.local_path(directory / "output.json")

    def test_bounded_inspection_read_and_hash(self):
        path = self.root / "data" / "proof.json"
        raw = final.binding.canonical_bytes({"synthetic": True})
        path.write_bytes(raw)
        self.assertEqual(final.read_inspection(path, final.sha(raw)), {"synthetic": True})
        with self.assertRaisesRegex(RuntimeError, "file-hash"):
            final.read_inspection(path, "0" * 64)
        with patch.object(final, "LIMIT", 1), patch.object(Path, "open") as opening, \
                self.assertRaisesRegex(RuntimeError, "file-size"):
            final.read_inspection(path, final.sha(raw))
        opening.assert_not_called()

    def test_cli_profile_and_write_retry_configuration_are_explicit(self):
        argv = ["finalizer", "inspect", "--project", "vevo", "--release-id", RID,
                "--release-source", SOURCE, "--control-source", CONTROL, "--failure-sha256", "0" * 64,
                "--lease-sha256", "1" * 64, "--lease-etag", '"old"', "--profile", "codex",
                "--output", str(self.root / "data" / "proof.json")]
        with patch.object(final.sys, "argv", argv), patch("boto3.Session") as session, \
                patch.object(final, "Inspector") as inspector, patch("builtins.print"):
            inspector.return_value.capture.return_value = {"synthetic": True}
            final.main()
        session.assert_called_once_with(profile_name="codex", region_name=final.REGION)
        config = session.return_value._session.set_default_client_config.call_args.args[0]
        self.assertEqual(config.retries["total_max_attempts"], 1)


if __name__ == "__main__":
    unittest.main()
