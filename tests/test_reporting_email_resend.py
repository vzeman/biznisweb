"""No network: exercise the immutable intent boundary with actual MIME formatter."""
from copy import deepcopy
from datetime import datetime, timezone
from email import policy
from email.parser import BytesParser
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
import zipfile

from botocore.exceptions import ClientError

from scripts import resend_reporting_email as mail


RID, SOURCE, DIGEST = "a" * 32, "b" * 40, "sha256:" + "c" * 64
DAY, GENERATION = "2026-10-26", "20261027T010203Z"
TASK = f"arn:aws:ecs:{mail.REGION}:{mail.ACCOUNT}:task/vevo-reporting-cluster/" + "d" * 32
TD = f"arn:aws:ecs:{mail.REGION}:{mail.ACCOUNT}:task-definition/vevo-reporting-daily:56"
SECRET = f"arn:aws:secretsmanager:{mail.REGION}:{mail.ACCOUNT}:secret:vevo/reporting-abcdef"


class MemoryS3:
    def __init__(self):
        self.objects, self.puts = {}, []
        self.fail_put = None
        self.encryption = "AES256"

    def get_object(self, **args):
        assert args["Bucket"] == mail.BUCKET and args["ExpectedBucketOwner"] == mail.ACCOUNT
        if args["Key"] not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[args["Key"]]), "ServerSideEncryption": self.encryption}

    def put_object(self, **args):
        assert args["ExpectedBucketOwner"] == mail.ACCOUNT and args["ServerSideEncryption"] == "AES256"
        assert args["IfNoneMatch"] == "*"
        key = args["Key"]
        self.puts.append(key)
        if key == self.fail_put:
            raise TimeoutError("private transport detail")
        if key in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.objects[key] = args["Body"]

    def list_objects_v2(self, **args):
        assert args["ExpectedBucketOwner"] == mail.ACCOUNT
        return {"Contents": [{"Key": key} for key in sorted(self.objects) if key.startswith(args["Prefix"])]}


class Fixture:
    def __init__(self):
        self.s3 = MemoryS3()
        self.schedule = {"Name": mail.VEVO.service, "State": "ENABLED", "ScheduleExpression": "cron(0 1 * * ? *)",
                         "ScheduleExpressionTimezone": "Europe/Bratislava", "FlexibleTimeWindow": {"Mode": "OFF"},
                         "Target": {"Arn": mail.binding.POLICY["cluster"], "EcsParameters": {"TaskDefinitionArn": TD}}}
        self.definition = {"taskDefinitionArn": TD, "status": "ACTIVE", "family": mail.VEVO.family, "networkMode": "awsvpc",
            "taskRoleArn": f"arn:aws:iam::{mail.ACCOUNT}:role/BiznisWebReportingTaskRole-vevo",
            "executionRoleArn": f"arn:aws:iam::{mail.ACCOUNT}:role/ecsTaskExecutionRole",
            "containerDefinitions": [{"name": "reporting", "workingDirectory": "/app",
                "image": f"{mail.ACCOUNT}.dkr.ecr.{mail.REGION}.amazonaws.com/vevo-reporting@{DIGEST}",
                "environment": [{"name": key, "value": value} for key, value in {
                    "REPORT_PROJECT": "vevo", "REPORT_S3_BUCKET": mail.BUCKET, "REPORT_S3_PREFIX": mail.VEVO.sink,
                    "REPORT_SKIP_INVOICES": "true", "SES_CONFIGURATION_SET": "vevo-reporting"}.items()],
                "secrets": [{"name": name, "valueFrom": SECRET + ":" + name + "::"} for name in mail.CONTACTS]}]}
        self.contacts = {"REPORT_EMAIL_FROM": "reports@example.test", "REPORT_EMAIL_TO": "owner@example.test,second@example.test"}
        self.manifest = {"schema_version": 1, "project": "vevo", "generation_id": GENERATION, "artifacts": {}}
        for period in ("latest", "7d", "30d", "90d"):
            payload = {"project": "vevo", "date_from": "2025-05-03", "date_to": DAY,
                "period_switcher": {"current_key": "full" if period == "latest" else period},
                "source_health": {"is_partial": False, "qa_status": "ok", "qa_failure_count": 0, "qa_errors": []},
                "dashboard": {"kpis": {"countries": {country: {"status": "ok", "windows": {
                    name: {} for name in ("daily", "weekly", "monthly", "all_time")}} for country in ("sk", "cz", "hu")}}}}
            raw = mail.canonical(payload)
            for name, content in ((f"dashboard_payload_{period}.json", raw),
                                  (f"report_{period}.html", b'<html><script id="report-dashboard-json" type="application/json">' + raw + b'</script></html>')):
                key = f"{mail.VEVO.sink}/{GENERATION}/{name}"
                self.s3.objects[key] = content
                self.manifest["artifacts"][name] = {"key": key, "size": len(content), "sha256": mail.sha(content)}
        self.manifest_raw = mail.canonical(self.manifest)
        self.manifest_sha = mail.sha(self.manifest_raw)
        self.s3.objects[mail.VEVO.sink + "/latest/generation.json"] = self.manifest_raw
        self.s3.objects[mail.VEVO.lease_key] = mail.canonical({"state": "released", "owner": RID})
        self.base = {"schema_version": 1, "project": "vevo", "release_id": RID, "source_commit": SOURCE, "report_to_date": DAY}
        self.live = {**self.base, "phase": "live-complete", "generation_manifest": self.manifest,
            "generation_manifest_sha256": self.manifest_sha, "email_sent": False,
            "terminal_task": {"taskArn": TASK, "taskDefinitionArn": TD, "lastStatus": "STOPPED",
                "containers": [{"exitCode": 0, "imageDigest": DIGEST}],
                "attachments": [{"details": [{"name": "privateIPv4Address", "value": "172.31.1.2"}]}]},
            "identity": {"path": "/app", "service": mail.VEVO.service, "marker": mail.VEVO.live_marker,
                         "task_arn": TASK, "task_definition": TD, "source_commit": SOURCE, "image_digest": DIGEST,
                         "project": "vevo", "release_id": RID, "report_to_date": DAY, "private_ip": "172.31.1.2",
                         "skip_email": True, "skip_invoices": True, "skip_inline_guard": True}}
        self.final = {**self.base, "phase": "release-complete", "probe_role_removed": True,
                      "all_owned_tasks_stopped": True, "final_schedule": deepcopy(self.schedule)}
        for index, row in enumerate(({**self.base, "phase": "preflight-complete"}, self.live, self.final), 1):
            self.s3.objects[self.journal_key(index, row["phase"])] = mail.canonical(row)
        self.clients = {"s3": self.s3, "sts": Mock(), "ecs": Mock(), "scheduler": Mock(), "secretsmanager": Mock(), "ses": Mock()}
        self.clients["sts"].get_caller_identity.return_value = {"Account": mail.ACCOUNT}
        self.clients["scheduler"].get_schedule.side_effect = lambda **_args: deepcopy(self.schedule)
        self.clients["ecs"].describe_task_definition.side_effect = lambda **_args: {"taskDefinition": deepcopy(self.definition)}
        self.clients["secretsmanager"].get_secret_value.side_effect = lambda **_args: {"SecretString": json.dumps(self.contacts)}
        self.clients["ses"].send_raw_email.return_value = {"MessageId": "ses-test-message"}

    @staticmethod
    def journal_key(index, phase):
        return f"{mail.VEVO.release_prefix}{RID}/{index:04d}-{phase}.json"


class ReportingEmailResendTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        (self.root / "projects/vevo").mkdir(parents=True)
        (self.root / "projects/vevo/settings.json").write_text('{"ses_configuration_set":"vevo-reporting"}')
        self.root_patch = patch.object(mail, "ROOT", self.root)
        self.root_patch.start()
        self.code_patch = patch.object(mail, "code_hashes", return_value={"fixture": "e" * 64})
        self.code_patch.start()
        self.network = patch("socket.socket.connect", side_effect=AssertionError("network-forbidden"))
        self.network.start()
        self.fixture = Fixture()

    def tearDown(self):
        self.network.stop()
        self.code_patch.stop()
        self.root_patch.stop()
        self.temp.cleanup()

    def prepare(self):
        with patch("dotenv.load_dotenv", return_value=False):
            return mail.prepare(self.fixture.clients, release_id=RID, manifest_sha256=self.fixture.manifest_sha,
                                to_date=DAY, scope="country-cards-20261027", output=self.root / "data/prepared")

    def send(self, result):
        return mail.send(self.fixture.clients, prepared=result["prepared"], prepared_sha256=result["prepared_sha256"], authorize_send=True)

    def test_prepare_uses_real_core_mime_no_ses_or_s3_writes_no_addresses_in_receipt(self):
        result = self.prepare()
        self.assertFalse(result["email_sent"])
        self.fixture.clients["ses"].send_raw_email.assert_not_called()
        self.assertEqual([], self.fixture.s3.puts)
        raw = Path(result["prepared"]).read_bytes()
        self.assertNotIn(b"example.test", raw)
        self.assertEqual(2, json.loads(raw)["recipient_count"])
        mime = BytesParser(policy=policy.default).parsebytes((Path(result["prepared"]).parent / "message.eml").read_bytes())
        attachments = list(mime.iter_attachments())
        self.assertEqual(["report_latest.html"], [item.get_filename() for item in attachments])
        expected = self.fixture.s3.objects[self.fixture.manifest["artifacts"]["report_latest.html"]["key"]]
        self.assertEqual(expected, attachments[0].get_payload(decode=True))

    def test_core_html_zip_threshold_is_preserved(self):
        with patch("dotenv.load_dotenv", return_value=False):
            runner = __import__("daily_report_runner")
        with patch.object(runner, "SES_RAW_MESSAGE_TARGET_BYTES", 1):
            result = self.prepare()
        mime = BytesParser(policy=policy.default).parsebytes((Path(result["prepared"]).parent / "message.eml").read_bytes())
        attachment = list(mime.iter_attachments())[0]
        self.assertEqual("report_latest.html.zip", attachment.get_filename())
        with zipfile.ZipFile(io.BytesIO(attachment.get_payload(decode=True))) as archive:
            self.assertEqual(["report_latest.html"], archive.namelist())

    def test_send_once_then_existing_accepted_never_resends(self):
        result = self.prepare()
        self.assertEqual("accepted", self.send(result)["status"])
        self.assertEqual("already-accepted", self.send(result)["status"])
        self.fixture.clients["ses"].send_raw_email.assert_called_once()
        self.assertEqual([mail.PREFIX + "country-cards-20261027/intent.json", mail.PREFIX + "country-cards-20261027/accepted.json"], self.fixture.s3.puts)

    def test_ambiguous_ses_failure_remains_uncertain_without_retry(self):
        result = self.prepare()
        self.fixture.clients["ses"].send_raw_email.side_effect = TimeoutError("private recipient detail")
        with self.assertRaises(TimeoutError):
            self.send(result)
        with self.assertRaisesRegex(RuntimeError, "email-intent-uncertain"):
            self.send(result)
        self.fixture.clients["ses"].send_raw_email.assert_called_once()

    def test_intent_put_failure_never_calls_ses(self):
        result = self.prepare()
        self.fixture.s3.fail_put = mail.PREFIX + "country-cards-20261027/intent.json"
        with self.assertRaises(TimeoutError):
            self.send(result)
        self.fixture.clients["ses"].send_raw_email.assert_not_called()

    def test_acceptance_archive_failure_retains_local_message_id_and_no_resend(self):
        result = self.prepare()
        self.fixture.s3.fail_put = mail.PREFIX + "country-cards-20261027/accepted.json"
        with self.assertRaises(TimeoutError):
            self.send(result)
        self.assertEqual("ses-test-message", json.loads((Path(result["prepared"]).parent / "accepted.json").read_bytes())["message_id"])
        with self.assertRaisesRegex(RuntimeError, "email-local-acceptance-exists-reconcile"):
            self.send(result)
        self.fixture.clients["ses"].send_raw_email.assert_called_once()

    def test_contact_change_stops_before_intent(self):
        result = self.prepare()
        self.fixture.contacts["REPORT_EMAIL_TO"] = "changed@example.test"
        with self.assertRaisesRegex(RuntimeError, "email-contacts-changed"):
            self.send(result)
        self.assertFalse(self.fixture.s3.puts)

    def test_mime_tamper_stops_before_intent(self):
        result = self.prepare()
        (Path(result["prepared"]).parent / "message.eml").write_bytes(b"tampered")
        with self.assertRaisesRegex(RuntimeError, "email-local-hash"):
            self.send(result)
        self.assertFalse(self.fixture.s3.puts)

    def test_explicit_send_required(self):
        result = self.prepare()
        with self.assertRaisesRegex(RuntimeError, "email-explicit-send-required"):
            mail.send(self.fixture.clients, prepared=result["prepared"], prepared_sha256=result["prepared_sha256"], authorize_send=False)
        self.assertFalse(self.fixture.s3.puts)

    def test_all_eight_artifact_bytes_are_bound(self):
        key = self.fixture.manifest["artifacts"]["report_90d.html"]["key"]
        self.fixture.s3.objects[key] = self.fixture.s3.objects[key].replace(b"<html>", b"<HTML>")
        with self.assertRaisesRegex(RuntimeError, "image-release-artifact-hash"):
            self.prepare()
        self.fixture.clients["secretsmanager"].get_secret_value.assert_called()
        self.assertFalse(self.fixture.s3.puts)

    def test_country_unavailable_or_missing_window_is_rejected(self):
        key = self.fixture.manifest["artifacts"]["dashboard_payload_latest.json"]["key"]
        payload = json.loads(self.fixture.s3.objects[key])
        for country in ("sk", "cz", "hu"):
            changed = deepcopy(payload)
            changed["dashboard"]["kpis"]["countries"][country]["status"] = "unavailable"
            with self.assertRaisesRegex(RuntimeError, "email-country-status"):
                mail.country_payload(mail.canonical(changed))
        del payload["dashboard"]["kpis"]["countries"]["sk"]["windows"]["daily"]
        with self.assertRaisesRegex(RuntimeError, "email-country-status"):
            mail.country_payload(mail.canonical(payload))

    def test_current_schedule_metadata_allowed_but_runtime_drift_rejected(self):
        self.fixture.schedule["LastModificationDate"] = datetime.now(timezone.utc)
        self.prepare()
        self.fixture.schedule["State"] = "DISABLED"
        with self.assertRaisesRegex(RuntimeError, "email-schedule-drift"):
            mail.release_context(self.fixture.clients, RID, self.fixture.manifest_sha, DAY)

    def test_release_failure_or_foreign_project_never_prepares(self):
        for field, value in (("phase", "release-failed"), ("project", "roy"), ("report_to_date", "2026-10-25")):
            row = {**self.fixture.final, field: value}
            self.fixture.s3.objects[self.fixture.journal_key(3, "release-complete")] = mail.canonical(row)
            with self.assertRaises(RuntimeError):
                self.prepare()

    def test_latest_changed_or_unreleased_lease_rejected(self):
        self.fixture.s3.objects[mail.VEVO.lease_key] = mail.canonical({"state": "active", "owner": RID})
        with self.assertRaisesRegex(RuntimeError, "email-release-lease"):
            self.prepare()
        self.fixture.s3.objects[mail.VEVO.lease_key] = mail.canonical({"state": "released", "owner": RID})
        self.fixture.s3.objects[mail.VEVO.sink + "/latest/generation.json"] += b" "
        with self.assertRaisesRegex(RuntimeError, "email-latest-drift"):
            self.prepare()

    def test_foreign_secret_or_environment_contacts_rejected_before_secret(self):
        for container in (self.fixture.definition["containerDefinitions"][0],):
            original = deepcopy(container)
            container["secrets"][0]["valueFrom"] = SECRET.replace("vevo/", "roy/") + ":REPORT_EMAIL_FROM::"
            with self.assertRaisesRegex(RuntimeError, "email-secret-reference"):
                self.prepare()
            container.clear()
            container.update(original)
            container["environment"].append({"name": "REPORT_EMAIL_FROM", "value": "arbitrary@example.test"})
            with self.assertRaisesRegex(RuntimeError, "email-contact-not-secret"):
                self.prepare()
        self.fixture.clients["secretsmanager"].get_secret_value.assert_not_called()

    def test_wrong_account_encryption_or_missing_release_rejected(self):
        self.fixture.clients["sts"].get_caller_identity.return_value = {"Account": "0" * 12}
        with self.assertRaisesRegex(RuntimeError, "email-account"):
            self.prepare()
        self.fixture.clients["sts"].get_caller_identity.return_value = {"Account": mail.ACCOUNT}
        self.fixture.s3.encryption = "aws:kms"
        with self.assertRaisesRegex(RuntimeError, "probe-encryption-invalid"):
            self.prepare()

    def test_schedule_contact_override_and_duplicate_secret_rejected(self):
        self.fixture.schedule["Target"]["Input"] = json.dumps({"containerOverrides": [{"environment": [
            {"name": "REPORT_EMAIL_TO", "value": "other@example.test"}]}]})
        self.fixture.final["final_schedule"] = deepcopy(self.fixture.schedule)
        self.fixture.s3.objects[self.fixture.journal_key(3, "release-complete")] = mail.canonical(self.fixture.final)
        with self.assertRaisesRegex(RuntimeError, "email-contact-schedule-override"):
            self.prepare()
        del self.fixture.schedule["Target"]["Input"]
        self.fixture.final["final_schedule"] = deepcopy(self.fixture.schedule)
        self.fixture.s3.objects[self.fixture.journal_key(3, "release-complete")] = mail.canonical(self.fixture.final)
        container = self.fixture.definition["containerDefinitions"][0]
        container["secrets"].append(deepcopy(container["secrets"][0]))
        with self.assertRaisesRegex(RuntimeError, "email-secret-duplicate"):
            self.prepare()

    def test_invalid_live_identity_never_reads_contacts(self):
        self.fixture.live["identity"]["private_ip"] = "172.31.9.9"
        self.fixture.s3.objects[self.fixture.journal_key(2, "live-complete")] = mail.canonical(self.fixture.live)
        with self.assertRaisesRegex(RuntimeError, "email-live-host"):
            self.prepare()
        self.fixture.clients["secretsmanager"].get_secret_value.assert_not_called()

    def test_intent_race_winner_prevents_second_ses_call(self):
        result = self.prepare()
        original_put = self.fixture.s3.put_object
        def raced(**args):
            self.fixture.s3.objects[args["Key"]] = b"foreign-race-winner"
            return original_put(**args)
        self.fixture.s3.put_object = raced
        with self.assertRaises(ClientError):
            self.send(result)
        self.fixture.clients["ses"].send_raw_email.assert_not_called()

    def test_post_intent_pointer_change_blocks_send_without_retry(self):
        result = self.prepare()
        original_put = self.fixture.s3.put_object
        def changed(**args):
            original_put(**args)
            self.fixture.s3.objects[mail.VEVO.sink + "/latest/generation.json"] += b" "
        self.fixture.s3.put_object = changed
        with self.assertRaisesRegex(RuntimeError, "email-latest-drift"):
            self.send(result)
        self.fixture.clients["ses"].send_raw_email.assert_not_called()

    def test_paths_cannot_escape_private_data_and_output_cannot_be_overwritten(self):
        with self.assertRaisesRegex(RuntimeError, "email-local-path-scope"):
            mail.private_path(self.root / "other/message.eml", exists=False)
        self.prepare()
        with self.assertRaisesRegex(RuntimeError, "email-output-exists"):
            self.prepare()

    def test_parent_traversal_rejected_for_prepare_and_read(self):
        escaped = self.root / "data" / ".." / "escape"
        with self.assertRaisesRegex(RuntimeError, "email-local-path-scope"):
            mail.prepare(self.fixture.clients, release_id=RID, manifest_sha256=self.fixture.manifest_sha,
                         to_date=DAY, scope="country-cards-20261027", output=escaped)
        outside = self.root / "escape"
        outside.write_bytes(b"outside private data")
        with self.assertRaisesRegex(RuntimeError, "email-local-path-scope"):
            mail.read_local(escaped, mail.sha(outside.read_bytes()))
        self.fixture.clients["sts"].get_caller_identity.assert_not_called()
        self.fixture.clients["ses"].send_raw_email.assert_not_called()
        self.assertEqual([], self.fixture.s3.puts)

    def test_existing_local_acceptance_blocks_before_any_cloud_write_or_ses(self):
        result = self.prepare()
        accepted = Path(result["prepared"]).parent / "accepted.json"
        accepted.write_bytes(b"preserved local acceptance for reconciliation")
        with self.assertRaisesRegex(RuntimeError, "email-local-acceptance-exists-reconcile"):
            self.send(result)
        self.assertEqual(b"preserved local acceptance for reconciliation", accepted.read_bytes())
        self.fixture.clients["ses"].send_raw_email.assert_not_called()
        self.assertEqual([], self.fixture.s3.puts)


if __name__ == "__main__":
    unittest.main()
