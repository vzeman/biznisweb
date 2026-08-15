import email
import os
import sys
import tempfile
import types
import unittest
import zipfile
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

import daily_report_runner as runner


class DailyReportEmailTests(unittest.TestCase):
    def test_large_html_is_zipped_before_ses_send(self) -> None:
        sent = {}

        class FakeSes:
            def send_raw_email(self, **kwargs):
                sent.update(kwargs)
                return {"MessageId": "test-message"}

        boto3 = types.ModuleType("boto3")
        boto3.client = lambda *_args, **_kwargs: FakeSes()
        botocore = types.ModuleType("botocore")
        botocore_exceptions = types.ModuleType("botocore.exceptions")
        botocore_exceptions.BotoCoreError = RuntimeError
        botocore_exceptions.ClientError = RuntimeError
        botocore.exceptions = botocore_exceptions

        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "report_latest.html"
            report.write_text("<html>" + ("report-row" * 900_000) + "</html>", encoding="utf-8")
            modules = {
                "boto3": boto3,
                "botocore": botocore,
                "botocore.exceptions": botocore_exceptions,
            }
            env = {"REPORT_EMAIL_FROM": "reports@example.test", "REPORT_EMAIL_TO": "owner@example.test"}
            with patch.dict(sys.modules, modules), patch.dict(os.environ, env, clear=False):
                message_id = runner.send_email_ses(
                    subject="Daily report",
                    body_text="Attached report",
                    file_paths={"report_html": report},
                    reporting_defaults={"ses_configuration_set": ""},
                )

        self.assertEqual(message_id, "test-message")
        raw = sent["RawMessage"]["Data"]
        self.assertLessEqual(len(raw), runner.SES_RAW_MESSAGE_LIMIT_BYTES)
        message = email.message_from_bytes(raw)
        attachments = [part for part in message.walk() if part.get_filename()]
        self.assertEqual([part.get_filename() for part in attachments], ["report_latest.html.zip"])
        with zipfile.ZipFile(BytesIO(attachments[0].get_payload(decode=True))) as archive:
            self.assertEqual(archive.namelist(), ["report_latest.html"])
            self.assertTrue(archive.read("report_latest.html").startswith(b"<html>"))

    def test_small_html_remains_direct_attachment(self) -> None:
        sent = {}

        class FakeSes:
            def send_raw_email(self, **kwargs):
                sent.update(kwargs)
                return {"MessageId": "small-message"}

        boto3 = types.ModuleType("boto3")
        boto3.client = lambda *_args, **_kwargs: FakeSes()
        botocore = types.ModuleType("botocore")
        botocore_exceptions = types.ModuleType("botocore.exceptions")
        botocore_exceptions.BotoCoreError = RuntimeError
        botocore_exceptions.ClientError = RuntimeError
        botocore.exceptions = botocore_exceptions

        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "report_latest.html"
            report.write_text("<html>small</html>", encoding="utf-8")
            modules = {
                "boto3": boto3,
                "botocore": botocore,
                "botocore.exceptions": botocore_exceptions,
            }
            env = {"REPORT_EMAIL_FROM": "reports@example.test", "REPORT_EMAIL_TO": "owner@example.test"}
            with patch.dict(sys.modules, modules), patch.dict(os.environ, env, clear=False):
                runner.send_email_ses(
                    subject="Daily report",
                    body_text="Attached report",
                    file_paths={"report_html": report},
                    reporting_defaults={"ses_configuration_set": ""},
                )

        message = email.message_from_bytes(sent["RawMessage"]["Data"])
        attachments = [part for part in message.walk() if part.get_filename()]
        self.assertEqual([part.get_filename() for part in attachments], ["report_latest.html"])


if __name__ == "__main__":
    unittest.main()
