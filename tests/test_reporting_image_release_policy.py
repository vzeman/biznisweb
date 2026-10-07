import copy
from dataclasses import FrozenInstanceError
from datetime import timedelta
import unittest
from unittest.mock import patch

from scripts import reporting_runtime_binding as binding
from scripts.deploy_vevo_report import diagnostic_policy
from scripts.reporting_image_release_policy import PROJECTS, VEVO, project_policy
from scripts.reporting_image_release_lease import ScopedImageLease, read_lease
from tests.test_reporting_runtime_binding import MemoryS3, NOW, StoreError


class ReleasePolicyTests(unittest.TestCase):
    def test_allowlist_is_immutable_and_vevo_iam_is_unchanged(self):
        self.assertEqual(diagnostic_policy("a" * 32), VEVO.probe_policy("a" * 32))
        with self.assertRaises(TypeError):
            PROJECTS["other"] = VEVO
        with self.assertRaises(FrozenInstanceError):
            VEVO.sink = "foreign"
        for value in ("other", "ROY", "../roy", None):
            with self.assertRaisesRegex(RuntimeError, "not-allowlisted"):
                project_policy(value)

    def test_roy_policy_cannot_write_live_output_or_read_vevo(self):
        policy = project_policy("roy")
        self.assertEqual("daily-reports/roy-sk", policy.sink)
        rules = policy.probe_policy("a" * 32)["Statement"]
        for rule in rules:
            self.assertNotIn("vevo", str(rule))
            self.assertEqual(["s3:GetObject"] if rule is rules[0] else ["s3:PutObject"], rule["Action"])
        self.assertEqual([f"arn:aws:s3:::{binding.BUCKET}/{policy.probe_prefix}" + "a" * 32 + suffix
                          for suffix in ("/markers/*", "/artifacts/*")], rules[1]["Resource"])


class ScopedLeaseTests(unittest.TestCase):
    def fixture(self):
        s3 = MemoryS3()
        vevo = binding.MigrationLease(s3, owner="a" * 32, now=lambda: NOW).acquire()
        vevo.retain_uncertain()
        before = copy.deepcopy(s3.objects[binding.LOCK_KEY])
        roy = ScopedImageLease(s3, owner="b" * 32, project="roy", now=lambda: NOW)
        return s3, roy, before

    def test_roy_lifecycle_preserves_retained_vevo_bytes_and_etag(self):
        s3, roy, before = self.fixture()
        start = len(s3.writes)
        roy.acquire().renew()
        roy.release()
        self.assertEqual(before, s3.objects[binding.LOCK_KEY])
        self.assertEqual([PROJECTS["roy"].lease_key] * 3, s3.writes[start:])
        self.assertEqual("released", read_lease(s3, "roy")[0]["state"])

    def test_every_vevo_gate_rejects_active_uncertain_and_missing_read_permission(self):
        s3, roy, _ = self.fixture()
        roy.acquire()
        for state in ("active", "uncertain"):
            if state == "uncertain":
                roy.retain_uncertain()
            with self.subTest(state=state), self.assertRaisesRegex(binding.BindingError, "peer-release-active-or-uncertain"):
                binding.check_migration(s3)
            with self.assertRaisesRegex(binding.BindingError, "peer-release-active-or-uncertain"):
                binding.MigrationLease(s3, owner="c" * 32, now=lambda: NOW).acquire()
        def denied(key):
            if key == PROJECTS["roy"].lease_key:
                raise StoreError("AccessDenied")
        s3.before_get = denied
        with self.assertRaises(StoreError):
            binding.check_migration(s3)

    def test_expired_roy_lease_cannot_be_stolen_or_wrong_project_constructed(self):
        s3, roy, _ = self.fixture()
        roy.acquire()
        later = ScopedImageLease(s3, owner="c" * 32, project="roy", now=lambda: NOW + timedelta(days=2))
        with self.assertRaisesRegex(binding.BindingError, "active-or-uncertain"):
            later.acquire()
        with self.assertRaisesRegex(binding.BindingError, "scoped-lease-project"):
            ScopedImageLease(s3, owner="c" * 32, project="vevo")

    def test_lost_put_or_read_ack_recovers_only_exact_own_generation(self):
        for lost in ("put", "read"):
            s3, roy, before = self.fixture()
            original_put, original_read = s3.put_object, roy._read
            count = [0]
            def put(**kwargs):
                result = original_put(**kwargs)
                if lost == "put":
                    raise OSError("response lost")
                return result
            def read(**kwargs):
                count[0] += 1
                if lost == "read" and count[0] == 2:
                    raise OSError("read response lost")
                return original_read(**kwargs)
            with self.subTest(lost=lost), patch.object(s3, "put_object", side_effect=put), patch.object(roy, "_read", side_effect=read):
                roy.acquire()
            self.assertIsNone(roy.pending_value)
            self.assertEqual(roy.owner, read_lease(s3, "roy")[0]["owner"])
            self.assertEqual(before, s3.objects[binding.LOCK_KEY])

    def test_foreign_cas_generation_is_not_retried_or_overwritten(self):
        s3, roy, before = self.fixture()
        foreign = ScopedImageLease(s3, owner="c" * 32, project="roy", now=lambda: NOW)
        def race(key):
            s3.before_put = None
            foreign.acquire()
        s3.before_put = race
        with self.assertRaises(StoreError):
            roy.acquire()
        with self.assertRaisesRegex(binding.BindingError, "lease-lost"):
            roy.retain_uncertain()
        self.assertEqual(foreign.owner, read_lease(s3, "roy")[0]["owner"])
        self.assertEqual(before, s3.objects[binding.LOCK_KEY])


if __name__ == "__main__":
    unittest.main()
