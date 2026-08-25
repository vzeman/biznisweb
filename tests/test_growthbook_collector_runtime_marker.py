from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from growthbook_collector.runtime_marker import RegistryMarkerError, build_registry_marker


class GrowthBookCollectorRuntimeMarkerTests(unittest.TestCase):
    def _write(self, payload: object) -> Path:
        handle = tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False)
        self.addCleanup(Path(handle.name).unlink, missing_ok=True)
        with handle:
            json.dump(payload, handle, separators=(",", ":"), sort_keys=True)
        return Path(handle.name)

    def test_marker_binds_raw_registry_and_sorted_environment_keys(self) -> None:
        path = self._write(
            {
                "schema_version": 1,
                "environments": {
                    "preview": {},
                    "production": {"z": {}, "a": {}},
                },
            }
        )
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        self.assertEqual(
            f"COLLECTOR_REGISTRY_OK:production:{digest}:a,z",
            build_registry_marker(path, "production"),
        )

    def test_marker_rejects_missing_environment(self) -> None:
        path = self._write({"schema_version": 1, "environments": {"preview": {}}})
        with self.assertRaisesRegex(RegistryMarkerError, "environment is missing"):
            build_registry_marker(path, "production")

    def test_marker_rejects_schema_drift(self) -> None:
        path = self._write({"schema_version": 2, "environments": {"production": {}}})
        with self.assertRaisesRegex(RegistryMarkerError, "schema drift"):
            build_registry_marker(path, "production")
