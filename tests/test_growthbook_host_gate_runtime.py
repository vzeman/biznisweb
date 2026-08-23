from __future__ import annotations

import copy
import pathlib
import tempfile
import unittest

from scripts.resolve_growthbook_host_gate_runtime import (
    HostGateRuntimeError,
    resolve_host_gate_runtime,
    write_env_file,
)


CLUSTER_ARN = (
    "arn:aws:ecs:eu-central-1:919341186960:"
    "cluster/vevo-growthbook-collector-preview"
)
TASK_DEFINITION_ARN = (
    "arn:aws:ecs:eu-central-1:919341186960:"
    "task-definition/vevo-growthbook-collector-preview:2"
)
IMAGE_DIGEST = "sha256:" + "a" * 64
TASK_ID = "b" * 32
LOG_STREAM = f"collector/collector/{TASK_ID}"


def _task_state(*, log_stream: str | None = LOG_STREAM) -> dict[str, object]:
    container = {
        "name": "collector",
        "exitCode": 0,
        "imageDigest": IMAGE_DIGEST,
    }
    if log_stream is not None:
        container["logStreamName"] = log_stream
    return {
        "failures": [],
        "tasks": [
            {
                "taskArn": CLUSTER_ARN.replace(":cluster/", ":task/") + f"/{TASK_ID}",
                "clusterArn": CLUSTER_ARN,
                "taskDefinitionArn": TASK_DEFINITION_ARN,
                "desiredStatus": "STOPPED",
                "lastStatus": "STOPPED",
                "containers": [container],
                "attachments": [
                    {
                        "details": [
                            {"name": "privateIPv4Address", "value": "172.31.9.10"}
                        ]
                    }
                ],
            }
        ],
    }


def _task_definition() -> dict[str, object]:
    return {
        "taskDefinition": {
            "taskDefinitionArn": TASK_DEFINITION_ARN,
            "containerDefinitions": [
                {
                    "name": "collector",
                    "image": f"example.invalid/collector@{IMAGE_DIGEST}",
                    "logConfiguration": {
                        "logDriver": "awslogs",
                        "options": {
                            "awslogs-group": "/ecs/vevo-growthbook-collector-preview",
                            "awslogs-region": "eu-central-1",
                            "awslogs-stream-prefix": "collector",
                        },
                    },
                }
            ],
        }
    }


def _resolve(task_state: dict[str, object]) -> dict[str, str]:
    return resolve_host_gate_runtime(
        task_state,
        _task_definition(),
        expected_cluster_arn=CLUSTER_ARN,
        expected_task_definition_arn=TASK_DEFINITION_ARN,
        expected_container_name="collector",
        expected_log_group="/ecs/vevo-growthbook-collector-preview",
        expected_log_region="eu-central-1",
        expected_log_prefix="collector",
        expected_image_digest=IMAGE_DIGEST,
        expected_private_cidr="172.31.0.0/16",
    )


class GrowthBookHostGateRuntimeTests(unittest.TestCase):
    def test_accepts_exact_ecs_log_stream(self) -> None:
        runtime = _resolve(_task_state())
        self.assertEqual(LOG_STREAM, runtime["log_stream"])
        self.assertEqual("exact_ecs", runtime["log_stream_source"])

    def test_accepts_only_absent_optional_ecs_log_stream(self) -> None:
        runtime = _resolve(_task_state(log_stream=None))
        self.assertEqual(LOG_STREAM, runtime["log_stream"])
        self.assertEqual(
            "optional_absent_taskdef_bound", runtime["log_stream_source"]
        )

    def test_rejects_nonempty_contradictory_ecs_log_stream(self) -> None:
        with self.assertRaisesRegex(HostGateRuntimeError, "contradicts task definition"):
            _resolve(_task_state(log_stream="collector/collector/" + "c" * 32))

    def test_rejects_runtime_or_log_boundary_drift(self) -> None:
        wrong_ip = _task_state()
        wrong_ip["tasks"][0]["attachments"][0]["details"][0]["value"] = "8.8.8.8"
        with self.assertRaisesRegex(HostGateRuntimeError, "private IP boundary"):
            _resolve(wrong_ip)

        wrong_digest = _task_state()
        wrong_digest["tasks"][0]["containers"][0]["imageDigest"] = "sha256:" + "c" * 64
        with self.assertRaisesRegex(HostGateRuntimeError, "image digest"):
            _resolve(wrong_digest)

        wrong_definition = _task_definition()
        wrong_definition["taskDefinition"]["containerDefinitions"][0][
            "logConfiguration"
        ]["options"]["awslogs-stream-prefix"] = "drift"
        with self.assertRaisesRegex(HostGateRuntimeError, "awslogs boundary"):
            resolve_host_gate_runtime(
                _task_state(),
                wrong_definition,
                expected_cluster_arn=CLUSTER_ARN,
                expected_task_definition_arn=TASK_DEFINITION_ARN,
                expected_container_name="collector",
                expected_log_group="/ecs/vevo-growthbook-collector-preview",
                expected_log_region="eu-central-1",
                expected_log_prefix="collector",
                expected_image_digest=IMAGE_DIGEST,
                expected_private_cidr="172.31.0.0/16",
            )

    def test_writes_only_sanitized_shell_environment(self) -> None:
        runtime = _resolve(_task_state(log_stream=None))
        with tempfile.TemporaryDirectory() as temporary_directory:
            output = pathlib.Path(temporary_directory) / "runtime.env"
            write_env_file(output, env_prefix="SOURCE_HOST", runtime=runtime)
            self.assertEqual(
                (
                    f"SOURCE_HOST_TASK_ID={TASK_ID}\n"
                    "SOURCE_HOST_PRIVATE_IP=172.31.9.10\n"
                    f"SOURCE_HOST_LOG_STREAM={LOG_STREAM}\n"
                    "SOURCE_HOST_LOG_STREAM_SOURCE=optional_absent_taskdef_bound\n"
                ),
                output.read_text(encoding="utf-8"),
            )

    def test_rejects_ambiguous_task_readback(self) -> None:
        ambiguous = _task_state()
        ambiguous["tasks"].append(copy.deepcopy(ambiguous["tasks"][0]))
        with self.assertRaisesRegex(HostGateRuntimeError, "exactly one"):
            _resolve(ambiguous)


if __name__ == "__main__":
    unittest.main()
