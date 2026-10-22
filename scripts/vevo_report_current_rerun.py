#!/usr/bin/env python3
"""Regenerate VEVO reports using the exact active immutable task definition.

Run only from clean merged main. ``--commit`` identifies this controller;
``--source-commit`` identifies the already deployed image, which may be older.
Neither a new image nor a task definition is registered. Email, invoices and the
inline credit-note guard are skipped; send verified artifacts separately once.

Usage (supply the full existing task-definition ARN and immutable image URI)::

    python scripts/vevo_report_current_rerun.py run --profile codex \
      --commit <controller-main-sha> --source-commit <deployed-image-sha> \
      --expected-definition <active-task-definition-arn> \
      --expected-image <repository@sha256:digest> --to-date YYYY-MM-DD
    python scripts/vevo_report_current_rerun.py status --profile codex \
      --release-id <printed-id>

The existing CAS lease, peer/output protection and 120-second dispatch quiet
window exclude competing work. The host verifies its identity and localhost
marker before providers, performs a fresh export and publishes all four periods.
Success verifies the new generation and restores the unchanged dynamic schedule.
An uncertain dispatch or attempted live failure retains the lease and paused
schedule for review; never blindly repeat ``run`` after such an outcome.
"""
from __future__ import annotations

import argparse
import copy
from datetime import date
import json
from pathlib import Path
import re
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import vevo_report_image_release as release  # noqa: E402


def controller_source(controller_commit, source_commit):
    release.exact_source(controller_commit)
    release.require(re.fullmatch(r"[a-f0-9]{40}", source_commit or ""), "current-rerun-source-invalid")
    result = subprocess.run(["git", "merge-base", "--is-ancestor", source_commit, controller_commit],
                            cwd=ROOT, capture_output=True, timeout=45)
    release.require(result.returncode == 0, "current-rerun-source-not-ancestor")


def validate_current(schedule, definition, expected_definition, expected_image):
    """Reject stale identities and financial/control overrides before any write."""
    release.require(re.fullmatch(
        rf"arn:aws:ecs:{release.REGION}:{release.ACCOUNT}:task-definition/{release.VEVO.family}:[1-9][0-9]*",
        expected_definition or ""), "current-rerun-definition-argument")
    release.require(schedule.get("Name") == release.VEVO.service
                    and schedule.get("State") == "ENABLED"
                    and schedule.get("Target", {}).get("Arn") == release.CLUSTER
                    and schedule["Target"].get("EcsParameters", {}).get("LaunchType") == "FARGATE"
                    and schedule["Target"]["EcsParameters"].get("TaskDefinitionArn") == expected_definition,
                    "current-rerun-schedule-identity")
    release.require(definition.get("taskDefinitionArn") == expected_definition
                    and definition.get("status") == "ACTIVE", "current-rerun-definition-identity")
    release.image_only_definition(definition, expected_image)
    release.require(definition["containerDefinitions"][0]["image"] == expected_image,
                    "current-rerun-image-drift")
    schedule_input = json.loads(schedule["Target"].get("Input", "{}"))
    release.require(isinstance(schedule_input, dict)
                    and set(schedule_input).issubset({"containerOverrides"}), "current-rerun-override-scope")
    overrides = schedule_input.get("containerOverrides", [])
    release.require(isinstance(overrides, list) and len(overrides) <= 1
                    and all(isinstance(row, dict) and row.get("name") == "reporting"
                            and set(row).issubset({"name", "environment"}) for row in overrides),
                    "current-rerun-container-override")
    effective = release.binding.unique_environment(definition["containerDefinitions"][0].get("environment", []))
    forced = {"REPORT_PROJECT", "REPORT_SKIP_EMAIL", "REPORT_SKIP_INVOICES",
              "REPORT_SKIP_CREDITNOTE_STORNO_GUARD", "REPORT_TO_DATE",
              "REPORT_FORCE_NO_CACHE", "REPORT_FORCE_CLEAR_CACHE"}
    for row in overrides:
        values = release.binding.unique_environment(row.get("environment", []))
        release.require(all(key in forced or effective.get(key) == value for key, value in values.items()),
                        "current-rerun-unpreserved-schedule-environment")
        effective.update(values)
    release.require(effective.get("REPORT_PROJECT") == "vevo"
                    and effective.get("REPORT_S3_BUCKET") == release.BUCKET
                    and effective.get("REPORT_S3_PREFIX") == release.VEVO.sink,
                    "current-rerun-project-or-sink")
    release.require(effective.get("REPORT_SKIP_INVOICES") == "true"
                    and effective.get("REPORT_SKIP_CREDITNOTE_STORNO_GUARD") == "true",
                    "current-rerun-financial-boundary")
    return effective


class CurrentRerun(release.ImageRelease):
    def __init__(self, session, controller_commit, source_commit, to_date, timeout, *,
                 expected_definition, expected_image, release_id=None):
        super().__init__(session, source_commit, to_date, timeout, release_id=release_id)
        self.controller_commit = controller_commit
        self.expected_definition, self.expected_image = expected_definition, expected_image
        self.receipt_prefix = "data/vevo/reporting/current-reruns/" + self.release_id + "/"

    def event(self, phase, **details):
        super().event(phase, controller_commit=self.controller_commit, runtime_reused=True, **details)

    def verify_definition(self):
        current = self.definition(self.expected_definition)
        release.require(current.get("status") == "ACTIVE"
                        and current.get("taskDefinitionArn") == self.expected_definition
                        and release.binding.definition_snapshot(current)
                        == release.binding.definition_snapshot(self.original_source),
                        "current-rerun-active-definition-drift")

    def checkpoint(self, *, poll=False):
        if poll and self.clock() - self.last_checkpoint < 45:
            return
        super().checkpoint(poll=poll)
        self.verify_definition()

    def run(self):
        controller_source(self.controller_commit, self.commit)
        release.require(self.session.client("sts").get_caller_identity()["Account"] == release.ACCOUNT,
                        "current-rerun-account-invalid")
        release.binding.require_private_bucket(self.s3)
        self.original = self.schedule()
        self.known_schedule = copy.deepcopy(self.original)
        self.original_source = self.definition(self.expected_definition)
        effective = validate_current(self.original, self.original_source, self.expected_definition, self.expected_image)
        self.image, self.build_id = self.exact_image()
        release.require(self.image == self.expected_image, "current-rerun-source-image-mismatch")
        self.gate_sha = release.committed_gate_sha(self.commit)
        self.report_from_date = effective.get("REPORT_FROM_DATE")
        release.require(self.report_from_date
                        and date.fromisoformat(self.report_from_date) <= date.fromisoformat(self.to_date),
                        "current-rerun-history-start-required")
        self.protected = {key: value for key, value in self.all_schedules().items() if key != self.policy.service}
        self.inspect_peer_boundary()
        self.exclusion()
        self.original_outputs = self.live_outputs()
        self.original_generation = json.loads(release.read_private(
            self.s3, self.policy.sink + "/latest/generation.json"))["generation_id"]
        self.no_report_tasks()
        # Candidate is only the existing active definition. No registration,
        # promotion, isolated probe role or image update belongs in this path.
        self.candidate = self.production_definition = self.original_source
        self.event("preflight-complete", schedule=self.original, source_definition=self.original_source,
                   protected_schedules=self.protected, protected_peer=self.peer_snapshot,
                   original_live_outputs=self.original_outputs, image=self.image, build_run_id=self.build_id)
        self.acquire_release_lease()
        try:
            paused = copy.deepcopy(self.original)
            paused["State"] = "DISABLED"
            self.update_owned_schedule(paused, "report-pause")
            for _ in range(12):
                self.no_report_tasks()
                self.checkpoint(poll=True)
                self.sleep(10)
            controller_source(self.controller_commit, self.commit)
            release.require(self.exact_image() == (self.image, self.build_id), "current-rerun-final-image-drift")
            self.start_task("live")
            self.wait_live()
            self.no_report_tasks()
            self.update_owned_schedule(copy.deepcopy(self.original), "report-enable")
            self.checkpoint()
            self.lease.release()
            self.lease_owned = False
            self.event("rerun-complete", final_schedule=self.known_schedule,
                       all_owned_tasks_stopped=True, task_definition_unchanged=True,
                       email_sent=False, financial_runners_skipped=True)
        except BaseException:
            self.recover_failure()
            raise

    def recover_failure(self):
        """Never deregister the active definition, replay a task or release uncertainty."""
        if not self.lease_owned:
            return
        try:
            self.lease._check(owned=True)
            if self.start_uncertain or self.rerun_attempted:
                # update() reads ownership/current schedule before writing. A
                # foreign schedule change must remain untouched.
                paused = copy.deepcopy(self.known_schedule)
                paused["State"] = "DISABLED"
                self.update(paused)
                self.lease.retain_uncertain()
                self.event("rerun-failure-paused-review-required", known_schedule=self.known_schedule,
                           start_uncertain=self.start_uncertain)
                return
            self.checkpoint()
            self.no_report_tasks()
            release.require(self.live_outputs() == self.original_outputs, "current-rerun-pre-dispatch-output-drift")
            self.update_owned_schedule(copy.deepcopy(self.original), "pre-dispatch-restore")
            self.lease.release()
            self.lease_owned = False
            self.event("failed-before-live-restored", final_schedule=self.known_schedule)
        except BaseException:
            if self.lease_owned:
                self.lease.retain_uncertain()
            raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("run", "status"))
    parser.add_argument("--profile")
    parser.add_argument("--commit", help="Clean merged controller HEAD")
    parser.add_argument("--source-commit", help="Source SHA of the existing deployed image")
    parser.add_argument("--expected-definition")
    parser.add_argument("--expected-image")
    parser.add_argument("--to-date")
    parser.add_argument("--release-id")
    parser.add_argument("--timeout-seconds", type=int, default=7200)
    args = parser.parse_args()
    import boto3
    from botocore.config import Config
    session = boto3.Session(profile_name=args.profile, region_name=release.REGION)
    original_client = session.client
    # A run_task outcome is never retried automatically; startedBy and the
    # deterministic client token remain available for explicit investigation.
    session.client = lambda name: original_client(name, config=Config(connect_timeout=10, read_timeout=45,
        retries={"total_max_attempts": 1}))
    release_id = args.release_id or uuid.uuid4().hex
    release.require(re.fullmatch(r"[a-f0-9]{32}", release_id), "current-rerun-id-invalid")
    if args.mode == "status":
        release.require(args.release_id, "current-rerun-id-required")
        s3 = session.client("s3")
        prefix = "data/vevo/reporting/current-reruns/" + release_id + "/"
        keys = [row["Key"] for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=release.BUCKET, Prefix=prefix, ExpectedBucketOwner=release.ACCOUNT)
            for row in page.get("Contents", [])]
        release.require(keys, "current-rerun-receipts-missing")
        value = json.loads(release.read_private(s3, sorted(keys)[-1], 1024 * 1024))
        print(json.dumps({key: value[key] for key in ("release_id", "phase", "source_commit", "controller_commit",
                         "report_to_date", "at", "rerun_attempted", "known_task_arns")}))
        return
    release.require(all((args.commit, args.source_commit, args.expected_definition, args.expected_image, args.to_date))
                    and 300 <= args.timeout_seconds <= 14400, "current-rerun-arguments-invalid")
    date.fromisoformat(args.to_date)
    try:
        CurrentRerun(session, args.commit, args.source_commit, args.to_date, args.timeout_seconds,
                     expected_definition=args.expected_definition, expected_image=args.expected_image,
                     release_id=release_id).run()
    except Exception as exc:
        reason = str(exc)
        if not re.fullmatch(r"(?:current-rerun|image-release|report|runtime)-[a-z0-9-]+", reason):
            reason = type(exc).__name__
        print(json.dumps({"release_id": release_id, "phase": "failed", "reason": reason}), file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
