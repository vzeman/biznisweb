#!/usr/bin/env python3
"""Fail before AWS credentials if an ordinary workflow could wake suspended Preview."""
import argparse
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--environment", required=True, choices=("preview", "production"))
    args = parser.parse_args()
    if args.environment == "production":
        return
    state = json.loads(Path("projects/vevo/growthbook_preview_lifecycle.json").read_text(encoding="utf-8"))
    if state.get("desired_state") != "active" or state.get("ordinary_preview_deploy_allowed") is not True:
        raise SystemExit("Preview intentionally suspended: reviewed lifecycle resume required before any ordinary Preview workflow")


if __name__ == "__main__":
    main()
