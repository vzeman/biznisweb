import argparse
import json
import re
import subprocess
import tomllib
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
PROJECT = ROOT / "content" / "VEVO_CONTENT"
README = PROJECT / "README_DEV.md"
STATE = PROJECT / "PROJECT_STATE.md"
EXPORTS = PROJECT / "exports"
IMPORTS = PROJECT / "imports"
MCP_SERVER = PROJECT / "tools" / "biznisweb_vevo_content_mcp.py"
MCP_PUBLISHER = IMPORTS / "publish_vevo_batch_via_content_mcp.py"
CHECK_SCRIPT = PROJECT / "scripts" / "check.ps1"
LEGACY_BRANCH = "opan-claw"
CONTENT_BRANCH_PREFIX = "codex/vevo-content-"


REQUIRED_FILES = [
    PROJECT / ".env.example",
    PROJECT / ".env.required",
    README,
    STATE,
    PROJECT / "content-plan" / "vevo-5000-content-plan.md",
    PROJECT / "tools" / "vevo_duplicate_guard.py",
    PROJECT / "tools" / "vevo_public_content_guard.py",
    PROJECT / "tools" / "vevo_article_depth_guard.py",
    PROJECT / "tools" / "vevo_html_safety_guard.py",
    MCP_SERVER,
    MCP_PUBLISHER,
    CHECK_SCRIPT,
    PROJECT / "tests" / "test_content_guards.py",
    PROJECT / "tests" / "test_vevo_content_mcp.py",
    PROJECT / "tests" / "test_vevo_content_mcp_publisher.py",
    PROJECT / "workflows" / "mcp-publication.md",
]

LIVE_MUTATION_MARKERS = (
    "biznisweb-add_news_post",
    "biznisweb-update_news_post",
    "biznisweb-delete_news_post",
    "vevo_add_news_post",
    "vevo_update_news_post",
    "vevo_delete_news_post",
    "/erp/pages/news/addcheck/",
    "/erp/pages/news/editcheck/",
)
LIVE_CONFIRMATION_FLAGS = (
    "--execute-live",
    "--update-live",
    "--publish",
    "--allow-unsafe-mcp-publish",
)
MCP_SERVER_GUARD_MARKERS = (
    "New VEVO posts must be created hidden",
    "confirm_visible=true",
    "delete requires confirm_delete=true",
    "DUPLICATE_SCAN_LIMIT",
    "assert_post_readback",
)


def run_git(args):
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
            timeout=20,
        )
    except Exception as exc:
        return {"ok": False, "stdout": "", "stderr": str(exc)}
    return {
        "ok": result.returncode == 0,
        "stdout": result.stdout.strip(),
        "stderr": result.stderr.strip(),
        "returncode": result.returncode,
    }


def is_supported_branch(branch_name):
    return branch_name == LEGACY_BRANCH or branch_name.startswith(CONTENT_BRANCH_PREFIX)


def inspect_local_mcp_registration():
    config_path = Path.home() / ".codex" / "config.toml"
    result = {
        "config_path": str(config_path),
        "configured": False,
        "command_exists": False,
        "script_exists": False,
        "script_matches_repo": False,
        "env_file_configured": False,
        "env_file_exists": False,
    }
    if not config_path.is_file():
        return result
    try:
        config = tomllib.loads(config_path.read_text(encoding="utf-8"))
        server = (config.get("mcp_servers") or {}).get("biznisweb-vevo-content")
        if not isinstance(server, dict):
            return result
        result["configured"] = True
        command = Path(str(server.get("command") or ""))
        args = server.get("args") if isinstance(server.get("args"), list) else []
        script_value = next(
            (str(value) for value in reversed(args) if str(value).lower().endswith(".py")),
            "",
        )
        script = Path(script_value) if script_value else None
        env_value = str((server.get("env") or {}).get("VEVO_CONTENT_ENV_FILE") or "")
        result["command_exists"] = command.is_file()
        result["script_exists"] = bool(script and script.is_file())
        result["script_matches_repo"] = bool(
            script and script.resolve() == MCP_SERVER.resolve()
        )
        result["env_file_configured"] = bool(env_value)
        result["env_file_exists"] = bool(env_value and Path(env_value).is_file())
        return result
    except Exception as exc:
        result["error"] = str(exc)
        return result


def latest_batch_number():
    numbers = []
    for path in IMPORTS.glob("batch-*-*-articles.json"):
        match = re.match(r"batch-(\d+)-", path.name)
        if match:
            numbers.append(int(match.group(1)))
    return max(numbers) if numbers else None


def load_json(path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"_load_error": str(exc)}


def analyze_publication_verify(batch):
    candidates = sorted(EXPORTS.glob(f"batch-{batch}-*-publication*.json"))
    if not candidates:
        candidates = sorted(EXPORTS.glob(f"batch-{batch}-*-rich-html-results.json"))
    if not candidates:
        return {"status": "missing", "files": []}

    summaries = []
    for path in candidates:
        data = load_json(path)
        if isinstance(data, dict) and data.get("_load_error"):
            summaries.append({"file": str(path.relative_to(ROOT)), "status": "invalid_json", "error": data["_load_error"]})
            continue

        articles = data.get("articles") if isinstance(data, dict) else None
        if articles is None and isinstance(data, dict):
            articles = data.get("records") or data.get("results") or data.get("posts")
        if articles is None and isinstance(data, list):
            articles = data

        ok_values = []
        if isinstance(articles, list):
            for item in articles:
                if isinstance(item, dict):
                    if "ok" in item:
                        ok_values.append(bool(item.get("ok")))
                    elif "verification" in item and isinstance(item["verification"], dict) and "ok" in item["verification"]:
                        ok_values.append(bool(item["verification"].get("ok")))

        if isinstance(data, dict) and "all_ok" in data:
            all_ok = bool(data["all_ok"])
        elif ok_values:
            all_ok = all(ok_values)
        else:
            all_ok = None

        ok_count = sum(1 for value in ok_values if value)
        if not ok_values and isinstance(data, dict) and isinstance(data.get("public_ok_count"), int):
            ok_count = data["public_ok_count"]

        summaries.append(
            {
                "file": str(path.relative_to(ROOT)),
                "status": "checked",
                "article_count": len(articles) if isinstance(articles, list) else None,
                "ok_count": ok_count,
                "all_ok": all_ok,
            }
        )
    return {"status": "present", "files": summaries}


def audit():
    findings = []
    recommendations = []

    for path in REQUIRED_FILES:
        if not path.exists():
            findings.append({"severity": "block", "code": "missing_required_file", "path": str(path.relative_to(ROOT))})

    branch = run_git(["branch", "--show-current"])
    remote = run_git(["remote", "-v"])
    upstream = run_git(["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"])
    status = run_git(["status", "--short", "--", "content/VEVO_CONTENT"])

    if not is_supported_branch(branch.get("stdout", "")):
        findings.append({"severity": "warn", "code": "unexpected_branch", "branch": branch.get("stdout")})
    if not remote.get("stdout"):
        findings.append({"severity": "warn", "code": "no_git_remote", "message": "Push is blocked until a remote is configured."})
    if not upstream.get("ok"):
        findings.append({"severity": "warn", "code": "no_upstream", "message": "git pull --rebase cannot work without branch upstream."})
    if status.get("stdout"):
        findings.append({"severity": "info", "code": "vevo_worktree_not_clean", "status": status.get("stdout")})

    readme_text = README.read_text(encoding="utf-8") if README.exists() else ""
    state_text = STATE.read_text(encoding="utf-8") if STATE.exists() else ""

    outdated_markers = [
        "dated before `2025-10-12`",
        "XLS import for slug/date",
        "API update for final rich HTML",
    ]
    stale = [marker for marker in outdated_markers if marker in readme_text]
    if stale:
        findings.append({"severity": "warn", "code": "readme_outdated_publish_rules", "markers": stale})

    required_state_markers = [
        "Legacy remote VEVO MCP",
        "biznisweb-vevo-content",
        "Batch 37 is complete",
    ]
    missing_state = [marker for marker in required_state_markers if marker not in state_text]
    if missing_state:
        findings.append({"severity": "warn", "code": "project_state_missing_known_guard", "markers": missing_state})

    latest_batch = latest_batch_number()
    latest_verify = analyze_publication_verify(latest_batch) if latest_batch else {"status": "missing"}
    if latest_batch and latest_verify["status"] == "missing":
        findings.append({"severity": "warn", "code": "latest_batch_missing_public_verify", "batch": latest_batch})
    elif latest_batch:
        if not any(item.get("all_ok") is True for item in latest_verify.get("files", [])):
            findings.append({"severity": "warn", "code": "latest_batch_without_all_ok_file", "batch": latest_batch})

    local_mcp_registration = inspect_local_mcp_registration()
    if not local_mcp_registration["configured"]:
        findings.append(
            {
                "severity": "warn",
                "code": "local_vevo_content_mcp_not_registered",
            }
        )
    elif not all(
        local_mcp_registration[key]
        for key in ("command_exists", "script_exists", "script_matches_repo", "env_file_exists")
    ):
        findings.append(
            {
                "severity": "warn",
                "code": "local_vevo_content_mcp_registration_invalid",
                "registration": local_mcp_registration,
            }
        )

    unsafe_publisher = IMPORTS / "publish_batch_35_via_mcp.py"
    if unsafe_publisher.exists():
        text = unsafe_publisher.read_text(encoding="utf-8")
        if "--allow-unsafe-mcp-publish" not in text:
            findings.append({"severity": "block", "code": "unsafe_mcp_publisher_without_escape_hatch", "path": str(unsafe_publisher.relative_to(ROOT))})

    if CHECK_SCRIPT.exists():
        check_script_text = CHECK_SCRIPT.read_text(encoding="utf-8-sig")
        if "$LASTEXITCODE" not in check_script_text or "Invoke-PythonChecked" not in check_script_text:
            findings.append(
                {
                    "severity": "block",
                    "code": "native_check_exit_codes_not_enforced",
                    "path": str(CHECK_SCRIPT.relative_to(ROOT)),
                }
            )

    live_mutation_entrypoints = []
    for directory in (IMPORTS, PROJECT / "scripts", PROJECT / "tools"):
        for path in sorted(directory.glob("*.py")):
            if path.resolve() == Path(__file__).resolve():
                continue
            text = path.read_text(encoding="utf-8")
            if not any(marker in text for marker in LIVE_MUTATION_MARKERS):
                continue
            if path.resolve() == MCP_SERVER.resolve():
                missing_guards = [marker for marker in MCP_SERVER_GUARD_MARKERS if marker not in text]
                explicit_flags = ["mcp-hidden-first-and-confirmation-guards"] if not missing_guards else []
                guard_type = "mcp_server"
            else:
                explicit_flags = [flag for flag in LIVE_CONFIRMATION_FLAGS if flag in text]
                missing_guards = []
                guard_type = "cli"
            record = {
                "path": str(path.relative_to(ROOT)),
                "explicit_flags": explicit_flags,
                "guarded": bool(explicit_flags),
                "guard_type": guard_type,
                "missing_guards": missing_guards,
            }
            live_mutation_entrypoints.append(record)
            if not explicit_flags:
                findings.append(
                    {
                        "severity": "block",
                        "code": "unguarded_live_mutation_script",
                        "path": record["path"],
                    }
                )

    live_catalog = {"status": "not_checked"}
    try:
        from vevo_duplicate_guard import catalog_health, fetch_rss_existing

        live_rows = fetch_rss_existing()
        live_catalog = {"status": "checked", "record_count": len(live_rows), **catalog_health(live_rows)}
        if live_catalog["duplicate_title_group_count"]:
            findings.append(
                {
                    "severity": "warn",
                    "code": "live_duplicate_titles",
                    "count": live_catalog["duplicate_title_group_count"],
                }
            )
        if live_catalog["bad_slug_count"]:
            findings.append(
                {
                    "severity": "warn",
                    "code": "live_invalid_slugs",
                    "count": live_catalog["bad_slug_count"],
                }
            )
    except Exception as exc:
        findings.append({"severity": "warn", "code": "live_catalog_audit_failed", "message": str(exc)})
        live_catalog = {"status": "failed", "error": str(exc)}

    if not remote.get("stdout"):
        recommendations.append("Configure the canonical Git remote before changing VEVO content.")
    if not upstream.get("ok"):
        recommendations.append("Configure an upstream for the current VEVO content branch before continuing multi-PC work.")

    recommendations.extend(
        [
            "Keep README_DEV.md aligned with PROJECT_STATE.md after every publishing workflow change.",
            "Keep the legacy remote VEVO MCP add/update tools blocked; use only the repo-local biznisweb-vevo-content helper after its hidden smoke passes.",
            "For every new batch, require duplicate guard, link preflight, public wording guard, depth guard, HTML safety guard, and public URL verification.",
            "Consider archiving old import/export artifacts with legacy mojibake or superseded workflows under a clearly named legacy folder after verifying they are no longer operational inputs.",
            "Keep every script that can mutate live news behind an explicit opt-in flag and prefer read-only verification as its default behavior.",
        ]
    )
    if live_catalog.get("duplicate_title_group_count") or live_catalog.get("bad_slug_count"):
        recommendations.append(
            "Resolve the duplicate titles or invalid public slugs listed by the live catalog audit after confirming canonical post IDs in admin."
        )

    return {
        "project": "VEVO_CONTENT",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "repo": str(ROOT),
        "branch": branch.get("stdout"),
        "latest_batch": latest_batch,
        "latest_publication_verify": latest_verify,
        "local_mcp_registration": local_mcp_registration,
        "live_mutation_entrypoint_count": len(live_mutation_entrypoints),
        "live_mutation_entrypoints": live_mutation_entrypoints,
        "live_catalog": live_catalog,
        "finding_count": len(findings),
        "block_count": sum(1 for item in findings if item["severity"] == "block"),
        "warn_count": sum(1 for item in findings if item["severity"] == "warn"),
        "findings": findings,
        "recommendations": recommendations,
    }


def main():
    parser = argparse.ArgumentParser(description="Audit VEVO_CONTENT project hygiene.")
    parser.add_argument("--report", type=Path, help="Write JSON report to this path.")
    args = parser.parse_args()

    report = audit()
    text = json.dumps(report, ensure_ascii=False, indent=2)
    print(text)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(text + "\n", encoding="utf-8")
    if report["block_count"]:
        raise SystemExit("VEVO project audit found blockers")


if __name__ == "__main__":
    main()
