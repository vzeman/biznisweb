#!/usr/bin/env python3
"""
Shared project/runtime configuration helpers for multi-client reporting flows.
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[1]
PROJECTS_DIR = ROOT_DIR / "projects"
ROOT_DATA_DIR = ROOT_DIR / "data"
BASE_DEFAULT_PROJECT = os.getenv("REPORT_PROJECT", "vevo").strip() or "vevo"
DEFAULT_CLOUDWATCH_NAMESPACE = "BizniswebReporting"


@dataclass(frozen=True)
class ReportingProjectContext:
    project_name: str
    project_dir: Path
    data_dir: Path
    settings: Dict[str, Any]
    reporting_defaults: Dict[str, Any]


def project_dir(project_name: str) -> Path:
    return PROJECTS_DIR / project_name


def project_data_dir(project_name: str) -> Path:
    data_dir = ROOT_DATA_DIR / project_name
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def load_project_env(project_name: str, logger: Optional[Any] = None) -> None:
    """
    Load per-project env file from projects/<project>/.env if it exists.
    Values override root .env so project configs remain isolated.
    """
    if os.getenv("REPORT_SKIP_PROJECT_ENV", "").strip().lower() in {"1", "true", "yes", "y", "on"}:
        _log(logger, "Skipping project .env load (REPORT_SKIP_PROJECT_ENV=true)")
        return

    env_path = project_dir(project_name) / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True, encoding="utf-8-sig")
        _log(logger, f"Loaded project env: {env_path}")
        return

    if project_name != BASE_DEFAULT_PROJECT:
        raise FileNotFoundError(
            f"Project env file not found: {env_path}. "
            f"Create projects/{project_name}/.env from projects/{project_name}/.env.example."
        )

    _log(logger, f"Project env not found for default project '{project_name}' ({env_path}), using current environment")


def load_project_settings(project_name: str) -> Dict[str, Any]:
    settings_path = project_dir(project_name) / "settings.json"
    if not settings_path.exists():
        return {}
    return _load_json_file(settings_path)


def build_project_context(project_name: str, settings: Optional[Dict[str, Any]] = None) -> ReportingProjectContext:
    loaded_settings = settings or load_project_settings(project_name)
    return ReportingProjectContext(
        project_name=project_name,
        project_dir=project_dir(project_name),
        data_dir=project_data_dir(project_name),
        settings=loaded_settings,
        reporting_defaults=resolve_reporting_defaults(project_name, loaded_settings),
    )


def get_project_display_name(project_name: str, settings: Optional[Dict[str, Any]] = None) -> str:
    settings = settings or {}
    raw_name = (
        str(settings.get("project_display_name", "")).strip()
        or str(settings.get("brand_name", "")).strip()
        or project_name
    )
    return raw_name[:1].upper() + raw_name[1:]


def resolve_biznisweb_api_url(project_name: str, settings: Optional[Dict[str, Any]] = None) -> str:
    settings = settings or {}
    api_url = os.getenv("BIZNISWEB_API_URL", "").strip() or str(settings.get("biznisweb_api_url", "")).strip()
    if not api_url:
        raise ValueError(
            f"BIZNISWEB_API_URL not found for project '{project_name}'. "
            f"Set it in projects/{project_name}/settings.json or environment variables."
        )
    return api_url


def derive_biznisweb_base_url(api_url: str) -> str:
    parsed = urlparse(api_url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"Invalid BizniWeb API URL: {api_url}")
    return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")


def resolve_reporting_defaults(project_name: str, settings: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    settings = settings or {}
    display_name = get_project_display_name(project_name, settings)
    reporting_system_name = (
        str(settings.get("reporting_system_name", "")).strip()
        or f"{display_name} reporting"
    )
    return {
        "project_name": project_name,
        "display_name": display_name,
        "reporting_system_name": reporting_system_name,
        "email_subject": (
            str(settings.get("report_email_subject", "")).strip()
            or f"Daily {display_name} report"
        ),
        "ses_configuration_set": str(settings.get("ses_configuration_set", "")).strip(),
        "cloudwatch_namespace": (
            str(settings.get("cloudwatch_namespace", "")).strip()
            or DEFAULT_CLOUDWATCH_NAMESPACE
        ),
        "enable_email_strategy_report": bool(settings.get("enable_email_strategy_report", False)),
    }


def _load_json_file(path: Path) -> Dict[str, Any]:
    import json

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f) or {}


def _log(logger: Optional[Any], message: str) -> None:
    if logger is None:
        print(message)
        return
    if hasattr(logger, "info"):
        logger.info(message)
    else:
        print(message)
