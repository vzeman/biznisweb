#!/usr/bin/env python3
"""
Minimal read-only live dashboard server for BizniWeb reporting.

Serves the latest generated HTML report per project and exposes the latest
dashboard JSON snapshot for future frontend work.
"""

from __future__ import annotations

import argparse
import json
from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

from reporting_core import project_data_dir


ROOT_DIR = Path(__file__).resolve().parent


def available_projects() -> List[str]:
    projects_root = ROOT_DIR / "projects"
    return sorted(
        entry.name
        for entry in projects_root.iterdir()
        if entry.is_dir() and (entry / "settings.json").exists()
    )


def _latest_non_tagged_file(data_dir: Path, prefix: str, suffix: str) -> Optional[Path]:
    candidates = sorted(
        (
            path for path in data_dir.glob(f"{prefix}*{suffix}")
            if "__" not in path.stem
        ),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def resolve_latest_report_path(project: str) -> Optional[Path]:
    data_dir = project_data_dir(project)
    latest = data_dir / "report_latest.html"
    if latest.exists():
        return latest
    return _latest_non_tagged_file(data_dir, "report_", ".html")


def resolve_latest_payload_path(project: str) -> Optional[Path]:
    data_dir = project_data_dir(project)
    latest = data_dir / "dashboard_payload_latest.json"
    if latest.exists():
        return latest
    return _latest_non_tagged_file(data_dir, "dashboard_payload_", ".json")


def build_index_html(projects: List[str]) -> str:
    cards = []
    for project in projects:
        report_path = resolve_latest_report_path(project)
        payload_path = resolve_latest_payload_path(project)
        cards.append(
            "<article class='card'>"
            f"<h2>{escape(project)}</h2>"
            f"<p>HTML report: {'ready' if report_path else 'missing'}</p>"
            f"<p>JSON payload: {'ready' if payload_path else 'missing'}</p>"
            f"<p><a href='/dashboard/{escape(project)}'>Open live dashboard</a></p>"
            f"<p><a href='/api/{escape(project)}/latest'>Open latest JSON</a></p>"
            "</article>"
        )
    cards_html = "".join(cards) or "<p>No reporting projects found.</p>"
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>BizniWeb Live Dashboards</title>
  <style>
    :root {{
      --bg: #f4efe7;
      --card: #fffaf3;
      --text: #1f1a17;
      --muted: #7d6c5e;
      --line: #e7d7c6;
      --accent: #b65a2a;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      background: radial-gradient(circle at top, #fffaf4 0%, var(--bg) 70%);
      color: var(--text);
    }}
    main {{
      max-width: 1100px;
      margin: 0 auto;
      padding: 40px 20px 60px;
    }}
    h1 {{ margin: 0 0 10px; font-size: 40px; }}
    p.lead {{ margin: 0 0 30px; color: var(--muted); }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 18px;
    }}
    .card {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 22px;
      box-shadow: 0 10px 30px rgba(95, 62, 38, 0.06);
    }}
    .card h2 {{ margin: 0 0 12px; }}
    .card p {{ margin: 8px 0; }}
    a {{ color: var(--accent); text-decoration: none; font-weight: 700; }}
  </style>
</head>
<body>
  <main>
    <h1>BizniWeb Live Dashboards</h1>
    <p class="lead">Existing nightly email reporting stays unchanged. This server only exposes the latest generated outputs online.</p>
    <section class="grid">{cards_html}</section>
  </main>
</body>
</html>"""


class LiveDashboardHandler(BaseHTTPRequestHandler):
    server_version = "BizniWebLiveDashboard/1.0"

    def _send_bytes(self, body: bytes, *, content_type: str, status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, payload: Dict[str, object], status: int = 200) -> None:
        self._send_bytes(
            json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
            content_type="application/json; charset=utf-8",
            status=status,
        )

    def _send_text(self, text: str, *, content_type: str, status: int = 200) -> None:
        self._send_bytes(text.encode("utf-8"), content_type=content_type, status=status)

    def log_message(self, format: str, *args: object) -> None:
        return

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        if path == "/health":
            self._send_json({"ok": True, "projects": available_projects()})
            return
        if path == "/":
            self._send_text(build_index_html(available_projects()), content_type="text/html; charset=utf-8")
            return
        if path == "/api/projects":
            self._send_json({"projects": available_projects()})
            return

        parts = [part for part in path.split("/") if part]
        if len(parts) == 3 and parts[0] == "api" and parts[2] == "latest":
            project = parts[1]
            if project not in available_projects():
                self._send_json({"error": f"Unknown project '{project}'."}, status=404)
                return
            payload_path = resolve_latest_payload_path(project)
            if payload_path is None or not payload_path.exists():
                self._send_json({"error": f"No latest dashboard payload found for '{project}'."}, status=404)
                return
            self._send_bytes(
                payload_path.read_bytes(),
                content_type="application/json; charset=utf-8",
            )
            return

        if len(parts) == 2 and parts[0] == "dashboard":
            project = parts[1]
            if project not in available_projects():
                self._send_text(f"Unknown project '{escape(project)}'.", content_type="text/plain; charset=utf-8", status=404)
                return
            report_path = resolve_latest_report_path(project)
            if report_path is None or not report_path.exists():
                self._send_text(
                    f"No latest HTML report found for '{escape(project)}'.",
                    content_type="text/plain; charset=utf-8",
                    status=404,
                )
                return
            self._send_bytes(report_path.read_bytes(), content_type="text/html; charset=utf-8")
            return

        self._send_text("Not found.", content_type="text/plain; charset=utf-8", status=404)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Serve latest BizniWeb live dashboards")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host")
    parser.add_argument("--port", type=int, default=8787, help="Bind port")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    server = ThreadingHTTPServer((args.host, args.port), LiveDashboardHandler)
    print(f"Serving BizniWeb live dashboards on http://{args.host}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
