from __future__ import annotations

import base64
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Mapping, Optional, Type
from urllib.parse import urlsplit

from .handler import CollectorConfig, _response, handle_request, load_registry


HEALTH_MARKER = "VEVO_GROWTHBOOK_COLLECTOR_HEALTH"
HOST_MARKER = "VEVO_GROWTHBOOK_COLLECTOR_HOST_OK"
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8080
RUNTIME_PATH = "/app"
MAX_REJECT_DRAIN_BYTES = 10 * 1024 * 1024


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _configured_port() -> int:
    try:
        port = int(os.environ.get("GROWTHBOOK_COLLECTOR_PORT", str(DEFAULT_PORT)))
    except ValueError as exc:
        raise RuntimeError("GROWTHBOOK_COLLECTOR_PORT must be an integer") from exc
    if not 1024 <= port <= 65535:
        raise RuntimeError("GROWTHBOOK_COLLECTOR_PORT is outside the non-root range")
    return port


def make_request_handler(
    *,
    config: CollectorConfig,
    registry: Mapping[str, Mapping[str, frozenset[str]]],
    s3: Any,
) -> Type[BaseHTTPRequestHandler]:
    class CollectorRequestHandler(BaseHTTPRequestHandler):
        server_version = "vevo-growthbook-collector"
        sys_version = ""

        def log_message(self, _format: str, *_args: Any) -> None:
            # Request logs could expose network metadata. API Gateway keeps the
            # bounded, payload-free access log; the container emits no request data.
            return

        def _write_response(self, response: Mapping[str, Any]) -> None:
            status = int(response["statusCode"])
            body_value = response.get("body", "")
            body = body_value.encode("utf-8") if isinstance(body_value, str) else bytes(body_value)
            self.send_response(status)
            for name, value in dict(response.get("headers") or {}).items():
                self.send_header(str(name), str(value))
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if self.command != "HEAD" and body:
                self.wfile.write(body)

        def _write_local_json(self, status: int, payload: Mapping[str, Any]) -> None:
            body = _json_bytes(payload)
            self.send_response(status)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Security-Policy", "default-src 'none'")
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def _collector_event(self, method: str, body: Optional[bytes] = None) -> dict[str, Any]:
            headers = {
                "origin": self.headers.get("Origin", ""),
                "content-type": self.headers.get("Content-Type", ""),
            }
            event: dict[str, Any] = {
                "requestContext": {"http": {"method": method}},
                "headers": headers,
            }
            if body is not None:
                event["body"] = base64.b64encode(body).decode("ascii")
                event["isBase64Encoded"] = True
            return event

        def _drain_rejected_body(self, content_length: int) -> None:
            # API Gateway buffers the public request before invoking the private
            # integration. Drain a bounded body so the client receives the 413
            # instead of a TCP reset, without allocating the rejected payload.
            if content_length > MAX_REJECT_DRAIN_BYTES:
                self.close_connection = True
                return
            remaining = content_length
            while remaining > 0:
                chunk = self.rfile.read(min(remaining, 64 * 1024))
                if not chunk:
                    self.close_connection = True
                    return
                remaining -= len(chunk)

        def do_GET(self) -> None:  # noqa: N802 - stdlib handler contract
            path = urlsplit(self.path).path
            if self.path != path:
                self._write_local_json(404, {"ok": False, "code": "not_found"})
                return
            if path == "/health":
                self._write_local_json(
                    200,
                    {
                        "environment": config.environment,
                        "marker": HEALTH_MARKER,
                        "ok": True,
                        "version": config.collector_version,
                    },
                )
                return
            if path == "/marker.json":
                self._write_local_json(
                    200,
                    {
                        "environment": config.environment,
                        "marker": HOST_MARKER,
                        "runtime_path": RUNTIME_PATH,
                        "version": config.collector_version,
                    },
                )
                return
            self._write_local_json(404, {"ok": False, "code": "not_found"})

        def do_HEAD(self) -> None:  # noqa: N802 - stdlib handler contract
            self.do_GET()

        def do_OPTIONS(self) -> None:  # noqa: N802 - stdlib handler contract
            if self.path != "/v1/events":
                self._write_local_json(404, {"ok": False, "code": "not_found"})
                return
            self._write_response(
                handle_request(
                    self._collector_event("OPTIONS"),
                    config=config,
                    registry=registry,
                    s3=s3,
                )
            )

        def do_POST(self) -> None:  # noqa: N802 - stdlib handler contract
            if self.path != "/v1/events":
                self._write_local_json(404, {"ok": False, "code": "not_found"})
                return
            if self.headers.get("Transfer-Encoding"):
                self._write_response(
                    _response(
                        400,
                        {"accepted": False, "code": "invalid_body_encoding"},
                        self.headers.get("Origin")
                        if self.headers.get("Origin") in config.allowed_origins
                        else None,
                    )
                )
                return
            try:
                content_length = int(self.headers.get("Content-Length", ""))
            except ValueError:
                content_length = -1
            if content_length < 1 or content_length > config.max_body_bytes:
                if content_length > config.max_body_bytes:
                    self._drain_rejected_body(content_length)
                self._write_response(
                    _response(
                        413 if content_length > config.max_body_bytes else 400,
                        {"accepted": False, "code": "invalid_body_size"},
                        self.headers.get("Origin")
                        if self.headers.get("Origin") in config.allowed_origins
                        else None,
                    )
                )
                return
            body = self.rfile.read(content_length)
            if len(body) != content_length:
                self._write_response(
                    _response(
                        400,
                        {"accepted": False, "code": "invalid_body_size"},
                        self.headers.get("Origin")
                        if self.headers.get("Origin") in config.allowed_origins
                        else None,
                    )
                )
                return
            self._write_response(
                handle_request(
                    self._collector_event("POST", body),
                    config=config,
                    registry=registry,
                    s3=s3,
                )
            )

    return CollectorRequestHandler


def create_server(
    *,
    host: str,
    port: int,
    config: CollectorConfig,
    registry: Mapping[str, Mapping[str, frozenset[str]]],
    s3: Any,
) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(
        (host, port),
        make_request_handler(config=config, registry=registry, s3=s3),
    )
    server.daemon_threads = True
    return server


def main() -> int:
    import boto3  # type: ignore

    config = CollectorConfig.from_environment()
    registry = load_registry(str(config.registry_path), config.environment)
    s3 = boto3.client("s3", region_name=config.region)
    host = os.environ.get("GROWTHBOOK_BIND_HOST", DEFAULT_HOST).strip() or DEFAULT_HOST
    port = _configured_port()
    server = create_server(host=host, port=port, config=config, registry=registry, s3=s3)
    print(
        json.dumps(
            {
                "environment": config.environment,
                "marker": "VEVO_GROWTHBOOK_COLLECTOR_STARTED",
                "port": port,
                "version": config.collector_version,
            },
            separators=(",", ":"),
            sort_keys=True,
        ),
        flush=True,
    )
    try:
        server.serve_forever(poll_interval=0.25)
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
