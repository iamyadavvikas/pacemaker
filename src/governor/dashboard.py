"""A tiny, dependency-free live dashboard for a running ``Governor``.

Serves a single self-contained HTML page plus a JSON ``/api/state`` endpoint
that the page polls a few times a second. Built on the stdlib ``http.server``
so it adds **zero** runtime dependencies.

    gov = Governor(...); gov.start()
    dash = DashboardServer(gov)        # binds 127.0.0.1:8765 by default
    dash.start()
    print(dash.url)
    ...
    dash.stop(); gov.stop()

Bind host/port are env-overridable (``GOV_DASH_HOST`` / ``GOV_DASH_PORT``) so the
same code runs locally (127.0.0.1) and inside a container (0.0.0.0) without edits.

The dashboard is **read-only** for metrics: it reflects ``governor.snapshot()``
(and an optional ``extra_metrics`` callback, e.g. live checkout latency) plus an
optional static ``baseline`` (the ungoverned warmup profile). It also exposes one
control endpoint, ``POST /api/mode``, to flip ENFORCE<->OBSERVE at runtime.

Because that is a write/control surface, set an **auth token** before exposing it
beyond localhost: pass ``auth_token=...`` or set ``GOV_DASH_TOKEN``. When set, every
route except the ``/api/health`` liveness probe requires HTTP Basic auth (any
username; password = the token). With no token it stays open and binds to localhost
by default, so keep it on a trusted network until a token is configured.
"""

from __future__ import annotations

import base64
import hmac
import json
import os
import threading
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources

DEFAULT_HOST = os.environ.get("GOV_DASH_HOST", "127.0.0.1")
DEFAULT_PORT = int(os.environ.get("GOV_DASH_PORT", "8765"))
DEFAULT_TOKEN = os.environ.get("GOV_DASH_TOKEN") or None
AUTH_REALM = "pacing-governor"

# Type of the optional live-metrics hook: returns a JSON-serializable dict that
# gets merged into /api/state (the harness uses it to add checkout latency).
ExtraMetrics = Callable[[], dict]


def _load_index_html() -> str:
    """Read the bundled dashboard page (works installed and from source)."""
    return resources.files(__package__).joinpath("web/index.html").read_text(encoding="utf-8")


class DashboardServer:
    """Serves a live view of one ``Governor`` over HTTP on a daemon thread."""

    def __init__(
        self,
        governor,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        extra_metrics: ExtraMetrics | None = None,
        baseline: dict | None = None,
        auth_token: str | None = DEFAULT_TOKEN,
    ) -> None:
        self._governor = governor
        self._host = host
        self._port = port
        self._extra_metrics = extra_metrics
        self._baseline = baseline
        self._auth_token = auth_token or None
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._index_html = _load_index_html()

    # --- auth ---
    def _auth_ok(self, header: str | None) -> bool:
        """True if no token is configured, or the Basic-auth password matches it."""
        if self._auth_token is None:
            return True
        if not header or not header.startswith("Basic "):
            return False
        try:
            decoded = base64.b64decode(header[6:], validate=True).decode("utf-8")
        except (ValueError, UnicodeDecodeError):
            return False
        # username is ignored; password (after the first ':') must match the token
        _, _, password = decoded.partition(":")
        return hmac.compare_digest(password, self._auth_token)

    # --- state assembly ---
    def _state(self) -> dict:
        state = self._governor.snapshot()
        if self._extra_metrics is not None:
            try:
                extra = self._extra_metrics() or {}
            except Exception as exc:  # noqa: BLE001 - never let a metrics hook 500 the page
                extra = {"extra_metrics_error": repr(exc)}
            state["extra"] = extra
        if self._baseline is not None:
            state["baseline"] = self._baseline
        return state

    # --- control ---
    def _set_mode(self, body: bytes) -> tuple[int, dict]:
        """Handle a POST /api/mode body. Returns (http_status, json_dict)."""
        if not callable(getattr(self._governor, "set_mode", None)):
            # e.g. an ObserverAgent is OBSERVE-only and has no runtime switch.
            return 405, {"error": "mode is fixed for this server (observe-only)"}
        try:
            payload = json.loads(body or b"{}")
        except (ValueError, TypeError):
            return 400, {"error": "invalid JSON body"}
        if not isinstance(payload, dict) or payload.get("mode") not in ("enforce", "observe"):
            return 400, {"error": "body must be {\"mode\": \"enforce\"|\"observe\"}"}
        new_mode = self._governor.set_mode(payload["mode"])
        return 200, {"mode": new_mode.value}

    # --- throttle verdict (gh-ost --throttle-http / pt-osc compatible) ---
    def _throttle_verdict(self) -> tuple[int, dict]:
        """Return (http_status, body) for the throttle endpoint.

        Convention matches gh-ost ``--throttle-http``: HTTP 200 means *proceed*,
        any non-200 (we use 429) means *throttle / back off*. A migration tool can
        poll this URL and slow itself down whenever it sees a non-200.

        Works with an ``ObserverAgent`` (``throttle_verdict()``) and degrades
        gracefully for a plain ``Governor`` (falls back to ``snapshot()``: a limit
        of 0 == circuit-break == throttle).
        """
        verdict_fn = getattr(self._governor, "throttle_verdict", None)
        if callable(verdict_fn):
            verdict = verdict_fn()
        else:
            snap = self._governor.snapshot()
            verdict = {
                "throttle": snap.get("limit", 1) <= 0,
                "mode": snap.get("mode"),
                "limit": snap.get("limit"),
                "level": snap.get("last_level"),
            }
        code = 429 if verdict.get("throttle") else 200
        return code, verdict

    # --- lifecycle ---
    def start(self) -> None:
        if self._httpd is not None:
            return
        handler = _make_handler(self)
        # bind to (host, port); port 0 lets the OS pick a free port (tests)
        self._httpd = ThreadingHTTPServer((self._host, self._port), handler)
        self._httpd.daemon_threads = True
        # reflect the actually-bound port (in case 0 was requested)
        self._port = self._httpd.server_address[1]
        self._thread = threading.Thread(
            target=self._httpd.serve_forever,
            name="gov-dashboard",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
            self._httpd = None
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    @property
    def port(self) -> int:
        return self._port

    @property
    def url(self) -> str:
        host = "127.0.0.1" if self._host in ("0.0.0.0", "") else self._host
        return f"http://{host}:{self._port}"


def _make_handler(server: DashboardServer):
    """Build a request handler bound to one DashboardServer instance."""

    class _Handler(BaseHTTPRequestHandler):
        # quiet by default; the governor/harness own the console
        def log_message(self, *args) -> None:  # noqa: D401
            return

        def _send(self, code: int, body: bytes, content_type: str) -> None:
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _auth_failed(self) -> None:
            """Send a 401 that prompts the browser for Basic credentials."""
            body = b'{"error":"unauthorized"}'
            self.send_response(401)
            self.send_header("WWW-Authenticate", f'Basic realm="{AUTH_REALM}"')
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802 - stdlib API name
            path = self.path.split("?", 1)[0]
            # /api/health and /throttle stay open: liveness probes and external
            # migration tools (gh-ost --throttle-http) poll them without creds.
            open_paths = ("/api/health", "/throttle")
            if path not in open_paths and not server._auth_ok(self.headers.get("Authorization")):
                self._auth_failed()
                return
            if path in ("/", "/index.html"):
                self._send(200, server._index_html.encode("utf-8"), "text/html; charset=utf-8")
                return
            if path == "/api/state":
                body = json.dumps(server._state()).encode("utf-8")
                self._send(200, body, "application/json")
                return
            if path == "/api/health":
                self._send(200, b'{"ok":true}', "application/json")
                return
            if path == "/throttle":
                code, verdict = server._throttle_verdict()
                self._send(code, json.dumps(verdict).encode("utf-8"), "application/json")
                return
            self._send(404, b'{"error":"not found"}', "application/json")

        def do_HEAD(self) -> None:  # noqa: N802 - stdlib API name
            # gh-ost --throttle-http issues HEAD requests and treats any non-200
            # as "throttle". Mirror do_GET's status codes with no body.
            path = self.path.split("?", 1)[0]
            if path == "/throttle":
                code, _ = server._throttle_verdict()
                self.send_response(code)
                self.send_header("Content-Length", "0")
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                return
            if path == "/api/health":
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            if not server._auth_ok(self.headers.get("Authorization")):
                self.send_response(401)
                self.send_header("WWW-Authenticate", f'Basic realm="{AUTH_REALM}"')
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self.send_response(200 if path in ("/", "/index.html", "/api/state") else 404)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_POST(self) -> None:  # noqa: N802 - stdlib API name
            path = self.path.split("?", 1)[0]
            if not server._auth_ok(self.headers.get("Authorization")):
                self._auth_failed()
                return
            if path == "/api/mode":
                try:
                    length = int(self.headers.get("Content-Length", "0") or "0")
                except ValueError:
                    length = 0
                body = self.rfile.read(length) if length > 0 else b""
                code, payload = server._set_mode(body)
                self._send(code, json.dumps(payload).encode("utf-8"), "application/json")
                return
            self._send(404, b'{"error":"not found"}', "application/json")

    return _Handler
