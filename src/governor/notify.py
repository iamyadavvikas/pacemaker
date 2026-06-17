"""Alerting hooks: push the governor's key decisions to where on-call sees them.

The dashboard is live-only; these notifiers are the durable, push side — the
"I throttled projectmigrator at 02:17, DB stayed healthy, zero customer impact"
trail that turns a pilot into a permanent control. They are intentionally
dependency-free (stdlib ``urllib`` only) and fire-and-forget on a daemon thread,
so a slow or down alerting endpoint can never stall the sampling loop or add
latency to anything in the data path.

Notifiers receive a small set of high-signal events (not every poll):

  - ``would_circuit_break`` / ``circuit_break`` — the DB hit CRITICAL.
  - ``enforcer_tripped``  — the runaway guard disabled enforcement.
  - ``sensor_error``      — the DB became unreadable (fail-safe engaged).
  - ``throttle_started``  — a new throttling episode began.

    notifier = SlackNotifier(webhook_url=os.environ["SLACK_WEBHOOK"])
    agent = ObserverAgent(cfg, sensor, notifier=notifier)
"""

from __future__ import annotations

import json
import threading
import urllib.request
from collections.abc import Callable
from typing import Protocol, runtime_checkable


@runtime_checkable
class Notifier(Protocol):
    def notify(self, kind: str, message: str, context: dict | None = None) -> None:
        ...


class NullNotifier:
    """Default no-op notifier — alerting is opt-in."""

    def notify(self, kind: str, message: str, context: dict | None = None) -> None:
        return


class MultiNotifier:
    """Fan one event out to several notifiers; one failing never blocks the rest."""

    def __init__(self, *notifiers: Notifier) -> None:
        self._notifiers = list(notifiers)

    def notify(self, kind: str, message: str, context: dict | None = None) -> None:
        for n in self._notifiers:
            try:
                n.notify(kind, message, context)
            except Exception:  # noqa: BLE001 - a bad sink must not break the others
                pass


def _post_json(url: str, payload: dict, headers: dict[str, str], timeout: float) -> None:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    for k, v in headers.items():
        req.add_header(k, v)
    with urllib.request.urlopen(req, timeout=timeout):  # noqa: S310 - operator-supplied URL
        pass


class _AsyncWebhookNotifier:
    """Base for HTTP webhook notifiers: builds a payload, POSTs it off-thread."""

    def __init__(self, url: str, timeout: float = 5.0) -> None:
        self._url = url
        self._timeout = timeout

    def _payload(self, kind: str, message: str, context: dict | None) -> dict:
        raise NotImplementedError

    def _headers(self) -> dict[str, str]:
        return {}

    def notify(self, kind: str, message: str, context: dict | None = None) -> None:
        payload = self._payload(kind, message, context)
        headers = self._headers()

        def _send() -> None:
            try:
                _post_json(self._url, payload, headers, self._timeout)
            except Exception:  # noqa: BLE001 - fire-and-forget; never raise into the loop
                pass

        threading.Thread(target=_send, name="dbguard-notify", daemon=True).start()


class SlackNotifier(_AsyncWebhookNotifier):
    """Posts to a Slack Incoming Webhook (also works for any ``{"text": ...}`` sink)."""

    def __init__(self, webhook_url: str, prefix: str = "pacing-governor", timeout: float = 5.0) -> None:
        super().__init__(webhook_url, timeout)
        self._prefix = prefix

    def _payload(self, kind: str, message: str, context: dict | None) -> dict:
        ctx = "" if not context else "  " + " ".join(f"{k}={v}" for k, v in context.items())
        return {"text": f"[{self._prefix}] *{kind}* — {message}{ctx}"}


class SignalFxNotifier(_AsyncWebhookNotifier):
    """Sends a custom event to SignalFx so throttle decisions land on dashboards.

    Posts to the SignalFx event ingest endpoint. ``kind`` becomes the
    ``eventType`` and the message/context ride along as properties, so an alert
    detector or chart event-overlay can correlate a throttle with DB health.
    """

    def __init__(
        self,
        token: str,
        realm: str = "us1",
        url: str | None = None,
        timeout: float = 5.0,
    ) -> None:
        super().__init__(url or f"https://ingest.{realm}.signalfx.com/v2/event", timeout)
        self._token = token

    def _headers(self) -> dict[str, str]:
        return {"X-SF-Token": self._token}

    def _payload(self, kind: str, message: str, context: dict | None) -> list:
        properties = {"message": message}
        if context:
            properties.update({k: str(v) for k, v in context.items()})
        # SignalFx event ingest takes a JSON array of events.
        return [
            {
                "category": "USER_DEFINED",
                "eventType": f"dbguard.{kind}",
                "dimensions": {"source": "pacing-governor"},
                "properties": properties,
            }
        ]


class CallbackNotifier:
    """Wrap a plain callable as a Notifier (handy for tests / custom sinks)."""

    def __init__(self, fn: Callable[[str, str, dict | None], None]) -> None:
        self._fn = fn

    def notify(self, kind: str, message: str, context: dict | None = None) -> None:
        self._fn(kind, message, context)
