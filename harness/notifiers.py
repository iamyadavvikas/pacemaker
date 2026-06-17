"""Shared CLI wiring to attach a real alert notifier to the demo harnesses.

Lets you actually *see* the alerting hooks fire (``throttle_started``,
``would_circuit_break``, ``sensor_error``, ``enforcer_tripped``) instead of only
trusting the unit tests. Both ``--notify-console`` (prints to stdout) and
``--slack-webhook URL`` are supported, and they compose via ``MultiNotifier``.
Synthetic data only — point ``--slack-webhook`` at your own incoming webhook.
"""

from __future__ import annotations

import argparse
import os


def add_notifier_args(parser: argparse.ArgumentParser) -> None:
    """Register the shared notifier flags on a harness argument parser."""
    parser.add_argument(
        "--notify-console",
        action="store_true",
        help="print high-signal decisions (throttle/circuit-break/sensor-error) to stdout.",
    )
    parser.add_argument(
        "--slack-webhook",
        default=os.environ.get("GOV_SLACK_WEBHOOK"),
        metavar="URL",
        help="Slack incoming-webhook URL to push decisions to (or set GOV_SLACK_WEBHOOK).",
    )


def build_notifier(args: argparse.Namespace):
    """Build a notifier from parsed args, or ``None`` if no sink was requested."""
    from governor import CallbackNotifier, MultiNotifier, SlackNotifier

    sinks = []
    if getattr(args, "notify_console", False):
        def _print(kind: str, message: str, context: dict | None = None) -> None:
            extra = f"  {context}" if context else ""
            print(f"  [alert] {kind}: {message}{extra}", flush=True)

        sinks.append(CallbackNotifier(_print))
    if getattr(args, "slack_webhook", None):
        sinks.append(SlackNotifier(webhook_url=args.slack_webhook))

    if not sinks:
        return None
    return sinks[0] if len(sinks) == 1 else MultiNotifier(*sinks)
