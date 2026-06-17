"""Unit tests for the read-only ``dbguard preflight`` helper (no DB required)."""

from __future__ import annotations

from harness.preflight import _build_parser, _classifier


def test_classifier_none_when_no_attribution_flags():
    args = _build_parser().parse_args(["--dsn", "mongodb://localhost:27017"])
    # default --migration-tag is set, so the classifier is NOT empty
    cls = _classifier(args)
    assert cls is not None
    assert cls.query_tag == "dbguard:migration"


def test_classifier_collects_user_and_app():
    args = _build_parser().parse_args(
        [
            "--dsn", "postgresql://gov_sensor@host/app",
            "--migration-user", "backfill_job",
            "--migration-app", "ingest_worker",
        ]
    )
    cls = _classifier(args)
    assert cls is not None
    assert "backfill_job" in cls.usenames
    assert "ingest_worker" in cls.app_names


def test_parser_defaults():
    args = _build_parser().parse_args(["--dsn", "mongodb://localhost:27017"])
    assert args.samples == 5
    assert args.tolerance == 2
