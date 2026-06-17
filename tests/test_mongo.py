"""MongoSensor / MongoKiller tests — no real MongoDB required."""

from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

from conftest import FakeMongoClient

from governor.attribution import CohortClassifier
from governor.config import GovernorConfig, Thresholds
from governor.enforcer import EnforcerAgent
from governor.sensors.base import Level
from governor.sensors.mongo import MongoKiller, MongoSensor


def _op(active=True, waiting=False, opid=1, user=None, app=None, comment=None):
    op = {"active": active, "waitingForLock": waiting, "opid": opid}
    if user is not None:
        op["effectiveUsers"] = [{"user": user, "db": "admin"}]
    if app is not None:
        op["appName"] = app
    cmd = {"find": "orders"}
    if comment is not None:
        cmd["comment"] = comment
    op["command"] = cmd
    return op


def test_aggregate_counts_active_and_blocked():
    client = FakeMongoClient(
        inprog=[_op(active=True), _op(active=True), _op(active=False, waiting=True)],
        connections={"current": 1, "available": 999},
    )
    hr = MongoSensor(client=client).sample()
    assert hr.active_backends == 2
    assert hr.blocked_backends == 1


def test_attribution_splits_migration_by_user():
    client = FakeMongoClient(
        inprog=[
            _op(opid=1, user="backfill_job"),
            _op(opid=2, user="app_user"),
        ],
        connections={"current": 1, "available": 999},
    )
    classifier = CohortClassifier.from_lists(usenames=["backfill_job"])
    hr = MongoSensor(client=client, classifier=classifier).sample()
    assert hr.cohorts["migration"].active == 1
    assert hr.cohorts["prod"].active == 1
    # the migration op is recorded as a killable victim with a strong signal
    assert hr.cohorts["migration"].victims == [(1, "usename")]


def test_attribution_matches_comment_tag():
    client = FakeMongoClient(
        inprog=[_op(opid=7, comment="dbguard:migration")],
        connections={"current": 1, "available": 999},
    )
    classifier = CohortClassifier.from_lists()  # default query_tag = dbguard:migration
    hr = MongoSensor(client=client, classifier=classifier).sample()
    assert hr.cohorts["migration"].active == 1
    assert hr.cohorts["migration"].victims == [(7, "query_tag")]


def test_connection_pool_saturation_escalates_level():
    # 95/100 connections used with a 0.9 critical bound -> CRITICAL even with 0 ops.
    client = FakeMongoClient(
        inprog=[],
        connections={"current": 95, "available": 5},
    )
    thresholds = Thresholds(critical_conn_pool_frac=0.9)
    hr = MongoSensor(client=client, thresholds=thresholds).sample()
    assert hr.conn_pool_used == 95
    assert hr.conn_pool_max == 100
    assert hr.level is Level.CRITICAL


def test_replication_lag_escalates_level():
    now = datetime.now(timezone.utc)
    client = FakeMongoClient(
        inprog=[],
        connections={"current": 1, "available": 999},
        repl_status={
            "members": [
                {"stateStr": "PRIMARY", "optimeDate": now},
                {"stateStr": "SECONDARY", "optimeDate": now - timedelta(seconds=30)},
            ]
        },
    )
    thresholds = Thresholds(red_replication_lag_s=10.0)
    hr = MongoSensor(client=client, thresholds=thresholds).sample()
    assert hr.replication_lag_s is not None and hr.replication_lag_s >= 29
    assert hr.level is Level.RED


def test_replset_status_failure_degrades_gracefully():
    client = FakeMongoClient(
        inprog=[_op(active=True)],
        connections={"current": 1, "available": 999},
        repl_raises=True,  # standalone / no perms
    )
    hr = MongoSensor(client=client).sample()
    assert hr.replication_lag_s is None  # no lag signal, no crash
    assert hr.active_backends == 1


def test_mongo_killer_issues_killop():
    client = FakeMongoClient()
    killer = MongoKiller(client=client)
    assert killer.cancel(42) is True
    assert client.killed_ops == [42]


def test_query_latency_p99_surfaced_in_headroom():
    # Two consecutive samples populate the rolling latency window; p99 must be set.
    client = FakeMongoClient(
        inprog=[_op(active=True)],
        connections={"current": 1, "available": 999},
    )
    sensor = MongoSensor(client=client)
    sensor.sample()
    hr = sensor.sample()
    assert hr.query_latency_ms is not None
    assert hr.query_latency_ms >= 0.0


def test_mongodemo_classifier_attributes_demo_migration_cohort():
    """The harness.mongodemo wiring (appName + comment) attributes the migration cohort."""
    from harness.mongodemo import (
        DEMO_MIGRATION_APP,
        DEMO_PROD_APP,
        MIGRATION_COMMENT,
    )

    classifier = CohortClassifier.from_lists(
        app_names=[DEMO_MIGRATION_APP], query_tag=MIGRATION_COMMENT
    )
    client = FakeMongoClient(
        inprog=[
            _op(opid=1, app=DEMO_MIGRATION_APP, comment=MIGRATION_COMMENT),
            _op(opid=2, app=DEMO_PROD_APP),
        ],
        connections={"current": 1, "available": 999},
    )
    hr = MongoSensor(client=client, classifier=classifier).sample()
    assert hr.cohorts["migration"].active == 1
    assert hr.cohorts["prod"].active == 1
    # comment tag is the strong signal, so the migration op is a killable victim
    assert hr.cohorts["migration"].victims == [(1, "query_tag")]


def _wait(seconds: float = 0.2) -> None:
    threading.Event().wait(seconds)


def test_enforcer_paces_mongo_migration_cohort_via_killop():
    """End-to-end ENFORCE on Mongo: MongoSensor reads, MongoKiller acts.

    Five active migration ops (strong comment-tag signal) sit over a CRITICAL
    headroom reading, so the AIMD limit drops and the enforcer must ``killOp``
    them — while never touching the prod op.
    """
    classifier = CohortClassifier.from_lists(query_tag="dbguard:migration")
    client = FakeMongoClient(
        inprog=[
            _op(opid=10, comment="dbguard:migration"),
            _op(opid=11, comment="dbguard:migration"),
            _op(opid=12, comment="dbguard:migration"),
            _op(opid=13, comment="dbguard:migration"),
            _op(opid=14, comment="dbguard:migration"),
            _op(opid=99),  # prod op — must never be killed
        ],
        connections={"current": 1, "available": 999},
    )
    thresholds = Thresholds(critical_active=4)  # 5 active ops -> CRITICAL
    sensor = MongoSensor(client=client, thresholds=thresholds, classifier=classifier)
    cfg = GovernorConfig(dsn="mongodb://fake", poll_interval_s=0.01, max_cancels_per_interval=2)
    agent = EnforcerAgent(cfg, sensor, canceller=MongoKiller(client=client))
    agent.start()
    try:
        _wait(0.3)
        snap = agent.snapshot()
        assert snap["mode"] == "enforce"
        assert snap["cancels_total"] > 0
        # only migration opids were killed; the prod op (99) was never touched
        assert client.killed_ops, "expected at least one killOp"
        assert all(op in {10, 11, 12, 13, 14} for op in client.killed_ops)
        assert 99 not in client.killed_ops
    finally:
        agent.stop()


def test_enforcer_never_kills_weak_signal_mongo_ops():
    """An op matched only on the spoofable appName must never be killed."""
    classifier = CohortClassifier.from_lists(app_names=["backfill_app"])
    client = FakeMongoClient(
        inprog=[_op(opid=20, app="backfill_app"), _op(opid=21, app="backfill_app")],
        connections={"current": 1, "available": 999},
    )
    thresholds = Thresholds(critical_active=1)  # force CRITICAL
    sensor = MongoSensor(client=client, thresholds=thresholds, classifier=classifier)
    cfg = GovernorConfig(dsn="mongodb://fake", poll_interval_s=0.01)
    agent = EnforcerAgent(cfg, sensor, canceller=MongoKiller(client=client))
    agent.start()
    try:
        _wait(0.2)
        # require_strong_signal (default) drops appName-only matches
        assert client.killed_ops == []
    finally:
        agent.stop()


