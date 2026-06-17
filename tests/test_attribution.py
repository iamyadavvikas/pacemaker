"""Cohort attribution tests — no database required."""

from __future__ import annotations

from governor.attribution import MIGRATION, PROD, CohortClassifier


def test_usename_takes_precedence_over_app_name():
    c = CohortClassifier.from_lists(usenames=["backfill"], app_names=["prod_api"])
    # usename matches even though app_name is on the prod list
    assert c.matched_signal("backfill", "prod_api", None) == "usename"
    assert c.cohort("backfill", "prod_api", None) == MIGRATION


def test_query_tag_matches_when_role_shared():
    c = CohortClassifier.from_lists(query_tag="dbguard:migration")
    sql = "/* dbguard:migration */ UPDATE projects SET ..."
    assert c.cohort("shared_role", "whatever", sql) == MIGRATION
    # same role, no tag -> prod
    assert c.cohort("shared_role", "whatever", "SELECT 1") == PROD


def test_app_name_is_last_resort():
    c = CohortClassifier.from_lists(app_names=["migrator"], query_tag=None)
    assert c.matched_signal(None, "migrator", None) == "application_name"
    assert c.cohort(None, "other", None) == PROD


def test_precedence_order_usename_then_tag_then_app():
    c = CohortClassifier.from_lists(
        usenames=["u"], app_names=["a"], query_tag="tag"
    )
    # all three present -> usename wins
    assert c.matched_signal("u", "a", "/* tag */") == "usename"
    # no usename -> tag wins over app
    assert c.matched_signal("x", "a", "/* tag */") == "query_tag"
    # only app present
    assert c.matched_signal("x", "a", "SELECT 1") == "application_name"


def test_empty_classifier_flags_nothing():
    c = CohortClassifier.from_lists(query_tag=None)
    assert c.is_empty is True
    assert c.cohort("anyone", "anything", "/* dbguard:migration */") == PROD


def test_default_query_tag_present():
    c = CohortClassifier.from_lists()
    assert c.query_tag == "dbguard:migration"
    assert c.is_empty is False
