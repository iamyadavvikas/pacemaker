"""Workload attribution: decide which DB backends are a 'migration' cohort.

The migration/backfill job is the thing we want to pace; everything else is
treated as protected 'prod' traffic that must never be touched. We identify the
migration cohort from per-backend signals, consulted by precedence (most
reliable first):

  1. usename / role        -- the DB login the job connects as (hard to spoof)
  2. query comment tag      -- an inline SQL comment, e.g. /* dbguard:migration */
  3. application_name        -- libpq application_name (often unset/spoofable)

application_name is the weakest signal (frequently empty or wrong), so it is
consulted last. ``matched_signal`` returns the name of the signal that matched
(useful for the evidence report) or ``None`` when the backend looks like prod.
"""

from __future__ import annotations

from dataclasses import dataclass, field

MIGRATION = "migration"
PROD = "prod"
DEFAULT_QUERY_TAG = "dbguard:migration"


@dataclass(frozen=True)
class CohortClassifier:
    """Rules that flag a backend as the migration cohort, by signal precedence."""

    usenames: frozenset[str] = field(default_factory=frozenset)
    app_names: frozenset[str] = field(default_factory=frozenset)
    query_tag: str | None = DEFAULT_QUERY_TAG

    @classmethod
    def from_lists(
        cls,
        usenames: list[str] | None = None,
        app_names: list[str] | None = None,
        query_tag: str | None = DEFAULT_QUERY_TAG,
    ) -> "CohortClassifier":
        return cls(
            usenames=frozenset(usenames or ()),
            app_names=frozenset(app_names or ()),
            query_tag=query_tag or None,
        )

    @property
    def is_empty(self) -> bool:
        """True if no rule is configured (would classify everything as prod)."""
        return not (self.usenames or self.app_names or self.query_tag)

    def matched_signal(
        self,
        usename: str | None,
        application_name: str | None,
        query: str | None,
    ) -> str | None:
        """Return which signal marks this backend as migration (by precedence), else None."""
        if usename and usename in self.usenames:
            return "usename"
        if self.query_tag and query and self.query_tag in query:
            return "query_tag"
        if application_name and application_name in self.app_names:
            return "application_name"
        return None

    def cohort(
        self,
        usename: str | None,
        application_name: str | None,
        query: str | None,
    ) -> str:
        """Classify a backend as ``MIGRATION`` or ``PROD``."""
        return MIGRATION if self.matched_signal(usename, application_name, query) else PROD
