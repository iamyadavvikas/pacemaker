-- Least-privilege role for the governor's sensor in production.
--
-- The governor only ever runs read-only introspection queries against
-- pg_stat_activity. Grant it pg_monitor (lets it see all sessions' stats and
-- query text) and nothing else. It never writes to or locks your tables.
--
--   psql -f deploy/governor_readonly_role.sql
--
-- Then point the sensor at:  postgresql://gov_sensor:<pw>@host:5432/<db>

CREATE ROLE gov_sensor LOGIN PASSWORD 'CHANGE_ME';

-- See full pg_stat_activity (including other users' active queries / wait events).
GRANT pg_monitor TO gov_sensor;

-- Allow connecting to the target database.
-- GRANT CONNECT ON DATABASE your_db TO gov_sensor;
