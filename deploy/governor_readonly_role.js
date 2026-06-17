// Least-privilege user for the governor's MongoSensor in production.
//
// The governor only ever runs read-only, server-side admin commands against the
// cluster: currentOp (in-flight ops), serverStatus (connection counts) and
// replSetGetStatus (replica-set lag). The built-in `clusterMonitor` role grants
// exactly those read-only monitoring privileges and nothing else — it cannot
// read your collections, write, or run killOp. This is the OBSERVE-mode user.
//
//   mongosh "mongodb://admin@host:27017/admin" governor_readonly_role.js
//
// Then point the sensor at a SECONDARY so polling never touches the primary:
//   dbguard observe \
//     --dsn "mongodb://gov_sensor:<pw>@host:27017/?replicaSet=rs0&readPreference=secondaryPreferred"
//
// Atlas: create the user in the UI/API with the built-in role "Cluster Monitor"
// (atlasAdmin / readWriteAnyDatabase are NOT needed and must NOT be granted).

const password = "CHANGE_ME";

db.getSiblingDB("admin").createUser({
  user: "gov_sensor",
  pwd: password,
  roles: [
    // Read-only cluster monitoring: currentOp / serverStatus / replSetGetStatus.
    { role: "clusterMonitor", db: "admin" },
  ],
});

// NOTE: do NOT grant `clusterManager` / `clusterAdmin` (those allow killOp and
// other mutating cluster ops). ENFORCE mode (MongoKiller -> killOp) needs a
// SEPARATE, explicitly privileged user; keep this monitoring user read-only.
