## Endpoint Format

```
postgres://username:password@hostname:port/database-name
```

If you only supply `postgres://` as the endpoint, kine will attempt to do the following:
* Connect to localhost using `postgres` as the username and password
* Create a database named `kubernetes`

## Embedded connector options

Embedded users may set `endpoint.Config.PostgreSQL` (or
`drivers.Config.PostgreSQL`) to a `*drivers.PostgreSQLConfig`:

* `ConnConfig` supplies a configuration created by `pgx.ParseConfig`, replacing
  Kine's DSN/TLS parsing. Kine copies this configuration before use.
* `Options` supplies `stdlib.OptionOpenDB` hooks to both ordinary and
  administrative connectors. `stdlib.OptionBeforeConnect` can supply fresh
  credentials for every new physical connection. Keep the base configuration
  password-free and do not mutate shared maps or TLS state in callbacks.
* `SkipCreateDatabase` disables administrative connections, catalog checks, and
  database creation for databases provisioned separately.
* `SchemaLockKey` enables a transaction-scoped PostgreSQL advisory lock across
  schema setup, enabled migrations, and `Backend.Start`, including initial
  compaction and health-key setup. Every initializer must use the same key.
  The lock uses a dedicated connection, so the pool must permit at least two
  open connections. A limit of one is rejected.

The lock coordinates initialization; it does not make schema changes atomic.
Setup and backend startup use ordinary pooled connections so they can see
committed DDL. An interrupted initializer may leave partial idempotent setup
for the next initializer to finish. Lock acquisition and commit failures fail
startup without an unlocked fallback or automatic replay. Cancel a failed
backend's context before retrying. Do not hold the same advisory lock around
`endpoint.Listen`, which would self-deadlock. Verify advisory-lock support and
concurrent initialization against the target PostgreSQL-compatible service.

Nil PostgreSQL settings retain DSN parsing and database-creation behavior.
Connector authentication and callback errors are replaced by a fixed error to
avoid leaking credentials; cancellation, deadlines, and `driver.ErrBadConn`
remain recognizable.

The SQL poller discards partial row-stream batches and reprepares its retained
statement after query or row-stream failures without advancing the revision.
Failed polls wait for the next poll interval, and blocked publication observes
cancellation. This recovery does not replay writes or transactions. Healthy
polling still retains a checked-out connection; pool lifetime settings alone
do not proactively retire it.
