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
* `SchemaLockKey` enables a transaction-scoped PostgreSQL advisory lock for
  schema setup, enabled migrations, and compact-revision initialization.
  All of these operations use the **same transaction and connection** as the
  lock, and commit before `Backend.Start`. Every initializer must use the same
  key. Connection loss rolls back the initialization; it cannot leave an
  unfenced initializer executing DDL on another pooled connection.

The existing one-open-connection guard remains: ordinary backend startup
still needs at least two pooled connections, independently of schema setup.

The ordinary backend sees one committed compact-revision row, so its startup
does not need to insert or repair that row. Health-key creation remains
independently idempotent through the unique `(name, prev_revision)` index.
There is no startup gap-fill helper in this version; runtime gap filling is
unchanged. The schema lock does not fence normal runtime writes or replace
cluster compatibility checks.

Lock acquisition and commit failures fail startup without an unlocked fallback
or automatic replay. PostgreSQL sequence increments are not transactional and
may leave gaps after an aborted attempt. Cancel a failed backend's context
before retrying. Do not hold the same advisory lock around `endpoint.Listen`,
which would self-deadlock. The target PostgreSQL-compatible service must support
transactional DDL and transaction advisory locks. An opt-in integration test
uses `KINE_PG_TEST_DSN` for a disposable local PostgreSQL instance; it creates
isolated schemas and terminates only a test-owned backend connection.

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
