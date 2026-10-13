# Relus MySQL CDC candidate prototype

Independent prototype for GitHub issues #29 through #34 and the approved production-migration gate closure. It is deliberately excluded from the root workspace by its own `[workspace]` declaration and does not replace `reader/src/binlog_reader.rs`.

Selected dependency:

```toml
mysql_async = { version = "=0.37.1", default-features = false, features = [
  "minimal-rust", "rustls-tls", "ring", "tls12", "binlog"
] }
```

The public boundary is Relus-owned: `MySqlReplicationClient`, `RawReplicationEvent`, `CdcOffset`, `CdcMessage`, and `CdcEnvelope`. Third-party event and connection types stay in `src/adapter.rs`. At connection time the adapter discovers column names, ordinals, primary keys, and integer signedness from `information_schema.columns`; users do not provide a schema map. TABLE_MAP identifies the database/table and row values are mapped through that discovered schema. Missing metadata or a changed column count fails closed as `SchemaDrift`.

## Local checks

```bash
cargo test --manifest-path prototypes/mysql-cdc-candidate/Cargo.toml
cargo clippy --manifest-path prototypes/mysql-cdc-candidate/Cargo.toml --all-targets -- -D warnings
cargo run --manifest-path prototypes/mysql-cdc-candidate/Cargo.toml --bin verify
cargo tree --manifest-path prototypes/mysql-cdc-candidate/Cargo.toml -i openssl
cargo tree --manifest-path prototypes/mysql-cdc-candidate/Cargo.toml -i openssl-sys
```

The verification command prints JSON cases using only `passed`, `failed`, and `not_run`.
It exits non-zero on any executed failure. Missing MySQL versions, authentication-specific
accounts, certificate paths, or replication offsets remain `not_run`; they are never counted
as passes. The final two commands must report that no matching package exists.

## Secure connection and shutdown verification

`MySqlConnectionConfig` exposes four TLS modes:

- `disabled`
- `encrypted` for compatibility-only encryption without server identity verification
- `required` with a verified server name and an optional custom CA path
- `mutual` with the required-TLS fields plus client certificate and private-key paths

Without a custom CA, the adapter uses `mysql_async`'s built-in WebPKI roots. It disables those
built-in roots whenever a custom CA is selected. It does not expose
invalid-certificate or identity-validation bypasses in `required` or `mutual` mode. The explicit
`encrypted` mode is an exception for compatibility testing only and is never a production gate.
The prototype only passes the configured paths to `mysql_async`; Relus code does not open, copy,
modify, print, or parse certificate contents. Passwords and certificate paths are redacted from
`Debug` output.

Version connection profiles continue to use `RELUS_TEST_MYSQL{56,57,80,84}_{HOST,PORT,USERNAME,PASSWORD,DATABASE}`.
The following optional variables enable the issue #30 cases:

```text
RELUS_TEST_MYSQL{56,57,80,84}_CA_PATH
RELUS_TEST_MYSQL{56,57,80,84}_CLIENT_CERT_PATH
RELUS_TEST_MYSQL{56,57,80,84}_CLIENT_KEY_PATH
RELUS_TEST_MYSQL{56,57,80,84}_SERVER_NAME

RELUS_TEST_MYSQL80_NATIVE_USERNAME
RELUS_TEST_MYSQL80_NATIVE_PASSWORD
RELUS_TEST_MYSQL80_CACHING_SHA2_USERNAME
RELUS_TEST_MYSQL80_CACHING_SHA2_PASSWORD

RELUS_TEST_MYSQL80_BINLOG_FILE
RELUS_TEST_MYSQL80_BINLOG_POSITION
RELUS_TEST_MYSQL80_SERVER_ID
RELUS_TEST_MYSQL80_GTID_SET
```

TLS inputs are isolated per MySQL profile; the harness does not fall back to legacy global
certificate variables. `CA_PATH` is optional. Omitting it uses built-in WebPKI roots but never
disables certificate or server-name verification. A private or self-signed server chain still
requires an appropriate custom CA path.

Authentication and replication variables use the same suffixes for the other version profiles.
Authentication checks use `SHOW CREATE USER CURRENT_USER()` on MySQL 5.7 and later. MySQL 5.6
uses a read-only `mysql.user.plugin` lookup because it does not support that statement. Both paths
confirm the server-side plugin, and the returned definition is never printed or included in
errors. This avoids treating a mislabeled account as plugin evidence.
The wrong-CA case requires a second approved certificate path; the harness uses the supplied
client-certificate path as that distinct trust root and reports `not_run` when no such approved
path exists.
The cancellation check races a pending replication read against a cancellation token, drops the
read future, and closes the owned replication connection within a bounded timeout. It starts no
blocking worker thread.

`MySqlConnectionConfig::protocol_compression` explicitly controls MySQL protocol compression.
When enabled, the adapter requests the driver's fast compression level. The uncompressed path
remains available. The live same-coordinate A/B benchmark measured a 10.83% median improvement
from compression on the local MySQL 5.6 workload. The optional streaming binary enables it when
`RELUS_TEST_MYSQL80_COMPRESSION` is present.

The `tls_encrypted` and `gtid` verification cases use the versioned replication profile. The
encrypted case deliberately does not verify the server identity and is compatibility evidence
only; it is not a production-migration gate. The GTID case requires a current versioned
`GTID_SET` and only proves that a GTID replication request can connect and close.

## Recovery and at-least-once crash proof

Issue #31 adds a Relus-owned recovery seam around the candidate adapter:

- `TestCheckpointStore` persists a versioned checkpoint containing the last sink-confirmed
  complete transaction offset and a monotonic sequence.
- `FileCheckpointStore` uses asynchronous filesystem I/O to write and fsync a temporary JSON
  record before atomically replacing the prior checkpoint. An interrupted save leaves the prior
  record readable, and a restart safely replaces the leftover temporary file on the next save.
- File/position offsets persist directly. GTID checkpoints normalize the completed transaction
  into the MySQL resume set with `mysql_async`'s own `Sid` parser.
- `PrototypeSink` separates row writes from transaction acknowledgment. The checkpoint advances
  only after the sink acknowledges the complete COMMIT.
- `MySqlRecoveryClientFactory` creates each real replication client from the last confirmed
  checkpoint. Retryable connection/read errors, temporary MySQL server-unavailable errors,
  unexpected replication EOF (including between transactions), and heartbeat silence reconnect
  with a configured bound using exponential delay with jitter; permanent errors stop immediately.

`tests/recovery.rs` exercises crash/restart before sink write, after sink acknowledgment but before
checkpoint, during the atomic checkpoint operation, during a multi-row transaction, and after a
snapshot-complete checkpoint but before incremental handoff. It proves unconfirmed work may replay
with stable event IDs, while a checkpoint never crosses a rejected or incomplete transaction.

## Startup modes and snapshot handoff

Issue #32 adds `prepare_startup` and public seams for startup offset discovery and a single-table
consistent snapshot. The prototype proves:

- `specific` preserves and validates both file/position and GTID offsets;
- `earliest` and `latest` use the currently retained boundaries;
- offset-aware RFC 3339 `timestamp` lookup selects the complete transaction covering the time and
  persists the resolved offset for restart;
- missing or purged history fails without falling back to another mode;
- `initial` requires InnoDB, a primary or non-null unique key with key-ordered pagination, and an
  idempotent snapshot sink;
- snapshot rows use `operation="snapshot"`, stable key-based event IDs, and are acknowledged before
  a `snapshot_complete` checkpoint captures the incremental handoff offset; the incremental seam
  then consumes events from that exact captured offset;
- a failed snapshot leaves no checkpoint, so the next run starts a fresh consistent snapshot.

`tests/startup.rs` contains the deterministic startup proofs. The real MySQL adapter remains
isolated behind these seams until an approved production migration.

The documented harmless allowlist is intentionally narrow: format-description,
previous-GTID-set, rows-query metadata, and events explicitly marked ignorable by MySQL.
TABLE_MAP and ROTATE are emitted as versioned Relus control messages. Transaction-payload
markers fail closed because upstream decompression failures are not surfaced reliably.

## Optional MySQL 8.0 run

The streaming binary only connects when all required `RELUS_TEST_MYSQL80_*` variables are supplied, including `BINLOG_FILE`, `BINLOG_POSITION`, and `SERVER_ID`. It prints versioned JSON messages. The #29 slice implements file/position and GTID-set streaming; #30 adds TLS configuration, authentication verification, and controlled cancellation; #31 adds the independent recovery loop and crash proofs; #32 adds startup-mode and snapshot-handoff proofs. Production Reader integration remains outside the approved prototype scope.

## Issue #33 boundedness and fidelity update

Date: 2026-09-26

The prototype now contains deterministic seams for the boundedness and data-fidelity gates:

- `BoundedEventBuffer` serializes each message once, limits both queued event count and actual
  serialized bytes, and exposes those same bytes to the Sink. A slow consumer makes the producer
  await capacity, oversized messages fail before enqueue, and metrics expose current and peak
  event/byte usage.
- The transaction assembler emits row messages incrementally, retains only transaction counters,
  preserves cross-table row order, and emits the checkpoint-eligible boundary only at COMMIT.
- `CdcValue` uses an unambiguous externally tagged representation so signed/unsigned integers,
  floating point values, byte strings, decimal text, UTF-8 text, zero dates, and temporal values
  round-trip without the prior Date/DateTime ambiguity.
- Schema validation rejects spatial types. DDL query events and observed schema changes fail with
  `SchemaDrift`; they are never treated as harmless events or allowed to advance a checkpoint.
- `measure_bounded_load` records throughput and peak resource use, while `compare_to_baseline`
  applies the required 80 percent gate.

The deterministic evidence is in `tests/bounded.rs`, `tests/data_fidelity.rs`,
`tests/performance.rs`, and the transaction unit tests. The 2026-10-10 live comparison is recorded
in `docs/research/mysql-cdc-performance-evaluation.md`. Memory boundedness and a 500,000-row
transaction passed. After compact-row, shared-metadata, in-place decode, event-level column
ordering, scalar fast-path, event-ID-prefix, serialization-capacity, and protocol-compression
optimizations, candidate median throughput reached 101.59% of the current Reader baseline and
passed the required 80% gate.

| Issue #33 evidence item | Result |
| --- | --- |
| Event and actual serialized-byte limits apply producer backpressure | `passed` |
| Buffer metrics stay within configured limits under slow consumption | `passed` |
| Large transactions stream rows and checkpoint only at COMMIT | `passed` |
| Multi-table transaction order is preserved | `passed` |
| Common value matrix round-trips without integer/decimal loss | `passed` |
| Unsupported spatial types fail validation | `passed` |
| Schema changes stop safely without checkpoint advancement | `passed` |
| 80 percent performance gate logic | `passed` |
| Current `mysql_cdc` baseline in the same environment | `passed` |
| Sustained live throughput gate | `passed` (101.59%; minimum 80%) |
| Live 1M/2M-row memory boundedness | `passed` |
| Live 500,000-row transaction resource check | `passed` |

## 2026-10-12 live DML and recovery closure

The in-scope MySQL 5.6 and 8.0 environments both passed real INSERT, UPDATE, and DELETE capture,
named-column and primary-key mapping, unsigned integer fidelity, common temporal/binary/text/null
values, confirmed file/position restart, unconfirmed replay, and confirmed GTID restart. GTID dump
requests rely on the GTID set without incorrectly starting in the middle of a binlog file, while a
file/position startup remains a file/position checkpoint throughout recovery.

The final evaluation classifies the candidate as `recommended_for_production_migration`. The
prototype remains isolated until the separately executed production Reader migration begins.
