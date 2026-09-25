# Relus MySQL CDC candidate prototype

Independent prototype for GitHub issues #29 and #30. It is deliberately excluded from the root workspace by its own `[workspace]` declaration and does not replace `reader/src/binlog_reader.rs`.

Selected dependency:

```toml
mysql_async = { version = "=0.37.1", default-features = false, features = [
  "minimal-rust", "rustls-tls", "ring", "tls12", "binlog"
] }
```

The public boundary is Relus-owned: `MySqlReplicationClient`, `RawReplicationEvent`, `CdcOffset`, `CdcMessage`, and `CdcEnvelope`. Third-party event and connection types stay in `src/adapter.rs`. Row messages include a primary-key map when MySQL's table-map metadata identifies primary-key columns.

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

`MySqlConnectionConfig` exposes only three TLS modes:

- `disabled`
- `required` with a custom CA path and verified server name
- `mutual` with the required-TLS fields plus client certificate and private-key paths

The adapter disables built-in roots whenever a custom CA is selected. It does not expose
invalid-certificate or identity-validation bypasses. The prototype only passes the configured
paths to `mysql_async`; Relus code does not open, copy, modify, print, or parse certificate
contents. Passwords and certificate paths are redacted from `Debug` output.

Version connection profiles continue to use `RELUS_TEST_MYSQL{56,57,80,84}_{HOST,PORT,USERNAME,PASSWORD,DATABASE}`.
The following optional variables enable the issue #30 cases:

```text
RELUS_TEST_MYSQL_CA_PATH
RELUS_TEST_MYSQL_CLIENT_CERT_PATH
RELUS_TEST_MYSQL_CLIENT_KEY_PATH
RELUS_TEST_MYSQL_SERVER_NAME

RELUS_TEST_MYSQL80_NATIVE_USERNAME
RELUS_TEST_MYSQL80_NATIVE_PASSWORD
RELUS_TEST_MYSQL80_CACHING_SHA2_USERNAME
RELUS_TEST_MYSQL80_CACHING_SHA2_PASSWORD

RELUS_TEST_MYSQL80_BINLOG_FILE
RELUS_TEST_MYSQL80_BINLOG_POSITION
RELUS_TEST_MYSQL80_SERVER_ID
```

Authentication and replication variables use the same suffixes for the other version profiles.
The cancellation check races a pending replication read against a cancellation token, drops the
read future, and closes the owned replication connection within a bounded timeout. It starts no
blocking worker thread.

The documented harmless allowlist is intentionally narrow: format-description,
previous-GTID-set, rows-query metadata, and events explicitly marked ignorable by MySQL.
TABLE_MAP and ROTATE are emitted as versioned Relus control messages. Transaction-payload
markers fail closed because upstream decompression failures are not surfaced reliably.

## Optional MySQL 8.0 run

The streaming binary only connects when all required `RELUS_TEST_MYSQL80_*` variables are supplied, including `BINLOG_FILE`, `BINLOG_POSITION`, and `SERVER_ID`. It prints versioned JSON messages. The #29 slice implements file/position and GTID-set streaming; #30 adds TLS configuration, authentication verification, and controlled cancellation. Recovery, snapshot, and timestamp modes belong to later tickets.
