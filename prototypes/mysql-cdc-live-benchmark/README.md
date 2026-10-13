# MySQL CDC live benchmark

This isolated harness compares the current `mysql_cdc 0.2.1` parser with the proposed
`mysql_async 0.37.1` adapter. Each implementation is a separate release binary and replays the
same retained file/position range. The baseline performs the current Reader's column-to-JSON
conversion and serialization. The candidate performs row conversion, transaction assembly, and
`CdcMessage` serialization. Output byte counts prevent either output path from being optimized
away; the richer candidate envelope is intentionally included in its cost.

The baseline supports plaintext only, so the comparable run uses the locally available MySQL 5.6
instance. MySQL 8.0 candidate-only load is separate evidence and is not used to calculate the
80-percent comparison ratio. The benchmark baseline enables vendored OpenSSL solely to make the
current `mysql_cdc` dependency build on Windows MSVC; the candidate remains OpenSSL-free.

Required process environment:

```text
RELUS_BENCH_HOST
RELUS_BENCH_PORT
RELUS_BENCH_USERNAME
RELUS_BENCH_PASSWORD
RELUS_BENCH_DATABASE
RELUS_BENCH_TABLE
RELUS_BENCH_BINLOG_FILE
RELUS_BENCH_BINLOG_POSITION
RELUS_BENCH_SERVER_ID
RELUS_BENCH_EXPECTED_ROWS
RELUS_BENCH_TIMEOUT_SECONDS
```

Optional candidate controls:

```text
RELUS_BENCH_MYSQL_COMPRESSION=1
RELUS_BENCH_CANDIDATE_MODE=decode|assemble|full
RELUS_BENCH_DIAGNOSTICS=1
```

`RELUS_BENCH_MYSQL_COMPRESSION` enables the candidate's explicit MySQL protocol-compression
setting. `RELUS_BENCH_CANDIDATE_MODE` and `RELUS_BENCH_DIAGNOSTICS` are diagnostic-only controls;
when diagnostics are disabled, the candidate does not execute per-message timing calls or
capacity accounting.

Build the binaries independently:

```text
cargo build --release --locked --manifest-path prototypes/mysql-cdc-live-benchmark/Cargo.toml --features baseline --bin baseline
cargo build --release --locked --manifest-path prototypes/mysql-cdc-live-benchmark/Cargo.toml --features candidate --bin candidate
```

Passwords and endpoint values are never printed. Peak process Working Set is sampled externally
so the memory result includes each implementation's complete process without linking both clients
into one binary.
