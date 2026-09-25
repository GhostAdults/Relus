# MySQL CDC library static evaluation

Date: 2026-09-25

Scope: GitHub issue [#28](https://github.com/GhostAdults/Relus/issues/28) and the approved `SPEC READY` design in `docs/specs/mysql-cdc-reader-redesign.md`

Evaluation type: static source/dependency analysis, isolated compile checks, and the issue #30 environment-aware runtime harness

## Decision

Select **`mysql_async = 0.37.1`** as the first independent prototype candidate, with exact features:

```toml
mysql_async = { version = "=0.37.1", default-features = false, features = [
  "minimal-rust",
  "rustls-tls",
  "ring",
  "tls12",
  "binlog",
] }
```

The isolated resolution used `mysql_common 0.37.3`. The prototype must commit its lockfile so that the complete graph, not only the direct version, is reproducible.

Overall status: **`conditionally_recommended`**. This is only the candidate selection required by phase 1. Protocol behavior, TLS identity verification, authentication, cancellation, resource bounds, MySQL-version support, and non-Windows targets remain `not_run` until the independent prototype supplies runtime or target-build evidence. It is not approved for production migration.

`mysql 28.0.2` exposes essentially the same Blackbeam binlog parser and wire primitives but uses a blocking iterator and blocking socket. It is **`rejected`** for the first prototype because active cancellation would require an independently managed blocking thread/socket ownership design, contrary to the hard gate against an uncontrolled blocking reader. `mysql-binlog-connector-rust 0.3.3` is **`rejected`** because the exact published release has no TLS backend at all; absence of OpenSSL is not equivalent to verified TLS.

## Meaning of statuses and evidence

The required status vocabulary is used literally:

- `passed`: the specifically stated static or compile check succeeded. A row marked “static” is not a runtime pass.
- `failed`: an executed check failed because the candidate or resolved graph did not satisfy it.
- `not_run`: no relevant runtime or target-build test was executed.
- `unsupported`: the exact candidate API/source lacks the required capability.
- `blocked`: the check could not be completed because the required environment/toolchain was unavailable.

Source code or upstream tests demonstrate an implementation surface, not that it works against Relus's MySQL, TLS, failure, or load matrix. Consequently, runtime subchecks remain `not_run` even where the static surface is `passed`.

## Candidate and dependency snapshot

| Candidate | Exact release | Release/source identity | Feature selection evaluated | Direct license | Maintenance evidence |
| --- | --- | --- | --- | --- | --- |
| `mysql_async` | `0.37.1` | tag commit [`ce4b276`](https://github.com/blackbeam/mysql_async/tree/ce4b27698c50fb945d8c9ff8c40a2a646be50b12); released 2026-09-01 | `minimal-rust,rustls-tls,ring,tls12,binlog` | MIT OR Apache-2.0 | Repository is not archived; release history includes 0.36.2, 0.37.0, and 0.37.1 in 2026 ([releases](https://github.com/blackbeam/mysql_async/releases)). |
| `mysql` | `28.0.2` | tag commit [`61aba4e`](https://github.com/blackbeam/rust-mysql-simple/tree/61aba4ef81a5dffd8f5083da8084417c5f79b25b); released 2026-09-01 | `minimal-rust,rustls-tls-ring,binlog` | MIT/Apache-2.0 | Repository is not archived; 28.0.2 is a 2026 release ([releases](https://github.com/blackbeam/rust-mysql-simple/releases)). |
| `mysql-binlog-connector-rust` | `0.3.3` | tag commit [`35534a2`](https://github.com/apecloud/mysql-binlog-connector-rust/tree/35534a20f7ddd1db4593145026706cb339057d82); published 2025-10-31 | no features exist in 0.3.3 | MIT OR Apache-2.0 | Repository is not archived, but 0.3.3 has no upstream CI target matrix. TLS landed only after this release in commit [`f7cca8e`](https://github.com/apecloud/mysql-binlog-connector-rust/commit/f7cca8ec2ebb55a1eb24c52702f9694c42d6a093). |

Primary metadata: [mysql_async 0.37.1](https://crates.io/crates/mysql_async/0.37.1), [mysql 28.0.2](https://crates.io/crates/mysql/28.0.2), and [mysql-binlog-connector-rust 0.3.3](https://crates.io/crates/mysql-binlog-connector-rust/0.3.3).

## Hard-gate matrix

The matrix summarizes static suitability. “passed (static)” means the released source exposes the required primitive; the corresponding behavior is still `not_run` in the prototype/runtime matrix below.

| Hard gate | `mysql_async 0.37.1` | `mysql 28.0.2` | `mysql-binlog-connector-rust 0.3.3` |
| --- | --- | --- | --- |
| Replication/binlog API | `passed` (static) | `passed` (static) | `passed` (static) |
| File/position request | `passed` (static) | `passed` (static) | `passed` (static) |
| MySQL GTID request/set | `passed` (static) | `passed` (static) | `passed` (static) |
| Required event parser surface | `passed` (static) | `passed` (static) | `passed` (static) |
| Transaction-boundary primitives | `passed` (static) | `passed` (static) | `passed` (static); assembly remains Relus-owned |
| Rustls without OpenSSL | `passed` (dependency/static) | `passed` (dependency/static) | `unsupported` (no TLS in exact release) |
| CA and server identity verification | `passed` (static API/default verifier path) | `passed` (static API/default verifier path) | `unsupported` |
| Mutual TLS identity | `passed` (static API) | `passed` (static API) | `unsupported` |
| `mysql_native_password` | `passed` (static) | `passed` (static) | `passed` (static) |
| `caching_sha2_password` | `passed` (static) | `passed` (static) | `passed` (static, but plaintext/RSA security caveat) |
| Active cancellation/no blocking worker | `not_run` (async ownership model is a suitable prototype seam) | `unsupported` as a direct API; needs an external blocking-thread design | `not_run` / unverified; no independent cancellation handle |
| Consumer backpressure | `passed` (static pull stream; byte bounds still absent) | `passed` (static pull iterator; blocking) | `passed` (static pull API; large packets are fully buffered) |
| Workspace MSRV 1.93 | `passed` on checked Windows targets | `passed` on checked Windows targets | `passed` on host only; MSVC check was not completed |
| Permissive direct license | `passed` | `passed` | `passed` |
| Transitive license hard gate | `blocked` pending policy/legal review of non-enumerated SPDX choices | `blocked` pending policy/legal review; `webpki 0.22.4` has missing registry license metadata | `blocked` pending policy/legal review of non-enumerated SPDX choices |
| Windows GNU compile | `passed` | `passed` | `passed` |
| Windows MSVC compile | `passed` | `passed` | `blocked` (check was stopped before completion) |
| Linux x86_64 compile | `blocked` (cross C compiler absent) | `blocked` (cross C compiler absent) | `not_run` |
| Linux ARM64/macOS Apple Silicon/MUSL | `not_run` | `not_run` | `not_run` |
| MySQL 5.7/8.0/8.4 runtime | `not_run` | `not_run` | `not_run`; upstream README only reports 5.6.51/5.7.40/8.0.31 |

## Detailed evidence

### 1. `mysql_async 0.37.1`

#### Replication, offsets, events, and transaction boundaries

`BinlogStreamRequest` supports `with_filename`, `with_pos`, `with_gtid`, and `with_gtid_set`, and `Conn::get_binlog_stream` turns the authenticated connection into an asynchronous `Stream<Item = Result<Event>>` ([request source](https://github.com/blackbeam/mysql_async/blob/ce4b27698c50fb945d8c9ff8c40a2a646be50b12/src/conn/binlog_stream/request.rs), [stream source](https://github.com/blackbeam/mysql_async/blob/ce4b27698c50fb945d8c9ff8c40a2a646be50b12/src/conn/binlog_stream/mod.rs)). The upstream test exercises both `COM_BINLOG_DUMP` and `COM_BINLOG_DUMP_GTID`, row decoding, non-blocking dump mode, and explicit stream close. This is strong source/test evidence, but Relus did not execute that MySQL test in this phase.

With feature `binlog`, parsing comes from `mysql_common`. The event model includes generic event headers/positions, GTID and previous-GTID events, table-map and rows events, rotate, query, XID, heartbeat, and MySQL 8 transaction-payload support ([mysql_common binlog events](https://docs.rs/mysql_common/0.37.3/mysql_common/binlog/events/index.html), [event type enum](https://docs.rs/mysql_common/0.37.3/mysql_common/binlog/consts/enum.EventType.html)). `mysql_async` also expands compressed `TRANSACTION_PAYLOAD_EVENT` entries through its stream reader. Relus must still assemble BEGIN/GTID/row/XID or COMMIT into its own transaction envelope and derive confirmed offsets/event IDs; the crate does not implement Relus checkpoint semantics.

#### TLS and authentication

The selected features use `rustls 0.23` plus `tokio-rustls`, web PKI roots, TLS 1.2, and the ring provider; `native-tls` is optional and is not enabled ([manifest](https://github.com/blackbeam/mysql_async/blob/ce4b27698c50fb945d8c9ff8c40a2a646be50b12/Cargo.toml)). `SslOpts` accepts a root certificate path and a client identity, while the Rustls implementation builds a root store and uses `ServerName` for normal certificate and identity validation ([options](https://docs.rs/mysql_async/0.37.1/mysql_async/struct.SslOpts.html), [Rustls I/O source](https://github.com/blackbeam/mysql_async/blob/ce4b27698c50fb945d8c9ff8c40a2a646be50b12/src/io/tls/rustls_io.rs), [client identity source](https://github.com/blackbeam/mysql_async/blob/ce4b27698c50fb945d8c9ff8c40a2a646be50b12/src/opts/rustls_opts.rs)). The dangerous invalid-certificate/domain options exist, so the prototype adapter must never expose or enable them. Required and mutual TLS, correct/wrong CA, and correct/wrong server name are all `not_run`.

The crate documents and implements `mysql_native_password` and `caching_sha2_password`; the latter includes secure-channel and RSA continuation paths ([crate documentation](https://docs.rs/mysql_async/0.37.1/mysql_async/), [connection authentication source](https://github.com/blackbeam/mysql_async/blob/ce4b27698c50fb945d8c9ff8c40a2a646be50b12/src/conn/mod.rs)). Authentication runtime results are `not_run`.

#### Cancellation and backpressure

`BinlogStream` is polled by the consumer and owns its `Conn`; `close(self)` closes that connection. There is no library-owned producer thread or unbounded channel in the binlog path ([stream source](https://github.com/blackbeam/mysql_async/blob/ce4b27698c50fb945d8c9ff8c40a2a646be50b12/src/conn/binlog_stream/mod.rs)). A prototype can race `next()` with a cancellation token and, on cancellation, drop/close the owned stream; this is a source-level fit for active cancellation and natural read backpressure. Runtime cancellation latency and absence of resource leaks are `not_run`.

The parser still materializes a protocol packet and may decompress a transaction payload into memory. Therefore it does not itself meet Relus's event-and-byte buffer limits or large-transaction streaming promise. Those controls and measurements belong in the adapter/prototype and remain `not_run`.

#### MSRV, dependencies, licenses, and targets

Neither `mysql_async 0.37.1` nor resolved `mysql_common 0.37.3` declares `rust-version`. The isolated graph's highest declared Rust version was 1.88, below Relus's workspace `rust-version = 1.93.0`. `cargo check --locked` passed with rustc 1.93.0 on the host `x86_64-pc-windows-gnu`, and `cargo check --locked --target x86_64-pc-windows-msvc` passed.

Both `cargo tree -i openssl` and `cargo tree -i openssl-sys` reported no matching package for the selected feature graph. Direct dependencies are permissively licensed, but the resolved graph contains SPDX choices outside the spec's short allow-list, notably Unicode-3.0 data crates and target-conditional `r-efi` metadata containing an LGPL alternative alongside MIT/Apache options. This is not evidence of a forbidden GPL/AGPL-only runtime dependency, but the strict production license gate remains `blocked` pending the project's automated license policy/legal confirmation.

Linux x86_64 cross-check was attempted and blocked because `x86_64-linux-gnu-gcc` was unavailable while `ring` and `zstd-sys` ran build scripts. Linux ARM64, Apple Silicon, and MUSL were not run. `zstd-sys` means “no OpenSSL” does not mean “pure Rust/static build proven”; the Phase 4 target matrix remains mandatory.

### 2. `mysql 28.0.2`

`mysql` uses the same `mysql_common 0.37.x` binlog model and provides `Conn::get_binlog_stream`, `BinlogRequest` file/position and GTID requests, row/event parsing, and Rustls TLS features ([binlog stream](https://github.com/blackbeam/rust-mysql-simple/blob/61aba4ef81a5dffd8f5083da8084417c5f79b25b/src/conn/binlog_stream.rs), [connection/request tests](https://github.com/blackbeam/rust-mysql-simple/blob/61aba4ef81a5dffd8f5083da8084417c5f79b25b/src/conn/mod.rs), [manifest](https://github.com/blackbeam/rust-mysql-simple/blob/61aba4ef81a5dffd8f5083da8084417c5f79b25b/Cargo.toml)). Its TLS options include custom roots/client identity and normal Rustls identity verification, and its authentication code handles `mysql_native_password` and `caching_sha2_password` ([TLS options](https://github.com/blackbeam/rust-mysql-simple/blob/61aba4ef81a5dffd8f5083da8084417c5f79b25b/src/conn/opts/mod.rs), [Rustls I/O](https://github.com/blackbeam/rust-mysql-simple/blob/61aba4ef81a5dffd8f5083da8084417c5f79b25b/src/io/tls/rustls_io.rs)).

The critical difference is integration behavior: `BinlogStream` implements blocking `Iterator`, and its read path is a blocking socket read. Cleanly interrupting an in-flight read requires additional thread/socket coordination outside the crate. That is a core runtime hard gate for this project, not a cosmetic adapter patch, so it is not the first prototype candidate.

The selected graph passed host Windows GNU and Windows MSVC checks and contains neither `openssl` nor `openssl-sys`. Linux cross-compilation was blocked by the same missing C cross compiler for `ring`/`zstd-sys`. The registry metadata for optional `webpki 0.22.4` has no SPDX `license` field, so the strict transitive-license gate is `blocked` even though the upstream project is published under a permissive license. All runtime behavior remains `not_run`.

### 3. `mysql-binlog-connector-rust 0.3.3`

The crate is purpose-built for binlog streaming. `StartPosition` supports file/position, GTID, and latest; source encodes both dump command forms and parses GTID, query, table-map, write/update/delete rows, XID, rotate, heartbeat, and transaction-payload events ([client](https://github.com/apecloud/mysql-binlog-connector-rust/blob/35534a20f7ddd1db4593145026706cb339057d82/src/binlog_client.rs), [parser](https://github.com/apecloud/mysql-binlog-connector-rust/blob/35534a20f7ddd1db4593145026706cb339057d82/src/binlog_parser.rs), [event variants](https://github.com/apecloud/mysql-binlog-connector-rust/blob/35534a20f7ddd1db4593145026706cb339057d82/src/event/event_data.rs)). Upstream documents testing on MySQL 5.6.51, 5.7.40, and 8.0.31, but Relus has not reproduced those tests, and there is no 8.4 evidence ([README](https://github.com/apecloud/mysql-binlog-connector-rust/blob/35534a20f7ddd1db4593145026706cb339057d82/README.md)).

The release's manifest contains no TLS crate, TLS feature, or TLS configuration, and `PacketChannel` stores a plain `async_std::net::TcpStream` ([0.3.3 manifest](https://github.com/apecloud/mysql-binlog-connector-rust/blob/35534a20f7ddd1db4593145026706cb339057d82/Cargo.toml), [packet channel](https://github.com/apecloud/mysql-binlog-connector-rust/blob/35534a20f7ddd1db4593145026706cb339057d82/src/network/packet_channel.rs)). Thus `openssl`/`openssl-sys` are absent, but TLS required, CA verification, server identity verification, and mutual TLS are `unsupported`. This is a decisive hard-gate rejection.

Upstream added optional TLS after 0.3.3 in commit [`f7cca8e`](https://github.com/apecloud/mysql-binlog-connector-rust/commit/f7cca8ec2ebb55a1eb24c52702f9694c42d6a093), tagged as 0.3.4 in the repository. That change uses a custom no-certificate-verification path; it does not rescue the identity-verification hard gate and would require security-sensitive upstream work or a separately approved fork. No fork is proposed or adopted by this report.

Authentication source recognizes `mysql_native_password` and `caching_sha2_password` and implements plugin switching/RSA continuation ([plugin enum](https://github.com/apecloud/mysql-binlog-connector-rust/blob/35534a20f7ddd1db4593145026706cb339057d82/src/command/auth_plugin.rs), [authenticator](https://github.com/apecloud/mysql-binlog-connector-rust/blob/35534a20f7ddd1db4593145026706cb339057d82/src/command/authenticator.rs)). In 0.3.3 this operates without an authenticated TLS channel, so it is not sufficient for the approved security design.

The stream has `close(&mut self)`, but both `read(&mut self)` and `close(&mut self)` require the same mutable object; there is no independent cancellation handle to interrupt an in-flight read ([stream](https://github.com/apecloud/mysql-binlog-connector-rust/blob/35534a20f7ddd1db4593145026706cb339057d82/src/binlog_stream.rs)). A caller may be able to cancel and drop the read future, regain the stream, and then close it, but that future-cancellation safety is undocumented and was not tested. The correct classification is therefore `not_run` / unverified, not `unsupported`. Timeouts bound a read, but are not active cancellation. Its pull API provides slow-consumer backpressure, while full protocol packets and compressed transaction payloads are allocated/decompressed in memory, so byte-bound and large-transaction requirements remain `not_run`.

The crate does not declare `rust-version`. The isolated host check passed on rustc 1.93.0 (`x86_64-pc-windows-gnu`) and the graph contains neither `openssl` nor `openssl-sys`. The interrupted Windows MSVC check is `blocked`, not passed. Other targets were not run. Direct license is MIT OR Apache-2.0; the complete graph still needs the same production license-policy review noted above.

## Static scoring

Scores are comparative static estimates, not acceptance results. A hard-gate rejection is not overridden by a high parser score.

| Category | Weight | `mysql_async` | `mysql` | `mysql-binlog-connector-rust` |
| --- | ---: | ---: | ---: | ---: |
| Protocol and event correctness | 30 | 25 | 25 | 25 |
| TLS, authentication, security | 20 | 17 | 17 | 4 |
| Cross-platform and MUSL evidence | 15 | 8 | 8 | 5 |
| Cancellation, backpressure, async integration | 15 | 12 | 4 | 8 |
| Maintenance and code quality | 10 | 8 | 8 | 5 |
| Performance evidence | 5 | 0 | 0 | 0 |
| Relus migration size | 5 | 4 | 2 | 4 |
| **Static total / 100** | **100** | **74** | **64** | **51** |

## Prototype obligations for `mysql_async 0.37.1`

The selection becomes invalid if the prototype cannot establish all of the following without modifying production code:

1. Lock `mysql_async 0.37.1`, the resolved `mysql_common` version, and selected features in the independent prototype.
2. Re-run `cargo tree -i openssl` and `cargo tree -i openssl-sys`; both must report no matching package.
3. Prove required and mutual Rustls connections with correct CA/server identity, and rejection for wrong CA/name; never enable dangerous verification bypasses.
4. Prove `mysql_native_password` and `caching_sha2_password` against available environments.
5. Parse and preserve INSERT, UPDATE, DELETE, TABLE_MAP, ROTATE, GTID, XID/COMMIT, HEARTBEAT, file/position, and transaction order.
6. Demonstrate cancellation of a pending read and clean shutdown with no leaked worker or connection.
7. Add explicit event-count and byte-count backpressure above the pull stream; measure large transactions and compressed transaction payloads.
8. Implement Relus-owned transaction assembly, stable event IDs, error classification, and checkpoint experiment; do not treat the crate as providing these semantics.
9. Run the available Windows MSVC/MySQL 8.0 and compatibility-only MySQL 5.6 matrix. Keep MySQL 5.7, 8.4, Linux runtime, ARM64, Apple Silicon, and MUSL as `not_run` until genuinely executed.
10. Complete target builds and a production license audit before requesting production migration.

## Reproducible commands used

The candidate manifests were created under a system temporary directory, outside the repository and default workspace. No production manifest or lockfile was changed.

```text
cargo info mysql_async --registry crates-io
cargo info mysql --registry crates-io
cargo info mysql-binlog-connector-rust --registry crates-io

cargo tree --manifest-path <temp>/mysql-async-eval/Cargo.toml --all-features
cargo tree --manifest-path <temp>/mysql-async-eval/Cargo.toml -i openssl
cargo tree --manifest-path <temp>/mysql-async-eval/Cargo.toml -i openssl-sys
cargo metadata --manifest-path <temp>/mysql-async-eval/Cargo.toml --format-version 1 --locked

# Equivalent tree/metadata commands were run for mysql-sync-eval and
# binlog-connector-eval.

cargo check --locked --manifest-path <temp>/mysql-async-eval/Cargo.toml
cargo check --locked --target x86_64-pc-windows-msvc --manifest-path <temp>/mysql-async-eval/Cargo.toml
cargo check --locked --manifest-path <temp>/mysql-sync-eval/Cargo.toml
cargo check --locked --target x86_64-pc-windows-msvc --manifest-path <temp>/mysql-sync-eval/Cargo.toml
cargo check --locked --manifest-path <temp>/binlog-connector-eval/Cargo.toml
```

Host evidence was `rustc 1.93.0` with host triple `x86_64-pc-windows-gnu`. The successful host checks therefore count as Windows GNU compile checks, not Windows MSVC runtime tests. Linux cross-checks for the Blackbeam candidates were blocked by the missing `x86_64-linux-gnu-gcc`; they are not reported as candidate failures.

## Runtime and platform matrix after this static phase

| Evidence item | Result |
| --- | --- |
| Windows GNU compile: selected `mysql_async` graph | `passed` |
| Windows MSVC compile: selected `mysql_async` graph | `passed` |
| OpenSSL absence: selected `mysql_async` graph | `passed` |
| MySQL 5.6 compatibility runtime | `not_run` |
| MySQL 5.7 formal runtime | `not_run` |
| MySQL 8.0 formal runtime | `not_run` |
| MySQL 8.4 formal runtime | `not_run` |
| TLS/auth/mTLS runtime | `not_run` |
| File-position/GTID/recovery runtime | `not_run` |
| Transaction/event/type fidelity runtime | `not_run` |
| Cancellation/reconnect/backpressure runtime | `not_run` |
| Performance and memory gates | `not_run` |
| Linux x86_64 compile/runtime | `blocked` / `not_run` |
| Windows GNU runtime | `not_run` |
| Linux ARM64 compile | `not_run` |
| macOS Apple Silicon compile | `not_run` |
| MUSL static-target compile | `not_run` |

## Approval boundary

This report selects the first prototype candidate only. It does not modify the production Reader, adopt a fork, remove `mysql_cdc`/OpenSSL, change the workspace dependency graph, or authorize production migration. Those actions remain behind the explicit gates in the approved specification.

## Issue #30 verification update

Date: 2026-09-25

Issue: [#30](https://github.com/GhostAdults/Relus/issues/30)

The independent prototype now accepts explicit `disabled`, identity-verified `required`, and
`mutual` TLS modes. Required and mutual modes configure only caller-supplied certificate paths,
disable built-in roots, and retain Rustls CA and server-name validation. Relus code does not read,
copy, modify, print, or parse certificate contents; loading and parsing remain inside
`mysql_async`. The adapter exposes no invalid-certificate or hostname-validation bypass.

The new `verify` binary reports each available MySQL/version capability as `passed`, `failed`, or
`not_run`. Negative TLS cases only pass when the connection returns the stable Relus `Tls` error
classification, so a network or authentication failure cannot masquerade as proof that an invalid
CA or server identity was rejected. Authentication-specific accounts are optional and missing
profiles are reported as `not_run`. A successful authentication case also runs
`SHOW CREATE USER CURRENT_USER()` and verifies that the server-side account definition names the
expected plugin; the returned definition is not printed or included in errors. The wrong-CA case
uses the separately supplied client-certificate path as an intentionally incorrect trust root,
and remains `not_run` when no second approved certificate path is available.

Cancellation is implemented without a blocking worker: the consumer races the pending binlog
read against a cancellation token, drops the read future, and then closes the stream-owned
connection within a bounded timeout. A public-boundary test proves that the pending read future is
dropped and the close path completes without leaving an active read. A real replication
connection check is included in the environment-aware harness and remains `not_run` when no
approved offset/server ID is supplied.

Commands executed for this update:

```text
cargo test --manifest-path prototypes/mysql-cdc-candidate/Cargo.toml
cargo clippy --locked --manifest-path prototypes/mysql-cdc-candidate/Cargo.toml --all-targets -- -D warnings
cargo run --manifest-path prototypes/mysql-cdc-candidate/Cargo.toml --bin verify
cargo check --locked --manifest-path prototypes/mysql-cdc-candidate/Cargo.toml --target x86_64-pc-windows-gnu
cargo check --locked --manifest-path prototypes/mysql-cdc-candidate/Cargo.toml --target x86_64-pc-windows-msvc
cargo check --locked --manifest-path prototypes/mysql-cdc-candidate/Cargo.toml --target x86_64-unknown-linux-gnu
cargo tree --locked --manifest-path prototypes/mysql-cdc-candidate/Cargo.toml -i openssl
cargo tree --locked --manifest-path prototypes/mysql-cdc-candidate/Cargo.toml -i openssl-sys
```

| Issue #30 evidence item | Result |
| --- | --- |
| TLS mode configuration and path redaction tests | `passed` |
| Cancellation future/close behavior at Relus-owned seam | `passed` |
| Windows GNU compile | `passed` |
| Windows MSVC compile | `passed` |
| Linux x86_64 cross-compile | `blocked` (`x86_64-linux-gnu-gcc` unavailable for `ring`/`zstd-sys`) |
| Linux ARM64 compile | `not_run` (target/toolchain unavailable) |
| macOS Apple Silicon compile | `not_run` (target/toolchain unavailable) |
| MUSL compile | `not_run` (target/toolchain unavailable) |
| MySQL 5.6 TLS/auth/cancellation runtime | `not_run` (versioned environment unavailable) |
| MySQL 5.7 TLS/auth/cancellation runtime | `not_run` (versioned environment unavailable) |
| MySQL 8.0 TLS/auth/cancellation runtime | `not_run` (versioned environment unavailable) |
| MySQL 8.4 TLS/auth/cancellation runtime | `not_run` (versioned environment unavailable) |
| `openssl` inverse dependency | `passed` (no matching package) |
| `openssl-sys` inverse dependency | `passed` (no matching package) |

These results do not upgrade the candidate beyond `conditionally_recommended`: the approved live
MySQL environments and certificate paths were absent, Linux compilation remains blocked by a
missing cross C compiler, and the remaining formal target toolchains were unavailable.

## Issue #31 recovery update

Date: 2026-09-25

Issue: [#31](https://github.com/GhostAdults/Relus/issues/31)

The prototype now contains a simplified persistent checkpoint and recovery loop outside the
production Reader/Engine/Writer path. A checkpoint is the last complete transaction acknowledged
by the `PrototypeSink` seam. File/position values persist directly; GTID values are normalized
with the candidate library's `Sid` parser so the completed transaction becomes part of the next
replication resume set.

File checkpoints use asynchronous filesystem I/O, a versioned JSON record, monotonic sequence,
fsynced temporary file, and atomic rename. The interrupted-checkpoint failpoint stops after the new
temporary record is durable but before replacement; a restarted store reads the prior confirmed
record and safely replaces the leftover temporary file on the next save. The real
`MySqlRecoveryClientFactory` injects that confirmed offset into each new
`MySqlAsyncReplicationClient`.

The recovery loop classifies connection, read, and representative MySQL server errors through the
stable Relus error type. Only errors marked retryable reconnect, reconnect count is bounded, delay
uses exponential growth with jitter, and every retry starts from the last confirmed checkpoint. A
valid replication heartbeat completes a read window; subsequent silence triggers the same bounded
reconnect path. Unexpected EOF both during and between transactions reconnects from the confirmed
checkpoint; incomplete work replays. Permanent errors return immediately.

The public-boundary recovery tests cover all five requested failpoints: before sink write, after
sink acknowledgment but before checkpoint, during atomic checkpoint replacement, during a
multi-row transaction, and after a snapshot-complete checkpoint but before incremental handoff.
They prove stable replay IDs, allowed replay of unconfirmed writes, no checkpoint advancement when
the sink rejects a transaction, and no checkpoint crossing an incomplete transaction.

| Issue #31 evidence item | Result |
| --- | --- |
| File/position checkpoint persistence and restart offset | `passed` |
| GTID checkpoint normalization and restart offset | `passed` |
| Checkpoint after sink acknowledgment only | `passed` |
| Stable replay identity for unconfirmed transactions | `passed` |
| Retryable/permanent connection and read classification | `passed` |
| Bounded reconnect from last confirmed checkpoint | `passed` |
| Heartbeat activity and silence detection | `passed` |
| Exit before sink write | `passed` |
| Exit after sink acknowledgment before checkpoint | `passed` |
| Exit during checkpoint replacement | `passed` |
| Exit during multi-row transaction | `passed` |
| Exit after snapshot checkpoint before incremental handoff | `passed` |
| Live MySQL file/position and GTID restart | `not_run` (versioned environment unavailable) |

These are deterministic prototype proofs, not production checkpoint authorization. They do not
modify the production Reader, Engine, Writer, Redb schema, snapshot implementation, or startup
mode selection.
