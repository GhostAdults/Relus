# MySQL CDC Live Performance Evaluation

Date: 2026-10-12

## Decision

The optimized `mysql_async 0.37.1` prototype **passed** the required throughput gate. Its median
row-event throughput was **101.59%** of the current `mysql_cdc 0.2.1` baseline, above the required
80%. Process memory remained bounded during the 2,000,000-row replay.

This result makes performance and resource use non-blocking for the current production-migration
scope. The project owner subsequently accepted the resolved license set and explicitly approved
production migration; the consolidated decision is recorded in the final evaluation.

## Method

- Platform: Windows x86_64 GNU release binaries.
- Server: the locally available MySQL 5.6 instance. The current `mysql_cdc` implementation does
  not support TLS, so MySQL 5.6 is the available same-server comparison environment.
- Dataset: 2,000,000 inserted rows, 2,000 transactions, four columns, and a 128-byte text payload.
- Replay: both implementations consumed the same retained file/position range.
- Runs: one warm-up followed by three measured runs per implementation with alternating order.
- Baseline boundary: decode rows, perform the current Reader's column-to-JSON conversion, and
  serialize every output row.
- Candidate boundary: decode rows, create Relus values, assemble transactions, and serialize every
  `CdcMessage`.
- Candidate transport: explicitly configured MySQL protocol compression at the driver's fast
  compression level. No server-global variable was changed.
- Resource sampling: process Working Set and Private Bytes sampled externally every 10 ms.

The candidate deliberately emits a richer envelope. It serialized **1,515,918,220 bytes** while
the baseline serialized **601,777,900 bytes** for the same rows. The richer output remains part of
the proposed design and was not removed to obtain the pass.

## Optimizations Retained

- Each `CdcMessage` is serialized once and the exact bytes are reused for buffer accounting and
  Sink output.
- JSON serialization starts with a 768-byte output allocation. In the measured million-row
  diagnostic workload, all 1,002,000 messages fit without a growth allocation.
- Row maps use a compact sorted representation; column sort order is computed once per RowsEvent
  instead of once per row.
- Database, table, column, transaction, offset, and row event-ID prefix metadata is shared.
- Binlog rows are decoded in place without two intermediate value vectors, and primary-key rows
  are moved rather than cloned.
- Common scalar `BinlogValue::Value` entries bypass the generic JSONB conversion path.
- Concrete adapter calls use inherent async methods and avoid the boxed `async_trait` future.
- MySQL protocol compression is an explicit Reader connection option. A same-coordinate A/B run
  measured a compressed median of **266,590.98 rows/s** versus **240,536.86 rows/s** without
  compression, a **10.83%** improvement.

Client-level table metadata caching and a shared-column-array `CdcRow` representation were tested
and removed because controlled A/B measurements showed no improvement or a regression. The live
benchmark's optional diagnostic mode times socket reading, event parsing, Relus mapping,
transaction assembly, and serialization only when `RELUS_BENCH_DIAGNOSTICS` is set; the final
results below ran with diagnostics disabled.

## Throughput Results

| Implementation | Run 1 rows/s | Run 2 rows/s | Run 3 rows/s | Median rows/s |
| --- | ---: | ---: | ---: | ---: |
| `mysql_cdc 0.2.1` baseline | 238,515.00 | 237,744.72 | 233,055.60 | **237,744.72** |
| `mysql_async 0.37.1` candidate | 208,678.57 | 241,515.81 | 279,026.49 | **241,515.81** |

Candidate/baseline median ratio: **101.59%**.

Required ratio: **80%**.

Result: **`passed`**.

The individual candidate runs still show local socket/read variability, so the decision uses the
required median rather than a single fast run.

Windows MSVC candidate builds pass. Perl is now installed, OpenSSL configuration/compilation
completed, and a Windows MSVC current-Reader baseline executable was produced. A subsequent clean
default vendored-OpenSSL orchestration still exited from `openssl-sys` after producing its static
libraries; this is retained as a non-gating legacy packaging diagnostic. It does not affect the
already executed same-server performance comparison or the selected candidate, whose dependency
graph contains no OpenSSL.

## Resource Results

| Measurement | Baseline | Candidate | Result |
| --- | ---: | ---: | --- |
| Median peak Working Set, 2,000,000 rows | 7.62 MiB | 12.60 MiB | Bounded |
| Median peak Private Bytes, 2,000,000 rows | 1.61 MiB | 10.76 MiB | Bounded |
| Candidate single-transaction rows | - | 500,000 | Completed in prior resource run |
| Candidate single-transaction peak Working Set | - | 8.60 MiB | Bounded |
| Candidate single-transaction peak Private Bytes | - | 3.11 MiB | Bounded |

Protocol compression adds a fixed connection/buffer cost compared with the uncompressed candidate,
but the 2,000,000-row run did not show memory growth proportional to total event count. The
bounded event buffer continues to account for the exact serialized bytes and enforces its
configured event and byte limits independently of protocol compression.

## Scope And Safety

The test used one dedicated database containing generated benchmark data. That database was
deleted after validation, and its schema count was verified as zero. The test changed no
server-global variables, users, grants, certificates, services, or binlog files. The report
contains no endpoint, credential, certificate path, user directory, server UUID, or complete
binlog coordinate.

The benchmark harness is isolated under `prototypes/mysql-cdc-live-benchmark/` and is outside the
root workspace. Its baseline feature intentionally resolves `mysql_cdc` and OpenSSL; this does not
change the selected candidate's independent OpenSSL-free dependency graph.

## Gate Status

The throughput and resource gates are closed. License review, scoped-blocker disposition, and user
approval are recorded as passed in `docs/research/mysql-cdc-final-evaluation.md`. No performance or
resource item remains `blocked` inside the approved migration scope.
