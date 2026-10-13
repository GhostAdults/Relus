# MySQL CDC Candidate Final Evaluation

Date: 2026-10-12

## Decision

Prototype candidate: **`mysql_async 0.37.1`**.

Production candidate: **`mysql_async 0.37.1`**.

Final status: **`recommended_for_production_migration`**.

The candidate satisfies every production-migration gate in the approved scope. The project owner
accepted the resolved transitive-license set and explicitly approved production migration. MySQL
5.6 and 8.0 both passed live INSERT/UPDATE/DELETE capture, automatic column/primary-key metadata
mapping, common-type fidelity, file/position restart, GTID restart, and confirmed-checkpoint
recovery. Their previously recorded authentication, transport-policy, cancellation, and startup
evidence remains valid. Verified server identity is retained as optional evidence only and is not
a production-migration gate in the current scope. Required Windows compile evidence passes. Live
memory boundedness and the large-transaction resource check pass. With explicit MySQL protocol
compression, optimized median candidate throughput is 101.59% of the current Reader baseline.

## Candidate

| Field | Result |
| --- | --- |
| Prototype candidate | `mysql_async 0.37.1` |
| Production candidate | `mysql_async 0.37.1` |
| Resolved parser dependency | `mysql_common 0.37.3` |
| Enabled features | `minimal-rust`, `rustls-tls`, `ring`, `tls12`, `binlog` |
| Reader transport optimization | Explicit MySQL protocol compression (`fast`) |
| Static score | `74/100` |
| Rustls | `rustls 0.23.45`, `tokio-rustls 0.26.5` |
| Fork required for prototype | No |
| Fork required for production migration | No current evidence requires a fork; any future fork remains separately approved |

## Approved Runtime Scope

The current production-migration version scope is MySQL 5.6 and MySQL 8.0. MySQL 5.7 and 8.4
remain `not_run` and outside this migration scope; the available results make no compatibility
claim for those versions.

The version-applicable gates are different:

- MySQL 5.6 requires ROW binlog, file/position, `mysql_native_password`, transactions, recovery,
  cancellation, and common-type fidelity.
- MySQL 8.0 additionally requires GTID, `caching_sha2_password`, encrypted TLS,
  file/position, and applicable replication/cancellation evidence. Verified server identity,
  wrong-CA, wrong-server-identity, and mutual-TLS remain optional follow-up evidence and are not
  current production gates.

## Executed Checks

The initial checks ran on Windows on September 26, 2026; performance evidence was added on
October 10 and the live DML/recovery plus current-code target checks were completed on October 12:

| Check | Result |
| --- | --- |
| Locked prototype test suite, 50 tests | `passed` |
| Locked prototype clippy with warnings denied | `passed` |
| Candidate compile for Windows MSVC | `passed` after the verification-harness changes |
| Candidate compile for Windows GNU | `passed` |
| Candidate dependency graph has no `openssl` | `passed` |
| Candidate dependency graph has no `openssl-sys` | `passed` |
| Live candidate-vs-current-Reader throughput | `passed` (101.59%; required minimum 80%) |
| Live 1M/2M-row memory boundedness | `passed` |
| Live 500,000-row transaction resource check | `passed` |
| Windows MSVC candidate performance binary build | `passed` |
| Windows MSVC current-Reader baseline build | `passed` (baseline executable produced after Perl installation; legacy default vendored orchestration remains non-gating) |
| Deterministic protocol and transaction proofs | `passed` |
| Deterministic recovery and checkpoint proofs | `passed` |
| Deterministic bounded-buffer and load proofs | `passed` |
| Deterministic type and schema proofs | `passed` |
| Live MySQL 5.6 and 8.0 DML/metadata/recovery tests | `passed` (2/2) |
| Current-code Windows GNU and Windows MSVC checks | `passed` |

The verification harness now isolates TLS inputs per MySQL version. A custom CA path is optional:
when absent, `mysql_async` uses built-in WebPKI roots while retaining certificate and server-name
validation. When a custom CA path is present, built-in roots are disabled. The strict required and
mutual modes expose no invalid-certificate or identity-validation bypass; the separate encrypted
mode intentionally skips those checks and is labelled compatibility-only.

## Runtime Matrix

| Environment or gate | Result | Reason |
| --- | --- | --- |
| MySQL 5.6 plaintext policy | `passed` | The configured account connected without TLS |
| MySQL 5.6 native authentication | `passed` | A read-only server lookup confirmed `mysql_native_password` |
| MySQL 5.6 ROW binlog and file/position | `passed` | The live instance exposed ROW binlog and a current retained position |
| MySQL 5.6 replication cancellation | `passed` | A real pending replication read cancelled and closed within the bound |
| MySQL 5.6 live DML, metadata, and recovery | `passed` | Real INSERT/UPDATE/DELETE, named-column and primary-key mapping, common types, file/position restart, and GTID restart passed |
| MySQL 5.6 encrypted TLS compatibility check | `failed` | The instance negotiated legacy TLSv1; this optional compatibility check is not a current migration gate |
| MySQL 8.0 plaintext policy | `passed` | The server required secure transport and rejected plaintext without fallback |
| MySQL 8.0 encryption-only TLS control | `passed` | The official client established TLS 1.3 with encryption required but identity verification disabled |
| MySQL 8.0 verified TLS | `not_run` | No custom CA path was supplied; this is optional follow-up evidence |
| MySQL 8.0 wrong-server-identity check | `not_run` | Requires a verified TLS baseline; not a current production gate |
| MySQL 8.0 wrong-CA check | `not_run` | Requires a verified TLS baseline; not a current production gate |
| MySQL 8.0 mutual TLS | `not_run` | Optional identity evidence was not configured |
| MySQL 8.0 caching authentication | `passed` | The account plugin was confirmed over encrypted TLS |
| MySQL 8.0 replication cancellation | `passed` | A file/position replication read cancelled and closed within the bound |
| MySQL 8.0 GTID runtime | `passed` | A GTID replication request connected and closed successfully |
| MySQL 8.0 live DML, metadata, and recovery | `passed` | Real INSERT/UPDATE/DELETE, named-column and primary-key mapping, common types, file/position restart, and GTID restart passed |
| MySQL 5.7 runtime | `not_run` | Outside the current migration scope |
| MySQL 8.4 runtime | `not_run` | Outside the current migration scope |
| Live current-reader baseline | `passed` | Same-server release baseline completed on MySQL 5.6 |
| Sustained live throughput | `passed` | Protocol-compressed candidate median was 101.59% of baseline; 80% is required |
| Sustained process memory | `passed` | Candidate median peaks were 12.60 MiB Working Set and 10.76 MiB Private Bytes for 2M rows, with no event-count-proportional growth |
| Linux x86_64 runtime | `not_run` | Deferred because the target environment requires changes; not a current gate |
| Windows GNU runtime | `not_run` | No separate live MySQL profile was supplied |
| Linux ARM64, macOS Apple Silicon, MUSL | `not_run` | Deferred target environments; not current gates |

The candidate's explicit `tls_encrypted` mode and the official MySQL 8.0 client control both
established encrypted TLS 1.3 without server identity verification. This is retained as encrypted
transport evidence only; it must not be described as verified TLS or used as a trust proof. The
verification command can still exit nonzero because the optional MySQL 5.6 encrypted-TLS check
records a genuine legacy-TLS failure; that exit status does not change the scoped gate result.

## Capability Results

| Capability | Result | Evidence |
| --- | --- | --- |
| File/position request and recovery | `passed` | Deterministic startup tests plus live 5.6 and 8.0 checkpoint restart |
| GTID request and recovery | `passed` | Deterministic startup tests plus live 5.6 and 8.0 checkpoint restart |
| INSERT, UPDATE, DELETE envelope semantics | `passed` | Transaction assembler tests plus live 5.6 and 8.0 DML |
| TABLE_MAP, ROTATE, heartbeat handling | `passed` | Adapter and envelope tests |
| Transaction ordering and commit boundary | `passed` | Multi-table and large-transaction tests |
| Stable replay identity and checkpoint semantics | `passed` | Recovery tests |
| Cancellation and clean shutdown seam | `passed` | Deterministic tests and live MySQL 5.6 cancellation |
| Event-count and byte-count backpressure | `passed` | Bounded-buffer tests |
| Configurable MySQL protocol compression | `passed` | Unit configuration proof and live same-coordinate A/B benchmark |
| Common MySQL value matrix | `passed` | Live 5.6 and 8.0 integer, unsigned-integer, decimal-text, binary, temporal, UTF-8, and null checks |
| Automatic table metadata mapping | `passed` | Live column-name, ordinal, primary-key, and unsigned-type discovery from `information_schema.columns` |
| Unsupported spatial types | `passed` | Schema validation tests |
| Schema drift and unsupported semantic events stop safely | `passed` | Schema and transaction tests |
| Encrypted TLS against MySQL 8.0 | `passed` | Candidate connected without server identity verification |
| TLS identity verification against MySQL 8.0 | `not_run` | Optional evidence; no custom CA path supplied |
| Caching authentication against MySQL 8.0 | `passed` | Live account/plugin verification over encrypted TLS |
| Native authentication against MySQL 5.6 | `passed` | Live connection and server-side plugin evidence |
| Current-reader throughput comparison | `passed` | Candidate median 241,515.81 rows/s versus baseline 237,744.72 rows/s |
| Large-transaction memory | `passed` | One 500,000-row transaction completed with 8.60 MiB peak Working Set |

Deterministic prototype results are not substitutes for live MySQL evidence.

## Test Safety

The compatibility checks were read-only. The performance harness created one dedicated database
containing generated benchmark data, deleted it after validation, and verified its schema count
as zero. No check changed server-global variables, users, grants, binlog files, certificate files,
or service state.

The verification harness accepts versioned configuration through environment variables and does
not print their values. Certificate inputs are path references only. Relus code does not open,
copy, parse, modify, or print certificate contents.

## Production Migration Gate Status

The revised version gate requires MySQL 5.6 and MySQL 8.0 rather than MySQL 5.7/8.0/8.4. Linux,
ARM64, Apple Silicon, and MUSL target environments are deferred because they require environment
changes and are not current production gates. Verified TLS, wrong-CA/name, and mutual TLS remain
optional follow-up evidence; encrypted-only TLS is not identity or trust evidence. All other
production gates remain unchanged.

All eight production-migration gates are `passed`:

1. MySQL 5.6 and 8.0 version-applicable runtime gates passed, including live DML, metadata,
   file/position recovery, and GTID recovery.
2. Windows GNU and Windows MSVC candidate checks passed on the current code.
3. Version-applicable transport, authentication, transaction, recovery, cancellation, and common
   type checks passed.
4. The selected candidate graph contains neither `openssl` nor `openssl-sys`.
5. Throughput, memory, large-transaction, and backpressure gates passed.
6. MSRV passed on Rust 1.93.0, and the project owner accepted the resolved license set.
7. No unresolved `blocked` item remains inside the approved scope. Historical alternative-candidate
   blocks and deferred Linux/ARM/macOS/MUSL checks are outside this migration scope. The legacy
   current-Reader vendored-OpenSSL orchestration is not a candidate gate; a Windows MSVC baseline
   executable was produced after Perl installation, while the production candidate remains
   independently OpenSSL-free.
8. The user reviewed the evidence and explicitly approved production migration.

### Gate 7 blocker disposition

| Previously blocked or incomplete evidence | Final disposition |
| --- | --- |
| Resolved transitive-license policy review | `passed`; the project owner accepted the selected graph |
| Candidate Windows GNU and Windows MSVC checks | `passed` on the current prototype code |
| Live DML, metadata, type fidelity, and recovery | `passed` on both in-scope versions, MySQL 5.6 and 8.0 |
| Live throughput, memory, and large-transaction evidence | `passed` |
| Linux x86_64, Linux ARM64, Apple Silicon, and MUSL | Explicitly deferred outside the approved migration scope |
| MySQL 5.7 and 8.4 | Explicitly outside the approved migration version scope |
| Verified TLS identity, negative trust tests, and mutual TLS | Explicitly optional follow-up evidence, not a current gate |
| Legacy current-Reader vendored-OpenSSL MSVC orchestration | Non-gating transitional diagnostic; an MSVC baseline executable was produced, and this dependency is removed by the migration |
| Blockers belonging only to rejected library alternatives | Disposed with those rejected candidates; they do not apply to `mysql_async 0.37.1` |

Every historical `blocked` entry is therefore either passed, explicitly deferred/outside scope,
optional by the approved specification, transitional to the component being replaced, or attached
to a rejected candidate. No unresolved blocker remains on the selected production candidate.

## Optional Follow-up

- Optionally supply a trust chain that verifies the MySQL 8.0 server identity; this remains
  follow-up evidence and is not a current production gate.
- Optionally supply a matching, server-accepted client certificate and private key for mutual TLS
  follow-up evidence.
- Keep MySQL protocol compression explicit in the Reader configuration and retain the uncompressed
  fallback for environments where compression is undesirable or unsupported.
- Preserve this evidence matrix when moving the adapter into the production Reader.

## Final Scope at Evaluation Completion

At the time this candidate evaluation completed, the candidate remained isolated under
`prototypes/mysql-cdc-candidate/` and was eligible for the separately approved production Reader
migration. The production implementation record below supersedes that historical state.

## Production Migration Implementation (2026-10-12)

The approved migration is now implemented in the working tree:

- Production source type is `mysql_cdc`; legacy `mysql_binlog` returns an explicit migration error.
- The production Reader uses exact `mysql_async 0.37.1` with Rustls-only features and no OpenSSL.
- Versioned configuration, secret references, TLS modes, startup modes, table-local routing and
  mapping, stable event envelopes, bounded row buffering, heartbeat/retry handling, and Redb
  checkpoints are connected to the Engine and database Sink.
- Sink commit acknowledgment gates checkpoint advancement. The declared guarantee remains
  `at_least_once`; target writes are not claimed to be one atomic cross-table transaction.
- Confirmed GTIDs are merged into the persisted resume set. Checkpoints are bound to the source
  identity, server UUID, database, table-rule fingerprint, and schema fingerprint; mismatches stop
  instead of silently resuming from an incompatible coordinate.
- UPDATE events that change the stable key delete the old-key row and then upsert the new-key row
  before commit acknowledgment. A failure between those operations remains replay-safe because the
  checkpoint is not advanced.
- Binary-vs-text conversion uses discovered MySQL column metadata. Binary, varbinary, blob, bit,
  and geometry values remain bytes even when their payload is valid UTF-8.
- `initial` uses a short global read lock to capture the Binlog coordinate, a single consistent
  snapshot transaction, stable-key ordering, streaming rows, target-policy preflight, and then
  incremental handoff from the captured coordinate.
- `initial_target_policy=require_empty` is checked by the database Sink and `upsert` is replay-safe.
  `truncate_then_load` is rejected until the Sink capability model can declare it safe.
- The checked-in CLI example now uses the production configuration shape and an environment
  reference for the MySQL password instead of literal credentials.

Local migration validation passed the common CDC contract tests, Reader tests, Writer tests,
Engine tests, host workspace check, and Windows GNU/MSVC workspace target checks. Root-workspace
inverse dependency checks report no package named `openssl`, `openssl-sys`, or `mysql_cdc`.
No production database, account, grant, certificate, service, or Binlog state was changed during
this implementation pass; runtime compatibility continues to rely on the already accepted MySQL
5.6/8.0 evidence recorded above.
