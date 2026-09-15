# JobConfig Parsing and Planner Boundary

Status: published as https://github.com/GhostAdults/Relus/issues/7

## Problem Statement

The first Planner migration prepared an `ExecutionPlan`, but its public boundary still mixes raw JSON parsing with planning. Production entry points also deserialize `JobConfig` independently, while the configuration model uses inconsistent source/target terminology. This leaves multiple parsing paths, inconsistent error behavior, and an unfinished migration boundary.

## Solution

Make `JobConfig` the single typed configuration boundary and make the execution flow:

`external JSON -> JobConfig::parse_* -> JobConfig -> Planner -> ExecutionPlan -> Runner/Pipeline`

`JobConfig` owns JSON and structural parsing. `Planner` accepts an already parsed `JobConfig` and owns semantic validation, component creation, `RecordBuilder` construction, pipeline resolution, and the single Reader split. All CLI, HTTP, and Scheduler paths use the same parsing API before calling the existing `start_job(JobConfig)` execution entrypoint.

The canonical data-source fields become `source` and `sink`. Compatibility aliases remain accepted at the parsing boundary: `input` for `source`, and `target` or `output` for `sink`.

## User Stories

1. As a Relus maintainer, I want every external configuration input parsed through one `JobConfig` API, so that configuration behavior is consistent across CLI, HTTP, and Scheduler.
2. As a Relus maintainer, I want JSON syntax failures reported before planning, so that malformed documents never reach Reader or Writer creation.
3. As a Relus maintainer, I want structural field errors reported by `JobConfig`, so that parsing does not depend on runtime registries.
4. As a Relus maintainer, I want semantic configuration errors reported by Planner, so that planning failures are separated from syntax and shape failures.
5. As a configuration author, I want `source` and `sink` to be the canonical field names, so that the data flow is explicit.
6. As a configuration author, I want existing `input`, `target`, and `output` documents to remain readable, so that this refactor does not require an immediate configuration migration.
7. As a Relus maintainer, I want parsed configurations to expose only canonical `source` and `sink` fields downstream, so that aliases do not leak into execution code.
8. As a Relus maintainer, I want `JobConfig::parse_json` and `JobConfig::parse_value` to share one structural validation path, so that string and value inputs cannot diverge.
9. As a Relus maintainer, I want a compatibility parsing mode for unknown fields, so that historical configuration files continue to work.
10. As a Relus maintainer, I want an explicit strict parsing mode, so that callers can detect misspelled or unsupported fields when desired.
11. As a Relus maintainer, I want parse errors to identify their phase and field path, so that users can correct configuration efficiently.
12. As a Relus maintainer, I want `start_job(JobConfig)` to remain the common execution entrypoint, so that existing callers can migrate without introducing a second runtime API.
13. As a Scheduler user, I want schedule metadata to remain configuration data while runtime schedule state stays outside `JobConfig`, so that planning and scheduling remain separate concerns.
14. As a Scheduler user, I want each firing to create a fresh plan from a parsed configuration, so that prepared readers, writers, and split tasks are not reused.
15. As a test author, I want to construct `JobConfig` directly in unit tests, so that tests do not need needless serialize/deserialize round trips.
16. As a test author, I want external-input tests to exercise the public parsing API, so that compatibility aliases and error contracts are protected.
17. As a Relus maintainer, I want Runner and Pipeline to consume only `ExecutionPlan`, so that future configuration changes do not propagate into runtime execution.
18. As a Relus maintainer, I want Reader splitting to remain authoritative in the plan, so that planning and execution never perform duplicate splits.

## Implementation Decisions

- Add `JobConfig::parse_json` for text input and `JobConfig::parse_value` for an existing JSON value.
- Both parsing methods use the same structural parsing and validation implementation.
- Keep serde deserialization as an implementation mechanism, but production entry points must not call it directly for `JobConfig`.
- Provide compatibility and strict unknown-field behavior. Compatibility is the default for the public convenience parser; strict behavior is explicitly selectable.
- Use stage-qualified errors for JSON syntax, structural parsing, and field-path failures. Planner retains separate semantic, component-creation, RecordBuilder, pipeline, and split context.
- Rename the canonical destination field in `JobConfig` from `target` to `sink`.
- Accept `input` as the compatibility alias for `source`.
- Accept `target` and `output` as compatibility aliases for `sink`.
- Migrate internal Reader, Writer, Planner, Scheduler, CLI, examples, and tests to canonical `source`/`sink` access.
- Keep `job_id` and `schedule` on `JobConfig`; keep cron parsing results, scheduler state, and lifecycle state outside it.
- Change Planner production construction to accept `JobConfig` (normally shared through the existing ownership convention), not raw JSON.
- Keep `start_job(JobConfig)` as the common execution boundary; it creates one Planner and passes the resulting `ExecutionPlan` to Runner/Pipeline.
- Make CLI, HTTP, and Scheduler external configuration loaders call `JobConfig::parse_json` before invoking `start_job` or the Planner boundary.
- Preserve direct `JobConfig` construction for tests and internal callers where no external parsing boundary exists.
- Do not change Reader/Writer registry factory signatures in this scope; they may continue receiving the canonical typed `JobConfig` during planning.
- Do not change runtime batching, streaming, cancellation, progress, pairing, or result semantics.

## Testing Decisions

- Test the highest behavior seam: public `JobConfig` parsing APIs and the Planner-to-ExecutionPlan boundary with injected fake dependencies.
- Assert behavior and error contracts rather than private helper structure.
- Cover valid canonical `source`/`sink` input.
- Cover compatibility input using `input`, `target`, and `output`.
- Cover invalid JSON, missing required fields, wrong field types, and field-path context.
- Cover unknown fields in compatibility mode and strict mode.
- Verify parsing failures occur before Reader/Writer factories are called.
- Verify Planner semantic, component, RecordBuilder, pipeline, and split failures retain stage context.
- Verify a counting fake Reader is split exactly once and execution does not split again.
- Verify empty splits preserve successful no-work behavior.
- Verify CLI, HTTP, and Scheduler loaders converge on the same parsing behavior through focused tests or testable loader seams.
- Preserve direct-construction tests for internal behavior without forcing serialization round trips.
- Run focused common and core tests, workspace check, and diff validation. Full integration with real databases or external APIs remains unnecessary for this change.

## Out of Scope

- A separate raw-JSON Planner API as the production architecture.
- A serializable logical-plan model.
- Persisting, caching, or transporting `ExecutionPlan`.
- Removing `JobConfig` from Reader/Writer internals.
- Changing database/API connection behavior.
- Adding TOML, YAML, or other configuration formats.
- Removing compatibility aliases in this change.
- Moving Scheduler runtime state or cron execution into Planner.
- Changing execution algorithms, concurrency, cancellation, progress, or result schemas.
- Real external-service or database integration tests.
- Commit, push, PR creation, deployment, or production-data access.

## Further Notes

- This spec supersedes the raw-JSON Planner boundary described in the earlier Planner spec only for the parsing boundary; the existing `ExecutionPlan` and single-split decisions remain in force.
- The key invariant is that parsing happens once at each external input boundary, semantic planning happens once per run, and execution receives only the prepared plan.
- The current baseline is `main` at commit `fc9f215d7b1f16c60e7c580e67b0c6939fa9057b`.

SPEC READY

- Status: ready for implementation
- Source: https://github.com/GhostAdults/Relus/issues/7 and docs/specs/jobconfig-planner-boundary.md
- Repository: E:/github/Relus
- Baseline: main at fc9f215d7b1f16c60e7c580e67b0c6939fa9057b
- Test seam: public JobConfig parsing APIs plus Planner-to-ExecutionPlan with injected dependencies
- Non-goals: raw-JSON Planner production API, runtime algorithm changes, real external-service integration, commit/push/PR/deploy
- External authority: local implementation and validation only; commit, push, review, deploy, tracker changes beyond this issue, data access, and real-service access remain ungranted
- Next route: fork + /spec-executor
