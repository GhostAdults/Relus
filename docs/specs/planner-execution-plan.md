# Planner and Prepared Execution Plan

Status: published as https://github.com/GhostAdults/Relus/issues/6

## Problem Statement

Relus currently lets the execution layer understand and carry the full `JobConfig`. Runner construction reads pipeline settings from it, creates Reader and Writer instances from it, calls Reader splitting to choose an execution strategy, and later passes it into the pipeline so that the pipeline can construct a `RecordBuilder`. The pipeline then calls Reader splitting a second time before execution.

This mixes configuration parsing, component preparation, planning, strategy selection, and runtime execution. It makes the execution engine dependent on the JSON-shaped job model, duplicates planning work, makes failure timing unclear, and makes the execution path harder to test without real data sources.

## Solution

Introduce a Planner that owns the transition from raw JSON job configuration to a prepared, single-use `ExecutionPlan`:

`raw JSON -> JobConfig -> prepared ExecutionPlan -> execution layer`

The Planner parses JSON, validates the typed job configuration, resolves pipeline settings, creates the Reader and Writer, builds the `RecordBuilder`, calls `reader.split()` exactly once, and records the resulting stream mode and reader tasks in the plan.

After planning succeeds, Runner strategies and the pipeline executor consume only the prepared plan and runtime controls such as cancellation. They do not receive or inspect `JobConfig`.

## User Stories

1. As a Relus maintainer, I want raw job JSON converted into a prepared execution plan before execution, so that configuration concerns are separated from runtime concerns.
2. As a Relus maintainer, I want malformed JSON rejected by the Planner, so that the execution layer never sees an invalid configuration document.
3. As a Relus maintainer, I want semantic configuration errors reported during planning, so that failures occur before a task begins running.
4. As a Relus maintainer, I want pipeline defaults and job overrides resolved during planning, so that the execution layer receives concrete values only.
5. As a Relus maintainer, I want the Planner to create the configured Reader, so that Runner does not depend on the Reader registry or source configuration.
6. As a Relus maintainer, I want the Planner to create the configured Writer, so that Runner does not depend on the Writer registry or target configuration.
7. As a Relus maintainer, I want mapping and type configuration compiled into a `RecordBuilder` during planning, so that DSL or mapping errors are returned before execution.
8. As a Relus maintainer, I want source type behavior applied to the prepared `RecordBuilder`, so that runtime transformation preserves current semantics.
9. As a Relus maintainer, I want Reader splitting performed once during planning, so that expensive discovery or database work is not duplicated.
10. As a Relus maintainer, I want the split tasks and total record count retained in the plan, so that the pipeline executes exactly what was prepared.
11. As a Relus maintainer, I want stream mode derived from the same split result stored in the plan, so that strategy selection and executed tasks cannot disagree.
12. As a Relus maintainer, I want batch and streaming runners selected from the prepared stream mode, so that strategy dispatch does not inspect job configuration.
13. As a Relus maintainer, I want Runner and Pipeline APIs to omit `JobConfig`, so that future configuration format changes do not propagate into execution code.
14. As a Relus maintainer, I want planning failures propagated through immediate jobs and scheduled jobs, so that all entry points report consistent errors.
15. As a scheduler user, I want every scheduled firing to receive a fresh prepared plan, so that execution objects and split tasks are never reused across runs.
16. As a scheduler user, I want cron scheduling metadata to remain separate from the prepared execution plan, so that scheduling and execution retain distinct responsibilities.
17. As a plugin author, I want existing Reader and Writer registry construction to remain compatible in this phase, so that introducing Planner does not require redesigning every plugin configuration API.
18. As a test author, I want Planner dependencies replaceable with fakes, so that planning can be tested without connecting to a database or API.
19. As a test author, I want to observe that Reader splitting happens exactly once, so that regressions cannot restore the current duplicate split behavior.
20. As a test author, I want execution tested with a prepared plan, so that tests demonstrate that the runtime no longer requires raw JSON or `JobConfig`.
21. As an operator, I want planning errors to retain useful context about parsing, Reader creation, Writer creation, mapping construction, or splitting, so that configuration failures are diagnosable.
22. As a Relus maintainer, I want a single public planning boundary, so that CLI, HTTP, and Scheduler execution paths converge before entering the engine.

## Implementation Decisions

- The plan is a prepared runtime plan, not a serializable logical plan. It contains live Reader and Writer trait objects and is intended for one execution only.
- The Planner accepts raw JSON text at its public boundary and owns JSON deserialization into `JobConfig`.
- The Planner owns typed semantic validation after deserialization.
- The prepared plan contains the Reader, Writer, resolved pipeline configuration, prepared `RecordBuilder`, the complete Reader split result, and the stream mode derived from that split result.
- The plan must not retain `JobConfig` merely for downstream convenience.
- Planner may temporarily pass the typed `JobConfig` to the existing Reader and Writer registries while constructing components. This compatibility is contained inside planning.
- Runner strategies and the pipeline executor must not accept, inspect, or reconstruct `JobConfig` after planning.
- The Planner calls `reader.split()` exactly once. Neither Runner nor Pipeline may call it again.
- Pipeline execution consumes the stored Reader tasks and total record count from the plan.
- Writer splitting remains an execution preparation step driven by the number of prepared Reader tasks, because the approved plan shape contains the Reader split but not a Writer split.
- Batch versus streaming strategy dispatch uses the stream mode in the prepared plan.
- Every task run creates a fresh plan. Prepared plans are not cloned, cached, serialized, persisted, or reused by cron firings.
- Scheduling policy and schedule metadata remain outside `ExecutionPlan`. The Scheduler may retain configuration source data needed to create a fresh plan for each firing, but spawned execution receives only the plan and cancellation control.
- Immediate, API, CLI, and Scheduler paths must converge on the same Planner behavior rather than duplicating parsing or preparation.
- Planning errors should add stage context while preserving underlying causes.
- Introduce one injectable planning dependency boundary for tests. The production implementation delegates to the existing Reader and Writer registries; tests provide fake factories and fake Reader/Writer implementations.
- Existing job JSON fields and their current default/override precedence remain compatible.
- Existing execution results, cancellation semantics, batch completion semantics, streaming shutdown semantics, progress reporting, and one-reader-to-one-writer pairing remain compatible.

## Testing Decisions

- The highest test seam is the public asynchronous Planner operation from raw JSON plus injected planning dependencies to `Result<ExecutionPlan>`.
- Tests assert external behavior and plan contents, not private helper calls or internal module layout.
- A valid JSON job with fake components produces a fully prepared plan with resolved pipeline values, Reader, Writer, `RecordBuilder`, split tasks, total records, and matching stream mode.
- Malformed JSON fails before component factories are invoked.
- Semantically invalid configuration fails before runtime execution.
- Reader factory, Writer factory, `RecordBuilder`, and Reader split failures are returned with useful planning-stage context.
- A counting fake Reader proves that planning calls `split()` once and plan execution does not call it again.
- Batch and streaming split results lead to their corresponding execution strategies.
- An empty Reader split remains a valid plan and preserves the current no-work completion behavior.
- Execution tests accept a prepared plan and cancellation control without any `JobConfig` argument.
- Existing unit-test style inside core modules is prior art. Async planning behavior uses Tokio tests and fake trait implementations; it must not require a real database, network service, or global registry mutation.
- Focused verification should include core crate tests and checks before broader workspace verification.

## Out of Scope

- A separate serializable `LogicalPlan` model.
- Persisting, caching, displaying, or remotely transporting execution plans.
- Reusing a prepared plan across scheduled runs.
- Redesigning Reader and Writer plugin factory signatures to accept source-specific configuration types.
- Removing `JobConfig` from the Reader and Writer crates internally.
- Moving schedule parsing or Scheduler policy into Planner.
- Adding new configuration formats such as TOML or YAML.
- Changing the public job JSON schema or migration behavior.
- Changing pipeline concurrency algorithms, Reader/Writer pairing, progress UI, cancellation policy, or execution result schemas.
- Adding database-backed integration tests.

## Further Notes

- The current code performs Reader splitting once to choose a Runner and again inside the paired pipeline. The new plan must make the first split authoritative.
- Although the architectural flow is commonly described as `JobConfig -> ExecutionPlan -> Engine`, the approved public Planner boundary begins at raw JSON. `JobConfig` is an internal typed intermediate owned by planning.
- The term “Engine” refers to the post-planning Runner strategy and pipeline execution path; no new monolithic Engine type is required.
- The implementation should keep public execution entrypoints focused on start/execute behavior and keep parsing and preparation inside Planner.
