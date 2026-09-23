# Execution Engine Foundation

Status: published as https://github.com/GhostAdults/Relus/issues/8

## Problem Statement

Relus 已完成 Planner 与 `ExecutionPlan` 的基础重构，但执行核心仍集中在 Runner 和 Pipeline Executor 中。Runner 继续承担策略分发和生命周期编排，TaskGroup、取消、运行状态和结果聚合缺少独立的 Engine 边界，未来难以逐步加入 Worker、背压、重试、Checkpoint 和指标能力。

本阶段需要建立面向未来的 Execution Engine 架构，同时保持当前 Reader → RecordBuilder → Channel → Writer 的执行逻辑和对外结果兼容，避免一次性重写带来行为回归。

## Solution

引入 Engine 分层，并以 CoordinatorService 作为任务提交入口：

`CoordinatorService.submit_job(JobConfig) → JobMaster preparation → RuntimeJob/TaskGroup/Task → TaskExecutionService → Runtime → Pipeline → RunResult`

Engine 第一阶段建立状态、提交、静态编排和执行适配边界。`pipeline_executor.rs` 演进为 `TaskExecutionService`，负责部署 TaskGroup 并复用现有 Pipeline 执行逻辑。Runner 保留兼容入口和结果 DTO，但不再新增核心编排职责。

## User Stories

1. As a Relus maintainer, I want a CoordinatorService submission boundary, so that Engine task lifecycle has one authoritative entrypoint.
2. As a Relus maintainer, I want submission to consume a parsed `JobConfig`, so that each JobMaster owns planning and physical-plan initialization for its Job.
3. As an operator, I want submission to return a unique JobId quickly, so that I can query or cancel a job without waiting for completion.
4. As a Relus maintainer, I want JobMaster to prepare configuration and initialize a runtime Job, so that the complete per-Job initialization lifecycle has one owner.
5. As a Relus maintainer, I want JobMaster to create static TaskGroups and Tasks from the Reader split, so that execution does not repeat split discovery.
6. As an operator, I want each Task to expose lifecycle state, so that submitted and actually running work are distinguishable.
7. As an operator, I want Tasks to enter RUNNING only after their runtime resources are deployed, so that status reflects real execution.
8. As a maintainer, I want terminal Task states protected from later overwrites, so that completion, failure, and cancellation remain trustworthy.
9. As a maintainer, I want TaskExecutionService to deploy TaskGroups, so that Coordinator and JobMaster do not contain Source/Sink execution details.
10. As a maintainer, I want existing RecordBuilder behavior represented as the Transform stage internally, so that future node modeling does not change current transformations.
11. As a maintainer, I want Runtime to own Tokio task submission and cancellation handles, so that execution resources are isolated from lifecycle policy.
12. As an operator, I want cancellation to propagate from a Job to its TaskGroups and Tasks, so that stopping a job uses the existing cancellation semantics consistently.
13. As an operator, I want Job state aggregated from TaskGroup outcomes, so that partial startup, success, failure, and cancellation are visible.
14. As a maintainer, I want TaskGroup results to contain read/write/failure counts, cancellation, errors, and elapsed time, so that future metrics can consume stable intermediate data.
15. As an API consumer, I want the existing RunResult, RunStatus, and RunnerStats response contract preserved, so that this internal refactor does not break CLI, HTTP, or Scheduler clients.
16. As a maintainer, I want Runner to remain a compatibility adapter, so that existing callers can migrate incrementally.
17. As a maintainer, I want a dedicated State model, so that lifecycle state is not hidden inside Pipeline private work structures.
18. As a maintainer, I want a Channel boundary for capacity and backpressure, so that future channel changes do not leak into Coordinator logic.
19. As a maintainer, I want Retry, Checkpoint, and Metrics seams reserved, so that later capabilities can be added without another architectural reset.
20. As a test author, I want Engine lifecycle tested with fake plans and components, so that architecture tests do not require real databases or APIs.
21. As a test author, I want to verify TaskGroup static compilation and single deployment, so that workers cannot silently recreate or split tasks.
22. As a test author, I want existing pipeline behavior tests to remain valid, so that the migration proves semantic compatibility.

## Implementation Decisions

- Add an `engine` module with the conceptual components `coordinator`, `worker`, `channel`, `checkpoint`, `retry`, `state`, `metrics`, and `runtime`.
- Implement the first slice in this order: State, Coordinator, Runtime, Worker, Channel, Metrics, Retry, Checkpoint.
- The first slice may provide minimal compiling interfaces for capabilities that are not yet active, but must not claim behavior that is not implemented.
- `CoordinatorService.submit_job(JobConfig)` is the Engine submission boundary. Rust APIs use `snake_case`; the product concept remains submitJob.
- Submission generates a unique JobId, registers the Job in an in-memory state repository, and schedules asynchronous initialization. It does not wait for full completion.
- `JobMaster::initialize` consumes the parsed JobConfig, prepares resources once, and creates the RuntimeJob, TaskGroup pairings, and initial CREATED state.
- TaskGroup creation uses the prepared Reader split and current Writer split/pairing rules. Workers never call Reader split again.
- `pipeline_executor.rs` evolves into `TaskExecutionService`, which accepts prepared TaskGroups and adapts them to the existing Pipeline executor. The read/transform/channel/write loop is not rewritten in this phase.
- The runtime model is `Job → TaskGroup[] → Task[]`, with Source and Sink represented by current Reader and Writer tasks. RecordBuilder remains the internal Transform implementation; no independent Transform trait or DAG is added yet.
- Task lifecycle is:

  `CREATED → SUBMITTED → INITIALIZING → RUNNING → SUCCEEDED | FAILED | CANCELLED`

- A task enters RUNNING after Source/Transform/Sink execution units are submitted to the Runtime. Terminal states cannot transition back to RUNNING. Initialization failure becomes FAILED.
- Job state is aggregated from TaskGroup states. At least one running TaskGroup makes the Job RUNNING; all tasks succeeding makes it SUCCEEDED; an unrecoverable failure without retry makes it FAILED; cancellation makes it CANCELLED; empty prepared work preserves successful no-work behavior.
- Coordinator owns an in-memory state repository with controlled updates. Scheduler state remains separate.
- Cancellation reuses `CancellationToken` and propagates Coordinator → Job → TaskGroup → Task. The phase does not add forced kill or timeout recovery.
- TaskGroup execution results include status, records read, records written, records failed, cancellation indicator, error summary, and elapsed duration.
- Engine aggregates TaskGroup results into existing `PipelineStats`, `RunnerStats`, and `RunResult`/`RunStatus` compatibility DTOs. CLI, HTTP, and Scheduler response shapes remain unchanged.
- Runner keeps `RunResult`, `RunStatus`, `RunnerStats`, and `start_task` as compatibility surfaces. It delegates to the Engine boundary and does not gain new business orchestration.
- Channel, Metrics, Retry, and Checkpoint modules define extension seams only in this phase. Existing Pipeline channels, statistics, and execution behavior remain authoritative.
- Checkpoint persistence, retry policy, independent metrics storage, resource quotas, and complete Transform DAG execution are explicitly deferred.
- No changes are made to `ExecutionPlan` fields, Reader/Writer registry signatures, execution algorithms, progress reporting, or external service behavior.

## Testing Decisions

- Test the highest available Engine behavior seam: submission of a fake `JobConfig` through injected planning dependencies and observation of Job/Task lifecycle and final `RunResult`.
- Use injected fake Reader/Writer and deterministic TaskExecutionService/Runtime seams; tests must not require a database, API, network, or global registry mutation.
- Verify unique JobId generation, initial registration, asynchronous initialization, and JobMaster static TaskGroup/Task creation.
- Verify lifecycle transitions through RUNNING and each terminal state, including initialization failure and cancellation.
- Verify terminal states cannot regress to RUNNING.
- Verify empty prepared work completes successfully.
- Verify TaskExecutionService deploys each TaskGroup once and does not call Reader split.
- Verify cancellation propagates through the existing CancellationToken path.
- Verify TaskGroup result aggregation maps to the existing PipelineStats and RunnerStats fields and preserves RunResult status semantics.
- Keep current Planner, Pipeline, Runner, Reader, and Writer unit tests as regression coverage; do not rewrite them to test private module layout.
- Focused checks should include Engine/core tests, workspace tests, workspace check, and diff validation. Clippy and formatting are separate toolchain gates if components are unavailable.

## Out of Scope

- Rewriting the current Reader → RecordBuilder → Channel → Writer algorithm.
- Replacing `RunResult`, `RunStatus`, or `RunnerStats` public response DTOs.
- A complete Source/Transform/Sink DAG or independent Transform trait.
- Production retry policy, exponential backoff, or retry budgets.
- Checkpoint serialization, persistence, recovery, or exactly-once guarantees.
- Independent Metrics storage, exporters, dashboards, or tracing schema changes.
- Forced task termination, timeout recovery, or resource quota enforcement.
- Scheduler persistence redesign or coupling Engine state to Scheduler state.
- Changing `ExecutionPlan` shape or Reader/Writer factory contracts.
- Real database/API integration changes.
- Commit, push, PR creation, deployment, production-data access, or external service calls.

## Plan terminology

`JobMaster` validates configuration, creates Reader/Writer resources, builds
the RecordBuilder, and performs the one-time Reader split. Its prepared
intermediate remains private to the module. JobMaster then produces
`RuntimeJob`, the actual resource-bound physical execution plan.
`RuntimeTaskGroup` is a deployable physical task group and `RuntimeTask` is a
concrete paired Reader/Writer execution unit. Writer splitting and task pairing
also remain JobMaster concerns.

The future architectural direction is to separate immutable
`PhysicalExecutionPlan` topology from `RuntimeJobState`, while `RuntimeJob`
acts as the execution wrapper. This phase does not introduce those types.

## Current `relus_core` transition boundary

The current workspace keeps explicit responsibilities in one crate:

| Module | Responsibility |
| --- | --- |
| `engine::job_master` | Validate `JobConfig`, create resources, build `RecordBuilder`, perform the single Reader split, and produce the resource-bound `RuntimeJob`. |
| `engine` | Let a per-Job `JobMaster` prepare and build `RuntimeJob`, manage lifecycle, deploy groups, and aggregate outcomes. |
| `pipeline` | Execute prepared Reader/Writer pairs through RecordBuilder and channels; it does not parse configuration or create physical topology. |
| `core::runner` | Compatibility adapter exposing `RunResult`, `RunStatus`, `RunnerStats`, and `start_task`; delegates to Planner and Coordinator/Engine. |
| `core::scheduler` | Upper-layer application orchestration for schedules, control, cancellation, and fresh plan submission; it is not part of Engine. |
| `core::serve` and service adapters | CLI, HTTP, and Desktop entry points preserving external contracts and routing to `start_task`/`start_job`. |

Production flow:

`service/CLI/HTTP/Desktop → starter::start_job → CoordinatorService → per-Job JobMaster → RuntimeJob → TaskExecutionService → Worker → Pipeline Executor`

Engine owns job lifecycle, physical deployment, cancellation propagation, and
result aggregation. Scheduler owns application scheduling policy. This is an
intentional transition boundary; Cargo crates have not yet been split.

## Future crate and planning direction

Intended dependency direction:

`relus_core → relus_engine → pipeline / reader / writer / common`

`relus_core` retains Scheduler, compatibility services, CLI, and HTTP concerns.
`relus_engine` contains configuration preparation, Coordinator, JobMaster, Runtime,
Worker, and TaskExecutionService. Engine must not depend on Scheduler, CLI, HTTP,
or Runner compatibility DTOs.

Future planning vocabulary:

`LogicalDag → PipelineList/SubPlan[] → RuntimeJob → PipelineExecutor`

Future runtime separation:

- `PhysicalExecutionPlan`: immutable, resource-bound task topology.
- `RuntimeJobState`: mutable lifecycle state for Job, TaskGroup, and Task.
- `RuntimeJob`: transitional execution wrapper combining topology, resources,
  and lifecycle-facing handles.

This issue records terminology and dependency constraints only. It does not
introduce `LogicalDag`, `PipelineList`, `SubPlan`, `PhysicalExecutionPlan`, or
`RuntimeJobState`, and does not change execution behavior.

## Further Notes

- This specification builds on the completed planning work. JobMaster now owns configuration preparation and physical-plan construction; Coordinator remains the runtime lifecycle interface.
- The first implementation should prefer adapters over moving large blocks of Pipeline code. Physical relocation can happen after lifecycle behavior is proven.
- The current baseline is `main` at commit `fc9f215d7b1f16c60e7c580e67b0c6939fa9057b`; the worktree may contain unrelated uncommitted changes that must remain untouched.

SPEC READY

- Status: ready for implementation
- Source: https://github.com/GhostAdults/Relus/issues/8 and docs/specs/execution-engine-foundation.md
- Repository: E:/github/Relus
- Baseline: main at fc9f215d7b1f16c60e7c580e67b0c6939fa9057b
- Test seam: CoordinatorService submission with fake planning dependencies, lifecycle state observation, and compatible RunResult aggregation
- Non-goals: rewriting Pipeline algorithms, implementing Retry/Checkpoint/Metrics persistence, changing public result DTOs, real service integration
- External authority: local implementation and validation only; commit, push, review, deploy, production data, and real-service access remain ungranted
- Next route: to-tickets
