# CLI progress observer and engine submission options

## Problem Statement

After the Engine architecture migration, CLI data synchronization no longer reliably displays the existing `indicatif` progress bars. The progress renderer is currently coupled to pipeline execution, so background and non-terminal entry points can implicitly write to stderr and concurrent task groups can create competing progress displays.

## Solution

Move progress reporting behind an Engine-owned event interface. The Engine crate provides an Indicatif adapter that CLI sync commands explicitly inject as a per-submission option. Execution remains observer-driven without automatic terminal output, while CLI sync restores one aggregated Reader/Writer display per Job.

## User Stories

1. As a CLI user, I want `sync` to show live Reader and Writer progress so that I can see that a long synchronization is active.
2. As a CLI user, I want `sync-with-mapping` to show the same progress behavior so that both one-shot synchronization commands are consistent.
3. As an HTTP, Desktop, or Scheduler user, I want execution to remain free of terminal progress output so that service logs are not polluted.
4. As a CLI user, I want one aggregated progress display per Job so that parallel TaskGroups do not produce competing progress bars.
5. As a CLI user, I want Batch jobs to show a determinate total when the prepared reader split provides one.
6. As a CLI user, I want Streaming jobs to show cumulative Reader and Writer event counts without a fake percentage.
7. As a CLI user, I want successful work to finish with a clear Done state.
8. As a CLI user, I want failed work to finish with a Failed state and retain final counts.
9. As a CLI user, I want cancelled work to finish with a Cancelled state and retain final counts.
10. As a CLI user, I want progress rendering failures not to turn a successful data synchronization into a failed synchronization.
11. As a CI or redirected-output user, I want non-TTY execution to avoid dynamic terminal control sequences while preserving execution and event accounting.
12. As an Engine caller, I want the existing submission call to remain valid without progress options.
13. As an Engine caller, I want to attach progress behavior to one Job submission without modifying the prepared execution plan.
14. As an Engine caller, I want progress events from concurrent TaskGroups to aggregate into one Job-level observer.
15. As a test author, I want to verify progress behavior through the submission and execution seams without using a real database or terminal.

## Implementation Decisions

- The Engine owns a synchronous, thread-safe `ProgressObserver` interface that reports Job start, read increments, sent increments, and terminal completion.
- Progress outcome values map to the Engine's success, failure, and cancellation semantics.
- A Job submission model owns per-execution `ExecutionOptions`; options are not stored in the prepared execution plan or global Coordinator state.
- A builder-style submission API supports `JobSubmission::new(plan).with_options(...)` while preserving the existing plan-only submission form through conversion into a default submission.
- The Coordinator, JobMaster, TaskExecutionService, Worker, and Pipeline propagate one optional observer for the submitted Job. All TaskGroups share that observer.
- Batch start events use the prepared total record count. Streaming start events use an unknown total.
- The Engine crate owns the terminal adapter at `engine/src/progress.rs` and depends on `indicatif`. Coordinator, Worker, and Pipeline continue to report events through `ProgressObserver` rather than drawing terminal output directly.
- `relus_engine::progress::IndicatifProgress` owns terminal detection, Reader/Writer bars or spinners, rendering failures, and final status text. Rendering tests live at `engine/src/progress/render_tests.rs`. Core CLI imports the Engine module directly; the old `core::cli::progress` module is removed without a re-export.
- Only the CLI `sync` and `sync-with-mapping` commands inject the Indicatif adapter. Service, Desktop, Scheduler, and other CLI commands use the default submission with no observer.
- Writer live progress represents records sent toward the Sink; final completion uses actual writer results for final Job statistics.
- Empty prepared plans do not create dynamic progress bars and still retain their existing successful execution semantics.
- `main` and server bootstrap responsibilities remain unchanged except for wiring the CLI adapter through the submission options.

### Display ownership and completion ordering

- Exactly two Job-level displays are owned by one `MultiProgress`: Reader and Writer. Configure bars with a hidden target before attaching them; never draw independent startup frames.
- Display `Relus Data Sync`, a separator, and a blank line. Each Batch bar shows grouped counts and percentage, followed by a separate Speed / Elapsed / ETA line. Streaming displays event counts and a spinner, not a percentage. Do not render a Tasks table.
- Display `Readers: N    Writers: N    Workers: N` once below both bars. These are physical-plan counts, not OS thread counts or instantaneous concurrency: paired Reader tasks, paired Writer tasks, and TaskGroup Workers. The current topology pairs one Reader with one Writer; extra unpaired writer descriptors are not counted.
- Coordinator reports the physical topology through an optional default `planned(ProgressTopology)` observer callback after JobMaster preparation and before `started`. Existing observers remain source-compatible.
- Serialize the adapter lifecycle and counters. Duplicate start/finish events and events after completion must not create bars, change final counts, or draw again. Non-TTY and empty Jobs allocate no bars.
- Completion corrects both counters from the Engine result and leaves the original two displays with Done, Failed, or Cancelled. It does not force positions to the planned total. Stop tickers and finish all terminal writes before publishing the waitable result; the CLI summary follows below the footer.
- Synchronous observer callbacks must return promptly and must not wait on their own Job result. Catch unwinding observer panics at the submission seam, disable subsequent live callbacks, and still attempt final cleanup without changing the execution result. This cannot recover process aborts or a callback that blocks indefinitely.

## Testing Decisions

- Test the highest behavior seam: Job submission through Coordinator to terminal Engine result and observer events.
- Use an in-memory recording observer to verify exactly one start and one terminal event, aggregated increments across multiple TaskGroups, and success/failure/cancellation outcomes.
- Verify plan-only submission produces no observer events.
- Verify Batch totals, Streaming unknown totals, empty plans, and concurrent groups.
- Verify rendering adapter lifecycle and non-TTY behavior without requiring a real database or external service.
- Preserve existing Coordinator, TaskExecutionService, Worker, Pipeline, and CLI regression tests.
- Use Indicatif's in-memory terminal to assert the visible screen after startup, concurrent updates, completion, late events, and drop. Test 60-, 80-, and 120-column terminals, retained failure/cancellation counts, Streaming, the topology footer, and terminal I/O failure.
- Gate `finished` deterministically to prove `JobHandle::wait` and Snapshot results remain unpublished until it returns. Verify observer panics do not fail the Job or strand repeated waiters.
- Keep the synthetic interactive terminal preview opt-in; it does not access databases or networks.

## Out of Scope

- No retry, checkpoint, metrics, scheduling, or persistence policy implementation.
- No changes to Reader or Writer traits or their asynchronous dispatch model.
- No new CLI progress flags in this iteration.
- No changes to database behavior, mapping semantics, job lifecycle semantics, or result status compatibility.
- No benchmark claims or performance redesign.

## Further Notes

The progress renderer is presentation logic packaged in the Engine crate by explicit ownership decision. This supersedes the earlier Core-only adapter placement and the no-Indicatif Engine dependency constraint. The execution modules remain observer-driven; HTTP, Desktop, Scheduler, and submissions without a progress observer do not create terminal displays.
