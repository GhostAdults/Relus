# Relus Execution

Relus synchronizes data from a Source to a Sink. Execution progress describes one Job and its physical work topology.

## Language

**Job**:
A single submitted synchronization execution, with one aggregated progress display and one final result.

**Task**:
A physical unit of synchronization work pairing one Reader task with one Writer task.

**TaskGroup**:
A group of physical Tasks executed together under a shared concurrency limit.

**Worker**:
The execution unit responsible for one TaskGroup. The progress footer counts these units, not operating-system threads.

**Progress topology counts**:
The numbers of paired Reader tasks, paired Writer tasks, and TaskGroup Workers in a Job's physical plan. These totals do not represent the number currently active or a configured maximum.
