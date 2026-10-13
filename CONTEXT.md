# Relus

Relus synchronizes data from a Source to a Sink. Its shared language distinguishes execution
topology from the evidence and approval required to replace production connectors.

## Execution Language

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

## Connector Candidate Language

**Prototype candidate**:
A dependency selected for isolated validation because it is the best current technical option. It
is not approved for production use.
_Avoid_: Production candidate, production-ready library

**Production candidate**:
A prototype candidate that has satisfied every applicable Production Migration Gate and is
eligible for explicit migration approval.
_Avoid_: Preferred prototype

**Conditionally recommended**:
The status of a prototype candidate that remains the preferred option but still has missing or
failed production gates.
_Avoid_: Recommended for production

**Production Migration Gate**:
The complete approved set of runtime, platform, security, performance, compatibility, and review
evidence required before production migration may be requested.
_Avoid_: Prototype acceptance criteria

**Encrypted TLS evidence**:
Evidence that a database connection is encrypted while server certificate identity verification is
not performed. It demonstrates transport encryption only and is not trust evidence or, by itself,
a production approval.
_Avoid_: Verified TLS

**Verified TLS evidence**:
Evidence that the server certificate chain and server identity were validated against an approved
trust root and name.
_Avoid_: Encrypted-only TLS
