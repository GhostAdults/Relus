pub mod adapter;
pub mod buffer;
pub mod envelope;
pub mod error;
pub mod performance;
pub mod recovery;
pub mod schema;
pub mod startup;
pub mod transaction;
pub mod type_matrix;
pub mod verification;

pub use adapter::{
    classify_mysql_server_error, consume_until_cancelled, MySqlAsyncReplicationClient,
    MySqlConnectionConfig, MySqlReplicationClient, TlsMode,
};
pub use buffer::{
    estimate_message_bytes, serialize_message, BoundedEventBuffer, BoundedEventSender,
    BufferLimits, BufferMetrics, BufferedMessage,
};
pub use envelope::{
    CdcEnvelope, CdcEventId, CdcMessage, CdcOffset, CdcOperation, CdcRow, CdcValue, RowChange,
    TransactionCommit,
};
pub use error::{ReplicationError, ReplicationErrorKind};
pub use performance::{
    compare_to_baseline, measure_bounded_load, LoadMeasurement, PerformanceGate,
};
pub use recovery::{
    run_recovery_loop, CheckpointPhase, FileCheckpointStore, MySqlRecoveryClientFactory,
    PersistentTestCheckpoint, PrototypeSink, RecoveryClientFactory, RecoveryFailpoint,
    RecoveryOptions, TestCheckpointStore,
};
pub use schema::{validate_mysql_schema, MySqlColumn, MySqlColumnType, SchemaRegistry};
pub use startup::{
    prepare_startup, BinlogTransactionBoundary, InitialSnapshotSource, SnapshotCompletion,
    SnapshotCursor, SnapshotRow, SnapshotSink, StartupMode, StartupOffsetProvider,
};
pub use transaction::{RawReplicationEvent, TransactionAssembler};
pub use type_matrix::{round_trip_value, CommonMySqlType};
pub use verification::{
    verify_available_runtime, VerificationCase, VerificationReport, VerificationStatus,
};
