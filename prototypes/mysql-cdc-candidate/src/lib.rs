pub mod adapter;
pub mod envelope;
pub mod error;
pub mod recovery;
pub mod startup;
pub mod transaction;
pub mod verification;

pub use adapter::{
    classify_mysql_server_error, consume_until_cancelled, MySqlAsyncReplicationClient,
    MySqlConnectionConfig, MySqlReplicationClient, TlsMode,
};
pub use envelope::{
    CdcEnvelope, CdcMessage, CdcOffset, CdcOperation, CdcValue, RowChange, TransactionCommit,
};
pub use error::{ReplicationError, ReplicationErrorKind};
pub use recovery::{
    run_recovery_loop, CheckpointPhase, FileCheckpointStore, MySqlRecoveryClientFactory,
    PersistentTestCheckpoint, PrototypeSink, RecoveryClientFactory, RecoveryFailpoint,
    RecoveryOptions, TestCheckpointStore,
};
pub use startup::{
    prepare_startup, BinlogTransactionBoundary, InitialSnapshotSource, SnapshotCompletion,
    SnapshotCursor, SnapshotRow, SnapshotSink, StartupMode, StartupOffsetProvider,
};
pub use transaction::{RawReplicationEvent, TransactionAssembler};
pub use verification::{
    verify_available_runtime, VerificationCase, VerificationReport, VerificationStatus,
};
