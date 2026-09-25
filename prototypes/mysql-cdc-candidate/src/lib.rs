pub mod adapter;
pub mod envelope;
pub mod error;
pub mod transaction;
pub mod verification;

pub use adapter::{
    consume_until_cancelled, MySqlAsyncReplicationClient, MySqlConnectionConfig,
    MySqlReplicationClient, TlsMode,
};
pub use envelope::{
    CdcEnvelope, CdcMessage, CdcOffset, CdcOperation, CdcValue, RowChange, TransactionCommit,
};
pub use error::{ReplicationError, ReplicationErrorKind};
pub use transaction::{RawReplicationEvent, TransactionAssembler};
pub use verification::{
    verify_available_runtime, VerificationCase, VerificationReport, VerificationStatus,
};
