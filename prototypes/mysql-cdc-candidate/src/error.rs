use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationErrorKind {
    Configuration,
    Authentication,
    Tls,
    Network,
    ServerUnavailable,
    InvalidOffset,
    BinlogPurged,
    Decode,
    UnsupportedEvent,
    Protocol,
    Shutdown,
    Checkpoint,
    HeartbeatTimeout,
    Failpoint,
}

#[derive(Debug, Error)]
#[error("{kind:?}: {message}")]
pub struct ReplicationError {
    pub kind: ReplicationErrorKind,
    pub retryable: bool,
    message: String,
}

impl ReplicationError {
    pub fn new(kind: ReplicationErrorKind, retryable: bool, message: impl Into<String>) -> Self {
        Self {
            kind,
            retryable,
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}
