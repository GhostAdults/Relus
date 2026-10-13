use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, Semaphore};

use crate::{CdcMessage, ReplicationError, ReplicationErrorKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferLimits {
    pub max_events: usize,
    pub max_bytes: usize,
}

impl BufferLimits {
    pub fn validate(self) -> Result<Self, ReplicationError> {
        if self.max_events == 0 || self.max_bytes == 0 {
            return Err(ReplicationError::new(
                ReplicationErrorKind::Configuration,
                false,
                "CDC buffer limits must be greater than zero",
            ));
        }
        if self.max_bytes > u32::MAX as usize {
            return Err(ReplicationError::new(
                ReplicationErrorKind::Configuration,
                false,
                "CDC byte limit exceeds the semaphore capacity",
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferMetrics {
    pub events: usize,
    pub bytes: usize,
    pub max_events: usize,
    pub max_bytes: usize,
    pub peak_events: usize,
    pub peak_bytes: usize,
    pub pushed_events: u64,
    pub popped_events: u64,
    pub blocked_pushes: u64,
}

#[derive(Debug)]
struct MetricsState {
    current_events: usize,
    current_bytes: usize,
    peak_events: usize,
    peak_bytes: usize,
    pushed_events: u64,
    popped_events: u64,
    blocked_pushes: u64,
}

pub struct BoundedEventBuffer {
    sender: BoundedEventSender,
    receiver: mpsc::Receiver<BufferedMessage>,
}

#[derive(Clone)]
pub struct BoundedEventSender {
    sender: mpsc::Sender<BufferedMessage>,
    byte_capacity: Arc<Semaphore>,
    limits: BufferLimits,
    metrics: Arc<Mutex<MetricsState>>,
}

#[derive(Debug)]
pub struct BufferedMessage {
    pub serialized: Vec<u8>,
    _byte_permit: tokio::sync::OwnedSemaphorePermit,
}

impl BufferedMessage {
    pub fn serialized_bytes(&self) -> usize {
        self.serialized.len()
    }
}

impl BoundedEventBuffer {
    pub fn new(limits: BufferLimits) -> Result<Self, ReplicationError> {
        let limits = limits.validate()?;
        let metrics = Arc::new(Mutex::new(MetricsState {
            current_events: 0,
            current_bytes: 0,
            peak_events: 0,
            peak_bytes: 0,
            pushed_events: 0,
            popped_events: 0,
            blocked_pushes: 0,
        }));
        let (sender, receiver) = mpsc::channel(limits.max_events);
        Ok(Self {
            sender: BoundedEventSender {
                sender,
                byte_capacity: Arc::new(Semaphore::new(limits.max_bytes)),
                limits,
                metrics,
            },
            receiver,
        })
    }

    pub fn sender(&self) -> BoundedEventSender {
        self.sender.clone()
    }

    pub async fn pop(&mut self) -> Option<BufferedMessage> {
        let message = self.receiver.recv().await?;
        let mut metrics = self
            .sender
            .metrics
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        metrics.current_events = metrics.current_events.saturating_sub(1);
        metrics.current_bytes = metrics
            .current_bytes
            .saturating_sub(message.serialized_bytes());
        metrics.popped_events += 1;
        Some(message)
    }

    pub fn close(&mut self) {
        self.receiver.close();
    }

    pub fn metrics(&self) -> BufferMetrics {
        let metrics = self
            .sender
            .metrics
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        BufferMetrics {
            events: metrics.current_events,
            bytes: metrics.current_bytes,
            max_events: self.sender.limits.max_events,
            max_bytes: self.sender.limits.max_bytes,
            peak_events: metrics.peak_events,
            peak_bytes: metrics.peak_bytes,
            pushed_events: metrics.pushed_events,
            popped_events: metrics.popped_events,
            blocked_pushes: metrics.blocked_pushes,
        }
    }
}

impl BoundedEventSender {
    pub async fn push(&self, message: CdcMessage) -> Result<(), ReplicationError> {
        let serialized = serialize_message(&message)?;
        let serialized_bytes = serialized.len();
        if serialized_bytes > self.limits.max_bytes {
            return Err(ReplicationError::new(
                ReplicationErrorKind::Configuration,
                false,
                format!(
                    "CDC serialized message ({serialized_bytes} bytes) exceeds the configured byte limit"
                ),
            ));
        }

        let byte_permit = match self
            .byte_capacity
            .clone()
            .try_acquire_many_owned(serialized_bytes as u32)
        {
            Ok(permit) => permit,
            Err(_) => {
                self.mark_blocked();
                self.byte_capacity
                    .clone()
                    .acquire_many_owned(serialized_bytes as u32)
                    .await
                    .map_err(|_| closed_error())?
            }
        };
        let buffered = BufferedMessage {
            serialized,
            _byte_permit: byte_permit,
        };
        match self.sender.try_send(buffered) {
            Ok(()) => {
                self.mark_pushed(serialized_bytes);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(buffered)) => {
                self.mark_blocked();
                self.sender
                    .send(buffered)
                    .await
                    .map_err(|_| closed_error())?;
                self.mark_pushed(serialized_bytes);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => Err(closed_error()),
        }
    }

    fn mark_blocked(&self) {
        let mut metrics = self
            .metrics
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        metrics.blocked_pushes += 1;
    }

    fn mark_pushed(&self, estimated_bytes: usize) {
        let mut metrics = self
            .metrics
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        metrics.current_events += 1;
        metrics.current_bytes += estimated_bytes;
        metrics.peak_events = metrics.peak_events.max(metrics.current_events);
        metrics.peak_bytes = metrics.peak_bytes.max(metrics.current_bytes);
        metrics.pushed_events += 1;
    }
}

pub fn estimate_message_bytes(message: &CdcMessage) -> Result<usize, ReplicationError> {
    serialize_message(message).map(|bytes| bytes.len())
}

pub fn serialize_message(message: &CdcMessage) -> Result<Vec<u8>, ReplicationError> {
    let mut serialized = Vec::with_capacity(768);
    serde_json::to_writer(&mut serialized, message).map_err(|error| {
        ReplicationError::new(ReplicationErrorKind::Decode, false, error.to_string())
    })?;
    Ok(serialized)
}

fn closed_error() -> ReplicationError {
    ReplicationError::new(
        ReplicationErrorKind::Shutdown,
        false,
        "CDC buffer is closed",
    )
}
