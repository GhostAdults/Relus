use tokio::sync::mpsc;

/// Small boundary around pipeline channels; capacity/backpressure stays local.
#[derive(Debug, Clone, Copy)]
pub struct Channel {
    capacity: usize,
}

impl Channel {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
        }
    }
    pub fn pair<T>(&self) -> (mpsc::Sender<T>, mpsc::Receiver<T>) {
        mpsc::channel(self.capacity)
    }
}
