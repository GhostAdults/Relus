use std::time::{Duration, Instant};

use crate::{BoundedEventBuffer, BufferLimits, CdcMessage, ReplicationError, ReplicationErrorKind};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LoadMeasurement {
    pub events: u64,
    pub elapsed: Duration,
    pub events_per_second: f64,
    pub peak_events: usize,
    pub peak_bytes: usize,
    pub configured_max_events: usize,
    pub configured_max_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PerformanceGate {
    pub baseline_events_per_second: f64,
    pub candidate_events_per_second: f64,
    pub ratio: f64,
    pub passed: bool,
}

pub async fn measure_bounded_load<F>(
    limits: BufferLimits,
    events: u64,
    message_factory: F,
) -> Result<LoadMeasurement, ReplicationError>
where
    F: Fn(u64) -> CdcMessage + Send + Sync + 'static,
{
    if events == 0 {
        return Err(ReplicationError::new(
            ReplicationErrorKind::Configuration,
            false,
            "load measurement requires at least one event",
        ));
    }
    let mut buffer = BoundedEventBuffer::new(limits)?;
    let sender = buffer.sender();
    let started = Instant::now();
    let producer = tokio::spawn(async move {
        for index in 0..events {
            sender.push(message_factory(index)).await?;
        }
        Ok::<_, ReplicationError>(())
    });
    let mut consumed = 0;
    while consumed < events {
        let _message = buffer.pop().await.ok_or_else(|| {
            ReplicationError::new(ReplicationErrorKind::Shutdown, false, "load buffer closed")
        })?;
        consumed += 1;
    }
    producer.await.map_err(|error| {
        ReplicationError::new(ReplicationErrorKind::Shutdown, false, error.to_string())
    })??;
    let elapsed = started.elapsed();
    let metrics = buffer.metrics();
    Ok(LoadMeasurement {
        events,
        elapsed,
        events_per_second: events as f64 / elapsed.as_secs_f64().max(f64::EPSILON),
        peak_events: metrics.peak_events,
        peak_bytes: metrics.peak_bytes,
        configured_max_events: limits.max_events,
        configured_max_bytes: limits.max_bytes,
    })
}

pub fn compare_to_baseline(
    baseline_events_per_second: f64,
    candidate_events_per_second: f64,
) -> Result<PerformanceGate, ReplicationError> {
    if !baseline_events_per_second.is_finite()
        || baseline_events_per_second <= 0.0
        || !candidate_events_per_second.is_finite()
        || candidate_events_per_second < 0.0
    {
        return Err(ReplicationError::new(
            ReplicationErrorKind::Configuration,
            false,
            "baseline rate must be positive and both rates must be finite",
        ));
    }
    let ratio = candidate_events_per_second / baseline_events_per_second;
    Ok(PerformanceGate {
        baseline_events_per_second,
        candidate_events_per_second,
        ratio,
        passed: ratio >= 0.8,
    })
}
