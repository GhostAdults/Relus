#![allow(clippy::expect_used)]

use std::sync::Arc;

use relus_mysql_cdc_candidate::{
    estimate_message_bytes, BoundedEventBuffer, BufferLimits, CdcEnvelope, CdcMessage, CdcOffset,
    CdcOperation, CdcRow, CdcValue, RowChange,
};

fn row_message(id: u64) -> CdcMessage {
    CdcMessage::Row(CdcEnvelope {
        schema_version: 1,
        event_id: format!("event-{id}").into(),
        transaction_id: Arc::from("tx-1"),
        transaction_event_index: id,
        row_index: 0,
        event_timestamp_seconds: 1,
        offset: Arc::new(CdcOffset::Position {
            file: "mysql-bin.000001".to_owned(),
            position: 100 + id,
        }),
        change: RowChange {
            database: Arc::from("sales"),
            table: if id.is_multiple_of(2) {
                "orders"
            } else {
                "items"
            }
            .into(),
            operation: CdcOperation::Insert,
            key: CdcRow::from([(String::from("id"), CdcValue::Unsigned(id))]),
            before: None,
            after: Some(CdcRow::from([
                (String::from("id"), CdcValue::Unsigned(id)),
                (
                    String::from("payload"),
                    CdcValue::String("bounded".to_owned()),
                ),
            ])),
        },
    })
}

#[tokio::test]
async fn producer_waits_for_slow_consumer_and_stays_within_both_limits() {
    let sample = row_message(0);
    let bytes = estimate_message_bytes(&sample).expect("sample should serialize");
    let limits = BufferLimits {
        max_events: 3,
        max_bytes: bytes * 3,
    };
    let mut buffer = BoundedEventBuffer::new(limits).expect("limits should be valid");
    let sender = buffer.sender();
    let producer = tokio::spawn(async move {
        for id in 0..64 {
            sender
                .push(row_message(id))
                .await
                .expect("consumer stays alive");
        }
    });

    let mut consumed = 0;
    while consumed < 64 {
        let message = buffer.pop().await.expect("producer stays alive");
        assert!(message.serialized_bytes() <= limits.max_bytes);
        let decoded: CdcMessage =
            serde_json::from_slice(&message.serialized).expect("buffer stores reusable JSON bytes");
        assert!(matches!(decoded, CdcMessage::Row(_)));
        consumed += 1;
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        let metrics = buffer.metrics();
        assert!(metrics.events <= limits.max_events);
        assert!(metrics.bytes <= limits.max_bytes);
        assert!(metrics.peak_events <= limits.max_events);
        assert!(metrics.peak_bytes <= limits.max_bytes);
    }
    producer.await.expect("producer should finish");

    let metrics = buffer.metrics();
    assert_eq!(metrics.pushed_events, 64);
    assert_eq!(metrics.popped_events, 64);
    assert!(metrics.blocked_pushes > 0);
}

#[tokio::test]
async fn buffer_exposes_the_exact_serialized_payload_for_the_sink() {
    let sample = row_message(42);
    let expected = serde_json::to_vec(&sample).expect("sample should serialize");
    let mut buffer = BoundedEventBuffer::new(BufferLimits {
        max_events: 1,
        max_bytes: expected.len(),
    })
    .expect("limits should be valid");

    buffer
        .sender()
        .push(sample)
        .await
        .expect("message should fit exactly");
    let buffered = buffer.pop().await.expect("message should be available");

    assert_eq!(buffered.serialized, expected);
    assert_eq!(buffered.serialized_bytes(), expected.len());
}

#[tokio::test]
async fn one_message_larger_than_byte_limit_is_rejected_before_enqueue() {
    let sample = row_message(0);
    let bytes = estimate_message_bytes(&sample).expect("sample should serialize");
    let buffer = BoundedEventBuffer::new(BufferLimits {
        max_events: 1,
        max_bytes: bytes - 1,
    })
    .expect("positive limit should be valid");

    let error = buffer
        .sender()
        .push(sample)
        .await
        .expect_err("oversized messages must fail closed");
    assert_eq!(
        error.kind,
        relus_mysql_cdc_candidate::ReplicationErrorKind::Configuration
    );
}
