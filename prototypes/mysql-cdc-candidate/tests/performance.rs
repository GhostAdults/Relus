#![allow(clippy::expect_used)]

use std::collections::BTreeMap;

use relus_mysql_cdc_candidate::{
    compare_to_baseline, estimate_message_bytes, measure_bounded_load, BufferLimits, CdcEnvelope,
    CdcMessage, CdcOffset, CdcOperation, CdcValue, RowChange,
};

fn message(index: u64) -> CdcMessage {
    CdcMessage::Row(CdcEnvelope {
        schema_version: 1,
        event_id: format!("load-{index}"),
        transaction_id: "load".to_owned(),
        transaction_event_index: index,
        row_index: 0,
        event_timestamp_seconds: 1,
        offset: CdcOffset::Position {
            file: "mysql-bin.000001".to_owned(),
            position: index,
        },
        change: RowChange {
            database: "sales".to_owned(),
            table: "orders".to_owned(),
            operation: CdcOperation::Insert,
            key: BTreeMap::from([(String::from("id"), CdcValue::Unsigned(index))]),
            before: None,
            after: Some(BTreeMap::from([(
                String::from("id"),
                CdcValue::Unsigned(index),
            )])),
        },
    })
}

#[tokio::test]
async fn sustained_load_reports_bounded_usage() {
    let bytes = estimate_message_bytes(&message(0)).expect("message should serialize");
    let limits = BufferLimits {
        max_events: 8,
        max_bytes: bytes * 8,
    };
    let measurement = measure_bounded_load(limits, 100_000, message)
        .await
        .expect("synthetic load should complete");
    assert_eq!(measurement.events, 100_000);
    assert!(measurement.peak_events <= limits.max_events);
    assert!(measurement.peak_bytes <= limits.max_bytes);
    assert!(measurement.events_per_second > 0.0);
}

#[test]
fn performance_gate_requires_at_least_eighty_percent_of_baseline() {
    assert!(
        compare_to_baseline(100.0, 80.0)
            .expect("valid rates")
            .passed
    );
    assert!(
        !compare_to_baseline(100.0, 79.9)
            .expect("valid rates")
            .passed
    );
}
