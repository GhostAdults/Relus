#![allow(clippy::expect_used)]

use std::sync::Arc;

use relus_common::cdc::{
    cdc_commit_ack_channel, CdcEnvelope, CdcEventId, CdcOffset, CdcOperation, CdcRow, CdcValue,
    RowChange, TransactionCommit, CDC_SCHEMA_VERSION,
};

#[test]
fn cdc_envelope_serializes_a_stable_replay_identity_and_full_row_images() {
    let offset = Arc::new(CdcOffset::Gtid {
        resume_set: "source:1-42".to_owned(),
        transaction_gtid: Some("source:42".to_owned()),
        file: "mysql-bin.000007".to_owned(),
        position: 512,
    });
    let envelope = CdcEnvelope {
        schema_version: CDC_SCHEMA_VERSION,
        event_id: CdcEventId::from("source:42:3:0"),
        transaction_id: Arc::from("source:42"),
        transaction_event_index: 3,
        row_index: 0,
        event_timestamp_seconds: 1_700_000_000,
        offset,
        change: RowChange {
            database: Arc::from("sales"),
            table: Arc::from("orders"),
            operation: CdcOperation::Update,
            key: CdcRow::from([("id", CdcValue::Unsigned(u64::MAX))]),
            before: Some(CdcRow::from([
                ("id", CdcValue::Unsigned(u64::MAX)),
                ("status", CdcValue::String("pending".to_owned())),
            ])),
            after: Some(CdcRow::from([
                ("id", CdcValue::Unsigned(u64::MAX)),
                ("status", CdcValue::String("paid".to_owned())),
            ])),
        },
    };

    let json = serde_json::to_value(&envelope).expect("CDC envelope should serialize");
    assert_eq!(json["schema_version"], 1);
    assert_eq!(json["event_id"], "source:42:3:0");
    assert_eq!(json["change"]["operation"], "update");
    assert_eq!(json["change"]["before"]["status"]["string"], "pending");
    assert_eq!(json["change"]["after"]["status"]["string"], "paid");
    assert_eq!(
        json["change"]["key"]["id"]["unsigned"].as_u64(),
        Some(u64::MAX)
    );
}

#[tokio::test]
async fn transaction_commit_acknowledges_the_exact_confirmed_offset() {
    let offset = CdcOffset::Position {
        file: "mysql-bin.000009".to_owned(),
        position: 1_024,
    };
    let commit = TransactionCommit {
        schema_version: CDC_SCHEMA_VERSION,
        event_id: "mysql-bin.000009:1024:commit".to_owned(),
        transaction_id: Arc::from("mysql-bin.000009:900"),
        offset: Arc::new(offset.clone()),
        event_count: 2,
    };
    let (acknowledger, acknowledgement) = cdc_commit_ack_channel(commit.clone());

    acknowledger
        .acknowledge()
        .expect("the first acknowledgement should be delivered");

    let confirmed = acknowledgement
        .await
        .expect("the writer acknowledgement should arrive");
    assert_eq!(confirmed, commit);
    assert_eq!(*confirmed.offset, offset);
}
