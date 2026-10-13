#![allow(clippy::expect_used)]

use std::{collections::BTreeMap, sync::Arc};

use relus_common::{
    cdc::{
        CdcEnvelope, CdcEventId, CdcOffset, CdcOperation, CdcRow, CdcRowMessage, CdcValue,
        RowChange, CDC_SCHEMA_VERSION,
    },
    job_config::WriteMode,
    types::UnifiedValue,
};
use relus_writer::rdbms_writer_util::rdbms_writer::plan_cdc_write;

fn update_message() -> CdcRowMessage {
    let offset = Arc::new(CdcOffset::Position {
        file: "mysql-bin.000001".to_owned(),
        position: 120,
    });
    CdcRowMessage {
        envelope: CdcEnvelope {
            schema_version: CDC_SCHEMA_VERSION,
            event_id: CdcEventId::from("mysql-bin.000001:100:0:0"),
            transaction_id: Arc::from("mysql-bin.000001:100"),
            transaction_event_index: 0,
            row_index: 0,
            event_timestamp_seconds: 1,
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
        },
        target_table: "ods_orders".to_owned(),
        column_mapping: BTreeMap::from([
            ("id".to_owned(), "order_id".to_owned()),
            ("status".to_owned(), "status".to_owned()),
        ]),
    }
}

#[test]
fn update_plan_routes_columns_and_preserves_unsigned_values_without_loss() {
    let plans = plan_cdc_write(&update_message()).expect("the CDC update should be writable");
    let plan = &plans[0];

    assert_eq!(plan.table, "ods_orders");
    assert_eq!(plan.mode, WriteMode::Update);
    assert_eq!(plan.key_columns, vec!["order_id"]);
    assert_eq!(plan.rows.columns, vec!["order_id", "status"]);
    assert!(matches!(
        plan.rows.values[0][0],
        UnifiedValue::Decimal(value) if value.to_string() == u64::MAX.to_string()
    ));
    assert_eq!(
        plan.rows.values[0][1],
        UnifiedValue::String("paid".to_owned())
    );
}

#[test]
fn inserts_use_upsert_so_replayed_events_remain_idempotent() {
    let mut message = update_message();
    message.envelope.change.operation = CdcOperation::Insert;
    message.envelope.change.before = None;
    let plans = plan_cdc_write(&message).expect("the CDC insert should be writable");
    let plan = &plans[0];

    assert_eq!(plan.mode, WriteMode::Upsert);
    assert_eq!(plan.key_columns, vec!["order_id"]);
}

#[test]
fn explicit_table_mapping_excludes_unlisted_source_columns() {
    let mut message = update_message();
    message
        .envelope
        .change
        .after
        .as_mut()
        .unwrap_or_else(|| panic!("update fixture should have an after image"))
        .insert("internal_note", CdcValue::String("do not copy".to_owned()));

    let plans = plan_cdc_write(&message)
        .unwrap_or_else(|error| panic!("the mapped CDC update should be writable: {error}"));
    let plan = &plans[0];

    assert_eq!(plan.rows.columns, vec!["order_id", "status"]);
}

#[test]
fn primary_key_changes_delete_the_old_row_before_upserting_the_new_row() {
    let mut message = update_message();
    message
        .envelope
        .change
        .after
        .as_mut()
        .unwrap_or_else(|| panic!("update fixture should have an after image"))
        .insert("id", CdcValue::Unsigned(42));

    let plans = plan_cdc_write(&message)
        .unwrap_or_else(|error| panic!("the primary-key update should be writable: {error}"));

    assert_eq!(plans.len(), 2);
    assert_eq!(plans[0].mode, WriteMode::Delete);
    assert_eq!(plans[1].mode, WriteMode::Upsert);
    assert!(matches!(
        plans[0].rows.values[0][0],
        UnifiedValue::Decimal(value) if value.to_string() == u64::MAX.to_string()
    ));
    assert_eq!(plans[1].rows.values[0][0], UnifiedValue::Int(42));
}
