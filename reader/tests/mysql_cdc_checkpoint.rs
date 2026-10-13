#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use relus_common::cdc::CdcOffset;
use relus_reader::mysql_cdc::{CdcCheckpoint, CheckpointIdentity, RedbCheckpointStore};

fn identity(source_id: &str) -> CheckpointIdentity {
    CheckpointIdentity::new(
        source_id,
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "sales",
        "rules-fingerprint",
        "schema-fingerprint",
    )
}

#[test]
fn checkpoint_store_persists_only_monotonic_confirmed_transactions() {
    let directory = tempfile::tempdir().expect("temporary checkpoint directory");
    let store = RedbCheckpointStore::open(&directory.path().join("mysql-cdc.redb"))
        .expect("checkpoint store should open");
    assert!(store
        .load("mysql-sales")
        .expect("empty checkpoint lookup should succeed")
        .is_none());

    let first = CdcCheckpoint::new(
        "mysql-sales",
        identity("mysql-sales"),
        Arc::new(CdcOffset::Position {
            file: "mysql-bin.000007".to_owned(),
            position: 120,
        }),
        1,
    );
    store.save(&first).expect("first checkpoint should persist");
    assert_eq!(
        store
            .load("mysql-sales")
            .expect("checkpoint lookup should succeed")
            .expect("checkpoint should exist"),
        first
    );

    let stale = CdcCheckpoint::new(
        "mysql-sales",
        identity("mysql-sales"),
        Arc::new(CdcOffset::Position {
            file: "mysql-bin.000007".to_owned(),
            position: 100,
        }),
        0,
    );
    let error = store
        .save(&stale)
        .expect_err("an older sequence must not overwrite the confirmed checkpoint")
        .to_string();
    assert!(error.contains("sequence"));
    assert_eq!(
        store
            .load("mysql-sales")
            .unwrap()
            .unwrap()
            .confirmed_offset
            .position(),
        120
    );
}

#[test]
fn checkpoint_store_rejects_a_source_identity_mismatch() {
    let directory = tempfile::tempdir().expect("temporary checkpoint directory");
    let store = RedbCheckpointStore::open(&directory.path().join("mysql-cdc.redb"))
        .expect("checkpoint store should open");
    store
        .save(&CdcCheckpoint::new(
            "shared-checkpoint",
            identity("source-a"),
            Arc::new(CdcOffset::Position {
                file: "mysql-bin.000001".to_owned(),
                position: 4,
            }),
            1,
        ))
        .unwrap();

    let error = store
        .validate_source("shared-checkpoint", "source-b")
        .expect_err("a checkpoint cannot silently move to a different source")
        .to_string();
    assert!(error.contains("source_id"));
}

#[test]
fn checkpoint_store_rejects_schema_or_routing_changes() {
    let directory = tempfile::tempdir().expect("temporary checkpoint directory");
    let store = RedbCheckpointStore::open(&directory.path().join("mysql-cdc.redb"))
        .expect("checkpoint store should open");
    store
        .save(&CdcCheckpoint::new(
            "shared-checkpoint",
            identity("source-a"),
            Arc::new(CdcOffset::Position {
                file: "mysql-bin.000001".to_owned(),
                position: 4,
            }),
            1,
        ))
        .expect("checkpoint should persist");
    let mut changed = identity("source-a");
    changed.schema_fingerprint = "changed-schema".to_owned();

    let error = store
        .validate_identity("shared-checkpoint", &changed)
        .expect_err("schema drift must not reuse an existing checkpoint")
        .to_string();

    assert!(error.contains("identity mismatch"));
}
