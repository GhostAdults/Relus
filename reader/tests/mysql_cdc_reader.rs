#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use relus_common::{
    cdc::{CdcOffset, CdcOperation, CdcRow, CdcValue, RowChange},
    PipelineMessage,
};
use relus_reader::mysql_cdc::{
    forward_replication_client, CheckpointIdentity, ColumnSelection, ForwardReplicationOptions,
    InitialTargetPolicy, MySqlReplicationClient, RawReplicationEvent, RedbCheckpointStore,
    ReplicationError, TableRule,
};
use tokio::sync::{mpsc, Semaphore};
use tokio_util::sync::CancellationToken;

struct FiniteClient {
    events: VecDeque<RawReplicationEvent>,
}

struct HangingClient {
    closed: Arc<AtomicBool>,
}

#[async_trait]
impl MySqlReplicationClient for HangingClient {
    async fn connect(&mut self) -> Result<(), ReplicationError> {
        Ok(())
    }

    async fn next_event(&mut self) -> Result<Option<RawReplicationEvent>, ReplicationError> {
        futures::future::pending().await
    }

    async fn close(&mut self) -> Result<(), ReplicationError> {
        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl MySqlReplicationClient for FiniteClient {
    async fn connect(&mut self) -> Result<(), ReplicationError> {
        Ok(())
    }

    async fn next_event(&mut self) -> Result<Option<RawReplicationEvent>, ReplicationError> {
        Ok(self.events.pop_front())
    }

    async fn close(&mut self) -> Result<(), ReplicationError> {
        Ok(())
    }

    fn is_finite_stream(&self) -> bool {
        true
    }
}

fn selected_row() -> RowChange {
    RowChange {
        database: Arc::from("sales"),
        table: Arc::from("orders"),
        operation: CdcOperation::Insert,
        key: CdcRow::from([("id", CdcValue::Unsigned(1))]),
        before: None,
        after: Some(CdcRow::from([("id", CdcValue::Unsigned(1))])),
    }
}

fn checkpoint_identity() -> CheckpointIdentity {
    CheckpointIdentity::new(
        "mysql-sales",
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "sales",
        "rules-fingerprint",
        "schema-fingerprint",
    )
}

#[tokio::test]
async fn checkpoint_advances_only_after_the_sink_acknowledges_commit() {
    let start = CdcOffset::Position {
        file: "mysql-bin.000001".to_owned(),
        position: 4,
    };
    let commit = CdcOffset::Position {
        file: "mysql-bin.000001".to_owned(),
        position: 120,
    };
    let mut client = FiniteClient {
        events: VecDeque::from([
            RawReplicationEvent::Begin {
                transaction_id: "mysql-bin.000001:4".to_owned(),
                offset: start.clone(),
            },
            RawReplicationEvent::Row {
                timestamp_seconds: 1,
                offset: start,
                rows: vec![selected_row()],
            },
            RawReplicationEvent::Commit {
                offset: commit.clone(),
            },
        ]),
    };
    let directory = tempfile::tempdir().unwrap();
    let store =
        Arc::new(RedbCheckpointStore::open(&directory.path().join("checkpoint.redb")).unwrap());
    let (sender, mut receiver) = mpsc::channel(4);
    let rules = vec![TableRule {
        include: "orders".to_owned(),
        target: "ods_orders".to_owned(),
        columns: ColumnSelection::All("*".to_owned()),
        initial_target_policy: InitialTargetPolicy::RequireEmpty,
    }];
    let cancellation = CancellationToken::new();
    let identity = checkpoint_identity();

    let run = forward_replication_client(
        &mut client,
        ForwardReplicationOptions {
            checkpoint_store: Arc::clone(&store),
            checkpoint_id: "mysql-sales",
            checkpoint_identity: &identity,
            table_rules: &rules,
            byte_capacity: Arc::new(Semaphore::new(1024 * 1024)),
            max_message_bytes: 1024 * 1024,
            heartbeat_timeout: std::time::Duration::from_secs(45),
            sender,
            cancellation: &cancellation,
        },
    );
    tokio::pin!(run);

    let first = tokio::select! {
        message = receiver.recv() => message.expect("row message"),
        result = &mut run => panic!("reader completed before sending the row: {result:?}"),
    };
    assert!(matches!(
        first.expect("valid row"),
        PipelineMessage::CdcRow(_)
    ));
    let second = tokio::select! {
        message = receiver.recv() => message.expect("commit message"),
        result = &mut run => panic!("reader completed before sending the commit: {result:?}"),
    };
    let PipelineMessage::CdcCommit(acknowledger) = second.expect("valid commit") else {
        panic!("expected CDC commit")
    };
    assert!(store.load("mysql-sales").unwrap().is_none());

    acknowledger.acknowledge().expect("commit acknowledgement");
    let forwarded = run.await.expect("finite replication should complete");
    assert_eq!(forwarded, 1);
    assert_eq!(
        *store.load("mysql-sales").unwrap().unwrap().confirmed_offset,
        commit
    );
}

#[tokio::test]
async fn heartbeat_timeout_closes_the_replication_connection_for_retry() {
    let closed = Arc::new(AtomicBool::new(false));
    let mut client = HangingClient {
        closed: Arc::clone(&closed),
    };
    let directory = tempfile::tempdir().unwrap();
    let store =
        Arc::new(RedbCheckpointStore::open(&directory.path().join("checkpoint.redb")).unwrap());
    let (sender, _receiver) = mpsc::channel(1);
    let identity = checkpoint_identity();

    let error = forward_replication_client(
        &mut client,
        ForwardReplicationOptions {
            checkpoint_store: store,
            checkpoint_id: "mysql-sales",
            checkpoint_identity: &identity,
            table_rules: &[],
            byte_capacity: Arc::new(Semaphore::new(1024)),
            max_message_bytes: 1024,
            heartbeat_timeout: std::time::Duration::from_millis(10),
            sender,
            cancellation: &CancellationToken::new(),
        },
    )
    .await
    .expect_err("an idle connection must time out");

    assert_eq!(
        error.kind,
        relus_reader::mysql_cdc::ReplicationErrorKind::HeartbeatTimeout
    );
    assert!(error.retryable);
    assert!(closed.load(Ordering::SeqCst));
}
