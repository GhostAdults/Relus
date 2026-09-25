use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use async_trait::async_trait;
use relus_mysql_cdc_candidate::{
    classify_mysql_server_error, run_recovery_loop, CdcEnvelope, CdcOffset, CdcOperation, CdcValue,
    CheckpointPhase, FileCheckpointStore, MySqlReplicationClient, PersistentTestCheckpoint,
    PrototypeSink, RawReplicationEvent, RecoveryClientFactory, RecoveryFailpoint, RecoveryOptions,
    ReplicationError, ReplicationErrorKind, RowChange, TestCheckpointStore, TransactionCommit,
};

#[tokio::test]
async fn file_position_and_gtid_checkpoints_persist_as_resume_offsets(
) -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("checkpoint.json");
    let mut store = FileCheckpointStore::new(path);
    let file_checkpoint = PersistentTestCheckpoint::new(
        1,
        CdcOffset::Position {
            file: "mysql-bin.000007".to_owned(),
            position: 900,
        },
        CheckpointPhase::Streaming,
    )?;

    store.save(&file_checkpoint).await?;
    assert_eq!(store.load().await?, Some(file_checkpoint));

    let gtid_checkpoint = PersistentTestCheckpoint::new(
        2,
        CdcOffset::Gtid {
            resume_set: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-41".to_owned(),
            transaction_gtid: Some("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:42".to_owned()),
            file: "mysql-bin.000008".to_owned(),
            position: 120,
        },
        CheckpointPhase::Streaming,
    )?;

    store.save(&gtid_checkpoint).await?;
    let Some(loaded) = store.load().await? else {
        return Err("checkpoint should exist".into());
    };
    assert_eq!(loaded.sequence, 2);
    assert_eq!(
        loaded.confirmed_offset,
        CdcOffset::Gtid {
            resume_set: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-42".to_owned(),
            transaction_gtid: None,
            file: "mysql-bin.000008".to_owned(),
            position: 120,
        }
    );
    Ok(())
}

#[derive(Default)]
struct MemoryCheckpointStore {
    checkpoint: Option<PersistentTestCheckpoint>,
}

#[async_trait]
impl TestCheckpointStore for MemoryCheckpointStore {
    async fn load(&self) -> Result<Option<PersistentTestCheckpoint>, ReplicationError> {
        Ok(self.checkpoint.clone())
    }

    async fn save(
        &mut self,
        checkpoint: &PersistentTestCheckpoint,
    ) -> Result<(), ReplicationError> {
        self.checkpoint = Some(checkpoint.clone());
        Ok(())
    }
}

struct ScriptClient {
    events: VecDeque<Result<Option<RawReplicationEvent>, ReplicationError>>,
    finite: bool,
}

struct DelayedClient {
    delay: Duration,
    event: Option<RawReplicationEvent>,
}

#[async_trait]
impl MySqlReplicationClient for DelayedClient {
    async fn connect(&mut self) -> Result<(), ReplicationError> {
        Ok(())
    }

    async fn next_event(&mut self) -> Result<Option<RawReplicationEvent>, ReplicationError> {
        if let Some(event) = self.event.take() {
            tokio::time::sleep(self.delay).await;
            Ok(Some(event))
        } else {
            std::future::pending().await
        }
    }

    async fn close(&mut self) -> Result<(), ReplicationError> {
        Ok(())
    }
}

#[async_trait]
impl MySqlReplicationClient for ScriptClient {
    async fn connect(&mut self) -> Result<(), ReplicationError> {
        Ok(())
    }

    async fn next_event(&mut self) -> Result<Option<RawReplicationEvent>, ReplicationError> {
        self.events.pop_front().unwrap_or(Ok(None))
    }

    async fn close(&mut self) -> Result<(), ReplicationError> {
        Ok(())
    }

    fn is_finite_stream(&self) -> bool {
        self.finite
    }
}

struct ScriptFactory {
    scripts: VecDeque<Vec<Result<Option<RawReplicationEvent>, ReplicationError>>>,
    starts: Vec<CdcOffset>,
}

struct HeartbeatFactory {
    starts: Vec<CdcOffset>,
    calls: u32,
    heartbeat: RawReplicationEvent,
}

struct EofRecoveryFactory {
    starts: Vec<CdcOffset>,
    scripts: VecDeque<EofScript>,
}

struct EofScript {
    events: Vec<Result<Option<RawReplicationEvent>, ReplicationError>>,
    finite: bool,
}

#[async_trait]
impl RecoveryClientFactory for EofRecoveryFactory {
    async fn connect(
        &mut self,
        start_offset: CdcOffset,
    ) -> Result<Box<dyn MySqlReplicationClient>, ReplicationError> {
        self.starts.push(start_offset);
        let script = self.scripts.pop_front().unwrap_or(EofScript {
            events: Vec::new(),
            finite: true,
        });
        Ok(Box::new(ScriptClient {
            events: script.events.into(),
            finite: script.finite,
        }))
    }
}

#[async_trait]
impl RecoveryClientFactory for HeartbeatFactory {
    async fn connect(
        &mut self,
        start_offset: CdcOffset,
    ) -> Result<Box<dyn MySqlReplicationClient>, ReplicationError> {
        self.starts.push(start_offset);
        self.calls += 1;
        if self.calls == 1 {
            Ok(Box::new(DelayedClient {
                delay: Duration::from_millis(50),
                event: Some(self.heartbeat.clone()),
            }))
        } else {
            Ok(Box::new(ScriptClient {
                events: VecDeque::from([Ok(None)]),
                finite: true,
            }))
        }
    }
}

#[async_trait]
impl RecoveryClientFactory for ScriptFactory {
    async fn connect(
        &mut self,
        start_offset: CdcOffset,
    ) -> Result<Box<dyn MySqlReplicationClient>, ReplicationError> {
        self.starts.push(start_offset);
        let events = self.scripts.pop_front().unwrap_or_default().into();
        Ok(Box::new(ScriptClient {
            events,
            finite: true,
        }))
    }
}

#[derive(Default)]
struct RecordingSink {
    event_ids: Vec<String>,
    acknowledged: Vec<CdcOffset>,
    fail_acknowledge: bool,
}

#[async_trait]
impl PrototypeSink for RecordingSink {
    async fn write(&mut self, envelope: &CdcEnvelope) -> Result<(), ReplicationError> {
        self.event_ids.push(envelope.event_id.clone());
        Ok(())
    }

    async fn acknowledge(&mut self, commit: &TransactionCommit) -> Result<(), ReplicationError> {
        if self.fail_acknowledge {
            return Err(ReplicationError::new(
                ReplicationErrorKind::Protocol,
                false,
                "prototype sink rejected the transaction",
            ));
        }
        self.acknowledged.push(commit.offset.clone());
        Ok(())
    }
}

struct ConnectFailureFactory {
    results: VecDeque<Result<(), ReplicationError>>,
    starts: Vec<CdcOffset>,
}

#[async_trait]
impl RecoveryClientFactory for ConnectFailureFactory {
    async fn connect(
        &mut self,
        start_offset: CdcOffset,
    ) -> Result<Box<dyn MySqlReplicationClient>, ReplicationError> {
        self.starts.push(start_offset);
        match self.results.pop_front().unwrap_or(Ok(())) {
            Ok(()) => Ok(Box::new(ScriptClient {
                events: VecDeque::from([Ok(None)]),
                finite: true,
            })),
            Err(error) => Err(error),
        }
    }
}

fn transaction(
    start: CdcOffset,
    commit: CdcOffset,
    id: u64,
) -> Vec<Result<Option<RawReplicationEvent>, ReplicationError>> {
    let row = RowChange {
        database: "sales".to_owned(),
        table: "orders".to_owned(),
        operation: CdcOperation::Insert,
        key: BTreeMap::from([("id".to_owned(), CdcValue::Unsigned(id))]),
        before: None,
        after: Some(BTreeMap::from([("id".to_owned(), CdcValue::Unsigned(id))])),
    };
    vec![
        Ok(Some(RawReplicationEvent::Begin {
            transaction_id: start.transaction_id(),
            offset: start.clone(),
        })),
        Ok(Some(RawReplicationEvent::Row {
            timestamp_seconds: 1,
            offset: start,
            rows: vec![row],
        })),
        Ok(Some(RawReplicationEvent::Commit { offset: commit })),
        Ok(None),
    ]
}

fn multi_row_transaction(
    start: CdcOffset,
    commit: CdcOffset,
    rows: u64,
) -> Vec<Result<Option<RawReplicationEvent>, ReplicationError>> {
    let changes = (1..=rows)
        .map(|id| RowChange {
            database: "sales".to_owned(),
            table: "orders".to_owned(),
            operation: CdcOperation::Insert,
            key: BTreeMap::from([("id".to_owned(), CdcValue::Unsigned(id))]),
            before: None,
            after: Some(BTreeMap::from([("id".to_owned(), CdcValue::Unsigned(id))])),
        })
        .collect();
    vec![
        Ok(Some(RawReplicationEvent::Begin {
            transaction_id: start.transaction_id(),
            offset: start.clone(),
        })),
        Ok(Some(RawReplicationEvent::Row {
            timestamp_seconds: 1,
            offset: start,
            rows: changes,
        })),
        Ok(Some(RawReplicationEvent::Commit { offset: commit })),
        Ok(None),
    ]
}

#[tokio::test]
async fn unconfirmed_transaction_replays_with_stable_identity_and_no_checkpoint_advance(
) -> Result<(), ReplicationError> {
    let initial = CdcOffset::Position {
        file: "mysql-bin.000001".to_owned(),
        position: 4,
    };
    let row_offset = CdcOffset::Position {
        file: "mysql-bin.000001".to_owned(),
        position: 100,
    };
    let commit_offset = CdcOffset::Position {
        file: "mysql-bin.000001".to_owned(),
        position: 140,
    };
    let mut factory = ScriptFactory {
        scripts: VecDeque::from([
            transaction(row_offset.clone(), commit_offset.clone(), 1),
            transaction(row_offset, commit_offset.clone(), 1),
        ]),
        starts: Vec::new(),
    };
    let mut sink = RecordingSink::default();
    let mut checkpoints = MemoryCheckpointStore::default();

    let first = run_recovery_loop(
        &mut factory,
        &mut sink,
        &mut checkpoints,
        initial.clone(),
        RecoveryOptions::default(),
        Some(RecoveryFailpoint::AfterSinkWriteBeforeCheckpoint),
    )
    .await;
    assert!(first.is_err());
    assert!(checkpoints.load().await?.is_none());

    run_recovery_loop(
        &mut factory,
        &mut sink,
        &mut checkpoints,
        initial.clone(),
        RecoveryOptions::default(),
        None,
    )
    .await?;

    assert_eq!(factory.starts, vec![initial.clone(), initial]);
    assert_eq!(sink.event_ids.len(), 2);
    assert_eq!(sink.event_ids[0], sink.event_ids[1]);
    assert_eq!(
        sink.acknowledged,
        vec![commit_offset.clone(), commit_offset.clone()]
    );
    assert_eq!(
        checkpoints
            .load()
            .await?
            .map(|checkpoint| checkpoint.confirmed_offset),
        Some(commit_offset)
    );
    Ok(())
}

#[tokio::test]
async fn confirmed_transaction_is_retained_when_the_next_transaction_replays(
) -> Result<(), ReplicationError> {
    let initial = CdcOffset::Position {
        file: "mysql-bin.000001".to_owned(),
        position: 4,
    };
    let first_commit = initial.with_position("mysql-bin.000001", 140);
    let second_commit = initial.with_position("mysql-bin.000001", 240);
    let mut checkpoints = MemoryCheckpointStore::default();
    let mut sink = RecordingSink::default();
    let mut first_factory = ScriptFactory {
        scripts: VecDeque::from([transaction(
            initial.with_position("mysql-bin.000001", 100),
            first_commit.clone(),
            1,
        )]),
        starts: Vec::new(),
    };

    run_recovery_loop(
        &mut first_factory,
        &mut sink,
        &mut checkpoints,
        initial.clone(),
        RecoveryOptions::default(),
        None,
    )
    .await?;
    assert_eq!(first_factory.starts, vec![initial.clone()]);
    assert_eq!(
        checkpoints
            .load()
            .await?
            .map(|checkpoint| checkpoint.confirmed_offset),
        Some(first_commit.clone())
    );

    let mut second_factory = ScriptFactory {
        scripts: VecDeque::from([
            transaction(
                initial.with_position("mysql-bin.000001", 200),
                second_commit.clone(),
                2,
            ),
            transaction(
                initial.with_position("mysql-bin.000001", 200),
                second_commit.clone(),
                2,
            ),
        ]),
        starts: Vec::new(),
    };
    let result = run_recovery_loop(
        &mut second_factory,
        &mut sink,
        &mut checkpoints,
        initial.clone(),
        RecoveryOptions::default(),
        Some(RecoveryFailpoint::AfterSinkWriteBeforeCheckpoint),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(
        checkpoints
            .load()
            .await?
            .map(|checkpoint| checkpoint.confirmed_offset),
        Some(first_commit.clone())
    );

    run_recovery_loop(
        &mut second_factory,
        &mut sink,
        &mut checkpoints,
        initial.clone(),
        RecoveryOptions::default(),
        None,
    )
    .await?;

    assert_eq!(
        second_factory.starts,
        vec![first_commit.clone(), first_commit]
    );
    assert_eq!(sink.event_ids.len(), 3);
    assert_eq!(sink.event_ids[1], sink.event_ids[2]);
    assert_ne!(sink.event_ids[0], sink.event_ids[1]);
    assert_eq!(
        sink.acknowledged,
        vec![
            initial.with_position("mysql-bin.000001", 140),
            second_commit.clone(),
            second_commit.clone(),
        ]
    );
    assert_eq!(
        checkpoints
            .load()
            .await?
            .map(|checkpoint| checkpoint.confirmed_offset),
        Some(second_commit)
    );
    Ok(())
}

#[tokio::test]
async fn eof_during_transaction_reconnects_and_replays_from_confirmed_checkpoint(
) -> Result<(), ReplicationError> {
    let initial = CdcOffset::Position {
        file: "mysql-bin.000001".to_owned(),
        position: 4,
    };
    let row_offset = initial.with_position("mysql-bin.000001", 300);
    let commit_offset = initial.with_position("mysql-bin.000001", 340);
    let partial_transaction = transaction(row_offset.clone(), commit_offset.clone(), 3)
        .into_iter()
        .take(2)
        .collect();
    let full_transaction = transaction(row_offset, commit_offset.clone(), 3);
    let mut factory = ScriptFactory {
        scripts: VecDeque::from([partial_transaction, full_transaction]),
        starts: Vec::new(),
    };
    let mut sink = RecordingSink::default();
    let mut checkpoints = MemoryCheckpointStore::default();

    run_recovery_loop(
        &mut factory,
        &mut sink,
        &mut checkpoints,
        initial.clone(),
        RecoveryOptions {
            max_reconnects: 1,
            reconnect_delay: Duration::ZERO,
            heartbeat_timeout: Duration::from_secs(1),
        },
        None,
    )
    .await?;

    assert_eq!(factory.starts, vec![initial.clone(), initial]);
    assert_eq!(sink.event_ids.len(), 2);
    assert_eq!(sink.event_ids[0], sink.event_ids[1]);
    assert_eq!(sink.acknowledged, vec![commit_offset.clone()]);
    assert_eq!(
        checkpoints
            .load()
            .await?
            .map(|checkpoint| checkpoint.confirmed_offset),
        Some(commit_offset)
    );
    Ok(())
}

#[tokio::test]
async fn eof_between_transactions_reconnects_from_the_confirmed_checkpoint(
) -> Result<(), ReplicationError> {
    let initial = CdcOffset::Position {
        file: "mysql-bin.000001".to_owned(),
        position: 4,
    };
    let commit_offset = initial.with_position("mysql-bin.000001", 440);
    let mut factory = EofRecoveryFactory {
        starts: Vec::new(),
        scripts: VecDeque::from([
            EofScript {
                events: transaction(
                    initial.with_position("mysql-bin.000001", 400),
                    commit_offset.clone(),
                    4,
                ),
                finite: false,
            },
            EofScript {
                events: vec![Ok(None)],
                finite: true,
            },
        ]),
    };
    let mut sink = RecordingSink::default();
    let mut checkpoints = MemoryCheckpointStore::default();

    run_recovery_loop(
        &mut factory,
        &mut sink,
        &mut checkpoints,
        initial.clone(),
        RecoveryOptions {
            max_reconnects: 1,
            reconnect_delay: Duration::ZERO,
            heartbeat_timeout: Duration::from_secs(1),
        },
        None,
    )
    .await?;

    assert_eq!(factory.starts, vec![initial, commit_offset.clone()]);
    assert_eq!(sink.acknowledged, vec![commit_offset.clone()]);
    assert_eq!(
        checkpoints
            .load()
            .await?
            .map(|checkpoint| checkpoint.confirmed_offset),
        Some(commit_offset)
    );
    Ok(())
}

#[tokio::test]
async fn retryable_errors_reconnect_from_last_confirmed_checkpoint_but_permanent_errors_stop(
) -> Result<(), ReplicationError> {
    let initial = CdcOffset::Position {
        file: "mysql-bin.000002".to_owned(),
        position: 4,
    };
    let confirmed = CdcOffset::Position {
        file: "mysql-bin.000002".to_owned(),
        position: 200,
    };
    let first_transaction = transaction(
        CdcOffset::Position {
            file: "mysql-bin.000002".to_owned(),
            position: 150,
        },
        confirmed.clone(),
        1,
    );
    let mut first_script = first_transaction[..3]
        .iter()
        .map(|item| match item {
            Ok(event) => Ok(event.clone()),
            Err(_) => unreachable!(),
        })
        .collect::<Vec<_>>();
    first_script.push(Err(ReplicationError::new(
        ReplicationErrorKind::Network,
        true,
        "temporary network error",
    )));
    let permanent = ReplicationError::new(
        ReplicationErrorKind::Authentication,
        false,
        "permanent authentication error",
    );
    let mut factory = ScriptFactory {
        scripts: VecDeque::from([first_script, vec![Err(permanent)]]),
        starts: Vec::new(),
    };
    let mut sink = RecordingSink::default();
    let mut checkpoints = MemoryCheckpointStore::default();
    let result = run_recovery_loop(
        &mut factory,
        &mut sink,
        &mut checkpoints,
        initial.clone(),
        RecoveryOptions {
            max_reconnects: 2,
            reconnect_delay: Duration::ZERO,
            heartbeat_timeout: Duration::from_secs(1),
        },
        None,
    )
    .await;

    assert!(matches!(
        result,
        Err(ReplicationError {
            kind: ReplicationErrorKind::Authentication,
            ..
        })
    ));
    assert_eq!(factory.starts, vec![initial, confirmed.clone()]);
    assert_eq!(
        checkpoints
            .load()
            .await?
            .map(|checkpoint| checkpoint.confirmed_offset),
        Some(confirmed)
    );
    Ok(())
}

#[tokio::test]
async fn heartbeat_activity_prevents_timeout_and_silence_reconnects_from_checkpoint(
) -> Result<(), ReplicationError> {
    let confirmed = CdcOffset::Position {
        file: "mysql-bin.000003".to_owned(),
        position: 300,
    };
    let heartbeat = RawReplicationEvent::Heartbeat {
        offset: confirmed.clone(),
    };
    let mut factory = HeartbeatFactory {
        starts: Vec::new(),
        calls: 0,
        heartbeat,
    };
    let mut sink = RecordingSink::default();
    let mut checkpoints = MemoryCheckpointStore {
        checkpoint: Some(PersistentTestCheckpoint::new(
            1,
            confirmed.clone(),
            CheckpointPhase::Streaming,
        )?),
    };

    run_recovery_loop(
        &mut factory,
        &mut sink,
        &mut checkpoints,
        confirmed.clone(),
        RecoveryOptions {
            max_reconnects: 1,
            reconnect_delay: Duration::ZERO,
            heartbeat_timeout: Duration::from_millis(75),
        },
        None,
    )
    .await?;

    assert_eq!(factory.starts, vec![confirmed.clone(), confirmed]);
    Ok(())
}

#[tokio::test]
async fn exit_before_sink_write_replays_without_advancing_checkpoint(
) -> Result<(), ReplicationError> {
    let initial = CdcOffset::Position {
        file: "mysql-bin.000004".to_owned(),
        position: 4,
    };
    let row_offset = initial.with_position("mysql-bin.000004", 100);
    let commit_offset = initial.with_position("mysql-bin.000004", 140);
    let mut factory = ScriptFactory {
        scripts: VecDeque::from([
            transaction(row_offset.clone(), commit_offset.clone(), 1),
            transaction(row_offset, commit_offset.clone(), 1),
        ]),
        starts: Vec::new(),
    };
    let mut sink = RecordingSink::default();
    let mut checkpoints = MemoryCheckpointStore::default();

    let result = run_recovery_loop(
        &mut factory,
        &mut sink,
        &mut checkpoints,
        initial.clone(),
        RecoveryOptions::default(),
        Some(RecoveryFailpoint::BeforeSinkWrite),
    )
    .await;
    assert!(result.is_err());
    assert!(sink.event_ids.is_empty());
    assert!(checkpoints.load().await?.is_none());

    run_recovery_loop(
        &mut factory,
        &mut sink,
        &mut checkpoints,
        initial.clone(),
        RecoveryOptions::default(),
        None,
    )
    .await?;
    assert_eq!(factory.starts, vec![initial.clone(), initial]);
    assert_eq!(sink.event_ids.len(), 1);
    assert_eq!(
        checkpoints
            .load()
            .await?
            .map(|checkpoint| checkpoint.confirmed_offset),
        Some(commit_offset)
    );
    Ok(())
}

#[tokio::test]
async fn exit_during_checkpoint_keeps_previous_checkpoint_and_replays(
) -> Result<(), Box<dyn std::error::Error>> {
    let initial = CdcOffset::Position {
        file: "mysql-bin.000005".to_owned(),
        position: 40,
    };
    let row_offset = initial.with_position("mysql-bin.000005", 100);
    let commit_offset = initial.with_position("mysql-bin.000005", 140);
    let mut factory = ScriptFactory {
        scripts: VecDeque::from([
            transaction(row_offset.clone(), commit_offset.clone(), 1),
            transaction(row_offset, commit_offset.clone(), 1),
        ]),
        starts: Vec::new(),
    };
    let mut sink = RecordingSink::default();
    let temp_dir = tempfile::tempdir()?;
    let checkpoint_path = temp_dir.path().join("checkpoint.json");
    let mut checkpoints = FileCheckpointStore::new(checkpoint_path.clone());
    checkpoints
        .save(&PersistentTestCheckpoint::new(
            1,
            initial.clone(),
            CheckpointPhase::Streaming,
        )?)
        .await?;

    let result = run_recovery_loop(
        &mut factory,
        &mut sink,
        &mut checkpoints,
        initial.clone(),
        RecoveryOptions::default(),
        Some(RecoveryFailpoint::DuringCheckpoint),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(
        checkpoints
            .load()
            .await?
            .map(|checkpoint| checkpoint.confirmed_offset),
        Some(initial.clone())
    );

    let mut checkpoints = FileCheckpointStore::new(checkpoint_path);

    run_recovery_loop(
        &mut factory,
        &mut sink,
        &mut checkpoints,
        initial.clone(),
        RecoveryOptions::default(),
        None,
    )
    .await?;
    assert_eq!(factory.starts, vec![initial.clone(), initial]);
    assert_eq!(sink.event_ids[0], sink.event_ids[1]);
    assert_eq!(
        checkpoints
            .load()
            .await?
            .map(|checkpoint| checkpoint.confirmed_offset),
        Some(commit_offset)
    );
    Ok(())
}

#[tokio::test]
async fn exit_during_large_transaction_replays_all_rows_without_partial_checkpoint(
) -> Result<(), ReplicationError> {
    let initial = CdcOffset::Position {
        file: "mysql-bin.000006".to_owned(),
        position: 4,
    };
    let row_offset = initial.with_position("mysql-bin.000006", 100);
    let commit_offset = initial.with_position("mysql-bin.000006", 180);
    let mut factory = ScriptFactory {
        scripts: VecDeque::from([
            multi_row_transaction(row_offset.clone(), commit_offset.clone(), 5),
            multi_row_transaction(row_offset, commit_offset.clone(), 5),
        ]),
        starts: Vec::new(),
    };
    let mut sink = RecordingSink::default();
    let mut checkpoints = MemoryCheckpointStore::default();

    let result = run_recovery_loop(
        &mut factory,
        &mut sink,
        &mut checkpoints,
        initial.clone(),
        RecoveryOptions::default(),
        Some(RecoveryFailpoint::DuringLargeTransaction { after_rows: 2 }),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(sink.event_ids.len(), 2);
    assert!(sink.acknowledged.is_empty());
    assert!(checkpoints.load().await?.is_none());

    run_recovery_loop(
        &mut factory,
        &mut sink,
        &mut checkpoints,
        initial.clone(),
        RecoveryOptions::default(),
        None,
    )
    .await?;
    assert_eq!(factory.starts, vec![initial.clone(), initial]);
    assert_eq!(sink.event_ids.len(), 7);
    assert_eq!(sink.event_ids[0], sink.event_ids[2]);
    assert_eq!(sink.event_ids[1], sink.event_ids[3]);
    assert_eq!(
        checkpoints
            .load()
            .await?
            .map(|checkpoint| checkpoint.confirmed_offset),
        Some(commit_offset)
    );
    Ok(())
}

#[tokio::test]
async fn snapshot_handoff_checkpoint_survives_exit_before_incremental_connect(
) -> Result<(), ReplicationError> {
    let handoff = CdcOffset::Position {
        file: "mysql-bin.000007".to_owned(),
        position: 700,
    };
    let mut factory = ScriptFactory {
        scripts: VecDeque::from([vec![Ok(None)]]),
        starts: Vec::new(),
    };
    let mut sink = RecordingSink::default();
    let mut checkpoints = MemoryCheckpointStore {
        checkpoint: Some(PersistentTestCheckpoint::new(
            1,
            handoff.clone(),
            CheckpointPhase::SnapshotComplete,
        )?),
    };

    let result = run_recovery_loop(
        &mut factory,
        &mut sink,
        &mut checkpoints,
        handoff.clone(),
        RecoveryOptions::default(),
        Some(RecoveryFailpoint::AfterSnapshotBeforeIncrementalHandoff),
    )
    .await;
    assert!(result.is_err());
    assert!(factory.starts.is_empty());

    run_recovery_loop(
        &mut factory,
        &mut sink,
        &mut checkpoints,
        handoff.clone(),
        RecoveryOptions::default(),
        None,
    )
    .await?;
    assert_eq!(factory.starts, vec![handoff]);
    Ok(())
}

#[tokio::test]
async fn confirmed_gtid_checkpoint_is_the_next_connection_start() -> Result<(), ReplicationError> {
    let initial = CdcOffset::Gtid {
        resume_set: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-41".to_owned(),
        transaction_gtid: None,
        file: "mysql-bin.000008".to_owned(),
        position: 4,
    };
    let transaction_offset = CdcOffset::Gtid {
        resume_set: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-41".to_owned(),
        transaction_gtid: Some("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:42".to_owned()),
        file: "mysql-bin.000008".to_owned(),
        position: 100,
    };
    let commit_offset = transaction_offset.with_position("mysql-bin.000008", 140);
    let expected_resume = CdcOffset::Gtid {
        resume_set: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-42".to_owned(),
        transaction_gtid: None,
        file: "mysql-bin.000008".to_owned(),
        position: 140,
    };
    let mut checkpoints = MemoryCheckpointStore::default();
    let mut sink = RecordingSink::default();
    let mut first_factory = ScriptFactory {
        scripts: VecDeque::from([transaction(transaction_offset, commit_offset, 1)]),
        starts: Vec::new(),
    };

    run_recovery_loop(
        &mut first_factory,
        &mut sink,
        &mut checkpoints,
        initial.clone(),
        RecoveryOptions::default(),
        None,
    )
    .await?;

    let mut restarted_factory = ScriptFactory {
        scripts: VecDeque::from([vec![Ok(None)]]),
        starts: Vec::new(),
    };
    run_recovery_loop(
        &mut restarted_factory,
        &mut sink,
        &mut checkpoints,
        initial,
        RecoveryOptions::default(),
        None,
    )
    .await?;

    assert_eq!(restarted_factory.starts, vec![expected_resume]);
    Ok(())
}

#[tokio::test]
async fn retryable_errors_stop_after_the_configured_reconnect_limit() {
    let initial = CdcOffset::Position {
        file: "mysql-bin.000009".to_owned(),
        position: 4,
    };
    let network_error = || {
        ReplicationError::new(
            ReplicationErrorKind::Network,
            true,
            "temporary network error",
        )
    };
    let mut factory = ScriptFactory {
        scripts: VecDeque::from([
            vec![Err(network_error())],
            vec![Err(network_error())],
            vec![Err(network_error())],
        ]),
        starts: Vec::new(),
    };
    let mut sink = RecordingSink::default();
    let mut checkpoints = MemoryCheckpointStore::default();

    let result = run_recovery_loop(
        &mut factory,
        &mut sink,
        &mut checkpoints,
        initial.clone(),
        RecoveryOptions {
            max_reconnects: 2,
            reconnect_delay: Duration::ZERO,
            heartbeat_timeout: Duration::from_secs(1),
        },
        None,
    )
    .await;

    assert!(matches!(
        result,
        Err(ReplicationError {
            kind: ReplicationErrorKind::Network,
            retryable: true,
            ..
        })
    ));
    assert_eq!(
        factory.starts,
        vec![initial.clone(), initial.clone(), initial]
    );
}

#[tokio::test]
async fn sink_rejection_prevents_checkpoint_advance() {
    let initial = CdcOffset::Position {
        file: "mysql-bin.000010".to_owned(),
        position: 4,
    };
    let mut factory = ScriptFactory {
        scripts: VecDeque::from([transaction(
            initial.with_position("mysql-bin.000010", 100),
            initial.with_position("mysql-bin.000010", 140),
            1,
        )]),
        starts: Vec::new(),
    };
    let mut sink = RecordingSink {
        fail_acknowledge: true,
        ..RecordingSink::default()
    };
    let mut checkpoints = MemoryCheckpointStore::default();

    let result = run_recovery_loop(
        &mut factory,
        &mut sink,
        &mut checkpoints,
        initial,
        RecoveryOptions::default(),
        None,
    )
    .await;

    assert!(result.is_err());
    assert_eq!(sink.event_ids.len(), 1);
    assert!(sink.acknowledged.is_empty());
    assert!(checkpoints.checkpoint.is_none());
}

#[tokio::test]
async fn interrupted_file_checkpoint_keeps_the_previous_atomic_record(
) -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let checkpoint_path = temp_dir.path().join("checkpoint.json");
    let mut store = FileCheckpointStore::new(checkpoint_path.clone());
    let first = PersistentTestCheckpoint::new(
        1,
        CdcOffset::Position {
            file: "mysql-bin.000011".to_owned(),
            position: 100,
        },
        CheckpointPhase::Streaming,
    )?;
    let second = PersistentTestCheckpoint::new(
        2,
        CdcOffset::Position {
            file: "mysql-bin.000011".to_owned(),
            position: 200,
        },
        CheckpointPhase::Streaming,
    )?;
    store.save(&first).await?;

    assert!(store.save_interrupted(&second).await.is_err());
    drop(store);

    let mut restarted_store = FileCheckpointStore::new(checkpoint_path);
    assert_eq!(restarted_store.load().await?, Some(first));
    restarted_store.save(&second).await?;
    assert_eq!(restarted_store.load().await?, Some(second));
    Ok(())
}

#[tokio::test]
async fn connection_errors_use_the_same_bounded_retry_classification(
) -> Result<(), ReplicationError> {
    let initial = CdcOffset::Position {
        file: "mysql-bin.000012".to_owned(),
        position: 4,
    };
    let mut factory = ConnectFailureFactory {
        results: VecDeque::from([
            Err(ReplicationError::new(
                ReplicationErrorKind::Network,
                true,
                "temporary connect failure",
            )),
            Ok(()),
        ]),
        starts: Vec::new(),
    };
    let mut sink = RecordingSink::default();
    let mut checkpoints = MemoryCheckpointStore::default();

    run_recovery_loop(
        &mut factory,
        &mut sink,
        &mut checkpoints,
        initial.clone(),
        RecoveryOptions {
            max_reconnects: 1,
            reconnect_delay: Duration::ZERO,
            heartbeat_timeout: Duration::from_secs(1),
        },
        None,
    )
    .await?;
    assert_eq!(factory.starts, vec![initial.clone(), initial.clone()]);

    let mut permanent_factory = ConnectFailureFactory {
        results: VecDeque::from([Err(ReplicationError::new(
            ReplicationErrorKind::Authentication,
            false,
            "permanent connect failure",
        ))]),
        starts: Vec::new(),
    };
    let result = run_recovery_loop(
        &mut permanent_factory,
        &mut sink,
        &mut checkpoints,
        initial.clone(),
        RecoveryOptions::default(),
        None,
    )
    .await;
    assert!(matches!(
        result,
        Err(ReplicationError {
            kind: ReplicationErrorKind::Authentication,
            ..
        })
    ));
    assert_eq!(permanent_factory.starts, vec![initial]);
    Ok(())
}

#[tokio::test]
async fn mysql_server_error_classification_drives_recovery_policy() -> Result<(), ReplicationError>
{
    let initial = CdcOffset::Position {
        file: "mysql-bin.000013".to_owned(),
        position: 4,
    };
    let unavailable = classify_mysql_server_error(1040, "too many connections");
    assert_eq!(unavailable.kind, ReplicationErrorKind::ServerUnavailable);
    assert!(unavailable.retryable);
    let authentication = classify_mysql_server_error(1045, "access denied");
    assert_eq!(authentication.kind, ReplicationErrorKind::Authentication);
    assert!(!authentication.retryable);
    let purged = classify_mysql_server_error(1236, "binlog unavailable");
    assert_eq!(purged.kind, ReplicationErrorKind::BinlogPurged);
    assert!(!purged.retryable);

    let mut factory = ScriptFactory {
        scripts: VecDeque::from([
            vec![Err(classify_mysql_server_error(
                1053,
                "server shutdown in progress",
            ))],
            vec![Ok(None)],
        ]),
        starts: Vec::new(),
    };
    let mut sink = RecordingSink::default();
    let mut checkpoints = MemoryCheckpointStore::default();
    run_recovery_loop(
        &mut factory,
        &mut sink,
        &mut checkpoints,
        initial.clone(),
        RecoveryOptions {
            max_reconnects: 1,
            reconnect_delay: Duration::ZERO,
            heartbeat_timeout: Duration::from_secs(1),
        },
        None,
    )
    .await?;

    assert_eq!(factory.starts, vec![initial.clone(), initial]);
    Ok(())
}
