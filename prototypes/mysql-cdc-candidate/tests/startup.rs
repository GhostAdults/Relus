use std::collections::{BTreeMap, VecDeque};

use async_trait::async_trait;
use relus_mysql_cdc_candidate::{
    prepare_startup, BinlogTransactionBoundary, CdcEnvelope, CdcOffset, CdcValue, CheckpointPhase,
    InitialSnapshotSource, PersistentTestCheckpoint, ReplicationError, ReplicationErrorKind,
    SnapshotCompletion, SnapshotCursor, SnapshotRow, SnapshotSink, StartupMode,
    StartupOffsetProvider, TestCheckpointStore,
};

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

struct TestStartupSource {
    earliest: CdcOffset,
    latest: CdcOffset,
    valid_specific: Vec<CdcOffset>,
    boundaries: Vec<BinlogTransactionBoundary>,
    snapshots: VecDeque<Result<TestSnapshotCursor, ReplicationError>>,
    snapshot_starts: usize,
}

#[async_trait]
impl StartupOffsetProvider for TestStartupSource {
    async fn earliest_offset(&mut self) -> Result<CdcOffset, ReplicationError> {
        Ok(self.earliest.clone())
    }

    async fn latest_offset(&mut self) -> Result<CdcOffset, ReplicationError> {
        Ok(self.latest.clone())
    }

    async fn validate_specific_offset(
        &mut self,
        offset: &CdcOffset,
    ) -> Result<(), ReplicationError> {
        if self.valid_specific.contains(offset) {
            Ok(())
        } else {
            Err(ReplicationError::new(
                ReplicationErrorKind::BinlogPurged,
                false,
                "requested startup offset is not retained",
            ))
        }
    }

    async fn transaction_boundaries(
        &mut self,
    ) -> Result<Vec<BinlogTransactionBoundary>, ReplicationError> {
        Ok(self.boundaries.clone())
    }
}

#[async_trait]
impl InitialSnapshotSource for TestStartupSource {
    async fn begin_consistent_snapshot(
        &mut self,
        _table: &str,
    ) -> Result<Box<dyn SnapshotCursor>, ReplicationError> {
        self.snapshot_starts += 1;
        match self.snapshots.pop_front() {
            Some(Ok(cursor)) => Ok(Box::new(cursor)),
            Some(Err(error)) => Err(error),
            None => Err(ReplicationError::new(
                ReplicationErrorKind::Protocol,
                false,
                "no snapshot script available",
            )),
        }
    }
}

struct TestSnapshotCursor {
    database: String,
    table: String,
    engine: String,
    unique_key_columns: Vec<String>,
    captured_offset: CdcOffset,
    rows: VecDeque<Result<Option<SnapshotRow>, ReplicationError>>,
}

#[async_trait]
impl SnapshotCursor for TestSnapshotCursor {
    fn database(&self) -> &str {
        &self.database
    }

    fn table(&self) -> &str {
        &self.table
    }

    fn engine(&self) -> &str {
        &self.engine
    }

    fn unique_key_columns(&self) -> &[String] {
        &self.unique_key_columns
    }

    fn captured_offset(&self) -> &CdcOffset {
        &self.captured_offset
    }

    async fn next_row(&mut self) -> Result<Option<SnapshotRow>, ReplicationError> {
        self.rows.pop_front().unwrap_or(Ok(None))
    }
}

struct RecordingSnapshotSink {
    event_ids: Vec<String>,
    operations: Vec<String>,
    completions: Vec<SnapshotCompletion>,
    fail_after_writes: Option<usize>,
    idempotent: bool,
}

impl Default for RecordingSnapshotSink {
    fn default() -> Self {
        Self {
            event_ids: Vec::new(),
            operations: Vec::new(),
            completions: Vec::new(),
            fail_after_writes: None,
            idempotent: true,
        }
    }
}

#[async_trait]
impl SnapshotSink for RecordingSnapshotSink {
    fn supports_idempotent_snapshot(&self) -> bool {
        self.idempotent
    }
    async fn write_snapshot(&mut self, envelope: &CdcEnvelope) -> Result<(), ReplicationError> {
        self.event_ids.push(envelope.event_id.clone());
        self.operations
            .push(format!("{:?}", envelope.change.operation));
        if self
            .fail_after_writes
            .is_some_and(|limit| self.event_ids.len() >= limit)
        {
            return Err(ReplicationError::new(
                ReplicationErrorKind::Protocol,
                false,
                "snapshot sink failure",
            ));
        }
        Ok(())
    }

    async fn acknowledge_snapshot(
        &mut self,
        completion: &SnapshotCompletion,
    ) -> Result<(), ReplicationError> {
        self.completions.push(completion.clone());
        Ok(())
    }
}

fn position(file: &str, position: u64) -> CdcOffset {
    CdcOffset::Position {
        file: file.to_owned(),
        position,
    }
}

fn gtid(set: &str, file: &str, position: u64) -> CdcOffset {
    CdcOffset::Gtid {
        resume_set: set.to_owned(),
        transaction_gtid: None,
        file: file.to_owned(),
        position,
    }
}

fn snapshot_row(id: u64) -> SnapshotRow {
    SnapshotRow {
        key: BTreeMap::from([("id".to_owned(), CdcValue::Unsigned(id))]),
        values: BTreeMap::from([
            ("id".to_owned(), CdcValue::Unsigned(id)),
            ("name".to_owned(), CdcValue::String(format!("order-{id}"))),
        ]),
    }
}

fn snapshot_cursor(
    captured_offset: CdcOffset,
    rows: impl IntoIterator<Item = Result<Option<SnapshotRow>, ReplicationError>>,
) -> TestSnapshotCursor {
    TestSnapshotCursor {
        database: "sales".to_owned(),
        table: "orders".to_owned(),
        engine: "InnoDB".to_owned(),
        unique_key_columns: vec!["id".to_owned()],
        captured_offset,
        rows: rows.into_iter().collect(),
    }
}

fn source() -> TestStartupSource {
    TestStartupSource {
        earliest: position("mysql-bin.000010", 4),
        latest: position("mysql-bin.000012", 900),
        valid_specific: Vec::new(),
        boundaries: Vec::new(),
        snapshots: VecDeque::new(),
        snapshot_starts: 0,
    }
}

#[tokio::test]
async fn specific_file_position_and_gtid_offsets_are_preserved() -> Result<(), ReplicationError> {
    let file_offset = position("mysql-bin.000011", 456);
    let gtid_offset = gtid(
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100",
        "mysql-bin.000012",
        4,
    );
    let mut source = source();
    source.valid_specific = vec![file_offset.clone(), gtid_offset.clone()];
    let mut sink = RecordingSnapshotSink {
        idempotent: true,
        ..RecordingSnapshotSink::default()
    };
    let mut checkpoints = MemoryCheckpointStore::default();

    let resolved_file = prepare_startup(
        &mut source,
        &mut sink,
        &mut checkpoints,
        StartupMode::Specific(file_offset.clone()),
    )
    .await?;
    let resolved_gtid = prepare_startup(
        &mut source,
        &mut sink,
        &mut checkpoints,
        StartupMode::Specific(gtid_offset.clone()),
    )
    .await?;

    assert_eq!(resolved_file, file_offset);
    assert_eq!(resolved_gtid, gtid_offset);
    Ok(())
}

#[tokio::test]
async fn earliest_and_latest_use_current_retained_boundaries() -> Result<(), ReplicationError> {
    let mut source = source();
    let mut sink = RecordingSnapshotSink::default();
    let mut checkpoints = MemoryCheckpointStore::default();

    let earliest = prepare_startup(
        &mut source,
        &mut sink,
        &mut checkpoints,
        StartupMode::Earliest,
    )
    .await?;
    let latest = prepare_startup(
        &mut source,
        &mut sink,
        &mut checkpoints,
        StartupMode::Latest,
    )
    .await?;

    assert_eq!(earliest, position("mysql-bin.000010", 4));
    assert_eq!(latest, position("mysql-bin.000012", 900));
    Ok(())
}

#[tokio::test]
async fn timestamp_selects_the_start_of_the_complete_covering_transaction(
) -> Result<(), ReplicationError> {
    let first_start = position("mysql-bin.000010", 100);
    let second_start = position("mysql-bin.000010", 300);
    let mut source = source();
    source.boundaries = vec![
        BinlogTransactionBoundary {
            start_offset: first_start.clone(),
            commit_offset: position("mysql-bin.000010", 200),
            started_at_utc_seconds: 1_767_225_600,
            committed_at_utc_seconds: 1_767_225_610,
        },
        BinlogTransactionBoundary {
            start_offset: second_start.clone(),
            commit_offset: position("mysql-bin.000010", 400),
            started_at_utc_seconds: 1_767_225_620,
            committed_at_utc_seconds: 1_767_225_660,
        },
    ];
    let mut sink = RecordingSnapshotSink::default();
    let mut checkpoints = MemoryCheckpointStore::default();

    let resolved = prepare_startup(
        &mut source,
        &mut sink,
        &mut checkpoints,
        StartupMode::Timestamp("2026-01-01T08:00:30+08:00".to_owned()),
    )
    .await?;

    assert_eq!(resolved, second_start);
    source.boundaries.clear();
    let resumed = prepare_startup(
        &mut source,
        &mut sink,
        &mut checkpoints,
        StartupMode::Timestamp("not parsed on resume".to_owned()),
    )
    .await?;
    assert_eq!(resumed, resolved);
    Ok(())
}

#[tokio::test]
async fn missing_or_purged_history_fails_without_startup_fallback() {
    let mut source = source();
    source.boundaries = vec![BinlogTransactionBoundary {
        start_offset: position("mysql-bin.000010", 100),
        commit_offset: position("mysql-bin.000010", 200),
        started_at_utc_seconds: 1_767_225_600,
        committed_at_utc_seconds: 1_767_225_610,
    }];
    let mut sink = RecordingSnapshotSink::default();
    let mut checkpoints = MemoryCheckpointStore::default();

    let purged_timestamp = prepare_startup(
        &mut source,
        &mut sink,
        &mut checkpoints,
        StartupMode::Timestamp("2025-12-31T23:59:59Z".to_owned()),
    )
    .await;
    assert!(matches!(
        purged_timestamp,
        Err(ReplicationError {
            kind: ReplicationErrorKind::BinlogPurged,
            ..
        })
    ));

    let missing_specific = prepare_startup(
        &mut source,
        &mut sink,
        &mut checkpoints,
        StartupMode::Specific(position("mysql-bin.000001", 4)),
    )
    .await;
    assert!(matches!(
        missing_specific,
        Err(ReplicationError {
            kind: ReplicationErrorKind::BinlogPurged,
            ..
        })
    ));

    let missing_offset = prepare_startup(
        &mut source,
        &mut sink,
        &mut checkpoints,
        StartupMode::Timestamp("2026-01-02T00:00:00Z".to_owned()),
    )
    .await;
    assert!(matches!(
        missing_offset,
        Err(ReplicationError {
            kind: ReplicationErrorKind::InvalidOffset,
            ..
        })
    ));

    let offsetless_timestamp = prepare_startup(
        &mut source,
        &mut sink,
        &mut checkpoints,
        StartupMode::Timestamp("2026-01-01T00:00:00".to_owned()),
    )
    .await;
    assert!(matches!(
        offsetless_timestamp,
        Err(ReplicationError {
            kind: ReplicationErrorKind::InvalidOffset,
            ..
        })
    ));
}

#[tokio::test]
async fn initial_snapshot_is_acknowledged_before_handoff_to_the_captured_offset(
) -> Result<(), ReplicationError> {
    let captured = position("mysql-bin.000020", 500);
    let mut source = source();
    source.valid_specific.push(captured.clone());
    source.snapshots.push_back(Ok(snapshot_cursor(
        captured.clone(),
        [
            Ok(Some(snapshot_row(1))),
            Ok(Some(snapshot_row(2))),
            Ok(None),
        ],
    )));
    let mut sink = RecordingSnapshotSink::default();
    let mut checkpoints = MemoryCheckpointStore::default();

    let handoff = prepare_startup(
        &mut source,
        &mut sink,
        &mut checkpoints,
        StartupMode::Initial {
            table: "orders".to_owned(),
        },
    )
    .await?;

    assert_eq!(handoff, captured.clone());
    assert_eq!(sink.operations, vec!["Snapshot", "Snapshot"]);
    assert_eq!(sink.completions.len(), 1);
    assert_eq!(sink.completions[0].captured_offset, captured.clone());
    assert_eq!(sink.completions[0].row_count, 2);
    assert_eq!(source.snapshot_starts, 1);
    assert_eq!(
        checkpoints
            .load()
            .await?
            .map(|checkpoint| (checkpoint.phase, checkpoint.confirmed_offset)),
        Some((CheckpointPhase::SnapshotComplete, captured))
    );
    Ok(())
}

#[tokio::test]
async fn failed_initial_snapshot_restarts_from_the_beginning() -> Result<(), ReplicationError> {
    let captured = position("mysql-bin.000021", 500);
    let scripted_rows = || {
        [
            Ok(Some(snapshot_row(1))),
            Ok(Some(snapshot_row(2))),
            Ok(None),
        ]
    };
    let mut source = source();
    source.valid_specific.push(captured.clone());
    source
        .snapshots
        .push_back(Ok(snapshot_cursor(captured.clone(), scripted_rows())));
    source
        .snapshots
        .push_back(Ok(snapshot_cursor(captured.clone(), scripted_rows())));
    let mut failing_sink = RecordingSnapshotSink {
        fail_after_writes: Some(2),
        ..RecordingSnapshotSink::default()
    };
    let mut checkpoints = MemoryCheckpointStore::default();

    let first = prepare_startup(
        &mut source,
        &mut failing_sink,
        &mut checkpoints,
        StartupMode::Initial {
            table: "orders".to_owned(),
        },
    )
    .await;
    assert!(first.is_err());
    assert!(checkpoints.load().await?.is_none());

    let first_attempt_ids = failing_sink.event_ids.clone();
    let mut retry_sink = RecordingSnapshotSink::default();
    let handoff = prepare_startup(
        &mut source,
        &mut retry_sink,
        &mut checkpoints,
        StartupMode::Initial {
            table: "orders".to_owned(),
        },
    )
    .await?;

    assert_eq!(handoff, captured);
    assert_eq!(source.snapshot_starts, 2);
    assert_eq!(first_attempt_ids, retry_sink.event_ids);
    Ok(())
}

#[tokio::test]
async fn checkpoint_takes_priority_over_configured_startup_mode() -> Result<(), ReplicationError> {
    let checkpoint_offset = position("mysql-bin.000030", 700);
    let mut source = source();
    let mut sink = RecordingSnapshotSink::default();
    let mut checkpoints = MemoryCheckpointStore {
        checkpoint: Some(PersistentTestCheckpoint::new(
            4,
            checkpoint_offset.clone(),
            CheckpointPhase::Streaming,
        )?),
    };

    let resolved = prepare_startup(
        &mut source,
        &mut sink,
        &mut checkpoints,
        StartupMode::Earliest,
    )
    .await?;

    assert_eq!(resolved, checkpoint_offset);
    Ok(())
}

#[tokio::test]
async fn initial_snapshot_rejects_non_innodb_and_missing_or_null_unique_keys() {
    let captured = position("mysql-bin.000040", 500);
    let mut non_innodb = snapshot_cursor(captured.clone(), [Ok(None)]);
    non_innodb.engine = "MyISAM".to_owned();
    let mut missing_key = snapshot_cursor(captured.clone(), [Ok(None)]);
    missing_key.unique_key_columns.clear();
    let mut null_key = snapshot_cursor(
        captured.clone(),
        [
            Ok(Some(SnapshotRow {
                key: BTreeMap::from([("id".to_owned(), CdcValue::Null)]),
                values: BTreeMap::from([("id".to_owned(), CdcValue::Null)]),
            })),
            Ok(None),
        ],
    );
    null_key.unique_key_columns = vec!["id".to_owned()];
    let mut source1 = source();
    source1.valid_specific.push(captured);
    source1
        .snapshots
        .extend([Ok(non_innodb), Ok(missing_key), Ok(null_key)]);
    let mut checkpoints = MemoryCheckpointStore::default();

    for _ in 0..3 {
        let mut sink = RecordingSnapshotSink::default();
        let result = prepare_startup(
            &mut source1,
            &mut sink,
            &mut checkpoints,
            StartupMode::Initial {
                table: "orders".to_owned(),
            },
        )
        .await;
        assert!(matches!(
            result,
            Err(ReplicationError {
                kind: ReplicationErrorKind::Configuration,
                ..
            })
        ));
        assert!(checkpoints.checkpoint.is_none());
    }

    let mut source2 = source();
    let captured = position("mysql-bin.000041", 500);
    source2.valid_specific.push(captured.clone());
    source2
        .snapshots
        .push_back(Ok(snapshot_cursor(captured, [Ok(None)])));
    let mut sink = RecordingSnapshotSink {
        idempotent: false,
        ..RecordingSnapshotSink::default()
    };
    let result = prepare_startup(
        &mut source2,
        &mut sink,
        &mut checkpoints,
        StartupMode::Initial {
            table: "orders".to_owned(),
        },
    )
    .await;
    assert!(matches!(
        result,
        Err(ReplicationError {
            kind: ReplicationErrorKind::Configuration,
            ..
        })
    ));
}
