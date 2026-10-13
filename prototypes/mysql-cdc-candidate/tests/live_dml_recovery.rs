#![allow(clippy::expect_used)]

use std::{env, time::Duration};

use mysql_async::{prelude::Queryable, Conn, Opts, OptsBuilder, Row, SslOpts, TxOpts};
use relus_mysql_cdc_candidate::{
    CdcEnvelope, CdcMessage, CdcOffset, CdcOperation, CdcValue, CheckpointPhase,
    FileCheckpointStore, MySqlAsyncReplicationClient, MySqlConnectionConfig,
    PersistentTestCheckpoint, TestCheckpointStore, TlsMode, TransactionAssembler,
    TransactionCommit,
};

const TABLE_NAME: &str = "cdc_events";
const LIVE_TIMEOUT: Duration = Duration::from_secs(45);

#[derive(Clone, Copy)]
enum LiveTls {
    Disabled,
    Encrypted,
}

#[derive(Clone)]
struct LiveProfile {
    label: &'static str,
    host: String,
    port: u16,
    username: String,
    password: String,
    tls: LiveTls,
    server_id_base: u32,
}

#[derive(Clone, Copy)]
enum OffsetMode {
    Position,
    Gtid,
}

#[derive(Clone, Copy)]
enum WriteAction {
    TypeMatrix,
    ReplayCandidate,
    AfterCheckpoint,
}

struct CapturedTransaction {
    rows: Vec<CdcEnvelope>,
    commit: TransactionCommit,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires configured live MySQL 5.6 and 8.0 instances"]
async fn live_mysql56_dml_and_recovery() -> Result<(), String> {
    run_profile(LiveProfile::from_environment(
        "mysql56",
        "RELUS_TEST_MYSQL56_",
        LiveTls::Disabled,
        561_100,
    )?)
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires configured live MySQL 5.6 and 8.0 instances"]
async fn live_mysql80_dml_and_recovery() -> Result<(), String> {
    run_profile(LiveProfile::from_environment(
        "mysql80",
        "RELUS_TEST_MYSQL80_",
        LiveTls::Encrypted,
        801_100,
    )?)
    .await
}

impl LiveProfile {
    fn from_environment(
        label: &'static str,
        prefix: &str,
        tls: LiveTls,
        server_id_base: u32,
    ) -> Result<Self, String> {
        Ok(Self {
            label,
            host: required_environment(&format!("{prefix}HOST"))?,
            port: required_environment(&format!("{prefix}PORT"))?
                .parse()
                .map_err(|_| format!("{label}: invalid port configuration"))?,
            username: required_environment(&format!("{prefix}USERNAME"))?,
            password: required_environment(&format!("{prefix}PASSWORD"))?,
            tls,
            server_id_base,
        })
    }

    fn control_opts(&self) -> Opts {
        let mut builder = OptsBuilder::default()
            .ip_or_hostname(self.host.clone())
            .tcp_port(self.port)
            .user(Some(self.username.clone()))
            .pass(Some(self.password.clone()));
        if matches!(self.tls, LiveTls::Encrypted) {
            builder = builder.ssl_opts(Some(
                SslOpts::default()
                    .with_danger_accept_invalid_certs(true)
                    .with_danger_skip_domain_validation(true),
            ));
        }
        builder.into()
    }

    fn replication_config(
        &self,
        database: &str,
        server_id: u32,
        start_offset: CdcOffset,
    ) -> MySqlConnectionConfig {
        MySqlConnectionConfig {
            host: self.host.clone(),
            port: self.port,
            username: self.username.clone(),
            password: self.password.clone(),
            database: database.to_owned(),
            server_id,
            start_offset,
            tls: match self.tls {
                LiveTls::Disabled => TlsMode::Disabled,
                LiveTls::Encrypted => TlsMode::Encrypted,
            },
            protocol_compression: false,
        }
    }
}

async fn run_profile(profile: LiveProfile) -> Result<(), String> {
    let mut failures = Vec::new();
    if let Err(error) = run_isolated_scenario(&profile, OffsetMode::Position, 1).await {
        failures.push(error);
    }
    if let Err(error) = run_isolated_scenario(&profile, OffsetMode::Gtid, 2).await {
        failures.push(error);
    }
    if !failures.is_empty() {
        return Err(failures.join("; "));
    }
    println!(
        "{} live DML/recovery passed for file-position and GTID",
        profile.label
    );
    Ok(())
}

async fn run_isolated_scenario(
    profile: &LiveProfile,
    mode: OffsetMode,
    scenario_number: u32,
) -> Result<(), String> {
    let database = unique_database(profile.label, mode);
    let mut control = connect_control(profile, "control connection").await?;
    let setup = setup_database(&mut control, &database, profile.label).await;
    if let Err(error) = setup {
        let _ = cleanup_database(profile, &database).await;
        return Err(error);
    }

    let result = run_scenario(
        profile,
        &mut control,
        &database,
        mode,
        profile.server_id_base + scenario_number,
    )
    .await;
    let disconnect_result = control.disconnect().await;
    let cleanup_result = cleanup_database(profile, &database).await;
    if disconnect_result.is_err() && result.is_ok() {
        return Err(format!("{}: control disconnect failed", profile.label));
    }
    cleanup_result?;
    result
}

async fn setup_database(control: &mut Conn, database: &str, label: &str) -> Result<(), String> {
    let format: String = control
        .query_first("SELECT @@GLOBAL.binlog_format")
        .await
        .map_err(|_| format!("{label}: binlog format query failed"))?
        .ok_or_else(|| format!("{label}: binlog format was unavailable"))?;
    if !format.eq_ignore_ascii_case("ROW") {
        return Err(format!("{label}: binlog_format is not ROW"));
    }
    let row_image: String = control
        .query_first("SELECT @@GLOBAL.binlog_row_image")
        .await
        .map_err(|_| format!("{label}: binlog row-image query failed"))?
        .ok_or_else(|| format!("{label}: binlog row image was unavailable"))?;
    if !row_image.eq_ignore_ascii_case("FULL") {
        return Err(format!("{label}: binlog_row_image is not FULL"));
    }

    control
        .query_drop(format!(
            "CREATE DATABASE `{database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        ))
        .await
        .map_err(|_| format!("{label}: test database creation failed"))?;
    control
        .query_drop(format!(
            "CREATE TABLE `{database}`.`{TABLE_NAME}` (\
             id BIGINT UNSIGNED NOT NULL PRIMARY KEY,\
             signed_value BIGINT NOT NULL,\
             unsigned_value BIGINT UNSIGNED NOT NULL,\
             decimal_value DECIMAL(30,10) NOT NULL,\
             text_value VARCHAR(255) CHARACTER SET utf8mb4 NOT NULL,\
             binary_value VARBINARY(8) NOT NULL,\
             date_value DATE NOT NULL,\
             datetime_value DATETIME(6) NOT NULL,\
             time_value TIME(6) NOT NULL,\
             nullable_value VARCHAR(32) NULL\
             ) ENGINE=InnoDB"
        ))
        .await
        .map_err(|_| format!("{label}: test table creation failed"))?;
    Ok(())
}

async fn run_scenario(
    profile: &LiveProfile,
    control: &mut Conn,
    database: &str,
    mode: OffsetMode,
    server_id: u32,
) -> Result<(), String> {
    let initial_offset = capture_start_offset(control, profile.label, mode).await?;
    let initial_file = initial_offset.file().to_owned();
    let initial_position = initial_offset.position();
    let first = capture_after(
        profile.replication_config(database, server_id, initial_offset),
        database,
        control,
        WriteAction::TypeMatrix,
        matches!(mode, OffsetMode::Position),
    )
    .await
    .map_err(|error| format!("{}: initial DML capture: {error}", profile.label))?;
    if first.commit.offset.file().is_empty()
        || first.commit.offset.file().contains('\0')
        || first.commit.offset.file() != initial_file
    {
        return Err(format!(
            "{}: commit checkpoint carried an invalid binlog filename",
            profile.label
        ));
    }
    if first.commit.offset.position() < initial_position {
        return Err(format!(
            "{}: commit checkpoint moved backwards",
            profile.label
        ));
    }
    if !checkpoint_matches_binlog_boundary(
        control,
        &initial_file,
        initial_position,
        first.commit.offset.position(),
        profile.label,
    )
    .await?
    {
        return Err(format!(
            "{}: commit checkpoint was not a server binlog event boundary",
            profile.label
        ));
    }
    assert_type_matrix(profile.label, &first)?;

    let checkpoint_dir = tempfile::tempdir()
        .map_err(|_| format!("{}: checkpoint directory creation failed", profile.label))?;
    let checkpoint_path = checkpoint_dir.path().join("checkpoint.json");
    let mut checkpoints = FileCheckpointStore::new(checkpoint_path);
    let first_checkpoint = PersistentTestCheckpoint::new(
        1,
        first.commit.offset.as_ref().clone(),
        CheckpointPhase::Streaming,
    )
    .map_err(|_| format!("{}: first checkpoint creation failed", profile.label))?;
    checkpoints
        .save(&first_checkpoint)
        .await
        .map_err(|_| format!("{}: first checkpoint persistence failed", profile.label))?;

    let confirmed = checkpoints
        .load()
        .await
        .map_err(|_| format!("{}: first checkpoint reload failed", profile.label))?
        .ok_or_else(|| format!("{}: first checkpoint was not persisted", profile.label))?;
    let second = capture_after(
        profile.replication_config(database, server_id + 10, confirmed.confirmed_offset.clone()),
        database,
        control,
        WriteAction::ReplayCandidate,
        false,
    )
    .await
    .map_err(|error| format!("{}: checkpoint restart: {error}", profile.label))?;
    assert_single_change(profile.label, &second, CdcOperation::Insert)?;
    let original_event_ids = event_ids(&second);

    let replay = capture_existing(
        profile.replication_config(database, server_id + 11, confirmed.confirmed_offset),
        database,
    )
    .await
    .map_err(|error| format!("{}: unconfirmed replay: {error}", profile.label))?;
    assert_single_change(profile.label, &replay, CdcOperation::Insert)?;
    if event_ids(&replay) != original_event_ids {
        return Err(format!(
            "{}: replayed transaction event IDs were not stable",
            profile.label
        ));
    }

    let second_checkpoint = PersistentTestCheckpoint::new(
        2,
        replay.commit.offset.as_ref().clone(),
        CheckpointPhase::Streaming,
    )
    .map_err(|_| format!("{}: second checkpoint creation failed", profile.label))?;
    checkpoints
        .save(&second_checkpoint)
        .await
        .map_err(|_| format!("{}: second checkpoint persistence failed", profile.label))?;
    let confirmed = checkpoints
        .load()
        .await
        .map_err(|_| format!("{}: second checkpoint reload failed", profile.label))?
        .ok_or_else(|| format!("{}: second checkpoint was not persisted", profile.label))?;

    let third = capture_after(
        profile.replication_config(database, server_id + 12, confirmed.confirmed_offset),
        database,
        control,
        WriteAction::AfterCheckpoint,
        false,
    )
    .await
    .map_err(|error| format!("{}: confirmed restart: {error}", profile.label))?;
    assert_single_change(profile.label, &third, CdcOperation::Update)?;
    if event_ids(&third)
        .iter()
        .any(|event_id| original_event_ids.contains(event_id))
    {
        return Err(format!(
            "{}: confirmed transaction replayed after checkpoint restart",
            profile.label
        ));
    }
    Ok(())
}

async fn capture_start_offset(
    control: &mut Conn,
    label: &str,
    mode: OffsetMode,
) -> Result<CdcOffset, String> {
    let status: Row = control
        .query_first("SHOW MASTER STATUS")
        .await
        .map_err(|_| format!("{label}: master status query failed"))?
        .ok_or_else(|| format!("{label}: master status was unavailable"))?;
    let file = status
        .get::<String, _>(0)
        .ok_or_else(|| format!("{label}: master file was unavailable"))?;
    let position = status
        .get::<u64, _>(1)
        .ok_or_else(|| format!("{label}: master position was unavailable"))?;
    match mode {
        OffsetMode::Position => Ok(CdcOffset::Position { file, position }),
        OffsetMode::Gtid => {
            let resume_set: String = control
                .query_first("SELECT @@GLOBAL.gtid_executed")
                .await
                .map_err(|_| format!("{label}: GTID set query failed"))?
                .ok_or_else(|| format!("{label}: GTID set was unavailable"))?;
            if resume_set.trim().is_empty() {
                return Err(format!("{label}: GTID set is empty"));
            }
            Ok(CdcOffset::Gtid {
                resume_set,
                transaction_gtid: None,
                file,
                position,
            })
        }
    }
}

async fn checkpoint_matches_binlog_boundary(
    control: &mut Conn,
    file: &str,
    start_position: u64,
    checkpoint_position: u64,
    label: &str,
) -> Result<bool, String> {
    let escaped_file = file.replace('\'', "''");
    let events: Vec<Row> = control
        .query(format!(
            "SHOW BINLOG EVENTS IN '{escaped_file}' FROM {start_position} LIMIT 100"
        ))
        .await
        .map_err(|_| format!("{label}: binlog-boundary query failed"))?;
    Ok(events.iter().any(|row| {
        row.get::<u64, _>(4)
            .is_some_and(|end_position| end_position == checkpoint_position)
    }))
}

async fn capture_after(
    config: MySqlConnectionConfig,
    database: &str,
    control: &mut Conn,
    write: WriteAction,
    connect_before_write: bool,
) -> Result<CapturedTransaction, String> {
    let label = if matches!(config.tls, TlsMode::Disabled) {
        "mysql56"
    } else {
        "mysql80"
    };
    let mut client = MySqlAsyncReplicationClient::new(config);
    if connect_before_write {
        client
            .connect()
            .await
            .map_err(|_| format!("{label}: replication connection failed"))?;
    }
    let write_result = match write {
        WriteAction::TypeMatrix => insert_update_delete_matrix(control, database).await,
        WriteAction::ReplayCandidate => insert_replay_candidate(control, database).await,
        WriteAction::AfterCheckpoint => update_after_checkpoint(control, database).await,
    };
    if let Err(error) = write_result {
        let _ = client.close().await;
        return Err(error);
    }
    if !connect_before_write {
        client
            .connect()
            .await
            .map_err(|_| format!("{label}: replication connection failed"))?;
    }
    let result = collect_target_transaction(&mut client, database).await;
    let close_result = client.close().await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    if close_result.is_err() && result.is_ok() {
        return Err(format!("{label}: replication close failed"));
    }
    result
}

async fn capture_existing(
    config: MySqlConnectionConfig,
    database: &str,
) -> Result<CapturedTransaction, String> {
    let label = if matches!(config.tls, TlsMode::Disabled) {
        "mysql56"
    } else {
        "mysql80"
    };
    let mut client = MySqlAsyncReplicationClient::new(config);
    client
        .connect()
        .await
        .map_err(|_| format!("{label}: replay connection failed"))?;
    let result = collect_target_transaction(&mut client, database).await;
    let close_result = client.close().await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    if close_result.is_err() && result.is_ok() {
        return Err(format!("{label}: replay close failed"));
    }
    result
}

async fn collect_target_transaction(
    client: &mut MySqlAsyncReplicationClient,
    database: &str,
) -> Result<CapturedTransaction, String> {
    let mut assembler = TransactionAssembler::default();
    let mut target_rows = Vec::new();
    let deadline = tokio::time::Instant::now() + LIVE_TIMEOUT;
    loop {
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .ok_or_else(|| "live replication timed out".to_owned())?;
        let event = tokio::time::timeout(remaining, client.next_event())
            .await
            .map_err(|_| "live replication timed out".to_owned())?
            .map_err(|error| {
                format!(
                    "live replication failed: {:?}/{}",
                    error.kind,
                    sanitized_replication_category(error.message())
                )
            })?
            .ok_or_else(|| "live replication stream ended unexpectedly".to_owned())?;
        for message in assembler
            .push(event)
            .map_err(|error| format!("live transaction assembly failed: {:?}", error.kind))?
        {
            match message {
                CdcMessage::Row(envelope)
                    if envelope.change.database.as_ref() == database
                        && envelope.change.table.as_ref() == TABLE_NAME =>
                {
                    target_rows.push(envelope);
                }
                CdcMessage::Commit(commit) if !target_rows.is_empty() => {
                    return Ok(CapturedTransaction {
                        rows: target_rows,
                        commit,
                    });
                }
                _ => {}
            }
        }
    }
}

fn sanitized_replication_category(message: &str) -> String {
    let message = message.to_ascii_lowercase();
    let category = if message.contains("same server") || message.contains("server-id") {
        "duplicate_server_id"
    } else if message.contains("first log file") || message.contains("binary log index") {
        "binlog_file_unavailable"
    } else if message.contains("position") && message.contains("file size") {
        "position_out_of_range"
    } else if message.contains("impossible position") {
        "impossible_position"
    } else if message.contains("purged") && message.contains("gtid") {
        "gtid_history_unavailable"
    } else if message.contains("gtid") || message.contains("auto_position") {
        "gtid_request_rejected"
    } else if message.contains("requested") && message.contains("master") {
        "requested_offset_rejected"
    } else if message.contains("truncated") {
        "truncated_binlog_event"
    } else if message.contains("crc") {
        "binlog_crc"
    } else if message.contains("checksum") {
        "binlog_checksum"
    } else if message.contains("max_allowed_packet") {
        "event_exceeds_packet_limit"
    } else if message.contains("bogus") || message.contains("corrupt") {
        "invalid_binlog_event"
    } else {
        "unclassified_server_1236"
    };
    if category != "unclassified_server_1236" {
        return category.to_owned();
    }
    let allowed = [
        "after",
        "at",
        "auto",
        "before",
        "binary",
        "binlog",
        "bogus",
        "byte",
        "checksum",
        "client",
        "configured",
        "could",
        "crc",
        "data",
        "entry",
        "error",
        "event",
        "fatal",
        "file",
        "find",
        "first",
        "for",
        "from",
        "gtid",
        "header",
        "id",
        "impossible",
        "in",
        "index",
        "initialization",
        "last",
        "log",
        "master",
        "name",
        "of",
        "packet",
        "position",
        "purged",
        "read",
        "reading",
        "relay",
        "replication",
        "requested",
        "same",
        "server",
        "size",
        "slave",
        "start",
        "starting",
        "target",
        "the",
        "to",
        "truncated",
        "uuid",
    ];
    let words = message
        .split(|character: char| !character.is_ascii_alphabetic())
        .filter(|word| allowed.contains(word))
        .collect::<Vec<_>>()
        .join("_");
    format!("unclassified_server_1236[{words}]")
}

async fn insert_update_delete_matrix(control: &mut Conn, database: &str) -> Result<(), String> {
    let mut transaction = control
        .start_transaction(TxOpts::default())
        .await
        .map_err(|_| "matrix transaction start failed".to_owned())?;
    transaction
            .query_drop(format!(
                "INSERT INTO `{database}`.`{TABLE_NAME}` VALUES \
                 (1,-9223372036854775808,18446744073709551614,12345678901234567890.1234567890,'多字节 utf8',UNHEX('00FF80'),'2026-10-12','2026-10-12 12:34:56.123456','12:34:56.654321',NULL),\
                 (2,7,8,9.2500000000,'delete-me',UNHEX('FE'),'2026-10-13','2026-10-13 01:02:03.000004','01:02:03.000004','present')"
            ))
            .await
            .map_err(|_| "matrix insert failed".to_owned())?;
    transaction
            .query_drop(format!(
                "UPDATE `{database}`.`{TABLE_NAME}` SET signed_value=42,text_value='updated',nullable_value='not-null' WHERE id=1"
            ))
            .await
            .map_err(|_| "matrix update failed".to_owned())?;
    transaction
        .query_drop(format!(
            "DELETE FROM `{database}`.`{TABLE_NAME}` WHERE id=2"
        ))
        .await
        .map_err(|_| "matrix delete failed".to_owned())?;
    transaction
        .commit()
        .await
        .map_err(|_| "matrix transaction commit failed".to_owned())
}

async fn insert_replay_candidate(control: &mut Conn, database: &str) -> Result<(), String> {
    control
            .query_drop(format!(
                "INSERT INTO `{database}`.`{TABLE_NAME}` VALUES \
                 (3,30,31,32.5000000000,'replay-me',UNHEX('FD80'),'2026-10-14','2026-10-14 02:03:04.000005','02:03:04.000005',NULL)"
            ))
            .await
            .map_err(|_| "replay-candidate insert failed".to_owned())
}

async fn update_after_checkpoint(control: &mut Conn, database: &str) -> Result<(), String> {
    control
        .query_drop(format!(
            "UPDATE `{database}`.`{TABLE_NAME}` SET text_value='after-checkpoint' WHERE id=3"
        ))
        .await
        .map_err(|_| "post-checkpoint update failed".to_owned())
}

fn assert_type_matrix(label: &str, transaction: &CapturedTransaction) -> Result<(), String> {
    if transaction.rows.len() != 4 || transaction.commit.event_count != 4 {
        return Err(format!("{label}: DML transaction row count was not four"));
    }
    let mut inserts = transaction
        .rows
        .iter()
        .filter(|envelope| envelope.change.operation == CdcOperation::Insert);
    let insert_one = inserts
        .next()
        .ok_or_else(|| format!("{label}: first insert was absent"))?;
    let insert_two = inserts
        .next()
        .ok_or_else(|| format!("{label}: second insert was absent"))?;
    let update_one = transaction
        .rows
        .iter()
        .find(|envelope| envelope.change.operation == CdcOperation::Update)
        .ok_or_else(|| format!("{label}: update was absent"))?;
    let delete_two = transaction
        .rows
        .iter()
        .find(|envelope| envelope.change.operation == CdcOperation::Delete)
        .ok_or_else(|| format!("{label}: delete was absent"))?;

    let inserted = insert_one
        .change
        .after
        .as_ref()
        .ok_or_else(|| format!("{label}: insert after-image was absent"))?;
    expect_value(
        insert_one.change.key.get("id"),
        CdcValue::Unsigned(1),
        label,
    )?;
    expect_value(
        insert_two.change.key.get("id"),
        CdcValue::Unsigned(2),
        label,
    )?;
    expect_value(
        update_one.change.key.get("id"),
        CdcValue::Unsigned(1),
        label,
    )?;
    expect_value(
        delete_two.change.key.get("id"),
        CdcValue::Unsigned(2),
        label,
    )?;
    expect_value(
        inserted.get("signed_value"),
        CdcValue::Signed(i64::MIN),
        label,
    )?;
    expect_value(
        inserted.get("unsigned_value"),
        CdcValue::Unsigned(u64::MAX - 1),
        label,
    )?;
    expect_value(
        inserted.get("decimal_value"),
        CdcValue::String("12345678901234567890.1234567890".to_owned()),
        label,
    )?;
    expect_value(
        inserted.get("text_value"),
        CdcValue::String("多字节 utf8".to_owned()),
        label,
    )?;
    expect_value(
        inserted.get("binary_value"),
        CdcValue::Bytes(vec![0, 255, 128]),
        label,
    )?;
    expect_value(
        inserted.get("date_value"),
        CdcValue::Date {
            year: 2026,
            month: 10,
            day: 12,
        },
        label,
    )?;
    expect_value(
        inserted.get("datetime_value"),
        CdcValue::DateTime {
            year: 2026,
            month: 10,
            day: 12,
            hour: 12,
            minute: 34,
            second: 56,
            micros: 123_456,
        },
        label,
    )?;
    expect_value(
        inserted.get("time_value"),
        CdcValue::Time {
            negative: false,
            days: 0,
            hours: 12,
            minutes: 34,
            seconds: 56,
            micros: 654_321,
        },
        label,
    )?;
    expect_value(inserted.get("nullable_value"), CdcValue::Null, label)?;

    let before = update_one
        .change
        .before
        .as_ref()
        .ok_or_else(|| format!("{label}: update before-image was absent"))?;
    let after = update_one
        .change
        .after
        .as_ref()
        .ok_or_else(|| format!("{label}: update after-image was absent"))?;
    expect_value(
        before.get("signed_value"),
        CdcValue::Signed(i64::MIN),
        label,
    )?;
    expect_value(after.get("signed_value"), CdcValue::Signed(42), label)?;
    expect_value(
        after.get("text_value"),
        CdcValue::String("updated".to_owned()),
        label,
    )?;
    expect_value(
        after.get("nullable_value"),
        CdcValue::String("not-null".to_owned()),
        label,
    )?;
    if insert_two.change.after.is_none() || delete_two.change.before.is_none() {
        return Err(format!("{label}: insert/delete row images were incomplete"));
    }
    Ok(())
}

fn assert_single_change(
    label: &str,
    transaction: &CapturedTransaction,
    operation: CdcOperation,
) -> Result<(), String> {
    if transaction.rows.len() != 1 || transaction.commit.event_count != 1 {
        return Err(format!("{label}: recovery transaction was not isolated"));
    }
    if transaction.rows[0].change.operation != operation {
        return Err(format!(
            "{label}: recovery DML operation did not match; observed {}",
            transaction_shape(transaction)
        ));
    }
    Ok(())
}

fn transaction_shape(transaction: &CapturedTransaction) -> String {
    transaction
        .rows
        .iter()
        .map(|envelope| {
            let key_columns = serde_json::to_value(&envelope.change.key)
                .ok()
                .and_then(|value| value.as_object().cloned())
                .map(|object| object.keys().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            let before_columns = envelope
                .change
                .before
                .as_ref()
                .and_then(|row| serde_json::to_value(row).ok())
                .and_then(|value| value.as_object().cloned())
                .map(|object| object.keys().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            let after_columns = envelope
                .change
                .after
                .as_ref()
                .and_then(|row| serde_json::to_value(row).ok())
                .and_then(|value| value.as_object().cloned())
                .map(|object| object.keys().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            format!(
                "{:?}:key={key_columns:?}:before={before_columns:?}:after={after_columns:?}",
                envelope.change.operation
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn expect_value(actual: Option<&CdcValue>, expected: CdcValue, label: &str) -> Result<(), String> {
    if actual == Some(&expected) {
        Ok(())
    } else {
        Err(format!(
            "{label}: common-type value did not round trip (expected {expected:?}, observed {actual:?})"
        ))
    }
}

fn event_ids(transaction: &CapturedTransaction) -> Vec<String> {
    transaction
        .rows
        .iter()
        .map(|envelope| envelope.event_id.to_string())
        .chain(std::iter::once(transaction.commit.event_id.clone()))
        .collect()
}

async fn connect_control(profile: &LiveProfile, stage: &str) -> Result<Conn, String> {
    Conn::new(profile.control_opts())
        .await
        .map_err(|_| format!("{}: {stage} failed", profile.label))
}

async fn cleanup_database(profile: &LiveProfile, database: &str) -> Result<(), String> {
    let mut control = connect_control(profile, "cleanup connection").await?;
    control
        .query_drop(format!("DROP DATABASE IF EXISTS `{database}`"))
        .await
        .map_err(|_| format!("{}: test database cleanup failed", profile.label))?;
    let remaining: Option<u64> = control
        .exec_first(
            "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?",
            (database,),
        )
        .await
        .map_err(|_| format!("{}: cleanup verification failed", profile.label))?;
    control
        .disconnect()
        .await
        .map_err(|_| format!("{}: cleanup disconnect failed", profile.label))?;
    if remaining != Some(0) {
        return Err(format!(
            "{}: test database still exists after cleanup",
            profile.label
        ));
    }
    Ok(())
}

fn unique_database(label: &str, mode: OffsetMode) -> String {
    let mode = match mode {
        OffsetMode::Position => "position",
        OffsetMode::Gtid => "gtid",
    };
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("relus_{label}_{mode}_{:012x}", nonce & 0xffff_ffff_ffff)
}

fn required_environment(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("missing required environment variable {name}"))
}
