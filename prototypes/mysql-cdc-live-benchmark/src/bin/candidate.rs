use std::{
    env,
    hint::black_box,
    time::{Duration, Instant},
};

use relus_mysql_cdc_candidate::{
    serialize_message, CdcOffset, MySqlAsyncReplicationClient, MySqlConnectionConfig,
    RawReplicationEvent, TlsMode, TransactionAssembler,
};
use relus_mysql_cdc_live_benchmark::{BenchmarkConfig, BenchmarkResult};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = BenchmarkConfig::from_environment()?;
    let mode = CandidateMode::from_environment()?;
    let target_database = config.database.clone();
    let target_table = config.table.clone();
    let connection = MySqlConnectionConfig {
        host: config.host,
        port: config.port,
        username: config.username,
        password: config.password,
        database: config.database,
        server_id: config.server_id,
        start_offset: CdcOffset::Position {
            file: config.binlog_file,
            position: config.binlog_position,
        },
        tls: TlsMode::Disabled,
        protocol_compression: env::var_os("RELUS_BENCH_MYSQL_COMPRESSION").is_some(),
    };

    let started = Instant::now();
    let mut client = MySqlAsyncReplicationClient::new(connection);
    client.connect().await?;
    let mut rows = 0_u64;
    let mut output_bytes = 0_u64;
    let mut assembler = TransactionAssembler::default();
    let diagnostics_enabled = env::var_os("RELUS_BENCH_DIAGNOSTICS").is_some();
    let mut diagnostics = Diagnostics::default();

    while rows < config.expected_rows {
        let remaining = config
            .timeout
            .checked_sub(started.elapsed())
            .ok_or("candidate timed out before reaching the target row count")?;
        let read_started = diagnostics_enabled.then(Instant::now);
        let event = if diagnostics_enabled {
            let (event, adapter_read, adapter_parse, adapter_map) =
                tokio::time::timeout(remaining, client.next_event_profiled())
                    .await
                    .map_err(|_| "candidate timed out before reaching the target row count")??;
            diagnostics.adapter_read += adapter_read;
            diagnostics.adapter_parse += adapter_parse;
            diagnostics.adapter_map += adapter_map;
            event
        } else {
            tokio::time::timeout(remaining, client.next_event())
                .await
                .map_err(|_| "candidate timed out before reaching the target row count")??
        }
        .ok_or("candidate binlog stream ended before the target row count")?;
        if let Some(read_started) = read_started {
            diagnostics.read += read_started.elapsed();
        }
        diagnostics.events += 1;
        if let RawReplicationEvent::Row { rows: changes, .. } = &event {
            if changes.first().is_some_and(|change| {
                change.database.as_ref() == target_database && change.table.as_ref() == target_table
            }) {
                rows += u64::try_from(changes.len())?;
            }
        }
        if mode != CandidateMode::Decode {
            let assemble_started = diagnostics_enabled.then(Instant::now);
            let messages = assembler.push(event)?;
            if let Some(assemble_started) = assemble_started {
                diagnostics.assemble += assemble_started.elapsed();
            }
            diagnostics.messages += u64::try_from(messages.len())?;
            for message in messages {
                let serialize_started = diagnostics_enabled.then(Instant::now);
                output_bytes += match mode {
                    CandidateMode::Decode => 0,
                    CandidateMode::Assemble => {
                        black_box(&message);
                        1
                    }
                    CandidateMode::Full => {
                        let serialized = serialize_message(&message)?;
                        if diagnostics_enabled {
                            diagnostics.max_serialized_capacity = diagnostics
                                .max_serialized_capacity
                                .max(serialized.capacity());
                            diagnostics.grown_serializations +=
                                u64::from(serialized.capacity() > 768);
                        }
                        u64::try_from(serialized.len())?
                    }
                };
                if let Some(serialize_started) = serialize_started {
                    diagnostics.serialize += serialize_started.elapsed();
                }
            }
        }
    }

    client.close().await?;
    if diagnostics_enabled {
        eprintln!(
            "diagnostics events={} messages={} read_seconds={:.6} adapter_read_seconds={:.6} adapter_parse_seconds={:.6} adapter_map_seconds={:.6} assemble_seconds={:.6} serialize_seconds={:.6} max_serialized_capacity={} grown_serializations={}",
            diagnostics.events,
            diagnostics.messages,
            diagnostics.read.as_secs_f64(),
            diagnostics.adapter_read.as_secs_f64(),
            diagnostics.adapter_parse.as_secs_f64(),
            diagnostics.adapter_map.as_secs_f64(),
            diagnostics.assemble.as_secs_f64(),
            diagnostics.serialize.as_secs_f64(),
            diagnostics.max_serialized_capacity,
            diagnostics.grown_serializations,
        );
    }
    let result = BenchmarkResult::new("mysql_async 0.37.1", rows, output_bytes, started.elapsed());
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

#[derive(Default)]
struct Diagnostics {
    events: u64,
    messages: u64,
    read: Duration,
    adapter_read: Duration,
    adapter_parse: Duration,
    adapter_map: Duration,
    assemble: Duration,
    serialize: Duration,
    max_serialized_capacity: usize,
    grown_serializations: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CandidateMode {
    Decode,
    Assemble,
    Full,
}

impl CandidateMode {
    fn from_environment() -> Result<Self, String> {
        match env::var("RELUS_BENCH_CANDIDATE_MODE").as_deref() {
            Ok("decode") => Ok(Self::Decode),
            Ok("assemble") => Ok(Self::Assemble),
            Ok("full") | Err(env::VarError::NotPresent) => Ok(Self::Full),
            Ok(value) => Err(format!(
                "invalid RELUS_BENCH_CANDIDATE_MODE {value}; expected decode, assemble, or full"
            )),
            Err(error) => Err(format!("invalid RELUS_BENCH_CANDIDATE_MODE value: {error}")),
        }
    }
}
