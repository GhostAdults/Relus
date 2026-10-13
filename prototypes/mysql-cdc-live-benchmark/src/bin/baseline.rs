use std::{collections::HashMap, time::Instant};

use mysql_cdc::{
    binlog_client::BinlogClient,
    binlog_options::BinlogOptions,
    events::binlog_event::BinlogEvent,
    events::row_events::{mysql_value::MySqlValue, row_data::RowData},
    replica_options::ReplicaOptions,
    ssl_mode::SslMode,
};
use relus_mysql_cdc_live_benchmark::{BenchmarkConfig, BenchmarkResult};
use serde_json::Value;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = BenchmarkConfig::from_environment()?;
    let position = u32::try_from(config.binlog_position)
        .map_err(|_| "baseline only accepts a 32-bit binlog position")?;
    let target = config.qualified_table();
    let column_names = ["id", "payload", "amount", "created_at"];
    let options = ReplicaOptions {
        hostname: config.host,
        port: config.port,
        username: config.username,
        password: config.password,
        database: Some(config.database.clone()),
        server_id: config.server_id,
        binlog: BinlogOptions::from_position(config.binlog_file, position),
        ssl_mode: SslMode::Disabled,
        blocking: true,
        ..Default::default()
    };

    let started = Instant::now();
    let mut events = BinlogClient::new(options)
        .replicate()
        .map_err(|error| format!("baseline connection failed: {error:?}"))?;
    let mut table_names = HashMap::<u64, String>::new();
    let mut rows = 0_u64;
    let mut output_bytes = 0_u64;

    while rows < config.expected_rows {
        if started.elapsed() > config.timeout {
            return Err(format!(
                "baseline timed out after decoding {rows} of {} rows",
                config.expected_rows
            )
            .into());
        }
        let (header, event) = events
            .next()
            .ok_or("baseline binlog stream ended before the target row count")?
            .map_err(|error| format!("baseline event decoding failed: {error:?}"))?;
        match event {
            BinlogEvent::TableMapEvent(event) => {
                table_names.insert(
                    event.table_id,
                    format!("{}.{}", event.database_name, event.table_name),
                );
            }
            BinlogEvent::WriteRowsEvent(event)
                if table_names.get(&event.table_id) == Some(&target) =>
            {
                for row in &event.rows {
                    output_bytes += serialized_row_bytes(
                        "insert",
                        &target,
                        header.timestamp,
                        row,
                        &column_names,
                    )?;
                }
                rows += u64::try_from(event.rows.len())?;
            }
            BinlogEvent::UpdateRowsEvent(event)
                if table_names.get(&event.table_id) == Some(&target) =>
            {
                for row in &event.rows {
                    output_bytes += serialized_row_bytes(
                        "update",
                        &target,
                        header.timestamp,
                        &row.after_update,
                        &column_names,
                    )?;
                }
                rows += u64::try_from(event.rows.len())?;
            }
            BinlogEvent::DeleteRowsEvent(event)
                if table_names.get(&event.table_id) == Some(&target) =>
            {
                for row in &event.rows {
                    output_bytes += serialized_row_bytes(
                        "delete",
                        &target,
                        header.timestamp,
                        row,
                        &column_names,
                    )?;
                }
                rows += u64::try_from(event.rows.len())?;
            }
            _ => {}
        }
    }

    let result = BenchmarkResult::new("mysql_cdc 0.2.1", rows, output_bytes, started.elapsed());
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

fn serialized_row_bytes(
    operation: &str,
    table: &str,
    timestamp: u32,
    row: &RowData,
    column_names: &[&str],
) -> Result<u64, Box<dyn std::error::Error>> {
    let mut object = serde_json::Map::new();
    object.insert("_op".to_owned(), Value::String(operation.to_owned()));
    object.insert("_table".to_owned(), Value::String(table.to_owned()));
    object.insert("_timestamp".to_owned(), Value::Number(timestamp.into()));
    for (index, cell) in row.cells.iter().enumerate() {
        let name = column_names
            .get(index)
            .map_or_else(|| format!("col_{index}"), |name| (*name).to_owned());
        let value = cell.as_ref().map_or(Value::Null, mysql_value_to_json);
        object.insert(name, value);
    }
    Ok(u64::try_from(
        serde_json::to_vec(&Value::Object(object))?.len(),
    )?)
}

fn mysql_value_to_json(value: &MySqlValue) -> Value {
    match value {
        MySqlValue::TinyInt(number) => Value::Number((*number).into()),
        MySqlValue::SmallInt(number) => Value::Number((*number).into()),
        MySqlValue::MediumInt(number) => Value::Number((*number).into()),
        MySqlValue::Int(number) => Value::Number((*number).into()),
        MySqlValue::BigInt(number) => Value::Number((*number as i64).into()),
        MySqlValue::Float(number) => {
            serde_json::Number::from_f64(f64::from(*number)).map_or(Value::Null, Value::Number)
        }
        MySqlValue::Double(number) => {
            serde_json::Number::from_f64(*number).map_or(Value::Null, Value::Number)
        }
        MySqlValue::Decimal(text) | MySqlValue::String(text) => Value::String(text.clone()),
        MySqlValue::Blob(bytes) => Value::String(String::from_utf8_lossy(bytes).into_owned()),
        MySqlValue::Year(year) => Value::Number((*year).into()),
        MySqlValue::Date(date) => {
            Value::String(format!("{}-{:02}-{:02}", date.year, date.month, date.day))
        }
        MySqlValue::Time(time) => Value::String(format!(
            "{}:{:02}:{:02}.{:03}",
            time.hour, time.minute, time.second, time.millis
        )),
        MySqlValue::DateTime(date_time) => Value::String(format!(
            "{}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}",
            date_time.year,
            date_time.month,
            date_time.day,
            date_time.hour,
            date_time.minute,
            date_time.second,
            date_time.millis
        )),
        MySqlValue::Timestamp(millis) => Value::Number((*millis as i64).into()),
        MySqlValue::Bit(bits) => {
            let number = bits
                .iter()
                .rev()
                .enumerate()
                .fold(
                    0_u64,
                    |value, (index, bit)| {
                        if *bit {
                            value | (1 << index)
                        } else {
                            value
                        }
                    },
                );
            Value::Number(number.into())
        }
        MySqlValue::Enum(number) => Value::Number(u64::from(*number).into()),
        MySqlValue::Set(number) => Value::Number((*number).into()),
    }
}
