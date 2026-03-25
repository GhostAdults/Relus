use anyhow::{bail, Result};
use chrono::NaiveDateTime;
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, HashMap};

use crate::rdbms_reader_util::util::client_tool::extract_by_path;
pub use crate::{JsonRowMapper, RowMapper};
use data_trans_common::pipeline::{MappedRow, PipelineMessage};
use data_trans_common::types::TypedVal;

pub struct PipelineRowMapper {
    column_mapping: BTreeMap<String, String>,
    column_types: Option<BTreeMap<String, String>>,
}

impl PipelineRowMapper {
    pub fn new(
        column_mapping: BTreeMap<String, String>,
        column_types: Option<BTreeMap<String, String>>,
    ) -> Self {
        Self {
            column_mapping,
            column_types,
        }
    }
    fn to_typed_value(v: &JsonValue, ty: Option<&str>) -> Result<TypedVal> {
        to_typed_value_origin(v, ty)
    }
}

impl RowMapper<PipelineMessage> for PipelineRowMapper {
    fn map_rows(&self, rows: &[JsonValue]) -> Result<Vec<PipelineMessage>> {
        let mut mapped_rows = Vec::new();

        for item in rows {
            let mut row = HashMap::new();

            for (target_col, source_path) in &self.column_mapping {
                let source_val = extract_by_path(item, source_path).unwrap_or(&JsonValue::Null);

                let type_hint = self
                    .column_types
                    .as_ref()
                    .and_then(|m| m.get(target_col))
                    .map(|s| s.as_str());

                let typed_val = Self::to_typed_value(source_val, type_hint)?;
                row.insert(target_col.clone(), typed_val);
            }

            mapped_rows.push(MappedRow {
                values: row,
                source: item.clone(),
            });
        }

        Ok(vec![PipelineMessage::DataBatch(mapped_rows)])
    }
}

pub fn to_typed_value_origin(v: &JsonValue, ty: Option<&str>) -> Result<TypedVal> {
    match ty.unwrap_or("text") {
        "int" => {
            if v.is_null() {
                return Ok(TypedVal::OptI64(None));
            }
            if let Some(n) = v.as_i64() {
                Ok(TypedVal::I64(n))
            } else if let Some(s) = v.as_str() {
                let n = s.parse::<i64>()?;
                Ok(TypedVal::I64(n))
            } else {
                bail!("无法转换为 int: {}", v)
            }
        }
        "float" => {
            if v.is_null() {
                return Ok(TypedVal::OptF64(None));
            }
            if let Some(n) = v.as_f64() {
                Ok(TypedVal::F64(n))
            } else if let Some(s) = v.as_str() {
                let n = s.parse::<f64>()?;
                Ok(TypedVal::F64(n))
            } else {
                bail!("无法转换为 float: {}", v)
            }
        }
        "bool" => {
            if v.is_null() {
                Ok(TypedVal::OptBool(None))
            } else if let Some(b) = v.as_bool() {
                Ok(TypedVal::Bool(b))
            } else if let Some(s) = v.as_str() {
                let b = match s {
                    "true" | "1" => true,
                    "false" | "0" => false,
                    _ => bail!("无法转换为 bool: {}", s),
                };
                Ok(TypedVal::Bool(b))
            } else {
                bail!("无法转换为 bool: {}", v)
            }
        }
        "json" => {
            let s = serde_json::to_string(v)?;
            Ok(TypedVal::Text(s))
        }
        "timestamp" => {
            if v.is_null() {
                return Ok(TypedVal::OptNaiveTs(None));
            }
            if let Some(s) = v.as_str() {
                if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                    Ok(TypedVal::OptNaiveTs(Some(dt)))
                } else if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
                    Ok(TypedVal::OptNaiveTs(Some(dt)))
                } else {
                    bail!("无法解析时间: {}", s)
                }
            } else {
                bail!("无法转换为 timestamp: {}", v)
            }
        }
        _ => {
            if v.is_null() {
                Ok(TypedVal::Text("".to_string()))
            } else if let Some(s) = v.as_str() {
                Ok(TypedVal::Text(s.to_string()))
            } else {
                Ok(TypedVal::Text(v.to_string()))
            }
        }
    }
}
