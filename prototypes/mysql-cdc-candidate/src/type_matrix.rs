use crate::{CdcValue, ReplicationError, ReplicationErrorKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommonMySqlType {
    Integer,
    Decimal,
    FloatingPoint,
    Text,
    Binary,
    DateTime,
    Time,
    Json,
    EnumSet,
    Bit,
    Null,
}

pub fn round_trip_value(value: &CdcValue) -> Result<CdcValue, ReplicationError> {
    let encoded = serde_json::to_vec(value).map_err(|error| {
        ReplicationError::new(ReplicationErrorKind::Decode, false, error.to_string())
    })?;
    serde_json::from_slice(&encoded).map_err(|error| {
        ReplicationError::new(ReplicationErrorKind::Decode, false, error.to_string())
    })
}
