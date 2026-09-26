use std::collections::BTreeMap;

use crate::{ReplicationError, ReplicationErrorKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MySqlColumnType {
    TinyInt,
    SmallInt,
    MediumInt,
    Int,
    BigInt,
    Decimal,
    Float,
    Double,
    Char,
    VarChar,
    Text,
    Binary,
    VarBinary,
    Blob,
    Date,
    Time,
    DateTime,
    Timestamp,
    Year,
    Json,
    Enum,
    Set,
    Bit,
    Null,
    Geometry,
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
    GeometryCollection,
    Unknown(String),
}

impl MySqlColumnType {
    pub fn is_supported(&self) -> bool {
        matches!(
            self,
            Self::TinyInt
                | Self::SmallInt
                | Self::MediumInt
                | Self::Int
                | Self::BigInt
                | Self::Decimal
                | Self::Float
                | Self::Double
                | Self::Char
                | Self::VarChar
                | Self::Text
                | Self::Binary
                | Self::VarBinary
                | Self::Blob
                | Self::Date
                | Self::Time
                | Self::DateTime
                | Self::Timestamp
                | Self::Year
                | Self::Json
                | Self::Enum
                | Self::Set
                | Self::Bit
                | Self::Null
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MySqlColumn {
    pub name: String,
    pub column_type: MySqlColumnType,
}

pub fn validate_mysql_schema(columns: &[MySqlColumn]) -> Result<(), ReplicationError> {
    for column in columns {
        if !column.column_type.is_supported() {
            return Err(ReplicationError::new(
                ReplicationErrorKind::Configuration,
                false,
                format!("unsupported MySQL type in column {}", column.name),
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Default)]
pub struct SchemaRegistry {
    tables: BTreeMap<String, Vec<MySqlColumn>>,
}

impl SchemaRegistry {
    pub fn observe_table(
        &mut self,
        table: impl Into<String>,
        columns: Vec<MySqlColumn>,
    ) -> Result<(), ReplicationError> {
        validate_mysql_schema(&columns)?;
        let table = table.into();
        if let Some(previous) = self.tables.get(&table) {
            if previous != &columns {
                return Err(ReplicationError::new(
                    ReplicationErrorKind::SchemaDrift,
                    false,
                    format!("schema changed for table {table}"),
                ));
            }
        } else {
            self.tables.insert(table, columns);
        }
        Ok(())
    }
}
