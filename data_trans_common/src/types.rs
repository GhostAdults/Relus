use chrono::NaiveDateTime;

pub type OptionalString = Option<String>;

#[derive(Debug, Clone)]
pub enum TypedVal {
    I64(i64),
    F64(f64),
    Bool(bool),
    OptI64(Option<i64>),
    OptF64(Option<f64>),
    OptBool(Option<bool>),
    OptNaiveTs(Option<NaiveDateTime>),
    Text(String),
}

#[derive(Debug, Clone)]
pub enum DataSourceType {
    Api,
    Database {
        query: String,
        limit: Option<usize>,
        offset: Option<usize>,
    },
}