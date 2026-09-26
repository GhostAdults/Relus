#![allow(clippy::expect_used)]

use relus_mysql_cdc_candidate::{
    round_trip_value, validate_mysql_schema, CdcValue, MySqlColumn, MySqlColumnType,
    ReplicationErrorKind, SchemaRegistry,
};

#[test]
fn common_mysql_type_matrix_round_trips_without_precision_loss() {
    let values = [
        CdcValue::Null,
        CdcValue::Bool(true),
        CdcValue::Signed(i64::MIN),
        CdcValue::Unsigned(u64::MAX),
        CdcValue::Float(std::f64::consts::PI),
        CdcValue::String(
            "多字节 utf8 / decimal 12345678901234567890.12345678901234567890".to_owned(),
        ),
        CdcValue::Bytes(vec![0, 1, 127, 128, 255]),
        CdcValue::Date {
            year: 0,
            month: 0,
            day: 0,
        },
        CdcValue::DateTime {
            year: 2026,
            month: 9,
            day: 26,
            hour: 23,
            minute: 59,
            second: 58,
            micros: 999_999,
        },
        CdcValue::Time {
            negative: true,
            days: 34,
            hours: 23,
            minutes: 59,
            seconds: 59,
            micros: 999_999,
        },
    ];

    for value in values {
        assert_eq!(
            round_trip_value(&value).expect("value should round trip"),
            value
        );
    }
}

#[test]
fn spatial_types_fail_schema_validation_and_schema_drift_fails_closed() {
    let spatial = [
        MySqlColumnType::Geometry,
        MySqlColumnType::Point,
        MySqlColumnType::LineString,
        MySqlColumnType::Polygon,
        MySqlColumnType::MultiPoint,
        MySqlColumnType::MultiLineString,
        MySqlColumnType::MultiPolygon,
        MySqlColumnType::GeometryCollection,
    ];
    for column_type in spatial {
        let error = validate_mysql_schema(&[MySqlColumn {
            name: "shape".to_owned(),
            column_type,
        }])
        .expect_err("spatial types are unsupported");
        assert_eq!(error.kind, ReplicationErrorKind::Configuration);
    }
    let unknown = validate_mysql_schema(&[MySqlColumn {
        name: "future_type".to_owned(),
        column_type: MySqlColumnType::Unknown("future".to_owned()),
    }])
    .expect_err("unknown types must fail closed");
    assert_eq!(unknown.kind, ReplicationErrorKind::Configuration);

    let mut registry = SchemaRegistry::default();
    registry
        .observe_table(
            "orders",
            vec![MySqlColumn {
                name: "id".to_owned(),
                column_type: MySqlColumnType::BigInt,
            }],
        )
        .expect("initial schema should be accepted");
    let error = registry
        .observe_table(
            "orders",
            vec![
                MySqlColumn {
                    name: "id".to_owned(),
                    column_type: MySqlColumnType::BigInt,
                },
                MySqlColumn {
                    name: "status".to_owned(),
                    column_type: MySqlColumnType::VarChar,
                },
            ],
        )
        .expect_err("schema drift must stop the stream");
    assert_eq!(error.kind, ReplicationErrorKind::SchemaDrift);
}
