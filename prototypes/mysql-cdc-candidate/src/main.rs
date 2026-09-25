use relus_mysql_cdc_candidate::{
    CdcOffset, MySqlAsyncReplicationClient, MySqlConnectionConfig, MySqlReplicationClient, TlsMode,
    TransactionAssembler,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Some(config) = config_from_environment()? else {
        eprintln!("set RELUS_TEST_MYSQL80_* variables to run the prototype");
        return Ok(());
    };

    let mut client = MySqlAsyncReplicationClient::new(config);
    let mut assembler = TransactionAssembler::default();
    client.connect().await?;
    while let Some(event) = client.next_event().await? {
        for message in assembler.push(event)? {
            println!("{}", serde_json::to_string(&message)?);
        }
    }
    client.close().await?;
    Ok(())
}

fn config_from_environment() -> Result<Option<MySqlConnectionConfig>, Box<dyn std::error::Error>> {
    let Ok(host) = std::env::var("RELUS_TEST_MYSQL80_HOST") else {
        return Ok(None);
    };
    let port = std::env::var("RELUS_TEST_MYSQL80_PORT")?.parse()?;
    let position = std::env::var("RELUS_TEST_MYSQL80_BINLOG_POSITION")?.parse()?;
    Ok(Some(MySqlConnectionConfig {
        host,
        port,
        username: std::env::var("RELUS_TEST_MYSQL80_USERNAME")?,
        password: std::env::var("RELUS_TEST_MYSQL80_PASSWORD")?,
        database: std::env::var("RELUS_TEST_MYSQL80_DATABASE")?,
        server_id: std::env::var("RELUS_TEST_MYSQL80_SERVER_ID")?.parse()?,
        start_offset: CdcOffset::Position {
            file: std::env::var("RELUS_TEST_MYSQL80_BINLOG_FILE")?,
            position,
        },
        tls: TlsMode::Disabled,
    }))
}
