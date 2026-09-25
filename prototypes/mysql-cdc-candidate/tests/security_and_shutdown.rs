use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use relus_mysql_cdc_candidate::{
    consume_until_cancelled, CdcOffset, MySqlConnectionConfig, MySqlReplicationClient,
    RawReplicationEvent, ReplicationError, TlsMode,
};
use tokio_util::sync::CancellationToken;

fn config(tls: TlsMode) -> MySqlConnectionConfig {
    MySqlConnectionConfig {
        host: "database.internal".to_owned(),
        port: 3306,
        username: "replication".to_owned(),
        password: "super-secret".to_owned(),
        database: "sales".to_owned(),
        server_id: 42,
        start_offset: CdcOffset::Position {
            file: "mysql-bin.000001".to_owned(),
            position: 4,
        },
        tls,
    }
}

#[test]
fn disabled_tls_needs_no_certificate_paths() {
    assert!(config(TlsMode::Disabled).validate().is_ok());
}

#[test]
fn required_tls_rejects_empty_ca_or_server_identity() {
    let missing_ca = config(TlsMode::Required {
        ca_path: PathBuf::new(),
        server_name: "mysql.internal".to_owned(),
    });
    let missing_server_name = config(TlsMode::Required {
        ca_path: PathBuf::from("custom-ca.pem"),
        server_name: String::new(),
    });

    assert!(missing_ca.validate().is_err());
    assert!(missing_server_name.validate().is_err());
}

#[test]
fn mutual_tls_requires_all_identity_paths() {
    let missing_key = config(TlsMode::Mutual {
        ca_path: PathBuf::from("custom-ca.pem"),
        server_name: "mysql.internal".to_owned(),
        client_cert_path: PathBuf::from("client-cert.pem"),
        client_key_path: PathBuf::new(),
    });

    assert!(missing_key.validate().is_err());
}

#[test]
fn debug_output_redacts_secrets_and_certificate_paths() {
    let config = config(TlsMode::Mutual {
        ca_path: PathBuf::from("sensitive-ca.pem"),
        server_name: "mysql.internal".to_owned(),
        client_cert_path: PathBuf::from("sensitive-client-cert.pem"),
        client_key_path: PathBuf::from("sensitive-client-key.pem"),
    });

    let debug = format!("{config:?}");

    assert!(!debug.contains("super-secret"));
    assert!(!debug.contains("sensitive-ca.pem"));
    assert!(!debug.contains("sensitive-client-cert.pem"));
    assert!(!debug.contains("sensitive-client-key.pem"));
}

struct PendingGuard(Arc<AtomicUsize>);

impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

struct PendingClient {
    active_reads: Arc<AtomicUsize>,
    closed: Arc<AtomicBool>,
}

#[async_trait]
impl MySqlReplicationClient for PendingClient {
    async fn connect(&mut self) -> Result<(), ReplicationError> {
        Ok(())
    }

    async fn next_event(&mut self) -> Result<Option<RawReplicationEvent>, ReplicationError> {
        self.active_reads.fetch_add(1, Ordering::SeqCst);
        let _guard = PendingGuard(Arc::clone(&self.active_reads));
        std::future::pending().await
    }

    async fn close(&mut self) -> Result<(), ReplicationError> {
        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn cancellation_drops_pending_read_and_closes_within_timeout() {
    let active_reads = Arc::new(AtomicUsize::new(0));
    let closed = Arc::new(AtomicBool::new(false));
    let mut client = PendingClient {
        active_reads: Arc::clone(&active_reads),
        closed: Arc::clone(&closed),
    };
    let cancellation = CancellationToken::new();
    let cancel_handle = cancellation.clone();
    let cancel_task = tokio::spawn(async move {
        tokio::task::yield_now().await;
        cancel_handle.cancel();
    });

    let result = tokio::time::timeout(
        Duration::from_millis(250),
        consume_until_cancelled(&mut client, &cancellation, Duration::from_millis(100)),
    )
    .await;
    assert!(cancel_task.await.is_ok(), "cancellation task should finish");

    assert!(result.is_ok(), "cancellation exceeded the outer timeout");
    assert!(matches!(result, Ok(Ok(()))), "shutdown should be clean");
    assert!(closed.load(Ordering::SeqCst));
    assert_eq!(active_reads.load(Ordering::SeqCst), 0);
}
