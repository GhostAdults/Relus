use std::{collections::BTreeMap, path::PathBuf, time::Duration};

use serde::Serialize;
use tokio_util::sync::CancellationToken;

use crate::{
    adapter::{consume_until_cancelled, verify_authentication_plugin, verify_connection},
    CdcOffset, MySqlAsyncReplicationClient, MySqlConnectionConfig, ReplicationErrorKind, TlsMode,
};

const MYSQL_VERSIONS: [&str; 4] = ["56", "57", "80", "84"];
const PROFILE_CASE_SUFFIXES: [&str; 10] = [
    "plaintext_policy",
    "tls_encrypted",
    "tls_required",
    "tls_wrong_server_identity",
    "tls_wrong_ca",
    "mutual_tls",
    "auth.mysql_native_password",
    "auth.caching_sha2_password",
    "cancellation",
    "gtid",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationStatus {
    Passed,
    Failed,
    NotRun,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VerificationCase {
    pub name: String,
    pub status: VerificationStatus,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VerificationReport {
    pub candidate: &'static str,
    pub cases: Vec<VerificationCase>,
}

impl VerificationReport {
    pub fn has_failures(&self) -> bool {
        self.cases
            .iter()
            .any(|case| case.status == VerificationStatus::Failed)
    }
}

#[derive(Clone)]
struct RuntimeProfile {
    version: &'static str,
    host: String,
    port: u16,
    username: String,
    password: String,
    database: String,
}

#[derive(Default)]
struct CertificatePaths {
    ca: Option<PathBuf>,
    client_cert: Option<PathBuf>,
    client_key: Option<PathBuf>,
    server_name: Option<String>,
}

pub async fn verify_available_runtime() -> VerificationReport {
    let environment = std::env::vars().collect::<BTreeMap<_, _>>();
    verify_runtime_with_environment(&environment).await
}

async fn verify_runtime_with_environment(
    environment: &BTreeMap<String, String>,
) -> VerificationReport {
    let mut cases = Vec::new();

    for version in MYSQL_VERSIONS {
        match RuntimeProfile::from_environment(version, environment) {
            Ok(Some(profile)) => {
                let certificates = CertificatePaths::from_environment(version, environment);
                verify_profile(&profile, &certificates, environment, &mut cases).await;
            }
            Ok(None) => append_unavailable_profile(version, &mut cases),
            Err(detail) => append_failed_profile(version, detail, &mut cases),
        }
    }

    VerificationReport {
        candidate: "mysql_async 0.37.1",
        cases,
    }
}

impl RuntimeProfile {
    fn from_environment(
        version: &'static str,
        environment: &BTreeMap<String, String>,
    ) -> Result<Option<Self>, String> {
        let prefix = format!("RELUS_TEST_MYSQL{version}_");
        let names = ["HOST", "PORT", "USERNAME", "PASSWORD", "DATABASE"];
        let values = names
            .iter()
            .map(|name| environment.get(&format!("{prefix}{name}")))
            .collect::<Vec<_>>();
        if values.iter().all(|value| value.is_none()) {
            return Ok(None);
        }
        if values.iter().any(|value| value.is_none()) {
            return Err("the versioned MySQL environment is incomplete".to_owned());
        }
        let [Some(host), Some(port), Some(username), Some(password), Some(database)] =
            values.as_slice()
        else {
            return Err("the versioned MySQL environment is incomplete".to_owned());
        };
        let port = port
            .parse::<u16>()
            .map_err(|_| "the versioned MySQL port is invalid".to_owned())?;
        Ok(Some(Self {
            version,
            host: (*host).clone(),
            port,
            username: (*username).clone(),
            password: (*password).clone(),
            database: (*database).clone(),
        }))
    }

    fn connection_config(
        &self,
        username: String,
        password: String,
        tls: TlsMode,
    ) -> MySqlConnectionConfig {
        MySqlConnectionConfig {
            host: self.host.clone(),
            port: self.port,
            username,
            password,
            database: self.database.clone(),
            server_id: 1,
            start_offset: CdcOffset::Position {
                file: String::new(),
                position: 4,
            },
            tls,
            protocol_compression: false,
        }
    }

    fn default_config(&self, tls: TlsMode) -> MySqlConnectionConfig {
        self.connection_config(self.username.clone(), self.password.clone(), tls)
    }

    fn capability_tls(&self) -> Option<TlsMode> {
        if self.version == "56" {
            Some(TlsMode::Disabled)
        } else {
            Some(TlsMode::Encrypted)
        }
    }
}

impl CertificatePaths {
    fn from_environment(version: &str, environment: &BTreeMap<String, String>) -> Self {
        let prefix = format!("RELUS_TEST_MYSQL{version}_");
        Self {
            ca: environment
                .get(&format!("{prefix}CA_PATH"))
                .map(PathBuf::from),
            client_cert: environment
                .get(&format!("{prefix}CLIENT_CERT_PATH"))
                .map(PathBuf::from),
            client_key: environment
                .get(&format!("{prefix}CLIENT_KEY_PATH"))
                .map(PathBuf::from),
            server_name: environment.get(&format!("{prefix}SERVER_NAME")).cloned(),
        }
    }

    fn required_tls(&self) -> Option<TlsMode> {
        Some(TlsMode::Required {
            ca_path: Some(self.ca.clone()?),
            server_name: self.server_name.clone()?,
        })
    }

    fn mutual_tls(&self) -> Option<TlsMode> {
        Some(TlsMode::Mutual {
            ca_path: Some(self.ca.clone()?),
            server_name: self.server_name.clone()?,
            client_cert_path: self.client_cert.clone()?,
            client_key_path: self.client_key.clone()?,
        })
    }
}

async fn verify_profile(
    profile: &RuntimeProfile,
    certificates: &CertificatePaths,
    environment: &BTreeMap<String, String>,
    cases: &mut Vec<VerificationCase>,
) {
    cases.push(plaintext_policy_case(
        format!("mysql{}.plaintext_policy", profile.version),
        verify_connection(&profile.default_config(TlsMode::Disabled)).await,
    ));

    cases.push(connection_case(
        format!("mysql{}.tls_encrypted", profile.version),
        verify_connection(&profile.default_config(TlsMode::Encrypted)).await,
        "the connection was encrypted without server identity verification",
    ));

    if let Some(required_tls) = certificates.required_tls() {
        cases.push(connection_case(
            format!("mysql{}.tls_required", profile.version),
            verify_connection(&profile.default_config(required_tls.clone())).await,
            "custom CA and server identity were verified",
        ));

        let server_name = certificates
            .server_name
            .as_deref()
            .unwrap_or("mysql.invalid");
        let wrong_name = required_tls
            .with_server_name(format!("invalid.{server_name}"))
            .unwrap_or(TlsMode::Disabled);
        cases.push(expected_tls_failure_case(
            format!("mysql{}.tls_wrong_server_identity", profile.version),
            verify_connection(&profile.default_config(wrong_name)).await,
        ));

        if let (Some(client_cert), Some(server_name)) =
            (&certificates.client_cert, &certificates.server_name)
        {
            let wrong_ca = TlsMode::Required {
                ca_path: Some(client_cert.clone()),
                server_name: server_name.clone(),
            };
            cases.push(expected_tls_failure_case(
                format!("mysql{}.tls_wrong_ca", profile.version),
                verify_connection(&profile.default_config(wrong_ca)).await,
            ));
        } else {
            cases.push(not_run(
                format!("mysql{}.tls_wrong_ca", profile.version),
                "a distinct approved certificate path was not supplied",
            ));
        }
    } else {
        for suffix in ["tls_required", "tls_wrong_server_identity", "tls_wrong_ca"] {
            cases.push(not_run(
                format!("mysql{}.{suffix}", profile.version),
                "the custom CA path and TLS server name are unavailable",
            ));
        }
    }

    if let Some(mutual_tls) = certificates.mutual_tls() {
        cases.push(connection_case(
            format!("mysql{}.mutual_tls", profile.version),
            verify_connection(&profile.default_config(mutual_tls)).await,
            "mutual TLS authenticated with the supplied identity paths",
        ));
    } else {
        cases.push(not_run(
            format!("mysql{}.mutual_tls", profile.version),
            "the custom CA, server name, client certificate, and client key paths are incomplete",
        ));
    }

    for (suffix, label) in [
        ("NATIVE", "mysql_native_password"),
        ("CACHING_SHA2", "caching_sha2_password"),
    ] {
        let prefix = format!("RELUS_TEST_MYSQL{}_{}", profile.version, suffix);
        match (
            environment.get(&format!("{prefix}_USERNAME")),
            environment.get(&format!("{prefix}_PASSWORD")),
        ) {
            (Some(username), Some(password)) => {
                let Some(capability_tls) = profile.capability_tls() else {
                    cases.push(not_run(
                        format!("mysql{}.auth.{label}", profile.version),
                        "a verified transport for authentication is unavailable",
                    ));
                    continue;
                };
                let config =
                    profile.connection_config(username.clone(), password.clone(), capability_tls);
                cases.push(connection_case(
                    format!("mysql{}.auth.{label}", profile.version),
                    verify_authentication_plugin(
                        &config,
                        label,
                    )
                    .await,
                    "the supplied account connected and its server definition confirmed the expected authentication plugin",
                ));
            }
            (None, None) => cases.push(not_run(
                format!("mysql{}.auth.{label}", profile.version),
                "the authentication-specific account was not supplied",
            )),
            _ => cases.push(failed(
                format!("mysql{}.auth.{label}", profile.version),
                "the authentication-specific account environment is incomplete",
            )),
        }
    }

    cases.push(verify_cancellation(profile, environment).await);
    cases.push(verify_gtid(profile, environment).await);
}

async fn verify_cancellation(
    profile: &RuntimeProfile,
    environment: &BTreeMap<String, String>,
) -> VerificationCase {
    let prefix = format!("RELUS_TEST_MYSQL{}", profile.version);
    let Some(file) = environment.get(&format!("{prefix}_BINLOG_FILE")) else {
        return not_run(
            format!("mysql{}.cancellation", profile.version),
            "replication offset and server ID are unavailable",
        );
    };
    let Some(position) = environment
        .get(&format!("{prefix}_BINLOG_POSITION"))
        .and_then(|value| value.parse::<u64>().ok())
    else {
        return not_run(
            format!("mysql{}.cancellation", profile.version),
            "replication offset and server ID are unavailable",
        );
    };
    let Some(server_id) = environment
        .get(&format!("{prefix}_SERVER_ID"))
        .and_then(|value| value.parse::<u32>().ok())
    else {
        return not_run(
            format!("mysql{}.cancellation", profile.version),
            "replication offset and server ID are unavailable",
        );
    };

    let Some(capability_tls) = profile.capability_tls() else {
        return not_run(
            format!("mysql{}.cancellation", profile.version),
            "a verified transport for replication is unavailable",
        );
    };
    let mut config = profile.default_config(capability_tls);
    config.server_id = server_id;
    config.start_offset = CdcOffset::Position {
        file: file.clone(),
        position,
    };
    let mut client = MySqlAsyncReplicationClient::new(config);
    if client.connect().await.is_err() {
        return failed(
            format!("mysql{}.cancellation", profile.version),
            "the replication connection could not be established",
        );
    }

    let cancellation = CancellationToken::new();
    let cancel_handle = cancellation.clone();
    let cancel_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        cancel_handle.cancel();
    });
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        consume_until_cancelled(&mut client, &cancellation, Duration::from_secs(2)),
    )
    .await;
    let _ = cancel_task.await;

    match result {
        Ok(Ok(())) => passed(
            format!("mysql{}.cancellation", profile.version),
            "the pending replication read was cancelled and closed within the bounded timeout",
        ),
        Ok(Err(_)) | Err(_) => failed(
            format!("mysql{}.cancellation", profile.version),
            "the replication connection did not close cleanly within the bounded timeout",
        ),
    }
}

async fn verify_gtid(
    profile: &RuntimeProfile,
    environment: &BTreeMap<String, String>,
) -> VerificationCase {
    let prefix = format!("RELUS_TEST_MYSQL{}_", profile.version);
    let Some(resume_set) = environment.get(&format!("{prefix}GTID_SET")) else {
        return not_run(
            format!("mysql{}.gtid", profile.version),
            "the GTID resume set and server ID are unavailable",
        );
    };
    let Some(server_id) = environment
        .get(&format!("{prefix}SERVER_ID"))
        .and_then(|value| value.parse::<u32>().ok())
    else {
        return not_run(
            format!("mysql{}.gtid", profile.version),
            "the GTID resume set and server ID are unavailable",
        );
    };

    let mut config = profile.default_config(profile.capability_tls().unwrap_or(TlsMode::Disabled));
    config.server_id = server_id;
    config.start_offset = CdcOffset::Gtid {
        resume_set: resume_set.clone(),
        transaction_gtid: None,
        file: String::new(),
        position: 4,
    };
    let mut client = MySqlAsyncReplicationClient::new(config);
    match client.connect().await {
        Ok(()) => match client.close().await {
            Ok(()) => passed(
                format!("mysql{}.gtid", profile.version),
                "the GTID replication request connected and closed successfully",
            ),
            Err(_) => failed(
                format!("mysql{}.gtid", profile.version),
                "the GTID replication connection did not close cleanly",
            ),
        },
        Err(error) => case(
            format!("mysql{}.gtid", profile.version),
            VerificationStatus::Failed,
            &format!(
                "the GTID replication connection failed with {:?}",
                error.kind
            ),
        ),
    }
}

fn connection_case(
    name: String,
    result: Result<(), crate::ReplicationError>,
    success_detail: &'static str,
) -> VerificationCase {
    match result {
        Ok(()) => passed(name, success_detail),
        Err(error) => case(
            name,
            VerificationStatus::Failed,
            &format!("the connection attempt failed with {:?}", error.kind),
        ),
    }
}

fn plaintext_policy_case(
    name: String,
    result: Result<(), crate::ReplicationError>,
) -> VerificationCase {
    match result {
        Ok(()) => passed(name, "plaintext connection authenticated"),
        Err(error) if error.kind == ReplicationErrorKind::Tls => passed(
            name,
            "the server required secure transport and plaintext was not used as a fallback",
        ),
        Err(error) => case(
            name,
            VerificationStatus::Failed,
            &format!("the plaintext policy check failed with {:?}", error.kind),
        ),
    }
}

fn expected_tls_failure_case(
    name: String,
    result: Result<(), crate::ReplicationError>,
) -> VerificationCase {
    match result {
        Err(error) if error.kind == ReplicationErrorKind::Tls => passed(
            name,
            "the invalid TLS identity was rejected without plaintext fallback",
        ),
        Ok(()) => failed(name, "the invalid TLS identity was accepted"),
        Err(_) => failed(
            name,
            "the negative check failed for a reason other than TLS verification",
        ),
    }
}

fn append_unavailable_profile(version: &str, cases: &mut Vec<VerificationCase>) {
    append_profile_cases(
        version,
        cases,
        VerificationStatus::NotRun,
        "the versioned MySQL environment is unavailable",
    );
}

fn append_failed_profile(version: &str, detail: String, cases: &mut Vec<VerificationCase>) {
    append_profile_cases(version, cases, VerificationStatus::Failed, &detail);
}

fn append_profile_cases(
    version: &str,
    cases: &mut Vec<VerificationCase>,
    status: VerificationStatus,
    detail: &str,
) {
    for suffix in PROFILE_CASE_SUFFIXES {
        cases.push(case(format!("mysql{version}.{suffix}"), status, detail));
    }
}

fn passed(name: String, detail: &str) -> VerificationCase {
    case(name, VerificationStatus::Passed, detail)
}

fn failed(name: String, detail: &str) -> VerificationCase {
    case(name, VerificationStatus::Failed, detail)
}

fn not_run(name: String, detail: &str) -> VerificationCase {
    case(name, VerificationStatus::NotRun, detail)
}

fn case(name: String, status: VerificationStatus, detail: &str) -> VerificationCase {
    VerificationCase {
        name,
        status,
        detail: detail.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn absent_environments_are_reported_as_not_run() {
        let report = verify_runtime_with_environment(&BTreeMap::new()).await;

        assert_eq!(report.cases.len(), 40);
        assert!(report
            .cases
            .iter()
            .all(|case| case.status == VerificationStatus::NotRun));
    }

    #[tokio::test]
    async fn incomplete_environment_is_a_failure_not_a_skip() {
        let environment =
            BTreeMap::from([("RELUS_TEST_MYSQL80_HOST".to_owned(), "localhost".to_owned())]);

        let report = verify_runtime_with_environment(&environment).await;
        let mysql80 = report
            .cases
            .iter()
            .filter(|case| case.name.starts_with("mysql80."))
            .collect::<Vec<_>>();

        assert_eq!(mysql80.len(), 10);
        assert!(mysql80
            .iter()
            .all(|case| case.status == VerificationStatus::Failed));
    }

    #[test]
    fn certificate_paths_are_scoped_to_one_version() {
        let environment = BTreeMap::from([
            (
                "RELUS_TEST_MYSQL56_CA_PATH".to_owned(),
                "mysql56-ca.pem".to_owned(),
            ),
            (
                "RELUS_TEST_MYSQL56_SERVER_NAME".to_owned(),
                "mysql56.local".to_owned(),
            ),
            (
                "RELUS_TEST_MYSQL_CA_PATH".to_owned(),
                "legacy-global-ca.pem".to_owned(),
            ),
        ]);

        let mysql56 = CertificatePaths::from_environment("56", &environment);
        let mysql80 = CertificatePaths::from_environment("80", &environment);

        assert_eq!(mysql56.ca, Some(PathBuf::from("mysql56-ca.pem")));
        assert_eq!(mysql56.server_name.as_deref(), Some("mysql56.local"));
        assert_eq!(mysql80.ca, None);
        assert_eq!(mysql80.server_name, None);
    }

    #[test]
    fn plaintext_policy_accepts_an_explicit_secure_transport_rejection() {
        let case = plaintext_policy_case(
            "mysql80.plaintext_policy".to_owned(),
            Err(crate::ReplicationError::new(
                ReplicationErrorKind::Tls,
                false,
                "secure transport required",
            )),
        );

        assert_eq!(case.status, VerificationStatus::Passed);
    }
}
