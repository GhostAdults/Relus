use std::{fmt, path::PathBuf};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, PartialEq, Eq)]
pub enum SecretValue {
    Direct(String),
    Environment(String),
    File(PathBuf),
}

#[derive(Deserialize)]
#[serde(untagged)]
enum SecretInput {
    Direct(String),
    Reference(SecretReference),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SecretReference {
    value: Option<String>,
    env: Option<String>,
    file: Option<PathBuf>,
}

impl<'de> Deserialize<'de> for SecretValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let input = SecretInput::deserialize(deserializer)?;
        match input {
            SecretInput::Direct(value) => Ok(Self::Direct(value)),
            SecretInput::Reference(reference) => {
                let configured = usize::from(reference.value.is_some())
                    + usize::from(reference.env.is_some())
                    + usize::from(reference.file.is_some());
                if configured != 1 {
                    return Err(serde::de::Error::custom(
                        "secret must configure exactly one of value, env, or file",
                    ));
                }
                if let Some(value) = reference.value {
                    Ok(Self::Direct(value))
                } else if let Some(variable) = reference.env {
                    Ok(Self::Environment(variable))
                } else if let Some(path) = reference.file {
                    Ok(Self::File(path))
                } else {
                    Err(serde::de::Error::custom("secret reference is empty"))
                }
            }
        }
    }
}

impl Serialize for SecretValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str("<redacted>")
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

impl SecretValue {
    pub fn resolve(&self) -> Result<String> {
        match self {
            Self::Direct(value) => Ok(value.clone()),
            Self::Environment(variable) => std::env::var(variable).with_context(|| {
                format!("secret environment variable `{variable}` is unavailable")
            }),
            Self::File(path) => {
                let value = std::fs::read_to_string(path).with_context(|| {
                    format!("secret file `{}` could not be read", path.display())
                })?;
                Ok(value.trim_end_matches(['\r', '\n']).to_owned())
            }
        }
    }
}

#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum TlsConfig {
    Disabled,
    Encrypted,
    Required {
        #[serde(default)]
        ca_path: Option<PathBuf>,
        #[serde(default)]
        server_name: Option<String>,
    },
    Mutual {
        #[serde(default)]
        ca_path: Option<PathBuf>,
        #[serde(default)]
        server_name: Option<String>,
        client_cert_path: PathBuf,
        client_key_path: PathBuf,
    },
}

impl fmt::Debug for TlsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disabled => formatter.write_str("Disabled"),
            Self::Encrypted => formatter.write_str("Encrypted(<identity-unverified>)"),
            Self::Required { server_name, .. } => formatter
                .debug_struct("Required")
                .field("ca_path", &"<optional-redacted-path>")
                .field("server_name", server_name)
                .finish(),
            Self::Mutual { server_name, .. } => formatter
                .debug_struct("Mutual")
                .field("ca_path", &"<optional-redacted-path>")
                .field("server_name", server_name)
                .field("client_cert_path", &"<redacted-path>")
                .field("client_key_path", &"<redacted-path>")
                .finish(),
        }
    }
}

impl TlsConfig {
    pub fn validate(&self, connection_host: &str) -> Result<()> {
        match self {
            Self::Disabled | Self::Encrypted => Ok(()),
            Self::Required {
                ca_path,
                server_name,
            } => validate_tls_fields(
                connection_host,
                ca_path.as_ref(),
                server_name.as_deref(),
                None,
                None,
            ),
            Self::Mutual {
                ca_path,
                server_name,
                client_cert_path,
                client_key_path,
            } => validate_tls_fields(
                connection_host,
                ca_path.as_ref(),
                server_name.as_deref(),
                Some(client_cert_path),
                Some(client_key_path),
            ),
        }
    }

    pub fn server_name<'a>(&'a self, connection_host: &'a str) -> Option<&'a str> {
        match self {
            Self::Required { server_name, .. } | Self::Mutual { server_name, .. } => {
                Some(server_name.as_deref().unwrap_or(connection_host))
            }
            Self::Disabled | Self::Encrypted => None,
        }
    }
}

fn validate_tls_fields(
    connection_host: &str,
    ca_path: Option<&PathBuf>,
    server_name: Option<&str>,
    client_cert_path: Option<&PathBuf>,
    client_key_path: Option<&PathBuf>,
) -> Result<()> {
    if connection_host.trim().is_empty() {
        bail!("TLS connection host must not be empty");
    }
    if server_name.is_some_and(|name| name.trim().is_empty()) {
        bail!("TLS server_name must not be empty when supplied");
    }
    for (label, path) in [
        ("CA", ca_path),
        ("client certificate", client_cert_path),
        ("client key", client_key_path),
    ] {
        if path.is_some_and(|value| value.as_os_str().is_empty()) {
            bail!("TLS {label} path must not be empty");
        }
    }
    Ok(())
}
