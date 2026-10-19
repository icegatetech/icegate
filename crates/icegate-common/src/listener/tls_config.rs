//! Operator-facing configuration of server-side TLS.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::error::{CommonError, Result};

/// Default for [`TlsConfig::reload_interval_secs`].
///
/// The interval is the longest a renewed certificate waits in this process once both
/// files hold it, so a shorter interval shortens every renewal. The price of a shorter
/// one is reading two small files per interval, with nothing parsed while their bytes
/// are unchanged. The default balances the two: a renewal is picked up promptly, and
/// the reads stay rare enough that no one needs to tune them.
pub const DEFAULT_TLS_RELOAD_INTERVAL_SECS: u64 = 30;

/// Upper bound for [`TlsConfig::reload_interval_secs`].
///
/// The interval is how long a renewed pair can wait, once both files hold it, before it
/// is served, and the pair in service keeps running toward its expiry meanwhile. The bound
/// keeps that wait well inside the lead time a renewal is normally issued with: past it,
/// the pair in service could lapse before the new one is read, and nothing is gained,
/// because a re-read is only two small file reads. It also keeps the interval far from
/// anything the clock cannot represent.
pub const MAX_TLS_RELOAD_INTERVAL_SECS: u64 = 86_400;

const fn default_reload_interval_secs() -> u64 {
    DEFAULT_TLS_RELOAD_INTERVAL_SECS
}

/// Server-side TLS for one listener: the certificate chain and private key to present.
///
/// The presence of this section is what turns TLS on; there is no separate switch.
/// The server never requests a client certificate.
///
/// # Metrics
///
/// The certificate in service is reported on the metrics endpoint of the operational
/// listener, when the component enables it, under these scraped series:
///
/// * `icegate_tls_certificate_not_after_seconds`, a gauge: the expiry of the leaf
///   certificate in service, in seconds since the Unix epoch. Alert on it to catch a
///   renewal that never arrived.
/// * `icegate_tls_certificate_reloads_total`, a counter labelled by `outcome`: the reloads
///   that replaced the pair or were rejected.
///
/// The exporter derives these from the instrument names, so they differ from what the code
/// registers. The `tls_scraped_metrics` integration test of this crate scrapes them through
/// the exporter the services use and pins both names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TlsConfig {
    /// PEM file holding the certificate chain, leaf first.
    pub certificate_path: PathBuf,
    /// PEM file holding the private key of the leaf certificate.
    pub private_key_path: PathBuf,
    /// How often, in seconds, both files are re-read. Must be greater than zero and at most
    /// [`MAX_TLS_RELOAD_INTERVAL_SECS`].
    ///
    /// A renewed pair is served within one interval after both files hold it,
    /// whatever writes them (for example the kubelet refreshing a mounted Secret).
    #[serde(default = "default_reload_interval_secs")]
    pub reload_interval_secs: u64,
}

impl TlsConfig {
    /// Check the settings that can be judged without touching the files.
    ///
    /// # Errors
    /// [`CommonError::Config`] when a path is empty or `reload_interval_secs` is zero or
    /// exceeds [`MAX_TLS_RELOAD_INTERVAL_SECS`].
    /// Whether the files exist and hold a matching pair is judged when they are loaded.
    pub fn validate(&self) -> Result<()> {
        if self.certificate_path.as_os_str().is_empty() {
            return Err(CommonError::Config("tls certificate_path must not be empty".into()));
        }
        if self.private_key_path.as_os_str().is_empty() {
            return Err(CommonError::Config("tls private_key_path must not be empty".into()));
        }
        if self.reload_interval_secs == 0 {
            return Err(CommonError::Config("tls reload_interval_secs must be > 0".into()));
        }
        if self.reload_interval_secs > MAX_TLS_RELOAD_INTERVAL_SECS {
            return Err(CommonError::Config(format!(
                "tls reload_interval_secs must be <= {MAX_TLS_RELOAD_INTERVAL_SECS}"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_omitted_reload_interval_takes_the_default_and_validates() {
        let config: TlsConfig =
            serde_yaml::from_str("certificate_path: /tls/tls.crt\nprivate_key_path: /tls/tls.key\n")
                .expect("a config without a reload interval deserializes");

        assert_eq!(config.reload_interval_secs, DEFAULT_TLS_RELOAD_INTERVAL_SECS);
        config.validate().expect("the default interval is accepted");
    }

    #[test]
    fn a_configured_reload_interval_overrides_the_default() {
        let config: TlsConfig = serde_yaml::from_str(
            "certificate_path: /tls/tls.crt\nprivate_key_path: /tls/tls.key\nreload_interval_secs: 7\n",
        )
        .expect("a config with a reload interval deserializes");

        assert_eq!(config.reload_interval_secs, 7);
    }

    #[test]
    fn validate_accepts_the_longest_reload_interval_and_rejects_anything_longer() {
        let with_interval = |reload_interval_secs| TlsConfig {
            certificate_path: PathBuf::from("/tls/tls.crt"),
            private_key_path: PathBuf::from("/tls/tls.key"),
            reload_interval_secs,
        };

        with_interval(MAX_TLS_RELOAD_INTERVAL_SECS)
            .validate()
            .expect("the bound itself is accepted");

        for reload_interval_secs in [MAX_TLS_RELOAD_INTERVAL_SECS + 1, u64::MAX] {
            let result = with_interval(reload_interval_secs).validate();

            assert!(
                matches!(result, Err(CommonError::Config(_))),
                "{reload_interval_secs}: got {result:?}"
            );
        }
    }

    #[test]
    fn validate_rejects_an_empty_private_key_path() {
        let config = TlsConfig {
            certificate_path: PathBuf::from("/tls/tls.crt"),
            private_key_path: PathBuf::new(),
            reload_interval_secs: DEFAULT_TLS_RELOAD_INTERVAL_SECS,
        };

        let result = config.validate();

        assert!(matches!(result, Err(CommonError::Config(_))), "got {result:?}");
    }
}
