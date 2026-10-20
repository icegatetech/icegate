//! The one rule for static S3 keys.
//!
//! Every place that accepts an access key pair, a configured storage backend
//! and each job-storage config alike, applies the same rule through
//! [`StaticCredentials::from_parts`], so the places cannot drift apart. Where
//! the values come from (a config field, the `AWS_*` environment) stays with
//! the caller; this module only decides whether the values it is handed are a
//! usable set.

use std::fmt;

/// Stand-in printed for a secret in [`Debug`] output.
///
/// Public so every config that holds a secret key prints the same stand-in, rather than
/// each spelling its own.
pub const REDACTED: &str = "<redacted>";

/// A static S3 access key pair, with the session token of temporary keys.
///
/// Built only through [`Self::from_parts`], which guarantees both keys are
/// present and non-blank. The fields are public so a caller can move them into
/// the configuration of the client it builds.
#[derive(Clone, PartialEq, Eq)]
pub struct StaticCredentials {
    /// Public half of the key pair.
    pub access_key_id: String,
    /// Secret half of the key pair. Redacted in [`Debug`] output.
    pub secret_access_key: String,
    /// Session token of temporary keys, absent for long-lived ones. Redacted in
    /// [`Debug`] output.
    pub session_token: Option<String>,
}

/// Why a set of static key values is not a usable [`StaticCredentials`].
///
/// The message names the fields without a config path, so a caller prefixes
/// the path of its own config (`jobs_storage.credentials`, say) when it
/// reports the error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StaticCredentialsError {
    /// Exactly one of the access key id and the secret access key is set. A
    /// half pair signs nothing, so it fails per request, far from its cause,
    /// if it is let through.
    #[error("only one of access_key_id and secret_access_key is set; set both or neither")]
    HalfKeyPair,
    /// A session token is set with neither key. The token belongs to a key
    /// pair and means nothing without one.
    #[error("session_token is set without access_key_id and secret_access_key")]
    SessionTokenWithoutKeys,
}

impl StaticCredentials {
    /// Decide whether the three values make static credentials.
    ///
    /// A blank value (empty or whitespace only) counts as absent, so an unset
    /// environment variable and one set to an empty string mean the same.
    /// Values that are kept are not trimmed.
    ///
    /// Returns `Ok(None)` when no key and no token is set: the caller then
    /// authenticates through the ambient credential chain (an IAM role, for
    /// instance), which is not an error.
    ///
    /// # Errors
    ///
    /// Returns [`StaticCredentialsError::HalfKeyPair`] when only one key is set,
    /// and [`StaticCredentialsError::SessionTokenWithoutKeys`] when a token is
    /// set with neither key.
    pub fn from_parts(
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        session_token: Option<String>,
    ) -> Result<Option<Self>, StaticCredentialsError> {
        let access_key_id = discard_blank_value(access_key_id);
        let secret_access_key = discard_blank_value(secret_access_key);
        let session_token = discard_blank_value(session_token);

        match (access_key_id, secret_access_key) {
            (Some(access_key_id), Some(secret_access_key)) => Ok(Some(Self {
                access_key_id,
                secret_access_key,
                session_token,
            })),
            (None, None) if session_token.is_some() => Err(StaticCredentialsError::SessionTokenWithoutKeys),
            (None, None) => Ok(None),
            (Some(_), None) | (None, Some(_)) => Err(StaticCredentialsError::HalfKeyPair),
        }
    }
}

// Manual `Debug` so an accidental log of a config holding these can never leak
// the secret key or the token; the access key id is an identifier, not a secret.
impl fmt::Debug for StaticCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StaticCredentials")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &REDACTED)
            .field("session_token", &self.session_token.as_ref().map(|_| REDACTED))
            .finish()
    }
}

/// Turn a blank value (empty or whitespace only) into `None`.
///
/// Shared with the registry's environment fallback so that "blank means
/// absent" is decided in one place: a blank configured value must fall through
/// to the environment exactly as a missing one does.
pub(super) fn discard_blank_value(value: Option<String>) -> Option<String> {
    value.filter(|text| !text.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `from_parts` over borrowed values, so a case reads as the three inputs.
    fn resolve_credentials(
        access_key_id: Option<&str>,
        secret_access_key: Option<&str>,
        session_token: Option<&str>,
    ) -> Result<Option<StaticCredentials>, StaticCredentialsError> {
        StaticCredentials::from_parts(
            access_key_id.map(str::to_string),
            secret_access_key.map(str::to_string),
            session_token.map(str::to_string),
        )
    }

    #[test]
    fn both_keys_make_static_credentials() {
        let credentials = resolve_credentials(Some("key"), Some("secret"), None)
            .expect("a complete pair is accepted")
            .expect("a complete pair yields credentials");

        assert_eq!(
            credentials,
            StaticCredentials {
                access_key_id: "key".to_string(),
                secret_access_key: "secret".to_string(),
                session_token: None,
            }
        );
    }

    #[test]
    fn a_session_token_travels_with_the_keys() {
        let credentials = resolve_credentials(Some("key"), Some("secret"), Some("token"))
            .expect("a complete pair with a token is accepted")
            .expect("a complete pair yields credentials");

        assert_eq!(credentials.session_token.as_deref(), Some("token"));
    }

    #[test]
    fn no_keys_mean_no_static_credentials() {
        assert_eq!(resolve_credentials(None, None, None), Ok(None));
    }

    #[test]
    fn blank_keys_count_as_absent() {
        let cases = [
            (Some(""), Some("  "), None),
            (Some("\t"), Some(""), Some(" ")),
            (None, None, Some("")),
        ];

        for (access_key_id, secret_access_key, session_token) in cases {
            assert_eq!(
                resolve_credentials(access_key_id, secret_access_key, session_token),
                Ok(None),
                "{access_key_id:?} {secret_access_key:?} {session_token:?}"
            );
        }
    }

    #[test]
    fn a_blank_token_is_dropped_from_a_complete_pair() {
        let credentials = resolve_credentials(Some("key"), Some("secret"), Some("  "))
            .expect("a complete pair is accepted")
            .expect("a complete pair yields credentials");

        assert_eq!(credentials.session_token, None);
    }

    #[test]
    fn half_a_key_pair_is_rejected() {
        let cases = [
            (Some("key"), None),
            (None, Some("secret")),
            (Some("key"), Some("  ")),
            (Some(""), Some("secret")),
        ];

        for (access_key_id, secret_access_key) in cases {
            assert_eq!(
                resolve_credentials(access_key_id, secret_access_key, None),
                Err(StaticCredentialsError::HalfKeyPair),
                "{access_key_id:?} {secret_access_key:?}"
            );
        }
    }

    #[test]
    fn half_a_key_pair_is_rejected_even_with_a_token() {
        assert_eq!(
            resolve_credentials(Some("key"), None, Some("token")),
            Err(StaticCredentialsError::HalfKeyPair)
        );
    }

    #[test]
    fn a_session_token_without_keys_is_rejected() {
        assert_eq!(
            resolve_credentials(None, None, Some("token")),
            Err(StaticCredentialsError::SessionTokenWithoutKeys)
        );
    }

    #[test]
    fn debug_output_redacts_the_secret_and_the_token() {
        let credentials = StaticCredentials {
            access_key_id: "AKIDEXAMPLE".to_string(),
            secret_access_key: "wJalrSECRETvalue".to_string(),
            session_token: Some("FwoGZXIvSESSIONtoken".to_string()),
        };

        let rendered = format!("{credentials:?}");

        assert!(!rendered.contains("wJalrSECRETvalue"), "{rendered}");
        assert!(!rendered.contains("FwoGZXIvSESSIONtoken"), "{rendered}");
        assert!(rendered.contains("AKIDEXAMPLE"), "{rendered}");
    }
}
