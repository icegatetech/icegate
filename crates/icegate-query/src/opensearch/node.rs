//! What the listener reports about itself: a cluster of one node.
//!
//! The build block of `GET /` is a set of IceGate constants, not the description of a real
//! build: the hash and the date are fixed placeholders no release changes, and only
//! `version.number` follows the configuration.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use sha2::{Digest as _, Sha256};

use super::config::EmulatedVersion;

/// `version.distribution` of the main response.
pub(crate) const DISTRIBUTION: &str = "opensearch";
/// `version.build_type` of the main response.
pub(crate) const BUILD_TYPE: &str = "tar";
/// `version.build_hash`: a placeholder that names no commit.
pub(crate) const BUILD_HASH: &str = "0000000000000000000000000000000000000000";
/// `version.build_date`: a fixed placeholder no release changes, for the reason [`BUILD_HASH`]
/// is one.
pub(crate) const BUILD_DATE: &str = "1970-01-01T00:00:00.000Z";
/// `version.lucene_version`: the Lucene release whose standard analyzer and classic query
/// parser IceGate's search semantics follow.
pub(crate) const LUCENE_VERSION: &str = "10.5.1";
/// `version.minimum_wire_compatibility_version` of the main response.
pub(crate) const MINIMUM_WIRE_COMPATIBILITY_VERSION: &str = "2.19.0";
/// `version.minimum_index_compatibility_version` of the main response.
pub(crate) const MINIMUM_INDEX_COMPATIBILITY_VERSION: &str = "2.0.0";
/// `tagline` of the main response.
pub(crate) const TAGLINE: &str = "The OpenSearch Project: https://opensearch.org/";

/// The identity the listener reports as a cluster of one node.
///
/// `cluster_uuid` and `node_id` derive from the cluster name alone, so they are the same across
/// restarts and replicas of one deployment, and two listeners configured with one cluster name
/// report the same identifiers.
#[derive(Debug, Clone)]
pub(crate) struct NodeIdentity {
    /// Reported `cluster_name`.
    pub(crate) cluster_name: String,
    /// Reported `cluster_uuid`: [`derive_identifier`] of purpose `cluster` and the cluster name.
    pub(crate) cluster_uuid: String,
    /// Key of the node under `nodes` in `/_nodes`: [`derive_identifier`] of purpose `node` and the
    /// cluster name.
    pub(crate) node_id: String,
    /// Reported node `name`: the cluster name followed by `-node`.
    pub(crate) node_name: String,
    /// Reported `version.number`.
    pub(crate) version: EmulatedVersion,
    /// Reported `http.bound_address`: the address the listener bound, as bound.
    pub(crate) bound_address: SocketAddr,
    /// Reported `ip` and `http.publish_address`: the bound address, with an unspecified IP
    /// replaced by the loopback of its family.
    pub(crate) publish_address: SocketAddr,
}

impl NodeIdentity {
    /// Describe the one node of the cluster named `cluster_name`, reporting `version` and bound at
    /// `bound_address`.
    pub(crate) fn new(cluster_name: &str, version: EmulatedVersion, bound_address: SocketAddr) -> Self {
        let publish_ip = match bound_address.ip() {
            IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
            ip @ (IpAddr::V4(_) | IpAddr::V6(_)) => ip,
        };
        Self {
            cluster_name: cluster_name.to_string(),
            cluster_uuid: derive_identifier("cluster", cluster_name),
            node_id: derive_identifier("node", cluster_name),
            node_name: format!("{cluster_name}-node"),
            version,
            bound_address,
            publish_address: SocketAddr::new(publish_ip, bound_address.port()),
        }
    }
}

/// A 22-character URL-safe identifier, stable for the pair `(purpose, seed)`: the SHA-256 of
/// `purpose`, a zero byte and `seed`, cut to its first 16 bytes and encoded base64url without
/// padding. The zero byte keeps `("ab", "c")` and `("a", "bc")` apart.
pub(crate) fn derive_identifier(purpose: &str, seed: &str) -> String {
    let digest = Sha256::new()
        .chain_update(purpose.as_bytes())
        .chain_update([0_u8])
        .chain_update(seed.as_bytes())
        .finalize();
    URL_SAFE_NO_PAD.encode(&digest[..16])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_are_22_url_safe_characters_stable_and_distinct_per_purpose() {
        let cluster_identifier = derive_identifier("cluster", "icegate");

        assert_eq!(cluster_identifier.len(), 22);
        assert!(
            cluster_identifier
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        );
        assert_eq!(cluster_identifier, derive_identifier("cluster", "icegate"));
        assert_ne!(cluster_identifier, derive_identifier("node", "icegate"));
        assert_ne!(cluster_identifier, derive_identifier("cluster", "stand"));
        assert_ne!(derive_identifier("ab", "c"), derive_identifier("a", "bc"));
    }

    /// The expected values were computed outside this code. `printf 'cluster\0icegate' | shasum -a 256`
    /// prints `221dd997f7c3ff7eb964cb550e0b8f7ec28c3f5c59dfbd6223b6b15238bd4616`; its first 16
    /// bytes in base64url without padding are `Ih3Zl_fD_365ZMtVDguPfg`. `printf 'node\0icegate'`
    /// prints `41df6c1029571b26ac8af9b1ecfa346db75bdcf8de5978a243a3b32f1ff263f5`, which gives
    /// `Qd9sEClXGyasivmx7Po0bQ`. A change here hands every deployment new identifiers.
    #[test]
    fn a_derived_identifier_matches_an_independently_computed_sha256_prefix() {
        assert_eq!(derive_identifier("cluster", "icegate"), "Ih3Zl_fD_365ZMtVDguPfg");
        assert_eq!(derive_identifier("node", "icegate"), "Qd9sEClXGyasivmx7Po0bQ");
    }

    /// An unspecified bind address is not one a client can connect to, so the loopback of its
    /// family is published with the bound port; a concrete address is published as bound. The
    /// bound address itself is reported as bound, never rewritten.
    #[test]
    fn an_unspecified_bind_address_publishes_loopback_on_the_bound_port() {
        let version = EmulatedVersion::parse("3.9.0").expect("version");
        let cases = [
            ("0.0.0.0:9200", "127.0.0.1:9200"),
            ("[::]:19200", "[::1]:19200"),
            ("10.1.2.3:9200", "10.1.2.3:9200"),
        ];
        for (bound_text, published_text) in cases {
            let node = NodeIdentity::new("icegate", version, bound_text.parse().expect("socket address"));

            assert_eq!(node.publish_address.to_string(), published_text, "{bound_text}");
            assert_eq!(node.bound_address.to_string(), bound_text, "{bound_text}");
        }
    }
}
