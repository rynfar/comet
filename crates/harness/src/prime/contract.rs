use std::collections::BTreeSet;

use serde::Deserialize;

use super::PrimeDaemonError;

pub const MIN_PROTOCOL_VERSION: u64 = 7;
pub const CONTROL_VERSION: u8 = 1;
pub const MAX_FRAME_BYTES: usize = 64 * 1024;
pub const MAX_CAPABILITIES: usize = 128;
pub const MAX_CAPABILITY_BYTES: usize = 128;
const MAX_ERROR_CODE_BYTES: usize = 64;

/// Stock-compatible capabilities required before later session work can build
/// on this process owner. Fork extensions are negotiated separately.
pub const REQUIRED_DAEMON_CAPABILITIES: &[&str] = &[
    "attach_snapshot",
    "event_sequence",
    "client_owned_sessions",
    "extension_ui",
    "session_input_admission",
    "prompt_admission_cancellation",
];

/// Server offer used by the Pylon fork for correlated prompt settlement.
/// Later attach logic must also advertise the matching client capability.
pub const CORRELATED_PROMPT_LIFECYCLE_CAPABILITY: &str = "correlated_prompt_lifecycle_v1";

/// Bounded server capability offers from `daemon_hello`.
///
/// An offer is not an active session negotiation. Later attach code must also
/// advertise and confirm its matching client capability before enabling a
/// fork-only behavior.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimeServerCapabilities {
    protocol_version: u64,
    values: BTreeSet<String>,
}

impl PrimeServerCapabilities {
    /// The compatible daemon protocol version reported by `daemon_hello`.
    pub fn protocol_version(&self) -> u64 {
        self.protocol_version
    }

    /// Whether the server offered a validated capability token. This does not
    /// prove that a later session attachment negotiated the matching client side.
    pub fn server_offers(&self, capability: &str) -> bool {
        self.values.contains(capability)
    }

    pub(super) fn validated(
        protocol_version: u64,
        capabilities: Vec<String>,
    ) -> Result<Self, PrimeDaemonError> {
        if protocol_version < MIN_PROTOCOL_VERSION {
            return Err(PrimeDaemonError::IncompatibleHello {
                reason: format!("protocol v{MIN_PROTOCOL_VERSION}+ is required"),
            });
        }
        if capabilities.len() > MAX_CAPABILITIES
            || capabilities.iter().any(|value| {
                value.is_empty()
                    || value.len() > MAX_CAPABILITY_BYTES
                    || !value.bytes().all(|byte| {
                        byte.is_ascii_lowercase()
                            || byte.is_ascii_digit()
                            || matches!(byte, b'_' | b'-' | b'.')
                    })
            })
        {
            return Err(PrimeDaemonError::IncompatibleHello {
                reason: "the capability set is invalid".into(),
            });
        }
        let values = capabilities.into_iter().collect::<BTreeSet<_>>();
        let missing = REQUIRED_DAEMON_CAPABILITIES
            .iter()
            .copied()
            .filter(|capability| !values.contains(*capability))
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(PrimeDaemonError::IncompatibleHello {
                reason: format!("missing required capabilities: {}", missing.join(", ")),
            });
        }
        Ok(Self {
            protocol_version,
            values,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub(super) enum BridgeResponse {
    #[serde(rename_all = "camelCase")]
    Loaded {
        v: u8,
        id: u64,
        protocol_version: u64,
    },
    #[serde(rename_all = "camelCase")]
    Ready {
        v: u8,
        id: u64,
        protocol_version: u64,
        capabilities: Vec<String>,
    },
    Shutdown {
        v: u8,
        id: u64,
        acknowledged: bool,
    },
    Error {
        v: u8,
        id: u64,
        code: String,
    },
}

impl BridgeResponse {
    pub(super) fn validate_meta(&self, expected_id: u64) -> Result<(), PrimeDaemonError> {
        let (version, id) = match self {
            Self::Loaded { v, id, .. }
            | Self::Ready { v, id, .. }
            | Self::Shutdown { v, id, .. }
            | Self::Error { v, id, .. } => (*v, *id),
        };
        if version != CONTROL_VERSION {
            return Err(PrimeDaemonError::InvalidBridgeFrame {
                reason: "unsupported control version",
            });
        }
        if id != expected_id {
            return Err(PrimeDaemonError::InvalidBridgeFrame {
                reason: "response id mismatch",
            });
        }
        if let Self::Error { code, .. } = self
            && (code.is_empty()
                || code.len() > MAX_ERROR_CODE_BYTES
                || !code
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'))
        {
            return Err(PrimeDaemonError::InvalidBridgeFrame {
                reason: "error code is invalid",
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn optional_and_unknown_capabilities_do_not_change_the_baseline() {
        let mut values = REQUIRED_DAEMON_CAPABILITIES
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>();
        values.push("unknown_future_capability".into());
        let caps = PrimeServerCapabilities::validated(8, values).unwrap();
        assert!(caps.server_offers("unknown_future_capability"));
        assert!(!caps.server_offers(CORRELATED_PROMPT_LIFECYCLE_CAPABILITY));
    }

    #[test]
    fn missing_baseline_is_explicit() {
        let error = PrimeServerCapabilities::validated(7, Vec::new()).unwrap_err();
        assert!(error.to_string().contains("attach_snapshot"), "{error}");
    }

    #[test]
    fn capability_tokens_cannot_smuggle_control_text() {
        let mut values = REQUIRED_DAEMON_CAPABILITIES
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>();
        values.push("unknown\n/private/path".into());
        assert!(PrimeServerCapabilities::validated(7, values).is_err());
    }

    #[test]
    fn bridge_error_codes_cannot_smuggle_diagnostics() {
        let response = BridgeResponse::Error {
            v: CONTROL_VERSION,
            id: 1,
            code: "/private/path".into(),
        };
        assert!(response.validate_meta(1).is_err());
    }
}
