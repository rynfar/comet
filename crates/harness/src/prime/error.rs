use std::io::ErrorKind;
use std::time::Duration;

/// Sanitized local failure from the Prime daemon foundation. It deliberately
/// stores no native path, raw daemon payload, environment value, or stderr.
#[derive(Debug, thiserror::Error)]
pub enum PrimeDaemonError {
    #[error("Prime Agent is not installed ({reason})")]
    NotInstalled { reason: &'static str },
    #[error("Prime Agent package is incompatible ({reason})")]
    IncompatiblePackage { reason: &'static str },
    #[error("Prime Agent private transport is unavailable ({reason})")]
    TransportSecurity { reason: &'static str },
    #[error("Prime Agent {stage} failed ({kind:?})")]
    Io {
        stage: &'static str,
        kind: ErrorKind,
    },
    #[error("Prime Agent bridge rejected {stage} ({code})")]
    Bridge { stage: &'static str, code: String },
    #[error("Prime Agent daemon handshake is incompatible ({reason})")]
    IncompatibleHello { reason: String },
    #[error("Prime Agent {stage} timed out after {} ms", timeout.as_millis())]
    Timeout {
        stage: &'static str,
        timeout: Duration,
    },
    #[error("Prime Agent {process} exited unexpectedly ({status})")]
    ProcessExit {
        process: &'static str,
        status: String,
    },
    #[error("Prime Agent bridge emitted an invalid control frame ({reason})")]
    InvalidBridgeFrame { reason: &'static str },
}

impl PrimeDaemonError {
    pub(super) fn io(stage: &'static str, error: &std::io::Error) -> Self {
        Self::Io {
            stage,
            kind: error.kind(),
        }
    }
}

impl From<PrimeDaemonError> for crate::HarnessError {
    fn from(error: PrimeDaemonError) -> Self {
        match error {
            PrimeDaemonError::NotInstalled { reason } => Self::NotInstalled(reason.into()),
            other => Self::Protocol(other.to_string()),
        }
    }
}
