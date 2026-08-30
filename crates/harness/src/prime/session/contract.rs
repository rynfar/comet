use serde::Deserialize;
use serde_json::Value;

use super::{PrimeSessionEvent, PrimeSessionSnapshotReceipt};
use crate::prime::PrimeDaemonError;

pub(super) const CONTROL_VERSION: u8 = 1;
/// Raw JSON bytes before the line-feed terminator.
pub(super) const MAX_FRAME_BYTES: usize = 16 * 1024;
pub(super) const MAX_PENDING_REQUESTS: usize = 8;
pub(super) const EVENT_QUEUE_CAPACITY: usize = 32;
pub(super) const RETIRED_ID_CAPACITY: usize = 64;
pub(super) const MAX_CONTROL_ID_DIGITS: usize = 16;
pub(super) const MAX_ERROR_CODE_BYTES: usize = 64;
pub(super) const MAX_NATIVE_ID_BYTES: usize = 256;
const MAX_MESSAGE_COUNT: u64 = 9_007_199_254_740_991;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Operation {
    Load,
    Connect,
    Create,
    Attach,
    Snapshot,
    Close,
}

impl Operation {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Load => "load",
            Self::Connect => "connect",
            Self::Create => "create",
            Self::Attach => "attach",
            Self::Snapshot => "snapshot",
            Self::Close => "close",
        }
    }

    pub(super) const fn stage(self) -> &'static str {
        match self {
            Self::Load => "session host load",
            Self::Connect => "session host connect",
            Self::Create => "session creation",
            Self::Attach => "session attachment",
            Self::Snapshot => "session snapshot",
            Self::Close => "session close",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "load" => Some(Self::Load),
            "connect" => Some(Self::Connect),
            "create" => Some(Self::Create),
            "attach" => Some(Self::Attach),
            "snapshot" => Some(Self::Snapshot),
            "close" => Some(Self::Close),
            _ => None,
        }
    }
}

/// An opaque native identifier. It never leaves this private module and does
/// not implement formatting or serialization traits.
pub(super) struct ActiveSessionId(String);

impl ActiveSessionId {
    pub(super) fn as_str(&self) -> &str {
        &self.0
    }
}

pub(super) enum HostReply {
    Loaded,
    Connected,
    Created(ActiveSessionId),
    Attached,
    Snapshot(PrimeSessionSnapshotReceipt),
    Closed,
    Rejected { operation: Operation },
}

pub(super) enum Inbound {
    Response {
        id: String,
        operation: Operation,
        reply: HostReply,
    },
    Event(PrimeSessionEvent),
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
enum RawInbound {
    #[serde(rename_all = "camelCase")]
    Response {
        v: u8,
        id: String,
        op: String,
        ok: bool,
        #[serde(default)]
        data: Option<Value>,
        #[serde(default)]
        code: Option<String>,
    },
    Event {
        v: u8,
        event: String,
        code: String,
    },
}

pub(super) fn parse_inbound(bytes: &[u8]) -> Result<Inbound, PrimeDaemonError> {
    let frame =
        serde_json::from_slice::<RawInbound>(bytes).map_err(|_| invalid("malformed frame"))?;
    match frame {
        RawInbound::Response {
            v,
            id,
            op,
            ok,
            data,
            code,
        } => {
            validate_version(v)?;
            validate_control_id(&id)?;
            let operation =
                Operation::parse(&op).ok_or_else(|| invalid("unknown response operation"))?;
            let reply = validate_response(operation, ok, data, code)?;
            Ok(Inbound::Response {
                id,
                operation,
                reply,
            })
        }
        RawInbound::Event { v, event, code } => {
            validate_version(v)?;
            if event != "closed" || code != "client-closed" {
                return Err(invalid("unknown session event"));
            }
            Ok(Inbound::Event(PrimeSessionEvent::Closed))
        }
    }
}

fn validate_version(version: u8) -> Result<(), PrimeDaemonError> {
    if version != CONTROL_VERSION {
        return Err(invalid("unsupported session control version"));
    }
    Ok(())
}

pub(super) fn validate_control_id(id: &str) -> Result<(), PrimeDaemonError> {
    if id.is_empty()
        || id.len() > MAX_CONTROL_ID_DIGITS
        || id.as_bytes()[0] == b'0'
        || !id.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(invalid("invalid response id"));
    }
    Ok(())
}

fn validate_response(
    operation: Operation,
    ok: bool,
    data: Option<Value>,
    code: Option<String>,
) -> Result<HostReply, PrimeDaemonError> {
    if !ok {
        if data.is_some() {
            return Err(invalid("rejected response contains data"));
        }
        let code = code.ok_or_else(|| invalid("rejected response has no code"))?;
        validate_error_code(&code)?;
        return Ok(HostReply::Rejected { operation });
    }
    if code.is_some() {
        return Err(invalid("successful response contains an error code"));
    }

    match operation {
        Operation::Load => {
            validate_empty_data(data)?;
            Ok(HostReply::Loaded)
        }
        Operation::Connect => {
            validate_empty_data(data)?;
            Ok(HostReply::Connected)
        }
        Operation::Create => {
            #[derive(Deserialize)]
            #[serde(rename_all = "camelCase", deny_unknown_fields)]
            struct Data {
                active_session_id: String,
            }
            let data = deserialize_data::<Data>(data)?;
            validate_native_id(&data.active_session_id)?;
            Ok(HostReply::Created(ActiveSessionId(data.active_session_id)))
        }
        Operation::Attach => {
            validate_empty_data(data)?;
            Ok(HostReply::Attached)
        }
        Operation::Snapshot => {
            #[derive(Deserialize)]
            #[serde(rename_all = "camelCase", deny_unknown_fields)]
            struct Data {
                message_count: u64,
                is_streaming: bool,
                has_cursor: bool,
            }
            let data = deserialize_data::<Data>(data)?;
            if data.message_count > MAX_MESSAGE_COUNT {
                return Err(invalid("snapshot message count is invalid"));
            }
            Ok(HostReply::Snapshot(PrimeSessionSnapshotReceipt::new(
                data.message_count,
                data.is_streaming,
                data.has_cursor,
            )))
        }
        Operation::Close => {
            validate_empty_data(data)?;
            // The fixed host contract emits success only after it validates the
            // SDK's raw authoritative owning-cleanup completion. Rust never
            // accepts connection disposal or an untyped payload as proof.
            Ok(HostReply::Closed)
        }
    }
}

fn deserialize_data<T>(data: Option<Value>) -> Result<T, PrimeDaemonError>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_value(data.ok_or_else(|| invalid("successful response has no data"))?)
        .map_err(|_| invalid("response data is malformed"))
}

fn validate_empty_data(data: Option<Value>) -> Result<(), PrimeDaemonError> {
    match data {
        None => Ok(()),
        Some(_) => Err(invalid("response contains unexpected data")),
    }
}

fn validate_error_code(code: &str) -> Result<(), PrimeDaemonError> {
    if code.is_empty()
        || code.len() > MAX_ERROR_CODE_BYTES
        || !code
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(invalid("invalid session host error code"));
    }
    Ok(())
}

fn validate_native_id(value: &str) -> Result<(), PrimeDaemonError> {
    if value.is_empty()
        || value.len() > MAX_NATIVE_ID_BYTES
        || value
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(invalid("invalid private session identifier"));
    }
    Ok(())
}

pub(super) fn invalid(reason: &'static str) -> PrimeDaemonError {
    PrimeDaemonError::InvalidSessionHostFrame { reason }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(id: &str, op: &str, data: Value) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "v": CONTROL_VERSION,
            "kind": "response",
            "id": id,
            "op": op,
            "ok": true,
            "data": data,
        }))
        .unwrap()
    }

    #[test]
    fn control_ids_are_checked_decimal_strings() {
        for valid in ["1", "9", "10", "9999999999999999"] {
            validate_control_id(valid).unwrap();
        }
        for invalid_id in ["", "0", "01", "-1", "+1", "1.0", "99999999999999999"] {
            assert!(
                validate_control_id(invalid_id).is_err(),
                "accepted {invalid_id}"
            );
        }
        let numeric = br#"{"v":1,"kind":"response","id":1,"op":"load","ok":true}"#;
        assert!(parse_inbound(numeric).is_err());
    }

    #[test]
    fn native_create_identity_stays_in_a_private_typed_reply() {
        let frame = response(
            "7",
            "create",
            serde_json::json!({"activeSessionId":"private-native-value"}),
        );
        match parse_inbound(&frame).unwrap() {
            Inbound::Response {
                reply: HostReply::Created(id),
                ..
            } => assert_eq!(id.as_str(), "private-native-value"),
            _ => panic!("unexpected reply"),
        }
        let too_large = "x".repeat(MAX_NATIVE_ID_BYTES + 1);
        assert!(
            parse_inbound(&response(
                "7",
                "create",
                serde_json::json!({"activeSessionId":too_large}),
            ))
            .is_err()
        );
    }

    #[test]
    fn snapshot_projection_contains_only_safe_receipt_fields() {
        let frame = response(
            "4",
            "snapshot",
            serde_json::json!({
                "messageCount": 3,
                "isStreaming": false,
                "hasCursor": true,
            }),
        );
        match parse_inbound(&frame).unwrap() {
            Inbound::Response {
                reply: HostReply::Snapshot(receipt),
                ..
            } => {
                assert_eq!(receipt.message_count(), 3);
                assert!(!receipt.is_streaming());
                assert!(receipt.has_cursor());
            }
            _ => panic!("unexpected reply"),
        }
    }

    #[test]
    fn error_codes_and_fixed_events_are_bounded() {
        let code = "x".repeat(MAX_ERROR_CODE_BYTES + 1);
        let frame = serde_json::to_vec(&serde_json::json!({
            "v": 1,
            "kind": "response",
            "id": "1",
            "op": "load",
            "ok": false,
            "code": code,
        }))
        .unwrap();
        assert!(parse_inbound(&frame).is_err());
        assert!(
            parse_inbound(br#"{"v":1,"kind":"event","event":"closed","code":"client-closed"}"#)
                .is_ok()
        );
        assert!(
            parse_inbound(br#"{"v":1,"kind":"event","event":"closed","code":"raw-private"}"#)
                .is_err()
        );
    }

    #[test]
    fn queue_and_frame_bounds_are_fixed() {
        assert_eq!(MAX_FRAME_BYTES, 16 * 1024);
        assert_eq!(MAX_PENDING_REQUESTS, 8);
        assert_eq!(EVENT_QUEUE_CAPACITY, 32);
        assert_eq!(MAX_ERROR_CODE_BYTES, 64);
        assert_eq!(MAX_NATIVE_ID_BYTES, 256);
    }
}
