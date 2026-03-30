use std::time::Duration;

use rand::RngCore;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::Error;
use crate::recovery::NegotiatedRecoveryOptions;

pub(crate) const CONTROL_STREAM_ID: u32 = u32::MAX;

pub(crate) const CONTROL_TYPE_ACK: u8 = 1;
pub(crate) const CONTROL_TYPE_PING: u8 = 2;

pub(crate) const ATTACH_MODE_NEW: u8 = 1;
pub(crate) const ATTACH_MODE_RESUME: u8 = 2;

pub(crate) const ATTACH_STATUS_OK: u8 = 1;
pub(crate) const ATTACH_STATUS_REJECTED: u8 = 2;

pub(crate) const RECOVERY_TOKEN_LEN: usize = 16;
pub(crate) const ATTACH_REQUEST_LEN: usize = 44;
pub(crate) const ATTACH_RESPONSE_LEN: usize = 60;
pub(crate) const CONTROL_ACK_FRAME_LEN: usize = 9;
pub(crate) const CONTROL_PING_LEN: usize = 1;

pub(crate) type RecoveryToken = [u8; RECOVERY_TOKEN_LEN];

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct AttachRequest {
    pub mode: u8,
    pub connection_id: RecoveryToken,
    pub resume_secret: RecoveryToken,
    pub last_recv_seq: u64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct AttachResponse {
    pub status: u8,
    pub connection_id: RecoveryToken,
    pub resume_secret: RecoveryToken,
    pub last_recv_seq: u64,
    pub negotiated: NegotiatedRecoveryOptions,
}

pub(crate) fn new_recovery_token() -> Result<RecoveryToken, Error> {
    let mut token = [0u8; RECOVERY_TOKEN_LEN];
    rand::thread_rng().fill_bytes(&mut token);
    Ok(token)
}

pub(crate) async fn write_attach_request<W>(
    writer: &mut W,
    req: &AttachRequest,
) -> Result<(), Error>
where
    W: AsyncWrite + Unpin,
{
    let mut buf = [0u8; ATTACH_REQUEST_LEN];
    buf[0] = req.mode;
    buf[4..20].copy_from_slice(&req.connection_id);
    buf[20..36].copy_from_slice(&req.resume_secret);
    buf[36..44].copy_from_slice(&req.last_recv_seq.to_be_bytes());
    writer.write_all(&buf).await?;
    writer.flush().await?;
    Ok(())
}

pub(crate) async fn read_attach_request<R>(reader: &mut R) -> Result<AttachRequest, Error>
where
    R: AsyncRead + Unpin,
{
    let mut buf = [0u8; ATTACH_REQUEST_LEN];
    reader.read_exact(&mut buf).await?;
    let mut connection_id = [0u8; RECOVERY_TOKEN_LEN];
    connection_id.copy_from_slice(&buf[4..20]);
    let mut resume_secret = [0u8; RECOVERY_TOKEN_LEN];
    resume_secret.copy_from_slice(&buf[20..36]);
    Ok(AttachRequest {
        mode: buf[0],
        connection_id,
        resume_secret,
        last_recv_seq: u64::from_be_bytes(buf[36..44].try_into().unwrap()),
    })
}

pub(crate) async fn write_attach_response<W>(
    writer: &mut W,
    resp: &AttachResponse,
) -> Result<(), Error>
where
    W: AsyncWrite + Unpin,
{
    let encoded = encode_negotiated_recovery_options(&resp.negotiated)?;
    let mut buf = [0u8; ATTACH_RESPONSE_LEN];
    buf[0] = resp.status;
    buf[4..20].copy_from_slice(&resp.connection_id);
    buf[20..36].copy_from_slice(&resp.resume_secret);
    buf[36..44].copy_from_slice(&resp.last_recv_seq.to_be_bytes());
    buf[44..48].copy_from_slice(&encoded.ack_every.to_be_bytes());
    buf[48..52].copy_from_slice(&encoded.ack_delay_ms.to_be_bytes());
    buf[52..56].copy_from_slice(&encoded.heartbeat_interval_ms.to_be_bytes());
    buf[56..60].copy_from_slice(&encoded.heartbeat_timeout_ms.to_be_bytes());
    writer.write_all(&buf).await?;
    writer.flush().await?;
    Ok(())
}

pub(crate) async fn read_attach_response<R>(reader: &mut R) -> Result<AttachResponse, Error>
where
    R: AsyncRead + Unpin,
{
    let mut buf = [0u8; ATTACH_RESPONSE_LEN];
    reader.read_exact(&mut buf).await?;
    let mut connection_id = [0u8; RECOVERY_TOKEN_LEN];
    connection_id.copy_from_slice(&buf[4..20]);
    let mut resume_secret = [0u8; RECOVERY_TOKEN_LEN];
    resume_secret.copy_from_slice(&buf[20..36]);
    Ok(AttachResponse {
        status: buf[0],
        connection_id,
        resume_secret,
        last_recv_seq: u64::from_be_bytes(buf[36..44].try_into().unwrap()),
        negotiated: decode_negotiated_recovery_options(
            u32::from_be_bytes(buf[44..48].try_into().unwrap()),
            u32::from_be_bytes(buf[48..52].try_into().unwrap()),
            u32::from_be_bytes(buf[52..56].try_into().unwrap()),
            u32::from_be_bytes(buf[56..60].try_into().unwrap()),
        )?,
    })
}

struct EncodedNegotiatedRecoveryOptions {
    ack_every: u32,
    ack_delay_ms: u32,
    heartbeat_interval_ms: u32,
    heartbeat_timeout_ms: u32,
}

fn encode_negotiated_recovery_options(
    opts: &NegotiatedRecoveryOptions,
) -> Result<EncodedNegotiatedRecoveryOptions, Error> {
    let opts = opts.normalized();
    Ok(EncodedNegotiatedRecoveryOptions {
        ack_every: opts.ack_every,
        ack_delay_ms: duration_to_millis(opts.ack_delay)?,
        heartbeat_interval_ms: duration_to_millis(opts.heartbeat_interval)?,
        heartbeat_timeout_ms: duration_to_millis(opts.heartbeat_timeout)?,
    })
}

fn decode_negotiated_recovery_options(
    ack_every: u32,
    ack_delay_ms: u32,
    heartbeat_interval_ms: u32,
    heartbeat_timeout_ms: u32,
) -> Result<NegotiatedRecoveryOptions, Error> {
    let opts = NegotiatedRecoveryOptions {
        ack_every,
        ack_delay: Duration::from_millis(u64::from(ack_delay_ms)),
        heartbeat_interval: Duration::from_millis(u64::from(heartbeat_interval_ms)),
        heartbeat_timeout: Duration::from_millis(u64::from(heartbeat_timeout_ms)),
    };
    if opts.ack_every == 0 {
        return Err(Error::InvalidMessage(
            "negotiated ack_every must be positive".to_string(),
        ));
    }
    if opts.ack_delay.is_zero() {
        return Err(Error::InvalidMessage(
            "negotiated ack_delay must be positive".to_string(),
        ));
    }
    if opts.heartbeat_interval.is_zero() {
        return Err(Error::InvalidMessage(
            "negotiated heartbeat_interval must be positive".to_string(),
        ));
    }
    if opts.heartbeat_timeout < opts.heartbeat_interval.saturating_mul(2) {
        return Err(Error::InvalidMessage(
            "negotiated heartbeat_timeout too small".to_string(),
        ));
    }
    Ok(opts)
}

fn duration_to_millis(duration: Duration) -> Result<u32, Error> {
    if duration.is_zero() {
        return Err(Error::InvalidMessage(
            "recovery duration must be positive".to_string(),
        ));
    }
    let millis = duration.as_millis().max(1);
    if millis > u32::MAX as u128 {
        return Err(Error::InvalidMessage(
            "recovery duration too large".to_string(),
        ));
    }
    Ok(millis as u32)
}

pub(crate) fn build_ack_control_payload(last_recv_seq: u64) -> Vec<u8> {
    let mut buf = vec![0u8; CONTROL_ACK_FRAME_LEN];
    buf[0] = CONTROL_TYPE_ACK;
    buf[1..9].copy_from_slice(&last_recv_seq.to_be_bytes());
    buf
}

pub(crate) fn build_ping_control_payload() -> Vec<u8> {
    vec![CONTROL_TYPE_PING]
}

pub(crate) fn parse_control_payload(payload: &[u8]) -> Result<(u8, u64), Error> {
    if payload.is_empty() {
        return Err(Error::InvalidMessage("empty control payload".to_string()));
    }
    match payload[0] {
        CONTROL_TYPE_ACK => {
            if payload.len() != CONTROL_ACK_FRAME_LEN {
                return Err(Error::InvalidMessage(
                    "invalid ack control payload length".to_string(),
                ));
            }
            Ok((
                payload[0],
                u64::from_be_bytes(payload[1..9].try_into().unwrap()),
            ))
        }
        CONTROL_TYPE_PING => {
            if payload.len() != CONTROL_PING_LEN {
                return Err(Error::InvalidMessage(
                    "invalid heartbeat control payload length".to_string(),
                ));
            }
            Ok((payload[0], 0))
        }
        _ => Err(Error::InvalidMessage(
            "unknown control frame type".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn attach_request_roundtrip() {
        let (mut client, mut server) = tokio::io::duplex(ATTACH_REQUEST_LEN * 2);
        let request = AttachRequest {
            mode: ATTACH_MODE_RESUME,
            connection_id: [1; RECOVERY_TOKEN_LEN],
            resume_secret: [2; RECOVERY_TOKEN_LEN],
            last_recv_seq: 17,
        };

        let write = tokio::spawn(async move { write_attach_request(&mut client, &request).await });
        let read = read_attach_request(&mut server)
            .await
            .expect("read attach request");
        write
            .await
            .expect("writer task")
            .expect("write attach request");

        assert_eq!(read.mode, ATTACH_MODE_RESUME);
        assert_eq!(read.connection_id, [1; RECOVERY_TOKEN_LEN]);
        assert_eq!(read.resume_secret, [2; RECOVERY_TOKEN_LEN]);
        assert_eq!(read.last_recv_seq, 17);
    }

    #[tokio::test]
    async fn attach_response_roundtrip() {
        let (mut client, mut server) = tokio::io::duplex(ATTACH_RESPONSE_LEN * 2);
        let response = AttachResponse {
            status: ATTACH_STATUS_OK,
            connection_id: [3; RECOVERY_TOKEN_LEN],
            resume_secret: [4; RECOVERY_TOKEN_LEN],
            last_recv_seq: 23,
            negotiated: NegotiatedRecoveryOptions {
                ack_every: 8,
                ack_delay: Duration::from_millis(10),
                heartbeat_interval: Duration::from_millis(20),
                heartbeat_timeout: Duration::from_millis(60),
            },
        };

        let write =
            tokio::spawn(async move { write_attach_response(&mut client, &response).await });
        let read = read_attach_response(&mut server)
            .await
            .expect("read attach response");
        write
            .await
            .expect("writer task")
            .expect("write attach response");

        assert_eq!(read.status, ATTACH_STATUS_OK);
        assert_eq!(read.connection_id, [3; RECOVERY_TOKEN_LEN]);
        assert_eq!(read.resume_secret, [4; RECOVERY_TOKEN_LEN]);
        assert_eq!(read.last_recv_seq, 23);
        assert_eq!(read.negotiated.ack_every, 8);
    }

    #[tokio::test]
    async fn read_attach_response_rejects_invalid_negotiated_options() {
        let (mut client, mut server) = tokio::io::duplex(ATTACH_RESPONSE_LEN * 2);
        let mut buf = [0u8; ATTACH_RESPONSE_LEN];
        buf[0] = ATTACH_STATUS_OK;
        buf[4..20].copy_from_slice(&[3; RECOVERY_TOKEN_LEN]);
        buf[20..36].copy_from_slice(&[4; RECOVERY_TOKEN_LEN]);
        buf[36..44].copy_from_slice(&23u64.to_be_bytes());
        // ack_every is invalid when zero.
        buf[44..48].copy_from_slice(&0u32.to_be_bytes());
        buf[48..52].copy_from_slice(&10u32.to_be_bytes());
        buf[52..56].copy_from_slice(&20u32.to_be_bytes());
        buf[56..60].copy_from_slice(&60u32.to_be_bytes());

        let write = tokio::spawn(async move {
            client.write_all(&buf).await.expect("write response bytes");
            client.flush().await.expect("flush response bytes");
        });
        let err = read_attach_response(&mut server)
            .await
            .expect_err("invalid negotiated options should fail");
        write.await.expect("writer task");

        assert!(matches!(err, Error::InvalidMessage(_)));
    }

    #[test]
    fn control_payload_roundtrip() {
        let ack = build_ack_control_payload(55);
        let (kind, seq) = parse_control_payload(&ack).expect("parse ack payload");
        assert_eq!(kind, CONTROL_TYPE_ACK);
        assert_eq!(seq, 55);

        let ping = build_ping_control_payload();
        let (kind, seq) = parse_control_payload(&ping).expect("parse ping payload");
        assert_eq!(kind, CONTROL_TYPE_PING);
        assert_eq!(seq, 0);
    }
}
