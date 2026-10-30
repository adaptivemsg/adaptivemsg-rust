use crate::error::Error;

use crate::protocol::{PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3};

pub const FRAME_HEADER_LEN_V2: usize = 10;
pub const FRAME_HEADER_LEN_V3: usize = 18;

/// Reserved stream for v3 control frames (ACK, PING, CLOSE).
pub(crate) const CONTROL_STREAM_ID: u32 = u32::MAX;

/// Frame header flag bits. Each bit is an independent property.
///
/// - bit 0 `FIN`: last frame for a non-zero stream, sent only by the side
///   that opened it
/// - bit 1 `META`: a length-prefixed metadata block follows the fixed header
/// - bits 2–6: unassigned
/// - bit 7 `EXT`: another flags byte follows the fixed header
pub const FRAME_FLAG_FIN: u8 = 1 << 0;
/// Reserved. A length-prefixed metadata block follows the fixed header.
#[allow(dead_code)]
pub const FRAME_FLAG_META: u8 = 1 << 1;
/// Reserved. Another flags byte follows the fixed header.
#[allow(dead_code)]
pub const FRAME_FLAG_EXT: u8 = 1 << 7;

/// Bits this runtime implements. Every other bit, including the reserved
/// `META` and `EXT`, must be zero until it is implemented and negotiated in
/// the handshake.
pub const SUPPORTED_FRAME_FLAGS: u8 = FRAME_FLAG_FIN;

pub fn build_header(
    version: u8,
    stream_id: u32,
    seq: u64,
    flags: u8,
    payload_len: usize,
    max_frame: u32,
) -> Result<[u8; FRAME_HEADER_LEN_V3], Error> {
    if payload_len > u32::MAX as usize {
        return Err(Error::FrameTooLarge(payload_len));
    }
    if payload_len as u32 > max_frame {
        return Err(Error::FrameTooLarge(payload_len));
    }
    let mut header = [0u8; FRAME_HEADER_LEN_V3];
    header[0] = version;
    header[1] = flags;
    header[2..6].copy_from_slice(&stream_id.to_be_bytes());
    header[6..10].copy_from_slice(&(payload_len as u32).to_be_bytes());
    if version == PROTOCOL_VERSION_V3 {
        header[10..18].copy_from_slice(&seq.to_be_bytes());
    }
    Ok(header)
}

pub fn frame_header_len_for_version(version: u8) -> Result<usize, Error> {
    match version {
        PROTOCOL_VERSION_V2 => Ok(FRAME_HEADER_LEN_V2),
        PROTOCOL_VERSION_V3 => Ok(FRAME_HEADER_LEN_V3),
        other => Err(Error::UnsupportedFrameVersion(other)),
    }
}

pub fn parse_header(header: &[u8], expected_version: u8) -> Result<(u32, u64, u8, usize), Error> {
    let version = header[0];
    if version != expected_version {
        return Err(Error::UnsupportedFrameVersion(version));
    }
    let flags = header[1];
    if flags & !SUPPORTED_FRAME_FLAGS != 0 {
        return Err(Error::InvalidMessage("unsupported frame flags".to_string()));
    }
    let expected_len = frame_header_len_for_version(version)?;
    if header.len() < expected_len {
        return Err(Error::InvalidMessage(
            "invalid frame header length".to_string(),
        ));
    }
    let stream_id = u32::from_be_bytes(header[2..6].try_into().unwrap());
    let len = u32::from_be_bytes(header[6..10].try_into().unwrap()) as usize;
    let seq = if version == PROTOCOL_VERSION_V3 {
        u64::from_be_bytes(header[10..18].try_into().unwrap())
    } else {
        0
    };
    Ok((stream_id, seq, flags, len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3};

    #[test]
    fn v2_header_roundtrip() {
        let header = build_header(PROTOCOL_VERSION_V2, 7, 0, FRAME_FLAG_FIN, 11, 1024).expect("build header");
        let (stream_id, seq, flags, len) =
            parse_header(&header, PROTOCOL_VERSION_V2).expect("parse header");
        assert_eq!(stream_id, 7);
        assert_eq!(seq, 0);
        assert_eq!(flags, FRAME_FLAG_FIN);
        assert_eq!(len, 11);
    }

    #[test]
    fn rejects_unsupported_flags() {
        for flags in [FRAME_FLAG_META, FRAME_FLAG_EXT, 1 << 2] {
            let header = build_header(PROTOCOL_VERSION_V2, 7, 0, flags | FRAME_FLAG_FIN, 0, 1024)
                .expect("build header");
            assert!(matches!(
                parse_header(&header, PROTOCOL_VERSION_V2),
                Err(Error::InvalidMessage(_))
            ));
        }
    }

    #[test]
    fn v3_header_roundtrip() {
        let header = build_header(PROTOCOL_VERSION_V3, 9, 42, 0, 13, 1024).expect("build header");
        let (stream_id, seq, flags, len) =
            parse_header(&header, PROTOCOL_VERSION_V3).expect("parse header");
        assert_eq!(stream_id, 9);
        assert_eq!(seq, 42);
        assert_eq!(flags, 0);
        assert_eq!(len, 13);
    }
}
