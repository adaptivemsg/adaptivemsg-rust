use crate::error::Error;

use crate::protocol::{PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3};

pub const FRAME_HEADER_LEN_V2: usize = 10;
pub const FRAME_HEADER_LEN_V3: usize = 18;

pub fn build_header(
    version: u8,
    stream_id: u32,
    seq: u64,
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
    header[1] = 0;
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

pub fn parse_header(header: &[u8], expected_version: u8) -> Result<(u32, u64, usize), Error> {
    let version = header[0];
    if version != expected_version {
        return Err(Error::UnsupportedFrameVersion(version));
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
    Ok((stream_id, seq, len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3};

    #[test]
    fn v2_header_roundtrip() {
        let header = build_header(PROTOCOL_VERSION_V2, 7, 0, 11, 1024).expect("build header");
        let (stream_id, seq, len) =
            parse_header(&header, PROTOCOL_VERSION_V2).expect("parse header");
        assert_eq!(stream_id, 7);
        assert_eq!(seq, 0);
        assert_eq!(len, 11);
    }

    #[test]
    fn v3_header_roundtrip() {
        let header = build_header(PROTOCOL_VERSION_V3, 9, 42, 13, 1024).expect("build header");
        let (stream_id, seq, len) =
            parse_header(&header, PROTOCOL_VERSION_V3).expect("parse header");
        assert_eq!(stream_id, 9);
        assert_eq!(seq, 42);
        assert_eq!(len, 13);
    }
}
