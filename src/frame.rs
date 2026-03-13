use crate::error::Error;

pub const FRAME_HEADER_LEN: usize = 10;

pub fn build_header(
    version: u8,
    stream_id: u32,
    payload_len: usize,
    max_frame: u32,
) -> Result<[u8; FRAME_HEADER_LEN], Error> {
    if payload_len > u32::MAX as usize {
        return Err(Error::FrameTooLarge(payload_len));
    }
    if payload_len as u32 > max_frame {
        return Err(Error::FrameTooLarge(payload_len));
    }
    let mut header = [0u8; FRAME_HEADER_LEN];
    header[0] = version;
    header[1] = 0;
    header[2..6].copy_from_slice(&stream_id.to_be_bytes());
    header[6..10].copy_from_slice(&(payload_len as u32).to_be_bytes());
    Ok(header)
}

pub fn parse_header(
    header: [u8; FRAME_HEADER_LEN],
    expected_version: u8,
) -> Result<(u32, usize), Error> {
    let version = header[0];
    if version != expected_version {
        return Err(Error::UnsupportedFrameVersion(version));
    }
    let stream_id = u32::from_be_bytes(header[2..6].try_into().unwrap());
    let len = u32::from_be_bytes(header[6..10].try_into().unwrap()) as usize;
    Ok((stream_id, len))
}
