use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::codec::CodecID;
use crate::codec_registry::codec_by_id;
use crate::error::Error;

pub const PROTOCOL_VERSION: u8 = 2;
pub const HANDSHAKE_HEADER_LEN: usize = 12;
pub const MAX_CODEC_COUNT: usize = 16;
pub const DEFAULT_MAX_FRAME: u32 = u32::MAX;

pub const HANDSHAKE_MAGIC: [u8; 2] = *b"AM";

#[derive(Clone, Copy, Debug)]
pub struct HandshakeConfig {
    pub version: u8,
    pub codec_id: CodecID,
    pub max_frame: u32,
}

pub async fn handshake_client<R, W>(
    reader: &mut R,
    writer: &mut W,
    codecs: &[CodecID],
    max_frame: u32,
) -> Result<HandshakeConfig, Error>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    validate_codec_list(codecs)?;

    let mut request = [0u8; HANDSHAKE_HEADER_LEN];
    request[0..2].copy_from_slice(&HANDSHAKE_MAGIC);
    request[2] = PROTOCOL_VERSION;
    request[3] = codecs.len() as u8;
    request[4] = 0;
    request[5] = 0;
    request[6] = 0;
    request[7] = 0;
    request[8..12].copy_from_slice(&max_frame.to_be_bytes());
    writer.write_all(&request).await?;

    let mut list = vec![0u8; codecs.len()];
    for (idx, codec_id) in codecs.iter().enumerate() {
        list[idx] = codec_id.0;
    }
    writer.write_all(&list).await?;
    writer.flush().await?;

    let mut response = [0u8; HANDSHAKE_HEADER_LEN];
    reader.read_exact(&mut response).await?;
    if response[0..2] != HANDSHAKE_MAGIC {
        return Err(Error::BadHandshakeMagic);
    }
    let accept = response[2];
    let version = response[3];
    let selected = CodecID(response[4]);
    let server_max = u32::from_be_bytes([response[8], response[9], response[10], response[11]]);
    if version != PROTOCOL_VERSION {
        return Err(Error::UnsupportedFrameVersion(version));
    }
    if accept == 0 {
        return Err(Error::NoCommonCodec);
    }
    if !contains_codec(codecs, selected) {
        return Err(Error::NoCommonCodec);
    }
    if codec_by_id(selected).is_none() {
        return Err(Error::UnsupportedCodec(selected.0));
    }
    Ok(HandshakeConfig {
        version,
        codec_id: selected,
        max_frame: server_max,
    })
}

pub async fn handshake_server<R, W>(
    reader: &mut R,
    writer: &mut W,
    codecs: &[CodecID],
    max_frame: u32,
) -> Result<HandshakeConfig, Error>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    validate_codec_list(codecs)?;

    let mut request = [0u8; HANDSHAKE_HEADER_LEN];
    reader.read_exact(&mut request).await?;
    if request[0..2] != HANDSHAKE_MAGIC {
        return Err(Error::BadHandshakeMagic);
    }
    let version = request[2];
    let codec_count = request[3] as usize;
    let client_max = u32::from_be_bytes([request[8], request[9], request[10], request[11]]);
    if version != PROTOCOL_VERSION {
        let _ = write_handshake_reply(writer, 0, PROTOCOL_VERSION, CodecID(0), 0).await;
        return Err(Error::UnsupportedFrameVersion(version));
    }
    if codec_count == 0 {
        let _ = write_handshake_reply(writer, 0, PROTOCOL_VERSION, CodecID(0), 0).await;
        return Err(Error::NoCommonCodec);
    }
    if codec_count > MAX_CODEC_COUNT {
        let _ = write_handshake_reply(writer, 0, PROTOCOL_VERSION, CodecID(0), 0).await;
        return Err(Error::TooManyCodecs(codec_count));
    }
    let mut client_codecs = vec![0u8; codec_count];
    reader.read_exact(&mut client_codecs).await?;
    let selected = match select_codec(&client_codecs, codecs) {
        Some(codec) => codec,
        None => {
            let _ = write_handshake_reply(writer, 0, PROTOCOL_VERSION, CodecID(0), 0).await;
            return Err(Error::NoCommonCodec);
        }
    };
    let negotiated_max = negotiate_max_frame(client_max, max_frame);
    write_handshake_reply(writer, 1, PROTOCOL_VERSION, selected, negotiated_max).await?;
    if codec_by_id(selected).is_none() {
        return Err(Error::UnsupportedCodec(selected.0));
    }
    Ok(HandshakeConfig {
        version: PROTOCOL_VERSION,
        codec_id: selected,
        max_frame: negotiated_max,
    })
}

fn validate_codec_list(codecs: &[CodecID]) -> Result<(), Error> {
    if codecs.is_empty() {
        return Err(Error::InvalidMessage("codec list must be non-empty".to_string()));
    }
    if codecs.len() > MAX_CODEC_COUNT {
        return Err(Error::TooManyCodecs(codecs.len()));
    }
    for codec in codecs {
        if codec.0 == 0 {
            return Err(Error::InvalidMessage("codec ID must be non-zero".to_string()));
        }
        if codec_by_id(*codec).is_none() {
            return Err(Error::UnsupportedCodec(codec.0));
        }
    }
    Ok(())
}

fn negotiate_max_frame(client_max: u32, server_max: u32) -> u32 {
    if client_max == 0 {
        return 0;
    }
    if client_max < server_max {
        client_max
    } else {
        server_max
    }
}

async fn write_handshake_reply<W>(
    writer: &mut W,
    accept: u8,
    version: u8,
    codec_id: CodecID,
    max_frame: u32,
) -> Result<(), Error>
where
    W: AsyncWrite + Unpin,
{
    let mut response = [0u8; HANDSHAKE_HEADER_LEN];
    response[0..2].copy_from_slice(&HANDSHAKE_MAGIC);
    response[2] = accept;
    response[3] = version;
    response[4] = codec_id.0;
    response[5] = 0;
    response[6] = 0;
    response[7] = 0;
    response[8..12].copy_from_slice(&max_frame.to_be_bytes());
    writer.write_all(&response).await?;
    writer.flush().await?;
    Ok(())
}

fn contains_codec(codecs: &[CodecID], id: CodecID) -> bool {
    codecs.iter().any(|codec| *codec == id)
}

fn select_codec(client_codecs: &[u8], supported: &[CodecID]) -> Option<CodecID> {
    for raw in client_codecs {
        for sup in supported {
            if *raw == sup.0 {
                return Some(*sup);
            }
        }
    }
    None
}
