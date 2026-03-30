use std::path::PathBuf;

use tokio::net::{UnixListener, UnixStream};

use crate::error::Error;

pub(crate) async fn dial(path: &str) -> Result<UnixStream, Error> {
    let path = to_uds_path(path)?;
    Ok(UnixStream::connect(path).await?)
}

pub async fn listen(path: &str) -> Result<UnixListener, Error> {
    let path = to_uds_path(path)?;
    Ok(UnixListener::bind(path)?)
}

pub(crate) async fn accept_stream<L>(listener: L) -> Result<(UnixStream, Option<String>), Error>
where
    L: std::ops::Deref<Target = UnixListener>,
{
    let (stream, _) = listener.accept().await?;
    let peer_addr = stream.peer_addr().ok().map(|addr| format!("{addr:?}"));
    Ok((stream, peer_addr))
}

fn to_uds_path(path: &str) -> Result<PathBuf, Error> {
    if let Some(stripped) = path.strip_prefix("unix://") {
        return to_uds_path(stripped);
    }
    if let Some(stripped) = path.strip_prefix("uds://") {
        return to_uds_path(stripped);
    }
    if let Some(name) = path.strip_prefix('@') {
        return abstract_uds(name);
    }
    Ok(PathBuf::from(path))
}

#[cfg(target_os = "linux")]
fn abstract_uds(name: &str) -> Result<PathBuf, Error> {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    let mut bytes = Vec::with_capacity(name.len() + 1);
    bytes.push(0);
    bytes.extend_from_slice(name.as_bytes());
    Ok(PathBuf::from(OsString::from_vec(bytes)))
}

#[cfg(not(target_os = "linux"))]
fn abstract_uds(_name: &str) -> Result<PathBuf, Error> {
    Err(Error::Io(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "abstract UDS is only supported on linux",
    )))
}
