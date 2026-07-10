use std::fmt;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    /// Structural problem in the file: bad ETF, bad checksum, bad chunk, no header.
    Corrupt(String),
    /// The file is valid but uses a feature we don't support (e.g. zstd compression).
    Unsupported(String),
    /// Caller error: unknown doc id, bad rev, invalid input document.
    BadRequest(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "io error: {e}"),
            Error::Corrupt(m) => write!(f, "file corruption: {m}"),
            Error::Unsupported(m) => write!(f, "unsupported: {m}"),
            Error::BadRequest(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

pub fn corrupt(msg: impl Into<String>) -> Error {
    Error::Corrupt(msg.into())
}
