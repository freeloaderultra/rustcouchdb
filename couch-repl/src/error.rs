use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("HTTP {status} from {url}: {body}")]
    Http {
        status: u16,
        url: String,
        body: String,
    },

    #[error("network error: {0}")]
    Net(#[from] reqwest::Error),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("invalid URL: {0}")]
    Url(String),

    #[error("document {id} failed permanently: {reason}")]
    Doc { id: String, reason: String },

    #[error("replication canceled")]
    Canceled,

    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

impl Error {
    /// Whether a request that produced this error may be retried.
    pub fn retryable(&self) -> bool {
        match self {
            Error::Http { status, .. } => {
                matches!(status, 408 | 429 | 500 | 502 | 503 | 504)
            }
            Error::Net(e) => {
                // Anything transport-level: connect failures, timeouts, resets,
                // bodies cut off mid-stream.
                e.is_timeout() || e.is_connect() || e.is_request() || e.is_body() || e.is_decode()
            }
            _ => false,
        }
    }

    pub fn status(&self) -> Option<u16> {
        match self {
            Error::Http { status, .. } => Some(*status),
            _ => None,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
