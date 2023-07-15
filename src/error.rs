use std::error::Error as StdError;

use crate::types::ColumnType;

/// Represents all possible errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[allow(missing_docs)]
pub enum Error {
    #[error("invalid params: {0}")]
    InvalidParams(#[source] Box<dyn StdError + Send + Sync>),
    #[error("network error: {0:?}")]
    Network(#[from] hyper::Error),
    #[error("invalid utf-8: {0}")]
    InvalidUnicode(#[from] std::string::FromUtf8Error),
    #[error("no rows returned by a query that expected to return at least one row")]
    RowNotFound,
    #[error("sequences must have a known size ahead of time")]
    SequenceMustHaveLength,
    #[error("`deserialize_any` is not supported")]
    NotEnoughData,
    #[error("tag for enum is not valid")]
    InvalidTagEncoding(u8),
    #[error("bad response: {0}")]
    BadResponse(String),
    #[error("Unsupported column type: {0}")]
    UnsupportedColumn(String),
    #[error("Column types mismatch: {schema:?} vs {row:?}")]
    WrongColumnTypes {
        schema: Vec<ColumnType>,
        row: Vec<ColumnType>,
    },
    #[error("Column names mismatch: {schema:?} vs {row:?}")]
    WrongColumnNames {
        schema: Vec<&'static str>,
        row: Box<[String]>,
    },
    #[error("Each column must have a name: {row:?}")]
    MissingColumnName { row: Vec<&'static str> },

    // Internally handled errors, not part of public API.
    // XXX: move to another error?
    #[error("internal error: too small buffer, need another {0} bytes")]
    #[doc(hidden)]
    TooSmallBuffer(usize),
}

impl Error {
    pub async fn from_bad_response(response: hyper::Response<hyper::Body>) -> Self {
        let status = response.status();
        let raw_bytes = match hyper::body::to_bytes(response.into_body()).await {
            Ok(bytes) => bytes,
            Err(err) => return err.into(),
        };
        let reason = String::from_utf8(raw_bytes.into())
            .map(|reason| reason.trim().into())
            .unwrap_or_else(|_| {
                // If we have a unreadable response, return standardised reason for the status code.
                format!(
                    "{} {}",
                    status.as_str(),
                    status.canonical_reason().unwrap_or("<unknown>"),
                )
            });

        Error::BadResponse(reason)
    }
}
