use actix_web::{error, http::StatusCode, HttpResponse};
use serde::Serialize;
use std::{fmt, num::ParseFloatError};

#[derive(Debug, Serialize)]
pub enum CoralError {
    RedisError(String),
    ArrowError(String),
    DataFusionError(String),
    SqlParseError(String),
    ActixError(String),
    InternalError(String),
    NotFound(String),
    BadRequest(String),
}

#[derive(Debug, Serialize)]
pub struct CoralErrorResponse {
    error_message: String,
}

impl CoralError {
    fn error_response(&self) -> String {
        match self {
            CoralError::RedisError(msg) => {
                format!("SeahorseDB error: {}", msg)
            }
            CoralError::ArrowError(msg) => {
                format!("Arrow error: {}", msg)
            }
            CoralError::DataFusionError(msg) => {
                format!("DataFusion error: {}", msg)
            }
            CoralError::SqlParseError(msg) => {
                format!("SQL parse error: {}", msg)
            }
            CoralError::ActixError(msg) => {
                format!("Actix error: {}", msg)
            }
            CoralError::InternalError(msg) => {
                format!("Internal error: {}", msg)
            }
            CoralError::NotFound(msg) => {
                format!("Not found: {}", msg)
            }
            CoralError::BadRequest(msg) => {
                format!("Bad Request: {}", msg)
            }
        }
    }
}

impl error::ResponseError for CoralError {
    fn status_code(&self) -> StatusCode {
        match self {
            CoralError::RedisError(_)
            | CoralError::ArrowError(_)
            | CoralError::DataFusionError(_)
            | CoralError::SqlParseError(_)
            | CoralError::ActixError(_)
            | CoralError::InternalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            CoralError::NotFound(_) => StatusCode::NOT_FOUND,
            CoralError::BadRequest(_) => StatusCode::BAD_REQUEST,
        }
    }
    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code()).json(CoralErrorResponse {
            error_message: self.error_response(),
        })
    }
}

impl fmt::Display for CoralError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self)
    }
}

impl From<redis::RedisError> for CoralError {
    fn from(err: redis::RedisError) -> Self {
        CoralError::RedisError(err.to_string())
    }
}

impl From<arrow_schema::ArrowError> for CoralError {
    fn from(err: arrow_schema::ArrowError) -> Self {
        CoralError::ArrowError(err.to_string())
    }
}

impl From<datafusion::error::DataFusionError> for CoralError {
    fn from(err: datafusion::error::DataFusionError) -> Self {
        CoralError::DataFusionError(err.to_string())
    }
}

impl From<sqlparser::parser::ParserError> for CoralError {
    fn from(err: sqlparser::parser::ParserError) -> Self {
        CoralError::SqlParseError(err.to_string())
    }
}

impl From<actix_web::error::Error> for CoralError {
    fn from(err: actix_web::error::Error) -> Self {
        CoralError::ActixError(err.to_string())
    }
}

impl From<ParseFloatError> for CoralError {
    fn from(err: ParseFloatError) -> Self {
        CoralError::InternalError(err.to_string())
    }
}

impl From<base64::DecodeError> for CoralError {
    fn from(err: base64::DecodeError) -> Self {
        CoralError::InternalError(err.to_string())
    }
}

impl From<parquet::errors::ParquetError> for CoralError {
    fn from(err: parquet::errors::ParquetError) -> Self {
        CoralError::InternalError(err.to_string())
    }
}

impl From<deadpool_redis::PoolError> for CoralError {
    fn from(err: deadpool_redis::PoolError) -> Self {
        CoralError::InternalError(err.to_string())
    }
}

impl From<reqwest::Error> for CoralError {
    fn from(err: reqwest::Error) -> Self {
        CoralError::InternalError(err.to_string())
    }
}

impl From<std::io::Error> for CoralError {
    fn from(err: std::io::Error) -> Self {
        CoralError::InternalError(err.to_string())
    }
}

impl From<sonic_rs::Error> for CoralError {
    fn from(err: sonic_rs::Error) -> Self {
        CoralError::InternalError(err.to_string())
    }
}
