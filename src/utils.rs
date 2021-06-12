//! Useful utilities

use arrow::error::ArrowError;
use mongodb::error::Error;

pub(crate) fn mongo_to_arrow_error(error: Error) -> ArrowError {
    ArrowError::ExternalError(Box::new(error))
}
