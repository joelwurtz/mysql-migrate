mod table;

use crate::value::ValueError;

#[derive(Debug)]
pub enum ExtractorError {
    Sqlx(sqlx::Error),
    ValueError(ValueError),
}

impl From<sqlx::Error> for ExtractorError {
    fn from(err: sqlx::Error) -> Self {
        ExtractorError::Sqlx(err)
    }
}

impl From<ValueError> for ExtractorError {
    fn from(err: ValueError) -> Self {
        ExtractorError::ValueError(err)
    }
}

impl std::fmt::Display for ExtractorError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ExtractorError::Sqlx(err) => write!(f, "Sqlx error: {}", err),
            ExtractorError::ValueError(err) => write!(f, "Value error: {}", err),
        }
    }
}

pub use table::TableExtractor;
