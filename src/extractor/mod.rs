mod table;

#[derive(Debug)]
pub enum ExtractorError {
    Sqlx(sqlx::Error),
}

impl From<sqlx::Error> for ExtractorError {
    fn from(err: sqlx::Error) -> Self {
        ExtractorError::Sqlx(err)
    }
}

impl std::fmt::Display for ExtractorError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ExtractorError::Sqlx(err) => write!(f, "Sqlx error: {}", err),
        }
    }
}

pub use table::TableExtractor;
