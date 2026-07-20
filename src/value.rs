use sqlx::mysql::MySqlValue;
use sqlx::types::Decimal;
use sqlx::{TypeInfo, Value};
use std::fmt;

#[derive(Debug)]
pub(crate) enum MysqlValueDecoded {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Double(f64),
    Decimal(Decimal),
    String(String),
    DateTime(chrono::NaiveDateTime),
    Bytes(Vec<u8>),
}

#[derive(Debug)]
pub enum ValueError {
    DecodeError(sqlx::Error),
}

impl From<sqlx::Error> for ValueError {
    fn from(err: sqlx::Error) -> Self {
        ValueError::DecodeError(err)
    }
}

impl fmt::Display for ValueError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ValueError::DecodeError(err) => write!(f, "Decode error: {}", err),
        }
    }
}

/// Values are fetched over the MySQL text protocol, so anything can be read back as text.
/// Fallback to raw bytes when the content is not valid UTF-8.
fn decode_text_or_bytes(value: &MySqlValue) -> Result<MysqlValueDecoded, ValueError> {
    match value.try_decode_unchecked::<String>() {
        Ok(s) => Ok(MysqlValueDecoded::String(s)),
        Err(_) => Ok(MysqlValueDecoded::Bytes(
            value.try_decode_unchecked::<Vec<u8>>()?,
        )),
    }
}

impl TryFrom<MySqlValue> for MysqlValueDecoded {
    type Error = ValueError;

    fn try_from(value: MySqlValue) -> Result<Self, Self::Error> {
        if value.is_null() {
            return Ok(MysqlValueDecoded::Null);
        }

        let type_info = value.type_info();

        Ok(match type_info.name() {
            "NULL" => MysqlValueDecoded::Null,
            "BOOLEAN" => MysqlValueDecoded::Bool(value.try_decode::<bool>()?),
            "TINYINT" => MysqlValueDecoded::Int(value.try_decode::<i8>()? as i64),
            "SMALLINT" => MysqlValueDecoded::Int(value.try_decode::<i16>()? as i64),
            "MEDIUMINT" | "INT" => MysqlValueDecoded::Int(value.try_decode::<i32>()? as i64),
            "BIGINT" => MysqlValueDecoded::Int(value.try_decode::<i64>()?),
            "TINYINT UNSIGNED" => MysqlValueDecoded::UInt(value.try_decode::<u8>()? as u64),
            "SMALLINT UNSIGNED" => MysqlValueDecoded::UInt(value.try_decode::<u16>()? as u64),
            "MEDIUMINT UNSIGNED" | "INT UNSIGNED" => {
                MysqlValueDecoded::UInt(value.try_decode::<u32>()? as u64)
            }
            "BIGINT UNSIGNED" => MysqlValueDecoded::UInt(value.try_decode::<u64>()?),
            "FLOAT" | "DOUBLE" => MysqlValueDecoded::Double(value.try_decode::<f64>()?),
            "DECIMAL" => MysqlValueDecoded::Decimal(value.try_decode::<Decimal>()?),
            // NaiveDateTime, not DateTime<Utc>: the latter binds as a TIMESTAMP parameter,
            // and the server nullifies TIMESTAMP values outside the 1970-2038 epoch range.
            // Unchecked because NaiveDateTime's checked decode rejects TIMESTAMP columns.
            "TIMESTAMP" | "DATETIME" => {
                MysqlValueDecoded::DateTime(value.try_decode_unchecked::<chrono::NaiveDateTime>()?)
            }

            // Binary data, never valid text: go straight to bytes.
            "BINARY" | "VARBINARY" | "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" | "BIT"
            | "GEOMETRY" => MysqlValueDecoded::Bytes(value.try_decode_unchecked::<Vec<u8>>()?),

            // Text, plus the types without a dedicated variant that round trip fine as text:
            // DATE/TIME/YEAR keep their MySQL literal form, JSON keeps its serialized form.
            "CHAR" | "VARCHAR" | "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" | "ENUM"
            | "SET" | "JSON" | "DATE" | "TIME" | "YEAR" => decode_text_or_bytes(&value)?,

            // Never abort a whole table because of an unknown type.
            name => {
                tracing::warn!("unhandled MySQL type {}, migrating it as text/bytes", name);
                decode_text_or_bytes(&value)?
            }
        })
    }
}
