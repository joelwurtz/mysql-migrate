use chrono::Utc;
use sqlx::mysql::MySqlValue;
use sqlx::types::Decimal;
use sqlx::{TypeInfo, Value};
use std::fmt;

pub(crate) enum MysqlValueDecoded {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Double(f64),
    Decimal(Decimal),
    String(String),
    DateTime(chrono::DateTime<Utc>),
    Bytes(Vec<u8>),
}

#[derive(Debug)]
pub enum ValueError {
    InvalidType(String),
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
            ValueError::InvalidType(name) => write!(f, "Invalid type: {}", name),
            ValueError::DecodeError(err) => write!(f, "Decode error: {}", err),
        }
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
            "BOOLEAN" => MysqlValueDecoded::Bool(value.try_decode::<bool>()?),
            "TINYINT" => MysqlValueDecoded::Int(value.try_decode::<i8>()? as i64),
            "SMALLINT" => MysqlValueDecoded::Int(value.try_decode::<i16>()? as i64),
            "INT" => MysqlValueDecoded::Int(value.try_decode::<i32>()? as i64),
            "BIGINT" => MysqlValueDecoded::Int(value.try_decode::<i64>()?),
            "FLOAT" | "DOUBLE" => MysqlValueDecoded::Double(value.try_decode::<f64>()?),
            "VARCHAR" | "TEXT" | "CHAR" => MysqlValueDecoded::String(value.try_decode::<String>()?),
            "DECIMAL" => MysqlValueDecoded::Decimal(value.try_decode::<Decimal>()?),
            "INT UNSIGNED" => MysqlValueDecoded::UInt(value.try_decode::<u32>()? as u64),
            "TIMESTAMP" | "DATETIME" => {
                MysqlValueDecoded::DateTime(value.try_decode::<chrono::DateTime<Utc>>()?)
            }
            "BLOB" => MysqlValueDecoded::Bytes(value.try_decode::<Vec<u8>>()?),
            "ENUM" => MysqlValueDecoded::String(value.try_decode::<String>()?),
            name => Err(ValueError::InvalidType(name.to_string()))?,
        })
    }
}
