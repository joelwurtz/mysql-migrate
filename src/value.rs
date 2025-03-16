use chrono::Utc;
use sqlx::mysql::MySqlValue;
use sqlx::{TypeInfo, Value};

pub(crate) enum MysqlValueDecoded {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    String(String),
    DateTime(chrono::DateTime<Utc>),
    Bytes(Vec<u8>),
}

impl TryFrom<MySqlValue> for MysqlValueDecoded {
    type Error = sqlx::Error;

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
            "FLOAT" => MysqlValueDecoded::Int(value.try_decode::<f32>()? as i64),
            "VARCHAR" | "TEXT" => MysqlValueDecoded::String(value.try_decode::<String>()?),
            "INT UNSIGNED" => MysqlValueDecoded::UInt(value.try_decode::<u32>()? as u64),
            "TIMESTAMP" | "DATETIME" => MysqlValueDecoded::DateTime(value.try_decode::<chrono::DateTime<Utc>>()?),
            "BLOB" => MysqlValueDecoded::Bytes(value.try_decode::<Vec<u8>>()?),
            "ENUM" => MysqlValueDecoded::String(value.try_decode::<String>()?),
            name => panic!("name {} not supported", name),
        })
    }
}