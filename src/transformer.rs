use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use crate::value::MysqlValueDecoded;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Transformer {
    Replace(Value),
    Nullify,
}

impl Transformer {
    pub(crate) fn transform(&self, _value: MysqlValueDecoded) -> MysqlValueDecoded {
        match self {
            Transformer::Replace(replacement) => match replacement {
                Value::Null => MysqlValueDecoded::Null,
                Value::Bool(replacement) => MysqlValueDecoded::Bool(replacement.clone()),
                Value::Number(replacement) => MysqlValueDecoded::Int(replacement.as_i64().unwrap()),
                Value::String(replacement) => MysqlValueDecoded::String(replacement.clone()),
                // @TODO to json
                Value::Sequence(_) => MysqlValueDecoded::Null,
                Value::Mapping(_) => MysqlValueDecoded::Null,
                // @TODO to enum ?
                Value::Tagged(_) => MysqlValueDecoded::Null,
            },
            Transformer::Nullify => MysqlValueDecoded::Null,
        }
    }
}