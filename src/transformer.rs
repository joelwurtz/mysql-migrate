use crate::value::MysqlValueDecoded;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use serde_yaml::Value;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Transformer {
    Replace(Value),
    JsonMerge(String),
    Nullify,
}

impl Transformer {
    pub(crate) fn transform(&self, value: MysqlValueDecoded) -> MysqlValueDecoded {
        match (self, value) {
            (Transformer::Replace(replacement), _) => match replacement {
                Value::Null => MysqlValueDecoded::Null,
                Value::Bool(replacement) => MysqlValueDecoded::Bool(*replacement),
                Value::Number(replacement) => MysqlValueDecoded::Int(replacement.as_i64().unwrap()),
                Value::String(replacement) => MysqlValueDecoded::String(replacement.clone()),
                // @TODO to json
                Value::Sequence(_) => MysqlValueDecoded::Null,
                Value::Mapping(_) => MysqlValueDecoded::Null,
                // @TODO to enum ?
                Value::Tagged(_) => MysqlValueDecoded::Null,
            },
            (Transformer::Nullify, _) => MysqlValueDecoded::Null,
            (Transformer::JsonMerge(json_merge), MysqlValueDecoded::String(json)) => {
                // decode json to merge
                let mut json_value: serde_json::Value = match serde_json::from_str(json.as_str()) {
                    Ok(value) => value,
                    Err(_) => {
                        tracing::warn!("failed to decode json {}", json);
                        return MysqlValueDecoded::String(json);
                    }
                };

                let merge_value: serde_json::Value = match serde_json::from_str(json_merge.as_str())
                {
                    Ok(value) => value,
                    Err(_) => {
                        tracing::warn!("failed to decode json {}", json_merge);
                        return MysqlValueDecoded::String(json);
                    }
                };

                json_patch(&mut json_value, merge_value);

                let json_string = match serde_json::to_string(&json_value) {
                    Ok(value) => value,
                    Err(_) => {
                        tracing::warn!("failed to encode json {}", json_value);
                        return MysqlValueDecoded::String(json);
                    }
                };

                MysqlValueDecoded::String(json_string)
            }
            (Transformer::JsonMerge(json_merge), MysqlValueDecoded::Bytes(json)) => {
                // decode json to merge
                let mut json_value: serde_json::Value =
                    match serde_json::from_slice(json.as_slice()) {
                        Ok(value) => value,
                        Err(e) => {
                            tracing::warn!("failed to decode json {}", e);
                            return MysqlValueDecoded::Bytes(json);
                        }
                    };

                let merge_value: serde_json::Value = match serde_json::from_str(json_merge.as_str())
                {
                    Ok(value) => value,
                    Err(e) => {
                        tracing::warn!("failed to decode json {}", e);
                        return MysqlValueDecoded::Bytes(json);
                    }
                };

                json_patch(&mut json_value, merge_value);

                let json_string = match serde_json::to_vec(&json_value) {
                    Ok(value) => value,
                    Err(e) => {
                        tracing::warn!("failed to encode json {}", e);
                        return MysqlValueDecoded::Bytes(json);
                    }
                };

                MysqlValueDecoded::Bytes(json_string)
            }
            (trans, value) => {
                tracing::warn!(
                    "transformer {:?} not supported for value {:?}",
                    trans,
                    value
                );

                value
            }
        }
    }
}

fn json_patch(a: &mut JsonValue, b: JsonValue) {
    match (a, b) {
        (a @ &mut JsonValue::Object(_), JsonValue::Object(b)) => {
            let a = a.as_object_mut().unwrap();
            for (k, v) in b {
                json_patch(a.entry(k).or_insert(JsonValue::Null), v);
            }
        }
        (a, b) => *a = b,
    }
}
