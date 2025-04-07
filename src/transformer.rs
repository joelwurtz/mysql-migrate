use crate::value::MysqlValueDecoded;
use json_patch::{Patch, patch as json_patch};
use serde::{Deserialize, Serialize};
use serde_yaml::Value;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Transformer {
    Replace(Value),
    JsonPatch(Patch),
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
            (Transformer::JsonPatch(patch), MysqlValueDecoded::String(json)) => {
                // decode json to merge
                let mut json_value: serde_json::Value = match serde_json::from_str(json.as_str()) {
                    Ok(value) => value,
                    Err(_) => {
                        tracing::warn!("failed to decode json {}", json);
                        return MysqlValueDecoded::String(json);
                    }
                };

                if let Err(e) = json_patch(&mut json_value, patch) {
                    tracing::warn!("failed to apply json patch {}", e);

                    return MysqlValueDecoded::String(json);
                }

                let json_string = match serde_json::to_string(&json_value) {
                    Ok(value) => value,
                    Err(_) => {
                        tracing::warn!("failed to encode json {}", json_value);
                        return MysqlValueDecoded::String(json);
                    }
                };

                MysqlValueDecoded::String(json_string)
            }
            (Transformer::JsonPatch(patch), MysqlValueDecoded::Bytes(json)) => {
                // decode json to merge
                let mut json_value: serde_json::Value =
                    match serde_json::from_slice(json.as_slice()) {
                        Ok(value) => value,
                        Err(e) => {
                            tracing::warn!("failed to decode json {}", e);
                            return MysqlValueDecoded::Bytes(json);
                        }
                    };

                if let Err(e) = json_patch(&mut json_value, patch) {
                    tracing::warn!("failed to apply json patch {}", e);

                    return MysqlValueDecoded::Bytes(json);
                }

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
