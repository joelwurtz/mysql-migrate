use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::transformer::Transformer;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct Config {
    pub(crate) source: DatabaseConfig,
    pub(crate) target: DatabaseConfig,
    #[serde(default = "default_false")]
    pub(crate) create_target_database: bool,
    #[serde(with = "serde_yaml::with::singleton_map_recursive")]
    #[serde(default)]
    pub(crate) migrate: MigrateConfig,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct DatabaseConfig {
    pub(crate) dsn: String,
    #[serde(default = "default_max_connections")]
    pub(crate) max_connections: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub(crate) struct MigrateConfig {
    pub(crate) tables: HashMap<String, MigrateTableConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct MigrateTableConfig {
    #[serde(default = "default_batch_size")]
    pub(crate) batch_size: usize,
    #[serde(default = "default_false")]
    pub(crate) skip_data: bool,
    #[serde(default)]
    pub(crate) outfile: Option<String>,
    #[serde(default)]
    pub(crate) transformers: HashMap<String, Transformer>,
}

fn default_batch_size() -> usize {
    1000
}

fn default_false() -> bool {
    false
}

fn default_max_connections() -> u32 {
    10
}

impl Default for MigrateTableConfig {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            skip_data: default_false(),
            transformers: HashMap::new(),
            outfile: None,
        }
    }
}