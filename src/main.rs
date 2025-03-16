mod config;
mod extractor;
mod query;
mod transformer;
mod value;

use clap::{Parser};
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
use sqlx::{ConnectOptions, Executor};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use crate::config::{Config};
use sqlx::Row;
use tracing::Level;
use tracing::log::LevelFilter;

#[derive(Parser)]
pub struct Args {
    config: PathBuf,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let args = Args::parse();
    let config: Config =
        serde_yaml::from_reader(std::fs::File::open(args.config).unwrap()).unwrap();

    let source_connect_options = MySqlConnectOptions::from_str(config.source.dsn.as_str())
        .unwrap()
        .log_statements(LevelFilter::Trace);
    let target_connect_options = MySqlConnectOptions::from_str(config.target.dsn.as_str())
        .unwrap()
        .disable_statement_logging();

    let source_pool = match MySqlPoolOptions::new()
        .max_connections(config.source.max_connections)
        .connect_with(source_connect_options)
        .await
    {
        Ok(pool) => Arc::new(pool),
        Err(e) => {
            tracing::error!("failed to connect to source database: {}", e);

            return;
        }
    };

    let target_pool = match MySqlPoolOptions::new()
        .max_connections(config.target.max_connections)
        .acquire_timeout(Duration::from_secs(600))
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                // disable foreign key check
                conn.execute("SET FOREIGN_KEY_CHECKS=0").await?;

                Ok(())
            })
        })
        .connect_with(target_connect_options)
        .await
    {
        Ok(pool) => Arc::new(pool),
        Err(e) => {
            tracing::error!("failed to connect to target database: {}", e);

            return;
        }
    };

    // select tables
    let tables = sqlx::query("SHOW TABLES")
        .fetch_all(source_pool.as_ref())
        .await
        .unwrap();

    let mut handles = Vec::new();

    for table in tables {
        let name = table.try_get::<&str, usize>(0).unwrap().to_string();
        let migrate_config = config
            .migrate
            .tables
            .get(name.as_str())
            .cloned()
            .unwrap_or_default();
        let source_pool = source_pool.clone();
        let target_pool = target_pool.clone();

        let handle = tokio::task::spawn(async move {
            tracing::info!("migrating table: {}", name);

            let mut exporter = extractor::TableExtractor::new(
                source_pool,
                target_pool,
                migrate_config,
                name.clone(),
            );

            match exporter.extract().await {
                Ok(_) => {
                    tracing::info!("table backup completed {}", name);
                }
                Err(err) => {
                    tracing::error!("failed to backup table {}: {:?}", name, err);
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}
