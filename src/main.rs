mod config;
mod extractor;
mod transformer;
mod value;

use crate::config::{Config, CreateConfig, DatabaseConfig};
use clap::Parser;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
use sqlx::{AssertSqlSafe, Row};
use sqlx::{ConnectOptions, Executor};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tracing_indicatif::IndicatifLayer;
use tracing_subscriber::Layer;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Parser)]
pub struct Args {
    config: PathBuf,
    /// Enable debug logging (shows trace level logs)
    #[clap(short, long)]
    debug: bool,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let config: Config =
        serde_yaml::from_reader(std::fs::File::open(args.config).unwrap()).unwrap();

    let indicatif_layer = IndicatifLayer::new();

    // Set log level based on debug flag
    let log_level = if args.debug {
        LevelFilter::TRACE
    } else {
        LevelFilter::ERROR
    };

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(indicatif_layer.get_stderr_writer())
                .with_filter(log_level),
        )
        .with(indicatif_layer)
        .init();

    let source_connect_options = MySqlConnectOptions::from_str(config.source.dsn.as_str())
        .unwrap()
        .disable_statement_logging();
    let target_connect_options = MySqlConnectOptions::from_str(config.target.dsn.as_str())
        .unwrap()
        .disable_statement_logging();

    let source_pool = match MySqlPoolOptions::new()
        .max_connections(config.source.max_connections)
        .test_before_acquire(true)
        .connect_with(source_connect_options)
        .await
    {
        Ok(pool) => Arc::new(pool),
        Err(e) => {
            tracing::error!("failed to connect to source database: {}", e);

            return;
        }
    };

    // get source database charset
    // SELECT default_character_set_name FROM information_schema.SCHEMATA
    // WHERE schema_name = "mydatabasename";
    let charset_query = "SELECT default_character_set_name FROM information_schema.SCHEMATA WHERE schema_name = DATABASE()";
    let charset_row = sqlx::query(charset_query)
        .fetch_one(source_pool.as_ref())
        .await
        .unwrap();
    let charset = charset_row.get::<&str, usize>(0).to_string();

    prepare_target_database(&config.target, &config.create, charset).await;

    let target_pool = match MySqlPoolOptions::new()
        .max_connections(config.target.max_connections)
        .acquire_timeout(Duration::from_secs(600))
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                // disable foreign key check
                conn.execute("SET FOREIGN_KEY_CHECKS=0").await?;
                conn.execute("SET UNIQUE_CHECKS=0").await?;

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

    let multi_progress = MultiProgress::new();
    let sty = ProgressStyle::with_template(
        "[{elapsed_precise}] {bar:40} {eta_precise} {msg} {pos}/{len}",
    )
    .unwrap();

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
        let count = sqlx::query(AssertSqlSafe(format!("SELECT COUNT(*) FROM `{}`", name)))
            .fetch_one(source_pool.as_ref())
            .await
            .unwrap()
            .get::<i64, usize>(0);
        let progress_bar = multi_progress.add(ProgressBar::new(count as u64));
        progress_bar.set_style(sty.clone());

        let handle = tokio::task::spawn(async move {
            let mut exporter = extractor::TableExtractor::new(
                source_pool,
                target_pool,
                migrate_config,
                name.clone(),
            );

            match exporter.extract(&progress_bar).await {
                Ok(_) => (),
                Err(err) => {
                    progress_bar
                        .abandon_with_message(format!("table {} backup failed: {:?}", name, err));
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

async fn prepare_target_database(target: &DatabaseConfig, create: &CreateConfig, charset: String) {
    let mut target_connect_options = MySqlConnectOptions::from_str(target.dsn.as_str())
        .unwrap()
        .disable_statement_logging();

    let database_name = target_connect_options.get_database().unwrap().to_string();
    target_connect_options = target_connect_options.database("");

    let target_pool = match MySqlPoolOptions::new()
        .acquire_timeout(Duration::from_secs(600))
        .connect_with(target_connect_options)
        .await
    {
        Ok(pool) => Arc::new(pool),
        Err(e) => {
            tracing::error!("failed to connect to target database for prepare: {}", e);

            return;
        }
    };

    if create.drop_if_exists {
        let drop_query = format!("DROP DATABASE IF EXISTS `{}`", database_name);
        sqlx::query(AssertSqlSafe(drop_query))
            .execute(target_pool.as_ref())
            .await
            .unwrap();
    } else {
        // check if database exists and return if it does
        let check_query = format!(
            "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '{}'",
            database_name
        );

        let exists_row = sqlx::query(AssertSqlSafe(check_query))
            .fetch_optional(target_pool.as_ref())
            .await
            .unwrap();

        if exists_row.is_some() {
            tracing::info!(
                "database {} already exists, skipping creation",
                database_name
            );
            return;
        }
    }

    let create_query = format!(
        "CREATE DATABASE `{}` DEFAULT CHARACTER SET {}",
        database_name, charset
    );

    sqlx::query(AssertSqlSafe(create_query))
        .execute(target_pool.as_ref())
        .await
        .unwrap();
}
