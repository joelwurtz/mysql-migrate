mod config;
mod extractor;
mod transformer;
mod value;

use crate::config::{Config, CreateConfig, DatabaseConfig};
use clap::Parser;
use indicatif::{FormattedDuration, MultiProgress, ProgressBar, ProgressStyle};
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
use sqlx::{AssertSqlSafe, Row};
use sqlx::{ConnectOptions, Executor};
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
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

/// Shared view of the migration used to derive the overall remaining time. indicatif alone
/// cannot do it: the summary bar advances in whole tables, so once only a few large tables
/// are left its position stalls and the extrapolated ETA explodes.
struct MigrationProgress {
    total: usize,
    completed: AtomicUsize,
    start: Instant,
    /// Bars of the tables currently running, keyed by table name.
    in_flight: Mutex<HashMap<String, ProgressBar>>,
}

impl MigrationProgress {
    fn new(total: usize) -> Self {
        Self {
            total,
            completed: AtomicUsize::new(0),
            start: Instant::now(),
            in_flight: Mutex::new(HashMap::new()),
        }
    }

    /// Overall remaining time, or None while nothing is measurable yet.
    ///
    /// When the running tables are the last ones, the migration ends when the slowest of them
    /// does, so their largest row-based ETA is the answer. Otherwise each table counts as one
    /// unit of work (running ones as their completed fraction) and the elapsed time is
    /// extrapolated linearly, floored by that same largest ETA since the longest running table
    /// bounds any schedule.
    fn remaining(&self) -> Option<Duration> {
        let done = self.completed.load(Ordering::SeqCst);
        if done >= self.total {
            return Some(Duration::ZERO);
        }

        let bars: Vec<ProgressBar> = self.in_flight.lock().unwrap().values().cloned().collect();

        // Tables whose COUNT(*) has not landed yet have no length: their fraction is 0 and
        // their ETA is meaningless, so they only weigh in as "not started".
        let longest_eta = bars
            .iter()
            .filter(|bar| bar.length().unwrap_or(0) > 0)
            .map(|bar| bar.eta())
            .max();

        let queued = self.total.saturating_sub(done + bars.len());
        if queued == 0
            && let Some(eta) = longest_eta
        {
            return Some(eta);
        }

        let effective: f64 = done as f64
            + bars
                .iter()
                .map(|bar| {
                    let len = bar.length().unwrap_or(0);
                    if len == 0 {
                        0.0
                    } else {
                        // Clamped because indicatif allows the position to run past the length:
                        // a bar is sized from SELECT COUNT(*) while its rows come from
                        // select_query, which may well yield more of them.
                        (bar.position() as f64 / len as f64).min(1.0)
                    }
                })
                .sum::<f64>();
        if effective < 1e-3 {
            return None;
        }

        // Duration::mul_f64 panics on a negative factor, hence the floor at zero.
        let remaining = self
            .start
            .elapsed()
            .mul_f64(((self.total as f64 - effective) / effective).max(0.0));

        Some(remaining.max(longest_eta.unwrap_or(Duration::ZERO)))
    }
}

/// Count a table as done, whether it succeeded or not, and refresh the summary line.
fn finish_table(summary_bar: &ProgressBar, progress: &MigrationProgress, name: &str) {
    progress.in_flight.lock().unwrap().remove(name);
    let done = progress.completed.fetch_add(1, Ordering::SeqCst) + 1;

    summary_bar.set_position(done as u64);
    summary_bar.set_message(format!(
        "{} done / {} remaining / {} tables",
        done,
        progress.total - done,
        progress.total
    ));
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
        // sqlx defaults to 30s, which a table queued behind a long running one blows through
        // easily. Table concurrency is capped to the pool size below, so reaching this timeout
        // now means something is genuinely stuck rather than merely busy.
        .acquire_timeout(Duration::from_secs(600))
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

                // We copy data, we do not validate it: the target must accept whatever the
                // source holds, including values a strict target would reject (zero dates
                // like '0000-00-00', out of range dates, over long strings). Under
                // STRICT_TRANS_TABLES such a value is turned into NULL, which then trips
                // the NOT NULL constraint and fails the whole batch.
                // NO_AUTO_VALUE_ON_ZERO is kept so an explicit 0 in an AUTO_INCREMENT
                // column stays 0 instead of being reassigned a fresh id, and
                // ALLOW_INVALID_DATES keeps dates such as '2024-02-31' as they are.
                // This mirrors what mysqldump writes at the top of a dump.
                conn.execute("SET SESSION sql_mode='NO_AUTO_VALUE_ON_ZERO,ALLOW_INVALID_DATES'")
                    .await?;

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

    // A table holds one source connection for as long as it streams its rows, so running more
    // tables at once than the source pool has connections means the surplus ones sit in the
    // acquire queue for the whole duration of the tables ahead of them. Gate the tables here
    // instead: they queue as tasks, and only start once a connection is actually free.
    let table_slots = Arc::new(tokio::sync::Semaphore::new(
        config.source.max_connections.max(1) as usize,
    ));

    // Overall progress, kept as the last line so the per table bars stack above it.
    let total_tables = tables.len();
    let progress = Arc::new(MigrationProgress::new(total_tables));
    let summary_bar = multi_progress.add(ProgressBar::new(total_tables as u64));
    let eta_progress = progress.clone();
    summary_bar.set_style(
        ProgressStyle::with_template("[{elapsed_precise}] {bar:40} {smart_eta} left | {msg}")
            .unwrap()
            .with_key(
                "smart_eta",
                move |_state: &indicatif::ProgressState, writer: &mut dyn std::fmt::Write| {
                    match eta_progress.remaining() {
                        Some(eta) => write!(writer, "{}", FormattedDuration(eta)).unwrap(),
                        None => write!(writer, "-").unwrap(),
                    }
                },
            ),
    );
    summary_bar.set_message(format!(
        "0 done / {} remaining / {} tables",
        total_tables, total_tables
    ));
    summary_bar.enable_steady_tick(Duration::from_millis(500));
    let summary_bar = Arc::new(summary_bar);

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
        let table_slots = table_slots.clone();
        let multi_progress = multi_progress.clone();
        let summary_bar = summary_bar.clone();
        let progress = progress.clone();
        let sty = sty.clone();

        let handle = tokio::task::spawn(async move {
            // Held until the table is done, so it covers both the count and the extraction.
            let _slot = table_slots.acquire().await.unwrap();

            // The bar only appears once the table actually starts, so queued tables stay off
            // screen. Inserted one from the back to stay above the summary line.
            let progress_bar = multi_progress.insert_from_back(1, ProgressBar::new(0));
            progress_bar.set_style(sty);
            progress
                .in_flight
                .lock()
                .unwrap()
                .insert(name.clone(), progress_bar.clone());

            // Counted here rather than up front: doing it in the loop would need a source
            // connection while every other table is holding one.
            let count = match sqlx::query(AssertSqlSafe(format!("SELECT COUNT(*) FROM `{}`", name)))
                .fetch_one(source_pool.as_ref())
                .await
            {
                Ok(row) => row.get::<i64, usize>(0),
                Err(err) => {
                    progress_bar
                        .abandon_with_message(format!("table {} count failed: {}", name, err));
                    finish_table(&summary_bar, &progress, &name);

                    return;
                }
            };
            progress_bar.set_length(count as u64);

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
                        .abandon_with_message(format!("table {} backup failed: {}", name, err));
                }
            }

            finish_table(&summary_bar, &progress, &name);
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
