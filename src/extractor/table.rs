use crate::config::{LoadStrategy, MigrateTableConfig};
use crate::extractor::ExtractorError;
use crate::value::MysqlValueDecoded;
use futures::TryStreamExt;
use indicatif::ProgressBar;
use sqlx::{Executor, MySqlPool, QueryBuilder, Row, ValueRef};
use std::io::Write;
use std::ops::DerefMut;
use std::sync::Arc;

const SELECT_COLUMNS_FOR_INSERT: &str = "SELECT `COLUMN_NAME` AS `Field`, `COLUMN_TYPE` AS `Type`, `IS_NULLABLE` AS `Null`, `COLUMN_KEY` AS `Key`, `COLUMN_DEFAULT` AS `Default`, `EXTRA` AS `Extra`, `COLUMN_COMMENT` AS `Comment` FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?";

pub struct TableExtractor {
    source_pool: Arc<MySqlPool>,
    target_pool: Arc<MySqlPool>,
    migrate_table_config: MigrateTableConfig,
    name: String,
}

impl TableExtractor {
    pub fn new(
        source_pool: Arc<MySqlPool>,
        target_pool: Arc<MySqlPool>,
        migrate_table_config: MigrateTableConfig,
        name: String,
    ) -> Self {
        Self {
            source_pool,
            target_pool,
            migrate_table_config,
            name,
        }
    }

    pub async fn extract(&mut self, progress_bar: &ProgressBar) -> Result<(), ExtractorError> {
        // keep same connection to disable key check
        // first acquire source_conn to ensure slot available to handle table
        let mut source_conn = self.source_pool.acquire().await?;
        let mut conn = self.target_pool.acquire().await?;
        progress_bar.set_message(format!("drop target table {}", self.name));

        // disable key check
        let disable_key_check_query = "SET FOREIGN_KEY_CHECKS=0";
        sqlx::query(disable_key_check_query)
            .execute(conn.deref_mut())
            .await?;

        // delete table if exists in target
        let delete_query = format!("DROP TABLE IF EXISTS `{}`", self.name);
        sqlx::query(&delete_query).execute(conn.deref_mut()).await?;

        // write table schema
        progress_bar.set_message(format!("create target table {}", self.name));
        let query = format!("SHOW CREATE TABLE `{}`", self.name);
        let create_table_row = sqlx::query(query.as_str())
            .fetch_one(source_conn.deref_mut())
            .await?;

        let create_table_query = create_table_row.get::<&str, usize>(1);

        let _ = sqlx::query(create_table_query)
            .execute(conn.deref_mut())
            .await?;

        // write table data
        if self.migrate_table_config.skip_data {
            return Ok(());
        }

        drop(conn);
        progress_bar.set_message(format!("migrate table data {}", self.name));

        // fetch columns for table
        let columns_query = sqlx::query(SELECT_COLUMNS_FOR_INSERT)
            .bind(&self.name)
            .fetch_all(source_conn.deref_mut())
            .await?;

        let mut indexed_fields = Vec::new();

        for row in columns_query {
            let field = row.get::<&str, &str>("Field");
            indexed_fields.push(field.to_string());
        }

        // get data
        let select_query = format!("SELECT * FROM `{}`", self.name);
        let mut select_stream = source_conn.fetch(select_query.as_str());

        let batch_size = self.migrate_table_config.batch_size;
        let strategy = self.migrate_table_config.load_strategy;
        let mut rows = Vec::with_capacity(batch_size);
        let mut batch_id = 0;

        while let Some(row) = select_stream.try_next().await? {
            let mut values = Vec::with_capacity(row.len());

            for i in 0..row.len() {
                let value = ValueRef::to_owned(&row.try_get_raw(i)?);
                let mut decoded = MysqlValueDecoded::try_from(value)?;

                if let Some(field) = indexed_fields.get(i) {
                    if let Some(transformer) = self.migrate_table_config.transformers.get(field) {
                        decoded = transformer.transform(decoded);
                    }
                }

                values.push(decoded);
            }

            rows.push(values);

            if rows.len() == batch_size {
                let length = rows.len();
                let old_rows = std::mem::replace(&mut rows, Vec::with_capacity(batch_size));

                // acquire a new connection for batch so we can insert in parallel with fetching data
                let mut conn = self.target_pool.acquire().await?;
                let name = self.name.clone();

                tokio::spawn(async move {
                    if let Err(e) = insert_batch(
                        name.as_str(),
                        conn.deref_mut(),
                        old_rows,
                        batch_id,
                        strategy,
                    )
                    .await
                    {
                        tracing::error!("Failed to insert batch for table {}: {}", name, e);
                    }
                });

                batch_id += 1;
                progress_bar.inc(length as u64);
            }
        }

        if !rows.is_empty() {
            let length = rows.len();
            let mut conn = self.target_pool.acquire().await?;

            insert_batch(
                self.name.as_str(),
                conn.deref_mut(),
                rows,
                batch_id,
                strategy,
            )
            .await?;
            progress_bar.inc(length as u64);
        }

        Ok(())
    }
}

async fn insert_batch(
    name: &str,
    conn: &mut sqlx::MySqlConnection,
    rows: Vec<Vec<MysqlValueDecoded>>,
    batch_id: u32,
    strategy: LoadStrategy,
) -> Result<(), sqlx::Error> {
    match strategy {
        LoadStrategy::Insert => insert_batch_insert(name, conn, rows, batch_id).await,
        LoadStrategy::LoadData => insert_batch_load_data(name, conn, rows, batch_id).await,
    }
}

async fn insert_batch_insert(
    name: &str,
    conn: &mut sqlx::MySqlConnection,
    rows: Vec<Vec<MysqlValueDecoded>>,
    batch_id: u32,
) -> Result<(), sqlx::Error> {
    let mut query_builder = QueryBuilder::new(format!("INSERT INTO `{}`", name));
    let length = rows.len();

    query_builder.push_values(rows, |mut b, new_category| {
        for value in new_category {
            match value {
                MysqlValueDecoded::Int(i) => {
                    b.push_bind(i);
                }
                MysqlValueDecoded::UInt(u) => {
                    b.push_bind(u);
                }
                MysqlValueDecoded::Double(f) => {
                    b.push_bind(f);
                }
                MysqlValueDecoded::Decimal(f) => {
                    b.push_bind(f);
                }
                MysqlValueDecoded::String(s) => {
                    b.push_bind(s);
                }
                MysqlValueDecoded::DateTime(dt) => {
                    b.push_bind(dt);
                }
                MysqlValueDecoded::Bytes(bytes) => {
                    b.push_bind(bytes);
                }
                MysqlValueDecoded::Null => {
                    b.push_bind(None::<i32>);
                }
                MysqlValueDecoded::Bool(bool) => {
                    b.push_bind(bool);
                }
            }
        }
    });

    tracing::trace!(
        "[{} - {}] prepare to inserted {} rows",
        name,
        batch_id,
        length
    );
    let query = query_builder.build();
    let result = query.execute(conn).await?;
    tracing::trace!(
        "[{} - {}] inserted {} rows",
        name,
        batch_id,
        result.rows_affected()
    );

    Ok(())
}

async fn insert_batch_load_data(
    name: &str,
    conn: &mut sqlx::MySqlConnection,
    rows: Vec<Vec<MysqlValueDecoded>>,
    batch_id: u32,
) -> Result<(), sqlx::Error> {
    use std::io::BufWriter;

    let length = rows.len();

    if length == 0 {
        return Ok(());
    }

    // Create a temporary file
    let temp_file_path = format!("/tmp/mysql_migrate_{}_{}.csv", name, batch_id);
    let file = std::fs::File::create(&temp_file_path).map_err(|e| sqlx::Error::Io(e))?;
    let mut writer = BufWriter::new(file);

    // Write rows to CSV file
    for row in rows {
        let mut first = true;
        for value in row {
            if !first {
                writer.write_all(b"\t").map_err(|e| sqlx::Error::Io(e))?;
            }
            first = false;

            match value {
                MysqlValueDecoded::Int(i) => {
                    write!(writer, "{}", i).map_err(|e| sqlx::Error::Io(e))?;
                }
                MysqlValueDecoded::UInt(u) => {
                    write!(writer, "{}", u).map_err(|e| sqlx::Error::Io(e))?;
                }
                MysqlValueDecoded::Double(f) => {
                    write!(writer, "{}", f).map_err(|e| sqlx::Error::Io(e))?;
                }
                MysqlValueDecoded::Decimal(d) => {
                    write!(writer, "{}", d).map_err(|e| sqlx::Error::Io(e))?;
                }
                MysqlValueDecoded::String(s) => {
                    // Escape special characters for MySQL LOAD DATA
                    let escaped = s
                        .replace("\\", "\\\\")
                        .replace("\t", "\\t")
                        .replace("\n", "\\n")
                        .replace("\r", "\\r");
                    writer
                        .write_all(escaped.as_bytes())
                        .map_err(|e| sqlx::Error::Io(e))?;
                }
                MysqlValueDecoded::DateTime(dt) => {
                    write!(writer, "{}", dt).map_err(|e| sqlx::Error::Io(e))?;
                }
                MysqlValueDecoded::Bytes(bytes) => {
                    // Encode bytes as hex string for safe transport
                    write!(writer, "0x").map_err(|e| sqlx::Error::Io(e))?;
                    for byte in bytes {
                        write!(writer, "{:02x}", byte).map_err(|e| sqlx::Error::Io(e))?;
                    }
                }
                MysqlValueDecoded::Null => {
                    writer.write_all(b"\\N").map_err(|e| sqlx::Error::Io(e))?;
                }
                MysqlValueDecoded::Bool(b) => {
                    write!(writer, "{}", if b { 1 } else { 0 }).map_err(|e| sqlx::Error::Io(e))?;
                }
            }
        }
        writer.write_all(b"\n").map_err(|e| sqlx::Error::Io(e))?;
    }

    // Flush the writer
    writer.flush().map_err(|e| sqlx::Error::Io(e))?;
    drop(writer);

    tracing::trace!(
        "[{}] prepare to load {} rows from file {}",
        name,
        length,
        temp_file_path
    );

    // Execute LOAD DATA LOCAL INFILE
    let load_query = format!(
        "LOAD DATA LOCAL INFILE '{}' INTO TABLE `{}` FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'",
        temp_file_path, name
    );

    let result = sqlx::query(&load_query).execute(conn).await;

    // Clean up temporary file
    let _ = std::fs::remove_file(&temp_file_path);

    let result = result?;
    tracing::trace!("[{}] loaded {} rows", name, result.rows_affected());

    Ok(())
}
