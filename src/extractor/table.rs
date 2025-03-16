use crate::config::{MigrateTableConfig};
use crate::extractor::ExtractorError;
use futures::TryStreamExt;
use sqlx::{MySqlPool, QueryBuilder, Row, ValueRef};
use std::ops::{DerefMut};
use std::sync::Arc;
use crate::value::MysqlValueDecoded;

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

    pub async fn extract(&mut self) -> Result<(), ExtractorError> {
        // keep same connection to disable key check
        let mut conn = self.target_pool.acquire().await?;

        // disable key check
        let disable_key_check_query = "SET FOREIGN_KEY_CHECKS=0";
        sqlx::query(disable_key_check_query)
            .execute(conn.deref_mut())
            .await?;

        // delete table if exists in target
        let delete_query = format!("DROP TABLE IF EXISTS `{}`", self.name);
        sqlx::query(&delete_query).execute(conn.deref_mut()).await?;

        // write table schema
        let query = format!("SHOW CREATE TABLE `{}`", self.name);
        let create_table_row = sqlx::query(query.as_str())
            .fetch_one(self.source_pool.as_ref())
            .await?;

        let create_table_query = create_table_row.get::<&str, usize>(1);

        let _ = sqlx::query(create_table_query)
            .execute(conn.deref_mut())
            .await?;

        // write table data
        if self.migrate_table_config.skip_data {
            return Ok(());
        }

        // fetch columns for table
        let columns_query = sqlx::query(SELECT_COLUMNS_FOR_INSERT)
            .bind(&self.name)
            .fetch_all(self.source_pool.as_ref())
            .await?;

        let mut indexed_fields = Vec::new();

        for row in columns_query {
            let field = row.get::<&str, &str>("Field");
            indexed_fields.push(field.to_string());
        }

        let select_query = format!("SELECT * FROM `{}`", self.name);
        let mut select_stream = sqlx::query(select_query.as_str()).fetch(&*self.source_pool);

        let batch_size = self.migrate_table_config.batch_size;
        let mut rows = Vec::with_capacity(batch_size);

        while let Some(row) = select_stream.try_next().await? {
            let mut values = Vec::with_capacity(row.len());

            for i in 0..row.len() {
                let value = ValueRef::to_owned(&row.try_get_raw(i)?);
                let mut decoded = MysqlValueDecoded::try_from(value)?;

                match indexed_fields.get(i) {
                    Some(field) => match self.migrate_table_config.transformers.get(field) {
                        Some(transformer) => {
                            decoded = transformer.transform(decoded);
                        },
                        None => (),
                    },
                    None => (),
                }

                values.push(decoded);
            }

            rows.push(values);

            if rows.len() == batch_size {
                let old_rows = std::mem::replace(&mut rows, Vec::with_capacity(batch_size));

                insert_batch(self.name.as_str(), conn.deref_mut(), old_rows).await?;
            }
        }

        if rows.len() > 0 {
            insert_batch(self.name.as_str(), conn.deref_mut(), rows).await?;
        }

        Ok(())
    }
}

async fn insert_batch(name: &str, conn: &mut sqlx::MySqlConnection, rows: Vec<Vec<MysqlValueDecoded>>) -> Result<(), sqlx::Error> {
    let mut query_builder = QueryBuilder::new(format!("INSERT INTO `{}`", name));

    query_builder.push_values(rows, |mut b, new_category| {
        for value in new_category {
            match value {
                MysqlValueDecoded::Int(i) => {
                    b.push_bind(i);
                },
                MysqlValueDecoded::UInt(u) => {
                    b.push_bind(u);
                },
                MysqlValueDecoded::String(s) => {
                    b.push_bind(s);
                },
                MysqlValueDecoded::DateTime(dt) => {
                    b.push_bind(dt);
                },
                MysqlValueDecoded::Bytes(bytes) => {
                    b.push_bind(bytes);
                },
                MysqlValueDecoded::Null => {
                    b.push_bind(None::<i32>);
                },
                MysqlValueDecoded::Bool(bool) => {
                    b.push_bind(bool);
                },
            }
        }
    });

    let query = query_builder.build();

    query.execute(conn).await?;

    Ok(())
}

