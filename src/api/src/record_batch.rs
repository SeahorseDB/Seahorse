use super::errors::CoralError;
use super::resultset::RecordBatchHandler;
use super::seahorse::shdb_table_batchinsert;
use arrow_array::RecordBatch;
use datafusion::scalar::ScalarValue;
use log::trace;
use redis::aio::ConnectionLike;

pub trait RecordBatchExt {
    fn trace_record_batch(
        &self,
        count: usize,
        is_printing_record_batch_log_enabled: bool,
    ) -> Result<(), CoralError>;

    async fn insert_into_seahorse_table(
        &self,
        conn: &mut impl ConnectionLike,
        table_name: &String,
        count: usize,
        is_printing_record_batch_log_enabled: bool,
    ) -> Result<usize, CoralError>;

    fn is_new_group(&self, segment_id_columns: &[String], row: usize) -> bool;
}

impl RecordBatchExt for RecordBatch {
    fn trace_record_batch(
        &self,
        count: usize,
        is_printing_record_batch_log_enabled: bool,
    ) -> Result<(), CoralError> {
        trace!(
            "record batch number={}, record batch count={}",
            count,
            self.num_rows()
        );
        if is_printing_record_batch_log_enabled {
            trace!("\n{}", {
                let mut record_batch = self.clone();
                for (i, field) in self.schema().fields().iter().enumerate() {
                    if matches!(
                        field.data_type(),
                        arrow_schema::DataType::List(_)
                            | arrow_schema::DataType::LargeList(_)
                            | arrow_schema::DataType::FixedSizeList(_, _)
                    ) {
                        record_batch.remove_column(i);
                    }
                }
                arrow_cast::pretty::pretty_format_batches(&vec![record_batch])?.to_string()
            });
        }
        Ok(())
    }

    async fn insert_into_seahorse_table(
        &self,
        conn: &mut impl ConnectionLike,
        table_name: &String,
        count: usize,
        is_printing_record_batch_log_enabled: bool,
    ) -> Result<usize, CoralError> {
        self.trace_record_batch(count, is_printing_record_batch_log_enabled)?;
        let inserted_row_count = self.num_rows();
        let ipc = self.to_ipc()?;
        shdb_table_batchinsert(conn, table_name, ipc).await?;
        Ok(inserted_row_count)
    }

    fn is_new_group(&self, segment_id_columns: &[String], row: usize) -> bool {
        segment_id_columns.iter().any(|col_name| {
            let column = self.column_by_name(col_name).unwrap();
            let prev_value = ScalarValue::try_from_array(column, row - 1).unwrap();
            let curr_value = ScalarValue::try_from_array(column, row).unwrap();
            prev_value != curr_value
        })
    }
}
