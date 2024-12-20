use super::errors::CoralError;
use super::{models::DataInsertResult, record_batch::RecordBatchExt, schema::SchemaExt};
use actix_multipart::form::tempfile::TempFile;
use arrow_array::RecordBatch;
use arrow_json::reader::Decoder;
use arrow_json::{Reader, ReaderBuilder};
use arrow_schema::{ArrowError, SchemaRef};
use futures::{ready, Stream};
use parquet::{
    arrow::{
        arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
        async_reader::{AsyncFileReader, ParquetRecordBatchStream},
        ParquetRecordBatchStreamBuilder,
    },
    file::reader::ChunkReader,
};
use redis::aio::ConnectionLike;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{
    fs::File,
    io::{BufRead, BufReader, Cursor},
    time::Instant,
};
use tokio::io::AsyncBufRead;

pub struct ParquetSyncReader {
    record_batches: VecDeque<RecordBatch>,
    schema: SchemaRef,
    segment_id_columns: Option<Vec<String>>,
    current_record_batch: Option<RecordBatch>,
}

impl ParquetSyncReader {
    pub fn try_new<R, C>(
        reader: R,
        schema: SchemaRef,
        batch_size: Option<usize>,
    ) -> Result<Self, CoralError>
    where
        R: ParquetReaderBuilder<C>,
        C: ChunkReader,
    {
        let segment_id_columns = schema.clone().get_segment_id_columns_from_schema();
        let reader = reader.try_new_parquet_reader(batch_size)?;
        let record_batches = reader.collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            record_batches: VecDeque::from(record_batches),
            schema,
            segment_id_columns,
            current_record_batch: None,
        })
    }
}

pub struct ParquetAsyncReader<T> {
    stream: ParquetRecordBatchStream<T>,
    schema: SchemaRef,
    segment_id_columns: Option<Vec<String>>,
    current_record_batch: Option<RecordBatch>,
}

impl<T> ParquetAsyncReader<T>
where
    T: AsyncFileReader + Unpin + Send + 'static,
{
    pub async fn try_new<U>(
        reader: U,
        schema: SchemaRef,
        batch_size: Option<usize>,
    ) -> Result<Self, CoralError>
    where
        U: ParquetAsyncReaderBuilder<T>,
    {
        let segment_id_columns = schema.clone().get_segment_id_columns_from_schema();
        Ok(Self {
            stream: reader.try_new_parquet_async_reader(batch_size).await?,
            schema,
            segment_id_columns,
            current_record_batch: None,
        })
    }
}

pub struct JsonSyncReader {
    record_batches: VecDeque<RecordBatch>,
    schema: SchemaRef,
    segment_id_columns: Option<Vec<String>>,
    current_record_batch: Option<RecordBatch>,
}

impl JsonSyncReader {
    pub fn try_new<T, U>(
        reader: U,
        schema: SchemaRef,
        batch_size: Option<usize>,
    ) -> Result<Self, CoralError>
    where
        T: BufRead,
        U: JsonReaderBuilder<T>,
    {
        use std::collections::HashMap;
        use std::sync::Arc;
        let segment_id_columns = schema.clone().get_segment_id_columns_from_schema();
        let reader = reader.try_new_json_reader(
            Arc::new(schema.as_ref().clone().with_metadata(HashMap::new())),
            batch_size,
        )?;
        let record_batches = reader.collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            record_batches: VecDeque::from(record_batches),
            schema,
            segment_id_columns,
            current_record_batch: None,
        })
    }
}

pub struct JsonStream<R> {
    reader: R,
    decoder: Decoder,
}

impl<R> Stream for JsonStream<R>
where
    R: AsyncBufRead + Unpin,
{
    type Item = Result<RecordBatch, ArrowError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let this = self.as_mut().get_mut();
            let b = match ready!(Pin::new(&mut this.reader).poll_fill_buf(cx)) {
                Ok(b) if b.is_empty() => break,
                Ok(b) => b,
                Err(e) => return Poll::Ready(Some(Err(e.into()))),
            };
            let read = b.len();
            let decoded = match this.decoder.decode(b) {
                Ok(decoded) => decoded,
                Err(e) => return Poll::Ready(Some(Err(e))),
            };
            Pin::new(&mut this.reader).consume(decoded);
            if decoded != read {
                break;
            }
        }

        Poll::Ready(self.decoder.flush().transpose())
    }
}

pub struct JsonAsyncReader<R> {
    stream: JsonStream<R>,
    schema: SchemaRef,
    segment_id_columns: Option<Vec<String>>,
    current_record_batch: Option<RecordBatch>,
}

impl<R> JsonAsyncReader<R>
where
    R: AsyncBufRead + Unpin,
{
    pub fn try_new(
        reader: R,
        schema: SchemaRef,
        batch_size: Option<usize>,
    ) -> Result<Self, CoralError> {
        let segment_id_columns = schema.clone().get_segment_id_columns_from_schema();
        let builder = ReaderBuilder::new(schema.clone());
        let decoder = if let Some(batch_size) = batch_size {
            builder.with_batch_size(batch_size).build_decoder()?
        } else {
            builder.build_decoder()?
        };
        Ok(Self {
            stream: JsonStream { reader, decoder },
            schema,
            segment_id_columns,
            current_record_batch: None,
        })
    }
}

async fn partition_if_has_multiple_segments(
    record_batch: RecordBatch,
    segment_id_columns: &Option<Vec<String>>,
) -> (RecordBatch, Option<RecordBatch>) {
    match segment_id_columns {
        Some(segment_id_columns) => {
            for row in 1..record_batch.num_rows() {
                if record_batch.is_new_group(&segment_id_columns, row) {
                    let ret = record_batch.slice(0, row);
                    let remain = record_batch.slice(row, record_batch.num_rows() - row);
                    return (ret, Some(remain));
                }
            }
            // not segmented, return the current record batch
            (record_batch, None)
        }
        None => {
            // not need to segmentize, return the current record batch
            (record_batch, None)
        }
    }
}

pub trait SegmentReader {
    async fn next_record_batch(&mut self) -> Option<Result<RecordBatch, CoralError>>;

    async fn next_segment(&mut self) -> Option<Result<RecordBatch, CoralError>>;

    async fn insert_all(
        &mut self,
        conn: &mut impl ConnectionLike,
        table_name: &String,
        is_printing_record_batch_log_enabled: bool,
    ) -> Result<DataInsertResult, CoralError>;
}

impl<T: RecordBatchReaderExt> SegmentReader for T {
    async fn next_record_batch(&mut self) -> Option<Result<RecordBatch, CoralError>> {
        self.next().await
    }

    async fn next_segment(&mut self) -> Option<Result<RecordBatch, CoralError>> {
        if self.current_record_batch().is_none() {
            if let Some(result) = self.next_record_batch().await {
                match result {
                    Ok(record_batch) => self.set_current_record_batch(Some(record_batch)),
                    Err(e) => return Some(Err(e)),
                }
            } else {
                self.set_current_record_batch(None);
            }
        }

        if let Some(record_batch) = self.take_current_record_batch() {
            let (ret, remain) =
                partition_if_has_multiple_segments(record_batch, self.segment_id_columns()).await;
            self.set_current_record_batch(remain);
            Some(Ok(ret))
        } else {
            None
        }
    }

    async fn insert_all(
        &mut self,
        conn: &mut impl ConnectionLike,
        table_name: &String,
        is_printing_record_batch_log_enabled: bool,
    ) -> Result<DataInsertResult, CoralError> {
        let start = Instant::now();
        let mut inserted_record_batches = 0;
        let mut inserted_row_count = 0;

        while let Some(record_batch) = self.next_segment().await {
            let record_batch = record_batch?;
            inserted_row_count += record_batch
                .insert_into_seahorse_table(
                    conn,
                    table_name,
                    inserted_record_batches,
                    is_printing_record_batch_log_enabled,
                )
                .await?;
            inserted_record_batches += 1;
        }

        Ok(DataInsertResult {
            inserted_record_batches,
            inserted_row_count,
            schema: self.schema().as_ref().clone(),
            elapsed_time: Some(start.elapsed().as_secs_f32()),
        })
    }
}

pub trait RecordBatchReaderExt {
    fn schema(&mut self) -> SchemaRef;

    fn segment_id_columns(&self) -> &Option<Vec<String>>;

    fn current_record_batch(&self) -> &Option<RecordBatch>;

    fn set_current_record_batch(&mut self, batch: Option<RecordBatch>);

    fn take_current_record_batch(&mut self) -> Option<RecordBatch>;

    async fn next(&mut self) -> Option<Result<RecordBatch, CoralError>>;
}

impl RecordBatchReaderExt for ParquetSyncReader {
    fn schema(&mut self) -> SchemaRef {
        self.schema.clone()
    }

    fn segment_id_columns(&self) -> &Option<Vec<String>> {
        &self.segment_id_columns
    }

    fn current_record_batch(&self) -> &Option<RecordBatch> {
        &self.current_record_batch
    }

    fn set_current_record_batch(&mut self, batch: Option<RecordBatch>) {
        self.current_record_batch = batch;
    }

    fn take_current_record_batch(&mut self) -> Option<RecordBatch> {
        self.current_record_batch.take()
    }

    async fn next(&mut self) -> Option<Result<RecordBatch, CoralError>> {
        self.record_batches.pop_front().map(Ok)
    }
}

impl RecordBatchReaderExt for ParquetAsyncReader<tokio::fs::File> {
    fn schema(&mut self) -> SchemaRef {
        self.schema.clone()
    }

    fn segment_id_columns(&self) -> &Option<Vec<String>> {
        &self.segment_id_columns
    }

    fn current_record_batch(&self) -> &Option<RecordBatch> {
        &self.current_record_batch
    }

    fn set_current_record_batch(&mut self, batch: Option<RecordBatch>) {
        self.current_record_batch = batch;
    }

    fn take_current_record_batch(&mut self) -> Option<RecordBatch> {
        self.current_record_batch.take()
    }

    async fn next(&mut self) -> Option<Result<RecordBatch, CoralError>> {
        use futures::StreamExt;
        self.stream
            .next()
            .await
            .map(|result| result.map_err(CoralError::from))
    }
}

impl RecordBatchReaderExt for JsonSyncReader {
    fn schema(&mut self) -> SchemaRef {
        self.schema.clone()
    }

    fn segment_id_columns(&self) -> &Option<Vec<String>> {
        &self.segment_id_columns
    }

    fn current_record_batch(&self) -> &Option<RecordBatch> {
        &self.current_record_batch
    }

    fn set_current_record_batch(&mut self, batch: Option<RecordBatch>) {
        self.current_record_batch = batch;
    }

    fn take_current_record_batch(&mut self) -> Option<RecordBatch> {
        self.current_record_batch.take()
    }

    async fn next(&mut self) -> Option<Result<RecordBatch, CoralError>> {
        self.record_batches.pop_front().map(Ok)
    }
}

impl<R> RecordBatchReaderExt for JsonAsyncReader<R>
where
    R: AsyncBufRead + Unpin,
{
    fn schema(&mut self) -> SchemaRef {
        self.schema.clone()
    }

    fn segment_id_columns(&self) -> &Option<Vec<String>> {
        &self.segment_id_columns
    }

    fn current_record_batch(&self) -> &Option<RecordBatch> {
        &self.current_record_batch
    }

    fn set_current_record_batch(&mut self, batch: Option<RecordBatch>) {
        self.current_record_batch = batch;
    }

    fn take_current_record_batch(&mut self) -> Option<RecordBatch> {
        self.current_record_batch.take()
    }

    async fn next(&mut self) -> Option<Result<RecordBatch, CoralError>> {
        use futures::StreamExt;
        self.stream
            .next()
            .await
            .map(|result| result.map_err(CoralError::from))
    }
}

pub trait ParquetReaderBuilder<T: ChunkReader> {
    fn try_new_parquet_reader(
        self,
        batch_insert_size: Option<usize>,
    ) -> Result<ParquetRecordBatchReader, CoralError>;
}

impl ParquetReaderBuilder<File> for TempFile {
    fn try_new_parquet_reader(
        self,
        batch_insert_size: Option<usize>,
    ) -> Result<ParquetRecordBatchReader, CoralError> {
        let builder = ParquetRecordBatchReaderBuilder::try_new(self.file.into_file())?;
        if let Some(batch_insert_size) = batch_insert_size {
            let reader = builder.with_batch_size(batch_insert_size).build()?;
            Ok(reader)
        } else {
            Ok(builder.build()?)
        }
    }
}

impl ParquetReaderBuilder<File> for File {
    fn try_new_parquet_reader(
        self,
        batch_insert_size: Option<usize>,
    ) -> Result<ParquetRecordBatchReader, CoralError> {
        let builder = ParquetRecordBatchReaderBuilder::try_new(self)?;
        if let Some(batch_insert_size) = batch_insert_size {
            let reader = builder.with_batch_size(batch_insert_size).build()?;
            Ok(reader)
        } else {
            Ok(builder.build()?)
        }
    }
}

impl ParquetReaderBuilder<bytes::Bytes> for Cursor<Vec<u8>> {
    fn try_new_parquet_reader(
        self,
        batch_insert_size: Option<usize>,
    ) -> Result<ParquetRecordBatchReader, CoralError> {
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(self.into_inner()))?;
        if let Some(batch_insert_size) = batch_insert_size {
            let reader = builder.with_batch_size(batch_insert_size).build()?;
            Ok(reader)
        } else {
            Ok(builder.build()?)
        }
    }
}

pub trait ParquetAsyncReaderBuilder<T: AsyncFileReader> {
    async fn try_new_parquet_async_reader(
        self,
        batch_size: Option<usize>,
    ) -> Result<ParquetRecordBatchStream<T>, CoralError>;
}

impl ParquetAsyncReaderBuilder<tokio::fs::File> for tokio::fs::File {
    async fn try_new_parquet_async_reader(
        self,
        batch_size: Option<usize>,
    ) -> Result<ParquetRecordBatchStream<tokio::fs::File>, CoralError> {
        let builder = ParquetRecordBatchStreamBuilder::new(self).await?;
        if let Some(batch_size) = batch_size {
            let reader = builder.with_batch_size(batch_size).build()?;
            Ok(reader)
        } else {
            Ok(builder.build()?)
        }
    }
}

pub trait JsonReaderBuilder<T: BufRead> {
    fn try_new_json_reader(
        self,
        schema: SchemaRef,
        batch_size: Option<usize>,
    ) -> Result<Reader<T>, CoralError>;
}

impl JsonReaderBuilder<BufReader<File>> for File {
    fn try_new_json_reader(
        self,
        schema: SchemaRef,
        batch_size: Option<usize>,
    ) -> Result<Reader<BufReader<File>>, CoralError> {
        let builder = ReaderBuilder::new(schema.clone());
        if let Some(batch_size) = batch_size {
            let reader = builder
                .with_batch_size(batch_size)
                .build(BufReader::new(self))?;
            Ok(reader)
        } else {
            Ok(builder.build(BufReader::new(self))?)
        }
    }
}

impl JsonReaderBuilder<BufReader<File>> for TempFile {
    fn try_new_json_reader(
        self,
        schema: SchemaRef,
        batch_size: Option<usize>,
    ) -> Result<Reader<BufReader<File>>, CoralError> {
        let builder = ReaderBuilder::new(schema.clone());
        if let Some(batch_size) = batch_size {
            let reader = builder
                .with_batch_size(batch_size)
                .build(BufReader::new(self.file.into_file()))?;
            Ok(reader)
        } else {
            Ok(builder.build(BufReader::new(self.file.into_file()))?)
        }
    }
}

impl JsonReaderBuilder<Cursor<String>> for String {
    fn try_new_json_reader(
        self,
        schema: SchemaRef,
        batch_size: Option<usize>,
    ) -> Result<Reader<Cursor<String>>, CoralError> {
        let builder = ReaderBuilder::new(schema.clone());
        if let Some(batch_size) = batch_size {
            let reader = builder
                .with_batch_size(batch_size)
                .build(std::io::Cursor::new(self))?;
            Ok(reader)
        } else {
            Ok(builder.build(std::io::Cursor::new(self))?)
        }
    }
}

impl JsonReaderBuilder<Cursor<Vec<u8>>> for Cursor<Vec<u8>> {
    fn try_new_json_reader(
        self,
        schema: SchemaRef,
        batch_size: Option<usize>,
    ) -> Result<Reader<Cursor<Vec<u8>>>, CoralError> {
        let builder = ReaderBuilder::new(schema.clone());
        if let Some(batch_size) = batch_size {
            let reader = builder.with_batch_size(batch_size).build(self)?;
            Ok(reader)
        } else {
            Ok(builder.build(self)?)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl::handle_segement_id_info;
    use arrow_array::RecordBatch;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::errors::Result;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_get_group_by_record_batches_single_segment_id() -> Result<(), CoralError> {
        // Create a sample schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("segment_id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        // Add metadata 'segment_id_info' to schema
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("segment_id_info".to_string(), "segment_id".to_string());
        handle_segement_id_info(&mut metadata, &schema.fields());
        let schema = Arc::new(Schema::new_with_metadata(
            schema.fields().to_vec(),
            metadata,
        ));

        // Create sample data with 1000 rows across multiple batches
        let mut batches = Vec::new();
        let segment_ids = ["A", "B", "C", "D", "E"];
        let batch_size = 100;
        let num_batches = 10;

        for batch_num in 0..num_batches {
            let start_id = batch_num * batch_size + 1;
            let ids = Int32Array::from((start_id..start_id + batch_size).collect::<Vec<i32>>());
            let segment_ids = StringArray::from(
                (0..batch_size)
                    .map(|i| segment_ids[((i + batch_num) as usize) % segment_ids.len()])
                    .collect::<Vec<&str>>(),
            );
            let values = Int32Array::from(
                (start_id..start_id + batch_size)
                    .map(|i| i * 10)
                    .collect::<Vec<i32>>(),
            );

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(ids), Arc::new(segment_ids), Arc::new(values)],
            )?;
            batches.push(batch);
        }

        // Save the record batch as a Parquet file
        let file = tempfile::tempfile().unwrap();
        let mut writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), schema.clone(), None).unwrap();
        for batch in batches {
            writer.write(&batch).unwrap();
        }
        writer.close().unwrap();

        // Read the Parquet file
        let file = file.try_clone().unwrap();
        let mut reader = ParquetSyncReader::try_new(file, schema, Some(100))?;

        // Call the function
        let mut result = Vec::new();
        while let Some(batch) = reader.next_segment().await {
            result.push(batch?);
        }

        // Validate the result
        println!("Number of result batches: {}", result.len());
        let total_rows: usize = result.iter().map(|batch| batch.num_rows()).sum();
        println!("Total number of rows: {}", total_rows);

        assert_eq!(total_rows, 1000, "Total number of rows should be 1000");
        println!("Record Batches:");
        for (i, batch) in result.iter().enumerate() {
            println!("Batch {}:", i + 1);
            println!(
                "{}",
                arrow_cast::pretty::pretty_format_batches(&[batch.clone()])?.to_string()
            );
        }
        assert_eq!(
            result.len(),
            1000,
            "There should be 1000 record batches, one for each segment_id"
        );

        // Check that each batch has the same segment_id and is sorted by id
        for (i, batch) in result.iter().enumerate() {
            let segment_ids: Vec<&str> = batch
                .column_by_name("segment_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|v| v.unwrap())
                .collect();

            // Check that all segment_ids in this batch are the same
            assert!(
                segment_ids.windows(2).all(|w| w[0] == w[1]),
                "All segment_ids in batch {} should be the same",
                i
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_get_group_by_record_batches_multiple_segment_ids() -> Result<(), CoralError> {
        // Create a sample schema with multiple segment ID columns
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("segment_id1", DataType::Utf8, false),
            Field::new("segment_id2", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        // Add metadata 'segment_id_info' to schema
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(
            "segment_id_info".to_string(),
            "[segment_id1,segment_id2]".to_string(),
        );
        handle_segement_id_info(&mut metadata, &schema.fields());
        let schema = Arc::new(Schema::new_with_metadata(
            schema.fields().to_vec(),
            metadata,
        ));

        // Create sample data
        let ids = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let segment_ids1 =
            StringArray::from(vec!["A", "A", "B", "B", "A", "C", "C", "B", "A", "C"]);
        let segment_ids2 = Int32Array::from(vec![1, 2, 1, 2, 1, 1, 2, 1, 2, 2]);
        let values = Int32Array::from(vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100]);

        // Create a record batch
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(ids),
                Arc::new(segment_ids1),
                Arc::new(segment_ids2),
                Arc::new(values),
            ],
        )?;

        // Save the record batch as a Parquet file
        let file = tempfile::tempfile().unwrap();
        let mut writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), schema.clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Read the Parquet file
        let file = file.try_clone().unwrap();
        let mut reader = ParquetSyncReader::try_new(file, schema, Some(100))?;

        // Call the function
        let mut result = Vec::new();
        while let Some(batch) = reader.next_segment().await {
            result.push(batch?);
        }

        // Validate the result
        println!("Number of result batches: {}", result.len());
        let total_rows: usize = result.iter().map(|batch| batch.num_rows()).sum();
        println!("Total number of rows: {}", total_rows);
        for (i, batch) in result.iter().enumerate() {
            println!("Batch {}:", i + 1);
            println!(
                "{}",
                arrow_cast::pretty::pretty_format_batches(&[batch.clone()])?.to_string()
            );
        }
        assert_eq!(result.len(), 10);

        // Helper function to check segment IDs
        fn check_segment_ids(
            batch: &RecordBatch,
            expected_id1: &str,
            expected_id2: i32,
            expected_count: usize,
            expected_id: i32,
            expected_value: i32,
        ) {
            assert_eq!(batch.num_rows(), expected_count);
            let segment_id1 = batch
                .column_by_name("segment_id1")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0);
            let segment_id2 = batch
                .column_by_name("segment_id2")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0);
            let id = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0);
            let value = batch
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0);
            assert_eq!(segment_id1, expected_id1);
            assert_eq!(segment_id2, expected_id2);
            assert_eq!(id, expected_id);
            assert_eq!(value, expected_value);
        }

        // Check each batch
        check_segment_ids(&result[0], "A", 1, 1, 1, 10);
        check_segment_ids(&result[1], "A", 2, 1, 2, 20);
        check_segment_ids(&result[2], "B", 1, 1, 3, 30);
        check_segment_ids(&result[3], "B", 2, 1, 4, 40);
        check_segment_ids(&result[4], "A", 1, 1, 5, 50);
        check_segment_ids(&result[5], "C", 1, 1, 6, 60);
        check_segment_ids(&result[6], "C", 2, 1, 7, 70);
        check_segment_ids(&result[7], "B", 1, 1, 8, 80);
        check_segment_ids(&result[8], "A", 2, 1, 9, 90);
        check_segment_ids(&result[9], "C", 2, 1, 10, 100);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_group_by_record_batches_empty_input() -> Result<(), CoralError> {
        // Create an empty schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("segment_id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        // Add metadata 'segment_id_info' to schema
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("segment_id_info".to_string(), "segment_id".to_string());
        handle_segement_id_info(&mut metadata, &schema.fields());
        let schema = Arc::new(Schema::new_with_metadata(
            schema.fields().to_vec(),
            metadata,
        ));

        // Create an empty record batch
        let batch = RecordBatch::new_empty(schema.clone());

        // Save the record batch as a Parquet file
        let file = tempfile::tempfile().unwrap();
        let mut writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), schema.clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Read the Parquet file
        let file = file.try_clone().unwrap();
        let mut reader = ParquetSyncReader::try_new(file, schema, Some(100))?;

        // Call the function
        let mut result = Vec::new();
        while let Some(batch) = reader.next_segment().await {
            result.push(batch?);
        }

        // Validate the result
        assert_eq!(result.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_group_by_record_batches_same_segment_id() -> Result<(), CoralError> {
        // Create a sample schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("segment_id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        // Add metadata 'segment_id_info' to schema
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("segment_id_info".to_string(), "segment_id".to_string());
        handle_segement_id_info(&mut metadata, &schema.fields());
        let schema = Arc::new(Schema::new_with_metadata(
            schema.fields().to_vec(),
            metadata,
        ));

        // Create sample data with all rows having the same segment_id
        let ids = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let segment_ids = StringArray::from(vec!["A", "A", "A", "A", "A"]);
        let values = Int32Array::from(vec![10, 20, 30, 40, 50]);

        // Create a record batch
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(ids), Arc::new(segment_ids), Arc::new(values)],
        )?;

        // Save the record batch as a Parquet file
        let file = tempfile::tempfile().unwrap();
        let mut writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), schema.clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Read the Parquet file
        let file = file.try_clone().unwrap();
        let mut reader = ParquetSyncReader::try_new(file, schema, Some(100))?;

        // Call the function
        let mut result = Vec::new();
        while let Some(batch) = reader.next_segment().await {
            result.push(batch?);
        }

        // Validate the result
        assert_eq!(result.len(), 1); // We expect 1 group since all rows have the same segment_id

        // Check the single group
        let group = &result[0];
        assert_eq!(group.num_rows(), 5);
        let segment_ids: Vec<_> = group
            .column_by_name("segment_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect();
        assert_eq!(
            segment_ids,
            vec![Some("A"), Some("A"), Some("A"), Some("A"), Some("A")]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_get_group_by_record_batches_json_reader() -> Result<(), CoralError> {
        // Define the schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("segment_id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        // Add metadata 'segment_id_info' to schema
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("segment_id_info".to_string(), "segment_id".to_string());
        handle_segement_id_info(&mut metadata, &schema.fields());
        let schema = Arc::new(Schema::new_with_metadata(
            schema.fields().to_vec(),
            metadata,
        ));

        // Create JSON data in the format expected by arrow-json
        let json_data = r#"
        {"id": 1, "segment_id": "A", "value": 10}
        {"id": 2, "segment_id": "A", "value": 20}
        {"id": 3, "segment_id": "B", "value": 30}
        {"id": 4, "segment_id": "B", "value": 40}
        {"id": 5, "segment_id": "A", "value": 50}
        {"id": 6, "segment_id": "C", "value": 60}
        {"id": 7, "segment_id": "C", "value": 70}
        {"id": 8, "segment_id": "B", "value": 80}
        {"id": 9, "segment_id": "A", "value": 90}
        {"id": 10, "segment_id": "C", "value": 100}
        "#;

        println!("json_data: {}", json_data);

        let mut reader = JsonSyncReader::try_new(
            std::io::Cursor::new(json_data.as_bytes().to_vec()),
            schema,
            Some(5),
        )?;

        // Call the function
        let mut result = Vec::new();
        while let Some(batch) = reader.next_segment().await {
            result.push(batch?);
        }

        // Validate the result
        println!("Number of result batches: {}", result.len());
        for (i, batch) in result.iter().enumerate() {
            println!("Batch {}:", i + 1);
            println!(
                "{}",
                arrow_cast::pretty::pretty_format_batches(&[batch.clone()])?.to_string()
            );
        }
        assert_eq!(result.len(), 7);

        // Helper function to check batch contents
        fn check_batch(
            batch: &RecordBatch,
            expected_ids: &[i32],
            expected_segment_ids: &[&str],
            expected_values: &[i32],
        ) {
            assert_eq!(batch.num_rows(), expected_ids.len());

            let ids: Vec<i32> = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec();

            let segment_ids: Vec<&str> = batch
                .column_by_name("segment_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|v| v.unwrap())
                .collect();

            let values: Vec<i32> = batch
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec();

            assert_eq!(ids, expected_ids);
            assert_eq!(segment_ids, expected_segment_ids);
            assert_eq!(values, expected_values);
        }

        // Check each batch
        check_batch(&result[0], &[1, 2], &["A", "A"], &[10, 20]);
        check_batch(&result[1], &[3, 4], &["B", "B"], &[30, 40]);
        check_batch(&result[2], &[5], &["A"], &[50]);
        check_batch(&result[3], &[6, 7], &["C", "C"], &[60, 70]);
        check_batch(&result[4], &[8], &["B"], &[80]);
        check_batch(&result[5], &[9], &["A"], &[90]);
        check_batch(&result[6], &[10], &["C"], &[100]);

        Ok(())
    }

    #[tokio::test]
    async fn test_async_record_batches_json_reader() -> Result<(), CoralError> {
        // Define the schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("segment_id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        // Add metadata 'segment_id_info' to schema
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("segment_id_info".to_string(), "segment_id".to_string());
        handle_segement_id_info(&mut metadata, &schema.fields());
        let schema = Arc::new(Schema::new_with_metadata(
            schema.fields().to_vec(),
            metadata,
        ));

        // Create JSON data in the format expected by arrow-json
        let json_data = r#"
        {"id": 1, "segment_id": "A", "value": 10}
        {"id": 2, "segment_id": "A", "value": 20}
        {"id": 3, "segment_id": "B", "value": 30}
        {"id": 4, "segment_id": "B", "value": 40}
        {"id": 5, "segment_id": "A", "value": 50}
        {"id": 6, "segment_id": "C", "value": 60}
        {"id": 7, "segment_id": "C", "value": 70}
        {"id": 8, "segment_id": "B", "value": 80}
        {"id": 9, "segment_id": "A", "value": 90}
        {"id": 10, "segment_id": "C", "value": 100}
        "#;

        println!("json_data length: {}", json_data.len());
        let mut reader = JsonAsyncReader::try_new(
            std::io::Cursor::new(json_data.as_bytes().to_vec()),
            schema.clone(),
            Some(5),
        )?;

        // Call the function
        let mut result = Vec::new();
        while let Some(batch) = reader.next_segment().await {
            result.push(batch?);
        }

        // Validate the result
        println!("Number of result batches: {}", result.len());
        for (i, batch) in result.iter().enumerate() {
            println!("Batch {}:", i + 1);
            println!(
                "{}",
                arrow_cast::pretty::pretty_format_batches(&[batch.clone()])?.to_string()
            );
        }
        assert_eq!(result.len(), 7);

        // Helper function to check batch contents
        fn check_batch(
            batch: &RecordBatch,
            expected_ids: &[i32],
            expected_segment_ids: &[&str],
            expected_values: &[i32],
        ) {
            assert_eq!(batch.num_rows(), expected_ids.len());

            let ids: Vec<i32> = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec();

            let segment_ids: Vec<&str> = batch
                .column_by_name("segment_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|v| v.unwrap())
                .collect();

            let values: Vec<i32> = batch
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec();

            assert_eq!(ids, expected_ids);
            assert_eq!(segment_ids, expected_segment_ids);
            assert_eq!(values, expected_values);
        }

        // Check each batch
        check_batch(&result[0], &[1, 2], &["A", "A"], &[10, 20]);
        check_batch(&result[1], &[3, 4], &["B", "B"], &[30, 40]);
        check_batch(&result[2], &[5], &["A"], &[50]);
        check_batch(&result[3], &[6, 7], &["C", "C"], &[60, 70]);
        check_batch(&result[4], &[8], &["B"], &[80]);
        check_batch(&result[5], &[9], &["A"], &[90]);
        check_batch(&result[6], &[10], &["C"], &[100]);

        Ok(())
    }

    #[tokio::test]
    async fn test_repartition_by_segment_id_columns_with_batch_insert_size(
    ) -> Result<(), CoralError> {
        // Create a sample schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("segment_id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        // Add metadata 'segment_id_info' to schema
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("segment_id_info".to_string(), "segment_id".to_string());
        handle_segement_id_info(&mut metadata, &schema.fields());
        let schema = Arc::new(Schema::new_with_metadata(
            schema.fields().to_vec(),
            metadata,
        ));

        // Create sample data with 20 rows
        let ids = Int32Array::from((1..=24).collect::<Vec<i32>>());
        let segment_ids = StringArray::from(vec![
            "A", "A", "A", "A", "A", "A", "A", "A", "A", "B", "B", "B", "B", "B", "B", "B", "C",
            "C", "C", "C", "C", "B", "B", "C",
        ]);
        let values = Int32Array::from((1..=24).map(|i| i * 10).collect::<Vec<i32>>());

        // Create a record batch
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(ids), Arc::new(segment_ids), Arc::new(values)],
        )?;

        // Save the record batch as a Parquet file
        let file = tempfile::tempfile().unwrap();
        let mut writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), schema.clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Read the Parquet file
        let file = file.try_clone().unwrap();
        let mut reader = ParquetSyncReader::try_new(file, schema, Some(5))?;

        // Call the function
        let mut result = Vec::new();
        while let Some(batch) = reader.next_segment().await {
            result.push(batch?);
        }

        // Validate the result
        println!("Number of result batches: {}", result.len());
        for (i, batch) in result.iter().enumerate() {
            println!("Batch {}:", i + 1);
            println!(
                "{}",
                arrow_cast::pretty::pretty_format_batches(&[batch.clone()])?.to_string()
            );
        }

        // We expect 5 batches: 2 for A (5 rows each), 2 for B (5 rows and 2 rows), and 1 for C (5 rows)
        assert_eq!(result.len(), 9);

        // Helper function to check batch contents
        fn check_batch(batch: &RecordBatch, expected_segment_id: &str, expected_count: usize) {
            assert_eq!(batch.num_rows(), expected_count);
            let segment_ids = batch
                .column_by_name("segment_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert!(segment_ids.iter().all(|id| id == Some(expected_segment_id)));
        }

        // Check each batch
        check_batch(&result[0], "A", 5);
        check_batch(&result[1], "A", 4);
        check_batch(&result[2], "B", 1);
        check_batch(&result[3], "B", 5);
        check_batch(&result[4], "B", 1);
        check_batch(&result[5], "C", 4);
        check_batch(&result[6], "C", 1);
        check_batch(&result[7], "B", 2);
        check_batch(&result[8], "C", 1);

        // Check total record count
        let total_rows: usize = result.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(total_rows, 24, "Total number of rows should be 24");

        Ok(())
    }

    #[tokio::test]
    async fn test_partition() {
        // Create a sample schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("segment_id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        // Add metadata 'segment_id_info' to schema
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("segment_id_info".to_string(), "segment_id".to_string());
        handle_segement_id_info(&mut metadata, &schema.fields());
        let schema = Arc::new(Schema::new_with_metadata(
            schema.fields().to_vec(),
            metadata,
        ));

        // Create sample data
        let ids = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]);
        let segment_ids = StringArray::from(vec!["A", "A", "A", "B", "B", "C", "C", "C"]);
        let values = Int32Array::from(vec![10, 20, 30, 40, 50, 60, 70, 80]);

        // Create a record batch
        let record_batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(ids), Arc::new(segment_ids), Arc::new(values)],
        )
        .unwrap();

        // Define segment_id_columns
        let segment_id_columns = Some(vec!["segment_id".to_string()]);

        // Call the partition method
        let (partitioned, remaining) =
            partition_if_has_multiple_segments(record_batch.clone(), &segment_id_columns).await;

        // Check partitioned batch
        assert_eq!(partitioned.num_rows(), 3);
        let partitioned_segment_ids: Vec<Option<&str>> = partitioned
            .column_by_name("segment_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect();
        assert_eq!(
            partitioned_segment_ids,
            vec![Some("A"), Some("A"), Some("A")]
        );

        // Check remaining batch
        assert!(remaining.is_some());
        let remaining = remaining.unwrap();
        assert_eq!(remaining.num_rows(), 5);
        let remaining_segment_ids: Vec<Option<&str>> = remaining
            .column_by_name("segment_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect();
        assert_eq!(
            remaining_segment_ids,
            vec![Some("B"), Some("B"), Some("C"), Some("C"), Some("C")]
        );

        // Test with no segment_id_columns
        let (partitioned, remaining) =
            partition_if_has_multiple_segments(record_batch.clone(), &None).await;
        assert_eq!(partitioned.num_rows(), 8);
        assert!(remaining.is_none());
    }

    #[tokio::test]
    async fn test_next_segment_async() -> Result<(), CoralError> {
        // Create a sample schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("segment_id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        // Add metadata 'segment_id_info' to schema
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("segment_id_info".to_string(), "segment_id".to_string());
        handle_segement_id_info(&mut metadata, &schema.fields());
        let schema = Arc::new(Schema::new_with_metadata(
            schema.fields().to_vec(),
            metadata,
        ));

        // Create sample data
        let ids = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]);
        let segment_ids = StringArray::from(vec!["A", "A", "A", "B", "B", "C", "C", "C"]);
        let values = Int32Array::from(vec![10, 20, 30, 40, 50, 60, 70, 80]);

        // Create a record batch
        let record_batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(ids), Arc::new(segment_ids), Arc::new(values)],
        )
        .unwrap();

        // Save the record batch as a Parquet file
        let file = tempfile::tempfile().unwrap();
        let mut writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), record_batch.schema(), None).unwrap();
        writer.write(&record_batch).unwrap();
        writer.close().unwrap();

        // Read the Parquet file
        let file = tokio::fs::File::from_std(file.try_clone().unwrap());
        let mut reader = ParquetAsyncReader::try_new(file, schema, Some(5)).await?;

        // Test next_segment
        let segment1 = reader.next_segment().await.unwrap()?;
        assert_eq!(segment1.num_rows(), 3);
        assert_eq!(
            segment1
                .column_by_name("segment_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("A"), Some("A"), Some("A")]
        );

        let segment2 = reader.next_segment().await.unwrap()?;
        assert_eq!(segment2.num_rows(), 2);
        assert_eq!(
            segment2
                .column_by_name("segment_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("B"), Some("B")]
        );

        let segment3 = reader.next_segment().await.unwrap()?;
        assert_eq!(segment3.num_rows(), 3);
        assert_eq!(
            segment3
                .column_by_name("segment_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("C"), Some("C"), Some("C")]
        );

        assert!(reader.next_segment().await.is_none());

        Ok(())
    }
}
