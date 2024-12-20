use super::errors::CoralError;
use super::models::*;
use actix_web::{http::header::ContentType, HttpResponse};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use arrow_select::concat::concat_batches;
use futures::future::join_all;
use log::{debug, trace};

#[derive(Debug, Clone)]
pub struct SeahorseDBResultSet {
    pub record_batch: RecordBatch,
}

pub struct ResultSetJson {
    pub data_json: serde_json::Value,
    pub schema_json: serde_json::Value,
}

pub trait IpcHandler {
    fn to_seahorse_db_resultset(
        &self,
        is_printing_record_batch_log_enabled: bool,
    ) -> Result<SeahorseDBResultSet, CoralError>;
}

impl IpcHandler for Vec<u8> {
    fn to_seahorse_db_resultset(
        &self,
        is_printing_record_batch_log_enabled: bool,
    ) -> Result<SeahorseDBResultSet, CoralError> {
        let reader = arrow_ipc::reader::StreamReader::try_new(self.as_slice(), None)?;
        let schema = reader.schema().clone();
        let record_batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;

        if is_printing_record_batch_log_enabled {
            trace!(
                "\n{}",
                arrow_cast::pretty::pretty_format_batches(&record_batches)?.to_string()
            );
        }

        Ok(SeahorseDBResultSet {
            record_batch: concat_batches(&schema, &record_batches)?,
        })
    }
}

pub trait RecordBatchHandler {
    fn to_ipc(&self) -> Result<Vec<u8>, CoralError>;
}

impl RecordBatchHandler for Vec<SeahorseDBResultSet> {
    fn to_ipc(&self) -> Result<Vec<u8>, CoralError> {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(
            Vec::new(),
            &self.first().unwrap().record_batch.schema(),
        )?;
        for resultset in self {
            writer.write(&resultset.record_batch)?;
        }
        writer.finish()?;
        Ok(writer.into_inner()?)
    }
}

impl RecordBatchHandler for RecordBatch {
    fn to_ipc(&self) -> Result<Vec<u8>, CoralError> {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(Vec::new(), &self.schema())?;
        writer.write(&self)?;
        writer.finish()?;
        Ok(writer.into_inner()?)
    }
}

pub fn schema_to_ipc(schema: &SchemaRef) -> Result<Vec<u8>, CoralError> {
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(Vec::new(), &schema)?;
    writer.finish()?;
    Ok(writer.get_ref().clone())
}

pub fn ipc_to_schema(bytes: &Vec<u8>) -> Result<SchemaRef, CoralError> {
    let reader = arrow_ipc::reader::StreamReader::try_new(bytes.as_slice(), None)?;
    Ok(reader.schema().clone())
}

pub fn resultset_to_serde_json_value(
    seahorse_resultset_list: Vec<SeahorseDBResultSet>,
) -> Result<ResultSetJson, CoralError> {
    let buf = Vec::new();
    let schema = seahorse_resultset_list
        .first()
        .unwrap()
        .record_batch
        .schema()
        .as_ref()
        .clone();
    let mut writer = arrow_json::ArrayWriter::new(buf);
    let record_batches: Vec<&RecordBatch> = seahorse_resultset_list
        .iter()
        .map(|r| &r.record_batch)
        .collect();
    writer.write_batches(record_batches.as_slice())?;
    writer.finish()?;

    let data_json = unsafe {
        sonic_rs::from_slice_unchecked::<serde_json::Value>(writer.into_inner().as_slice())
    }?;
    let schema_json = serde_json::to_value(schema).unwrap();

    Ok(ResultSetJson {
        data_json,
        schema_json,
    })
}

pub fn validate_schemas(resultsets: &Vec<SeahorseDBResultSet>) -> Result<(), CoralError> {
    if !resultsets.is_empty() {
        if !resultsets.iter().all(|r| {
            r.record_batch.schema().fields() == resultsets[0].record_batch.schema().fields()
        }) {
            return Err(CoralError::ArrowError(
                "Schemas in result sets are not match".to_string(),
            ));
        }
    }
    Ok(())
}

pub fn return_formatted_resultset(
    seahorse_resultset_list: Vec<SeahorseDBResultSet>,
    result_format: Option<ResultFormat>,
) -> Result<HttpResponse, CoralError> {
    match result_format.unwrap_or(ResultFormat::Json) {
        ResultFormat::ArrowIpc => {
            let ipc = seahorse_resultset_list.to_ipc()?;
            Ok(HttpResponse::Ok()
                .content_type(ContentType::octet_stream())
                .body(ipc))
        }
        ResultFormat::Json => {
            let ResultSetJson {
                data_json,
                schema_json,
            } = resultset_to_serde_json_value(seahorse_resultset_list)?;
            Ok(HttpResponse::Ok().json(serde_json::json!({
                "schema": schema_json,
                "data": data_json,
            })))
        }
    }
}

pub async fn return_multiple_resultsets(
    seahorse_resultset_list: Vec<SeahorseDBResultSet>,
) -> Result<HttpResponse, CoralError> {
    let mut multiple_resultset = Vec::new();
    let mut schema_json = Vec::new();

    if seahorse_resultset_list.len() == 1 {
        debug!("resultset.len() == 1, serialize resultset to json without multiple threads");
        let resultset_json = resultset_to_serde_json_value(seahorse_resultset_list)?;
        return Ok(HttpResponse::Ok().json(serde_json::json!({
            "schema": resultset_json.schema_json,
            "data_list": resultset_json.data_json,
        })));
    }

    debug!("resultset.len() > 1, serialize resultset to json on multiple threads");
    let futures = seahorse_resultset_list
        .into_iter()
        .map(|resultset| async move {
            tokio::spawn(async move {
                let resultset_json = resultset_to_serde_json_value(vec![resultset])?;
                Ok::<ResultSetJson, CoralError>(resultset_json)
            })
        });

    let future_results = join_all(futures).await;
    for future_result in future_results {
        match future_result.await {
            Ok(Ok(resultset_json)) => {
                multiple_resultset.push(resultset_json.data_json);
                schema_json.push(resultset_json.schema_json);
            }
            Ok(Err(err)) => return Err(err),
            Err(err) => return Err(CoralError::InternalError(err.to_string())),
        }
    }

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "schema": schema_json[0],
        "data_list": multiple_resultset,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_schema_to_ipc_and_ipc_to_schema() -> Result<(), CoralError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let ipc_bytes = schema_to_ipc(&schema)?;
        let recovered_schema = ipc_to_schema(&ipc_bytes)?;

        assert_eq!(schema.as_ref(), recovered_schema.as_ref());
        Ok(())
    }

    #[test]
    fn test_ipc_to_resultset() -> Result<(), CoralError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )?;

        let mut writer = arrow_ipc::writer::StreamWriter::try_new(Vec::new(), &schema)?;
        writer.write(&batch)?;
        writer.finish()?;
        let ipc_bytes = writer.into_inner()?;

        let seahorse_resulset = ipc_bytes.to_seahorse_db_resultset(false)?;

        assert_eq!(seahorse_resulset.record_batch.schema(), schema);
        assert_eq!(seahorse_resulset.record_batch, batch);

        Ok(())
    }

    #[test]
    fn test_resultset_to_serde_json_value() -> Result<(), CoralError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )?;

        let resultset = SeahorseDBResultSet {
            record_batch: batch,
        };

        let json_result = resultset_to_serde_json_value(vec![resultset])?;

        assert!(json_result.data_json.is_array());
        assert!(json_result.schema_json.is_object());

        Ok(())
    }

    #[test]
    fn test_record_batch_to_ipc() -> Result<(), CoralError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )?;

        let ipc_bytes = batch.to_ipc()?;
        let mut reader = arrow_ipc::reader::StreamReader::try_new(ipc_bytes.as_slice(), None)?;

        assert_eq!(reader.schema(), schema);
        assert_eq!(reader.next().unwrap()?, batch);

        Ok(())
    }

    #[test]
    fn test_resultsets_to_ipc() -> Result<(), CoralError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )?;

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(StringArray::from(vec!["d", "e", "f"])),
            ],
        )?;

        let concat = concat_batches(&schema, &vec![batch1.clone(), batch2.clone()])?;
        let ipc_bytes = concat.to_ipc()?;
        let mut reader = arrow_ipc::reader::StreamReader::try_new(ipc_bytes.as_slice(), None)?;

        assert_eq!(reader.schema(), schema);
        assert_eq!(reader.next().unwrap()?, concat);

        Ok(())
    }

    #[test]
    fn test_validate_schemas() -> Result<(), CoralError> {
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let schema2 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let schema3 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("age", DataType::Int32, false),
        ]));

        let seahorse_resultset1 = SeahorseDBResultSet {
            record_batch: RecordBatch::try_new(
                schema1.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec!["a", "b", "c"])),
                ],
            )?,
        };

        let seahorse_resultset2 = SeahorseDBResultSet {
            record_batch: RecordBatch::try_new(
                schema2.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![4, 5, 6])),
                    Arc::new(StringArray::from(vec!["d", "e", "f"])),
                ],
            )?,
        };

        let seahorse_resultset3 = SeahorseDBResultSet {
            record_batch: RecordBatch::try_new(
                schema3.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(Int32Array::from(vec![4, 5, 6])),
                ],
            )?,
        };

        // Test with matching schemas
        assert!(validate_schemas(&vec![
            seahorse_resultset1.clone(),
            seahorse_resultset2.clone()
        ])
        .is_ok());

        // Test with non-matching schemas
        assert!(validate_schemas(&vec![
            seahorse_resultset1.clone(),
            seahorse_resultset3.clone()
        ])
        .is_err());

        // Test with empty vector
        assert!(validate_schemas(&vec![]).is_ok());

        Ok(())
    }
}
