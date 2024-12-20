use super::errors::*;
use super::models::*;
use super::resultset::validate_schemas;
use super::resultset::SeahorseDBResultSet;
use super::topk::*;
use futures::future::join_all;
use log::debug;
use std::sync::Arc;

pub async fn get_embedding(
    embedding_request: EmbeddingRequest,
) -> Result<EmbeddingResult, CoralError> {
    let embedding_api_url = std::env::var("EMBEDDING_API_URL").unwrap_or("".to_string());
    if embedding_api_url.is_empty() {
        return Err(CoralError::InternalError(
            "EMBEDDING_API_URL is not set".to_string(),
        ));
    }

    let client = reqwest::Client::new();
    let response = client
        .post(embedding_api_url)
        .json(&embedding_request)
        .send()
        .await?;
    let embedding = response.json::<EmbeddingResult>().await?;
    Ok(embedding)
}

pub fn convert_vectors_to_record_batch(
    vectors: Vec<Vec<f32>>,
) -> Result<arrow_array::RecordBatch, CoralError> {
    if vectors.is_empty() {
        return Err(CoralError::BadRequest(
            "Input vectors are empty".to_string(),
        ));
    }

    let expected_length = vectors[0].len();
    if !vectors.iter().all(|v| v.len() == expected_length) {
        return Err(CoralError::BadRequest(
            "All input vectors must have the same dimension".to_string(),
        ));
    }

    let vectors_rb_schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "query",
        arrow_schema::DataType::FixedSizeList(
            Arc::new(arrow_schema::Field::new(
                "item",
                arrow_schema::DataType::Float32,
                false,
            )),
            vectors[0].len() as i32,
        ),
        false,
    )]);

    let flat_values: Vec<f32> = vectors.iter().flatten().cloned().collect();

    let value_data = arrow_data::ArrayData::builder(arrow_schema::DataType::Float32)
        .len(flat_values.len())
        .add_buffer(arrow_buffer::Buffer::from_slice_ref(&flat_values))
        .build()
        .unwrap();

    let list_data_type = arrow_schema::DataType::FixedSizeList(
        Arc::new(arrow_schema::Field::new(
            "item",
            arrow_schema::DataType::Float32,
            false,
        )),
        vectors[0].len() as i32,
    );

    let list_data = arrow_data::ArrayData::builder(list_data_type.clone())
        .len(vectors.len())
        .add_child_data(value_data.clone())
        .build()
        .unwrap();

    let fixed_size_list_array = arrow_array::FixedSizeListArray::from(list_data);

    let batch = arrow_array::RecordBatch::try_new(
        Arc::new(vectors_rb_schema),
        vec![Arc::new(fixed_size_list_array)],
    )?;

    Ok(batch)
}

pub async fn get_final_topk_list_from_batch_ann_resultsets(
    batch_ann_resultsets_list: Vec<Vec<SeahorseDBResultSet>>,
    topk: u32,
    is_printing_record_batch_log_enabled: bool,
) -> Result<Vec<SeahorseDBResultSet>, CoralError> {
    let mut aggregated_results_by_query = Vec::new();
    for query_index in 0..batch_ann_resultsets_list[0].len() {
        let mut aggregated_resultset_list = Vec::new();
        for batch_ann_resultsets in &batch_ann_resultsets_list {
            if let Some(resultset) = batch_ann_resultsets.get(query_index) {
                aggregated_resultset_list.push(resultset.clone());
            }
        }
        aggregated_results_by_query.push(aggregated_resultset_list);
    }

    let mut final_topk_list = Vec::new();
    if aggregated_results_by_query.len() == 1 {
        debug!("aggregated_results_by_query.len() == 1, so running final topk on single thread");
        let aggregated_results = aggregated_results_by_query.pop().unwrap();
        validate_schemas(&aggregated_results)?;
        let final_topk = get_final_topk(
            aggregated_results,
            topk,
            is_printing_record_batch_log_enabled,
        )
        .await?;
        final_topk_list.push(final_topk);
    } else {
        debug!("aggregated_results_by_query.len() > 1, running final topk on multiple threads");
        let futures =
            aggregated_results_by_query
                .into_iter()
                .map(|aggregated_resultset_list| async move {
                    tokio::spawn(async move {
                        validate_schemas(&aggregated_resultset_list)?;
                        let final_topk = get_final_topk(
                            aggregated_resultset_list,
                            topk,
                            is_printing_record_batch_log_enabled,
                        )
                        .await?;
                        Ok::<SeahorseDBResultSet, CoralError>(final_topk)
                    })
                });
        let future_results = join_all(futures).await;
        for future_result in future_results {
            match future_result.await {
                Ok(Ok(result)) => final_topk_list.push(result),
                Ok(Err(e)) => return Err(CoralError::InternalError(e.to_string())),
                Err(e) => return Err(CoralError::InternalError(e.to_string())),
            }
        }
    }

    Ok(final_topk_list)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use arrow_cast::pretty::print_batches;

    #[test]
    fn test_convert_vectors_to_record_batch() {
        // Test case 1: Valid input
        let vectors = vec![
            vec![1.0, 2.0, 3.0],
            vec![4.0, 5.0, 6.0],
            vec![7.0, 8.0, 9.0],
        ];
        let result = convert_vectors_to_record_batch(vectors.clone());
        assert!(result.is_ok());

        let record_batch = result.unwrap();
        assert_eq!(record_batch.num_columns(), 1);
        assert_eq!(record_batch.schema().field(0).name(), "query");
        assert_eq!(record_batch.num_rows(), 3);

        let column = record_batch.column(0);
        let fixed_size_list_array = column
            .as_any()
            .downcast_ref::<arrow_array::FixedSizeListArray>()
            .unwrap();
        assert_eq!(fixed_size_list_array.len(), 3);
        assert_eq!(fixed_size_list_array.value_length(), 3);

        let values = fixed_size_list_array.values();
        let float_array = values
            .as_any()
            .downcast_ref::<arrow_array::Float32Array>()
            .unwrap();
        assert_eq!(float_array.len(), 9);

        for (i, vector) in vectors.iter().enumerate() {
            for (j, &value) in vector.iter().enumerate() {
                assert_eq!(float_array.value(i * 3 + j), value);
            }
        }

        // Test case 2: Empty input
        let empty_vectors: Vec<Vec<f32>> = vec![];
        let empty_result = convert_vectors_to_record_batch(empty_vectors);
        assert!(empty_result.is_err());
        assert!(matches!(
            empty_result.unwrap_err(),
            CoralError::BadRequest(_)
        ));

        // Test case 3: Inconsistent vector lengths
        let inconsistent_vectors = vec![
            vec![1.0, 2.0, 3.0],
            vec![4.0, 5.0], // This vector has a different length
            vec![7.0, 8.0, 9.0],
        ];
        let inconsistent_result = convert_vectors_to_record_batch(inconsistent_vectors);
        assert!(inconsistent_result.is_err());
        assert!(matches!(
            inconsistent_result.unwrap_err(),
            CoralError::BadRequest(_)
        ));
    }

    #[tokio::test]
    async fn test_get_final_topk_list_from_batch_ann_resultsets() {
        let create_mock_record_batch = |base: f32| {
            let schema = Arc::new(arrow_schema::Schema::new(vec![
                arrow_schema::Field::new("id", arrow_schema::DataType::Int32, false),
                arrow_schema::Field::new("distance", arrow_schema::DataType::Float32, false),
            ]));

            let id_array = arrow_array::Int32Array::from(vec![1, 2, 3, 4, 5]);
            let distance_array = arrow_array::Float32Array::from(vec![
                base + 0.1,
                base + 0.2,
                base + 0.3,
                base + 0.4,
                base + 0.5,
            ]);

            arrow_array::RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(id_array), Arc::new(distance_array)],
            )
            .unwrap()
        };

        let create_mock_resultset = |base: f32| SeahorseDBResultSet {
            record_batch: create_mock_record_batch(base),
        };

        let batch_ann_resultsets_list = vec![
            vec![
                create_mock_resultset(0.0),
                create_mock_resultset(11.0),
                create_mock_resultset(12.0),
            ],
            vec![
                create_mock_resultset(5.0),
                create_mock_resultset(1.0),
                create_mock_resultset(7.0),
            ],
            vec![
                create_mock_resultset(10.0),
                create_mock_resultset(3.0),
                create_mock_resultset(2.0),
            ],
        ];

        let topk = 3;
        let is_printing_record_batch_log_enabled = false;

        let result = get_final_topk_list_from_batch_ann_resultsets(
            batch_ann_resultsets_list,
            topk,
            is_printing_record_batch_log_enabled,
        )
        .await;

        assert!(result.is_ok());
        let final_topk_list = result.unwrap();

        // Check if we got the expected number of results
        assert_eq!(final_topk_list.len(), 3);

        for (i, resultset) in final_topk_list.iter().enumerate() {
            let record_batch = &resultset.record_batch;

            let _ = print_batches(&[record_batch.clone()]);

            // Check if the record batch has the correct number of rows (topk)
            assert_eq!(record_batch.num_rows(), topk as usize);

            // Check if the record batch has the correct columns
            assert_eq!(record_batch.num_columns(), 2);
            assert_eq!(record_batch.schema().field(0).name(), "id");
            assert_eq!(record_batch.schema().field(1).name(), "distance");

            // Check if the distances are sorted in ascending order
            let distance_array = record_batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::Float32Array>()
                .unwrap();

            let distances: Vec<f32> = distance_array.iter().map(|v| v.unwrap()).collect();
            assert!(distances.windows(2).all(|w| w[0] <= w[1]));

            // Check if the base distance is correct for each resultset
            for (j, distance) in distances.iter().enumerate() {
                assert!(distance.clone() == i as f32 * 1.0 + (j + 1) as f32 * 0.1);
            }
        }
    }
}
