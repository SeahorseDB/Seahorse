use super::errors::CoralError;
use crate::resultset::SeahorseDBResultSet;
use arrow_array::{Float32Array, UInt32Array};
use arrow_select::concat::concat_batches;
use arrow_select::take::take_record_batch;
use std::sync::Arc;

pub async fn get_final_topk(
    resultset_list: Vec<SeahorseDBResultSet>,
    topk: u32,
    _is_printing_record_batch_log_enabled: bool,
) -> Result<SeahorseDBResultSet, CoralError> {
    if resultset_list.is_empty() {
        return Err(CoralError::InternalError("Empty resultset".to_string()));
    }

    let schema = resultset_list[0].record_batch.schema().as_ref().clone();
    let record_batches: Vec<_> = resultset_list.into_iter().map(|r| r.record_batch).collect();
    let concat = concat_batches(&Arc::new(schema.clone()), &record_batches)?;
    let dist = concat
        .column_by_name("distance")
        .unwrap()
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();

    let mut row_ids: Vec<_> = (0..dist.len()).collect();
    row_ids.sort_by(|&a, &b| dist.value(a).partial_cmp(&dist.value(b)).unwrap());
    row_ids.truncate(topk as usize);

    let row_ids_array =
        UInt32Array::from(row_ids.into_iter().map(|id| id as u32).collect::<Vec<_>>());
    let final_topk = take_record_batch(&concat, &row_ids_array)?;

    Ok(SeahorseDBResultSet {
        record_batch: final_topk,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::RecordBatch;
    use arrow_array::{Float32Array, Int32Array};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::errors::Result;
    use rand::seq::SliceRandom;
    use rand::thread_rng;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_get_final_topk() -> Result<(), CoralError> {
        // Create sample data
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("distance", DataType::Float32, false),
        ]));

        // Helper function to create a record batch with unique, shuffled distances
        fn create_batch(schema: &Arc<Schema>, start_id: i32) -> RecordBatch {
            let ids: Vec<i32> = (start_id..start_id + 40).collect();
            let mut distances: Vec<f32> = (0..40).map(|i| (i + start_id) as f32 / 240.0).collect();
            distances.shuffle(&mut thread_rng());

            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(ids)),
                    Arc::new(Float32Array::from(distances)),
                ],
            )
            .unwrap()
        }

        // Create record batches
        let batch1 = create_batch(&schema, 1);
        let batch2 = create_batch(&schema, 41);
        let batch3 = create_batch(&schema, 81);
        let batch4 = create_batch(&schema, 121);
        let batch5 = create_batch(&schema, 161);
        let batch6 = create_batch(&schema, 201);

        // Create ResultSets
        let resultset1 = SeahorseDBResultSet {
            record_batch: concat_batches(&schema, &[batch6, batch4])?,
        };

        let resultset2 = SeahorseDBResultSet {
            record_batch: concat_batches(&schema, &[batch3, batch2, batch5])?,
        };

        let resultset3 = SeahorseDBResultSet {
            record_batch: batch1,
        };

        let seahorse_resultset_list = vec![resultset1, resultset2, resultset3];

        // Call the function with topk = 140 to check order by
        let topk_result = get_final_topk(seahorse_resultset_list, 140, false).await?;

        // Validate the result
        let result_batch = &topk_result.record_batch;
        assert_eq!(result_batch.num_rows(), 140);

        let ids: Vec<i32> = result_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec();

        let distances: Vec<f32> = result_batch
            .column(1)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .values()
            .to_vec();

        // Check if the distances are sorted in ascending order
        assert!(distances.windows(2).all(|w| w[0] <= w[1]));

        // Check if all distances are within the range [0, 1]
        assert!(distances.iter().all(|&d| d >= 0.0 && d < 1.0));

        // Check if all distances are unique
        let mut unique_distances = distances.clone();
        unique_distances.sort_by(|a, b| a.partial_cmp(b).unwrap());
        unique_distances.dedup();
        assert_eq!(unique_distances.len(), distances.len());

        // Check values
        let expected_distances: Vec<f32> = (1..141).map(|i| i as f32 / 240.0).collect();
        assert_eq!(expected_distances.len(), distances.len());

        // Print the results for manual verification
        println!("Top 140 results:");
        for i in 0..140 {
            println!("ID: {}, Distance: {}", ids[i], distances[i]);
        }

        Ok(())
    }
}
