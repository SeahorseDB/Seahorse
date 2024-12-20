use arrow_schema::SchemaRef;
use log::debug;

pub trait SchemaExt {
    fn get_segment_id_columns_from_schema(&self) -> Option<Vec<String>>;
}

impl SchemaExt for SchemaRef {
    fn get_segment_id_columns_from_schema(&self) -> Option<Vec<String>> {
        self.metadata().get("segment_id_info").and_then(|value| {
            let bytes = value.as_bytes();
            let mut vec: Vec<i32> = Vec::new();
            for chunk in bytes.chunks(4) {
                vec.push(i32::from_le_bytes(chunk.try_into().unwrap_or_default()));
            }
            debug!("get segment id columns. vec={:?}", vec);
            let columns = vec
                .iter()
                .map(|idx| self.field(*idx as usize).name().clone())
                .collect::<Vec<_>>();
            Some(columns)
        })
    }
}
