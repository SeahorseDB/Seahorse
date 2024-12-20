use actix_multipart::form::{json::Json as MpJson, tempfile::TempFile, MultipartForm};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct QueryRequest {
    pub query: String,
    pub posted_time: Option<NaiveDateTime>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Node {
    pub name: String,
    pub endpoint: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Nodes {
    pub nodes: Vec<Node>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodePingResult {
    pub node_name: String,
    pub result: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CreateTableSql {
    pub ddl: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TablesResponse {
    pub tables: Vec<String>,
}

#[derive(Serialize, Debug, Clone)]
pub struct TableSchema {
    pub schema: arrow_schema::Schema,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ResultFormat {
    Json,
    ArrowIpc,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ScanRequest {
    pub table_name: String,
    pub projection: String,
    pub filter: String,
    pub result_format: Option<ResultFormat>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AnnRequest {
    pub table_name: String,
    pub topk: u32,
    pub vector: Option<String>,
    pub text: Option<String>,
    pub ef_search: Option<u32>,
    pub projection: Option<String>,
    pub filter: Option<String>,
    pub result_format: Option<ResultFormat>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BatchAnnRequest {
    pub table_name: String,
    pub topk: u32,
    pub vectors: Vec<Vec<f32>>,
    pub ef_search: Option<u32>,
    pub projection: Option<String>,
    pub filter: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ResultSetResponse {
    pub result_sets: Vec<serde_json::Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum FileFormat {
    None,
    Json,
    Parquet,
    ArrowIpc,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GenerateCreateTableSqlParameters {
    pub name: String,
    pub format: Option<FileFormat>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Rows {
    pub data: Vec<serde_json::Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataInsertParameters {
    pub format: FileFormat,
    pub batch_insert_size: Option<usize>,
    pub is_async: Option<bool>,
}

#[derive(Debug, MultipartForm)]
pub struct InsertForm {
    #[multipart(limit = "5000MB")]
    pub file: TempFile,
    pub json: MpJson<DataInsertParameters>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataInsertResult {
    pub inserted_record_batches: usize,
    pub inserted_row_count: usize,
    pub schema: arrow_schema::Schema,
    pub elapsed_time: Option<f32>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LogLevel {
    pub level: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PrintRecordBatchSetting {
    pub enabled: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EmbeddingRequest {
    pub text: String,
    pub is_normalized: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EmbeddingResult {
    pub feature: Vec<f32>,
    pub duration: f32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TableIndexedCount {
    pub indexed_count: i32,
    pub total_row_count: i32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataImportRequest {
    pub file_path: String,
    pub format: FileFormat,
    pub batch_insert_size: Option<usize>,
    pub is_async: Option<bool>,
}
