use super::ddl::*;
use super::errors::CoralError;
use super::models::*;
use super::pool::*;
use super::reader::{
    JsonAsyncReader, JsonSyncReader, ParquetAsyncReader, ParquetSyncReader, SegmentReader,
};
use super::resultset::*;
use super::seahorse::*;
use super::state::AppState;
use super::topk::*;
use super::utils::*;
use actix_multipart::form::MultipartForm;
use actix_web::Either;
use actix_web::{web, HttpResponse};
use arrow_array::RecordBatchReader;
use bytes::Bytes;
use futures::future::join_all;
use log::debug;
use log::LevelFilter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

pub async fn health_check_handler(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, CoralError> {
    let health_check_response = &app_state.health_check_response;
    let mut visit_count = app_state.visit_count.lock().unwrap();
    let response = format!("{} {} times", health_check_response, visit_count);
    *visit_count += 1;
    Ok(HttpResponse::Ok().json(&response))
}

pub async fn get_log_level_handler() -> Result<HttpResponse, CoralError> {
    let current_level = log::max_level();
    Ok(HttpResponse::Ok().json(current_level.as_str()))
}

pub async fn post_log_level_handler(
    level: web::Json<LogLevel>,
) -> Result<HttpResponse, CoralError> {
    let new_level = match level.level.to_lowercase().as_str() {
        "error" => LevelFilter::Error,
        "warn" => LevelFilter::Warn,
        "info" => LevelFilter::Info,
        "debug" => LevelFilter::Debug,
        "trace" => LevelFilter::Trace,
        _ => return Err(CoralError::BadRequest("Invalid log level".to_string())),
    };

    log::set_max_level(new_level);
    Ok(HttpResponse::Ok().json(new_level.as_str()))
}

pub async fn get_log_print_record_batch_handler(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, CoralError> {
    let state = app_state
        .is_printing_record_batch_log_enabled
        .lock()
        .unwrap();
    Ok(HttpResponse::Ok().json(*state))
}

pub async fn post_log_print_record_batch_handler(
    app_state: web::Data<AppState>,
    new_state: web::Json<PrintRecordBatchSetting>,
) -> Result<HttpResponse, CoralError> {
    let mut state = app_state
        .is_printing_record_batch_log_enabled
        .lock()
        .unwrap();
    *state = new_state.enabled;
    Ok(HttpResponse::Ok().json(*state))
}

pub async fn post_nodes_handler(
    app_state: web::Data<AppState>,
    nodes: web::Json<Nodes>,
) -> Result<HttpResponse, CoralError> {
    debug!("POST nodes handler: nodes={:?}", nodes);

    let mut added_nodes = Vec::new();

    for node in &nodes.nodes {
        if !node.endpoint.starts_with("redis://") {
            return Err(CoralError::InternalError(format!(
                "Invalid SeahorseDB endpoint format, should start with 'redis://': {}",
                &node.endpoint
            )));
        }
        let mut guard = app_state.node_pools.write().unwrap();
        let pool = create_redis_pool(&node.endpoint);
        let mut conn = pool.get().await?;
        match shdb_ping(&mut conn).await {
            Ok(_) => {
                guard.insert(node.name.clone(), pool);
                added_nodes.push(node.clone());
            }
            Err(err) => {
                return Err(CoralError::NotFound(format!(
                    "Could not access SeahorseDB node '{}' due to error '{}'",
                    &node.endpoint,
                    err.to_string()
                )))
            }
        }
    }

    Ok(HttpResponse::Ok().json(added_nodes))
}

pub async fn get_nodes_handler(app_state: web::Data<AppState>) -> Result<HttpResponse, CoralError> {
    let pools = app_state.node_pools.read().unwrap();
    let endpoints: Vec<String> = pools.keys().cloned().collect();
    Ok(HttpResponse::Ok().json(endpoints))
}

pub async fn delete_node_handler(
    app_state: web::Data<AppState>,
    node_name: web::Path<String>,
) -> Result<HttpResponse, CoralError> {
    let mut pools = app_state.node_pools.write().unwrap();
    let name = node_name.into_inner().clone();
    if pools.contains_key(&name) {
        let pool = pools.remove(&name);
        match pool {
            Some(_) => {
                Ok(HttpResponse::Ok().body(format!("'{name}' has been unregisterd from service")))
            }
            None => Err(CoralError::NotFound(format!(
                "Could not unregister '{name}'"
            ))),
        }
    } else {
        Err(CoralError::NotFound(format!("Could not find '{name}'")))
    }
}

pub async fn get_node_ping_handler(
    app_state: web::Data<AppState>,
    node_name: web::Path<String>,
) -> Result<HttpResponse, CoralError> {
    let pools = app_state.node_pools.read().unwrap();
    let name = node_name.to_string();
    match pools.get(&name) {
        Some(pool) => {
            let mut conn = pool.get().await?;
            let result = shdb_ping(&mut conn).await?;
            Ok(HttpResponse::Ok().json(NodePingResult {
                node_name: name,
                result,
            }))
        }
        None => Err(CoralError::NotFound(format!(
            "Could not find node '{name}'"
        ))),
    }
}

pub async fn post_node_tables_handler(
    app_state: web::Data<AppState>,
    node_name: web::Path<String>,
    request: web::Json<CreateTableSql>,
) -> Result<HttpResponse, CoralError> {
    let pools = app_state.node_pools.read().unwrap();
    let name = node_name.to_string();
    match pools.get(&name) {
        Some(pool) => {
            let schema = ddl_to_arrow_schema(&*request.ddl)?;
            let ipc_message = schema_to_ipc(&schema)?;
            let mut conn = pool.get().await?;
            let _ = shdb_create_table(&mut conn, &ipc_message).await?;
            Ok(HttpResponse::Ok().json(request))
        }
        None => Err(CoralError::NotFound(format!(
            "Could not find node '{name}'"
        ))),
    }
}

pub async fn get_node_tables_handler(
    app_state: web::Data<AppState>,
    node_name: web::Path<String>,
) -> Result<HttpResponse, CoralError> {
    let pools = app_state.node_pools.read().unwrap();
    let name = node_name.to_string();
    match pools.get(&name) {
        Some(pool) => {
            let mut conn = pool.get().await?;
            let tables = shdb_list_tables(&mut conn).await?;
            Ok(HttpResponse::Ok().json(TablesResponse { tables }))
        }
        None => Err(CoralError::NotFound(format!(
            "Could not find node '{name}'"
        ))),
    }
}

pub async fn get_node_table_schema_handler(
    app_state: web::Data<AppState>,
    path: web::Path<(String, String)>,
) -> Result<HttpResponse, CoralError> {
    let pools = app_state.node_pools.read().unwrap();
    let (node_name, table_name) = path.into_inner().clone();
    match pools.get(&node_name) {
        Some(pool) => {
            let mut conn = pool.get().await?;
            let describe_result = shdb_describe_table(&mut conn, &table_name).await?;
            let schema_ref = ipc_to_schema(&describe_result)?;
            Ok(HttpResponse::Ok().json(TableSchema {
                schema: schema_ref.as_ref().clone(),
            }))
        }
        None => Err(CoralError::NotFound(format!(
            "Could not find node '{node_name}'"
        ))),
    }
}

pub async fn put_node_table_handler(
    app_state: web::Data<AppState>,
    path: web::Path<(String, String)>,
    payload: Either<web::Json<serde_json::Value>, MultipartForm<InsertForm>>,
) -> Result<HttpResponse, CoralError> {
    let pools = app_state.node_pools.read().unwrap();
    let (node_name, table_name) = path.into_inner().clone();
    let pool = match pools.get(&node_name) {
        Some(p) => p,
        None => {
            return Err(CoralError::NotFound(format!(
                "Could not find node '{node_name}'"
            )))
        }
    };

    let mut conn = pool.get().await?;
    let table_schema = shdb_get_table_schema(&mut conn, &table_name).await?;
    let is_printing_record_batch_log_enabled = *app_state
        .is_printing_record_batch_log_enabled
        .lock()
        .unwrap();
    let result = match payload {
        Either::Left(json) => {
            debug!("Insert a row from json");
            let result = JsonSyncReader::try_new(
                std::io::Cursor::new(json.to_string().into_bytes()),
                table_schema.clone(),
                None,
            )?
            .insert_all(&mut conn, &table_name, is_printing_record_batch_log_enabled)
            .await?;
            result
        }
        Either::Right(MultipartForm(form)) => {
            debug!("Insert a row from multipart form");
            let format = form.json.format.clone();
            let batch_insert_size = form.json.batch_insert_size;
            let is_async = form.json.is_async.unwrap_or(false);
            let result = match format {
                FileFormat::Parquet => {
                    let result: Result<DataInsertResult, CoralError> = if is_async {
                        debug!("Data import from parquet file started with async mode");
                        let result = ParquetAsyncReader::try_new(
                            tokio::fs::File::from_std(form.file.file.reopen()?),
                            table_schema.clone(),
                            batch_insert_size,
                        )
                        .await?
                        .insert_all(&mut conn, &table_name, is_printing_record_batch_log_enabled)
                        .await?;
                        Ok(result)
                    } else {
                        debug!("Data import from parquet file started with sync mode");
                        let result = ParquetSyncReader::try_new(
                            form.file,
                            table_schema.clone(),
                            batch_insert_size,
                        )?
                        .insert_all(&mut conn, &table_name, is_printing_record_batch_log_enabled)
                        .await?;
                        Ok(result)
                    };
                    result?
                }
                FileFormat::Json => {
                    let result: Result<DataInsertResult, CoralError> = if is_async {
                        debug!("Data import from json file started with async mode");
                        let file = tokio::fs::File::from_std(form.file.file.reopen()?);
                        let reader = tokio::io::BufReader::new(file);
                        let result = JsonAsyncReader::try_new(
                            reader,
                            table_schema.clone(),
                            batch_insert_size,
                        )?
                        .insert_all(&mut conn, &table_name, is_printing_record_batch_log_enabled)
                        .await?;
                        Ok(result)
                    } else {
                        debug!("Data import from json file started with sync mode");
                        let result = JsonSyncReader::try_new(
                            std::io::Cursor::new(tokio::fs::read(form.file.file.path()).await?),
                            table_schema.clone(),
                            batch_insert_size,
                        )?
                        .insert_all(&mut conn, &table_name, is_printing_record_batch_log_enabled)
                        .await?;
                        Ok(result)
                    };
                    result?
                }
                _ => {
                    return Err(CoralError::InternalError(format!(
                        "Not supported format '{:?}'",
                        format
                    )))
                }
            };
            result
        }
    };

    Ok(HttpResponse::Ok().json(result))
}

pub async fn post_node_table_import_handler(
    app_state: web::Data<AppState>,
    path: web::Path<(String, String)>,
    request: web::Json<DataImportRequest>,
) -> Result<HttpResponse, CoralError> {
    debug!("Import handler:\nrequest={:?}", request);
    let pools = app_state.node_pools.read().unwrap();
    let (node_name, table_name) = path.into_inner().clone();
    let pool = match pools.get(&node_name) {
        Some(p) => p,
        None => {
            return Err(CoralError::NotFound(format!(
                "Could not find node '{node_name}'"
            )))
        }
    };

    let mut conn = pool.get().await?;
    let file_path = &request.file_path;
    let format = &request.format;
    let is_async = request.is_async.unwrap_or(false);
    let table_schema = shdb_get_table_schema(&mut conn, &table_name).await?;
    let is_printing_record_batch_log_enabled = *app_state
        .is_printing_record_batch_log_enabled
        .lock()
        .unwrap();

    let result = match format {
        FileFormat::Parquet => {
            let result: Result<DataInsertResult, CoralError> = if is_async {
                debug!("Data import from parquet file started with async mode");
                let result = ParquetAsyncReader::try_new(
                    tokio::fs::File::open(file_path).await?,
                    table_schema.clone(),
                    request.batch_insert_size,
                )
                .await?
                .insert_all(&mut conn, &table_name, is_printing_record_batch_log_enabled)
                .await?;
                Ok(result)
            } else {
                debug!("Data import from parquet file started with sync mode");
                let result = ParquetSyncReader::try_new(
                    std::io::Cursor::new(tokio::fs::read(file_path).await?),
                    table_schema.clone(),
                    request.batch_insert_size,
                )?
                .insert_all(&mut conn, &table_name, is_printing_record_batch_log_enabled)
                .await?;
                Ok(result)
            };
            result?
        }
        FileFormat::Json => {
            let result: Result<DataInsertResult, CoralError> = if is_async {
                debug!("Data import from json file started with async mode");
                let file = tokio::fs::File::open(file_path).await?;
                let reader = tokio::io::BufReader::new(file);
                let result = JsonAsyncReader::try_new(
                    reader,
                    table_schema.clone(),
                    request.batch_insert_size,
                )?
                .insert_all(&mut conn, &table_name, is_printing_record_batch_log_enabled)
                .await?;
                Ok(result)
            } else {
                debug!("Data import from json file started with sync mode");
                let result = JsonSyncReader::try_new(
                    std::io::Cursor::new(tokio::fs::read(file_path).await?),
                    table_schema.clone(),
                    request.batch_insert_size,
                )?
                .insert_all(&mut conn, &table_name, is_printing_record_batch_log_enabled)
                .await?;
                Ok(result)
            };
            result?
        }
        _ => {
            return Err(CoralError::BadRequest(format!(
                "Not supported format '{:?}'",
                format
            )))
        }
    };

    Ok(HttpResponse::Ok().json(result))
}

pub async fn delete_node_table_handler(
    app_state: web::Data<AppState>,
    path: web::Path<(String, String)>,
) -> Result<HttpResponse, CoralError> {
    let pools = app_state.node_pools.read().unwrap();
    let (node_name, table_name) = path.into_inner().clone();
    match pools.get(&node_name) {
        Some(pool) => {
            let mut conn = pool.get().await?;
            let drop_result = shdb_drop_table(&mut conn, &table_name).await?;
            if drop_result == "OK" {
                Ok(HttpResponse::Ok().body(drop_result))
            } else {
                Err(CoralError::InternalError(format!(
                    "Could not drop table due to error '{drop_result}'"
                )))
            }
        }
        None => Err(CoralError::NotFound(format!(
            "Could not find node '{node_name}'"
        ))),
    }
}

pub async fn post_scan_handler(
    app_state: web::Data<AppState>,
    query_request: web::Json<ScanRequest>,
) -> Result<HttpResponse, CoralError> {
    debug!("SCAN handler:\nrequest={:?}", query_request);

    let pools = app_state.node_pools.read().unwrap();
    let is_printing_record_batch_log_enabled = *app_state
        .is_printing_record_batch_log_enabled
        .lock()
        .unwrap();
    let futures = pools.iter().map(|(node, pool)| async {
        let table_name = query_request.table_name.clone();
        let projection = query_request.projection.clone();
        let filter = query_request.filter.clone();
        let node = node.clone();
        let conn = pool.get().await;
        tokio::spawn(async move {
            debug!("SCAN request to {}", node);
            let result = shdb_table_scan(&mut conn?, &table_name, &projection, &filter)
                .await?
                .to_seahorse_db_resultset(is_printing_record_batch_log_enabled)?;
            Ok::<SeahorseDBResultSet, CoralError>(result)
        })
    });

    let future_results = join_all(futures).await;
    let mut seahorse_resultset_list = Vec::new();
    for seahorse_resultset in future_results {
        match seahorse_resultset.await {
            Ok(Ok(result)) => seahorse_resultset_list.push(result),
            Ok(Err(err)) => return Err(err),
            Err(err) => return Err(CoralError::InternalError(err.to_string())),
        }
    }

    validate_schemas(&seahorse_resultset_list)?;
    return_formatted_resultset(seahorse_resultset_list, query_request.result_format.clone())
}

pub async fn post_ann_handler(
    app_state: web::Data<AppState>,
    query_request: web::Json<AnnRequest>,
) -> Result<HttpResponse, CoralError> {
    debug!("ANN handler:\nrequest={:?}", query_request);

    let vector: Vec<f32>;
    if query_request.vector.is_some() {
        debug!("vector is provided");
        vector = query_request
            .clone()
            .vector
            .unwrap()
            .split(',')
            .map(|s| s.trim().parse::<f32>())
            .collect::<Result<Vec<f32>, _>>()?;
    } else if query_request.text.is_some() {
        let text = query_request.clone().text.unwrap();
        let embedding_result = get_embedding(EmbeddingRequest {
            text,
            is_normalized: Some(true),
        })
        .await?;
        debug!(
            "text provided. text embedding result: {:?}",
            embedding_result
        );
        vector = embedding_result.feature;
    } else {
        return Err(CoralError::BadRequest(
            "Vector or text must be provided".to_string(),
        ));
    }

    let pools = app_state.node_pools.read().unwrap();
    let is_printing_record_batch_log_enabled = *app_state
        .is_printing_record_batch_log_enabled
        .lock()
        .unwrap();
    let futures = pools.iter().map(|(node, pool)| async {
        let table_name = query_request.table_name.clone();
        let topk = query_request.topk.clone();
        let vector = vector.clone();
        let filter = query_request.filter.clone();
        let ef_search = query_request.ef_search.clone();
        let projection = query_request.projection.clone();
        let node = node.clone();
        let conn = pool.get().await;
        tokio::spawn(async move {
            debug!("ANN request to {}", node);
            let resultset = shdb_table_ann(
                &mut conn?,
                &table_name,
                &topk,
                &vector,
                &ef_search,
                &projection,
                &filter,
            )
            .await?
            .to_seahorse_db_resultset(is_printing_record_batch_log_enabled)?;
            Ok::<SeahorseDBResultSet, CoralError>(resultset)
        })
    });

    let future_results = join_all(futures).await;
    let mut seahorse_resultset_list = Vec::new();
    for seahorse_resultset in future_results {
        match seahorse_resultset.await {
            Ok(Ok(result)) => seahorse_resultset_list.push(result),
            Ok(Err(err)) => return Err(err),
            Err(err) => return Err(CoralError::InternalError(err.to_string())),
        }
    }

    validate_schemas(&seahorse_resultset_list)?;
    let final_topk = get_final_topk(
        seahorse_resultset_list,
        query_request.topk,
        is_printing_record_batch_log_enabled,
    )
    .await?;

    return_formatted_resultset(vec![final_topk], query_request.result_format.clone())
}

pub async fn post_batch_ann_handler(
    app_state: web::Data<AppState>,
    batch_request: web::Json<BatchAnnRequest>,
) -> Result<HttpResponse, CoralError> {
    debug!("BATCH ANN handler:\nrequest={:?}", batch_request);

    let ipc = &convert_vectors_to_record_batch(batch_request.vectors.clone())?.to_ipc()?;

    let pools = app_state.node_pools.read().unwrap();
    let is_printing_record_batch_log_enabled = *app_state
        .is_printing_record_batch_log_enabled
        .lock()
        .unwrap();
    let futures = pools.iter().map(|(node, pool)| async {
        let table_name = batch_request.table_name.clone();
        let topk = batch_request.topk.clone();
        let vector = ipc.clone();
        let filter = batch_request.filter.clone();
        let ef_search = batch_request.ef_search.clone();
        let projection = batch_request.projection.clone();
        let node = node.clone();
        let conn = pool.get().await;
        tokio::spawn(async move {
            debug!("Batch ANN request to {}", node);
            let batch_ann_results = shdb_table_batch_ann(
                &mut conn?,
                &table_name,
                &topk,
                &vector,
                &ef_search,
                &projection,
                &filter,
            )
            .await?;
            let mut seahorse_resultset_list = Vec::new();
            for result in batch_ann_results {
                let seahorse_resultset =
                    result.to_seahorse_db_resultset(is_printing_record_batch_log_enabled)?;
                seahorse_resultset_list.push(seahorse_resultset);
            }
            Ok::<Vec<SeahorseDBResultSet>, CoralError>(seahorse_resultset_list)
        })
    });

    let future_results = join_all(futures).await;
    let mut batch_ann_resultsets_list = Vec::new();
    for batch_ann_resultsets in future_results {
        match batch_ann_resultsets.await {
            Ok(Ok(result)) => batch_ann_resultsets_list.push(result),
            Ok(Err(err)) => return Err(err),
            Err(err) => return Err(CoralError::InternalError(err.to_string())),
        }
    }

    let final_topk_list = get_final_topk_list_from_batch_ann_resultsets(
        batch_ann_resultsets_list,
        batch_request.topk,
        is_printing_record_batch_log_enabled,
    )
    .await?;

    return_multiple_resultsets(final_topk_list).await
}

pub async fn post_ddl_from_file(
    params: web::Query<GenerateCreateTableSqlParameters>,
    octet_stream: web::Bytes,
) -> Result<HttpResponse, CoralError> {
    let format = params.format.clone().unwrap_or(FileFormat::Parquet);

    match format {
        FileFormat::Parquet => {
            let bytes = Bytes::from(octet_stream);
            let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
            let reader = builder.build()?;
            let schema = reader.schema();

            Ok(HttpResponse::Ok().json(CreateTableSql {
                ddl: arrow_schema_to_ddl(params.name.clone(), schema)?,
            }))
        }
        _ => Err(CoralError::BadRequest(
            "Not supported file format".to_string(),
        )),
    }
}

pub async fn post_embedding_handler(
    embedding_request: web::Json<EmbeddingRequest>,
) -> Result<HttpResponse, CoralError> {
    debug!("EMBEDDING handler:\nrequest={:?}", embedding_request);
    let embedding_result = get_embedding(embedding_request.clone()).await?;
    Ok(HttpResponse::Ok().json(embedding_result))
}

pub async fn get_indexed_count_handler(
    app_state: web::Data<AppState>,
    table_name: web::Path<String>,
) -> Result<HttpResponse, CoralError> {
    let table_name = table_name.into_inner();
    let pools = app_state.node_pools.read().unwrap();

    let futures = pools.iter().map(|(node, pool)| async {
        let table_name = table_name.clone();
        let node = node.clone();
        let conn = pool.get().await;
        tokio::spawn(async move {
            debug!("SCAN request to {}", node);
            let (total_row_count, indexed_count) =
                shdb_get_indexed_count(&mut conn?, &table_name).await?;
            Ok::<TableIndexedCount, CoralError>(TableIndexedCount {
                indexed_count,
                total_row_count,
            })
        })
    });

    let future_results = join_all(futures).await;

    let mut table_indexed_counts = Vec::new();
    for indexed_count in future_results {
        match indexed_count.await {
            Ok(Ok(result)) => table_indexed_counts.push(result),
            Ok(Err(err)) => return Err(err),
            Err(err) => return Err(CoralError::InternalError(err.to_string())),
        }
    }

    let (indexed_count, total_row_count) =
        table_indexed_counts.iter().fold((0, 0), |acc, count| {
            (acc.0 + count.indexed_count, acc.1 + count.total_row_count)
        });

    Ok(HttpResponse::Ok().json(TableIndexedCount {
        indexed_count,
        total_row_count,
    }))
}

pub async fn post_save_handler(app_state: web::Data<AppState>) -> Result<HttpResponse, CoralError> {
    debug!("SAVE handler");

    let pools = app_state.node_pools.read().unwrap();

    let futures = pools.iter().map(|(node, pool)| async {
        let node = node.clone();
        let conn = pool.get().await;
        tokio::spawn(async move {
            debug!("SAVE request to {}", node);
            let response = shdb_save(&mut conn?).await?;
            Ok::<String, CoralError>(response)
        })
    });

    let future_results = join_all(futures).await;
    for join_result in future_results {
        match join_result.await {
            Ok(Ok(result)) => debug!("{}", result),
            Ok(Err(err)) => return Err(err),
            Err(err) => return Err(CoralError::InternalError(err.to_string())),
        }
    }
    Ok(HttpResponse::Ok().finish())
}
