use super::{errors::CoralError, resultset::ipc_to_schema};
use arrow_schema::SchemaRef;
use byteorder::{LittleEndian, WriteBytesExt};
use deadpool_redis::redis::{cmd, RedisResult};
use log::debug;
use redis::aio::ConnectionLike;

pub async fn shdb_ping(conn: &mut impl ConnectionLike) -> RedisResult<String> {
    let response = cmd("PING").query_async(conn).await?;
    Ok(response)
}

pub async fn shdb_create_table(
    conn: &mut impl ConnectionLike,
    schema_ipc_message: &Vec<u8>,
) -> RedisResult<String> {
    debug!("create table request");
    let response = cmd("TABLE")
        .arg("create")
        .arg(schema_ipc_message)
        .query_async(conn)
        .await?;
    Ok(response)
}

pub async fn shdb_list_tables(conn: &mut impl ConnectionLike) -> RedisResult<Vec<String>> {
    let response: Vec<String> = cmd("TABLE").arg("LIST").query_async(conn).await?;
    Ok(response)
}

pub async fn shdb_describe_table(
    conn: &mut impl ConnectionLike,
    table_name: &String,
) -> RedisResult<Vec<u8>> {
    let response: Vec<u8> = cmd("TABLE")
        .arg("DESCRIBE")
        .arg(&table_name)
        .query_async(conn)
        .await?;
    Ok(response)
}

pub async fn shdb_drop_table(
    conn: &mut impl ConnectionLike,
    table_name: &String,
) -> RedisResult<String> {
    let response: String = cmd("TABLE")
        .arg("DROP")
        .arg(&table_name)
        .query_async(conn)
        .await?;
    Ok(response)
}

pub async fn shdb_table_batchinsert(
    conn: &mut impl ConnectionLike,
    table_name: &String,
    ipc: Vec<u8>,
) -> RedisResult<String> {
    let response = cmd("TABLE")
        .arg("BATCHINSERT")
        .arg(table_name)
        .arg(ipc)
        .query_async(conn)
        .await?;
    Ok(response)
}

pub async fn shdb_table_scan(
    conn: &mut impl ConnectionLike,
    table_name: &String,
    projection: &String,
    filter: &String,
) -> RedisResult<Vec<u8>> {
    let response = cmd("TABLE")
        .arg("SCAN")
        .arg(table_name)
        .arg(projection)
        .arg(filter)
        .query_async(conn)
        .await?;
    Ok(response)
}

pub async fn shdb_table_ann(
    conn: &mut impl ConnectionLike,
    table_name: &String,
    topk: &u32,
    vector: &Vec<f32>,
    maybe_ef_search: &Option<u32>,
    maybe_projection: &Option<String>,
    maybe_filter: &Option<String>,
) -> RedisResult<Vec<u8>> {
    let query_as_bytes = {
        let mut wtr = Vec::with_capacity(vector.len() * std::mem::size_of::<f32>());
        for &f in vector {
            wtr.write_f32::<LittleEndian>(f).unwrap();
        }
        wtr
    };
    let ef_search: &u32 = match maybe_ef_search {
        Some(ef_search) => ef_search,
        None => topk,
    };
    let projection = match maybe_projection {
        Some(projection) => projection.clone(),
        None => "*".to_string(),
    };
    let filter = match maybe_filter {
        Some(filter) => filter.clone(),
        None => "".to_string(),
    };

    let response = cmd("TABLE")
        .arg("ANN")
        .arg(table_name)
        .arg(topk)
        .arg(query_as_bytes)
        .arg(ef_search)
        .arg(projection)
        .arg(filter)
        .query_async(conn)
        .await?;
    Ok(response)
}

pub async fn shdb_table_batch_ann(
    conn: &mut impl ConnectionLike,
    table_name: &String,
    topk: &u32,
    vectors: &Vec<u8>,
    maybe_ef_search: &Option<u32>,
    maybe_projection: &Option<String>,
    maybe_filter: &Option<String>,
) -> RedisResult<Vec<Vec<u8>>> {
    let ef_search: &u32 = match maybe_ef_search {
        Some(ef_search) => ef_search,
        None => topk,
    };
    let projection = match maybe_projection {
        Some(projection) => projection.clone(),
        None => "*".to_string(),
    };
    let filter = match maybe_filter {
        Some(filter) => filter.clone(),
        None => "".to_string(),
    };
    let response = cmd("TABLE")
        .arg("BATCHANN")
        .arg(table_name)
        .arg(topk)
        .arg(vectors)
        .arg(ef_search)
        .arg(projection)
        .arg(filter)
        .query_async(conn)
        .await?;
    Ok(response)
}

pub async fn shdb_get_table_schema(
    conn: &mut impl ConnectionLike,
    table_name: &String,
) -> Result<SchemaRef, CoralError> {
    let describe_result = shdb_describe_table(conn, &table_name).await?;
    ipc_to_schema(&describe_result)
}

pub async fn shdb_get_indexed_count(
    conn: &mut impl ConnectionLike,
    table_name: &String,
) -> RedisResult<(i32, i32)> {
    let (total_row_count, indexed_count) = cmd("TABLE")
        .arg("COUNTINDEXEDELEMENTS")
        .arg(table_name)
        .query_async(conn)
        .await?;
    Ok((total_row_count, indexed_count))
}

pub async fn shdb_save(conn: &mut impl ConnectionLike) -> RedisResult<String> {
    let response = cmd("BGREWRITEAOF").query_async(conn).await?;
    Ok(response)
}
