use actix_cors::Cors;
use actix_multipart::form::MultipartFormConfig;
use actix_web::{
    web::{self, PayloadConfig},
    App, HttpServer,
};
use models::Nodes;
use std::{
    collections::HashMap,
    sync::{Mutex, RwLock},
    thread,
};

#[path = "ddl.rs"]
mod ddl;
#[path = "errors.rs"]
mod errors;
#[path = "handlers.rs"]
mod handlers;
#[path = "models.rs"]
mod models;
#[path = "pool.rs"]
mod pool;
#[path = "reader.rs"]
mod reader;
#[path = "record_batch.rs"]
mod record_batch;
#[path = "resultset.rs"]
mod resultset;
#[path = "routes.rs"]
mod routes;
#[path = "schema.rs"]
mod schema;
#[path = "seahorse.rs"]
mod seahorse;
#[path = "state.rs"]
mod state;
#[path = "topk.rs"]
mod topk;
#[path = "utils.rs"]
mod utils;

use actix_web::middleware::Logger;
use env_logger::{fmt::style, Env};
use pool::*;
use routes::*;
use state::AppState;
use std::env;

#[cfg(debug_assertions)]
fn default_seahorse_db_nodes() -> Nodes {
    use log::debug;
    debug!(
        r#"Using default Seahorse DB nodes for local development, "node1: redis://127.0.0.1:6379""#
    );
    serde_json::from_str(r#"{"nodes" : [{"name": "node1", "endpoint": "redis://127.0.0.1:6379"}]}"#)
        .unwrap()
}

#[cfg(not(debug_assertions))]
fn default_seahorse_db_nodes() -> Nodes {
    use log::debug;
    match env::var("SEAHORSE_DB_NODES") {
        Ok(env_var) => {
            debug!("SEAHORSE_DB_NODES variable: {}", env_var);
            match serde_json::from_str(&env_var) {
                Ok(nodes) => {
                    debug!("SEAHORSE_DB_NODES: {:?}", nodes);
                    nodes
                }
                Err(e) => {
                    debug!("Failed to deserialize SEAHORSE_DB_NODES: {}", e);
                    Nodes { nodes: vec![] }
                }
            }
        }
        Err(_) => {
            debug!("SEAHORSE_DB_NODES is not set, using empty nodes");
            Nodes { nodes: vec![] }
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format(|buf, record| {
            use chrono::Local;
            use std::io::Write;
            use style::AnsiColor;
            let level_style = buf.default_level_style(record.level());
            let style = AnsiColor::BrightBlack.on_default();
            let thread_id = thread::current().id();
            let thread_name = thread::current().name().unwrap_or("unknown").to_owned();

            writeln!(
                buf,
                "{style}[{style:#}{} {level_style}{}{level_style:#} {} ({}:{}) ({}/{:?}){style}]{style:#} {}",
                Local::now().format("%Y-%m-%d %H:%M:%S%.6f %Z"),
                record.level(),
                record.target(),
                record.file().unwrap_or("unknown").split('/').last().unwrap_or("unknown"),
                record.line().unwrap_or(0),
                thread_name,
                thread_id,
                record.args()
            )
        })
        .init();

    let shared_data = web::Data::new(AppState {
        health_check_response: "I'm OK. You've already asked me".to_string(),
        visit_count: Mutex::new(0),
        node_pools: RwLock::new(HashMap::new()),
        is_printing_record_batch_log_enabled: Mutex::new(false),
    });

    let nodes = default_seahorse_db_nodes();
    for node in nodes.nodes {
        let mut guard = shared_data.node_pools.write().unwrap();
        let pool = create_redis_pool(&node.endpoint);
        guard.insert(node.name.clone(), pool);
    }

    let app = move || {
        App::new()
            .wrap(Logger::default())
            .wrap(Logger::new("%a %{User-Agent}i"))
            .wrap(
                Cors::default()
                    .allow_any_origin()
                    .allow_any_header()
                    .allow_any_method(),
            )
            .app_data(shared_data.clone())
            .app_data(
                // default multipart form size limit 10GB
                // default memory limit 15GB
                MultipartFormConfig::default()
                    .total_limit(
                        env::var("CORAL_MULTIPART_FORM_SIZE_LIMIT")
                            .unwrap_or("10737418240".to_string())
                            .parse::<usize>()
                            .unwrap_or(10737418240),
                    )
                    .memory_limit(
                        env::var("CORAL_MEMORY_LIMIT")
                            .unwrap_or("16106127360".to_string())
                            .parse::<usize>()
                            .unwrap_or(16106127360),
                    ),
            )
            .app_data(PayloadConfig::default().limit(10737418240))
            .configure(general_routes)
            .configure(seahorse_routes)
    };

    HttpServer::new(app)
        .bind(env::var("BIND_ADDR").unwrap_or("127.0.0.1:3000".to_string()))?
        .run()
        .await
}
