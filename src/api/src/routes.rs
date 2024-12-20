use super::handlers::*;
use actix_web::web;

pub fn general_routes(config: &mut web::ServiceConfig) {
    config.route("/health", web::get().to(health_check_handler));
    config.service(
        web::scope("/log")
            .route("/level", web::get().to(get_log_level_handler))
            .route("/level", web::post().to(post_log_level_handler))
            .route(
                "/print_record_batch",
                web::get().to(get_log_print_record_batch_handler),
            )
            .route(
                "/print_record_batch",
                web::post().to(post_log_print_record_batch_handler),
            ),
    );
}

pub fn seahorse_routes(config: &mut web::ServiceConfig) {
    config.service(
        web::scope("/v0")
            .route("/ann", web::post().to(post_ann_handler))
            .route("/batch-ann", web::post().to(post_batch_ann_handler))
            .route(
                "/tables/{table_name}/indexed-count",
                web::get().to(get_indexed_count_handler),
            )
            .route("/save", web::post().to(post_save_handler))
            .route("/scan", web::post().to(post_scan_handler))
            .service(
                web::scope("/utils")
                    .route("/ddl", web::post().to(post_ddl_from_file))
                    .route("/embedding", web::post().to(post_embedding_handler)),
            )
            .service(
                web::scope("/nodes")
                    .route("", web::get().to(get_nodes_handler))
                    .route("", web::post().to(post_nodes_handler))
                    .route("/{node_name}", web::delete().to(delete_node_handler))
                    .route("/{node_name}/ping", web::get().to(get_node_ping_handler))
                    .route(
                        "/{node_name}/tables",
                        web::get().to(get_node_tables_handler),
                    )
                    .route(
                        "/{node_name}/tables",
                        web::post().to(post_node_tables_handler),
                    )
                    .route(
                        "/{node_name}/tables/{table_name}",
                        web::put().to(put_node_table_handler),
                    )
                    .route(
                        "/{node_name}/tables/{table_name}",
                        web::delete().to(delete_node_table_handler),
                    )
                    .route(
                        "/{node_name}/tables/{table_name}/import",
                        web::post().to(post_node_table_import_handler),
                    )
                    .route(
                        "/{node_name}/tables/{table_name}/schema",
                        web::get().to(get_node_table_schema_handler),
                    ),
            ),
    );
}
