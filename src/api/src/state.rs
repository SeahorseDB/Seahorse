use deadpool_redis::Pool;
use std::{collections::HashMap, sync::Mutex, sync::RwLock};

pub struct AppState {
    pub health_check_response: String,
    pub visit_count: Mutex<i32>,
    pub node_pools: RwLock<HashMap<String, Pool>>,
    pub is_printing_record_batch_log_enabled: Mutex<bool>,
}
