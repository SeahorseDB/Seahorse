use deadpool_redis::{Config, Pool, Runtime};

// Function to create a Redis pool remains the same
pub fn create_redis_pool(url: &str) -> Pool {
    let cfg = Config::from_url(url);
    cfg.create_pool(Some(Runtime::Tokio1)).unwrap()
}
