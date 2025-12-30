use std::env;

pub struct Config {
    pub share_root: String,
    pub target_glob: String,
    pub poll_interval_secs: u64,
    pub db_path: String,
    pub log_level: String,
}

impl Config {
    pub fn from_env() -> Self {
        let share_root = env::var("SHARE_ROOT").unwrap_or_else(|_| String::from("."));
        let target_glob = env::var("TARGET_GLOB").unwrap_or_else(|_| String::from("**/*.csv"));
        let poll_interval_secs = env::var("POLL_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(30);
        let db_path = env::var("DB_PATH").unwrap_or_else(|_| String::from("data/app.db"));
        let log_level = env::var("LOG_LEVEL").unwrap_or_else(|_| String::from("info"));
        Self { share_root, target_glob, poll_interval_secs, db_path, log_level }
    }
}
