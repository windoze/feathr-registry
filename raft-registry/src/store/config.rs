use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub snapshot_path: String,
    pub instance_prefix: String,
    pub journal_path: String,
    pub snapshot_per_events: u32,
    pub management_code: Option<String>,
}
