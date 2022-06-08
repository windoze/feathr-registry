use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeConfig {
    pub snapshot_path: String,
    pub instance_prefix: String,
    pub journal_path: String,
    pub snapshot_per_events: u64,
    pub management_code: Option<String>,
}
