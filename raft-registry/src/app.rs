use std::sync::Arc;

use openraft::Config;

use crate::RegistryNodeId;
use crate::RegistryRaft;
use crate::RegistryStore;

// Representation of an application state. This struct can be shared around to share
// instances of raft, store and more.
#[derive(Clone)]
pub struct RaftRegistryApp {
    pub id: RegistryNodeId,
    pub addr: String,
    pub raft: RegistryRaft,
    pub store: Arc<RegistryStore>,
    pub config: Arc<Config>,
}
