use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use log::debug;
use log::trace;
use openraft::error::CheckIsLeaderError;
use openraft::error::InitializeError;
use openraft::raft::ClientWriteRequest;
use openraft::Config;
use openraft::EntryPayload;
use openraft::Node;
use openraft::Raft;
use openraft::SnapshotPolicy;
use registry_api::ApiError;
use registry_api::FeathrApiProvider;
use registry_api::FeathrApiRequest;
use registry_api::FeathrApiResponse;

use crate::ManagementCode;
use crate::RegistryClient;
use crate::RegistryNetwork;
use crate::RegistryNodeId;
use crate::RegistryRaft;
use crate::RegistryStore;
use crate::Restore;

// Representation of an application state. This struct can be shared around to share
// instances of raft, store and more.
#[derive(Clone)]
pub struct RaftRegistryApp {
    pub id: RegistryNodeId,
    pub addr: String,
    pub raft: RegistryRaft,
    pub store: Arc<RegistryStore>,
    pub config: Arc<Config>,
    pub forwarder: RegistryClient,
}

impl RaftRegistryApp {
    pub async fn new(node_id: RegistryNodeId, addr: String, cfg: crate::NodeConfig) -> Self {
        // Create a configuration for the raft instance.

        let mut config = Config::default().validate().unwrap();
        config.snapshot_policy = SnapshotPolicy::LogsSinceLast(cfg.snapshot_per_events);
        config.max_applied_log_to_keep = 20000;
        config.install_snapshot_timeout = 400;

        let config = Arc::new(config);

        // Create a instance of where the Raft data will be stored.
        let es = RegistryStore::open_create(node_id, cfg.clone());

        // es.load_latest_snapshot().await.unwrap();

        let mut store = Arc::new(es);

        store.restore().await;

        // Create the network layer that will connect and communicate the raft instances and
        // will be used in conjunction with the store created above.
        let network = RegistryNetwork::new(cfg);

        // Create a local raft instance.
        let raft = Raft::new(node_id, config.clone(), network, store.clone());

        let forwarder = RegistryClient::new(node_id, addr.clone(), store.get_management_code());

        // Create an application that will store all the instances created above, this will
        // be later used on the web services.
        RaftRegistryApp {
            id: node_id,
            addr,
            raft,
            store,
            config,
            forwarder,
        }
    }

    pub async fn check_code(&self, code: Option<ManagementCode>) -> poem::Result<()> {
        trace!("Checking code {:?}", code);
        match self.store.get_management_code() {
            Some(c) => match code.map(|c| c.code().to_string()) {
                Some(code) => {
                    if c == code {
                        return Ok(());
                    } else {
                        return Err(ApiError::Forbidden("forbidden".to_string()))?;
                    }
                }
                None => return Err(ApiError::Forbidden("forbidden".to_string()))?,
            },
            None => return Ok(()),
        }
    }

    pub async fn init(&self) -> Result<(), InitializeError<RegistryNodeId>> {
        let mut nodes = BTreeMap::new();
        nodes.insert(
            self.id,
            Node {
                addr: self.addr.clone(),
                data: Default::default(),
            },
        );
        self.raft.initialize(nodes).await
    }

    pub async fn request(&self, opt_seq: Option<u64>, req: FeathrApiRequest) -> FeathrApiResponse {
        let mut is_leader = true;
        let should_forward = match self.raft.is_leader().await {
            Ok(_) => {
                // This instance is the leader, do not forward
                trace!("This node is the leader");
                false
            }
            Err(CheckIsLeaderError::ForwardToLeader(node_id)) => {
                debug!("Should forward the request to node {}", node_id);
                is_leader = false;
                match opt_seq {
                    Some(seq) => match self.store.state_machine.read().await.last_applied_log {
                        Some(l) => {
                            // Check is local log index is newer than required seq, forward if local is out dated
                            trace!("Local log index is {}, required seq is {}", l.index, seq);
                            l.index < seq
                        }
                        None => {
                            // There is no local log index, so we have to forward
                            trace!("No last applied log");
                            true
                        }
                    },
                    // opt_seq is not set, forward to the leader for consistent read
                    None => true,
                }
            }
            Err(e) => {
                trace!("Check leader failed, error is {:?}", e);
                false
                // return FeathrApiResponse::Error(ApiError::InternalError("Raft cluster error".to_string()));
            }
        };
        if should_forward {
            debug!("The request is being forwarded to the leader");
            match self.forwarder.consistent_request(&req).await {
                Ok(v) => v,
                Err(e) => FeathrApiResponse::Error(ApiError::InternalError(format!("{:?}", e))),
            }
        } else {
            debug!("The request is being handled locally");
            // Only writing requests need to go to raft state machine
            if req.is_writing_request() {
                if is_leader {
                    let request = ClientWriteRequest::new(EntryPayload::Normal(req));
                    self.raft
                        .client_write(request)
                        .await
                        .map(|r| r.data)
                        .unwrap_or_else(|e| {
                            FeathrApiResponse::Error(ApiError::InternalError(format!("{:?}", e)))
                        })
                } else {
                    FeathrApiResponse::Error(ApiError::BadRequest(
                        "Updating requests must be submitted to the Raft leader".to_string(),
                    ))
                }
            } else {
                self.store
                    .state_machine
                    .write()
                    .await
                    .registry
                    .request(req)
                    .await
            }
        }
    }

    pub async fn join_cluster(&self, seeds: &[String], promote: bool) -> anyhow::Result<()> {
        // `self.forwarder` is unusable at the moment as this node is not member of any cluster
        for seed in seeds {
            debug!("Collecting cluster info from {}", seed);
            let client =
                RegistryClient::new(1, seed.to_owned(), self.store.get_management_code());
            if let Ok(metrics) = client.metrics().await {
                if let Some(leader_id) = metrics.current_leader {
                    if let Some(leader_node) = metrics.membership_config.get_node(&leader_id) {
                        debug!("Found leader node {} at {}", leader_id, leader_node.addr);
                        debug!(
                            "Trying to join the cluster via leader node {} at '{}'",
                            leader_id, leader_node.addr
                        );
                        // Create a new client that points to the leader instead of seed
                        let client = RegistryClient::new(
                            leader_id,
                            leader_node.addr.to_owned(),
                            self.store.get_management_code(),
                        );
                        debug!("Adding this node into the cluster as learner");
                        if let Ok(resp) = client.add_learner((self.id, self.addr.clone())).await
                        {
                            trace!("Got response {:?}", resp);
                            debug!("This node has joined the cluster as learner");
                            if promote {
                                debug!("Promoting this node into voter");
                                // Fetch metrics from the leader node
                                if let Ok(metrics) = client.metrics().await {
                                    debug!("Collecting node info from the leader");
                                    let mut nodes: BTreeSet<RegistryNodeId> = metrics
                                        .membership_config
                                        .get_nodes()
                                        .keys()
                                        .map(|&id| id)
                                        .collect();
                                    debug!("Found nodes: {:?}", nodes);
                                    nodes.insert(self.id);
                                    debug!("Updating cluster membership");
                                    if let Ok(resp) = client.change_membership(&nodes).await {
                                        trace!("Got response {:?}", resp);
                                        debug!("Node {} promoted into voter", self.id);
                                        return Ok(())
                                    }
                                }
                            }
                        }
                    }
                }
            }
            debug!("Failed to join the cluster via seed {}", seed);
        }
        Err(anyhow::Error::msg("Failed to join the cluster"))
    }
}